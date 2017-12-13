(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open Cohttp
open Cohttp_lwt_unix
open Batteries
open BatOption.Infix
open Lwt
open RamenLog
open RamenSharedTypes
open Helpers
open RamenHttpHelpers
open Lang
module C = RamenConf
module N = RamenConf.Node
module L = RamenConf.Layer
module SL = RamenSharedTypes.Layer
module SN = RamenSharedTypes.Node

let hostname =
  let cached = ref "" in
  fun () ->
    if !cached <> "" then return !cached else
    let%lwt c =
      catch
        (fun () -> run ~timeout:2. [| "hostname" |])
        (function _ -> return "unknown host") in
    cached := c ;
    return c

let replace_placeholders conf s =
  let%lwt hostname = hostname () in
  let rep sub by str = String.nreplace ~str ~sub ~by in
  return (
    rep "$RAMEN_URL$" conf.C.ramen_url s |>
    rep "$HOSTNAME$" hostname)

let serve_string conf _headers body =
  let%lwt body = replace_placeholders conf body in
  respond_ok ~body ~ct:Consts.html_content_type ()

let layer_node_of_user_string conf ?default_layer s =
  let s = String.trim s in
  (* rsplit because we might want to have '/'s in the layer name. *)
  try String.rsplit ~by:"/" s |> return
  with Not_found ->
    match default_layer with
    | Some l -> return (l, s)
    | None ->
      (* Last resort: look for the first node with that name: *)
      match C.fold_nodes conf None (fun res node ->
              if res = None && node.N.name = s then
                Some (node.N.layer, node.N.name)
              else res) with
      | Some res -> return res
      | None -> bad_request ("node "^ s ^" does not exist")

(*
    Returns the graph (as JSON, dot or mermaid representation)
*)

let rec find_int_opt_metric metrics name =
  let open Binocle in
  match metrics with
  | [] -> None
  | { measure = MInt n ; _ } as m ::_ when m.name = name -> Some n
  | _::rest -> find_int_opt_metric rest name

let find_int_metric metrics name = find_int_opt_metric metrics name |? 0

let rec find_float_metric metrics name =
  let open Binocle in
  match metrics with
  | [] -> 0.
  | { measure = MFloat n ; _ } as m ::_ when m.name = name -> n
  | _::rest -> find_float_metric rest name

let node_info_of_node node =
  let to_expr_type_info =
    List.map (fun (_, typ) -> Expr.to_expr_type_info typ)
  in
  let%lwt stats = RamenProcesses.last_report (N.fq_name node) in
  return SN.{
    definition = {
      name = node.N.name ;
      operation = node.N.op_text ;
    } ;
    exporting = Operation.is_exporting node.N.operation ;
    signature = if node.N.signature = "" then None else Some node.N.signature ;
    pid = node.N.pid ;
    input_type = C.list_of_temp_tup_type node.N.in_type |> to_expr_type_info ;
    output_type = C.list_of_temp_tup_type node.N.out_type |> to_expr_type_info ;
    parents = List.map N.fq_name node.N.parents ;
    children = List.map N.fq_name node.N.children ;
    stats }

let layer_info_of_layer layer =
  let%lwt nodes =
    Hashtbl.values layer.L.persist.L.nodes |>
    List.of_enum |>
    Lwt_list.map_s node_info_of_node in
  return SL.{
    name = layer.L.name ;
    nodes ;
    status = layer.L.persist.L.status ;
    last_started = layer.L.persist.L.last_started ;
    last_stopped = layer.L.persist.L.last_stopped }

let graph_layers conf = function
  | None ->
    Hashtbl.values conf.C.graph.C.layers |>
    List.of_enum |>
    return
  | Some l ->
    try Hashtbl.find conf.C.graph.C.layers l |>
        List.singleton |>
        return
    with Not_found -> bad_request ("Unknown layer "^l)

let get_graph_json _headers layers =
  let%lwt graph = Lwt_list.map_s layer_info_of_layer layers in
  let body = PPP.to_string get_graph_resp_ppp graph in
  respond_ok ~body ()

let dot_of_graph layers =
  let dot = IO.output_string () in
  Printf.fprintf dot "digraph g {\n" ;
  List.iter (fun layer ->
    Hashtbl.iter (fun _ node ->
        Printf.fprintf dot "\t%S\n" (N.fq_name node)
      ) layer.L.persist.L.nodes
    ) layers ;
  Printf.fprintf dot "\n" ;
  List.iter (fun layer ->
    Hashtbl.iter (fun _ node ->
        List.iter (fun p ->
            Printf.fprintf dot "\t%S -> %S\n" (N.fq_name p) (N.fq_name node)
          ) node.N.parents
      ) layer.L.persist.L.nodes
    ) layers ;
  Printf.fprintf dot "}\n" ;
  IO.close_out dot

let get_graph_dot _headers layers =
  let body = dot_of_graph layers in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.dot_content_type in
  Server.respond_string ~headers ~status ~body ()

let mermaid_of_graph layers =
  (* Build unique identifier that are valid for mermaid: *)
  let is_alphanum c =
    Char.(is_letter c || is_digit c) in
  let mermaid_id n =
    "id_" ^ (* To make it start with an alpha *)
    String.replace_chars (fun c ->
      if is_alphanum c then String.of_char c
      else string_of_int (Char.code c)) n
  (* And valid labels *)
  and mermaid_label n =
    "\""^ String.nreplace n "\"" "#quot;" ^"\""
  in
  let txt = IO.output_string () in
  Printf.fprintf txt "graph LR\n" ;
  List.iter (fun layer ->
    Hashtbl.iter (fun _ node ->
        Printf.fprintf txt "%s(%s)\n"
          (mermaid_id (N.fq_name node))
          (mermaid_label node.N.name)
      ) layer.L.persist.L.nodes
    ) layers ;
  Printf.fprintf txt "\n" ;
  List.iter (fun layer ->
    Hashtbl.iter (fun _ node ->
        List.iter (fun p ->
            Printf.fprintf txt "\t%s-->%s\n"
              (mermaid_id (N.fq_name p))
              (mermaid_id (N.fq_name node))
          ) node.N.parents
      ) layer.L.persist.L.nodes
    ) layers ;
  IO.close_out txt

let get_graph_mermaid _headers layers =
  let body = mermaid_of_graph layers in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.mermaid_content_type in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let headers = Header.add headers "Access-Control-Allow-Methods" "POST" in
  let headers = Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
  Server.respond_string ~headers ~status ~body ()

let get_graph conf headers layer_opt =
  let accept = get_accept headers in
  let%lwt layers = C.with_rlock conf (fun () ->
    graph_layers conf layer_opt) in
  let layers = L.order layers in
  if is_accepting Consts.json_content_type accept then
    get_graph_json headers layers
  else if is_accepting Consts.dot_content_type accept then
    get_graph_dot headers layers
  else if is_accepting Consts.mermaid_content_type accept then
    get_graph_mermaid headers layers
  else
    cant_accept accept

(*
    Add/Remove layers

    Layers and nodes within a layer are referred to via name that can be
    anything as long as they are unique.  So the clients decide on the name.
    The server ensure uniqueness by forbidding creation of a new layers by the
    same name as one that exists already.

*)

let find_node_or_fail conf layer_name node_name =
  match C.find_node conf layer_name node_name with
  | exception Not_found ->
    bad_request ("Node "^ layer_name ^"/"^ node_name ^" does not exist")
  | layer, _node as both ->
    RamenProcesses.use_layer (Unix.gettimeofday ()) layer ;
    return both

let find_exporting_node_or_fail conf layer_name node_name =
  let%lwt layer, node = find_node_or_fail conf layer_name node_name in
  if not (L.is_typed layer) then
    bad_request ("node "^ node_name ^" is not typed (yet)")
  else if not (Operation.is_exporting node.N.operation) then
    bad_request ("node "^ node_name ^" does not export data")
  else return (layer, node)

let node_of_name conf layer_name node_name =
  if node_name = "" then bad_request "Empty string is not a valid node name"
  else find_node_or_fail conf layer_name node_name

let del_layer_ conf layer =
  if layer.L.persist.L.status = Running then
    bad_request "Cannot update a running layer"
  else
    try
      C.del_layer conf layer ;
      return_unit
    with C.InvalidCommand e -> bad_request e

let del_layer conf _headers layer_name =
  C.with_wlock conf (fun () ->
    match Hashtbl.find conf.C.graph.C.layers layer_name with
    | exception Not_found ->
      let e = "Layer "^ layer_name ^" does not exist" in
      bad_request e
    | layer ->
      del_layer_ conf layer) >>=
  respond_ok

let put_layer conf headers body =
  let%lwt msg = of_json headers "Uploading layer" put_layer_req_ppp body in
  let layer_name = msg.name in
  (* Disallow anonymous layers for simplicity: *)
  if layer_name = "" then
    bad_request "Layers must have non-empty names"
  (* Check that this layer is new or stopped *)
  else (
    (* Delete the layer if it already exists. No worries the conf won't be
     * changed if there is any error. *)
    C.with_wlock conf (fun () ->
      let%lwt () =
        match Hashtbl.find conf.C.graph.C.layers layer_name with
        | exception Not_found -> return_unit
        | layer -> del_layer_ conf layer in
      (* TODO: Check that this layer node names are unique within the layer *)
      (* Create all the nodes *)
      let%lwt nodes = Lwt_list.map_s (fun def ->
          let name =
            if def.SN.name <> "" then def.SN.name
            else N.make_name () in
          try
            let _layer, node =
              C.add_node conf name layer_name def.SN.operation in
            return node
          with Invalid_argument x -> bad_request ("Invalid "^ x)
             | SyntaxError e -> bad_request (string_of_syntax_error e)
             | e -> fail e
        ) msg.nodes in
      (* Then all the links *)
      (* FIXME: actually, we might have nodes of other layers on top of this
       * one, feeding from these new nodes (check the operation FROM) that
       * we should reconnect as well. *)
      Lwt_list.iter_s (fun node ->
          Operation.parents_of_operation node.N.operation |>
          Lwt_list.iter_s (fun p ->
              let%lwt parent_layer, parent_name =
                layer_node_of_user_string conf ~default_layer:layer_name p in
              let%lwt _layer, src = node_of_name conf parent_layer parent_name in
              wrap (fun () -> C.add_link conf src node)
            )
        ) nodes) >>=
    respond_ok)
  (* TODO: why wait before compiling this layer? *)

(*
    Serving normal files
*)

let serve_file_with_replacements conf _headers path file =
  serve_file path file (replace_placeholders conf)

(*
    Whole graph operations: compile/run/stop
*)

let compile conf headers layer_opt =
  catch
    (fun () ->
      let rec loop left_try layers =
        !logger.debug "%d layers left to compile..." (List.length layers) ;
        if layers = [] then return_unit else
        if left_try < 0 then bad_request "Unsolvable dependency loop" else
        Lwt_list.fold_left_s (fun failed layer ->
            let open Compiler in
            catch
              (fun () -> let%lwt () = compile conf layer in
                         return failed)
              (function AlreadyCompiled -> return failed
                      | MissingDependency n ->
                        !logger.debug "We miss node %s" (N.fq_name n) ;
                        return (layer::failed)
                      | e -> fail e)
          ) [] layers >>=
        loop (left_try-1)
      in
      let%lwt () =
        C.with_wlock conf (fun () ->
          let%lwt layers = graph_layers conf layer_opt in
          loop (List.length layers) layers) in
      switch_accepted headers [
        Consts.json_content_type, (fun () -> respond_ok ()) ])
    (function SyntaxError _
            | Compiler.SyntaxErrorInNode _
            | C.InvalidCommand _ as e ->
              bad_request (Printexc.to_string e)
            | e -> fail e)

let run conf headers layer_opt =
  catch
    (fun () ->
      let%lwt () =
        C.with_wlock conf (fun () ->
          let%lwt layers = graph_layers conf layer_opt in
          let layers = L.order layers in
          wrap (fun () ->
            List.iter (fun layer ->
                let open RamenProcesses in
                try run conf layer with AlreadyRunning -> ()
              ) layers)) in
      switch_accepted headers [
        Consts.json_content_type, (fun () -> respond_ok ()) ])
    (function SyntaxError _
            | Compiler.SyntaxErrorInNode _
            | C.InvalidCommand _ as e ->
              bad_request (Printexc.to_string e)
            | x -> fail x)

let stop_layers conf layer_opt =
  C.with_wlock conf (fun () ->
    let%lwt layers = graph_layers conf layer_opt in
    wrap (fun () ->
      List.iter (fun layer ->
          let open RamenProcesses in
          try stop conf layer with NotRunning -> ()
        ) layers))

let stop conf headers layer_opt =
  catch
    (fun () ->
      let%lwt () = stop_layers conf layer_opt in
      switch_accepted headers [
        Consts.json_content_type, (fun () -> respond_ok ()) ])
    (function C.InvalidCommand e -> bad_request e
            | x -> fail x)

let shutdown conf _headers =
  (* TODO: also log client info *)
  !logger.info "Asked to shut down" ;
  (* Stop all workers *)
  let%lwt () = stop_layers conf None in
  (* Hopefully cohttp will serve this answer before stopping. *)
  List.iter (fun condvar ->
    Lwt_condition.signal condvar ()) !http_server_done ;
  respond_ok ()

(*
    Exporting tuples

    Clients can request to be sent the tuples from an exporting node.
*)

let get_tuples conf ?since ?max_res ?(wait_up_to=0.) layer_name node_name =
  (* Check that the node exists and exports *)
  let%lwt _layer, node =
    find_exporting_node_or_fail conf layer_name node_name in
  let k = RamenExport.history_key node in
  match Hashtbl.find RamenExport.imported_tuples k with
  | exception Not_found -> (* Nothing yet, just answer with empty result *)
      return (0, [])
  | history ->
      let start = Unix.gettimeofday () in
      let tuple_type = C.tup_typ_of_temp_tup_type node.N.out_type in
      let rec loop () =
        let first, values =
          RamenExport.fold_tuples_since
            ?since ?max_res history (None, [])
              (fun _ seqnum tup (first, prev) ->
                let first =
                  if first = None then Some seqnum else first in
                first, List.cons tup prev) in
        let first = first |? (since |? 0) in
        let dt = Unix.gettimeofday () -. start in
        if values = [] && dt < wait_up_to then (
          (* TODO: sleep for dt, queue the wakener on this history,
           * and wake all the sleeps when a tuple is received *)
          Lwt_unix.sleep 0.1 >>= loop
        ) else (
          return (
            first,
            RamenExport.export_columns_of_tuples tuple_type values |>
            List.map (fun (typ, nullmask, column) ->
              typ, Option.map RamenBitmask.to_bools nullmask, column))
        ) in
      loop ()

let export conf headers layer_name node_name body =
  let%lwt () = check_accept headers Consts.json_content_type in
  let%lwt req =
    if body = "" then return empty_export_req else
    of_json headers ("Exporting from "^ node_name) export_req_ppp body in
  let%lwt first, columns =
    C.with_rlock conf (fun () ->
      get_tuples conf ?since:req.since ?max_res:req.max_results
                      ~wait_up_to:req.wait_up_to layer_name node_name) in
  let resp = { first ; columns } in
  let body = PPP.to_string export_resp_ppp resp in
  respond_ok ~body ()

(*
    Grafana Datasource: autocompletion of node/field names
*)

let complete_nodes conf headers body =
  let%lwt msg =
    of_json headers "Complete tables" complete_node_req_ppp body in
  let%lwt lst =
    C.with_rlock conf (fun () ->
      C.complete_node_name conf msg.node_prefix msg.only_exporting |>
      return) in
  let body =
    PPP.to_string complete_resp_ppp lst
  in
  respond_ok ~body ()

let complete_fields conf headers body =
  let%lwt msg =
    of_json headers "Complete fields" complete_field_req_ppp body in
  let%lwt lst =
    C.with_rlock conf (fun () ->
      C.complete_field_name conf msg.node msg.field_prefix |>
      return) in
  let body =
    PPP.to_string complete_resp_ppp lst
  in
  respond_ok ~body ()

(*
    Grafana Datasource: data queries
*)

let timeseries conf headers body =
  let%lwt msg =
    of_json headers "time series query" timeseries_req_ppp body in
  let ts_of_node_field req layer node data_field =
    let%lwt _layer, node = find_exporting_node_or_fail conf layer node in
    let open RamenExport in
    let consolidation =
      match String.lowercase req.consolidation with
      | "min" -> bucket_min | "max" -> bucket_max | _ -> bucket_avg in
    wrap (fun () ->
      build_timeseries node data_field msg.max_data_points
                       msg.since msg.until consolidation)
  and create_temporary_node select_x select_y from where =
    (* First, we need to find out the name for this operation, and create it if
     * it does not exist yet. Name must be given by the operation and parent, so
     * that we do not create new nodes when not required (avoiding a costly
     * compilation and losing export history). This is not equivalent to the
     * signature: the signature identifies operation and types (aka the binary)
     * but not the data (since the same worker can be placed at several places
     * in the graph); while here we want to identify the data, that depends on
     * everything the user sent (aka operation text and parent name) but for the
     * formatting. We thus start by parsing and pretty-printing the operation: *)
    let%lwt parent_layer, parent_name =
      layer_node_of_user_string conf from in
    let%lwt _layer, parent = node_of_name conf parent_layer parent_name in
    let%lwt op_text =
      if select_x = "" then (
        let open Operation in
        match parent.N.operation with
        | Aggregate { export = Some (Some ((start, scale), DurationConst dur)) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s AS data \
             FROM '%s' \
             EXPORT EVENT STARTING AT %s * %g WITH DURATION %g"
            start select_y
            parent.N.name
            start scale dur |> return
        | Aggregate { export = Some (Some ((start, scale), DurationField (dur, scale2))) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s AS data \
             FROM '%s' \
             EXPORT EVENT STARTING AT %s * %g WITH DURATION %s * %g"
            start select_y
            parent.N.name
            start scale dur scale2 |> return
        | Aggregate { export = Some (Some ((start, scale), StopField (stop, scale2))) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s, %s AS data \
             FROM '%s' \
             EXPORT EVENT STARTING AT %s * %g AND STOPPING AT %s * %g"
            start stop select_y
            parent.N.name
            start scale stop scale2 |> return
        | _ ->
          bad_request "This parent does not provide time information"
      ) else return (
        "SELECT "^ select_x ^" AS time, "
                 ^ select_y ^" AS data \
         FROM '"^ parent.N.name ^"' \
         EXPORT EVENT STARTING AT time") in
    let op_text =
      if where = "" then op_text else op_text ^" WHERE "^ where in
    let%lwt operation = wrap (fun () -> C.parse_operation op_text) in
    let reformatted_op = IO.to_string Operation.print operation in
    let layer_name =
      "temp/"^ Cryptohash_md4.(string reformatted_op |> to_hex)
    and node_name = "operation" in
    (* So far so good. In all likelihood this layer exists already: *)
    (if Hashtbl.mem conf.C.graph.C.layers layer_name then (
      !logger.debug "Layer %S already there" layer_name ;
      return_unit
    ) else (
      (* Add this layer to the running configuration: *)
      let layer, node =
        C.add_parsed_node ~timeout:300.
          conf node_name layer_name op_text operation in
      let%lwt () = wrap (fun () -> C.add_link conf parent node) in
      let%lwt () = Compiler.compile conf layer in
      wrap (fun () -> RamenProcesses.run conf layer)
    )) >>= fun () ->
    return (layer_name, node_name, "data")
  in
  catch
    (fun () ->
      let%lwt resp = Lwt_list.map_s (fun req ->
          let%lwt layer_name, node_name, data_field =
            match req.spec with
            | Predefined { node ; data_field } ->
              let%lwt layer, node = layer_node_of_user_string conf node in
              return (layer, node, data_field)
            | NewTempNode { select_x ; select_y ; from ; where } ->
              C.with_wlock conf (fun () ->
                create_temporary_node select_x select_y from where) in
          let%lwt times, values =
            C.with_rlock conf (fun () ->
              ts_of_node_field req layer_name node_name data_field) in
          return { id = req.id ; times ; values }
        ) msg.timeseries in
      let body = PPP.to_string timeseries_resp_ppp resp in
      respond_ok ~body ())
    (function Failure err -> bad_request err | e -> fail e)

let timerange_of_node node =
  let open RamenSharedTypesJS in
  let k = RamenExport.history_key node in
  match Hashtbl.find RamenExport.imported_tuples k with
  | exception Not_found ->
    !logger.debug "Node %s has no history" (N.fq_name node) ; NoData
  | h ->
    (match RamenExport.hist_min_max h with
    | None -> NoData
    | Some (sta, sto) ->
      let oldest, latest =
        RamenExport.timerange_of_filenum node h sta in
      let latest =
        if sta = sto then latest
        else snd (RamenExport.timerange_of_filenum node h sto) in
      TimeRange (oldest, latest))

let get_timerange conf headers layer_name node_name =
  let%lwt resp =
    C.with_rlock conf (fun () ->
      let%lwt _layer, node =
        find_exporting_node_or_fail conf layer_name node_name in
      return (timerange_of_node node)) in
  switch_accepted headers [
    Consts.json_content_type, (fun () ->
      let body = PPP.to_string time_range_resp_ppp resp in
      respond_ok ~body ()) ]

(* A thread that hunt for unused layers *)
let rec timeout_layers conf =
  let%lwt () = wrap (fun () -> RamenProcesses.timeout_layers conf) in
  let%lwt () = Lwt_unix.sleep 7.1 in
  timeout_layers conf

(*
   Obtaining an SVG plot for an exporting node
*)

let plot conf _headers layer_name node_name params =
  (* Get all the parameters: *)
  let get ?def name conv =
    let lwt_conv s =
      try return (conv s)
      with Failure s -> bad_request ("Parameter "^ name ^": "^ s) in
    match Hashtbl.find params name with
    | exception Not_found ->
      (match def with
      | None -> bad_request ("Parameter "^ name ^" is mandatory")
      | Some def -> lwt_conv def)
    | x -> lwt_conv x in
  let to_relto s =
    match String.lowercase s with
    | "metric" -> true
    | "wallclock" -> false
    | _ -> failwith "rel must be 'metric' or 'wallclock'"
  and to_stacked s =
    let open RamenChart in
    match String.lowercase s with
    | "no" | "not" -> NotStacked
    | "yes" -> Stacked
    | "centered" -> StackedCentered
    | _ -> failwith "must be 'yes', 'no' or 'centered'"
  and to_float s =
    try float_of_string s
    with _ -> failwith "must be a float"
  and to_bool s =
    try bool_of_string s
    with _ -> failwith "must be 'false' or 'true'"
  in
  let%lwt fields = get "fields" identity in
  let fields = String.split_on_char ',' fields in
  let%lwt svg_width = get "width" ~def:"800" to_float in
  let%lwt svg_height = get "height" ~def:"400" to_float in
  let%lwt rel_to_metric = get "relto" ~def:"metric" to_relto in
  let%lwt duration = get "duration" ~def:"10800" to_float in
  let%lwt force_zero = get "force_zero" ~def:"false" to_bool in
  let%lwt stacked = get "stacked" ~def:"no" to_stacked in
  let%lwt single_field =
    match fields with
    | [] -> bad_request "You must supply at least one field"
    | [_] -> return_true
    | _ -> return_false in
  (* Fetch timeseries: *)
  let%lwt _layer, node =
    C.with_rlock conf (fun () ->
      find_exporting_node_or_fail conf layer_name node_name) in
  let now = Unix.gettimeofday () in
  let%lwt until =
    if rel_to_metric then
      match timerange_of_node node with
      | NoData -> return now
      | TimeRange (_oldest, latest) -> return latest
    else return now in
  let since = until -. duration in
  let pen_of_field field_name =
    RamenChart.{
      label = field_name ; draw_line = true ; draw_points = true ;
      color = RamenColor.random_of_string field_name ;
      stroke_width = 1.5 ; opacity = 1. ;
      dasharray = None ; filled = true ; fill_opacity = 0.3 } in
  let%lwt data_points =
    wrap (fun () ->
      List.map (fun data_field ->
          pen_of_field data_field,
          RamenExport.(
            build_timeseries node data_field (int_of_float svg_width + 1)
                             since until bucket_avg)
        ) fields) in
  let _fst_pen, (fst_times, _fst_data) = List.hd data_points in
  let nb_pts = Array.length fst_times in
  let shash = RamenChart.{
    create = (fun () -> Hashtbl.create 11) ;
    find = Hashtbl.find_option ;
    add = Hashtbl.add } in
  let open RamenHtml in
  let html =
    if nb_pts = 0 then svg svg_width svg_height [] [ svgtext "No data" ]
    else
    let vx_start = fst_times.(0) and vx_stop = fst_times.(nb_pts-1) in
    let fold = RamenChart.{ fold = fun f i ->
      List.fold_left (fun i (pen, (_times, data)) ->
        (* FIXME: xy_plout should handle None itself *)
        let getter j = data.(j) |? 0. in
        f i pen true getter) i data_points } in
    RamenChart.xy_plot
      ~svg_width ~svg_height ~force_show_0:force_zero ~stacked_y1:stacked
      ~string_of_x:RamenFormats.((timestamp string_of_timestamp).to_label)
      "time" (if single_field then List.hd fields else "")
      vx_start vx_stop nb_pts shash fold in
  let body = string_of_html html in
  let headers = Header.init_with "Content-Type" Consts.svg_content_type in
  let status = `Code 200 in
  Server.respond_string ~headers ~status ~body ()

(*
    Data Upload
    Data is then written to tmp_input_dirname/uploads/suffix/$random
*)

let save_in_tmp_file dir body =
  mkdir_all dir ;
  let fname = random_string 10 in
  let path = dir ^"/"^ random_string 10 in
  Lwt_io.(with_file Output path (fun oc ->
    let%lwt () = write oc body in
    return (path, fname)))

let upload conf headers layer node body =
  let%lwt _layer, node =
    C.with_rlock conf (fun () ->
      find_node_or_fail conf layer node) in
  (* Look for the node handling this suffix: *)
  match node.N.operation with
  | ReadCSVFile { where = ReceiveFile ; _ } ->
    let dir = C.upload_dir_of_node conf.C.persist_dir node in
    let ct = get_content_type headers |> String.lowercase in
    let content =
      if ct = Consts.urlencoded_content_type then Uri.pct_decode body
      else if ct = Consts.text_content_type then body
      else (
        !logger.info "Don't know how to convert content type '%s' into \
                      CSV, trying no conversion." ct ;
        body) in
    let%lwt path, fname = save_in_tmp_file dir content in
    Lwt_unix.rename path (dir ^"/_"^ fname) >>=
    respond_ok
  | _ ->
    bad_request ("Node "^ N.fq_name node ^" does not accept uploads")

let start debug daemonize rand_seed no_demo to_stderr ramen_url www_dir
          version_tag persist_dir default_team port cert_opt key_opt () =
  let demo = not no_demo in (* FIXME: in the future do not start demo by default? *)
  if to_stderr && daemonize then
    failwith "Options --daemonize and --to-stderr are incompatible." ;
  (match rand_seed with
  | None -> Random.self_init ()
  | Some seed -> Random.init seed) ;
  let logdir = if to_stderr then None else Some (persist_dir ^"/log") in
  Option.may mkdir_all logdir ;
  logger := make_logger ?logdir debug ;
  let conf =
    C.make_conf true ramen_url debug version_tag persist_dir default_team 5 (* TODO *) in
  (* When there is nothing to do, listen to collectd and netflow! *)
  if demo && Hashtbl.is_empty conf.C.graph.C.layers then (
    !logger.info "Adding default nodes since we have nothing to do..." ;
    C.add_node conf "collectd" "demo" "LISTEN FOR COLLECTD" |> ignore ;
    C.add_node conf "netflow" "demo" "LISTEN FOR NETFLOW" |> ignore) ;
  async (fun () -> timeout_layers conf) ;
  C.save_graph conf ;
  (* Read the instrumentation ringbuf: *)
  RamenProcesses.read_reports conf ;
  (* Start the alerter *)
  RamenAlerter.start_escalation_loop conf ;
  (* Start the HTTP server: *)
  let lyr = function
    | [] -> bad_request_exn "Layer name missing from URL"
    | lst -> String.concat "/" lst in
  let lyr_node_of path =
    let rec loop ls = function
      | [] -> bad_request_exn "node name missing from URL"
      | [x] ->
        if ls = [] then bad_request_exn "node name missing from URL"
        else lyr (List.rev ls), x
      | l::rest ->
        loop (l :: ls) rest in
    loop [] path in
  let alert_id_of_string s =
    match int_of_string s with
    | exception Failure _ ->
        bad_request "alert id must be numeric"
    | id -> return id in
  (* The function called for each HTTP request: *)
  let router meth path params headers body =
    match meth, path with
    (* Ramen API *)
    | `GET, ["graph"] ->
      get_graph conf headers None
    | `GET, ("graph" :: layers) ->
      get_graph conf headers (Some (lyr layers))
    | `PUT, ["graph"] ->
      put_layer conf headers body
    | `DELETE, ("graph" :: layers) ->
      del_layer conf headers (lyr layers)
    | `GET, ["compile"] ->
      compile conf headers None
    | `GET, ("compile" :: layers) ->
      compile conf headers (Some (lyr layers))
    | `GET, ["run" | "start"] ->
      run conf headers None
    | `GET, (("run" | "start") :: layers) ->
      run conf headers (Some (lyr layers))
    | `GET, ["stop"] ->
      stop conf headers None
    | `GET, ("stop" :: layers) ->
      stop conf headers (Some (lyr layers))
    | `GET, ["shutdown"] ->
      shutdown conf headers
    | (`GET|`POST), ("export" :: path) ->
      let layer, node = lyr_node_of path in
      (* We must allow both POST and GET for that one since we have an
       * optional body (and some client won't send a body with a GET) *)
      export conf headers layer node body
    | `GET, ("plot" :: path) ->
      let layer, node = lyr_node_of path in
      plot conf headers layer node params
    (* Grafana datasource plugin *)
    | `GET, ["grafana"] ->
      respond_ok ()
    | `POST, ["complete"; "nodes"] ->
      complete_nodes conf headers body
    | `POST, ["complete"; "fields"] ->
      complete_fields conf headers body
    | `POST, ["timeseries"] ->
      timeseries conf headers body
    | `GET, ("timerange" :: path) ->
      let layer, node = lyr_node_of path in
      get_timerange conf headers layer node
    | `OPTIONS, _ ->
      let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
      let headers =
        Header.add headers "Access-Control-Allow-Methods" "POST" in
      let headers =
        Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
      Server.respond_string ~status:(`Code 200) ~headers ~body:"" ()
    (* Uploads of data files *)
    | (`POST|`PUT), ("upload" :: path) ->
      let layer, node = lyr_node_of path in
      upload conf headers layer node body
    (* Alerter API *)
    | `GET, ["notify"] ->
      RamenAlerter.Api.notify conf params
    | `GET, ["teams"] ->
      RamenAlerter.Api.get_teams conf
    | `PUT, ["team"; name] ->
      RamenAlerter.Api.new_team conf headers name
    | `DELETE, ["team"; name] ->
      RamenAlerter.Api.del_team conf headers name
    | `GET, ["oncaller"; name] ->
      RamenAlerter.Api.get_oncaller conf headers name
    | `GET, ["ongoing"] ->
      RamenAlerter.Api.get_ongoing conf None
    | `GET, ["ongoing"; team] ->
      RamenAlerter.Api.get_ongoing conf (Some team)
    | (`POST|`PUT), ["history"] ->
      RamenAlerter.Api.get_history_post conf headers body
    | `GET, ("history" :: team) ->
      let team = if team = [] then None else Some (List.hd team) in
      RamenAlerter.Api.get_history_get conf headers team params
    | `GET, ["ack" ; id] ->
      let%lwt id = alert_id_of_string id in
      RamenAlerter.Api.get_ack conf id
    | `GET, ["extinguish" ; id] ->
      let%lwt id = alert_id_of_string id in
      RamenAlerter.Api.get_stop conf id
    | `POST, ["inhibit"; "add"; team] ->
      RamenAlerter.Api.add_inhibit conf headers team body
    | `POST, ["inhibit"; "edit" ; team] ->
      RamenAlerter.Api.edit_inhibit conf headers team body
    | `GET, ["stfu" ; team] ->
      RamenAlerter.Api.stfu conf headers team
    | `POST, ["oncaller"; name] ->
      RamenAlerter.Api.edit_oncaller conf headers name body
    | `POST, ["members"; name] ->
      RamenAlerter.Api.edit_members conf headers name body
    (* Web UI *)
    | `GET, ([]|["index.html"]) ->
      if www_dir = "" then
        serve_string conf headers RamenGui.without_link
      else
        serve_string conf headers RamenGui.with_links
    | `GET, [ "style.css" | "ramen_script.js" as file ] ->
      serve_file_with_replacements conf headers www_dir file
    (* Errors *)
    | `PUT, p | `GET, p | `DELETE, p ->
      let path = String.join "/" p in
      fail (HttpError (404, "Unknown resource "^ path))
    | _ ->
      fail (HttpError (405, "Method not implemented"))
  in
  if daemonize then do_daemonize () ;
  Lwt_main.run (http_service port cert_opt key_opt router)
