(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open Batteries
open BatOption.Infix
open Cohttp
open Cohttp_lwt_unix
open Lwt
open RamenLog
open RamenSharedTypes
open Helpers
module C = RamenConf
module N = RamenConf.Node
module L = RamenConf.Layer
module SL = RamenSharedTypes.Layer
module SN = RamenSharedTypes.Node

let not_implemented msg = fail (HttpError (501, msg))
let bad_request msg = fail (HttpError (400, msg))

let get_content_type headers =
  Header.get headers "Content-Type" |? Consts.json_content_type |> String.lowercase

let get_accept headers =
  let h =
    Header.get headers "Accept" |? Consts.json_content_type |>
    String.lowercase in
  let h =
    try String.split ~by:";" h |> fst
    with Not_found -> h in
  String.split_on_char ',' h

let is_accepting_anything = List.mem "*/*"

let is_accepting content_type accept =
  is_accepting_anything accept || List.mem content_type accept

(* When the client cannot accept the response *)
let cant_accept accept =
  let status = Code.status_of_code 406 in
  let body =
    Printf.sprintf "{\"error\": \"Can't produce any of %s\"}\n"
      (IO.to_string (List.print ~first:"" ~last:"" ~sep:", " String.print)
                    accept) in
  Server.respond_error ~status ~body ()

let check_accept headers content_type f =
  let accept = get_accept headers in
  if not (is_accepting content_type accept) then
    cant_accept accept
  else f ()

(* Helper to deserialize an incoming json *)
let of_json headers what ppp body =
  if get_content_type headers <> Consts.json_content_type then
    bad_request "Bad content type"
  else (
    try PPP.of_string_exc ppp body |> return
    with e ->
      !logger.info "%s: Cannot parse received body: %S, Exception %s"
        what body (Printexc.to_string e) ;
      fail e
  )

let ok_body = "{\"success\": true}"
let respond_ok ?(body=ok_body) () =
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.json_content_type in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let body = body ^"\n" in
  Server.respond_string ~status ~headers ~body ()

let type_of_operation_of =
  let open Lang.Operation in
  function
  | Yield _ -> "YIELD"
  | Aggregate _ -> "GROUP BY"
  | ReadCSVFile _ -> "READ CSV"

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
      | None -> fail_with ("node "^ s ^" does not exist")

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
  let to_expr_type_info lst =
    List.map (fun (rank, typ) -> rank, Lang.Expr.to_expr_type_info typ) lst
  in
  Node.{
    definition = {
      name = node.N.name ;
      operation = node.N.op_text ;
      parents = List.map (fun n -> n.N.name) node.N.parents } ;
    type_of_operation = Some (type_of_operation_of node.N.operation) ;
    signature = if node.N.signature = "" then None else Some node.N.signature ;
    pid = node.N.pid ;
    input_type = C.list_of_temp_tup_type node.N.in_type |> to_expr_type_info ;
    output_type = C.list_of_temp_tup_type node.N.out_type |> to_expr_type_info ;
    in_tuple_count = find_int_metric node.N.last_report Consts.in_tuple_count_metric ;
    selected_tuple_count = find_int_metric node.N.last_report Consts.selected_tuple_count_metric ;
    out_tuple_count = find_int_metric node.N.last_report Consts.out_tuple_count_metric ;
    group_count = find_int_opt_metric node.N.last_report Consts.group_count_metric ;
    cpu_time = find_float_metric node.N.last_report Consts.cpu_time_metric ;
    ram_usage = find_int_metric node.N.last_report Consts.ram_usage_metric }

let layer_info_of_layer layer =
  SL.{
    name = layer.L.name ;
    nodes = Hashtbl.values layer.L.persist.L.nodes /@
            node_info_of_node |>
            List.of_enum ;
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
  let body = List.map layer_info_of_layer layers |>
             PPP.to_string get_graph_resp_ppp in
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
  let%lwt layers = graph_layers conf layer_opt in
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
  try C.find_node conf layer_name node_name |> return
  with Not_found ->
    fail_with ("Node "^ layer_name ^"/"^ node_name ^" does not exist")

let node_of_name conf layer_name node_name =
  if node_name = "" then fail_with "Empty string is not a valid node name"
  else find_node_or_fail conf layer_name node_name

let put_layer conf headers body =
  let%lwt msg = of_json headers "Uploading layer" put_layer_req_ppp body in
  (* TODO: Check that this layer node names are unique within the layer *)

  (* Check that this layer is new *)
  if Hashtbl.mem conf.C.graph.C.layers msg.name then
    bad_request ("Layer "^ msg.name ^" already present")
  else (
    (* Create all the nodes *)
    let%lwt () = Lwt_list.iter_s (fun def ->
        let name =
          if def.Node.name <> "" then def.Node.name
          else N.make_name () in
        wrap (fun () -> C.add_node conf name msg.name def.Node.operation)
      ) msg.nodes in
    (* Then all the links *)
    let%lwt () = Lwt_list.iter_s (fun def ->
        let%lwt _layer, dst = node_of_name conf msg.name def.Node.name in
        Lwt_list.iter_s (fun p ->
            let%lwt parent_layer, parent_name =
              layer_node_of_user_string conf ~default_layer:msg.name p in
            let%lwt _layer, src = node_of_name conf parent_layer parent_name in
            wrap (fun () -> C.add_link conf src dst)
          ) def.SN.parents
      ) msg.nodes in
    respond_ok ())
  (* TODO: why wait before compiling this layer? *)

(*
    Whole graph operations: compile/run/stop
*)

let compile conf headers layer_opt =
  check_accept headers Consts.json_content_type (fun () ->
    let%lwt layers = graph_layers conf layer_opt in
    catch
      (fun () ->
        let rec loop left_try layers =
          !logger.debug "%d layers left to compile..." (List.length layers) ;
          if layers = [] then respond_ok () else
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
        loop (List.length layers) layers)
      (function Lang.SyntaxError e | C.InvalidCommand e -> bad_request e
              | e -> fail e))

let run conf headers layer_opt =
  check_accept headers Consts.json_content_type (fun () ->
    let%lwt layers = graph_layers conf layer_opt in
    let layers = L.order layers in
    try
      List.iter (fun layer ->
          let open RamenProcesses in
          try run conf layer with AlreadyRunning -> ()
        ) layers ;
      respond_ok ()
    with (Lang.SyntaxError e | C.InvalidCommand e) ->
      bad_request e)

let stop conf headers layer_opt =
  check_accept headers Consts.json_content_type (fun () ->
    let%lwt layers = graph_layers conf layer_opt in
    try
      List.iter (fun layer ->
          let open RamenProcesses in
          try stop conf layer with NotRunning -> ()
        ) layers ;
      respond_ok ()
    with C.InvalidCommand e -> bad_request e)

(*
    Exporting tuples

    Clients can request to be sent the tuples from an exporting node.
*)

let export conf headers layer_name node_name body =
  check_accept headers Consts.json_content_type (fun () ->
    let%lwt req =
      if body = "" then return empty_export_req else
      of_json headers ("Exporting from "^ node_name) export_req_ppp body in
    (* Check that the node exists and exports *)
    let%lwt layer, node = find_node_or_fail conf layer_name node_name in
    if not (L.is_typed layer) then
      bad_request ("node "^ node_name ^" is not typed (yet)")
    else if not (Lang.Operation.is_exporting node.N.operation) then
      bad_request ("node "^ node_name ^" does not export data")
    else (
      let start = Unix.gettimeofday () in
      let rec loop () =
        let history = RamenExport.get_history node in
        let fields = RamenExport.get_field_types history in
        let first, values =
          RamenExport.fold_tuples ?since:req.since ?max_res:req.max_results
                                  history [] List.cons in
        let dt = Unix.gettimeofday () -. start in
        if values = [] && dt < (req.wait_up_to |? 0.) then (
          (* TODO: sleep for dt, queue the wakener on this history,
           * and wake all the sleeps when a tuple is received *)
          Lwt_unix.sleep 0.1 >>= loop
        ) else (
          (* Store it in column to save variant types: *)
          let resp =
            { first ; columns = RamenExport.columns_of_tuples fields values } in
          let body = PPP.to_string export_resp_ppp resp in
          respond_ok ~body ()
        ) in
      loop ()))

(*
    API for Workers: health and report
*)

let report conf _headers layer name body =
  (* TODO: check application-type is marshaled.ocaml *)
  let last_report = Marshal.from_string body 0 in
  let%lwt _layer, node = find_node_or_fail conf layer name in
  node.N.last_report <- last_report ;
  respond_ok ()

(*
    Grafana Datasource: autocompletion of node/field names
*)

let complete_nodes conf headers body =
  let%lwt msg = of_json headers "Complete tables" complete_node_req_ppp body in
  let body =
    C.complete_node_name conf msg.node_prefix |>
    PPP.to_string complete_resp_ppp
  in
  respond_ok ~body ()

let complete_fields conf headers body =
  let%lwt msg = of_json headers "Complete fields" complete_field_req_ppp body in
  let body =
    C.complete_field_name conf msg.node msg.field_prefix |>
    PPP.to_string complete_resp_ppp
  in
  respond_ok ~body ()

(*
    Grafana Datasource: data queries
*)

let timeseries conf headers body =
  let open Lang.Operation in
  let%lwt msg = of_json headers "time series query" timeseries_req_ppp body in
  let ts_of_node_field req layer node data_field =
    let%lwt _layer, node = find_node_or_fail conf layer node in
    if not (is_exporting node.N.operation) then
      fail_with ("node "^ node.N.name ^" does not export data")
    else match export_event_info node.N.operation with
    | None ->
      fail_with ("node "^ node.N.name ^" does not specify event time info")
    | Some ((start_field, start_scale), duration_info) ->
      let open RamenExport in
      let consolidation =
        match String.lowercase req.consolidation with
        | "min" -> bucket_min | "max" -> bucket_max | _ -> bucket_avg in
      wrap (fun () ->
        build_timeseries
          node start_field start_scale data_field duration_info
          msg.max_data_points msg.from msg.to_ consolidation)
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
        let open Lang.Operation in
        match parent.N.operation with
        | Aggregate { export = Some (Some ((start, scale), DurationConst dur)) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s AS data \
             EXPORT EVENT STARTING AT %s * %g WITH DURATION %g"
            start select_y
            start scale dur |> return
        | Aggregate { export = Some (Some ((start, scale), DurationField (dur, scale2))) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s AS data \
             EXPORT EVENT STARTING AT %s * %g WITH DURATION %s * %g"
            start select_y
            start scale dur scale2 |> return
        | Aggregate { export = Some (Some ((start, scale), StopField (stop, scale2))) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s, %s AS data \
             EXPORT EVENT STARTING AT %s * %g AND STOPPING AT %s * %g"
            start stop select_y
            start scale stop scale2 |> return
        | _ ->
          fail_with "This parent does not provide time information"
      ) else return (
        "SELECT "^ select_x ^" AS time, "
                 ^ select_y ^" AS data \
         EXPORT EVENT STARTING AT time") in
    let op_text =
      if where = "" then op_text else op_text ^" WHERE "^ where in
    let%lwt operation = wrap (fun () -> C.parse_operation op_text) in
    let reformatted_op = IO.to_string Lang.Operation.print operation in
    let layer_name =
      "temp/from_"^ from ^"_"^ Cryptohash_md4.(string reformatted_op |> to_hex)
    and node_name = "operation" in
    (* So far so good. In all likelihood this layer exists already: *)
    (if Hashtbl.mem conf.C.graph.C.layers layer_name then (
      !logger.debug "Layer %S already there" layer_name ;
      return_unit
    ) else (
      (* Add this layer to the running configuration: *)
      let layer =
        C.add_parsed_node ~timeout:300.
          conf node_name layer_name op_text operation in
      let node = Hashtbl.find layer.L.persist.L.nodes node_name in
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
              create_temporary_node select_x select_y from where in
          let%lwt _layer, node = find_node_or_fail conf layer_name node_name in
          RamenProcesses.use_layer conf (Unix.gettimeofday ()) node.N.layer ;
          let%lwt times, values =
            ts_of_node_field req layer_name node_name data_field in
          return { id = req.id ; times ; values }
        ) msg.timeseries in
      let body = PPP.to_string timeseries_resp_ppp resp in
      respond_ok ~body ())
    (function Failure err -> bad_request err | e -> fail e)

(* A thread that hunt for unused layers *)
let rec timeout_layers conf =
  let%lwt () = wrap (fun () -> RamenProcesses.timeout_layers conf) in
  let%lwt () = Lwt_unix.sleep 7.1 in
  timeout_layers conf

let start do_persist debug ramen_url version_tag node_dir port cert_opt
          key_opt () =
  logger := make_logger debug ;
  let conf = C.make_conf do_persist ramen_url debug version_tag node_dir in
  async (fun () -> timeout_layers conf) ;
  let router meth path _params headers body =
    (* The function called for each HTTP request: *)
      match meth, path with
      (* API *)
      | `GET, ["graph"] -> get_graph conf headers None
      | `GET, ["graph" ; layer] -> get_graph conf headers (Some layer)
      | `PUT, ["graph"] -> put_layer conf headers body
(*      | `DELETE, ["graph" ; layer] -> del_layer conf headers *)
      | `GET, ["compile"] -> compile conf headers None
      | `GET, ["compile" ; layer] -> compile conf headers (Some layer)
      | `GET, ["run" | "start"] -> run conf headers None
      | `GET, ["run" | "start" ; layer] -> run conf headers (Some layer)
      | `GET, ["stop"] -> stop conf headers None
      | `GET, ["stop" ; layer] -> stop conf headers (Some layer)
      | (`GET|`POST), ["export" ; layer ; node] ->
        (* TODO: a variant where we do not have to specify layer *)
        (* We must allow both POST and GET for that one since we have an optional
         * body (and some client won't send a body with a GET) *)
        export conf headers layer node body
      (* API for children *)
      | `PUT, ["report" ; layer ; node] -> report conf headers layer node body
      (* Grafana datasource plugin *)
      | `GET, ["grafana"] -> respond_ok ()
      | `POST, ["complete"; "nodes"] ->
        complete_nodes conf headers body
      | `POST, ["complete"; "fields"] ->
        complete_fields conf headers body
      | `POST, ["timeseries"] ->
        timeseries conf headers body
      | `OPTIONS, _ ->
        let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
        let headers = Header.add headers "Access-Control-Allow-Methods" "POST" in
        let headers = Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
        Server.respond_string ~status:(`Code 200) ~headers ~body:"" ()
      (* Errors *)
      | `PUT, _ | `GET, _ | `DELETE, _ ->
        fail (HttpError (404, "No such resource"))
      | _ ->
        fail (HttpError (405, "Method not implemented"))
  in
  Lwt_main.run (http_service port cert_opt key_opt router)
