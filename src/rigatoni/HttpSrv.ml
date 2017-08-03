(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open Batteries
open BatOption.Infix
open Cohttp
open Cohttp_lwt_unix
open Lwt
open Log
open RamenSharedTypes
module C = RamenConf

exception HttpError of (int * string)

let not_implemented msg = fail (HttpError (501, msg))
let bad_request msg = fail (HttpError (400, msg))

let get_content_type headers =
  Header.get headers "Content-Type" |? Consts.json_content_type |> String.lowercase

let get_accept headers =
  Header.get headers "Accept" |? Consts.json_content_type |> String.lowercase

let is_accepting_anything s =
  String.starts_with s "*/*"

let is_accepting content_type accept =
  is_accepting_anything accept || String.starts_with accept content_type

(* When the client cannot accept the response *)
let cant_accept accept =
  let status = Code.status_of_code 406 in
  let body = "{\"error\": \"Can't produce "^ accept ^"\"}\n" in
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
    try PPP.of_string_exc ppp body |> Lwt.return
    with e ->
      !logger.info "%s: Cannot parse received body: %S, Exception %s"
        what body (Printexc.to_string e) ;
      fail e
  )

(*
== Add/Update/Delete a node ==

Nodes are referred to via name that can be anything as long as they are unique.
So the client decide on the name. The server ensure uniqueness by forbidding
creation of a new node by the same name as one that exists already. Actually,
such a request would result in altering the existing node.

So each node has a URL, such as: node/$name
We can then PUT, GET or DELETE that URL.

For RPC like messages, the server accept all encodings supported by PPP. But we
have to find a way to generate several ppp for a type, under different names.
Or even better, we should have a single value (t_ppp) with all the encodings
(so that we can pass around the ppp for all encodings). So the @@ppp notation
would just create a record with one field per implemented format. Later, if
some are expensive, we could have an option to list only those wanted ; the
default being to include them all. For now we will use only JSON.

Here is the node description. Typically, optional fields are optional or even
forbidden when creating the node and are set when getting node information.
*)

let ok_body = "{\"success\": true}"
let respond_ok ?(body=ok_body) () =
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.json_content_type in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let body = body ^"\n" in
  Server.respond_string ~status ~headers ~body ()

let put_node conf headers name body =
  (* Get the message from the body *)
  let%lwt msg = of_json headers ("Creating node "^ name) make_node_ppp body in
  (match C.find_node conf conf.C.building_graph name with
  | exception Not_found ->
    (match C.make_node conf.C.building_graph name msg.operation with
    | exception e ->
      bad_request ("Node "^ name ^": "^ Printexc.to_string e)
    | node ->
      Lwt.return (C.add_node conf conf.C.building_graph node))
  | node ->
    (match C.update_node conf.C.building_graph node msg.operation with
    | exception e ->
      bad_request ("Node "^ name ^": "^ Printexc.to_string e)
    | () -> Lwt.return_unit)
    ) >>= fun () ->
  respond_ok ()

let type_of_operation_of =
  let open Lang.Operation in
  function
  | Yield _ -> "YIELD"
  | Select _ -> "SELECT"
  | Aggregate _ -> "GROUP BY"
  | ReadCSVFile _ -> "READ CSV"

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
    name = node.C.name ;
    operation = node.C.op_text ;
    type_of_operation = Some (type_of_operation_of node.C.operation) ;
    command = node.C.command ;
    pid = node.C.pid ;
    parents = List.map (fun n -> n.C.name) node.C.parents ;
    children = List.map (fun n -> n.C.name) node.C.children ;
    input_type = C.list_of_temp_tup_type node.C.in_type |> to_expr_type_info ;
    output_type = C.list_of_temp_tup_type node.C.out_type |> to_expr_type_info ;
    in_tuple_count = find_int_metric node.C.last_report Consts.in_tuple_count_metric ;
    selected_tuple_count = find_int_metric node.C.last_report Consts.selected_tuple_count_metric ;
    out_tuple_count = find_int_metric node.C.last_report Consts.out_tuple_count_metric ;
    group_count = find_int_opt_metric node.C.last_report Consts.group_count_metric ;
    cpu_time = find_float_metric node.C.last_report Consts.cpu_time_metric ;
    ram_usage = find_int_metric node.C.last_report Consts.ram_usage_metric }

let get_node conf _headers name =
  match C.find_node conf conf.C.building_graph name with
  | exception Not_found ->
    fail (HttpError (404, "No such node"))
  | node ->
    let node_info = node_info_of_node node in
    let body = PPP.to_string Node.info_ppp node_info in
    respond_ok ~body ()

let del_node conf _headers name =
  match C.remove_node conf conf.C.building_graph name with
  | exception Not_found ->
    fail (HttpError (404, "No such node"))
  | () ->
    respond_ok ()

(*
== Connect nodes ==

We need to build connections between nodes. That's when type-checking happens.
Each link has a resource at /link/$node_src/$node_dest. Creating this resource
(PUT) will add this connection and deleting it will remove the connection.

GET will return some info on that connection (although for now we have not much
to say.
*)

let node_of_name conf graph n =
  match C.find_node conf graph n with
  | exception Not_found ->
    bad_request ("Node "^ n ^" does not exist")
  | node -> return node

let put_link conf _headers src dst =
  let%lwt src = node_of_name conf conf.C.building_graph src in
  let%lwt dst = node_of_name conf conf.C.building_graph dst in
  !logger.debug "Adding link from %s to %s" src.C.name dst.C.name ;
  if C.has_link conf src dst then
    let msg =
      "Creating link "^ src.C.name ^"-"^ dst.C.name ^": Link already exists" in
    bad_request msg
  else (
    C.make_link conf conf.C.building_graph src dst ;
    respond_ok ())

let del_link conf _headers src dst =
  let%lwt src = node_of_name conf conf.C.building_graph src in
  let%lwt dst = node_of_name conf conf.C.building_graph dst in
  if not (C.has_link conf src dst) then
    bad_request ("That link does not exist")
  else (
    C.remove_link conf conf.C.building_graph src dst ;
    respond_ok ())

let get_link conf _headers src dst =
  let%lwt src = node_of_name conf conf.C.building_graph src in
  let%lwt dst = node_of_name conf conf.C.building_graph dst in
  if not (C.has_link conf src dst) then
    bad_request ("That link does not exist")
  else
    respond_ok ()

(*
== Set all connections of a single node ==

Allows the node editor to set all connections at once.
*)

let diff_list bef aft =
  (* Remove an element from a list or return the original list if the
   * element was not present: *)
  let filter_out x lst =
    let rec loop prev = function
    | [] -> lst
    | e::rest ->
      if e == x then List.rev_append rest prev
      else loop (e::prev) rest in
    loop [] lst
  in
  (* Loop over aft, building to_add and to_del: *)
  let rec loop to_add to_del bef = function
  | [] -> to_add, List.rev_append bef to_del
  | a::rest ->
    let filtered = filter_out a bef in
    if filtered == bef then
      loop (a::to_add) to_del filtered rest
    else
      loop to_add to_del filtered rest
  in
  loop [] [] bef aft

let set_links conf headers name body =
  let%lwt node_links =
    of_json headers ("Set links for node "^ name) node_links_ppp body in
  let graph = conf.C.building_graph in
  let%lwt node = node_of_name conf graph name in
  let%lwt parents = Lwt_list.map_s (node_of_name conf graph) node_links.parents in
  let%lwt children = Lwt_list.map_s (node_of_name conf graph) node_links.children in
  let to_add, to_del = diff_list node.C.parents parents in
  List.iter (fun p -> C.remove_link conf graph p node) to_del ;
  List.iter (fun p -> C.make_link conf graph p node) to_add ;
  let to_add, to_del = diff_list node.C.children children in
  List.iter (fun c -> C.remove_link conf graph node c) to_del ;
  List.iter (fun c -> C.make_link conf graph node c) to_add ;
  respond_ok ()

(*
== Display the graph (JSON or SVG representation) ==
*)

let get_graph_json conf _headers =
  let graph_info =
    { nodes = Hashtbl.fold (fun _name node lst ->
        node_info_of_node node :: lst
      ) conf.C.building_graph.C.persist.C.nodes [] ;
      status = conf.C.building_graph.C.persist.C.status ;
      last_started = conf.C.building_graph.C.last_started ;
      last_stopped = conf.C.building_graph.C.last_stopped } in
  let body = PPP.to_string graph_info_ppp graph_info in
  respond_ok ~body ()

let dot_of_graph graph =
  let dot = IO.output_string () in
  Printf.fprintf dot "digraph rigatoni {\n" ;
  Hashtbl.keys graph.C.persist.C.nodes |>
    Enum.iter (Printf.fprintf dot "\t%S\n") ;
  Printf.fprintf dot "\n" ;
  Hashtbl.iter (fun name node ->
      List.iter (fun c ->
          Printf.fprintf dot "\t%S -> %S\n" name c.C.name
        ) node.C.children
    ) graph.C.persist.C.nodes ;
  Printf.fprintf dot "}\n" ;
  IO.close_out dot

let get_graph_dot conf _headers =
  let body = dot_of_graph conf.C.building_graph in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.dot_content_type in
  Server.respond_string ~headers ~status ~body ()

let mermaid_of_graph graph =
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
  Hashtbl.keys graph.C.persist.C.nodes |>
    Enum.iter (fun n ->
      Printf.fprintf txt "%s(%s)\n"
        (mermaid_id n)
        (mermaid_label n)) ;
  Printf.fprintf txt "\n" ;
  Hashtbl.iter (fun name node ->
      List.iter (fun c ->
          Printf.fprintf txt "\t%s-->%s\n"
            (mermaid_id name)
            (mermaid_id c.C.name)
        ) node.C.children
    ) graph.C.persist.C.nodes ;
  IO.close_out txt

let get_graph_mermaid conf _headers =
  let body = mermaid_of_graph conf.C.building_graph in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.mermaid_content_type in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let headers = Header.add headers "Access-Control-Allow-Methods" "POST" in
  let headers = Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
  Server.respond_string ~headers ~status ~body ()

let get_graph conf headers =
  let accept = get_accept headers in
  if is_accepting Consts.json_content_type accept then
    get_graph_json conf headers
  else if is_accepting Consts.dot_content_type accept then
    get_graph_dot conf headers
  else if is_accepting Consts.mermaid_content_type accept then
    get_graph_mermaid conf headers
  else
    cant_accept accept

let put_graph conf headers body =
  let%lwt msg = of_json headers "Uploading graph" graph_info_ppp body in
  let graph = C.make_graph () in
  (* First create all the nodes *)
  List.iter (fun info ->
      let n = C.make_node graph info.Node.name info.Node.operation in
      C.add_node conf graph n
    ) msg.nodes ;
  (* Then all the links *)
  let%lwt () = Lwt_list.iter_s (fun info ->
      let%lwt src = node_of_name conf graph info.Node.name in
      Lwt_list.iter_s (fun child ->
          let%lwt dst = node_of_name conf graph child in
          C.make_link conf graph src dst ;
          return_unit
        ) info.Node.children
    ) msg.nodes in
  (* Then make this graph the new one (TODO: support for multiple graphs) *)
  conf.C.building_graph <- graph ;
  respond_ok ()

(*
== Whole graph operations: compile/run/stop ==
*)

let compile conf headers =
  check_accept headers Consts.json_content_type (fun () ->
    match Compiler.compile conf conf.C.building_graph with
    | exception (Lang.SyntaxError e | C.InvalidCommand e) ->
      bad_request e
    | () ->
      respond_ok ())

let run conf headers =
  check_accept headers Consts.json_content_type (fun () ->
    match RamenProcesses.run conf conf.C.building_graph with
    | exception (Lang.SyntaxError e | C.InvalidCommand e) ->
      bad_request e
    | () ->
      respond_ok ())

let stop conf headers =
  check_accept headers Consts.json_content_type (fun () ->
    match RamenProcesses.stop conf conf.C.building_graph with
    | exception C.InvalidCommand e ->
      bad_request e
    | () ->
      respond_ok ())

(*
== Exporting tuples ==

Clients can request to be sent the tuples from an exporting node.
*)

let export conf headers node_name body =
  check_accept headers Consts.json_content_type (fun () ->
    let%lwt req =
      if body = "" then return empty_export_req else
      of_json headers ("Exporting from "^ node_name) export_req_ppp body in
    (* Check that the node exists and exports *)
    match C.find_node conf conf.C.building_graph node_name with
    | exception Not_found ->
      bad_request ("Unknown node "^ node_name)
    | node ->
      if not (Lang.Operation.is_exporting node.C.operation) then
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
== Serving normal files ==
*)

let ext_of_file fname =
  let _, ext = String.rsplit fname ~by:"." in ext

let content_type_of_ext = function
  | "html" -> Consts.html_content_type
  | "js" -> Consts.js_content_type
  | "css" -> Consts.css_content_type
  | _ -> "I_dont_know/Good_luck"

let get_file _conf _headers file =
  let fname = "www/"^ file in
  let headers =
    Header.init_with "Content-Type" (content_type_of_ext (ext_of_file file)) in
  Server.respond_file ~headers ~fname ()

(*
== Children health and report ==
*)

let report conf _headers name body =
  (* TODO: check application-type is marshaled.ocaml *)
  let last_report = Marshal.from_string body 0 in
  match C.find_node conf conf.C.building_graph name with
  | exception Not_found ->
    bad_request ("Node "^ name ^" does not exist")
  | node ->
    node.C.last_report <- last_report ;
    respond_ok ()

(*
== Grafana Datasource: autocompletion of node/field names ==
*)

type complete_node_req = { node_prefix : string } [@@ppp PPP_JSON] [@@ppp_extensible]
type complete_field_req = { node : string ; field_prefix : string } [@@ppp PPP_JSON] [@@ppp_extensible]
type complete_resp = string list [@@ppp PPP_JSON]

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
== Grafana Datasource: data queries ==
*)


type timeserie_req =
  { id : string ;
    node : string ;
    data_field : string ;
    consolidation : string [@ppp_default "avg"] } [@@ppp PPP_JSON] [@@ppp_extensible]

type timeseries_req =
  { from : float ; (* from and to_ are in milliseconds *)
    to_ : float [@ppp_rename "to"] ;
    interval_ms : float ;
    max_data_points : int ; (* FIXME: should be optional *)
    timeseries : timeserie_req list } [@@ppp PPP_JSON] [@@ppp_extensible]

type timeserie_resp =
  { id : string ;
    times : float array ;
    values : float option array } [@@ppp PPP_JSON]

type timeseries_resp = timeserie_resp list [@@ppp PPP_JSON]

type timeserie_bucket =
  (* Hopefully count will be small enough that sum can be tracked accurately *)
  { mutable count : int ; mutable sum : float ;
    mutable min : float ; mutable max : float }

let add_into_bucket b i v =
  if i > 0 && i < Array.length b then (
    b.(i).count <- succ b.(i).count ;
    b.(i).min <- min b.(i).min v ;
    b.(i).max <- max b.(i).max v ;
    b.(i).sum <- b.(i).sum +. v)

let bucket_avg b =
  if b.count = 0 then None else Some (b.sum /. float_of_int b.count)
let bucket_min b =
  if b.count = 0 then None else Some b.min
let bucket_max b =
  if b.count = 0 then None else Some b.max

let timeseries conf headers body =
  let open Lang.Operation in
  let%lwt msg = of_json headers "timeseries query" timeseries_req_ppp body in
  let ts_of_node (req : timeserie_req) =
    match C.find_node conf conf.C.building_graph req.node with
    | exception Not_found ->
      raise (Failure ("Unknown node "^ req.node))
    | node ->
      if not (is_exporting node.C.operation) then
        raise (Failure ("node "^ req.node ^" does not export data"))
      else match export_event_info node.C.operation with
      | None ->
        raise (Failure ("node "^ req.node ^" does not specify event time info"))
      | Some ((start_field, start_scale), duration_info) ->
        let consolidation =
          match String.lowercase req.consolidation with
          | "min" -> bucket_min | "max" -> bucket_max | _ -> bucket_avg in
        let history = RamenExport.get_history node in
        let find_field n =
          try (
            List.findi (fun _i (ft : field_typ) ->
              ft.typ_name = n) history.RamenExport.tuple_type |> fst
          ) with Not_found ->
            raise (Failure ("field "^ n ^" does not exist")) in
        let ti = find_field start_field
        and vi = find_field req.data_field in
        if msg.max_data_points < 1 then raise (Failure "invalid max_data_points") ;
        let dt = (msg.to_ -. msg.from) /. float_of_int msg.max_data_points in
        let buckets = Array.init msg.max_data_points (fun _ ->
          { count = 0 ; sum = 0. ; min = max_float ; max = min_float }) in
        let bucket_of_time t = int_of_float ((t -. msg.from) /. dt) in
        let _ =
          RamenExport.fold_tuples history () (fun tup () ->
            let t, v = RamenExport.float_of_scalar_value tup.(ti),
                       RamenExport.float_of_scalar_value tup.(vi) in
            let t1 = t *. start_scale in
            let t2 =
              match duration_info with
              | DurationConst f -> t1 +. f
              | DurationField (f, s) ->
                let fi = find_field f in
                t1 +. RamenExport.float_of_scalar_value tup.(fi) *. s
              | StopField (f, s) ->
                let fi = find_field f in
                RamenExport.float_of_scalar_value tup.(fi) *. s
            in
            (* We allow duration to be < 0 *)
            let t1, t2 = if t2 >= t1 then t1, t2 else t2, t1 in
            (* t1 and t2 are in secs. But the API is in milliseconds (thanks to
             * grafana) *)
            let t1, t2 = t1 *. 1000., t2 *. 1000. in
            if t1 < msg.to_ && t2 >= msg.from then
              let bi1 = bucket_of_time t1 and bi2 = bucket_of_time t2 in
              for bi = bi1 to bi2 do
                add_into_bucket buckets bi v
              done) in
        Array.mapi (fun i _ ->
          msg.from +. dt *. (float_of_int i +. 0.5)) buckets,
        Array.map consolidation buckets
  in
  match List.map (fun req ->
          let times, values = ts_of_node req in
          { id = req.id ; times ; values }) msg.timeseries with
  | exception Failure err -> bad_request err
  | resp ->
    let body = PPP.to_string timeseries_resp_ppp resp in
    respond_ok ~body ()

(* let fake_series from to_ (req : timeserie_req) =
    let nb_pts = 100 in
    let time_of_i i =
      from +. float_of_int i *. (to_ -. from) /. float_of_int nb_pts in
    let times = Array.init nb_pts time_of_i in
    let values = Array.init nb_pts (fun i ->
                    let t = times.(i) in sin(t)) in
    { id = req.id ; times ; values }
  in
  let body =
    List.map (fake_series msg.from msg.to_) msg.timeseries |>
    PPP.to_string timeseries_resp_ppp in
  respond_ok ~body () *)


(* The function called for each HTTP request: *)

let callback conf _conn req body =
  (* What is this about? *)
  let uri = Request.uri req in
  let paths =
    String.nsplit (Uri.path uri) "/" |>
    List.filter (fun s -> String.length s > 0) in
  let headers = Request.headers req in
  let%lwt body = Cohttp_lwt_body.to_string body
  in
  catch
    (fun () ->
      let dec = Uri.pct_decode in
      try
        match Request.meth req, paths with
        (* API *)
        | `PUT, ["node" ; name] -> put_node conf headers (dec name) body
        | `GET, ["node" ; name] -> get_node conf headers (dec name)
        | `DELETE, ["node" ; name] -> del_node conf headers (dec name)
        | _, ["node"] -> bad_request "Missing node name"
        | `PUT, ["link" ; src ; dst] -> put_link conf headers (dec src) (dec dst)
        | `GET, ["link" ; src ; dst] -> get_link conf headers (dec src) (dec dst)
        | `DELETE, ["link" ; src ; dst] -> del_link conf headers (dec src) (dec dst)
        | _, (["link"] | ["link" ; _ ]) -> bad_request "Missing node name"
        | `PUT, ["links" ; name] -> set_links conf headers (dec name) body
        | `PUT, ["links"] -> bad_request "Missing node name"
        | `GET, ["graph"] -> get_graph conf headers
        | `PUT, ["graph"] -> put_graph conf headers body
        | `GET, ["compile"] -> compile conf headers
        | `GET, ["run" | "start"] -> run conf headers
        | `GET, ["stop"] -> stop conf headers
        | (`GET|`POST), ["export" ; name] ->
          (* We must allow both POST and GET for that one since we have an optional
           * body (and some client won't send a body with a GET) *)
          export conf headers (dec name) body
        (* API for children *)
        | `PUT, ["report" ; name] -> report conf headers (dec name) body
        (* WWW Client *)
        | `GET, ([] | ["" | "index.html"]) ->
          get_file conf headers "index.html"
        | `GET, ["static"; "style.css"|"misc.js"|"graph_layout.js"
                |"node_edit.js" as file] ->
          get_file conf headers file
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
      with exn ->
        Helpers.print_exception exn ;
        fail exn)
    (function
      | HttpError (code, body) ->
        let body = body ^ "\n" in
        let status = Code.status_of_code code in
        let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
        Server.respond_error ~headers ~status ~body ()
      | exn ->
        let body = Printexc.to_string exn ^ "\n" in
        let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
        Server.respond_error ~headers ~body ())

let start debug save_file ramen_url port cert_opt key_opt () =
  logger := make_logger debug ;
  let conf = C.make_conf save_file ramen_url in
  let entry_point = Server.make ~callback:(callback conf) () in
  let tcp_mode = `TCP (`Port port) in
  let on_exn = Helpers.print_exception in
  let t1 =
    let%lwt () = return (!logger.info "Starting http server on port %d" port) in
    Server.create ~on_exn ~mode:tcp_mode entry_point
  and t2 =
    match cert_opt, key_opt with
    | Some cert, Some key ->
      let port = port + 1 in
      let ssl_mode = `TLS (`Crt_file_path cert, `Key_file_path key, `No_password, `Port port) in
      let%lwt () = return (!logger.info "Starting https server on port %d" port) in
      Server.create ~on_exn ~mode:ssl_mode entry_point
    | None, None ->
      return (!logger.info "Not starting https server")
    | _ ->
      return (!logger.info "Missing some of SSL configuration")
  in
  Lwt_main.run (join [ t1 ; t2 ])
