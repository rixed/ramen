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

let accept_anything s =
  String.starts_with s "*/*"

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

let ok_body = "{\"success\": true}\n"
let respond_ok ?(body=ok_body) () =
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.json_content_type in
  Server.respond_string ~status ~headers ~body ()

let put_node conf headers name body =
  (* Get the message from the body *)
  if get_content_type headers <> Consts.json_content_type then
    bad_request "Bad content type"
  else match PPP.of_string_exc make_node_ppp body with
  | exception e ->
    !logger.info "Creating node %s: Cannot parse received body: %S, Exception %s"
      name body (Printexc.to_string e) ;
    fail e
  | msg ->
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
  | Alert _ -> "ALERT"
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
  { name = node.C.name ;
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
    let body = PPP.to_string node_info_ppp node_info ^"\n" in
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

let set_links conf _headers name body =
  match PPP.of_string_exc node_links_ppp body with
  | exception e ->
    !logger.info "Set links for node %s: Cannot parse received body: %S, Exception %s"
      name body (Printexc.to_string e) ;
    fail e
  | node_links ->
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
      status = conf.C.building_graph.C.status ;
      last_started = conf.C.building_graph.C.last_started ;
      last_stopped = conf.C.building_graph.C.last_stopped } in
  let body = PPP.to_string graph_info_ppp graph_info ^"\n" in
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

let get_graph conf headers =
  let accept = get_accept headers in
  if accept_anything accept ||
     String.starts_with accept Consts.json_content_type then
    get_graph_json conf headers
  else if String.starts_with accept Consts.dot_content_type then
    get_graph_dot conf headers
  else
    let status = Code.status_of_code 406 in
    Server.respond_error ~status ~body:("{\"error\": \"Can't produce "^ accept ^"\"}\n") ()

let put_graph conf headers body =
  (* Get the message from the body *)
  if get_content_type headers <> Consts.json_content_type then
    bad_request "Bad content type"
  else match PPP.of_string_exc graph_info_ppp body with
  | exception e ->
    !logger.info "Uploading graph: Cannot parse received body: %S, Exception %s"
      body (Printexc.to_string e) ;
    fail e
  | msg ->
    let graph = C.make_graph () in
    (* First create all the nodes *)
    List.iter (fun info ->
        let n = C.make_node graph info.name info.operation in
        C.add_node conf graph n
      ) msg.nodes ;
    (* Then all the links *)
    let%lwt () = Lwt_list.iter_s (fun info ->
        let%lwt src = node_of_name conf graph info.name in
        Lwt_list.iter_s (fun child ->
            let%lwt dst = node_of_name conf graph child in
            C.make_link conf graph src dst ;
            return_unit
          ) info.children
      ) msg.nodes in
    (* Then make this graph the new one (TODO: support for multiple graphs) *)
    conf.C.building_graph <- graph ;
    respond_ok ()

(*
== Whole graph operations: compile/run/stop ==
*)

let compile conf _headers =
  (* TODO: check we accept json *)
  match Compiler.compile conf conf.C.building_graph with
  | exception (Lang.SyntaxError e | C.InvalidCommand e) ->
    bad_request e
  | () ->
    respond_ok ()

let run conf _headers =
  (* TODO: check we accept json *)
  match C.run conf conf.C.building_graph with
  | exception (Lang.SyntaxError e | C.InvalidCommand e) ->
    bad_request e
  | () ->
    respond_ok ()

let stop conf _headers =
  match C.stop conf conf.C.building_graph with
  | exception C.InvalidCommand e ->
    bad_request e
  | () ->
    respond_ok ()

let ext_of_file fname =
  let _, ext = String.rsplit fname ~by:"." in ext

let content_type_of_ext = function
  | "html" -> Consts.html_content_type
  | "js" -> Consts.js_content_type
  | "css" -> Consts.css_content_type
  | _ -> "I_dont_know/Good_luck"

let get_file _conf _headers file =
  let fname = "www/"^ file in
  !logger.debug "Serving file %S" fname ;
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

(* The function called for each HTTP request: *)

let callback conf _conn req body =
  (* What is this about? *)
  let uri = Request.uri req in
  let paths =
    String.nsplit (Uri.path uri) "/" |>
    List.filter (fun s -> String.length s > 0) in
  let headers = Request.headers req in
  let%lwt body_str = Cohttp_lwt_body.to_string body
  in
  catch
    (fun () ->
      let dec = Uri.pct_decode in
      try
        match Request.meth req, paths with
        (* API *)
        | `PUT, ["node" ; name] -> put_node conf headers (dec name) body_str
        | `GET, ["node" ; name] -> get_node conf headers (dec name)
        | `DELETE, ["node" ; name] -> del_node conf headers (dec name)
        | _, ["node"] -> bad_request "Missing node name"
        | `PUT, ["link" ; src ; dst] -> put_link conf headers (dec src) (dec dst)
        | `GET, ["link" ; src ; dst] -> get_link conf headers (dec src) (dec dst)
        | `DELETE, ["link" ; src ; dst] -> del_link conf headers (dec src) (dec dst)
        | _, (["link"] | ["link" ; _ ]) -> bad_request "Missing node name"
        | `PUT, ["links" ; name] -> set_links conf headers (dec name) body_str
        | `PUT, ["links"] -> bad_request "Missing node name"
        | `GET, ["graph"] -> get_graph conf headers
        | `PUT, ["graph"] -> put_graph conf headers body_str
        | `GET, ["compile"] -> compile conf headers
        | `GET, ["run" | "start"] -> run conf headers
        | `GET, ["stop"] -> stop conf headers
        (* API for children *)
        | `PUT, ["report" ; name] -> report conf headers (dec name) body_str
        (* WWW Client *)
        | `GET, ([] | ["" | "index.html"]) ->
          get_file conf headers "index.html"
        | `GET, ["static"; "style.css"|"misc.js"|"graph_layout.js"
                |"node_edit.js" as file] ->
          get_file conf headers file
        (* Errors *)
        | `PUT, _ | `GET, _ | `DELETE, _ ->
          fail (HttpError (404, "No such resource"))
        | _ ->
          fail (HttpError (405, "Method not implemented"))
      with HttpError _ as exn -> fail exn
         | exn ->
          !logger.error "Exception: %s at\n%s"
            (Printexc.to_string exn)
            (Printexc.get_backtrace ()) ;
          fail exn)
    (function
      | HttpError (code, body) ->
        let body = body ^ "\n" in
        let status = Code.status_of_code code in
        Server.respond_error ~status ~body ()
      | exn ->
        let body = Printexc.to_string exn ^ "\n" in
        Server.respond_error ~body ())

(* This will be called as a separate Lwt thread: *)
let start conf port cert_opt key_opt =
  let entry_point = Server.make ~callback:(callback conf) () in
  let tcp_mode = `TCP (`Port port) in
  let t1 =
    let%lwt () = return (!logger.info "Starting http server on port %d" port) in
    Server.create ~mode:tcp_mode entry_point in
  let t2 =
    match cert_opt, key_opt with
    | Some cert, Some key ->
      let port = port + 1 in
      let ssl_mode = `TLS (`Crt_file_path cert, `Key_file_path key, `No_password, `Port port) in
      let%lwt () = return (!logger.info "Starting https server on port %d" port) in
      Server.create ~mode:ssl_mode entry_point
    | None, None ->
      return (!logger.info "Not starting https server")
    | _ ->
      return (!logger.info "Missing some of SSL configuration") in
  join [ t1 ; t2 ]
