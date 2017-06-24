(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open Batteries
open BatOption.Infix
open Cohttp
open Cohttp_lwt_unix
open Lwt
open Log
module C = RamenConf

exception HttpError of (int * string)

let not_implemented msg = fail (HttpError (501, msg))
let bad_request msg = fail (HttpError (400, msg))

let json_content_type = "application/json"
let dot_content_type = "text/dot"
let text_content_type = "text/plain"
let html_content_type = "text/html"
let css_content_type = "text/css"
let js_content_type = "application/javascript"

let get_content_type headers =
  Header.get headers "Content-Type" |? json_content_type |> String.lowercase

let get_accept headers =
  Header.get headers "Accept" |? json_content_type |> String.lowercase

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

type make_node =
  { (* The input type of this node is any tuple source with at least all the
     * field mentioned in the "in" tuple of its operation. *)
    operation : string ; (* description of what this node does in the DSL defined in Lang.ml *)
    (* Fine tunning info about the size of in/out ring buffers etc. *)
    input_ring_size : int option [@ppp_default None] ;
    output_ring_size : int option [@ppp_default None] } [@@ppp PPP_JSON]

(*$= make_node_ppp & ~printer:(PPP.to_string make_node_ppp)
  { operation = "test" ;\
    input_ring_size = None ;\
    output_ring_size = None }\
    (PPP.of_string_exc make_node_ppp "{\"operation\":\"test\"}")

  { operation = "op" ;\
    input_ring_size = Some 42 ;\
    output_ring_size = None }\
    (PPP.of_string_exc make_node_ppp "{\"operation\":\"op\", \"input_ring_size\":42}")
*)

let compile_operation name operation =
  let open Lang.P in
  let p = Lang.(opt_blanks -+ Operation.Parser.p +- opt_blanks +- eof) in
  (* TODO: enable error correction *)
  match p [] None Parsers.no_error_correction (stream_of_string operation) |>
        to_result with
  | Bad e ->
    let err =
      IO.to_string (Lang.P.print_bad_result Lang.Operation.print) e in
    bad_request ("Creating node "^ name ^": Parse error: "^ err)
  | Ok (op, _) -> (* Since we force EOF, no need to keep what's left to parse *)
    (match Lang.Operation.Parser.check op with
    | exception (Lang.SyntaxError e) ->
      bad_request ("Creating node "^ name ^": "^ e)
    | () -> Lwt.return op)

let put_node conf headers name body =
  (* Get the message from the body *)
  if get_content_type headers <> json_content_type then
    bad_request "Bad content type"
  else match PPP.of_string_exc make_node_ppp body with
  | exception e ->
    !logger.info "Creating node %s: Cannot parse received body: %s"
      name body ;
    fail e
  | msg ->
    (match C.find_node conf conf.C.building_graph name with
    | exception Not_found ->
      let%lwt op = compile_operation name msg.operation in
      let node = C.make_node name op in
      Lwt.return (C.add_node conf conf.C.building_graph name node)
    | node ->
      (match node.C.pid with
      | Some pid ->
        (* TODO: monitor those process and clear the pid when they fail
         * (and record that they've failed! *)
        bad_request ("Node "^name^" is already running as pid "^
                     string_of_int pid)
      | None ->
        let%lwt op = compile_operation name msg.operation in
        Lwt.return (C.update_node node op)
      )) >>= fun () ->
    let status = `Code 200 in
    Server.respond_string ~status ~body:"" ()

type node_id = string [@@ppp PPP_JSON]

type node_info =
  (* I'd like to offer the AST but PPP still fails on recursive types :-( *)
  { name : string ;
    operation : string ;
    command : string option ;
    pid : int option ;
    nb_parents : int ;
    nb_children : int ;
    input_type : (int option * Lang.Expr.typ) list ;
    output_type : (int option * Lang.Expr.typ) list } [@@ppp PPP_JSON]

let node_info_of_node node =
  { name = node.C.name ;
    operation = IO.to_string Lang.Operation.print node.C.operation ;
    command = node.C.command ;
    pid = node.C.pid ;
    nb_parents = List.length node.C.parents ;
    nb_children = List.length node.C.children ;
    input_type = C.list_of_temp_tup_type node.C.in_type ;
    output_type = C.list_of_temp_tup_type node.C.out_type }

let get_node conf _headers name =
  match C.find_node conf conf.C.building_graph name with
  | exception Not_found ->
    fail (HttpError (404, "No such node"))
  | node ->
    let node_info = node_info_of_node node in
    let body = PPP.to_string node_info_ppp node_info ^"\n" in
    let status = `Code 200 in
    let headers = Header.init_with "Content-Type" json_content_type in
    Server.respond_string ~headers ~status ~body ()

let del_node conf _headers name =
  match C.remove_node conf conf.C.building_graph name with
  | exception Not_found ->
    fail (HttpError (404, "No such node"))
  | () ->
    let status = `Code 200 in
    Server.respond_string ~status ~body:"" ()

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
    let status = `Code 200 in
    Server.respond_string ~status ~body:"" ())

let del_link conf _headers src dst =
  let%lwt src = node_of_name conf conf.C.building_graph src in
  let%lwt dst = node_of_name conf conf.C.building_graph dst in
  if not (C.has_link conf src dst) then
    bad_request ("That link does not exist")
  else (
    C.remove_link conf conf.C.building_graph src dst ;
    let status = `Code 200 in
    Server.respond_string ~status ~body:"" ())

let get_link conf _headers src dst =
  let%lwt src = node_of_name conf conf.C.building_graph src in
  let%lwt dst = node_of_name conf conf.C.building_graph dst in
  if not (C.has_link conf src dst) then
    bad_request ("That link does not exist")
  else (
    let status = `Code 200 and body = "{}\n" in
    let headers = Header.init_with "Content-Type" json_content_type in
    Server.respond_string ~headers ~status ~body ())

(*
== Display the graph (JSON or SVG representation) ==

Begin with the graph as a JSON object.
*)

type graph_info =
  { nodes : node_info list ;
    links : (string * string) list } [@@ppp PPP_JSON]

let get_graph_json conf _headers =
  let graph_info =
    { nodes = Hashtbl.fold (fun _name node lst ->
        node_info_of_node node :: lst
      ) conf.C.building_graph.C.nodes [] ;
      links = Hashtbl.fold (fun name node lst ->
        let links = List.map (fun c -> name, c.C.name) node.C.children in
        List.rev_append links lst
      ) conf.C.building_graph.C.nodes [] } in
  let body = PPP.to_string graph_info_ppp graph_info ^"\n" in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" json_content_type in
  Server.respond_string ~headers ~status ~body ()

let dot_of_graph graph =
  let dot = IO.output_string () in
  Printf.fprintf dot "digraph rigatoni {\n" ;
  Hashtbl.keys graph.C.nodes |>
    Enum.iter (Printf.fprintf dot "\t%S\n") ;
  Printf.fprintf dot "\n" ;
  Hashtbl.iter (fun name node ->
      List.iter (fun c ->
          Printf.fprintf dot "\t%S -> %S\n" name c.C.name
        ) node.C.children
    ) graph.C.nodes ;
  Printf.fprintf dot "}\n" ;
  IO.close_out dot

let get_graph_dot conf _headers =
  let body = dot_of_graph conf.C.building_graph in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" dot_content_type in
  Server.respond_string ~headers ~status ~body ()

let get_graph conf headers =
  let accept = get_accept headers in
  if accept_anything accept ||
     String.starts_with accept json_content_type then
    get_graph_json conf headers
  else if String.starts_with accept dot_content_type then
    get_graph_dot conf headers
  else
    let status = Code.status_of_code 406 in
    Server.respond_error ~status ~body:("Can't produce "^ accept ^"\n") ()

let compile conf _headers =
  (* TODO: check we accept json *)
  match C.compile conf conf.C.building_graph with
  | exception (Lang.SyntaxError e) ->
    bad_request e
  | () ->
    let headers = Header.init_with "Content-Type" json_content_type in
    let status = `Code 200 in
    Server.respond_string ~headers ~status ~body:"" ()

let run conf _headers =
  (* TODO: check we accept json *)
  match C.run conf conf.C.building_graph with
  | exception (Lang.SyntaxError e) ->
    bad_request e
  | () ->
    let headers = Header.init_with "Content-Type" json_content_type in
    let status = `Code 200 in
    Server.respond_string ~headers ~status ~body:"" ()

let ext_of_file fname =
  let _, ext = String.rsplit fname ~by:"." in ext

let content_type_of_ext = function
  | "html" -> html_content_type
  | "js" -> js_content_type
  | "css" -> css_content_type
  | _ -> "I_dont_know/Good_luck"

let get_file _conf _headers file =
  let fname = "www/"^ file in
  !logger.info "Serving file %S" fname ;
  let headers =
    Header.init_with "Content-Type" (content_type_of_ext (ext_of_file file)) in
  Server.respond_file ~headers ~fname ()

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
      try
        match Request.meth req, paths with
        (* API *)
        | `PUT, ["node" ; name] -> put_node conf headers name body_str
        | `GET, ["node" ; name] -> get_node conf headers name
        | `DELETE, ["node" ; name] -> del_node conf headers name
        | `PUT, ["link" ; src ; dst] -> put_link conf headers src dst
        | `GET, ["link" ; src ; dst] -> get_link conf headers src dst
        | `DELETE, ["link" ; src ; dst] -> del_link conf headers src dst
        | `GET, ["graph"] -> get_graph conf headers
        | `GET, ["compile"] -> compile conf headers
        | `GET, ["run"] -> run conf headers
        (* WWW Client *)
        | `GET, ["" | "index.html"] ->
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
