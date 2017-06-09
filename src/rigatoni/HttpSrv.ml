(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open Batteries
open BatOption.Infix
open Cohttp
open Cohttp_lwt_unix
open Lwt
open RamenConf

(* API:

== Add/Delete a node ==

Nodes are referred to via name that can be anything as long as they are unique.
So the client decide on the name. The server ensure uniqueness by forbidding
creation of a new node by the same name as one that exists already.

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

exception HttpError of (int * string)

let not_implemented msg = fail (HttpError (501, msg))
let bad_request msg = fail (HttpError (400, msg))

let json_content_type = "application/json"

let get_content_type headers =
  Header.get headers "Content-Type" |? json_content_type |> String.lowercase

(* PUT *)

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
    output_ring_size = None ;\
    info = None }\
    (PPP.of_string_exc make_node_ppp "{\"operation\":\"test\"}")

  { operation = "op" ;\
    input_ring_size = Some 42 ;\
    output_ring_size = None ;\
    info = None }\
    (PPP.of_string_exc make_node_ppp "{\"operation\":\"op\", \"input_ring_size\":42}")
*)

let put_node conf headers name body =
  (* Get the message from the body *)
  if get_content_type headers <> json_content_type then
    bad_request "Bad content type"
  else match PPP.of_string_exc make_node_ppp body with
  | exception e -> fail e
  | msg ->
    if has_node conf conf.running_graph name then
      bad_request ("Node "^name^" already exists") else
    let open Lang.P in
    let p = Lang.Operation.Parser.p +- Lang.opt_blanks +- eof in
    (* TODO: enable error correction *)
    (match p [] None Parsers.no_error_correction (stream_of_string msg.operation) |>
          to_result with
    | Bad e ->
      let err = IO.to_string (Lang.P.print_bad_result Lang.Operation.print) e in
      bad_request ("Parse error: "^ err)
    | Ok (op, _) -> (* Since we force EOF, no need to keep what's left to parse *)
      (match Lang.Operation.Parser.check op with
      | Bad e -> bad_request ("Invalid operation: "^ e)
      | Ok op ->
        let node = make_node conf name op in
        add_node conf conf.running_graph name node ;
        let status = `Code 200 in
        Server.respond_string ~status ~body:"" ()))

(* GET *)

type node_id = string [@@ppp PPP_JSON]

type node_info =
  (* I'd like to offer the AST but PPP still fails on recursive types :-( *)
  { operation : string } [@@ppp PPP_JSON]

let get_node conf _headers name =
  match find_node conf conf.running_graph name with
  | exception Not_found ->
    fail (HttpError (404, "No such node"))
  | node ->
    let node_info =
      { operation = IO.to_string Lang.Operation.print node.operation } in
    let body = PPP.to_string node_info_ppp node_info ^"\n" in
    let status = `Code 200 in
    Server.respond_string ~status ~body ()

(* DELETE *)

let del_node conf _headers name =
  match remove_node conf conf.running_graph name with
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
  match find_node conf graph n with
  | exception Not_found ->
    bad_request ("Node "^ n ^" does not exist")
  | node -> return node

let put_link conf _headers src dst =
  let%lwt src = node_of_name conf conf.running_graph src in
  let%lwt dst = node_of_name conf conf.running_graph dst in
  if has_link conf src dst then
    bad_request ("Link already exists")
  else (
    make_link conf src dst ;
    let status = `Code 200 in
    Server.respond_string ~status ~body:"" ())

let del_link conf _headers src dst =
  let%lwt src = node_of_name conf conf.running_graph src in
  let%lwt dst = node_of_name conf conf.running_graph dst in
  if not (has_link conf src dst) then
    bad_request ("That link does not exist")
  else (
    make_link conf src dst ;
    let status = `Code 200 in
    Server.respond_string ~status ~body:"" ())

let get_link conf _headers src dst =
  let%lwt src = node_of_name conf conf.running_graph src in
  let%lwt dst = node_of_name conf conf.running_graph dst in
  if not (has_link conf src dst) then
    bad_request ("That link does not exist")
  else (
    let status = `Code 200 and body = "{}\n" in
    Server.respond_string ~status ~body ())

(*
== Get info about a node ==

== Display the graph (json or svg representation) ==

*)

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
        | `PUT, ["node" ; name] -> put_node conf headers name body_str
        | `GET, ["node" ; name] -> get_node conf headers name
        | `DELETE, ["node" ; name] -> del_node conf headers name
        | `PUT, ["link" ; src ; dst] -> put_link conf headers src dst
        | `GET, ["link" ; src ; dst] -> get_link conf headers src dst
        | `DELETE, ["link" ; src ; dst] -> del_link conf headers src dst
        | `PUT, _ | `GET, _ | `DELETE, _ ->
          fail (HttpError (404, "No such resource"))
        | _ ->
          fail (HttpError (405, "Method not implemented"))
      with exn -> fail exn)
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
    let%lwt () = return (conf.logger.Log.info "Starting http server on port %d" port) in
    Server.create ~mode:tcp_mode entry_point in
  let t2 =
    match cert_opt, key_opt with
    | Some cert, Some key ->
      let port = port + 1 in
      let ssl_mode = `TLS (`Crt_file_path cert, `Key_file_path key, `No_password, `Port port) in
      let%lwt () = return (conf.logger.Log.info "Starting https server on port %d" port) in
      Server.create ~mode:ssl_mode entry_point
    | None, None ->
      return (conf.logger.Log.info "Not starting https server")
    | _ ->
      return (conf.logger.Log.info "Missing some of SSL configuration") in
  join [ t1 ; t2 ]
