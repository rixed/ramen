open Batteries
open Lwt
open Cohttp
open Cohttp_lwt_unix
open Log
open RamenSharedTypes

let enc = Uri.pct_encode

let check_code (resp, body) =
  let code = resp |> Response.status |> Code.code_of_status in
  let%lwt body = Cohttp_lwt_body.to_string body in
  if code <> 200 then (
    !logger.error "Response code: %d" code ;
    !logger.error "Answer: %S" body ;
    fail_with ("Error HTTP "^ string_of_int code)
  ) else (
    !logger.debug "Response code: %d" code ;
    !logger.debug "Answer: %S" body ;
    return body
  )

(* Return the answered body *)
let http_do ?(cmd=Client.put) ?content_type ?body url =
  let headers = Header.init_with "Connection" "close" in
  let headers = match content_type with
    | Some ct -> Header.add headers "Content-Type" ct
    | None -> headers in
  !logger.debug "%S < %a" url (Option.print String.print) body ;
  let body = Option.map (fun s -> `String s) body in
  cmd ~headers ?body (Uri.of_string url) >>= check_code

(* Return the answered body *)
let http_put_json url ppp msg =
  let body = PPP.to_string ppp msg in
  http_do ~content_type:Consts.json_content_type ~body url

let http_post_json url ppp msg =
  let body = PPP.to_string ppp msg in
  http_do ~cmd:Client.post ~content_type:Consts.json_content_type ~body url

let http_get url =
  Client.get (Uri.of_string url) >>= check_code

let check_ok body =
  (* Yeah that's grand *)
  ignore body ;
  return_unit

let node_info_of_op op =
  let name, operation =
    try String.split ~by:":" op
    with Not_found -> "", op in
  Node.{ name ; operation ; parents = [] }

let add debug ramen_url name ops links () =
  logger := make_logger debug ;
  let nodes = List.map node_info_of_op ops in
  List.iter (fun link ->
    let n1, n2 = String.split ~by:":" link in
    let node = List.find (fun n -> n2 = n.Node.name || name ^"/"^ n2 = n.Node.name) nodes in
    node.Node.parents <- n1 :: node.Node.parents) links ;
  let msg = { name ; nodes } in
  Lwt_main.run (
    http_put_json (ramen_url ^"/graph") put_layer_req_ppp msg >>= check_ok)

let compile debug ramen_url () =
  logger := make_logger debug ;
  Lwt_main.run (
    http_get (ramen_url ^"/compile") >>= check_ok)

let run debug ramen_url () =
  logger := make_logger debug ;
  Lwt_main.run (
    http_get (ramen_url ^"/run") >>= check_ok)

let shutdown debug ramen_url () =
  logger := make_logger debug ;
  Lwt_main.run (
    http_get (ramen_url ^"/stop") >>= check_ok)

let tuples_of_columns columns =
  let field_types =
    List.map (fun (typ_name, nullable, ts) ->
      { typ_name ; nullable ; typ = type_of_column ts }) columns in
  (* Build the tuple of line l *)
  let tuple_of l =
    List.map (fun (_, _, ts) -> column_value_at l ts) columns in
  let len =
    let _, _, fst_col = List.hd columns in
    column_length fst_col
  in
  field_types, List.init len tuple_of

let display_tuple_as_csv ?(with_header=false) ?(separator=",") ?(null="") resp =
  (* We have to "turn" the arrays 90º *)
  let _field_types, tuples =
    tuples_of_columns resp.columns in
  (* TODO: print header line? *)
  ignore with_header ;
  ignore null ;
  let print_value =
    List.print ~first:"" ~last:"\n" ~sep:separator
                Lang.Scalar.print in
  List.print ~first:"" ~last:"" ~sep:""
             print_value stdout tuples

let display_tuple_as_is t =
  let s = PPP.to_string export_resp_ppp t in
  Printf.printf "%s\n" s

let display_tuple as_csv t =
  if as_csv then display_tuple_as_csv t
  else display_tuple_as_is t

let drop_firsts n resp =
  if n = 0 then resp else
  { resp with
      columns =
        List.map (fun (name, null, columns) ->
            let mapper =
              { f = (fun a -> Array.tail a n) ; null = fun l -> l - n } in
            name, null, column_map mapper columns
          ) resp.columns }

let ppp_of_string_exc ppp s =
  try PPP.of_string_exc ppp s |> return
  with e -> fail e

let tail debug ramen_url node_name as_csv last continuous () =
  logger := make_logger debug ;
  let url = ramen_url ^"/export/"^
    (match String.rsplit ~by:"/" node_name with
    | exception Not_found -> enc node_name
    | layer, node -> enc layer ^"/"^ enc node) in
  let rec get_next ?since ?max_results ?last () =
    let msg = { since ; max_results ; wait_up_to = Some 2.0 (* TODO: a param? *) } in
    let%lwt resp = http_post_json url export_req_ppp msg >>=
                   ppp_of_string_exc export_resp_ppp in
    (* TODO: check first_seqnum is not bigger than expected *)
    let len =
      let _, _, columns = List.hd resp.columns in
      column_length columns in
    let to_drop = Option.map_default (fun last ->
        if len > last then len - last else 0) 0 last in
    let resp = drop_firsts to_drop resp in
    display_tuple as_csv resp ;
    flush stdout ;
    if continuous then (
      let last = Option.map (fun l -> l - len) last in
      if last |? 1 > 0 then (
        let since = resp.first + len in
        get_next ~since ?max_results:last ?last ()
      ) else return_unit
    ) else return_unit
  in
  Lwt_main.run (get_next ?last ())
