open Batteries
open Lwt
open Cohttp
open Cohttp_lwt_unix
open RamenLog
open RamenSharedTypes

let enc = Uri.pct_encode

let check_code (resp, body) =
  let code = resp |> Response.status |> Code.code_of_status in
  let%lwt body = Cohttp_lwt.Body.to_string body in
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
  Node.{ name ; operation }

let add debug ramen_url name ops () =
  logger := make_logger debug ;
  let nodes = List.map node_info_of_op ops in
  let msg = { name ; nodes ; ok_if_running = false } in
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

let stop debug layer_name ramen_url () =
  logger := make_logger debug ;
  Lwt_main.run (
    let url = if layer_name = "" then
      ramen_url ^"/stop"
    else
      ramen_url ^"/stop/"^ enc layer_name in
    http_get url >>= check_ok)

let shutdown debug ramen_url () =
  logger := make_logger debug ;
  Lwt_main.run (
    let url = ramen_url ^"/shutdown" in
    (* Do not expect any response for now. *)
    catch (fun () ->
      Client.get (Uri.of_string url) >>=
        fun _ -> return_unit)
      (fun e ->
        (match e with
          Unix.Unix_error(Unix.ECONNREFUSED, "connect", "") ->
           Printf.eprintf "Cannot connect to ramen. Is it really running?\n"
         | _ -> ()) ;
        return_unit))

let resp_column_length = function
  | _typ, None, column -> column_length column
  | _typ, Some nullmask, _column -> Array.length nullmask

let column_value_at n =
  let g a = Array.get a n in
  let open Lang.Scalar in
  function
  | AFloat a -> to_string (VFloat (g a))
  | AString a -> to_string (VString (g a))
  | ABool a -> to_string (VBool (g a))
  | AU8 a -> to_string (VU8 (g a))
  | AU16 a -> to_string (VU16 (g a))
  | AU32 a -> to_string (VU32 (g a))
  | AU64 a -> to_string (VU64 (g a))
  | AU128 a -> to_string (VU128 (g a))
  | AI8 a -> to_string (VI8 (g a))
  | AI16 a -> to_string (VI16 (g a))
  | AI32 a -> to_string (VI32 (g a))
  | AI64 a -> to_string (VI64 (g a))
  | AI128 a -> to_string (VI128 (g a))
  | ANull _ -> to_string VNull
  | AEth a -> g a
  | AIpv4 a -> g a
  | AIpv6 a -> g a
  | ACidrv4 a -> g a
  | ACidrv6 a -> g a

let tuples_of_columns columns =
  assert (columns <> []) ;
  let nb_tuples = resp_column_length (List.hd columns) in
  let nb_fields = List.length columns in
  let field_types =
    List.map (fun (typ_name, nullmask_opt, ts) ->
      let nullable = nullmask_opt <> None in
      { typ_name ; nullable ; typ = type_of_column ts }) columns in
  (* Build the (all-string) tuple of line l *)
  let value_idx_of_tuple_idx col_idx =
    let _typ, nullmask, column = List.at columns col_idx in
    match nullmask with
    | None ->
      fun i -> column_value_at i column
    | Some nullmask ->
      let value_idx_of_tuple_idx = Array.make nb_tuples ~-1 in
      let _nb_set =
        Array.fold_lefti (fun nb_set i not_null ->
            if not_null then (
              value_idx_of_tuple_idx.(i) <- nb_set ;
              nb_set + 1
            ) else nb_set
          ) 0 nullmask in
      fun tuple_idx ->
        match value_idx_of_tuple_idx.(tuple_idx) with
        | -1 -> Lang.Scalar.to_string VNull
        | i -> column_value_at i column
  in
  let value_at = List.init nb_fields value_idx_of_tuple_idx in
  let tuple_of l =
    List.map (fun value_get -> value_get l) value_at
  in
  field_types, List.init nb_tuples tuple_of

let display_tuple_as_csv ?(with_header=false) ?(separator=",") ?(null="") to_drop resp =
  (* We have to "turn" the arrays 90º *)
  let _field_types, tuples =
    tuples_of_columns resp.columns in
  let tuples = List.drop to_drop tuples in
  if with_header then
    List.print ~first:"#" ~last:"\n" ~sep:separator
               (fun fmt (name, _, _) -> String.print fmt name)
               stdout resp.columns ;
  ignore null ;
  let print_row =
    List.print ~first:"" ~last:"\n" ~sep:separator
                String.print in
  List.print ~first:"" ~last:"" ~sep:""
             print_row stdout tuples

(* TODO: make as_csv the only possible option *)
let display_tuple_as_is t =
  let s = PPP.to_string export_resp_ppp t in
  Printf.printf "%s\n" s

let display_tuple as_csv with_header to_drop t =
  if as_csv then display_tuple_as_csv ~with_header to_drop t
  else display_tuple_as_is t ;
  Printf.printf "%!"

let ppp_of_string_exc ppp s =
  try PPP.of_string_exc ppp s |> return
  with e -> fail e

let export_and_display ramen_url node_name as_csv with_header continuous =
  let url = ramen_url ^"/export/"^
    (match String.rsplit ~by:"/" node_name with
    | exception Not_found -> enc node_name
    | layer, node -> enc layer ^"/"^ enc node) in
  let rec get_next ?since ?max_results () =
    let msg = { since ; max_results ; wait_up_to = 2.0 (* TODO: a param? *) } in
    let%lwt resp = http_post_json url export_req_ppp msg >>=
                   ppp_of_string_exc export_resp_ppp in
    (* TODO: check first_seqnum is not bigger than expected *)
    let len = if resp.columns = [] then 0
              else resp_column_length (List.hd resp.columns) in
    if resp.columns <> [] then (
      display_tuple as_csv with_header 0 resp ;
      flush stdout) ;
    let max_results = Option.map (fun l -> l - len) max_results in
    if max_results |? (if continuous then 1 else 0) > 0 then (
      let since = resp.first + len in
      get_next ~since ?max_results ()
    ) else return_unit
  in
  get_next

(* TODO: separator and null placeholder for csv *)
let tail debug ramen_url node_name as_csv with_header last continuous () =
  logger := make_logger debug ;
  let exporter = export_and_display ramen_url node_name as_csv with_header continuous in
  let max_results =
    if continuous then None else Some last
  and since = ~- last in
  Lwt_main.run (exporter ?max_results ~since ())

(* TODO: separator and null placeholder for csv *)
let export debug ramen_url node_name as_csv with_header max_results continuous () =
  logger := make_logger debug ;
  let exporter = export_and_display ramen_url node_name as_csv with_header continuous in
  Lwt_main.run (exporter ?max_results ())

let timeseries debug ramen_url since until max_data_points
               node data_field consolidation () =
  logger := make_logger debug ;
  let url = ramen_url ^"/timeseries"
  and msg =
    { since ; until ; max_data_points ;
      timeseries = [
        { id = "cmdline" ;
          consolidation = consolidation |? "avg" ;
          spec = Predefined { node ; data_field } } ] } in
  let th =
    let%lwt body = http_post_json url timeseries_req_ppp msg in
    Printf.printf "%s\n%!" body (* TODO *) ;
    return_unit in
  Lwt_main.run th

let timerange debug ramen_url node_name () =
  logger := make_logger debug ;
  let url = ramen_url ^"/timerange/"^
    (match String.rsplit ~by:"/" node_name with
    | exception Not_found -> enc node_name
    | layer, node -> enc layer ^"/"^ enc node) in
  Lwt_main.run (
    match%lwt http_get url >>=
               ppp_of_string_exc time_range_resp_ppp with
    | NoData ->
      Printf.printf "Node has no data (yet)\n%!" ;
      return_unit
    | TimeRange (oldest, latest) ->
      Printf.printf "%f...%f\n%!" oldest latest ;
      return_unit)
