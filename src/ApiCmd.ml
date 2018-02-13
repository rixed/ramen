open Batteries
open Lwt
open Cohttp
open Cohttp_lwt_unix
open RamenLog
open RamenSharedTypes
open Helpers
module C = RamenConf

type copts =
  { debug : bool ; server_url : string ; persist_dir : string ;
    max_history_archives : int ; use_embedded_compiler : bool ;
    bundle_dir : string ; max_simult_compilations : int }

let make_copts debug server_url persist_dir max_history_archives
               use_embedded_compiler bundle_dir max_simult_compilations =
  { debug ; server_url ; persist_dir ; max_history_archives ;
    use_embedded_compiler ; bundle_dir ; max_simult_compilations }

let enc = Uri.pct_encode

let exhort ?(err_ok=false) http_cmd =
  let on = function
    | Unix.Unix_error (Unix.ECONNREFUSED, "connect", "") -> return_true
    | _ -> return_false in
  let%lwt resp, body =
    Helpers.retry ~first_delay:0.2 ~on ~max_retry:3 http_cmd () in
  let code = resp |> Response.status |> Code.code_of_status in
  let%lwt body = Cohttp_lwt.Body.to_string body in
  if code <> 200 then (
    if err_ok then (
      !logger.debug "Response code: %d" code ;
      !logger.debug "Answer: %S" body
    ) else (
      !logger.error "Response code: %d" code ;
      !logger.error "Answer: %S" body
    ) ;
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
  exhort (fun () -> cmd ~headers ?body (Uri.of_string (sure_is_http url)))

(* Return the answered body *)
let http_put_json url ppp msg =
  let body = PPP.to_string ppp msg in
  http_do ~content_type:Consts.json_content_type ~body url

let http_post_json url ppp msg =
  let body = PPP.to_string ppp msg in
  http_do ~cmd:Client.post ~content_type:Consts.json_content_type ~body url

let http_get ?err_ok url =
  exhort ?err_ok (fun () -> Client.get (Uri.of_string (sure_is_http url)))

let check_ok body =
  (* Yeah that's grand *)
  ignore body ;
  return_unit

let add copts name program ok_if_running start remote () =
  logger := make_logger copts.debug ;
  if remote && ok_if_running then
    failwith "Options --remote and --ok-if-running are incompatible." ;
  if remote && start then
    failwith "Options --remote and --start are incompatible." ;
  Lwt_main.run (
    if remote then
      let msg = { name ; ok_if_running ; for_test = false ;
                  start ; program } in
      http_put_json (copts.server_url ^"/graph") put_program_req_ppp msg >>=
      check_ok
    else
      let conf =
        C.make_conf true copts.server_url copts.debug copts.persist_dir
                    copts.max_simult_compilations copts.max_history_archives
                    copts.use_embedded_compiler copts.bundle_dir in
      RamenOps.set_program conf ~ok_if_running ~start ~name ~program)

let compile copts () =
  logger := make_logger copts.debug ;
  Lwt_main.run (
    http_get (copts.server_url ^"/compile") >>= check_ok)

let run copts () =
  logger := make_logger copts.debug ;
  Lwt_main.run (
    http_get (copts.server_url ^"/run") >>= check_ok)

let start copts daemonize rand_seed no_demo to_stderr www_dir http_port
          ssl_cert ssl_key alert_conf_json () =
  let demo = not no_demo in (* FIXME: in the future do not start demo by default? *)
  if to_stderr && daemonize then
    failwith "Options --daemonize and --to-stderr are incompatible." ;
  (match rand_seed with
  | None -> Random.self_init ()
  | Some seed -> Random.init seed) ;
  let logdir =
    if to_stderr then None else Some (copts.persist_dir ^"/log") in
  Option.may mkdir_all logdir ;
  logger := make_logger ?logdir copts.debug ;
  let conf =
    C.make_conf true copts.server_url copts.debug copts.persist_dir
                copts.max_simult_compilations copts.max_history_archives
                copts.use_embedded_compiler copts.bundle_dir in
  HttpSrv.start conf daemonize demo www_dir http_port ssl_cert ssl_key
                alert_conf_json

let stop copts program_name () =
  logger := make_logger copts.debug ;
  Lwt_main.run (
    let url = if program_name = "" then
      copts.server_url ^"/stop"
    else
      copts.server_url ^"/stop/"^ enc program_name in
    http_get url >>= check_ok)

let shutdown copts () =
  logger := make_logger copts.debug ;
  Lwt_main.run (
    let url = copts.server_url ^"/shutdown" in
    (* Do not expect any response for now. *)
    try%lwt
      let%lwt _ = Client.get (Uri.of_string (sure_is_http url)) in
      return_unit
    with Unix.Unix_error(Unix.ECONNREFUSED, "connect", "") ->
           Printf.eprintf "Cannot connect to ramen. Is it really running?\n" ;
           return_unit
       | _ -> return_unit)

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

let export_and_display server_url func_name as_csv with_header continuous =
  let url = server_url ^"/export/"^
    (match String.rsplit ~by:"/" func_name with
    | exception Not_found -> enc func_name
    | program, func -> enc program ^"/"^ enc func) in
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
let tail copts func_name as_csv with_header last continuous () =
  logger := make_logger copts.debug ;
  let exporter = export_and_display copts.server_url func_name as_csv with_header continuous in
  let max_results =
    if continuous then None else Some last
  and since = ~- last in
  Lwt_main.run (exporter ?max_results ~since ())

(* TODO: separator and null placeholder for csv *)
let export copts func_name as_csv with_header max_results continuous () =
  logger := make_logger copts.debug ;
  let exporter = export_and_display copts.server_url func_name as_csv with_header continuous in
  Lwt_main.run (exporter ?max_results ())

let timeseries copts since until max_data_points
               operation data_field consolidation () =
  logger := make_logger copts.debug ;
  let url = copts.server_url ^"/timeseries"
  and msg =
    { since ; until ; max_data_points ;
      timeseries = [
        { id = "cmdline" ;
          consolidation = consolidation |? "avg" ;
          spec = Predefined { operation ; data_field } } ] } in
  let th =
    let%lwt body = http_post_json url timeseries_req_ppp msg in
    Printf.printf "%s\n%!" body (* TODO *) ;
    return_unit in
  Lwt_main.run th

let timerange copts func_name () =
  logger := make_logger copts.debug ;
  let url = copts.server_url ^"/timerange/"^
    (match String.rsplit ~by:"/" func_name with
    | exception Not_found -> enc func_name
    | program, func -> enc program ^"/"^ enc func) in
  Lwt_main.run (
    match%lwt http_get url >>=
               ppp_of_string_exc time_range_resp_ppp with
    | NoData ->
      Printf.printf "Function has no data (yet)\n%!" ;
      return_unit
    | TimeRange (oldest, latest) ->
      Printf.printf "%f...%f\n%!" oldest latest ;
      return_unit)


let get_program_info_remote ?err_ok copts name_opt =
  let url = copts.server_url ^"/graph"^
            (Option.map (fun n -> "/"^n) name_opt |? "") in
  http_get ?err_ok url >>=
  ppp_of_string_exc get_graph_resp_ppp

let get_program_info = get_program_info_remote

let int_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some i -> TermTable.ValInt i

let flt_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some f -> TermTable.ValFlt f

let str_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some s -> TermTable.ValStr s

let time_or_na = function
  | None -> TermTable.ValStr "n/a"
  | Some f -> TermTable.ValStr (string_of_time f)

let abbrev_str_list lst =
  TermTable.ValStr (
    if lst = [] then "ø" else
    let s =
      List.fold_left (fun s n ->
        (if s = "" then s else s ^", ")^ n
      ) "" lst in
    let s = RamenHtml.abbrev 17 s in
    s ^" ("^ string_of_int (List.length lst) ^")")

let display_program_info json short info =
  if json then
    let str = PPP.to_string get_graph_resp_ppp info |>
              PPP_prettify.prettify in
    Printf.printf "%s\n" str
  else if short then
    let open TermTable in
    let head = [| "name" ; "#ops" ; "status" ; "started" ; "stopped" |] in
    let lines =
      List.map (fun prog ->
        let open Info.Program in
        [| ValStr prog.name ;
           ValInt (List.length prog.operations) ;
           ValStr (string_of_status prog.status) ;
           time_or_na prog.last_started ;
           time_or_na prog.last_stopped |]
      ) info in
    print_table head lines
  else
    let open TermTable in
    let head = [| "program" ; "operation" ; "#in" ; "#selected" ; "#out" ;
                  "#groups" ; "CPU" ; "wait in" ; "wait out" ; "heap" ;
                  "volume in" ; "volume out" ; "#parents" ; "#children" ;
                  "pid" ; "signature" |] in
    let lines =
      List.fold_left (fun lines prog ->
        List.fold_left (fun lines func ->
          let open Info.Func in
          [| ValStr prog.Info.Program.name ;
             ValStr func.definition.name ;
             int_or_na func.stats.in_tuple_count ;
             int_or_na func.stats.selected_tuple_count ;
             int_or_na func.stats.out_tuple_count ;
             int_or_na func.stats.group_count ;
             ValFlt func.stats.cpu_time ;
             flt_or_na func.stats.in_sleep ;
             flt_or_na func.stats.out_sleep ;
             ValInt func.stats.ram_usage ;
             int_or_na func.stats.in_bytes ;
             int_or_na func.stats.out_bytes ;
             ValInt (List.length func.parents) ;
             ValInt (List.length func.children) ;
             int_or_na func.pid ;
             str_or_na func.signature |] :: lines
        ) lines prog.operations
      ) [] info in
    print_table head lines

let form_of_type =
  List.map (fun ti ->
    ti.name_info,
    TermTable.ValStr (
      (match ti.typ_info with
      | None -> "unknown type"
      | Some t -> Lang.Scalar.string_of_typ t) ^", "^
      (match ti.nullable_info with
      | None -> "unknown nullability"
      | Some true -> "NULL"
      | Some false -> "NOT NULL")))

let display_operation_info json short func =
  if json then
    let str = PPP.to_string Info.Func.info_ppp func |>
              PPP_prettify.prettify in
    Printf.printf "%s\n" str
  else
    let open Info.Func in
    let open TermTable in
    let form =
      [ "Name", ValStr func.definition.name ;
        "#in", int_or_na func.stats.in_tuple_count ;
        "#selected", int_or_na func.stats.selected_tuple_count ;
        "#out", int_or_na func.stats.out_tuple_count ;
        "#group", int_or_na func.stats.group_count ;
        "Event time", ValStr (string_of_time func.stats.time) ;
        "CPU", ValFlt func.stats.cpu_time ;
        "Wait-in", flt_or_na func.stats.in_sleep ;
        "Wait-out", flt_or_na func.stats.out_sleep ;
        "RAM", ValInt func.stats.ram_usage ;
        "Read", int_or_na func.stats.in_bytes ;
        "Written", int_or_na func.stats.out_bytes ;
        "Parents", abbrev_str_list func.parents ;
        "Children", abbrev_str_list func.children ;
        "PID", int_or_na func.pid ;
        "Signature", str_or_na func.signature ;
        "Last Exit Status", ValStr func.last_exit ] in
    print_form form ;
    if not short then (
      Printf.printf "\nInput:\n" ;
      print_form (form_of_type func.input_type) ;
      Printf.printf "\nOperation:\n%s\n"
        (PPP_prettify.prettify func.definition.operation) ;
      Printf.printf "\nOutput:\n" ;
      print_form (form_of_type func.output_type))

let info copts json short name_opt () =
  if json && short then
    failwith "Options --json and --short are incompatible." ;
  logger := make_logger copts.debug ;
  Lwt_main.run (
    match%lwt get_program_info ~err_ok:true copts name_opt with
    | exception (Failure _ as e) ->
        (* Maybe we supplied an operation name rather than a program name,
         * try again with only the `dirname` and print only that operation *)
        (match String.rsplit (name_opt |? "") ~by:"/" with
        | exception Not_found -> fail e
        | program, func ->
            (match%lwt get_program_info copts (Some program) with
            | [ program_info ] -> (* Since we ask for a single program *)
                (match List.find (fun n ->
                         n.Info.Func.definition.name = func
                       ) program_info.Info.Program.operations with
                | exception Not_found ->
                    fail_with ("Unknown object '"^ func ^"/"^ program ^"'")
                | func_info ->
                    display_operation_info json short func_info ;
                    return_unit)
            | lst ->
                fail_with (Printf.sprintf "Received %d results"
                             (List.length lst))))
    | info ->
        display_program_info json short info ;
        return_unit)

let test copts conf tests () =
  logger := make_logger copts.debug ;
  Lwt_main.run (
    RamenTests.run conf tests)
