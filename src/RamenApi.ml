(* JSONRPC API to interact with Ramen.
 *
 * Allows to query the running schema, extract timeseries, and create
 * new nodes.
 *)
open Batteries
open Lwt
open RamenLog
open RamenHelpers
open RamenHttpHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

module JSONRPC =
struct
  (* Id will be copied verbatim regardless of the type as long as it's a
   * JS value: *)
  type req = { method_ : string ; id : string ; params : string }

  let err id msg =
    Printf.sprintf "{\"id\":%s,\"error\":%S}" id msg |>
    (* Assuming jsonrpc does not mix transport errors with applicative errors: *)
    respond_ok

  let wrap id f =
    match%lwt f () with
    | exception e ->
        print_exception ~what:("Answering request "^id) e ;
        (match e with
        | Failure msg -> err id msg
        | e -> err id (Printexc.to_string e))
    | s -> respond_ok (Printf.sprintf "{\"id\":%s,\"result\":%s}" id s)

  (* We need to parse this by hand due to dispatching on the "method" field
   * and unspecified type for the "id" field. PPP provides some good helpers
   * though: *)

  (* PPP for the JSONRPC request, returning the params as a verbatim string: *)
  let req_ppp =
    let open PPP in
    let any_json : string t =
      fun () ->
      { printer = (fun o v -> o v) ;
        scanner = (fun i o ->
          match skip_any PPP_JSON.groupings PPP_JSON.delims i o with
          | None ->
              parse_error o "Cannot parse json blurb"
          | Some o' ->
              let str = i o (o'-o) in
              Ok (str, o')) ;
        descr = fun _ -> "some json blurb" }
    in
    PPP_JSON.(record (
      field "method" string <->
      field "id" any_json <->
      field ~default:"" "params" any_json)) >>:
    ((fun { method_ ; id ; params } -> Some (Some method_, Some id), Some params),
     (function Some (Some method_, Some id), Some params -> { method_ ; id ; params }
             | Some (Some method_, Some id), None -> { method_ ; id ; params = "" }
             | _ -> assert false))

  let parse =
    fail_with_context "Parsing JSON" (fun () ->
      PPP.of_string_exc req_ppp)
end

(*
 * Get Ramen version
 *)

type version_resp = string [@@ppp PPP_JSON]

let version () =
  return (PPP.to_string version_resp_ppp_json RamenVersions.release_tag)

(*
 * Get list of available tables which name starts with a given prefix
 *)

type get_tables_req = { prefix : string } [@@ppp PPP_JSON]

type get_tables_resp = string list [@@ppp PPP_JSON]

let get_tables conf msg =
  let req =
    fail_with_context "parsing get-tables request" (fun () ->
      PPP.of_string_exc get_tables_req_ppp_json msg) in
  let%lwt tables =
    C.with_rlock conf (fun programs ->
      Hashtbl.fold (fun _prog_name get_rc lst ->
        match get_rc () with
        | exception e -> lst
        | _bin, prog ->
            List.fold_left (fun lst f ->
              let fqn = RamenName.string_of_fq (F.fq_name f) in
              if f.F.event_time <> None && String.starts_with fqn req.prefix
              then fqn :: lst
              else lst
            ) lst prog.P.funcs
      ) programs [] |> Lwt.return) in
  return (PPP.to_string get_tables_resp_ppp_json tables)

(*
 * Get the schema of a given set of tables.
 * Schema being: list of columns and of threshold-based alerts.
 *)

let empty_units = Hashtbl.create 0

type get_columns_req = string list [@@ppp PPP_JSON]

type get_columns_resp = (string, columns_info) Hashtbl.t [@@ppp PPP_JSON]

and columns_info = (string, column_info) Hashtbl.t [@@ppp PPP_JSON]

and column_info =
  { type_ : string [@ppp_rename "type"] ;
    units : (string, float) Hashtbl.t [@ppp_default empty_units] ;
    factor : bool [@ppp_default false] ;
    alerts : alert_info_v1 list }
  [@@ppp PPP_JSON]

and alert_info_v1 =
  { enabled : bool [@ppp_default true] ;
    where : where_spec list [@ppp_default []] ;
    threshold : float ;
    recovery : float ;
    duration : float ;
    ratio : float [@ppp_default 1.] ;
    (* Unused, for the client purpose only *)
    id : string [@ppp_default ""] }
  [@@ppp PPP_JSON]
  [@@ppp PPP_OCaml]

and where_spec =
  { lhs : string ;
    rhs : string ;
    op : string }
  [@@ppp PPP_JSON]
  [@@ppp PPP_OCaml]

(* Alerts are saved on disc under this format: *)
and alert_source =
  | V1 of alert_info_v1
  (* ... and so on *)
  [@@ppp PPP_OCaml]

type ext_type = Numeric | String | Other

let ext_type_of_typ =
  let open RamenTypes in
  function
  | TString | TEth | TIpv4 | TIpv6 | TIp
  | TCidrv4 | TCidrv6 | TCidr ->
      String
  | x ->
      if is_a_num x then Numeric else Other

let string_of_ext_type = function
  | Numeric -> "numeric"
  | String -> "string"
  | Other -> "other"

let alert_info_of_alert_source enabled = function
  | V1 a -> { a with enabled }

let alerts_of_column conf programs func column =
  (* All files with extension ".alert" in this directory is supposed to be
   * an alert description: *)
  let func_path = F.path func in
  let dir = C.api_alerts_root conf ^"/"^ func_path ^"/"^ column in
  if is_directory dir then
    Sys.readdir dir |>
    Array.fold_left (fun lst f ->
      if String.ends_with f ".alert" then
        match ppp_of_file alert_source_ppp_ocaml (dir ^"/"^ f) with
        | exception e ->
            print_exception ~what:"Error while listing alerts" e ;
            lst
        | a ->
            let id = Filename.remove_extension f in
            let program_name =
              RamenName.program_of_string (func_path ^"/"^ column ^"/"^ id) in
            !logger.debug "Program implementing alert %s: %s"
              id (RamenName.string_of_program program_name) ;
            let enabled = Hashtbl.mem programs program_name in
            alert_info_of_alert_source enabled a :: lst
      else lst
    ) []
  else []

let units_of_column ft =
  match ft.RamenTuple.units with
  | None ->
      (* For simplicity, there is currently no way to distinguish no
       * units from dimensionless: *)
      empty_units
  | Some units ->
      let h = Hashtbl.create 3 in
      RamenUnits.MapUnit.iter (fun n (e, r) ->
        let n = if r then n ^"(rel)" else n in
        Hashtbl.add h n e
      ) units ;
      h

let columns_of_func conf programs func =
  let h = Hashtbl.create 11 in
  List.iter (fun ft ->
    if not (is_private_field ft.RamenTuple.typ_name) then
      let type_ = ext_type_of_typ ft.typ.structure in
      if type_ <> Other then
        let type_ = string_of_ext_type type_
        and factor = List.mem ft.typ_name func.F.factors
        and alerts = alerts_of_column conf programs func ft.typ_name
        and units = units_of_column ft in
        Hashtbl.add h ft.typ_name { type_ ; units ; factor ; alerts }
  ) func.F.out_type ;
  h

let columns_of_table conf table =
  (* A function is what is called here in baby-talk a "table": *)
  let prog_name, func_name = C.program_func_of_user_string table in
  C.with_rlock conf (fun programs ->
    match Hashtbl.find programs prog_name () with
    | exception _ -> return_none
    | _bin, prog ->
        (match List.find (fun f -> f.F.name = func_name) prog.P.funcs with
        | exception Not_found -> return_none
        | func -> return_some (columns_of_func conf programs func)))

let get_columns conf msg =
  let req = fail_with_context "parsing get-columns request" (fun () ->
    PPP.of_string_exc get_columns_req_ppp_json msg) in
  let h = Hashtbl.create 9 in
  Lwt_list.iter_p (fun table ->
    match%lwt columns_of_table conf table with
    | None -> return_unit
    | Some c -> Hashtbl.add h table c ; return_unit
  ) req ;%lwt
  return (PPP.to_string get_columns_resp_ppp_json h)

(*
 * Get a timeseries for a given set of columns.
 *)

type get_timeseries_req =
  { since : float ;
    until : float ;
    num_points : int ;
    data : (string, timeseries_data_spec) Hashtbl.t }
  [@@ppp PPP_JSON]

and timeseries_data_spec =
  { select : string list ;
    where : where_spec list [@ppp_default []] }
  [@@ppp PPP_JSON]

type get_timeseries_resp =
  { times : float array ;
    values : (string, table_values) Hashtbl.t }
  [@@ppp PPP_JSON]

and table_values = (string, float option array) Hashtbl.t
  [@@ppp PPP_JSON]

let get_timeseries conf msg =
  let req = fail_with_context "parsing get-timeseries request" (fun () ->
    PPP.of_string_exc get_timeseries_req_ppp_json msg) in
  let times = Array.make_float req.num_points in
  let times_inited = ref false in
  let values = Hashtbl.create 5 in
  let%lwt () =
    Hashtbl.enum req.data |> List.of_enum |>
    Lwt_list.iter_s (fun (table, data_spec) ->
      let prog_name, func_name = C.program_func_of_user_string table in
      let%lwt filters =
        C.with_rlock conf (fun programs ->
          let _bin, prog = Hashtbl.find programs prog_name () in
          let func = List.find (fun f -> f.F.name = func_name) prog.funcs in
          List.fold_left (fun filters where ->
            if is_private_field where.lhs then
              failwith ("Cannot filter through private field "^ where.lhs) ;
            if where.op = "=" then
              let open RamenSerialization in
              let _, ftyp = find_field func.F.out_type where.lhs in
              let v = value_of_string ftyp.typ where.rhs in
              (where.lhs, v) :: filters
            else filters
          ) [] data_spec.where |> return) in
      let%lwt columns, datapoints =
        RamenTimeseries.get conf req.num_points req.since req.until
                            filters [] table data_spec.select in
      assert (columns = [| |] (* if there was no result *) ||
              columns = [| [] |] (* As we asked for no factors *)) ;
      let num_selected = List.length data_spec.select in
      let table_columns =
        Array.init num_selected (fun _ ->
          Array.create req.num_points None
        ) in
      Enum.iteri (fun ti (t, data) ->
        assert (Array.length data <= 1) ; (* No factors *)
        for ci = 0 to num_selected - 1 do
          table_columns.(ci).(ti) <-
            if Array.length data = 0 then None else data.(0).(ci)
        done ;
        if not !times_inited then times.(ti) <- t
      ) datapoints ;
      times_inited := true ;
      let table_values = Hashtbl.create num_selected in
      List.iteri (fun ci field_name ->
        Hashtbl.add table_values field_name table_columns.(ci)
      ) data_spec.select ;
      Hashtbl.add values table table_values ;
      return_unit
    ) in
  let resp = { times ; values } in
  return (PPP.to_string get_timeseries_resp_ppp_json resp)

(*
 * Save the alerts
 *)

type set_alerts_req =
  (string, (string, alert_info_v1 list) Hashtbl.t) Hashtbl.t
  [@@ppp PPP_JSON]

(* Alert ids are used to uniquely identify alerts (for instance when
 * saving on disc). Alerts are identified by their defining properties,
 * which exclude "enabled" or other similar meta info, should we have
 * some. This is not to be confused with the field "id" from alert_info,
 * which is the id for the user and that, as far as we are concerned, need
 * not even be unique. But we have to save this user id as well even when
 * its the only thing that changed so it's easier to make it part of this
 * hash. *)
let alert_id column =
  let print_where_spec oc w =
    Printf.fprintf oc "(%s %s %s)" w.op w.lhs w.rhs in
  function
  | V1 { threshold ; where ; recovery ; duration ; ratio ; id ; _ } ->
      (Legacy.Printf.sprintf "%s-%h-%h-%h-%h-%s"
         column threshold recovery duration ratio id ^
       Printf.sprintf2 "-%a"
         (List.print ~first:"" ~last:"" ~sep:"-" print_where_spec)
          where) |> md5

let is_enabled = function
  | V1 { enabled ; _ } -> enabled

let generate_alert table ft =
  let column = ft.RamenTuple.typ_name
  and nullable = ft.typ.RamenTypes.nullable in
  IO.to_string (fun oc alert_info ->
    Printf.fprintf oc
      "-- Alert automatically generated by ramen %s at %s\n\n"
      RamenVersions.release_tag (ctime ()) ;
    match alert_info with
    | V1 a ->
        Printf.fprintf oc "DEFINE alert AS\n" ;
        Printf.fprintf oc "  FROM %s\n" (ramen_quote table) ;
        if a.where <> [] then
          List.print ~first:"  WHERE " ~sep:" AND " ~last:"\n"
            (fun oc w ->
              Printf.fprintf oc "(%s %s %s)" w.lhs w.op w.rhs)
            oc a.where ;
        Printf.fprintf oc "  SELECT %s\n"
          (if nullable then "COALESCE(" else "") ;
        Printf.fprintf oc "    HYSTERESIS (%s, %f, %f)%s AS firing\n"
          column a.recovery a.threshold
          (if nullable then ", false)" else "") ;
        Printf.fprintf oc "  NOTIFY \"%s is off!\" WITH\n" column ;
        Printf.fprintf oc "    firing AS firing,\n" ;
        Printf.fprintf oc "    %sreldiff(%s, %f)%s AS certainty,\n"
          (if nullable then "COALESCE(" else "")
          column a.threshold
          (if nullable then ", 0)" else "") ;
        Printf.fprintf oc "    %s AS values,\n" column ;
        Printf.fprintf oc "    %f AS thresholds\n" a.threshold ;
        (* TODO: a way to add zone, service, etc, if present in the
         * parent table *)
        Printf.fprintf oc "  AND KEEP ALL\n" ;
        Printf.fprintf oc "  AFTER firing <> COALESCE(previous.firing, false)\n")

let compile_alert conf programs program_name program_code bin_fname =
  (* Even though we do not look for parents in there we still need a
   * root_path where to write binaries files to: *)
  let root_path = RamenConf.api_alerts_root conf in
  let get_parent = RamenCompiler.parent_from_programs programs in
  RamenCompiler.compile conf root_path get_parent program_name program_code

let run_alert conf bin_fname =
  RamenRun.run conf [] true bin_fname

let stop_alert conf program_name =
  let glob =
    Globs.(RamenName.string_of_program program_name |> escape |> compile) in
  let%lwt num_kills = RamenRun.kill conf [ glob ] in
  if num_kills < 0 || num_kills > 1 then
    !logger.error "When attempting to kill alert %s, got num_kill = %d"
      (RamenName.string_of_program program_name) num_kills ;
  return_unit

let field_typ_of_column programs table column =
  let pn, fn = C.program_func_of_user_string table in
  match Hashtbl.find programs pn () with
  | exception Not_found ->
      Printf.sprintf "Program %s does not exist"
        (RamenName.string_of_program pn) |>
      failwith
  | _bin, prog ->
      (match List.find (fun f -> f.F.name = fn) prog.P.funcs with
      | exception Not_found ->
          Printf.sprintf "No function %s in program %s"
            (RamenName.string_of_func fn)
            (RamenName.string_of_program pn) |>
          failwith
      | func ->
          let open RamenTuple in
          try
            List.find (fun t -> t.typ_name = column) func.F.out_type
          with Not_found ->
            Printf.sprintf "No column %s in table %s" column table |>
            failwith)

let save_alert conf table column to_keep alert_info =
  let id = alert_id column alert_info in
  let program_name = table ^"/"^ column ^"/"^ id in
  to_keep := Set.String.add program_name !to_keep ;
  let program_name = RamenName.program_of_string program_name in
  let basename =
    C.api_alerts_root conf ^"/"^ RamenName.path_of_program program_name in
  let conf_fname = basename ^".alert"
  and tmp_fname = basename ^".tmp"
  and bin_fname = basename ^".x" in
  (* If that file is already there, assume that's the same and do nothing.
   * That the program is running would tell us that it exists and is
   * enabled, but that the program is not running wouldn't tell us that
   * the alert does not exist. *)
  if file_exists ~maybe_empty:false conf_fname then return_unit else (
    !logger.info "Saving new alert into %s" conf_fname ;
    (* But do not create this file yet so that we only report that the
     * alert is present unless it's indeed running. *)
    ppp_to_file tmp_fname alert_source_ppp_ocaml alert_info ;
    C.with_rlock conf (fun programs ->
      let%lwt ft = wrap (fun () ->
        field_typ_of_column programs table column) in
      if ext_type_of_typ ft.RamenTuple.typ.structure <> Numeric then
        Printf.sprintf "Column %s of table %s is not numeric" column table |>
        fail_with
      else return_unit ;%lwt
      let program_code = generate_alert table ft alert_info in
      !logger.info "Alert code:\n%s" program_code ;
      wrap (fun () ->
        compile_alert conf programs program_name program_code bin_fname)) ;%lwt
    Lwt_unix.rename tmp_fname conf_fname) ;%lwt
  if is_enabled alert_info then
    (* Won't do anything if it's running already *)
    run_alert conf bin_fname
  else
    (* Won't do anything if it's not running *)
    stop_alert conf program_name

let set_alerts conf msg =
  let req = fail_with_context "parsing set-alerts request" (fun () ->
    PPP.of_string_exc set_alerts_req_ppp_json msg) in
  (* In case the same table/column appear several times, build a single list
   * of all preexisting alert files for the mentioned tables/columns, and a
   * list of all that are set: *)
  let to_delete = ref Set.String.empty
  and to_keep = ref Set.String.empty in
  hash_iter_s req (fun table columns ->
    hash_iter_s columns (fun column alerts ->
      (* All non listed alerts must be suppressed *)
      let parent =
        RamenName.program_of_string (table ^"/"^ column) in
      let dir =
        C.api_alerts_root conf ^"/"^
        RamenName.path_of_program parent in
      if is_directory dir then (
        Sys.readdir dir |>
        Array.iter (fun f ->
          if String.ends_with f ".alert" then
            let id = Filename.remove_extension f in
            let program_name =
              RamenName.string_of_program parent ^"/"^ id in
            to_delete := Set.String.add program_name !to_delete)) ;
      Lwt_list.iter_s (fun alert ->
        (* We receive only the latest version: *)
        save_alert conf table column to_keep (V1 alert)
      ) alerts)) ;%lwt
  let to_delete = Set.String.diff !to_delete !to_keep in
  if Set.String.is_empty to_delete then return_unit else (
    !logger.info "going to delete non mentioned alerts %a"
      (Set.String.print String.print) to_delete ;
    Set.String.to_list to_delete |>
    Lwt_list.iter_s (fun program_name ->
      let program_name = RamenName.program_of_string program_name in
      stop_alert conf program_name ;%lwt
      let fname =
        C.api_alerts_root conf ^"/"^
        RamenName.(path_of_program program_name)
        ^".alert" in
      Lwt_unix.unlink fname)) ;%lwt
  (* Delete to_delete - to_keep *)
  return ""

(*
 * Dispatch queries
 *)

let router conf prefix =
  (* The function called for each HTTP request: *)
  fun meth path params headers body ->
    !logger.info "meth=%S, path=%a"
      (Cohttp.Code.string_of_method meth)
      (List.print String.print) path ;
    let%lwt prefix =
      wrap (fun () -> list_of_prefix prefix) in
    let path = chop_prefix prefix path in
    if path <> [] then fail BadPrefix
    else
      let open JSONRPC in
      let req = parse body in
      wrap req.id (fun () ->
        match String.lowercase_ascii req.method_ with
        | "version" -> version ()
        | "get-tables" -> get_tables conf req.params
        | "get-columns" -> get_columns conf req.params
        | "get-timeseries" -> get_timeseries conf req.params
        | "set-alerts" -> set_alerts conf req.params
        | m -> fail_with (Printf.sprintf "unknown method %S" m))
