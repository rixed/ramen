(* JSONRPC API to interact with Ramen.
 *
 * Allows to query the running schema, extract timeseries, and create
 * new nodes.
 *)
open Batteries
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
    (* FIXME: strip ANSI sequences from [msg] *)
    Printf.sprintf "{\"id\":%s,\"error\":%s}"
      id (PPP_JSON.json_encoded_string msg) |>
    (* Assuming jsonrpc does not mix transport errors with applicative errors: *)
    http_msg

  let wrap id f =
    match f () with
    | exception e ->
        print_exception ~what:("Answering request "^id) e ;
        err id (match e with
          | Failure msg -> msg
          | e -> Printexc.to_string e)
    | s ->
        Printf.sprintf "{\"id\":%s,\"result\":%s}" id s |>
        http_msg

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
  PPP.to_string version_resp_ppp_json RamenVersions.release_tag

(*
 * Get list of available tables which name starts with a given prefix
 *)

type get_tables_req = { prefix : string } [@@ppp PPP_JSON]

type get_tables_resp = (string, string) Hashtbl.t [@@ppp PPP_JSON]

let get_tables conf msg =
  let req =
    fail_with_context "parsing get-tables request" (fun () ->
      PPP.of_string_exc get_tables_req_ppp_json msg) in
  let tables = Hashtbl.create 31 in
  C.with_rlock conf (fun programs ->
    Hashtbl.iter (fun _prog_name (mre, get_rc) ->
      if not mre.C.killed then match get_rc () with
      | exception _ -> ()
      | prog ->
          List.iter (fun f ->
            let fqn = RamenName.string_of_fq (F.fq_name f) in
            if f.F.event_time <> None && String.starts_with fqn req.prefix
            then Hashtbl.add tables fqn f.F.doc
          ) prog.P.funcs
    ) programs) ;
  PPP.to_string get_tables_resp_ppp_json tables

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
    doc : string [@ppp_default ""] ;
    factor : bool [@ppp_default false] ;
    alerts : alert_info_v1 list }
  [@@ppp PPP_JSON]

and alert_info_v1 =
  { enabled : bool [@ppp_default true] ;
    where : where_spec list [@ppp_default []] ;
    threshold : float ;
    recovery : float ;
    duration : float [@ppp_default 0.] ;
    ratio : float [@ppp_default 1.] ;
    (* Unused, for the client purpose only *)
    id : string [@ppp_default ""] ;
    (* Desc to use when firing/recovering: *)
    desc_firing : string [@ppp_default ""] ;
    desc_recovered : string [@ppp_default ""] }
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
  | V1 of { table : string ; column : string ; alert : alert_info_v1 }
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
  | V1 { alert ; _ } -> { alert with enabled }

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
        Hashtbl.add h ft.typ_name {
          type_ = string_of_ext_type type_ ;
          units = units_of_column ft ;
          doc = ft.doc ;
          factor = List.mem ft.typ_name func.F.factors ;
          alerts = alerts_of_column conf programs func ft.typ_name }
  ) func.F.out_type ;
  h

let columns_of_table conf table =
  (* A function is what is called here in baby-talk a "table": *)
  let prog_name, func_name =
    RamenName.(fq_of_string table |> fq_parse) in
  C.with_rlock conf (fun programs ->
    match Hashtbl.find programs prog_name with
    | exception _ -> None
    | _mre, get_rc ->
      (match get_rc () with
      | exception _ -> None
      | prog ->
          (match List.find (fun f -> f.F.name = func_name) prog.P.funcs with
          | exception Not_found -> None
          | func -> Some (columns_of_func conf programs func))))

let get_columns conf msg =
  let req = fail_with_context "parsing get-columns request" (fun () ->
    PPP.of_string_exc get_columns_req_ppp_json msg) in
  let h = Hashtbl.create 9 in
  List.iter (fun table ->
    match columns_of_table conf table with
    | None -> ()
    | Some c -> Hashtbl.add h table c
  ) req ;
  PPP.to_string get_columns_resp_ppp_json h

(*
 * Get a timeseries for a given set of columns.
 *)

type get_timeseries_req =
  { since : float ;
    until : float ;
    num_points : int [@ppp_default 100] ;
    data : (string, timeseries_data_spec) Hashtbl.t }
  [@@ppp PPP_JSON]

and timeseries_data_spec =
  { select : string list ;
    where : where_spec list [@ppp_default []] ;
    factors : string list [@ppp_default []] }
  [@@ppp PPP_JSON]

type get_timeseries_resp =
  { times : float array ;
    values : (string (* table *), table_values) Hashtbl.t }
  [@@ppp PPP_JSON]

and table_values =
  { column_labels : string list array ;
    (* One optional float per selected field, per label, per time: *)
    column_values : float option array array array }
  [@@ppp PPP_JSON]

let get_timeseries conf msg =
  let req = fail_with_context "parsing get-timeseries request" (fun () ->
    PPP.of_string_exc get_timeseries_req_ppp_json msg) in
  let times = Array.make_float req.num_points in
  let times_inited = ref false in
  let values = Hashtbl.create 5 in
  Hashtbl.iter (fun table data_spec ->
    let fq = RamenName.fq_of_string table in
    let prog_name, func_name = RamenName.fq_parse fq in
    let filters =
      C.with_rlock conf (fun programs ->
        let _mre, get_rc = Hashtbl.find programs prog_name in
        let prog = get_rc () in
        let func = List.find (fun f -> f.F.name = func_name) prog.funcs in
        List.fold_left (fun filters where ->
          if is_private_field where.lhs then
            failwith ("Cannot filter through private field "^ where.lhs) ;
          let open RamenSerialization in
          let _, ftyp = find_field func.F.out_type where.lhs in
          let v = value_of_string ftyp.typ where.rhs in
          (where.lhs, where.op, v) :: filters
        ) [] data_spec.where) in
    let column_labels, datapoints =
      RamenTimeseries.get conf req.num_points req.since req.until
                          filters data_spec.factors fq data_spec.select in
    (* [column_labels] is an array of labels (empty if no result).
     * Each label is a list of factors values. *)
    let column_labels =
      Array.map (List.map RamenTypes.to_string) column_labels in
    let column_values = Array.create req.num_points [||] in
    Hashtbl.add values table { column_labels ; column_values } ;
    Enum.iteri (fun ti (t, data) ->
      (* in data we have one column per column-label: *)
      assert (Array.length data = Array.length column_labels) ;
      column_values.(ti) <- data ;
      if not !times_inited then times.(ti) <- t
    ) datapoints ;
    times_inited := true
  ) req.data ;
  let resp = { times ; values } in
  PPP.to_string get_timeseries_resp_ppp_json resp

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
  | V1 { alert = { threshold ; where ; recovery ; duration ; ratio ; id ; _ } ;
         _ } ->
      (Legacy.Printf.sprintf "V1-%s-%h-%h-%h-%h-%s"
         column threshold recovery duration ratio id ^
       Printf.sprintf2 "-%a"
         (List.print ~first:"" ~last:"" ~sep:"-" print_where_spec)
           where) |> md5

let is_enabled = function
  | V1 { alert = { enabled ; _ } ; _ } -> enabled

let field_typ_of_column programs table column =
  let pn, fn =
    RamenName.(fq_of_string table |> fq_parse) in
  match Hashtbl.find programs pn with
  | exception Not_found ->
      Printf.sprintf "Program %s does not exist"
        (RamenName.string_of_program pn) |>
      failwith
  | _mre, get_rc ->
      let prog = get_rc () in
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

let generate_alert programs src_file (V1 { table ; column ; alert = a }) =
  let ft = field_typ_of_column programs table column in
  let nullable = ft.typ.RamenTypes.nullable in
  File.with_file_out ~mode:[`create; `text ; `trunc] src_file (fun oc ->
    Printf.fprintf oc
      "-- Alert automatically generated by ramen %s at %s\n\n"
      RamenVersions.release_tag (ctime (Unix.time ())) ;
    (* TODO: if this sticks make the URL a parameter: *)
    let desc_link =
      Printf.sprintf
        "See https://${env.HOSTNAME}/view_alert?id=%s for details"
        (CodecUrl.encode a.id) in
    let desc_firing =
      if a.desc_firing <> "" then a.desc_firing else
        Printf.sprintf "%s went %s the configured threshold %f.\n%s"
          column
          (if a.threshold >= a.recovery then "above" else "below")
          a.threshold
          desc_link
    and desc_recovered =
      if a.desc_recovered <> "" then a.desc_recovered else
        Printf.sprintf "The value of %s recovered.\n%s"
          column
          desc_link
    in
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
    Printf.fprintf oc "    1 AS certainty,\n" ;
    Printf.fprintf oc "    %s AS values,\n" column ;
    Printf.fprintf oc "    %f AS thresholds,\n" a.threshold ;
    Printf.fprintf oc "    (IF firing THEN %S ELSE %S) AS desc\n"
      desc_firing desc_recovered ;
    (* TODO: a way to add zone, service, etc, if present in the
     * parent table *)
    Printf.fprintf oc "  AND KEEP ALL\n" ;
    Printf.fprintf oc "  AFTER firing <> COALESCE(previous.firing, false)\n")

(* Register a rule to turn an alert into a ramen source file: *)
let () =
  RamenMake.register "alert" "ramen"
    RamenMake.target_is_older
    (fun conf _prog_name src_file target_file ->
      let a = ppp_of_file alert_source_ppp_ocaml src_file in
      C.with_rlock conf (fun programs ->
        generate_alert programs target_file a))

let compile_alert conf programs program_name src_file =
  let get_parent = RamenCompiler.parent_from_programs programs in
  RamenCompiler.compile conf get_parent src_file program_name

let stop_alert conf program_name =
  let glob =
    Globs.(RamenName.string_of_program program_name |> escape |> compile) in
  let num_kills = RamenRun.kill conf [ glob ] in
  if num_kills < 0 || num_kills > 1 then
    !logger.error "When attempting to kill alert %s, got num_kill = %d"
      (RamenName.string_of_program program_name) num_kills

let same_file_exist fname new_alert =
  file_exists fname &&
  match ppp_of_file alert_source_ppp_ocaml fname with
  | exception e ->
      print_exception ~what:"Error while comparing with previous alert" e ;
      false
  | old_alert -> old_alert = new_alert

let save_alert conf program_name alert_info =
  let program_name = RamenName.program_of_string program_name in
  let basename =
    C.api_alerts_root conf ^"/"^ RamenName.path_of_program program_name in
  let src_file = basename ^".alert" in
  (* Avoid triggering a recompilation if it's unchanged: *)
  if same_file_exist src_file alert_info then
    !logger.debug "Alert %s preexist with same definition" src_file
  else (
    !logger.info "Saving new alert into %s" src_file ;
    ppp_to_file ~pretty:true src_file alert_source_ppp_ocaml alert_info ;
    if is_enabled alert_info then (
      (* Compile right now so that we can report errors to the client and RamenRun.run
       * can check linkage errors: *)
      let exec_file = basename ^".x" in
      RamenMake.build conf program_name src_file exec_file ;
      let debug = conf.C.log_level = Debug in
      let params = Hashtbl.create 0 in
      RamenRun.run conf params true RamenConsts.Default.report_period
                   program_name ~src_file exec_file debug
    ) else
      (* Won't do anything if it's not running *)
      stop_alert conf program_name)

let set_alerts conf msg =
  let req = fail_with_context "parsing set-alerts request" (fun () ->
    PPP.of_string_exc set_alerts_req_ppp_json msg) in
  (* In case the same table/column appear several times, build a single list
   * of all preexisting alert files for the mentioned tables/columns, and a
   * list of all that are set: *)
  let old_alerts = ref Set.String.empty
  and new_alerts = ref Set.String.empty in
  Hashtbl.iter (fun table columns ->
    Hashtbl.iter (fun column alerts ->
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
            old_alerts:= Set.String.add program_name !old_alerts)) ;
      List.iter (fun alert ->
        (* Check the alert: *)
        C.with_rlock conf (fun programs ->
          let ft = field_typ_of_column programs table column in
          if ext_type_of_typ ft.RamenTuple.typ.structure <> Numeric then
            Printf.sprintf "Column %s of table %s is not numeric"
              column table |>
            failwith) ;
        (* We receive only the latest version: *)
        let alert_source = V1 { table ; column ; alert } in
        let id = alert_id column alert_source in
        let program_name = table ^"/"^ column ^"/"^ id in
        new_alerts := Set.String.add program_name !new_alerts ;
        save_alert conf program_name alert_source
      ) alerts
    ) columns
  ) req ;
  let to_delete = Set.String.diff !old_alerts !new_alerts in
  if not (Set.String.is_empty to_delete) then (
    !logger.info "going to delete non mentioned alerts %a"
      (Set.String.print String.print) to_delete ;
    Set.String.iter (fun program_name ->
      let program_name = RamenName.program_of_string program_name in
      stop_alert conf program_name ;
      let fname =
        C.api_alerts_root conf ^"/"^
        RamenName.(path_of_program program_name)
        ^".alert" in
      Unix.unlink fname
    ) to_delete)

(*
 * Dispatch queries
 *)

let router conf prefix =
  (* The function called for each HTTP request: *)
  fun _meth path _params _headers body ->
    let prefix = list_of_prefix prefix in
    let path = chop_prefix prefix path in
    if path <> [] then raise BadPrefix
    else
      let open JSONRPC in
      let req = parse body in
      wrap req.id (fun () ->
        match String.lowercase_ascii req.method_ with
        | "version" -> version ()
        | "get-tables" -> get_tables conf req.params
        | "get-columns" -> get_columns conf req.params
        | "get-timeseries" -> get_timeseries conf req.params
        | "set-alerts" -> set_alerts conf req.params ; "null"
        | m -> failwith (Printf.sprintf "unknown method %S" m))
