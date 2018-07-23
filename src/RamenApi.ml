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
    | exception Failure msg -> err id msg
    | exception e -> err id (Printexc.to_string e)
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

  let parse = PPP.of_string_exc req_ppp
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
  let req = PPP.of_string_exc get_tables_req_ppp_json msg in
  let%lwt tables =
    C.with_rlock conf (fun programs ->
      Hashtbl.fold (fun _prog_name get_rc lst ->
        let _bin, prog = get_rc () in
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

type get_columns_req = string list [@@ppp PPP_JSON]

type get_columns_resp = (string, columns_info) Hashtbl.t [@@ppp PPP_JSON]

and columns_info = (string, column_info) Hashtbl.t [@@ppp PPP_JSON]

and column_info =
  { type_ : string [@ppp_rename "type"] ;
    factor : bool [@ppp_default false] ;
    alerts : alert_info list }
  [@@ppp PPP_JSON]

and alert_info =
  { enabled : bool ;
    threshold : float ;
    recovery : float ;
    duration : float ;
    ratio : float }
  [@@ppp PPP_JSON]

type ext_type = Numeric | String | Other

let ext_type_of_typ =
  let open RamenTypes in
  function
  | TFloat | TBool
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128 ->
      Numeric
  | TString | TEth | TIpv4 | TIpv6 | TIp
  | TCidrv4 | TCidrv6 | TCidr ->
      String
  | _ ->
      Other

let string_of_ext_type = function
  | Numeric -> "numeric"
  | String -> "string"
  | Other -> "other"

let columns_of_func func =
  let h = Hashtbl.create 11 in
  List.iter (fun ft ->
    let type_ = ext_type_of_typ ft.RamenTuple.typ.structure in
    if type_ <> Other then
      let type_ = string_of_ext_type type_
      and factor = List.mem ft.typ_name func.F.factors
      and alerts = [] (* TODO *) in
      Hashtbl.add h ft.typ_name { type_ ; factor ; alerts }
  ) func.F.out_type.ser ;
  h

let columns_of_table conf table =
  (* A function is what is called here in baby-talk a "table": *)
  let prog_name, func_name = C.program_func_of_user_string table in
  match%lwt C.with_rlock conf (fun programs ->
      match Hashtbl.find programs prog_name with
      | exception Not_found -> return_none
      | get_rc ->
          let _bin, prog = get_rc () in
          (match List.find (fun f -> f.F.name = func_name) prog.P.funcs with
          | exception Not_found -> return_none
          | func -> return_some func)) with
  | None -> return_none
  | Some func -> return_some (columns_of_func func)

let get_columns conf msg =
  let req = PPP.of_string_exc get_columns_req_ppp_json msg in
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
    nb_points : int ;
    data : (string, timeseries_data_spec) Hashtbl.t }
  [@@ppp PPP_JSON]

and timeseries_data_spec =
  { select : string list ;
    where : where_spec list }
  [@@ppp PPP_JSON]

and where_spec =
  { lhs : string ;
    rhs : string ;
    op : string }
  [@@ppp PPP_JSON]

type get_timeseries_resp =
  { times : float array ;
    values : (string, table_values) Hashtbl.t }
  [@@ppp PPP_JSON]

and table_values = (string, float option array) Hashtbl.t
  [@@ppp PPP_JSON]

let get_timeseries conf msg =
  let req = PPP.of_string_exc get_timeseries_req_ppp_json msg in
  let times = Array.make_float req.nb_points in
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
            if where.op = "=" then
              let open RamenSerialization in
              let _, ftyp = find_field func.F.out_type.ser where.lhs in
              let v = value_of_string ftyp.typ where.rhs in
              (where.lhs, v) :: filters
            else filters
          ) [] data_spec.where |> return) in
      let%lwt columns, datapoints =
        RamenTimeseries.get conf req.nb_points req.since req.until
                            filters [] table data_spec.select in
      assert (columns = [| |] (* if there was no result *) ||
              columns = [| [] |] (* As we asked for no factors *)) ;
      let nb_selected = List.length data_spec.select in
      let table_columns =
        Array.init nb_selected (fun _ ->
          Array.create req.nb_points None
        ) in
      Enum.iteri (fun ti (t, data) ->
        assert (Array.length data = 1) ; (* No factors *)
        for ci = 0 to nb_selected - 1 do
          table_columns.(ci).(ti) <- data.(0).(ci)
        done ;
        if not !times_inited then times.(ti) <- t
      ) datapoints ;
      times_inited := true ;
      let table_values = Hashtbl.create nb_selected in
      List.iteri (fun ci field_name ->
        Hashtbl.add table_values field_name table_columns.(ci)
      ) data_spec.select ;
      Hashtbl.add values table table_values ;
      return_unit
    ) in
  let resp = { times ; values } in
  return (PPP.to_string get_timeseries_resp_ppp_json resp)

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
        | m -> fail_with (Printf.sprintf "unknown method %S" m))
