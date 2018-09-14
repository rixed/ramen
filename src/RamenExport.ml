open Batteries
open Stdint
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

exception FuncHasNoEventTimeInfo of string
let () =
  Printexc.register_printer (function
    | FuncHasNoEventTimeInfo n -> Some (
      Printf.sprintf "Function %S has no event-time information" n)
    | _ -> None)

(* Returns the buffer name: *)
let make_temp_export ?duration conf func =
  let bname = C.archive_buf_name conf func in
  RingBuf.create ~wrap:false bname ;
  (* Add that name to the function out-ref *)
  let out_ref = C.out_ringbuf_names_ref conf func in
  let ser =
    RingBufLib.ser_tuple_typ_of_tuple_typ func.F.out_type in
  let file_spec =
    RamenOutRef.{
      field_mask =
        RingBufLib.skip_list ~out_type:ser ~in_type:ser ;
      timeout = match duration with
                | None -> 0.
                | Some d -> Unix.gettimeofday () +. d } in
  RamenOutRef.add out_ref (bname, file_spec) ;
  bname

(* Returns the func, and the buffer name: *)
let make_temp_export_by_name conf ?duration fq =
  let program_name, func_name = RamenName.fq_parse fq in
  C.with_rlock conf (fun programs ->
    match C.find_func programs program_name func_name with
    | exception Not_found ->
        failwith ("Function "^
                  RamenName.string_of_program program_name ^"/"^
                  RamenName.string_of_func func_name ^" does not exist")
    | prog, func ->
        let bname = make_temp_export conf ?duration func in
        prog, func, bname)

(* Some ringbuf are always available and their type known:
 * instrumentation, notifications. *)
let read_well_known fq where suffix bname typ () =
  let fq_str = RamenName.string_of_fq fq in
  if fq_str = suffix || String.ends_with fq_str suffix then
    (* For well-known tuple types, serialized tuple is as given (no
     * private fields, no reordering of fields): *)
    let ser = typ in
    let where =
      if fq_str = suffix then where else
      let fq = String.rchop ~n:(String.length suffix) fq_str in
      ("worker", "=", RamenTypes.VString fq) :: where in
    let filter = RamenSerialization.filter_tuple_by ser where in
    Some (bname, filter, typ, ser)
  else None

let read_output conf ?duration fq where =
  (* Read directly from the instrumentation ringbuf when fq ends
   * with "#stats": *)
  match read_well_known fq where "#stats"
          (C.report_ringbuf conf) RamenBinocle.tuple_typ () with
  | Some (bname, filter, typ, ser) ->
      bname, filter, typ, ser, [], RamenBinocle.event_time
  | None ->
      (* Or from the notifications ringbuf when fq ends with
       * "#notifs": *)
      (match read_well_known fq where "#notifs"
               (C.notify_ringbuf conf) RamenNotification.tuple_typ () with
      | Some (bname, filter, typ, ser) ->
          bname, filter, typ, ser, [], RamenNotification.event_time
      | None ->
          (* Normal case: Create the non-wrapping RingBuf (under a standard
           * name given by RamenConf *)
          let prog, func, bname =
            make_temp_export_by_name conf ?duration fq in
          let ser =
            RingBufLib.ser_tuple_typ_of_tuple_typ func.F.out_type in
          let filter = RamenSerialization.filter_tuple_by ser where in
          bname, filter, func.F.out_type, ser, prog.P.params,
          func.F.event_time)
