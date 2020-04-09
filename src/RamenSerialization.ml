open Batteries
open RingBuf
open RingBufLib
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
module T = RamenTypes
module E = RamenExpr
module N = RamenName
open RamenTypes

let verbose_serialization = false

(* Read all fields one by one. Not the real thing.
 * Slow unserializer used for command line tools such as `ramen tail`. *)
let read_array_of_values tuple_typ =
  (* TODO: RingBufLib.ser_tuple_typ_of_tuple_typ tuple_typ
   * for now caller must know the ser type as some types are special
   * (instrumentation, well known etc). FIXME. *)
  let ser_tuple_typ = tuple_typ in
  let tuple_len = List.length ser_tuple_typ in
  let nullmask_size = nullmask_bytes_of_tuple_type ser_tuple_typ in
  fun tx start_offs ->
    if verbose_serialization then
      !logger.debug "De-serializing a tuple of type %a with nullmask of %d bytes"
        RamenTuple.print_typ ser_tuple_typ
        nullmask_size ;
    let tuple = Array.make tuple_len VNull in
    List.fold_lefti (fun (offs, b) i typ ->
      let value, offs', b' =
        if typ.RamenTuple.typ.nullable &&
           not (get_bit tx start_offs b)
        then (
          None, offs, b+1
        ) else (
          let value = RingBufLib.read_value tx offs typ.typ.structure in
          if verbose_serialization then
            !logger.debug "Importing a single value for %a at offset %d: %a"
              RamenTuple.print_field_typ typ
              offs T.print value ;
          let offs' = offs + RingBufLib.sersize_of_value value in
          Some value, offs', if typ.typ.nullable then b+1 else b
        ) in
      Option.may (Array.set tuple i) value ;
      offs', b'
    ) (start_offs + nullmask_size, 0) ser_tuple_typ |> ignore ;
    tuple

let read_tuple unserialize tx =
  match read_message_header tx 0 with
  | EndOfReplay _ as m -> m, None
  | DataTuple _ as m ->
      let tuple = unserialize tx (message_header_sersize m) in
      m, Some tuple

(* Same as above but returns directly a tuple rather than an array of
 * RamenTypes.values: *)
let read_tuples ?while_ unserialize rb f =
  read_ringbuf ?while_ rb (fun tx ->
    match read_tuple unserialize tx with
    | exception e ->
        print_exception ~what:"reading a tuple" e ;
        dequeue_commit tx
    | msg ->
        dequeue_commit tx ;
        f msg)

let read_notifs ?while_ rb f =
  (* Ignore all notifications but on live channel. *)
  read_tuples ?while_ RamenNotificationSerialization.unserialize rb (function
    | DataTuple chan, Some notif
      when chan = RamenChannel.live ->
        f notif
    | _ -> ())

let print_value_with_type oc v =
  Printf.fprintf oc "%a of type %a"
    T.print v
    T.print_structure (structure_of v)

let value_of_string t s =
  let rec equivalent_types t1 t2 =
    match t1.structure, t2.structure with
    | T.TVec (_, t1), T.TList t2 ->
        equivalent_types t1 t2
    | s1, s2 ->
        can_enlarge ~from:s1 ~to_:s2 in
  let open RamenParsing in
  (* First parse the string as any immediate value: *)
  let p = allow_surrounding_blanks (T.Parser.p_ ~min_int_width:8) in
  let stream = stream_of_string s in
  match p ["value"] None Parsers.no_error_correction stream |>
        to_result with
  | Error ((NoSolution _ | Approximation _) as e) ->
      let msg =
        Printf.sprintf2 "Cannot parse %S: %a"
          s (print_bad_result print) e in
      failwith msg
  | Ok (v, _s (* no rest since we ends with eof *)) ->
      if v = VNull && t.nullable then v else
      let vt = structure_of v in
      if equivalent_types T.(make vt) t then
        T.enlarge_value t.structure v else
      let msg =
        Printf.sprintf2 "%S has type %a instead of expected %a"
          s T.print_structure vt T.print_structure t.structure in
      failwith msg
  | Error (Ambiguous lst) ->
      match List.filter (fun (v, _c, _s) ->
              equivalent_types T.(make (structure_of v)) t
            ) lst |>
            List.unique_hash with
      | [] ->
          let msg =
            Printf.sprintf2 "%S could have type %a but not the expected %a"
              s
              (List.print ~first:"" ~last:"" ~sep:" or "
                (fun oc (v, _c, _s) ->
                  T.print_structure oc (structure_of v))) lst
              T.print_typ t in
          failwith msg
      | [v, _, _] ->
          T.enlarge_value t.structure v
      | lst ->
          let msg =
            Printf.sprintf2 "%S is ambiguous: is it %a?"
              s
              (List.print ~first:"" ~last:"" ~sep:", or "
                (fun oc (v, _, _) -> print_value_with_type oc v)) lst in
          failwith msg

(*$inject open Stdint *)
(*$= value_of_string & ~printer:(BatIO.to_string print_value_with_type)
  (VString "glop") \
    (value_of_string { structure = TString ; nullable = false } "\"glop\"")
  (VString "glop") \
    (value_of_string { structure = TString ; nullable = false } " \"glop\"  ")
  (VU16 (Uint16.of_int 15042)) \
    (value_of_string { structure = TU16 ; nullable = false }    "15042")
  (VU32 (Uint32.of_int 15042)) \
    (value_of_string { structure = TU32 ; nullable = false }    "15042")
  (VI64 (Int64.of_int  15042)) \
    (value_of_string { structure = TI64 ; nullable = false }    "15042")
  (VFloat 15042.) \
    (value_of_string { structure = TFloat ; nullable = false }  "15042")
  VNull \
    (value_of_string { structure = TFloat ; nullable = true }   "null")
  (VList [| VFloat 0.; VFloat 1.; VFloat 2. |]) \
    (value_of_string { structure = TList { structure = TFloat ;\
                                           nullable = false } ;\
                       nullable = true } "[ 0; 1; 2]")
  (VI32 239l) \
    (value_of_string { structure = TList { structure = TI16 ;\
                                           nullable = false } ;\
                       nullable = true } \
      "[98;149;86;143;1;124;82;2;139;70;175;197;95;79;63;112;7;45;46;30;\
        61;18;148;23;26;74;87;81;147;144;146;11;25;32;43;56;3;4;39;88;20;\
        5;17;49;106;9;12;13;14;8;41;68;94;69;33;99;42;50;137;141;108;96;\
        53;67;71;142;133;128;118;120;131;130;129;132;113;24;154;214;213;\
        237;169;177;191;172;233;221;200;173;236;153;192;183;199;195;166;\
        189;220;196;216;163;193;224;194;179;186;126;215;217;168;229;225;\
        160;171;207;180;151;223;212;235;165;187;208;201;182;159;198;203;\
        155;218;157;209;211;161;188;206;210;205;227;238;164;178;231;226;\
        134;185;184;119;190;174;162;228;167;152;170;181;204;158;232;150;\
        222;156;219;234;117;97;55;58;138;65;85;0;83;38;114;90;36;48;37;122;\
        121;140;127;136;52;104;116;105;19;34;89;80;57;102;60;100;10;73;93;\
        109;15;47;115;103;22;35;125;176;64;77;123;44;29;40;72;51;54;62;27;\
        84;101;76;107;28;75;31;59;92;111;230;135;16;91;110;202;21;78;6;66;\
        145]" |> (function VList l -> T.VI32 (Int32.of_int (Array.length l)) \
                         | v -> v))
*)

let write_record in_type rb tuple =
  let nullmask_sz, values = (* List of nullable * scalar *)
    List.fold_left (fun (null_i, lst) f ->
      let f_name = E.id_of_path f.RamenFieldMaskLib.path in
      if f.RamenFieldMaskLib.typ.nullable then
        match Hashtbl.find tuple f_name with
        | exception Not_found ->
            (* Unspecified nullable fields are just null. *)
            null_i + 1, lst
        | s ->
            null_i + 1,
            (Some null_i, value_of_string f.typ s) :: lst
      else
        match Hashtbl.find tuple f_name with
        | exception Not_found ->
            null_i,
            (None, T.any_value_of_structure f.typ.structure) :: lst
        | s ->
            null_i,
            (None, value_of_string f.typ s) :: lst
    ) (0, []) in_type |>
    fun (null_i, lst) ->
      round_up_to_rb_word (bytes_for_bits null_i),
      List.rev lst in
  let sz =
    List.fold_left (fun sz (_, v) ->
      sz + sersize_of_value v
    ) nullmask_sz values in
  !logger.debug "Sending an input tuple of %d bytes" sz ;
  with_enqueue_tx rb sz (fun tx ->
    let head = DataTuple RamenChannel.live in
    write_message_header tx 0 head ;
    let start_offs = message_header_sersize head in
    zero_bytes tx start_offs nullmask_sz ; (* zero the nullmask *)
    (* Loop over all values: *)
    List.fold_left (fun offs (null_i, v) ->
      Option.may (set_bit tx start_offs) null_i ;
      write_value tx offs v ;
      offs + sersize_of_value v
    ) (start_offs + nullmask_sz) values |> ignore ;
    (* For tests we won't archive the ringbufs so no need for time info: *)
    0., 0.)

let find_field typ n =
  try List.findi (fun _i f -> f.RamenTuple.name = n) typ
  with Not_found ->
    let err_msg =
      Printf.sprintf2 "Field %s does not exist (possible fields are: %a)"
        (N.field_color n)
        RamenTuple.print_typ_names typ in
    failwith err_msg

let find_field_index typ n = find_field typ n |> fst

let find_param params n =
  let open RamenTuple in
  try (params_find n params).value
  with Not_found ->
    Printf.sprintf2 "Field %a is not a parameter (parameters are: %a)"
      N.field_print n
      RamenTuple.print_params_names params |>
    failwith

(* Build a filter function for tuples of the given type: *)
let filter_tuple_by ser where =
  (* Find the indices of all the involved fields, and parse the values: *)
  let where =
    List.map (fun (n, op, v) ->
      let idx, t = find_field ser n in
      let v =
        let open RamenTypes in
        if v = VNull then VNull else
        let to_structure =
          if op = "in" || op = "not in" then
            TVec (0, t.typ)
          else
            t.typ.structure in
        (try enlarge_value to_structure v
        with e ->
          !logger.error "Cannot enlarge %a to %a (ser = %a)"
            T.print v
            RamenTuple.print_field_typ t
            RamenTuple.print_typ ser ;
          raise e) in
      let op =
        let op_in x = function
          | VVec a | VList a -> Array.exists (fun x' -> x = x') a
          | _ -> assert false in
        match op with
        | "=" -> (=)
        | "!=" | "<>" -> (<>)
        | "<=" -> (<=) | ">=" -> (>=)
        | "<" -> (<) | ">" -> (>)
        | "in" -> op_in
        | "not in" -> (fun a b -> not (op_in a b))
        | _ -> failwith "Invalid operator" in
      idx, op, v
    ) where in
  fun tuple ->
    List.for_all (fun (idx, op, v) ->
      op tuple.(idx) v
    ) where

(* By default, this loops until we reach ma and while_ yields true.
 * Set ~wait_for_more to false if you want to stop once the end of data
 * is reached.
 * Note: mi is inclusive, ma exclusive *)
let rec fold_seq_range ?while_ ?wait_for_more ?(mi=0) ?ma bname init f =
  let fold_rb from rb usr =
    !logger.debug "fold_rb: from=%d, mi=%d, ma=%a"
      from mi (Option.print Int.print) ma ;
    read_buf ?while_ ?wait_for_more rb (usr, from) (fun (usr, seq) tx ->
      !logger.debug "fold_seq_range: read_buf seq=%d" seq ;
      if seq < mi then (usr, seq + 1), true else
      match ma with Some m when seq >= m ->
        (usr, seq + 1), false
      | _ ->
        let usr = f usr seq tx in
        (* Try to save the last sleep: *)
        let more_to_come =
          match ma with None -> true | Some m -> seq < m - 1 in
        (usr, seq + 1), more_to_come) in
  !logger.debug "fold_seq_range: mi=%d, ma=%a" mi (Option.print Int.print) ma ;
  match ma with Some m when mi >= m -> init
  | _ -> (
    let keep_going = match while_ with Some w -> w () | _ -> true in
    if not keep_going then init else (
      let dir = arc_dir_of_bname bname in
      !logger.debug "Archives taken from directory %a" N.path_print dir ;
      let entries =
        arc_files_of dir //
        (fun (from, to_, _t1, _t2, _typ, fname) ->
          !logger.debug "  ...Considering file %a, seqnums %d..%d"
            N.path_print fname from to_ ;
          (* in file names, to_ is inclusive *)
          to_ >= mi && Option.map_default (fun ma -> from < ma) true ma) |>
        Array.of_enum in
      Array.fast_sort arc_file_compare entries ;
      let usr, next_seq =
        Array.fold_left (fun (usr, _ as prev)
                             (from, _to, _t1, _t2, arc_typ, fname) ->
          if arc_typ = RingBufLib.RingBuf then
            let rb = load fname in
            finally (fun () -> unload rb)
              (fold_rb from rb) usr
          else prev
        ) (init, 0 (* unused if there are some entries *)) entries in
      !logger.debug "After archives, next_seq is %d" next_seq ;
      (* Of course by the time we reach here, new archives might have been
       * created. We will know after opening the current rb, if its starting
       * seqnum is > then max_seq then we should recurse into the archive ... *)
      (* Finish with the current rb: *)
      let rb = load bname in
      let s = stats rb in
      if next_seq > 0 && s.first_seq > next_seq && s.first_seq > mi then (
        !logger.debug "fold_seq_range: current starts at %d > %d, \
                       going through the archive again"
          s.first_seq next_seq ;
        unload rb ;
        fold_seq_range ?while_ ?wait_for_more ~mi:next_seq ?ma bname usr f
      ) else (
        !logger.debug "fold_seq_range: current starts at %d, lgtm"
          s.first_seq ;
        let usr, next_seq =
          finally (fun () -> unload rb)
            (fold_rb s.first_seq rb) usr in
        !logger.debug "After current, next_seq is %d" next_seq ;
        (* And of course, by the time we reach this point this ringbuf might
         * have been archived already. *)
        fold_seq_range ?while_ ?wait_for_more ~mi:next_seq ?ma bname usr f)))

let fold_buffer ?wait_for_more ?while_ bname init f =
  match load bname with
  | exception Failure msg ->
      !logger.debug "Cannot fold_buffer: %s" msg ;
      (* Therefore there is nothing to fold: *)
      init
  | rb ->
      finally
        (fun () -> unload rb)
        (read_buf ?wait_for_more ?while_ rb init) f

(* Like fold_buffer but call f with the message rather than the tx: *)
let fold_buffer_tuple ?while_ ?(early_stop=true) bname typ init f =
  !logger.debug "Going to fold over %a" N.path_print bname ;
  let unserialize = read_array_of_values typ in
  let f usr tx =
    match read_tuple unserialize tx with
    | exception e ->
        print_exception ~what:"reading a tuple" e ;
        usr, true
    | msg ->
        let usr, _more_to_come as res = f usr msg in
        if early_stop then res
        else (usr, true)
  in
  fold_buffer ~wait_for_more:false ?while_ bname init f

let event_time_of_tuple typ params
      ((start_field, start_field_src, start_scale), duration_info) =
  let open RamenEventTime in
  let float_of_field i s tup =
    s *. (float_of_scalar tup.(i) |>
          option_get "float_of_scalar of event_time field" __LOC__)
  and float_of_param n s =
    let pv = find_param params n in
    s *. (float_of_scalar pv |>
          option_get "float_of_scalar of event_time param" __LOC__) in
  let get_t1 = match !start_field_src with
    | OutputField ->
        let i = find_field_index typ start_field in
        float_of_field i start_scale
    | Parameter ->
        let c = float_of_param start_field 1. in
        fun _tup -> c in
  let get_t2 = match duration_info with
    | DurationConst k ->
        fun _tup t1 -> t1 +. k
    | DurationField (n, { contents = OutputField }, s) ->
        let i = find_field_index typ n in
        fun tup t1 -> t1 +. float_of_field i s tup
    | StopField (n, { contents = OutputField }, s) ->
        let i = find_field_index typ n in
        fun tup _t1 -> float_of_field i s tup
    | DurationField (n, { contents = Parameter }, s) ->
        let c = float_of_param n s in
        fun _tup t1 -> t1 +. c
    | StopField (n, { contents = Parameter }, s) ->
        let c = float_of_param n s in
        fun _tup _t1 -> c
  in
  fun tup ->
    let t1 = get_t1 tup in
    let t2 = get_t2 tup t1 in
    (* Allow duration to be < 0 *)
    if t2 >= t1 then t1, t2 else t2, t1

(* As tuples are not necessarily ordered by time we want the possibility
 * to override the more_to_come decision.
 * Notice that contrary to `ramen tail`, `ramen timeseries` must never
 * wait for data and must return as soon as we've reached the end of what's
 * available. *)
let fold_buffer_with_time ?(channel_id=RamenChannel.live)
                          ?while_ ?early_stop
                          bname typ params event_time init f =
  !logger.debug "Folding over %a" N.path_print bname ;
  let event_time_of_tuple =
    match event_time with
    | None ->
        failwith "Function has no time information"
    | Some event_time  ->
        event_time_of_tuple typ params event_time in
  let f usr = function
    | DataTuple chan, Some tuple when chan = channel_id ->
        (* Get the times from tuple: *)
        let t1, t2 = event_time_of_tuple tuple in
        f usr tuple t1 t2
    | _ ->
        usr, true
  in
  fold_buffer_tuple ?early_stop ?while_ bname typ init f

let time_range ?while_ bname typ params event_time =
  let dir = arc_dir_of_bname bname in
  let max_range mi_ma t1 t2 =
    match mi_ma with
    | None -> Some (t1, t2)
    | Some (mi, ma) -> Some (Float.min mi t1, Float.max ma t2) in
  let mi_ma =
    RingBufLib.arc_files_of dir |>
    Enum.fold (fun mi_ma (_s1, _s2, t1, t2, _typ, _fname) ->
      max_range mi_ma t1 t2
    ) None in
  (* Also look into the current rb: *)
  fold_buffer_with_time ?while_ bname typ params event_time mi_ma (fun mi_ma _tup t1 t2 ->
    max_range mi_ma t1 t2, true)

let fold_time_range ?(while_=always) bname typ params event_time since until init f =
  let dir = arc_dir_of_bname bname in
  let entries =
    RingBufLib.arc_files_of dir //
    (fun (_s1, _s2, t1, t2, _typ, _fname) -> since < t2 && until >= t1) in
  let f usr tuple t1 t2 =
    (if t1 >= until || t2 < since then usr else f usr tuple t1 t2), true in
  let rec loop usr =
    if while_ () then
      match Enum.get_exn entries with
      | exception Enum.No_more_elements -> usr
      | _s1, _s2, _t1, _t2, arc_typ, fname ->
          if arc_typ = RingBufLib.RingBuf then
            fold_buffer_with_time ~while_ ~early_stop:false
                                  fname typ params event_time usr f ;
          loop ()
    else usr
  in
  let usr = loop init in
  (* finish with the current rb: *)
  (* FIXME: shouldn't we check the min/max event time of this file against
   * since/until as we did for others? *)
  fold_buffer_with_time ~while_ bname typ params event_time usr f
