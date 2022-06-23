open Batteries
open Stdint

open RingBuf
open RingBufLib
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
module DT = DessserTypes
module DE = DessserExpressions
module T = RamenTypes
module E = RamenExpr
module N = RamenName
module O = RamenOperation
open RamenTypes

let verbose_serialization = false

(* Read all fields one by one. Not the real thing.
 * Slow unserializer used for command line tools such as `ramen tail`
 * or for reading output in `ramen test`.
 * There is no fieldmask, this reads every fields. [mn] must have no
 * private fields. *)
let read_array_of_values mn tx start_offs =
  if verbose_serialization then
    !logger.info "read_array_of_values: in_type = %a, starting at offset %d"
      DT.print_mn mn start_offs ;
  (* Ramen's value are always non-nullable records with no default value, so
   * they will always be present (bit=0), skip (and check) the prefix
   * (note that only one byte of bitmask is cleared by dessser): *)
  (*let pref = (RingBuf.read_word tx start_offs) land 0xffff in
  if pref <> 0x1 then
    !logger.error "prefix = %x at offset %d, tx = %t"
      pref start_offs (hex_print (RingBuf.read_raw_tx tx)) ;
  assert (pref = 0x0001) ;
  let start_offs = start_offs + DessserRamenRingBuffer.word_size in *)
  let tuple_len = T.num_columns mn in
  let fieldmask_words, bi = Uint8.to_int (RingBuf.read_u8 tx start_offs), 8 in
  if verbose_serialization then
    !logger.info "fieldmask_words = %d" fieldmask_words ;
  assert (fieldmask_words > 0) ;
  if verbose_serialization then (
    let fieldmask_str =
      Printf.sprintf " (first is 0x%04x)"
        (RingBuf.read_u32 tx start_offs |> Uint32.to_int) in
    !logger.info "De-serializing a tuple of type %a with fieldmask of \
                  %d words%s, starting at offset %d, up to %d bytes"
      DT.print_mn mn
      fieldmask_words fieldmask_str
      start_offs
      (tx_size tx - start_offs)
  ) ;
  let tuple = Array.make tuple_len Raql_value.VNull in
  let offs =
    start_offs + DessserRamenRingBuffer.word_size * fieldmask_words in
  T.fold_columns (fun (offs, bi, i) fn mn ->
    if N.is_private fn then
      !logger.warning "Asked to deserialize private field %a!"
        N.field_print fn ;
    if verbose_serialization then
      !logger.info "Field %a (#%d) of type %a at offset %d (bi=%d)"
        N.field_print fn i
        DT.print_mn mn
        offs bi ;
    let has_bit = DessserRamenRingBuffer.BitMaskWidth.has_bit mn in
    let value, offs' =
      if has_bit && get_bit tx start_offs bi then (
        (* Value is absent, take default: *)
        let value_expr =
          match mn.DT.default with
          | Some v -> v
          | None -> DE.default_mn mn in
        let value = T.of_const_expr value_expr in
        if verbose_serialization then
          !logger.info "...value is absent, use default: %a"
            T.print value ;
        Some value, offs
      ) else (
        (* Value is present *)
        let value, offs' = RingBufLib.read_value tx offs mn.DT.typ in
        if verbose_serialization then
          !logger.info "...value of type %a at offset %d..%d: %a"
            DT.print_mn mn
            offs offs' T.print value ;
        Some value, offs'
      ) in
    let bi = if has_bit then bi + 1 else bi in
    Option.may (Array.set tuple i) value ;
    offs', bi, i + 1
  ) (offs, bi, 0) mn |> ignore ;
  tuple

let read_tuple unserialize tx =
  if verbose_serialization then
    !logger.info "Read a tuple in TX@%d..+%d:%t"
      (tx_start tx)
      (tx_size tx)
      (hex_print ~address:(tx_start tx * DessserRamenRingBuffer.word_size)
                 (RingBuf.read_raw_tx tx)) ;
  match read_message_header tx 0 with
  | EndOfReplay _ as m -> m, None
  | DataTuple chan as m ->
      if verbose_serialization then
        !logger.info "Read a tuple for channel %a" RamenChannel.print chan ;
      let tuple = unserialize tx (message_header_sersize m) in
      m, Some tuple

let print_value_with_type oc v =
  Printf.fprintf oc "%a of type %a"
    T.print v
    DT.print (type_of_value v)

let value_of_string t s =
  let rec equivalent_types t1 t2 =
    match t1.DT.typ, t2.DT.typ with
    | DT.TVec (_, t1), DT.TArr t2 ->
        equivalent_types t1 t2
    | s1, s2 ->
        can_enlarge ~from:s1 ~to_:s2 in
  let equivalent_types t1 t2 =
    equivalent_types (DT.develop_mn t1)
                     (DT.develop_mn t2) in
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
      if v = VNull && t.DT.nullable then v else
      let vt = type_of_value v in
      if equivalent_types (DT.required vt) t then
        T.enlarge_value t.DT.typ v else
      let msg =
        Printf.sprintf2 "%S has type %a instead of expected %a"
          s
          DT.print vt
          DT.print t.DT.typ in
      failwith msg
  | Error (Ambiguous lst) ->
      (match List.filter (fun (v, _c, _s) ->
              equivalent_types DT.(required (type_of_value v)) t
            ) lst |>
            List.unique_hash with
      | [] ->
          let msg =
            Printf.sprintf2 "%S could have type %a but not the expected %a"
              s
              (List.print ~first:"" ~last:"" ~sep:" or "
                (fun oc (v, _c, _s) ->
                  DT.print oc (type_of_value v))) lst
              DT.print_mn t in
          failwith msg
      | [v, _, _] ->
          T.enlarge_value t.DT.typ v
      | lst ->
          let msg =
            Printf.sprintf2 "%S is ambiguous: is it %a?"
              s
              (List.print ~first:"" ~last:"" ~sep:", or "
                (fun oc (v, _, _) -> print_value_with_type oc v)) lst in
          failwith msg)

(*$inject open Stdint *)
(*$= value_of_string & ~printer:(BatIO.to_string print_value_with_type)
  (VString "glop") \
    (value_of_string DT.(required TString) "\"glop\"")
  (VString "glop") \
    (value_of_string DT.(required TString) " \"glop\"  ")
  (VU16 (Uint16.of_int 15042)) \
    (value_of_string DT.(required TU16) "15042")
  (VU32 (Uint32.of_int 15042)) \
    (value_of_string DT.(required TU32) "15042")
  (VI64 (Int64.of_int  15042)) \
    (value_of_string DT.(required TI64) "15042")
  (VFloat 15042.) \
    (value_of_string DT.(required TFloat) "15042")
  VNull \
    (value_of_string DT.(optional TFloat) "null")
  (VArr [| VFloat 0.; VFloat 1.; VFloat 2. |]) \
    (value_of_string DT.(optional (TArr (required TFloat))) "[ 0; 1; 2]")
  (VI32 239l) \
    (value_of_string DT.(optional (TArr (required TI16))) \
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
        145]" |> \
        (function VArr l -> Raql_value.VI32 (Int32.of_int (Array.length l)) \
                | v -> v))
*)

exception Result of (int * T.t)

(* Return the subtype and the index of the field column *)
let find_field mn n =
  try
    T.fold_columns (fun i fn mn ->
      if fn = n then raise (Result (i, mn))
      else i + 1
    ) 0 mn |> ignore ;
    (* Not found *)
    let err_msg =
      Printf.sprintf2 "Field %s does not exist (possible fields are: %a)"
        (N.field_color n)
        DT.print_mn mn in
    failwith err_msg
  with Result x ->
    x

let find_field_index mn n =
  find_field mn n |> fst

let find_param params n =
  let open RamenTuple in
  try (params_find n params).value
  with Not_found ->
    Printf.sprintf2 "Field %a is not a parameter (parameters are: %a)"
      N.field_print n
      RamenTuple.print_params_names params |>
    failwith

(* Build a filter function for tuples of the given type: *)
let filter_tuple_by fields where =
  (* Find the indices of all the involved fields, and parse the values: *)
  let where =
    List.map (fun (n, op, v) ->
      let idx, mn = find_field fields n in
      let v =
        if v = Raql_value.VNull then Raql_value.VNull else
        let to_structure =
          if op = "in" || op = "not in" then
            DT.TVec (0, mn)
          else
            mn.DT.typ in
        (try enlarge_value to_structure v
        with e ->
          !logger.error "Cannot enlarge %a to %a"
            T.print v
            DT.print_mn mn ;
          raise e) in
      let op =
        let op_in x = function
          | Raql_value.VVec a | Raql_value.VArr a ->
              Array.exists (fun x' -> x = x') a
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
let fold_buffer_tuple ?while_ ?(early_stop=true) bname mn init f =
  !logger.debug "Going to fold over %a" N.path_print bname ;
  let unserialize = read_array_of_values mn in
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

let event_time_of_tuple out_type params
      ((start_field, start_field_src, start_scale), duration_info) =
  let open RamenEventTime in
  let open Event_time_field.DessserGen in
  let float_of_field i s tup =
    s *. (float_of_scalar tup.(i) |>
          option_get "float_of_scalar of event_time field" __LOC__)
  and float_of_param n s =
    let pv = find_param params n in
    s *. (float_of_scalar pv |>
          option_get "float_of_scalar of event_time param" __LOC__) in
  let get_t1 = match start_field_src with
    | OutputField ->
        let i = find_field_index out_type start_field in
        float_of_field i start_scale
    | Parameter ->
        let c = float_of_param start_field 1. in
        fun _tup -> c in
  let get_t2 = match duration_info with
    | DurationConst k ->
        fun _tup t1 -> t1 +. k
    | DurationField (n, OutputField, s) ->
        let i = find_field_index out_type n in
        fun tup t1 -> t1 +. float_of_field i s tup
    | StopField (n, OutputField, s) ->
        let i = find_field_index out_type n in
        fun tup _t1 -> float_of_field i s tup
    | DurationField (n, Parameter, s) ->
        let c = float_of_param n s in
        fun _tup t1 -> t1 +. c
    | StopField (n, Parameter, s) ->
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
                          bname mn params event_time init f =
  !logger.debug "Folding over %a" N.path_print bname ;
  let event_time_of_tuple =
    match event_time with
    | None ->
        failwith "Function has no time information"
    | Some event_time  ->
        event_time_of_tuple mn params event_time in
  let f usr = function
    | DataTuple chan, Some tuple when chan = channel_id ->
        (* Get the times from tuple: *)
        let t1, t2 = event_time_of_tuple tuple in
        f usr tuple t1 t2
    | _ ->
        usr, true
  in
  fold_buffer_tuple ?early_stop ?while_ bname mn init f

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
