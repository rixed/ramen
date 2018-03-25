open Batteries
open Lwt
open Helpers
open RamenLog
open RamenSharedTypes
module C = RamenConf
module N = RamenConf.Func
module L = RamenConf.Program

(* FIXME: rather than this json, use mere files with plain program code,
 * and use basename without extention as the program name by default
 * (with a --name arg to provide another name). But how to provide several
 * programs then? Using the file extension, or an explicit --program argument
 * (similar to how compiler deal with input files)? *)
type programs_spec =
  { programs : (string, string) Hashtbl.t } [@@ppp PPP_JSON]

type tuple_spec = (string, string) Hashtbl.t [@@ppp PPP_JSON]

module Input = struct
  type spec =
    { pause : float [@ppp_default 0.] ;
      operation : string ;
      tuple : tuple_spec } [@@ppp PPP_JSON]
end

module Output = struct
  type spec =
    { present : tuple_spec list [@ppp_default []] ;
      absent : tuple_spec list [@ppp_default []] } [@@ppp PPP_JSON]
end

module Notifs = struct
  type spec =
    { present : string list [@ppp_default []] ;
      absent : string list [@ppp_default []] } [@@ppp PPP_JSON]
end

type test_spec =
  { inputs : Input.spec list [@ppp_default []] ;
    outputs : (string, Output.spec) Hashtbl.t
      [@ppp_default Hashtbl.create 0] ;
    notifications : Notifs.spec
      [@ppp_default Notifs.{ present=[]; absent=[] }] }
    [@@ppp PPP_JSON]

let enc = Uri.pct_encode

let write_scalar_value tx offs =
  let open RingBuf in
  let open RingBufLib in
  function
  | VFloat f -> write_float tx offs f ; sersize_of_float
  | VString s -> write_string tx offs s ; sersize_of_string s
  | VBool b -> write_bool tx offs b ; sersize_of_bool
  | VU8 i -> write_u8 tx offs i ; sersize_of_u8
  | VU16 i -> write_u16 tx offs i ; sersize_of_u16
  | VU32 i -> write_u32 tx offs i ; sersize_of_u32
  | VU64 i -> write_u64 tx offs i ; sersize_of_u64
  | VU128 i -> write_u128 tx offs i ; sersize_of_u128
  | VI8 i -> write_i8 tx offs i ; sersize_of_i8
  | VI16 i -> write_i16 tx offs i ; sersize_of_i16
  | VI32 i -> write_i32 tx offs i ; sersize_of_i32
  | VI64 i -> write_i64 tx offs i ; sersize_of_i64
  | VI128 i -> write_i128 tx offs i ; sersize_of_i128
  | VEth e -> write_eth tx offs e ; sersize_of_eth
  | VIpv4 i -> write_ip4 tx offs i ; sersize_of_ipv4
  | VIpv6 i -> write_ip6 tx offs i ; sersize_of_ipv6
  | VCidrv4 c -> write_cidr4 tx offs c ; sersize_of_cidrv4
  | VCidrv6 c -> write_cidr6 tx offs c ; sersize_of_cidrv6
  | VNull -> assert false

let send_tuple conf func rb tuple =
  let ser = C.tuple_ser_type func.N.in_type in
  let nullmask_sz, values = (* List of nullable * scalar *)
    List.fold_left (fun (null_i, lst) ftyp ->
      if ftyp.nullable then
        match Hashtbl.find tuple ftyp.typ_name with
        | exception Not_found ->
            (* Unspecified nullable fields are just null. *)
            null_i + 1, lst
        | s ->
            null_i + 1,
            (Some null_i, RamenScalar.value_of_string ftyp.typ s) :: lst
      else
        match Hashtbl.find tuple ftyp.typ_name with
        | exception Not_found ->
            null_i, (None, RamenScalar.any_value_of_type ftyp.typ) :: lst
        | s ->
            null_i, (None, RamenScalar.value_of_string ftyp.typ s) :: lst
    ) (0, []) ser |>
    fun (null_i, lst) ->
      RingBufLib.(round_up_to_rb_word (bytes_for_bits null_i)),
      List.rev lst in
  let sz =
    List.fold_left (fun sz (_, v) ->
      sz + RingBufLib.sersize_of_value v
    ) nullmask_sz values in
  !logger.debug "Sending an input tuple of %d bytes" sz ;
  RingBuf.with_enqueue_tx rb sz (fun tx ->
    RingBuf.zero_bytes tx 0 nullmask_sz ; (* zero the nullmask *)
    (* Loop over all values: *)
    List.fold_left (fun offs (null_i, v) ->
      Option.may (RingBuf.set_bit tx) null_i ;
      offs + write_scalar_value tx offs v
    ) nullmask_sz values |> ignore ;
    (* For tests we won't archive the ringbufs so no need for time info: *)
    0., 0.)

(* Return a random unique test identifier *)
let get_id () =
  (* Avoid usage of '+' and '/' *)
  let tbl = [|
    'a';'b';'c';'d';'e';'f';'g';'h';'i';'j';'k';'l';'m';'n';'o';'p';'q';
    'r';'s';'t';'u';'v';'w';'x';'y';'z';'A';'B';'C';'D';'E';'F';'G';'H';
    'I';'J';'K';'L';'M';'N';'O';'P';'Q';'R';'S';'T';'U';'V';'W';'X';'Y';
    'Z';'0';'1';'2';'3';'4';'5';'6';'7';'8';'9';' ';'_' |] in
  Random.full_range_int () |>
  packed_string_of_int |>
  Base64.str_encode ~tbl

let ppp_of_file ppp fname =
  let%lwt s = lwt_read_whole_file fname in
  Lwt.wrap (fun () -> PPP.of_string_exc ppp s)

(* Read a tuple described by the given type, and return a hash of fields
 * to string values *)

let test_output ser_type in_rb output_spec =
  let nullmask_sz =
    RingBufLib.nullmask_bytes_of_tuple_type ser_type in
  (* Change the hashtable of field to value into a list of field index
   * and value: *)
  let field_index field =
    match List.findi (fun _ ftyp -> ftyp.typ_name = field) ser_type with
    | exception Not_found ->
        let msg = Printf.sprintf "Unknown field %s in %s" field
                    (IO.to_string RamenTuple.print_typ ser_type) in
        failwith msg
    | idx, _ -> idx in
  let field_indices_of_tuples =
    List.map (fun spec ->
      Hashtbl.enum spec /@
      (fun (field, value) -> field_index field, value) |>
      List.of_enum) in
  let%lwt tuples_to_find = wrap (fun () -> ref (
    field_indices_of_tuples output_spec.Output.present)) in
  let%lwt tuples_must_be_absent = wrap (fun () ->
    field_indices_of_tuples output_spec.Output.absent) in
  let tuples_to_not_find = ref [] in
  let start = Unix.gettimeofday () in
  (* With tuples that must be absent, when to stop listening?
   * For now the rule is simple:
   * for as long as we have not yet received some tuples that
   * must be present and the time did not ran out. *)
  let timeout = 5. in (* TODO: a parameter in the test spec *)
  let while_ () =
    if !tuples_to_find <> [] &&
       !tuples_to_not_find = [] &&
       Unix.gettimeofday () -. start < timeout
    then return_true else return_false in
  let unserialize = RamenSerialization.read_tuple ser_type nullmask_sz in
  let%lwt () =
    RamenSerialization.read_tuples ~while_ unserialize in_rb (fun tuple ->
      tuples_to_find :=
        List.filter (fun spec ->
          List.for_all (fun (idx, value) ->
            RamenScalar.to_string tuple.(idx) = value) spec |> not
        ) !tuples_to_find ;
      tuples_to_not_find :=
        List.filter (fun spec ->
          List.for_all (fun (idx, value) ->
            RamenScalar.to_string tuple.(idx) = value) spec
        ) tuples_must_be_absent |>
        List.rev_append !tuples_to_not_find ;
      return_unit) in
  let success = !tuples_to_find = [] && !tuples_to_not_find = [] in
  let file_spec_print oc (idx, value) =
    Printf.fprintf oc "idx=%d, value=%S" idx value in
  let tuple_spec_print oc spec =
    List.print file_spec_print oc spec in
  let msg =
    if success then "" else
    (if !tuples_to_find = [] then "" else
      "Could not find these tuples: "^
        IO.to_string (List.print tuple_spec_print) !tuples_to_find) ^
    (if !tuples_to_not_find = [] then "" else
      "Found these tuples: "^
        IO.to_string (List.print tuple_spec_print) !tuples_to_not_find)
  in
  return (success, msg)

let test_notifications notify_rb notif_spec =
  (* We keep pat in order to be able to print it later: *)
  let to_regexp pat = pat, Str.regexp pat in
  let notifs_must_be_absent = List.map to_regexp notif_spec.Notifs.absent
  and notifs_to_find = ref (List.map to_regexp notif_spec.Notifs.present)
  and notifs_to_not_find = ref []
  and timeout = 5. (* TODO: a parameter in the test spec *)
  and start = Unix.gettimeofday () in
  let while_ () =
    if !notifs_to_find <> [] &&
       !notifs_to_not_find = [] &&
       Unix.gettimeofday () -. start < timeout
    then return_true else return_false in
  let%lwt () =
    RamenSerialization.read_notifs ~while_ notify_rb (fun (worker, url) ->
      !logger.debug "Got notification from %s: %S" worker url ;
      notifs_to_find :=
        List.filter (fun (_pat, re) ->
          Str.string_match re url 0 |>  not) !notifs_to_find ;
      notifs_to_not_find :=
        List.filter (fun (_pat, re) ->
          Str.string_match re url 0) notifs_must_be_absent |>
        List.rev_append !notifs_to_not_find ;
      return_unit) in
  let success = !notifs_to_find = [] && !notifs_to_not_find = [] in
  let re_print oc (pat, _re) = String.print oc pat in
  let msg =
    if success then "" else
    (if !notifs_to_find = [] then "" else
      "Could not find these notifs: "^
        IO.to_string (List.print re_print) !notifs_to_find) ^
    (if !notifs_to_not_find = [] then "" else
      "Found these notifs: "^
        IO.to_string (List.print re_print) !notifs_to_not_find)
  in
  return (success, msg)

let test_one conf server_url conf_spec test =
  (* Create the configuration: *)
  let test_id = get_id () in
  !logger.info "Creating test suite %S..." test_id ;
  let%lwt old_to_new_names =
    hash_map_s conf_spec.programs (fun name program_code ->
      RamenOps.set_program ~test_id conf name program_code) in
  (* Compile it locally *)
  !logger.info "Compiling all the test workers..." ;
  let new_names = Hashtbl.values old_to_new_names |> List.of_enum in
  let%lwt failures = RamenOps.compile_programs conf new_names in
  let%lwt () =
    if failures = [] then return_unit else (
      let print_failure oc (exn, prog) =
        Printf.fprintf oc "%s: %s" prog (Printexc.to_string exn) in
      let msg = "Cannot compile these programs: "^
                IO.to_string (List.print print_failure) failures in
      fail_with msg) in
  (* Create the notification RB before the workers try to load it: *)
  let notify_rb_name = C.notify_ringbuf ~test_id conf in
  !logger.info "Creating ringbuffer for notifications in %s" notify_rb_name ;
  RingBuf.create notify_rb_name RingBufLib.rb_default_words ;
  (* Ask the supervisor to start them all: *)
  !logger.info "Starting test programs..." ;
  let%lwt () =
    try%lwt
      hash_iter_s old_to_new_names (fun _old_name new_name ->
        let%lwt resp_str =
          RamenHttpHelpers.http_get (server_url ^"/start/"^ enc new_name) in
        let%lwt resp =
          wrap (fun () -> PPP.of_string_exc put_program_resp_ppp_json resp_str) in
        if not resp.success then
          fail_with "Cannot start test program"
        else return_unit)
    with Unix.Unix_error (Unix.ECONNREFUSED, _, _) ->
      let msg =
        Printf.sprintf "Cannot connect to Ramen process supervisor.\n\
                        Make sure it is running and listening at %s.\n"
          server_url in
      fail_with msg in
  (* Retrieve the workers signatures (and everything else): *)
  (* Note: we must be the only process interacting with this test instance
   * so that's fine releasing the lock *)
  let workers = Hashtbl.create 11 in (* from user visible fq_name to func *)
  let%lwt () =
    C.with_rlock conf (fun programs ->
      wrap (fun () ->
        Hashtbl.iter (fun old_name new_name ->
          match Hashtbl.find programs new_name with
          | exception Not_found ->
              failwith ("Test program "^ new_name ^" has been deleted?!") ;
          | p ->
              Hashtbl.iter (fun _ func ->
                let user_fq_name = old_name ^"/"^ func.N.name in
                Hashtbl.add workers user_fq_name func
              ) p.L.funcs
        ) old_to_new_names)) in
  (* When to get the output? We cannot wait until the test is over, because it
   * might never be (for instance it an operation selects from an external
   * program).  Instead we wait for the outputs to have been received or a
   * timeout.  So for each specified output start a reader thread (before we
   * have started sending any input!)
   * And we will do this directly, by creating manipulating the OutRef files. *)
  !logger.info "Reading all outputs..." ;
  let%lwt tester_threads =
    hash_fold_s test.outputs (fun user_fq_name output_spec thds ->
      let in_rb_name =
        C.temp_in_ringbuf_name conf ("tests/"^ test_id ^"/"^ user_fq_name) in
      RingBuf.create in_rb_name RingBufLib.rb_default_words ;
      let in_rb = RingBuf.load in_rb_name in
      let%lwt tester_thread =
        match Hashtbl.find workers user_fq_name with
        | exception Not_found ->
            fail_with ("Unknown operation "^ user_fq_name)
        | tested_func ->
            let out_ref_fname = C.out_ringbuf_names_ref conf tested_func in
            let out_type = C.tuple_ser_type tested_func.out_type in
            let in_type = out_type in (* Just receive everything *)
            let field_mask = RingBufLib.skip_list ~out_type ~in_type in
            let%lwt () =
              RamenOutRef.(add out_ref_fname
                (in_rb_name, { field_mask ; timeout = 0. })) in
            return
              (finalize
                (fun () -> test_output in_type in_rb output_spec)
                (fun () ->
                  let%lwt () =
                    RamenOutRef.remove out_ref_fname in_rb_name in
                  RingBuf.unload in_rb ;
                  (* TODO: unlink *)
                  return_unit)) in
      return (tester_thread :: thds)
    ) [] in
  (* Similarly, read all notifications: *)
  let notify_rb = RingBuf.load notify_rb_name in
  let tester_threads =
    finalize
      (fun () -> test_notifications notify_rb test.notifications)
      (fun () ->
        RingBuf.unload notify_rb ;
        (* TODO: unlink *)
        return_unit) :: tester_threads in
  (* Feed the workers *)
  !logger.info "Feeding workers with test inputs..." ;
  let worker_cache = Hashtbl.create 11 in
  let%lwt () =
    Lwt_list.iter_s (fun input ->
      match Hashtbl.find workers input.Input.operation with
      | exception Not_found ->
          fail_with ("Unknown test operation: "^ input.operation)
      | func ->
          let cache_key = func.program_name, func.name in
          let rb =
            match Hashtbl.find worker_cache cache_key with
            | exception Not_found ->
                if func.N.merge_inputs then
                  (* TODO: either specify a parent number or pick the first one? *)
                  let err = "Writing to merging operations is not \
                             supported yet!" in
                  raise (Failure err)
                else
                  let in_rb = C.in_ringbuf_name_single conf func in
                  let rb = RingBuf.load in_rb in
                  Hashtbl.add worker_cache cache_key rb ;
                  rb
            | rb -> rb in
          send_tuple conf func rb input.tuple
    ) test.inputs in
  Hashtbl.iter (fun _ rb -> RingBuf.unload rb) worker_cache ;
  (* Wait for all the verdicts: *)
  !logger.info "Wait for %d test verdict%s..."
    (List.length tester_threads) (if tester_threads = [] then "" else "s") ;
  let%lwt all_good =
    Lwt_list.fold_left_s (fun all_good thd ->
      let%lwt success, msg = thd in
      if success then return all_good else (
        Printf.printf "Failure: %s\n" msg ;
        return_false)
    ) true tester_threads in
  return all_good

let run conf server_url conf_file tests =
  let open RamenHttpHelpers in
  (* Parse the conf json file *)
  !logger.info "Parsing configuration in %S..." conf_file ;
  let%lwt conf_spec = ppp_of_file programs_spec_ppp_json conf_file in
  (* And tests so that we won't have to clean anything if they are bogus *)
  let%lwt test_specs =
    Lwt_list.map_s (fun fname ->
      !logger.info "Parsing test specification in %S..." fname ;
      ppp_of_file test_spec_ppp_json fname
    ) tests in
  (* Run all tests: *)
  (* Note: The workers states must be cleaned in between 2 tests ; the
   * simpler is to draw a new test_id. *)
  let%lwt nb_good, nb_tests =
    Lwt_list.fold_left_s (fun (nb_good, nb_tests) test ->
      let%lwt res = test_one conf server_url conf_spec test in
      return ((nb_good + if res then 1 else 0), nb_tests + 1)
    ) (0, 0) test_specs in
  if nb_good = nb_tests then (
    Printf.printf "All %d test%s succeeded.\n" nb_tests (if nb_tests > 1 then "s" else "") ;
    return_unit
  ) else
    let nb_fail = nb_tests - nb_good in
    let msg =
      Printf.sprintf "%d test%s failed."
        nb_fail (if nb_fail > 1 then "s" else "") in
    fail_with msg
