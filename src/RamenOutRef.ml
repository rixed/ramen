(* OutRef files are the files describing where a func should send its
 * output. It's basically a list of ringbuf files, with a bitmask of fields
 * that must be written, and an optional timestamp  after which the export
 * should stop (esp. useful for non-ring buffers).
 * Ring and Non-ring buffers are distinguished with their filename
 * extensions, which are either ".r" (ring) or ".b" (buffer).
 * This has the nice consequence that you cannot have a single file name
 * repurposed into a different kind of storage.
 *
 * In case of non-ring buffers, on completion the file is renamed with the
 * min and max sequence numbers and timestamps (if known), and a new one is
 * created.
 *
 * We want to lock those files, both internally (same process) and externally
 * (other processes), although we are fine with advisory locking.
 * Unfortunately lockf will only lock other processes out so we have to combine
 * RWLocks and lockf.
 *)
open Batteries
open RamenHelpers
open RamenLog
open RamenConsts
module N = RamenName
module Files = RamenFiles
module Channel = RamenChannel

type recipient =
  | File of N.path
  | SyncKey of string (* Some identifier for the client request *)
  [@@ppp PPP_OCaml]

let recipient_print oc = function
  | File p -> N.path_print oc p
  | SyncKey s -> String.print oc s

(* How are data represented on disk: *)
type out_ref_conf =
  (recipient, file_spec_conf) Hashtbl.t
  [@@ppp PPP_OCaml]

and file_spec_conf =
  file_type *
  (* field mask: *)
  string *
  (* per channel timeouts (0 = no timeout), number of sources (<0 for
   * endless channel), pid of the readers (or 0 if it does not depend on
   * a live reader or if the reader is not known yet) : *)
  (Channel.t, float * int * int) Hashtbl.t
  [@@ppp PPP_OCaml]

and file_type =
  | RingBuf
  | Orc of {
      with_index : bool [@ppp_default false] ;
      batch_size : int [@ppp_default Default.orc_rows_per_batch] ;
      num_batches : int [@ppp_default Default.orc_batches_per_file] }
  [@@ppp PPP_OCaml]

(* ...and internally, where the field mask is a proper list of booleans
 * and the source counter is a ref that can be decremented: *)
type file_spec =
  { file_type : file_type ;
    fieldmask : RamenFieldMask.fieldmask ;
    channels : (Channel.t, float * int ref * int) Hashtbl.t }

let print_out_specs oc =
  let chan_print oc (timeout, num_sources, pid) =
    Printf.fprintf oc "{ timeout=%a; %t; %t }"
      print_as_date timeout
      (fun oc ->
        if !num_sources >= 0 then
          Printf.fprintf oc " #sources=%d" !num_sources
        else
          Printf.fprintf oc " unlimited")
      (fun oc ->
        if pid = 0 then
          String.print oc "any readers"
        else
          Printf.fprintf oc "reader=%d" pid) in
  Hashtbl.print
    recipient_print
    (fun oc s ->
      Hashtbl.print Channel.print chan_print oc s.channels)
    oc

(* [combine_specs s1 s2] returns the result of replacing [s1] with [s2].
 * Basically, new fields prevail and we merge channels, keeping the longer
 * timeout, and the most recent non-zero reader pid: *)
let combine_specs (_, _, c1) (ft, s, c2) =
  let pid_merge p1 p2 =
    if p2 = 0 then p1 else p2 in
  ft, s,
  hashtbl_merge c1 c2
    (fun _ spec1 spec2 ->
      match spec1, spec2 with
      | (Some _ as s), None | None, (Some _ as s) -> s
      | Some (t1, s1, p1), Some (t2, s2, p2) ->
          Some (Float.max t1 t2, Int.max s1 s2, pid_merge p1 p2)
      | _ -> assert false)

let timed_out ~now t = t > 0. && now > t

let write_ (fname : N.path) fd c =
  let context = "Writing out_ref "^ (fname :> string) in
  fail_with_context context (fun () ->
    Files.ppp_to_fd out_ref_conf_ppp_ocaml fname fd c)

(* Timeout old chans and remove stale versions of files: *)
let filter_out_ref =
  let subdir = "/"^ (out_ref_subdir :> string) ^"/" in
  let can_be_written_to ft fname =
    match ft with
    | RingBuf ->
        (* Ringbufs are never created by the workers but by supervisor or
         * archivist (workers will rotate non-wrapping ringbuffers but even then
         * the original has to pre-exist) *)
        Files.exists fname
    | Orc _ ->
        (* Will be created as needed (including if the file name points at an
         * obsolete ringbuf version :( *)
        true in
  (* The above is not enough to detect wrong recipients, as even an existing
   * RingBuf must still be invalid: *)
  let file_is_obsolete fname =
    match String.find (fname : N.path :> string) subdir with
    | exception Not_found ->
        false
    | i ->
        let v = RamenVersions.out_ref in
        not (string_sub_eq (fname :> string) i v 0 (String.length v)) in
  let filter cause f fname =
    let r = f fname in
    if not r then
      !logger.warning "Ignoring %s ringbuffer %a" cause N.path_print fname ;
    r in
  fun ~now h ->
    let filter_chans chans =
      Hashtbl.filter (fun (t, _, _) ->
        not (timed_out ~now t)
      ) chans in
    Hashtbl.filteri (fun rcpt (ft, _, chans) ->
      let valid_rcpt =
        match rcpt with
        | File fname ->
            filter "non-writable" (can_be_written_to ft) fname &&
            filter "obsolete" (not % file_is_obsolete) fname
        | SyncKey _ ->
            true in
      if valid_rcpt then
        let chans = filter_chans chans in
        Hashtbl.length chans > 0
      else
        false
    ) h

let read_ =
  let ppp_of_fd =
    Files.ppp_of_fd ~default:"{}" out_ref_conf_ppp_ocaml in
  fun (fname : N.path) ~now fd ->
    let c = "Reading out_ref "^ (fname :> string) in
    fail_with_context c (fun () ->
      ppp_of_fd fname fd |> filter_out_ref ~now)

let read fname ~now =
  RamenAdvLock.with_r_lock fname (read_ fname ~now) |>
  Hashtbl.map (fun _ (file_type, mask_str, chans) ->
    { file_type ;
      fieldmask = RamenFieldMask.of_string mask_str ;
      channels = Hashtbl.map (fun _ (t, s, p) -> t, ref s, p) chans })

let read_live fname ~now =
  let h = read fname ~now in
  Hashtbl.filter_inplace (fun s ->
    Hashtbl.filteri_inplace (fun c _ -> c = Channel.live) s.channels ;
    not (Hashtbl.is_empty s.channels)
  ) h ;
  h

let add_ fname fd out_fname file_type timeout_date num_sources ?(pid=0) chan
         fieldmask ~now =
  let channels = Hashtbl.create 1 in
  Hashtbl.add channels chan (timeout_date, num_sources, pid) ;
  let file_spec =
    file_type, RamenFieldMask.to_string fieldmask, channels in
  let h =
    try read_ fname ~now fd
    with Sys_error _ ->
      Hashtbl.create 1
    in
  let rewrite file_spec =
    Hashtbl.replace h out_fname file_spec ;
    write_ fname fd h ;
    !logger.debug "Adding %a to %a with fieldmask %a"
      recipient_print out_fname
      N.path_print fname
      RamenFieldMask.print fieldmask
  in
  match Hashtbl.find h out_fname with
  | exception Not_found ->
      rewrite file_spec
  | prev_spec ->
      let file_spec = combine_specs prev_spec file_spec in
      if prev_spec <> file_spec then rewrite file_spec

let add ~now fname ?(timeout_date=0.) ?(num_sources= -1) ?pid
        ?(channel=Channel.live) ?(file_type=RingBuf) out_fname fieldmask =
  RamenAdvLock.with_w_lock fname (fun fd ->
    (*!logger.debug "Got write lock for add on %s" fname ;*)
    add_ fname fd out_fname file_type timeout_date num_sources ?pid channel
         fieldmask ~now)

let remove_ ~now fname fd out_fname ?(pid=0) chan =
  let h = read_ fname ~now fd in
  match Hashtbl.find h out_fname with
  | exception Not_found -> ()
  | _, _, channels ->
      Hashtbl.modify_opt chan (function
        | None ->
            None
        | Some (_timeout, _count, current_pid) as prev ->
            if current_pid = 0 || current_pid = pid then
              None
            else
              prev (* Do not remove someone else's link! *)
      ) channels ;
      if Hashtbl.is_empty channels then
        Hashtbl.remove h out_fname ;
      write_ fname fd h ;
      !logger.debug "Removed %a from %a"
        recipient_print out_fname
        N.path_print fname

let remove ~now fname out_fname ?pid chan =
  RamenAdvLock.with_w_lock fname (fun fd ->
    (*!logger.debug "Got write lock for remove on %s" fname ;*)
    remove_ ~now fname fd out_fname ?pid chan)

(* Check that fname is listed in outbuf_ref_fname for any non-timed out
 * channel: *)
let mem_ fname fd out_fname ~now =
  let h = read_ fname ~now fd in
  match Hashtbl.find h out_fname with
  | exception Not_found ->
      false
  | _, _, channels ->
      try
        Hashtbl.iter (fun _c (t, _, _) ->
          if not (timed_out ~now t) then raise Exit
        ) channels ;
        false
      with Exit -> true

let mem fname out_fname ~now =
  RamenAdvLock.with_r_lock fname (fun fd ->
    mem_ fname fd out_fname ~now)

let remove_channel ~now fname chan =
  RamenAdvLock.with_w_lock fname (fun fd ->
    let h = read_ fname ~now fd in
    Hashtbl.filter_inplace (fun (_, _, channels) ->
      Hashtbl.remove channels chan ;
      not (Hashtbl.is_empty channels)
    ) h ;
    write_ fname fd h) ;
  !logger.debug "Removed channel %a from %a"
    Channel.print chan
    N.path_print fname

let check_spec_change rcpt old new_ =
  (* Or the rcpt should have changed: *)
  if new_.file_type <> old.file_type then
    Printf.sprintf2 "Output file %a changed file type \
                     from %s to %s while in use"
      recipient_print rcpt
      (PPP.to_string file_type_ppp_ocaml old.file_type)
      (PPP.to_string file_type_ppp_ocaml new_.file_type) |>
    failwith ;
  if new_.fieldmask <> old.fieldmask then
    Printf.sprintf2 "Output file %a changed field mask \
                     from %a to %a while in use"
      recipient_print rcpt
      RamenFieldMask.print old.fieldmask
      RamenFieldMask.print new_.fieldmask |>
    failwith

(*$inject
  open Batteries
  module N = RamenName
*)

(*$R
  let outref_fname = N.path (Filename.temp_file "ramen_test_" ".out") in

  (* Prefer orc files as they are not filtered out if non-existent: *)
  let add =
    let file_type =
      Orc { with_index = false ; batch_size = 9 ; num_batches = 9 } in
    add ~file_type in

  (* out_ref is initially empty/absent: *)
  let now = Unix.gettimeofday () in
  assert_bool "outref is empty"
    (not (mem outref_fname (File (N.path "dest1")) ~now)) ;

  add outref_fname (File (N.path "dest1")) [|RamenFieldMask.Copy|] ~now ;
  assert_bool "dest1 is now in outref"
    (mem outref_fname (File (N.path "dest1")) ~now) ;

  add outref_fname (File (N.path "dest2")) ~channel:(RamenChannel.of_int 1)
      [|RamenFieldMask.Copy|] ~now ;
  assert_bool "dest2 is now in outref"
    (mem outref_fname (File (N.path "dest1")) ~now) ;

  remove_channel outref_fname (RamenChannel.of_int 1) ~now ;
  assert_bool "no more chan 1"
    (not (mem outref_fname (File (N.path "dest2")) ~now)) ;

  (* If all went well: *)
  RamenFiles.safe_unlink outref_fname
*)
