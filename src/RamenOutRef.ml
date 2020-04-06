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
open RamenHelpersNoLog
open RamenLog
open RamenConsts
open RamenSync
module C = RamenConf
module N = RamenName
module VOS = Value.OutputSpecs
module Files = RamenFiles
module Channel = RamenChannel
module ZMQClient = RamenSyncZMQClient

(* [combine_specs s1 s2] returns the result of replacing [s1] with [s2].
 * Basically, new fields prevail and we merge channels, keeping the longer
 * timeout, and the most recent non-zero reader pid: *)
let combine_specs s1 s2 =
  VOS.{ s2 with channels =
      hashtbl_merge s1.VOS.channels s2.VOS.channels
        (fun _ spec1 spec2 ->
          match spec1, spec2 with
          | (Some _ as s), None | None, (Some _ as s) -> s
          | Some (t1, s1, p1), Some (t2, s2, p2) ->
              let timeout =
                if t1 = 0. || t2 = 0. then 0. (* no timeout wins *)
                else Float.max t1 t2
              and num_sources =
                if s1 < 0 || s2 < 0 then -1 (* endless channels win! *)
                else Int.max s1 s2
              and pid =
                if p2 = 0 then p1 else p2 (* latest set pid wins *)
              in
              Some (timeout, num_sources, pid)
          | _ -> assert false) }

let timed_out ~now t = t > 0. && now > t

let topics = "sites/*/workers/*/outputs"

let output_specs_key site fq =
  Key.(PerSite (site, PerWorker (fq, OutputSpecs)))

let write ?while_ session k c =
  let v = Value.OutputSpecs c in
  ZMQClient.send_cmd ?while_ session (Client.CltMsg.SetKey (k, v))

(* Timeout old chans and remove stale versions of files: *)
let filter_out_ref =
  let subdir = "/"^ (out_ref_subdir :> string) ^"/" in
  let can_be_written_to ft fname =
    match ft with
    | VOS.RingBuf ->
        (* Ringbufs are never created by the workers but by supervisor or
         * archivist (workers will rotate non-wrapping ringbuffers but even then
         * the original has to pre-exist) *)
        Files.exists fname
    | VOS.Orc _ ->
        (* Will be created as needed (including if the file name points at an
         * obsolete ringbuf version :( *)
        true in
  (* The above is not enough to detect wrong recipients, as even an existing
   * RingBuf can still be invalid: *)
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
      !logger.warning "OutRef: Ignoring %s ringbuffer %a" cause N.path_print fname ;
    r in
  fun ~now h ->
    let filter_chans rcpt chans =
      Hashtbl.filter (fun (t, _, _) ->
        if timed_out ~now t then (
          !logger.warning "OutRef: Timing out recipient %a"
            VOS.recipient_print rcpt ;
          false
        ) else true
      ) chans in
    Hashtbl.filteri (fun rcpt spec ->
      let valid_rcpt =
        match rcpt with
        | VOS.DirectFile fname ->
            filter "non-writable"
              (can_be_written_to spec.VOS.file_type) fname &&
            filter "obsolete" (not % file_is_obsolete) fname
        | VOS.IndirectFile _
        | VOS.SyncKey _ ->
            true in
      if valid_rcpt then
        let chans = filter_chans rcpt spec.channels in
        spec.channels <- chans ;
        Hashtbl.length chans > 0
      else
        false
    ) h

let read session site fq ~now =
  let k = output_specs_key site fq in
  match (Client.find session.ZMQClient.clt k).value with
  | exception Not_found ->
      Hashtbl.create 0
  | Value.OutputSpecs s ->
      filter_out_ref ~now s
  | v ->
      if not Value.(equal dummy v) then
        err_sync_type k v "an output specifications" ;
      Hashtbl.create 0

let read_live session site fq ~now =
  let h = read session site fq ~now in
  Hashtbl.filter_inplace (fun s ->
    Hashtbl.filteri_inplace (fun c _ ->
      c = Channel.live
    ) s.VOS.channels ;
    not (Hashtbl.is_empty s.channels)
  ) h ;
  h

let with_outref_locked ?while_ session site fq f =
  let k = output_specs_key site fq in
  let res = ref None in
  let exn = ref None in
  ZMQClient.send_cmd ?while_ session (Client.CltMsg.LockOrCreateKey (k, 3.0))
    ~on_ok:(fun () ->
      (try
        res := Some (f ())
      with e ->
        exn := Some e) ;
      ZMQClient.send_cmd ?while_ session (Client.CltMsg.UnlockKey k))
    ~on_ko:(fun () ->
      exn := Some (Failure (Printf.sprintf2 "Cannot lock %a" Key.print k))) ;
  (* Pull result and exception from the callbacks:
   * (FIXME: ZMQClient API) *)
  ZMQClient.process_until ~while_:(fun () ->
    Option.map_default (fun f -> f ()) true while_ &&
    !res = None && !exn = None) session ;
  Option.may raise !exn ;
  Option.get !res

let add ~now ?while_ session site fq out_fname
        ?(file_type=VOS.RingBuf) ?(timeout_date=0.) ?(num_sources= -1)
        ?(pid=0) ?(channel=Channel.live) fieldmask =
  let channels = Hashtbl.create 1 in
  Hashtbl.add channels channel (timeout_date, num_sources, pid) ;
  let file_spec = VOS.{ file_type ; fieldmask ; channels } in
  with_outref_locked ?while_ session site fq (fun () ->
    let h = read session site fq ~now in
    let rewrite file_spec =
      Hashtbl.replace h out_fname file_spec ;
      let k = output_specs_key site fq in
      write ?while_ session k h ;
      !logger.debug "OutRef: Adding %a to %a with fieldmask %a"
        VOS.recipient_print out_fname
        Key.print k
        RamenFieldMask.print fieldmask
    in
    match Hashtbl.find h out_fname with
    | exception Not_found ->
        rewrite file_spec
    | prev_spec ->
        let file_spec = combine_specs prev_spec file_spec in
        if VOS.eq prev_spec file_spec then (
          !logger.debug "OutRef: same entry: %a vs %a"
            VOS.file_spec_print prev_spec
            VOS.file_spec_print file_spec
        ) else rewrite file_spec)

let remove ~now ?while_ session site fq out_fname ?(pid=0) chan =
  with_outref_locked ?while_ session site fq (fun () ->
    let h = read session site fq ~now in
    match Hashtbl.find h out_fname with
    | exception Not_found -> ()
    | spec ->
        Hashtbl.modify_opt chan (function
          | None ->
              None
          | Some (_timeout, _count, current_pid) as prev ->
              if current_pid = 0 || current_pid = pid then
                None
              else
                prev (* Do not remove someone else's link! *)
        ) spec.VOS.channels ;
        if Hashtbl.is_empty spec.VOS.channels then
          Hashtbl.remove h out_fname ;
        let k = output_specs_key site fq in
        write ?while_ session k h ;
        !logger.debug "OutRef: Removed %a from %a"
          VOS.recipient_print out_fname
          Key.print k)

(* Check that fname is listed in outbuf_ref_fname for any non-timed out
 * channel: *)
let mem session site fq out_fname ~now =
  let h = read session site fq ~now in
  match Hashtbl.find h out_fname with
  | exception Not_found ->
      false
  | spec ->
      try
        Hashtbl.iter (fun _c (t, _, _) ->
          if not (timed_out ~now t) then raise Exit
        ) spec.VOS.channels ;
        false
      with Exit -> true

let remove_channel ~now ?while_ session site fq chan =
  with_outref_locked ?while_ session site fq (fun () ->
    let h = read session site fq ~now in
    Hashtbl.filter_inplace (fun spec ->
      Hashtbl.remove spec.VOS.channels chan ;
      not (Hashtbl.is_empty spec.VOS.channels)
    ) h ;
    let k = output_specs_key site fq in
    write ?while_ session k h ;
    !logger.debug "OutRef: Removed channel %a from %a"
      Channel.print chan
      Key.print k)

let check_spec_change rcpt old new_ =
  (* Or the rcpt should have changed: *)
  if new_.VOS.file_type <> old.VOS.file_type then
    Printf.sprintf2 "Output file %a changed file type \
                     from %s to %s while in use"
      VOS.recipient_print rcpt
      VOS.(string_of_file_type old.file_type)
      VOS.(string_of_file_type new_.file_type) |>
    failwith ;
  if new_.fieldmask <> old.fieldmask then
    Printf.sprintf2 "Output file %a changed field mask \
                     from %a to %a while in use"
      VOS.recipient_print rcpt
      RamenFieldMask.print old.fieldmask
      RamenFieldMask.print new_.fieldmask |>
    failwith
