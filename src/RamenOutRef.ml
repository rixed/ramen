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

(* How are data represented on disk: *)
type out_ref_conf =
  (string (* dest file *), file_spec_conf) Hashtbl.t [@@ppp PPP_OCaml]
and file_spec_conf =
  (* field mask: *)
  string *
  (* per channel timeouts (0 = no timeout): *)
  (RamenChannel.t, float) Hashtbl.t
  [@@ppp PPP_OCaml]

(* ...and internally, where the field mask is a proper list of booleans: *)
type file_spec =
  { field_mask : bool list ;
    channels : (RamenChannel.t, float) Hashtbl.t }

let print_out_specs oc =
  Hashtbl.print String.print (fun oc s ->
    Hashtbl.print RamenChannel.print Float.print oc s.channels) oc

let string_of_field_mask mask =
  List.map (function true -> 'X' | false -> '_') mask |>
  String.of_list

(* [combine_specs s1 s2] returns the result of replacing [s1] with [s2].
 * Basically, new fields prevail and we merge channels, keeping the longer
 * timeout: *)
let combine_specs (_, c1) (s, c2) =
  s,
  hashtbl_merge c1 c2
    (fun _ t1 t2 -> match t1, t2 with
    | Some t, None | None, Some t -> Some t
    | Some t1, Some t2 -> Some (Float.max t1 t2)
    | _ -> assert false)

let timed_out now t = t > 0. && now > t

let write_ fname fd c =
  fail_with_context ("Writing out_ref "^ fname) (fun () ->
    ppp_to_fd out_ref_conf_ppp_ocaml fd c)

let read_ fname fd =
  fail_with_context ("Reading out_ref "^ fname) (fun () ->
    ppp_of_fd ~default:"{}" out_ref_conf_ppp_ocaml fd)

let read fname =
  RamenAdvLock.with_r_lock fname (fun fd ->
    let now = Unix.gettimeofday () in
    let field_mask_of_string s =
      String.to_list s |> List.map ((=) 'X') in
    read_ fname fd |>
    Hashtbl.filter_map (fun _ (mask_str, chans) ->
      let channels =
        Hashtbl.filter (fun t -> not (timed_out now t)) chans in
      if Hashtbl.length chans > 0 then
        Some { field_mask = field_mask_of_string mask_str ; channels }
      else None))

let add_ fname fd out_fname timeout chan field_mask =
  let channels = Hashtbl.create 1 in
  Hashtbl.add channels chan timeout ;
  let file_spec =
    string_of_field_mask field_mask,
    channels in
  let h =
    try read_ fname fd
    with Sys_error _ ->
      Hashtbl.create 1
    in
  let rewrite () =
    Hashtbl.replace h out_fname file_spec ;
    write_ fname fd h ;
    !logger.debug "Adding %s to %s" out_fname fname in
  match Hashtbl.find h out_fname with
  | exception Not_found -> rewrite ()
  | prev_spec ->
    let file_spec = combine_specs prev_spec file_spec in
    if prev_spec <> file_spec then rewrite ()

let add fname ?(timeout=0.) ?(channel=RamenChannel.live) out_fname
        field_mask =
  RamenAdvLock.with_w_lock fname (fun fd ->
    (*!logger.debug "Got write lock for add on %s" fname ;*)
    add_ fname fd out_fname timeout channel field_mask)

let remove_ fname fd out_fname chan =
  let h = read_ fname fd in
  match Hashtbl.find h out_fname with
  | exception Not_found -> ()
  | _, channels ->
      if Hashtbl.mem channels chan then (
        if Hashtbl.length channels > 1 then
          Hashtbl.remove channels chan
        else
          Hashtbl.remove h out_fname) ;
      write_ fname fd h ;
      !logger.debug "Removed %s from %s" out_fname fname

let remove fname out_fname chan =
  RamenAdvLock.with_w_lock fname (fun fd ->
    (*!logger.debug "Got write lock for remove on %s" fname ;*)
    remove_ fname fd out_fname chan)

(* Check that fname is listed in outbuf_ref_fname for any non-timed out
 * channel: *)
let mem_ fname fd out_fname now =
  let h = read_ fname fd in
  match Hashtbl.find h out_fname with
  | exception Not_found -> false
  | _, channels ->
      try
        Hashtbl.iter (fun _c t ->
          if not (timed_out now t) then raise Exit
        ) channels ;
        false
      with Exit -> true

let mem fname out_fname now =
  RamenAdvLock.with_r_lock fname (fun fd ->
    (*!logger.debug "Got read lock for mem on %s" fname ;*)
    mem_ fname fd out_fname now)

let remove_channel fname chan =
  RamenAdvLock.with_w_lock fname (fun fd ->
    let h = read_ fname fd in
    Hashtbl.filter_inplace (fun (_, channels) ->
      Hashtbl.remove channels chan ;
      not (Hashtbl.is_empty channels)
    ) h ;
    write_ fname fd h) ;
  !logger.debug "Removed chan %d from %s" chan fname

(*$inject open Batteries *)

(*$R
  let outref_fname = Filename.temp_file "ramen_test_" ".out" in

  (* out_ref is initially empty/absent: *)
  let now = Unix.gettimeofday () in
  assert_bool "outref is empty" (not (mem outref_fname "dest1" now)) ;

  add outref_fname "dest1" [true] ;
  assert_bool "dest1 is now in outref" (mem outref_fname "dest1" now) ;

  add outref_fname "dest2" ~channel:1 [true] ;
  assert_bool "dest2 is now in outref" (mem outref_fname "dest1" now) ;
  remove_channel outref_fname 1 ;
  assert_bool "no more chan 1" (not (mem outref_fname "dest2" now)) ;

  (* If all went well: *)
  RamenHelpers.safe_unlink outref_fname
*)
