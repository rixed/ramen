(* This module implements a simple event-based TCP server that transmit PDUs
 * prefixed with a 4 bytes length (in little endian). Used by the configuration
 * server. *)
open Batteries

open RamenLog
open RamenHelpersNoLog

let debug = false

type fd = Unix.file_descr

type fd_sets = fd list (* readables *)
             * fd list (* writables *)
             * fd list (* exceptional conditions *)

type handler =
  { (* Adds some files to the fd sets: *)
    register_files : fd_sets -> fd_sets ;
    (* Given the selected fd_sets, read from/write into them *)
    process_files : fd_sets -> unit }

let null_handler =
  { register_files = identity ;
    process_files = ignore }

let process_once ?(timeout= ~-.1.) handlers =
  let collect_all_monitored_files () =
    List.fold_left (fun files handler ->
      handler.register_files files
    ) ([], [], []) handlers
  and process_all_changed_files files =
    List.iter (fun handler ->
      handler.process_files files
    ) handlers in
  let rfiles, wfiles, efiles = collect_all_monitored_files () in
  if debug then
    !logger.debug "TcpSocket: selecting amongst %d+%d+%d files (timeo:%f)"
      (List.length rfiles) (List.length wfiles) (List.length efiles) timeout ;
  match Unix.select rfiles wfiles efiles timeout with
  | exception Unix.Unix_error (EINTR, _, s) ->
      !logger.debug "Interrupted by signal %s" s ;
      ()
  | [], [], _ ->
      !logger.debug "Nothing selected"
  | changed_files ->
      process_all_changed_files changed_files

(* We want to read/write entire messages so we need to buffer inputs and
 * outputs: *)

module BufferedIO =
struct
  (* Messages will be prefixed with a 32bits little endian word giving the
   * size (not including that prefix itself). *)

  type read_buffer =
    (* When nothing have been read so far from the next message: *)
    | NoSize
    (* In case less than 4 bytes could be read from the message prefix: *)
    (* Current size of the header, then 4 bytes initialized with 0: *)
    | PartialHeader of int * Bytes.t
    (* As soon as the size of the message is known a buffer is allocated
     * of that size. This int is the current size that's been read: *)
    | Msg of int * Bytes.t

  (* Try to read from [fd] and return a new [buf]. *)
  let try_read fd buf =
    let int_of_byte bytes n = Char.code (Bytes.get bytes n) in
    let int_of_bytes bytes =
      let rec loop n acc =
        if n >= Bytes.length bytes then acc else
        let d = int_of_byte bytes n in
        loop (n + 1) (acc lor (d lsl (8 * n))) in
      loop 0 0 in
    let msg_of header =
      let sz = int_of_bytes header in
      if sz <= 0 then failwith ("Invalid message size "^ string_of_int sz) ;
      if debug then !logger.debug "Preparing to read a message of %d bytes" sz ;
      Msg (0, Bytes.create sz) in
    match buf with
    | NoSize ->
        let bytes = Bytes.make 4 (Char.chr 0) in
        let read = Unix.read fd bytes 0 4 in
        if read = 0 then raise End_of_file else
        if read = 4 then msg_of bytes else
        PartialHeader (read, bytes)
    | PartialHeader (n, bytes) ->
        assert (n < 4) ;
        let read = Unix.read fd bytes n (4 - n) in
        if read = 0 then raise End_of_file else
        if n + read = 4 then msg_of bytes else
        PartialHeader (n + read, bytes)
    | Msg (ofs, bytes) ->
        assert (ofs < Bytes.length bytes) ;
        (match Unix.read fd bytes ofs (Bytes.length bytes - ofs) with
        | 0 -> raise End_of_file
        | sz -> Msg (ofs + sz, bytes))

  type write_buffer =
    { (* The message to be written, prefixed by the length (4 bytes, little
         endian), or an empty bytes, meaning close: *)
      bytes : Bytes.t ;
      (* How many bytes have been written so far. *)
      mutable written : int }

  (* Modifies the passed [buf]: *)
  let try_write fd buf =
    (* Closes are handled upriver: *)
    assert (Bytes.length buf.bytes > 0) ;
    let sz =
      let len = Bytes.length buf.bytes - buf.written in
      Unix.write fd buf.bytes buf.written len in
    if debug then !logger.debug "Sent %d bytes" sz ;
    buf.written <- buf.written + sz
end

type 'a peer =
  { (* Whatever we want in the user session: *)
    session : 'a ;
    mutable name : string ;
    mutable fd : fd option ;
    mutable inbuf : BufferedIO.read_buffer ;
    outbufs_lock : Mutex.t ;
    mutable outbufs : BufferedIO.write_buffer Queue.t }

let make_peer session name fd =
  if debug then !logger.debug "TcpSocket: make a new peer %S" name ;
  { session ; name ; fd = Some fd ;
    inbuf = BufferedIO.NoSize ;
    outbufs_lock = Mutex.create () ;
    outbufs = Queue.create () }

let send peer bytes =
  if peer.fd <> None then (
    let msg_len = Bytes.length bytes in
    let bytes =
      if msg_len = 0 then bytes else (
        (* Prepend the length in a single bytes that can then be written at
         * once (single syscall and single packet): *)
        if msg_len >= 0xfff_ffff then (
          !logger.error "Message suspiciously large, skipping (%d bytes)" msg_len ;
          assert false) ;
        let b = Bytes.create (4 + msg_len) in
        for i = 0 to 3 do
          Bytes.set b i (Char.chr ((msg_len lsr (8 * i)) land 0xff))
        done ;
        Bytes.blit bytes 0 b 4 msg_len ;
        b
      ) in
    if debug then
      !logger.debug "TcpSocket: Queuing a message of %d bytes to %s"
        msg_len peer.name ;
    (* It is assumed that contention is super low on this one, since writes
     * of the client thread are non locking (and server has no concurrent
     * thread at all) *)
    with_lock peer.outbufs_lock (fun () ->
      Queue.add (BufferedIO.{ bytes ; written = 0 }) peer.outbufs))

let close_peer reason peer =
  if debug then
    !logger.debug "TcpSocket: close_peer %s because of %s" peer.name reason ;
  Option.may (fun fd ->
    (* Make sure other threads see None before we close the fd *)
    peer.fd <- None ;
    Unix.close fd
  ) peer.fd

let empty_msg = Bytes.create 0

(* Process file descriptors for that peer, returning success: *)
let process_peer on_msg peer (r, w, _) =
  match peer.fd with
  | Some fd ->
      let ok =
        if List.mem fd r then (
          if debug then
            !logger.debug "TcpSocket: reading from %s" peer.name ;
          match BufferedIO.try_read fd peer.inbuf with
          | exception e ->
              if e = End_of_file then (
                !logger.debug "End of file while reading from %s, closing"
                  peer.name
              ) else
                !logger.error "Cannot read from %s: %s, closing connection"
                  peer.name
                  (Printexc.to_string e) ;
              close_peer "error" peer ;
              on_msg peer empty_msg ; (* Signal the end of file *)
              false
          | BufferedIO.Msg (ofs, bytes) when ofs >= Bytes.length bytes ->
              peer.inbuf <- NoSize ;
              (try on_msg peer bytes ; true
              with e ->
                !logger.error "Cannot answer on message %t from %s: \
                               %s, disconnecting"
                  (hex_print bytes)
                  peer.name
                  (Printexc.to_string e) ;
                close_peer "error" peer ;
                false)
          | inbuf ->
              peer.inbuf <- inbuf ;
              true
        ) else true in
      (* Now try writes on that peer: *)
      if not ok then false else
      if List.mem fd w then (
        if debug then !logger.debug "TcpSocket: writing to %s" peer.name ;
        with_lock peer.outbufs_lock (fun () ->
          match Queue.peek peer.outbufs with
          | exception Queue.Empty -> (* never mind *)
              true
          | outbuf ->
              if Bytes.length outbuf.bytes = 0 then (
                (* Closing *)
                close_peer "closing" peer ;
                false
              ) else (
                match BufferedIO.try_write fd outbuf with
                | exception e ->
                    !logger.error "Cannot write to %s: %s, closing connection"
                      peer.name
                      (Printexc.to_string e) ;
                    close_peer "error" peer ;
                    false
                | () ->
                    if outbuf.written >= Bytes.length outbuf.bytes then
                      ignore (Queue.take peer.outbufs) ;
                    true
              )
          )
      ) else true
  | None ->
      false

module Client =
struct
  exception Cannot_connect of string

  let () =
    Printexc.register_printer (function
      | Cannot_connect s ->
          Some (Printf.sprintf "Cannot connect to %s" s)
      | _ ->
          None)

  let connect host service_name =
    let open Unix in
    getaddrinfo host service_name [ AI_SOCKTYPE SOCK_STREAM ; AI_CANONNAME ] |>
    List.find_map (fun ai ->
      if debug then
        !logger.debug "TcpSocket: Trying to connect to %s:%s"
          ai.ai_canonname service_name ;
      try
        let sock = socket ai.ai_family ai.ai_socktype ai.ai_protocol in
        connect sock ai.ai_addr ;
        setsockopt_optint sock SO_LINGER (Some 3) ;
        setsockopt sock SO_KEEPALIVE true ;
        setsockopt sock TCP_NODELAY true ;
        set_close_on_exec sock ;
        (* FIXME: get back to a nice and clean event-based design one day: *)
        Unix.set_nonblock sock ;
        !logger.debug "Connected to %s" service_name ;
        Some sock
      with exn ->
        !logger.debug "Cannot connect: %s" (Printexc.to_string exn) ;
        None)

  let make_peer host service_name =
    let name = host ^":"^ service_name in
    match connect host service_name with
    | exception Not_found ->
        raise (Cannot_connect name)
    | fd ->
        make_peer () name fd

  type t =
    { peer : unit peer ;
      service_name : string ;
      recvtimeo : float ;
      ctrl_w : fd ;
      mutable handler : handler ;
      (* Since unfortunately ramen internal are pull-based received messages
       * are just waiting here until they are picked up. *)
      recvd_lock : Mutex.t ;
      recvd_cond : Condition.t ;
      recvd : Bytes.t Queue.t ;
      mutable select_thread : Thread.t option ;
      mutable timeo_thread : Thread.t option }

  (* Used just to send/receive useless bytes on the awakening pipe: *)
  let dummy_buf = Bytes.create 1

  let send t bytes =
    (* Without that explicit yield and in the absence of other scheduling point
     * (such as releasing the giant lock by logging for instance), the select
     * thread would not be scheduled often enough, resulting in *slow*
     * processing of incoming messages *)
    Thread.yield () ;
    send t.peer bytes ;
    (* Signal the processing thread: *)
    ignore (Unix.write t.ctrl_w dummy_buf 0 1)

  let on_msg t _peer bytes =
    with_lock t.recvd_lock (fun () ->
      Queue.add bytes t.recvd ;
      Condition.signal t.recvd_cond)

  let get_msg t =
    (* See [send] regarding this explicit yield *)
    Thread.yield () ;
    if debug then !logger.debug "TcpSocket: get_msg..." ;
    with_lock t.recvd_lock (fun () ->
      if t.recvtimeo > 0. && Queue.is_empty t.recvd then (
        if debug then !logger.debug "TcpSocket: waiting for condvar" ;
        (* Will very approximately timeout after recvtimeo: *)
        Condition.wait t.recvd_cond t.recvd_lock) ;
      try Some (Queue.take t.recvd)
      with Queue.Empty -> None)

  (* In client mode the reads and writes happen in a dedicated thread.
   * This thread must therefore also be in charge of closing the peer fd,
   * so users must only mark them for closing (with an empty msg). *)
  let select_thread t =
    while t.peer.fd <> None do
      process_once [ t.handler ] ;
    done

  let timeo_thread t =
    while t.peer.fd <> None do
      with_lock t.recvd_lock (fun () ->
        if debug then !logger.debug "timeo_thread: signaling!" ;
        Condition.signal t.recvd_cond) ;
      Thread.delay (2. *. t.recvtimeo)
    done

  (* Handle a single peer: *)
  let make recvtimeo host service_name =
    let peer = make_peer host service_name in
    let ctrl_r, ctrl_w = Unix.pipe ~cloexec:true () in
    Unix.set_nonblock ctrl_r ;
    Unix.set_nonblock ctrl_w ;
    let t =
      { peer ; recvtimeo ; service_name ; ctrl_w ;
        handler = null_handler ;
        recvd_lock = Mutex.create () ;
        recvd_cond = Condition.create () ;
        recvd = Queue.create () ;
        select_thread = None ;
        timeo_thread = None } in
    let register_files (r, w, e) =
      match peer.fd with
      | None ->
          ctrl_r :: r, w, e
      | Some fd ->
          fd :: ctrl_r :: r,
          (if Queue.is_empty peer.outbufs then w else fd :: w),
          e
    and process_files peer (r, _, _ as fds) =
      process_peer (on_msg t) peer fds |> ignore ;
      (* Although that pipe is just used to exit select() it still needs to
       * be emptied: *)
      if List.mem ctrl_r r then
        ignore (Unix.read ctrl_r dummy_buf 0 1) in
    t.handler <-
      { register_files ;
        process_files = process_files peer } ;
    t.select_thread <- Some (Thread.create select_thread t) ;
    if recvtimeo > 0. then
      t.timeo_thread <-
        Some (Thread.create timeo_thread t) ;
    t

  let shutdown t =
    !logger.debug "Shutting down connection to %s" t.service_name ;
    (* Will exit the select() and cause timeo thread to quit: *)
    send t empty_msg ;
    Option.may Thread.join t.select_thread ;
    Option.may Thread.join t.timeo_thread
end

module Server =
struct
  type 'a t =
    { name : string ;
      mutable peers : 'a peer list ;
      accepter_sock : fd ;
      mutable handler : handler }

  (* Handler that accept incoming connections: *)
  module Accepter =
  struct
    let name = "accepter"

    let register_files t (r, w, e) =
      (* Listening socket will became readable when a connection is pending: *)
      List.fold_left (fun r p ->
        match p.fd with
        | Some fd -> fd :: r
        | None -> r
      ) (t.accepter_sock :: r) t.peers,
      List.fold_left (fun w p ->
        match p.fd with
        | Some fd when not (Queue.is_empty p.outbufs) -> fd :: w
        | _ -> w
      ) w t.peers,
      e

    let process_files t make_session on_msg (r, _, _ as fds) =
      if List.mem t.accepter_sock r then (
        match Unix.accept t.accepter_sock with
        | exception e ->
            !logger.error "Cannot accept connection: %s"
              (Printexc.to_string e)
        | fd, sockaddr ->
            Unix.set_close_on_exec fd ;
            let session = make_session sockaddr in
            let name = name_of_sockaddr sockaddr in
            !logger.info "TcpSocket: accepted a connection to %s from %s"
              t.name name ;
            t.peers <- make_peer session name fd :: t.peers
      ) ;
      t.peers <-
        List.filter (fun peer ->
          process_peer on_msg peer fds
        ) t.peers
  end

  let make bind_addr service_name make_session on_msg =
    let open Unix in
    let port =
      try int_of_string service_name
      with _ -> (getservbyname service_name "tcp").s_port in
    let sockaddr = ADDR_INET (bind_addr, port) in
    let domain = domain_of_sockaddr sockaddr in
    let accepter_sock = socket ~cloexec:true domain SOCK_STREAM 0 in
    set_nonblock accepter_sock ;
    set_close_on_exec accepter_sock ;
    setsockopt accepter_sock SO_REUSEADDR true ;
    if debug then
      !logger.debug "TcpSocket: binding to %s" (string_of_sockaddr sockaddr) ;
    (try
      bind accepter_sock sockaddr ;
      (* Have a large pool of incoming SYNs in case many daemons and workers
       * rush to connect at once: *)
      listen accepter_sock 100
    with e ->
      !logger.error "Cannot bind and listen to %s: %s"
        (string_of_sockaddr sockaddr)
        (Printexc.to_string e) ;
      close accepter_sock ;
      raise e) ;
    let t =
      { name = service_name ;
        peers = [] ;
        accepter_sock ;
        handler = null_handler } in
    t.handler <-
      { register_files = Accepter.register_files t ;
        process_files = Accepter.process_files t make_session on_msg } ;
    t

  let shutdown t =
    if debug then
      !logger.debug "TcpSocket: Shutting down service %s" t.name ;
    List.iter (fun peer ->
      Option.may Unix.close peer.fd
    ) t.peers ;
    t.peers <- []
end
