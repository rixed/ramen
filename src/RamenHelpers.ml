open Batteries
open Stdint
open RamenHelpersNoLog
open RamenLog
open RamenConsts
module Atomic = RamenAtomic

(*$inject open Batteries *)

let print_exception ?(what="Exception") e =
  !logger.error "%s: %s\n%s" what
    (Printexc.to_string e)
    (Printexc.get_backtrace ())

exception Timeout

let retry
    ~on ?(first_delay=1.0) ?(min_delay=0.0001) ?(max_delay=10.0)
    ?(delay_adjust_ok=0.2) ?(delay_adjust_nok=1.5) ?delay_rec
    ?max_retry ?max_retry_time ?(while_=always) f =
  let next_delay = ref first_delay in
  let started = Unix.gettimeofday () in
  let can_wait_longer () =
    match max_retry_time with
    | None -> true
    | Some d -> Unix.gettimeofday () -. started < d in
  let rec loop num_try x =
    if not (while_ ()) then raise Exit
    else if not (can_wait_longer ()) then raise Timeout
    else match f x with
      | exception e ->
        let retry_on_this = on e in
        let should_retry =
          Option.map_default (fun max -> num_try < max) true max_retry &&
          retry_on_this in
        if should_retry then (
          if not (while_ ()) then raise Exit ; (* Before sleeping *)
          let delay = !next_delay in
          let delay = min delay max_delay in
          let delay = max delay min_delay in
          next_delay := !next_delay *. delay_adjust_nok ;
          Option.may (fun f -> f delay) delay_rec ;
          Unix.sleepf delay ;
          (loop [@tailcall]) (num_try + 1) x
        ) else (
          !logger.debug "Non-retryable error: %s after %d attempt%s"
            (Printexc.to_string e) num_try (if num_try > 1 then "s" else "") ;
          raise e)
      | r ->
        next_delay := !next_delay *. delay_adjust_ok ;
        r
  in
  loop 1

let log_exceptions ?what f =
  try f ()
  with e ->
    let bt = Printexc.get_raw_backtrace () in
    if e <> Exit then print_exception ?what e ;
    Printexc.raise_with_backtrace e bt

let log_and_ignore_exceptions ?what f x =
  try f x
  with Exit -> ()
     | e ->
        !logger.info "%sIgnoring that exception: %s"
          (match what with None -> "" | Some w -> w ^ ": ")
          (Printexc.to_string e)

let default_on_exception def ?what f x =
  try f x
  with e -> print_exception ?what e ; def

let time what f =
  let start = Unix.gettimeofday () in
  let res = f () in
  let dt = Unix.gettimeofday () -. start in
  !logger.info "%s in %gs." what dt ;
  res

(*
 * Some Unix utilities
 *)

let name_of_signal s =
  let open Sys in
  if s = sigabrt then "ABORT"
  else if s = sigalrm then "ALRM"
  else if s = sigfpe then "FPE"
  else if s = sighup then "HUP"
  else if s = sigill then "ILL"
  else if s = sigint then "INT"
  else if s = sigkill then "KILL"
  else if s = sigpipe then "PIPE"
  else if s = sigquit then "QUIT"
  else if s = sigsegv then "SEGV"
  else if s = sigterm then "TERM"
  else if s = sigusr1 then "USR1"
  else if s = sigusr2 then "USR2"
  else if s = sigchld then "CHLD"
  else if s = sigcont then "CONT"
  else if s = sigstop then "STOP"
  else if s = sigtstp then "TSTP"
  else if s = sigttin then "TTIN"
  else if s = sigttou then "TTOU"
  else if s = sigvtalrm then "VTALRM"
  else if s = sigprof then "PROF"
  else if s = sigbus then "BUS"
  else if s = sigpoll then "POLL"
  else if s = sigsys then "SYS"
  else if s = sigtrap then "TRAP"
  else if s = sigurg then "URG"
  else if s = sigxcpu then "XCPU"
  else if s = sigxfsz then "XFSZ"
  else "Unknown OCaml signal number "^ string_of_int s

let string_of_process_status = function
  | Unix.WEXITED 126 ->
      "couldn't execve after fork"
  | Unix.WEXITED 127 ->
      "couldn't be executed"
  | Unix.WEXITED code ->
      Printf.sprintf "terminated with code %d (%s)"
        code (ExitCodes.string_of_code code)
  | Unix.WSIGNALED sign ->
      Printf.sprintf "killed by signal %s" (name_of_signal sign)
  | Unix.WSTOPPED sign ->
      Printf.sprintf "stopped by signal %s" (name_of_signal sign)

let rec waitall_once ?expected_status ~what pids =
  let open Unix in
  Set.Int.filter (fun pid ->
    let complain is_err status =
      (if is_err then !logger.error else !logger.debug)
        "%s: %s" what (string_of_process_status status) in
    let flags = [ WUNTRACED ] in
    let flags =
      if set_int_is_singleton pids then flags
      else WNOHANG :: flags in
    match restart_on_EINTR (waitpid flags) pid with
    | 0, _ -> true
    | _, (WEXITED c as status) ->
        complain
          (expected_status <> Some c && expected_status <> None)
          status ;
        false
    | _, status ->
        complain true status ;
        false
  ) pids

let waitall ?(while_=always) ?expected_status ~what pids =
  let rec loop pids =
    if while_ () && not (Set.Int.is_empty pids) then (
      let pids = waitall_once ?expected_status ~what pids in
      if not (Set.Int.is_empty pids) then (
        Unix.sleep 1 ;
        loop pids))
  in
  loop pids

let waitpid_log ?expected_status ~what pid =
  waitall ?expected_status ~what (Set.Int.singleton pid)

exception InvalidCSVQuoting

(* If [may_quote] is false, no attempt at unquoting is performed.
 * Also, if not empty, interpret the escape sequence [escape_seq] "as usual".
 * The output are strings that are ready for [RamenTypeConverters.XXX_of_string]
 * family of functions.
 * For instance, if we end up converting this CSV field into a string, then it is
 * expected to be a valid Ramen string literals, for instance being escaped using
 * the usual \b, \n etc. So, if [escape_seq] is anything different it must be
 * converted. TODO.
 * Returns both the number of bytes consumed, and the list of strings. *)
type csv_line_phase = MaybeQuote | PastQuote | PastEscape | PastEnd
let strings_of_csv separator may_quote escape_seq bytes start stop =
  let escape_seq = Bytes.of_string escape_seq in
  let escape_len = Bytes.length escape_seq in
  let escape_seq_list = bytes_to_list escape_seq in
  let separator = Bytes.of_string separator in
  let separator_len = Bytes.length separator in
  assert (Bytes.length bytes >= max start stop) ;
  assert (min start stop >= 0) ;
  let print_line oc =
    String.print_quoted oc Bytes.(sub bytes start (stop - start) |> to_string) in
  (* Called when the field end is reached. Return its value. *)
  let end_field i field_start field_chars had_escape quoted phase =
    (match phase with
    | MaybeQuote ->
        (* Empty unquoted line, resulting in a single empty string: *)
        assert (i = field_start) ;
        assert (not quoted)
    | PastQuote ->
        assert (not quoted || may_quote) ;
        if quoted then
          (* either the line was truncated, or the initial quote was bogus: *)
          !logger.warning "Cannot find end quote (started at %d) in CSV line %t"
            (field_start - 1) print_line
    | PastEscape ->
        !logger.warning "Incomplete escape sequence (started at %d) in CSV line %t"
          (i - escape_len) print_line
    | PastEnd ->
        ()) ;
    if had_escape then
      String.of_list (List.rev field_chars)
    else
      Bytes.sub bytes field_start (i - field_start) |> Bytes.to_string
  in
  (* Note: had_escape means field_chars <> substring of bytes. *)
  let rec loop strs i field_start field_chars had_escape quoted phase =
    assert (i >= start) ;
    if i >= stop then (
      let s = end_field i field_start field_chars had_escape quoted phase in
      stop - start, List.rev (s :: strs)
    ) else (
      let c = Bytes.get bytes i in
      match phase with
      | MaybeQuote ->
          if may_quote && c = '\"' then
            loop strs (i + 1) (i + 1) [] false true PastQuote
          else
            loop strs i i [] false false PastQuote
      | PastQuote ->
          if quoted && c = '\"' then
            let s = end_field i field_start field_chars had_escape false PastEnd in
            loop (s :: strs) (i + 1) field_start field_chars had_escape quoted PastEnd
          else if escape_len > 0 &&
                  escape_len <= stop - i &&
                  bytes_sub_eq bytes i escape_seq 0 escape_len
          then
            loop strs (i + escape_len) field_start field_chars
                 had_escape quoted PastEscape
          else if not quoted &&
                  separator_len > 0 &&
                  separator_len <= stop - i &&
                  bytes_sub_eq bytes i separator 0 separator_len
          then
            let s = end_field i field_start field_chars had_escape false PastEnd in
            let field_start = i + separator_len in
            loop (s :: strs) field_start field_start [] false false MaybeQuote
          else
            loop strs (i + 1) field_start (c :: field_chars)
                 had_escape quoted PastQuote
      | PastEscape ->
          (* Try a single char replacement: *)
          let simple_rep =
            match c with
            | 'b' -> Some '\b' | 'r' -> Some '\r' | 'n' -> Some '\n'
            | 't' -> Some '\t' | '0' -> Some '\000' | '\\' -> Some '\\'
            | '"' -> Some '"'
            (* Preserve escape sequences for other characters, as
             * ClickHouse uses \N special sequence for nulls (despite
             * it also un-escape any characters.) *)
            | _ -> None in
          (match simple_rep with
          | Some c ->
              loop strs (i + 1) field_start (c :: field_chars)
                   true quoted PastQuote
          | None ->
              (* TODO: long escape sequences *)
              (* Preserve the whole sequence and try again that char: *)
              let field_chars =
                List.rev_append escape_seq_list field_chars in
              loop strs i field_start field_chars
                   had_escape quoted PastQuote)
      | PastEnd ->
          (* Only useful after the second quote has been read. *)
          assert (i < stop) ; (* or we would have exited earlier *)
          (* Skip over blanks but not over the separator *)
          if separator_len > 0 &&
             separator_len <= stop - i &&
             bytes_sub_eq bytes i separator 0 separator_len
          then
            let i = i + separator_len in
            loop strs i i [] false false MaybeQuote
          else if Char.is_whitespace c then
            loop strs (i + 1) field_start field_chars had_escape quoted PastEnd
          else (
            !logger.warning "Cannot find separator (at %d) in CSV line %t"
              i print_line ;
            loop strs i i [] false false MaybeQuote
          )
    )
  in
  loop [] start start [] false false MaybeQuote

(*$inject
  let strings_of_csv_string separator may_quote escape_seq str =
    let bytes = Bytes.of_string str in
    let stop = Bytes.length bytes in
    strings_of_csv separator may_quote escape_seq bytes 0 stop |> snd
*)
(*$= strings_of_csv_string & ~printer:(IO.to_string (List.print String.print))
  [ "glop" ; "glop" ] \
    (strings_of_csv_string " " true "\\" "glop glop")
  [ "John" ; "+500" ] \
    (strings_of_csv_string "," true "\\" "\"John\",+500")
  [ "\"John" ; "+500" ] \
    (strings_of_csv_string "," false "\\" "\"John,+500")
  [ "\"John\"" ; "+500" ] \
    (strings_of_csv_string "," false "\\" "\"John\",+500")
  [ "gl\\op" ; "\\\t\n\\N" ; "42" ] \
    (strings_of_csv_string "\t" false "\\" "gl\\op\t\\\\\\t\\n\\N\t42")
  [ "glop" ; "\\" ; "42" ] \
    (strings_of_csv_string "\t" false "\\" "glop\t\\\\\t42")
  [ "glop" ; "" ] \
    (strings_of_csv_string "\t" false "\\" "glop\t")
*)

let max_simult ~what ~max_count f =
  let rec loop () =
    if Atomic.Counter.get max_count <= 0 then (
      !logger.debug "Too many %s pending, waiting..." what ;
      Unix.sleepf (0.2 +. Random.float 0.2) ;
      loop ()
    ) else (
      Atomic.Counter.decr max_count ;
      finally (fun () -> Atomic.Counter.incr max_count)
        f ()
    ) in
  loop ()

(* Run given command, logging its output in our log-file *)
type coprocess_environment = EmptyEnv | ReducedEnv | FullEnv

let make_env = function
  | EmptyEnv -> [||]
  | ReducedEnv ->
      [| "HOME="^ getenv ~def:"/tmp" "HOME" ;
         "PATH="^ getenv ~def:"/usr/bin:/bin" "PATH" |]
  | FullEnv -> Unix.environment ()

let run_coprocess ~max_count ?(to_stdin="") ?(env=ReducedEnv) cmd_name cmd =
  !logger.debug "Executing: %s" cmd ;
  max_simult ~what:cmd_name ~max_count (fun () ->
    let open Legacy.Unix in
    let env = make_env env in
    let (pstdout, pstdin, pstderr as chans) = open_process_full cmd env in
    let pstdout = descr_of_in_channel pstdout
    and pstdin = descr_of_out_channel pstdin
    and pstderr = descr_of_in_channel pstderr in
    let status = ref None in
    let write_stdin () =
      try
        let len = String.length to_stdin in
        let w = write_substring pstdin to_stdin 0 len in
        if w < len then
          !logger.error "Can only write %d/%d bytes" w len
      with Unix_error (EPIPE, _, _) -> ()
    and read_out c =
      try
        read_lines c |>
        Enum.iter (fun line ->
          !logger.info "%s: %s" cmd_name line)
      with exn ->
        let msg = Printexc.to_string exn in
        !logger.error "%s: Cannot read output: %s" cmd_name msg
    in
    finally (fun () -> status := Some (close_process_full chans))
      (List.iter Thread.join)
        [ Thread.create write_stdin () ;
          Thread.create read_out pstdout ;
          Thread.create read_out pstderr ] ;
    !status)

let udp_server ?(buffer_size=2000) ~what ~inet_addr ~port ?(while_=always) k =
  if port < 0 || port > 65535 then
    Printf.sprintf "%s: port number (%d) not within valid range" what port |>
    failwith ;
  let open Unix in
  (* FIXME: it seems that binding that socket makes cohttp leak descriptors
   * when sending reports to ramen. Oh boy! *)
  let sock_of_domain domain =
    let sock = socket domain SOCK_DGRAM 0 in
    bind sock (ADDR_INET (inet_addr, port)) ;
    sock in
  let sock =
    try sock_of_domain PF_INET6
    with _ -> sock_of_domain PF_INET in
  !logger.info "Listening for datagrams on %s:%d"
    (Unix.string_of_inet_addr inet_addr) port ;
  let buffer = Bytes.create buffer_size in
  let rec until_exit () =
    if while_ () then
      let recv_len, sockaddr =
        restart_on_eintr ~while_ (fun () ->
          recvfrom sock buffer 0 (Bytes.length buffer) []) () in
      !logger.debug "Received %d bytes on UDP port %d" recv_len port ;
      let sender =
        match sockaddr with
        | ADDR_INET (addr, _port) -> Some addr
        | _ -> None in
      k ?sender buffer recv_len ;
      (until_exit [@tailcall]) ()
  in
  try until_exit ()
  with Exit -> (* from the above restart_on_eintr *)
    ()

let fail_for_good = ref false
let rec restart_on_failure ?(while_=always) what f x =
  if !fail_for_good then
    f x
  else
    try f x
    with e ->
      match e with
        | Stdlib.Exit -> ()
        | _ -> print_exception e ;
      if while_ () then (
        !logger.error "Will restart %s..." what ;
        Unix.sleepf (0.5 +. Random.float 0.5) ;
        (restart_on_failure [@tailcall]) ~while_ what f x)

let option_get what where = function
  | Some x -> x
  | None ->
      !logger.error "Forced the None value of %s in %s" what where ;
      invalid_arg "option_get"

let cached2 cache_name reread time =
  (* Cache is a hash from some key to last access time, last data time,
   * and data. *)
  !logger.debug "Create a new cache for %s" cache_name ;
  let cache = Hashtbl.create 31 in
  let next_clean = ref (Unix.time () +. Random.float cache_clean_after) in
  fun k u ->
    let ret = ref None in
    let now = Unix.time () (* Used only for access time not cache validity *) in
    Hashtbl.modify_opt k (function
      | None ->
          let t = time k u
          and v = reread k u in
          ret := Some v ;
          Some (ref now, t, v)
      | Some (a, t, v) as prev ->
          let t' = time k u in
          if t' <= t then (
            ret := Some v ;
            a := now ;
            prev
          ) else (
            let v = reread k u in
            ret := Some v ;
            Some (ref now, t', v)
          )
    ) cache ;
    (* Clean the cache every now and then *)
    if now > !next_clean then (
      Hashtbl.filter_inplace (fun (a, _, _) ->
        now -. !a  < cache_clean_after
      ) cache ;
      next_clean := now +. Random.float cache_clean_after ;
      !logger.debug "Cache size is now %d" (Hashtbl.length cache)
    ) ;
    Option.get !ret

(* Same as above without the additional user parameter: *)
let cached cache_name reread time =
  let c = cached2 cache_name (fun k () -> reread k)
                             (fun k () -> time k) in
  fun k -> c k ()

(* Replace ${tuple.field} by the actual value the passed string: *)
(* FIXME: Why not use RamenStringExpansion? *)
let subst_tuple_fields =
  let open Str in
  let re =
    regexp "\\${\\(\\([_a-zA-Z0-9.]+\\)\\.\\)?\\([_a-zA-Z0-9]+\\)}" in
  fun tuples text ->
    global_substitute re (fun s ->
      let tuple_name = try matched_group 2 s with Not_found -> "" in
      let field_name = matched_group 3 s in
      let tot_name =
        tuple_name ^ (if tuple_name <> "" then "." else "") ^ field_name in
      let search_all () =
        try
          List.find_map (fun (_, finder) ->
            try Some (finder field_name)
            with Not_found -> None
          ) tuples
        with Not_found ->
          !logger.error "Field %S used in text substitution is not \
                         present in any tuple." field_name ;
          "??"^ field_name ^"??"
      in
      match List.find (fun (names, _finder) ->
              List.mem tuple_name names
            ) tuples with
      | exception Not_found ->
        if tuple_name = "" then search_all () else (
          !logger.error "Unknown tuple %S used in text substitution!"
            tuple_name ;
          "??"^ tot_name ^"??")
      | _, finder ->
        (* We may provide "" explicitely to force order of search but
         * then if the field is not there we still want to look for it
         * elsewhere: *)
        try finder field_name
        with Not_found ->
          if tuple_name = "" then search_all () else (
            !logger.error "Field %S used in text substitution is not \
                           present in that tuple!" tot_name ;
            "??"^ tot_name ^"??")
    ) text

let rec reach_fixed_point ?max_try f =
  match max_try with Some n when n <= 0 -> false
  | _ ->
    if f () then (
      !logger.debug "Looping to reach fixed point" ;
      reach_fixed_point ?max_try:(Option.map pred max_try) f
    ) else true

(* We need an accept that stops waiting whenever the while_ condition become
 * false. It is not enough to check while_ on EINTR since the actual signal handler
 * will be resumed and the OCaml signal handler might not have run yet when
 * the accept is interrupted by EINTR. So we have to summon select.
 * raise Exit when while_ says so. *)
let rec my_accept ~while_ sock =
  let open Legacy.Unix in
  match restart_on_eintr ~while_ (select [sock] [] []) 0.5 with
  | [], _, _ ->
      if while_ () then my_accept ~while_ sock
      else raise Exit
  | _ ->
      restart_on_eintr ~while_ (accept ~cloexec:true) sock

(* This is a version of [Unix.establish_server] that pass the file
 * descriptor instead of buffered channels. Also, we want a way to stop
 * the server: *)
let forking_server ~while_ ~service_name sockaddr server_fun =
  let open Legacy.Unix in
  let sock =
    socket ~cloexec:true (domain_of_sockaddr sockaddr) SOCK_STREAM 0 in
  (* Keep an eye on my sons pids: *)
  let sons = Atomic.Set.make () in
  let killer_thread =
    Thread.create (fun () ->
      !logger.debug "Starting killer thread..." ;
      let stop_since = ref 0. in
      while true do
        log_and_ignore_exceptions ~what:"Killer thread" (fun () ->
          let now = gettimeofday () in
          let continue = while_ () in
          (* If we want to quit, kill the sons: *)
          if not continue then (
            if !stop_since = 0. then stop_since := now ;
            if not (Atomic.Set.is_empty sons) then (
              !logger.debug "Killing %d %a servers..."
                (Atomic.Set.cardinal sons)
                N.service_print service_name ;
              Atomic.Set.iter sons (fun (pid, _) ->
                !logger.info "Killing %d" pid ;
                let what =
                  Printf.sprintf2 "stopping %a servers"
                    N.service_print service_name in
                let signal =
                  let open Sys in
                  if now -. !stop_since > 3. then sigkill else sigterm in
                log_and_ignore_exceptions ~what
                  (kill pid) signal)
            ) else (
              !logger.debug "Quit killer thread" ;
              Thread.exit ()
            )
          ) ;
          (* Collect the sons statuses: *)
          Atomic.Set.filter sons
            (fun (pid, start) ->
              match BatUnix.restart_on_EINTR (waitpid [ WNOHANG ]) pid with
              | 0, _ ->
                  true
              | _, status ->
                  let dt = now -. start in
                  (if status = WEXITED 0 then !logger.info
                   else !logger.error)
                    "%a server %d %s after %fs"
                      N.service_print service_name
                      pid
                      (string_of_process_status status)
                      dt ;
                  false) ;
          sleep 1) ()
      done
    ) () in
  (* Now fork a new server for each new connection: *)
  finally
    (fun () -> close sock)
    (fun () ->
      setsockopt sock SO_REUSEADDR true ;
      bind sock sockaddr ;
      listen sock 5 ;
      while while_ () do
        match my_accept ~while_ sock with
        | exception Exit ->
            !logger.debug "%a stops accepting connections."
              N.service_print service_name
        | s, _caller ->
            (* Before forking, advance the PRNG so that all children do not
             * re-init their own PRNG with the same number: *)
            let prng_init = Random.bits () in
            flush_all () ;
            (match fork () with
            | 0 ->
                let what = "forked "^ (service_name :> string) ^" server" in
                (try
                  (* Server process must not escape this scope or it would
                   * try to use sock again! *)
                  Random.init prng_init ;
                  set_prefix (string_of_int (Unix.getpid ())) ;
                  close sock ;
                  (try
                    server_fun s ;
                    exit 0
                  with End_of_file ->
                      !logger.info "%s: client disconnected, exiting" what ;
                      exit 0
                    | Exit ->
                      !logger.info "%s: time to quit" what ;
                      exit 0)
                with e ->
                  print_exception ~what e ;
                  exit ExitCodes.uncaught_exception)
            | pid ->
                close s ;
                !logger.info "Forked server with pid %d" pid ;
                Atomic.Set.add sons (pid, Unix.gettimeofday ()))
      done
    ) () ;
  !logger.info "Waiting for killer thread to finish..." ;
  Thread.join killer_thread ;
  !logger.debug "Killed thread finished!"

(*
 * Some graph utilities
 *)

(* Given a set of edges, return the path (reverted) between any two vertices
 * or raise Not_found.
 * It is assumed that, as is the case with the running config or the graph
 * of builders, the graph is mostly a tree.
 * Edges are given as a folder passing each vertex and its "descendants"
 * as the vertex identifier and vertex content. *)
type ('id, 'vtx) fold_t =
  { fold : 'usr. 'id -> ('usr -> 'id -> 'vtx -> 'usr) -> 'usr -> 'usr }

let path_in_graph fold ?(max_len=50) ~src ~dst =
  (* Complete the given path towards [dst], return both the path (reverted)
   * and its length (which is not larger than max_len): *)
  let rec loop max_len prev prev_len id =
    !logger.debug "Looking for a path from %S to %S of max length %d"
      (dump id) (dump dst) max_len ;
    if prev_len > max_len then failwith "Path too long"
    else if id = dst then prev, prev_len
    else (
      (* Try each edge: *)
      let best_path_opt =
        fold.fold id (fun prev_best_opt id' v ->
          match loop (max_len - 1) (v :: prev) (prev_len + 1) id' with
          | exception _ -> prev_best_opt
          | _, path_len as res ->
              if match prev_best_opt with
                 | None -> true
                 | Some (_, best_len) -> path_len < best_len
              then Some res
              else prev_best_opt
        ) None
      in
      match best_path_opt with
      | None ->
          Printf.sprintf "No path from %s to %s" (dump src) (dump dst) |>
          failwith
      | Some (path, path_len) ->
          path, path_len)
  in
  let path_rev, _ = loop max_len [] 0 src in
  path_rev

let invalid_byte_for what x =
  !logger.error "Invalid byte 0x%0xd for %s" x what ;
  assert false
