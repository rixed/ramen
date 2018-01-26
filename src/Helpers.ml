open Batteries
open RamenLog

(*$inject open Batteries *)

let max_int_for_random = 0x3FFFFFFF

(* Small helper to return the ith entry of an array, capped to the last one.
 * Useful when we reach the last defined attempt while escalating an alert. *)
let get_cap a i =
  let len = Array.length a in
  assert (len > 0) ;
  a.(min i (len - 1))

let round_to_int f =
  int_of_float (Float.round f)

let retry
    ~on ?(first_delay=1.0) ?(min_delay=0.0001) ?(max_delay=10.0)
    ?(delay_adjust_ok=0.2) ?(delay_adjust_nok=1.1) ?delay_rec
    ?(max_retry=max_int) f =
  let open Lwt in
  let next_delay = ref first_delay in
  let rec loop max_retry x =
    (match%lwt f x with
    | exception e ->
      let%lwt should_retry =
        if max_retry > 0 then return_true else on e in
      if should_retry then (
        let delay = !next_delay in
        let delay = min delay max_delay in
        let delay = max delay min_delay in
        next_delay := !next_delay *. delay_adjust_nok ;
        Option.may (fun f -> f delay) delay_rec ;
        let%lwt () = Lwt_unix.sleep delay in
        loop (max_retry - 1) x
      ) else (
        !logger.error "Non-retryable error: %s"
          (Printexc.to_string e) ;
        fail e
      )
    | r ->
      next_delay := !next_delay *. delay_adjust_ok ;
      return r)
  in
  loop max_retry

let shell_quote s =
  "'"^ String.nreplace s "'" "'\\''" ^"'"

let sql_quote s =
  "'"^ String.nreplace s "'" "''" ^"'"

let list_existsi f l =
  match List.findi (fun i v -> f i v) l with
  | exception Not_found -> false
  | _ -> true

let print_exception e =
  !logger.error "Exception: %s\n%s"
    (Printexc.to_string e)
    (Printexc.get_backtrace ())

let looks_like_true s =
  s = "1" || (
    String.length s > 1 &&
    let lc = Char.lowercase s.[0] in lc = 'y' || lc = 't')

let time what f =
  let start = Unix.gettimeofday () in
  let res = f () in
  let dt = Unix.gettimeofday () -. start in
  !logger.info "%s in %gs." what dt ;
  res

(* TODO: add this into batteries *)

let is_directory f =
  try Sys.is_directory f with _ -> false

let mkdir_all ?(is_file=false) dir =
  let dir = if is_file then Filename.dirname dir else dir in
  let rec ensure_exist d =
    if String.length d > 0 && not (is_directory d) then (
      ensure_exist (Filename.dirname d) ;
      !logger.debug "mkdir %S" d ;
      try Unix.mkdir d 0o755
      with Unix.Unix_error (Unix.EEXIST, "mkdir", _) ->
        (* Happens when we have "somepath//someother" (dirname should handle this IMHO) *)
        ()
    ) in
  ensure_exist dir

let file_exists ?(maybe_empty=true) ?(has_perms=0) fname =
  let open Unix in
  match stat fname with
  | exception _ -> false
  | s ->
    (maybe_empty || s.st_size > 0) &&
    s.st_perm land has_perms = has_perms

let file_is_older_than age fname =
  let open Unix in
  match stat fname with
  | exception  _ -> false
  | s ->
    let now = gettimeofday () in
    s.st_mtime > now -. age

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
  | Unix.WEXITED 127 -> "shell couldn't be executed"
  | Unix.WEXITED code -> Printf.sprintf "terminated with code %d" code
  | Unix.WSIGNALED sign -> Printf.sprintf "killed by signal %s" (name_of_signal sign)
  | Unix.WSTOPPED sign -> Printf.sprintf "stopped by signal %s" (name_of_signal sign)

exception RunFailure of Unix.process_status
let () = Printexc.register_printer (function
  | RunFailure st -> Some ("RunFailure "^ string_of_process_status st)
  | _ -> None)

(* Low level stuff: run jobs and return lines: *)
let run ?timeout ?(to_stdin="") cmd =
  let open Lwt in
  let string_of_array a =
    Array.fold_left (fun s v ->
        s ^ (if String.length s > 0 then " " else "") ^ v
      ) "" a in
  if Array.length cmd < 1 then invalid_arg "cmd" ;
  !logger.debug "Running command %s" (string_of_array cmd) ;
  let%lwt lines =
    Lwt_process.with_process_full ?timeout (cmd.(0), cmd) (fun process ->
      (* What we write to stdin: *)
      let write_stdin =
        let%lwt () =
          try%lwt Lwt_io.write process#stdin to_stdin
          with (Unix.Unix_error (Unix.EPIPE, _, _)) -> return_unit in
        Lwt_io.close process#stdin in
      (* We need to read both stdout and stderr simultaneously or risk
       * interlocking: *)
      let lines = ref [] in
      let read_lines =
        match%lwt Lwt_io.read_lines process#stdout |>
                  Lwt_stream.to_list with
        | exception exn ->
          (* when this happens for some reason we are left with (null) *)
          let msg = Printexc.to_string exn in
          !logger.error "%s exception: %s"
            (string_of_array cmd) msg ;
          return_unit
        | l -> lines := l ; return_unit in
      let monitor_stderr =
        try%lwt
          Lwt_io.read_lines process#stderr |>
          Lwt_stream.iter (fun l ->
              !logger.error "%s stderr: %s" (string_of_array cmd) l)
        with exn ->
          !logger.error "Error while running %s: %s"
            (string_of_array cmd) (Printexc.to_string exn) ;
          return_unit in
      join [ write_stdin ; read_lines ; monitor_stderr ] >>
      match%lwt process#status with
      | Unix.WEXITED 0 ->
        return !lines
      | x ->
        !logger.error "Command '%s' %s"
          (string_of_array cmd)
          (string_of_process_status x) ;
        fail (RunFailure x)
    ) in
  return (String.concat "\n" lines)

let quote_at_start s =
  String.length s > 0 && s.[0] = '"'

let quote_at_end s =
  String.length s > 0 && s.[String.length s - 1] = '"'

exception InvalidCSVQuoting

let strings_of_csv separator line =
  (* If line is the empty string, String.nsplit returns an empty list
   * instead of a list with a single empty value. *)
  let strings =
    if line = "" then [ "" ]
    else String.nsplit line separator in
  (* Handle quoting in CSV values. TODO: enable/disable based on operation flag *)
  let strings', rem_s, has_quote =
    List.fold_left (fun (lst, prev_s, has_quote) s ->
      if prev_s = "" then (
        if quote_at_start s then (
          if quote_at_end s then (
            let len = String.length s in
            if len > 1 then String.sub s 1 (len - 2) :: lst, "", true
            else s :: lst, "", has_quote
          ) else lst, s, true
        ) else s :: lst, "", has_quote
      ) else (
        if quote_at_end s then (String.(lchop prev_s ^ rchop s) :: lst, "", true)
        else lst, prev_s ^ s, true
      )) ([], "", false) strings in
  if rem_s <> "" then raise InvalidCSVQuoting ;
  if has_quote then List.rev strings' else strings

(*$= strings_of_csv & ~printer:(IO.to_string (List.print String.print))
  [ "glop" ; "glop" ] (strings_of_csv " " "glop glop")
  [ "John" ; "+500" ] (strings_of_csv "," "\"John\",+500")
 *)

let lwt_read_whole_file fname =
  let open Lwt_io in
  let%lwt len = file_length fname in
  let len = Int64.to_int len in
  with_file ~mode:Input fname (fun ic ->
    let buf = Bytes.create len in
    let%lwt () = read_into_exactly ic buf 0 len in
    Lwt.return (Bytes.to_string buf))

let read_whole_file fname =
  File.with_file_in ~mode:[`text] fname IO.read_all

let file_print oc fname =
  let content = File.lines_of fname |> List.of_enum |> String.concat "\n" in
  String.print oc content

let getenv ?def n =
  try Sys.getenv n
  with Not_found ->
    match def with
    | Some d -> d
    | None ->
      Printf.sprintf "Cannot find envvar %s" n |>
      failwith

(* Trick from LWT: how to exit without executing the at_exit hooks: *)
external sys_exit : int -> 'a = "caml_sys_exit"

let do_daemonize () =
  let open Unix in
  if fork () > 0 then sys_exit 0 ;
  setsid () |> ignore ;
  (* Close all fds, ignoring errors in case they have been closed already: *)
  let null = openfile "/dev/null" [O_RDONLY] 0 in
  dup2 null stdin ;
  close null ;
  let null = openfile "/dev/null" [O_WRONLY; O_APPEND] 0 in
  dup2 null stdout ;
  dup2 null stderr ;
  close null

let random_string =
  let chars = "0123456789abcdefghijklmnopqrstuvwxyz" in
  let random_char _ =
    let i = Random.int (String.length chars) in chars.[i]
  in
  fun len ->
    Bytes.init len random_char |>
    Bytes.to_string

let max_simult ~max_count =
  let open Lwt in
  fun f ->
    let rec wait () =
      if !max_count <= 0 then
        Lwt_unix.sleep 0.3 >>= wait
      else (
        decr max_count ;
        finalize f (fun () ->
          incr max_count ; return_unit)) in
    wait ()

let max_coprocesses = ref max_int

(* Run given command using Lwt, logging its output in our log-file *)
let run_coprocess ?(max_count=max_coprocesses)
                  ?timeout ?(to_stdin="") cmd_name cmd =
  let prog, cmdline = cmd in
  !logger.debug "Executing: %s, %a" prog
    (Array.print String.print) cmdline ;
  max_simult ~max_count:max_count (fun () ->
    let open Lwt in
    Lwt_process.with_process_full ?timeout cmd (fun process ->
      let write_stdin =
        let%lwt () =
          try%lwt Lwt_io.write process#stdin to_stdin
          with Unix.Unix_error (Unix.EPIPE, _, _) -> return_unit in
        Lwt_io.close process#stdin
      and read_lines c =
        try%lwt
          Lwt_io.read_lines c |>
          Lwt_stream.iter (fun line ->
            !logger.info "%s: %s" cmd_name line)
        with exn ->
          let msg = Printexc.to_string exn in
          !logger.error "%s: Cannot read output: %s" cmd_name msg ;
          return_unit in
      join [ write_stdin ;
             read_lines process#stdout ;
             read_lines process#stderr ] >>
      process#status))

let start_with c f =
  String.length f > 0 && f.[0] = c

let is_virtual_field = start_with '#'

let is_private_field = start_with '_'

let string_of_time ts =
  let open Unix in
  let tm = localtime ts in
  Printf.sprintf "%04d-%02d-%02d %02dh%02dm%02ds"
    (tm.tm_year + 1900) (tm.tm_mon + 1) tm.tm_mday
    tm.tm_hour tm.tm_min tm.tm_sec

let udp_server ?(buffer_size=2000) ~inet_addr ~port k =
  let open Lwt in
  let open Lwt_unix in
  (* FIXME: it seems that binding that socket makes cohttp leack descriptors
   * when sending reports to ramen. Oh boy! *)
  let sock_of_domain domain =
    let sock = socket domain SOCK_DGRAM 0 in
    let%lwt () = bind sock (ADDR_INET (inet_addr, port)) in
    return sock in
  let%lwt sock =
    try%lwt sock_of_domain PF_INET6
    with _ -> sock_of_domain PF_INET in
  !logger.debug "Listening for datagrams on port %d" port ;
  let buffer = Bytes.create buffer_size in
  let rec forever () =
    let%lwt recv_len, sockaddr =
      recvfrom sock buffer 0 (Bytes.length buffer) [] in
    let sender =
      match sockaddr with
      | ADDR_INET (addr, port) ->
        Printf.sprintf "%s:%d" (Unix.string_of_inet_addr addr) port
      | _ -> "??" in
    k sender buffer recv_len >>=
    forever
  in
  forever ()

let hex_of =
  let zero = Char.code '0'
  and ten = Char.code 'a' - 10 in
  fun n ->
    if n < 10 then Char.chr (zero + n)
    else Char.chr (ten + n)

let rec restart_on_failure f x =
  try%lwt f x
  with e ->
    print_exception e ;
    let%lwt () = Lwt_unix.sleep 0.5 in
    restart_on_failure f x
