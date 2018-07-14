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

(* The original Float.to_string adds a useless dot at the end of
 * round numbers: *)
let nice_string_of_float v =
  let s = Float.to_string v in
  assert (String.length s > 0) ;
  if s.[String.length s - 1] <> '.' then s else String.rchop s

exception Timeout

let retry
    ~on ?(first_delay=1.0) ?(min_delay=0.0001) ?(max_delay=10.0)
    ?(delay_adjust_ok=0.2) ?(delay_adjust_nok=1.5) ?delay_rec
    ?max_retry ?max_retry_time ?(while_ = fun () -> Lwt.return_true) f =
  let open Lwt in
  let next_delay = ref first_delay in
  let started = Unix.gettimeofday () in
  let can_wait_longer () =
    match max_retry_time with
    | None -> true
    | Some d -> Unix.gettimeofday () -. started < d in
  let rec loop nb_try x =
    let%lwt keep_going = while_ () in
    if not keep_going then fail Exit
    else if not (can_wait_longer ()) then fail Timeout
    else match%lwt f x with
      | exception e ->
        let%lwt retry_on_this = on e in
        let should_retry =
          Option.map_default (fun max -> nb_try < max) true max_retry &&
          retry_on_this in
        if should_retry then (
          let delay = !next_delay in
          let delay = min delay max_delay in
          let delay = max delay min_delay in
          next_delay := !next_delay *. delay_adjust_nok ;
          Option.may (fun f -> f delay) delay_rec ;
          let%lwt () = Lwt_unix.sleep delay in
          (loop [@tailcall]) (nb_try + 1) x
        ) else (
          !logger.debug "Non-retryable error: %s after %d attempt%s"
            (Printexc.to_string e) nb_try (if nb_try > 1 then "s" else "") ;
          fail e
        )
      | r ->
        next_delay := !next_delay *. delay_adjust_ok ;
        return r
  in
  loop 1

let shell_quote s =
  "'"^ String.nreplace s "'" "'\\''" ^"'"

let sql_quote s =
  "'"^ String.nreplace s "'" "''" ^"'"

let path_quote s =
  let s = String.nreplace s "%" "%%" in
  String.nreplace s "/" "%2F"
(*$= path_quote & ~printer:(fun x -> x)
  "" (path_quote "")
  "glop" (path_quote "glop")
  "pas%2Fglop" (path_quote "pas/glop")
  "%%%2Fglop%2Fpas%%pas%2Fglop%2F%%" (path_quote "%/glop/pas%pas/glop/%")
  "%2F%%glop%2Fpas%%pas%2Fglop%%%2F" (path_quote "/%glop/pas%pas/glop%/")
 *)

let path_unquote s =
  let len = String.length s in
  let b = Buffer.create len in
  let rec loop i =
    if i >= len then Buffer.contents b
    else if s.[i] = '%' then (
      if i < len - 1 && s.[i+1] = '%' then (
        Buffer.add_char b '%' ;
        loop (i + 2)
      ) else if i < len - 2 && s.[i+1] = '2' && s.[i+2] = 'F' then (
        Buffer.add_char b '/' ;
        loop (i + 3)
      ) else invalid_arg s
    ) else if s.[i] = '/' then invalid_arg s else (
      Buffer.add_char b s.[i] ;
      loop (i + 1)
    )
  in
  loop 0
(*$= path_unquote & ~printer:(fun x -> x)
  "" (path_unquote "")
  "glop" (path_unquote "glop")
  "pas/glop" (path_unquote "pas%2Fglop")
  "%/glop/pas%pas/glop/%" (path_unquote "%%%2Fglop%2Fpas%%pas%2Fglop%2F%%")
  "/%glop/pas%pas/glop%/" (path_unquote "%2F%%glop%2Fpas%%pas%2Fglop%%%2F")
 *)
(*$T path_unquote
  try ignore (path_unquote "%") ; false with Invalid_argument _ -> true
  try ignore (path_unquote "%%%") ; false with Invalid_argument _ -> true
  try ignore (path_unquote "%2") ; false with Invalid_argument _ -> true
  try ignore (path_unquote "%2f") ; false with Invalid_argument _ -> true
  try ignore (path_unquote "%2z") ; false with Invalid_argument _ -> true
  try ignore (path_unquote "%3F") ; false with Invalid_argument _ -> true
 *)


let list_existsi f l =
  match List.findi (fun i v -> f i v) l with
  | exception Not_found -> false
  | _ -> true

let print_exception ?(what="Exception") e =
  !logger.error "%s: %s\n%s" what
    (Printexc.to_string e)
    (Printexc.get_backtrace ())

let log_exceptions ?what f x =
  try f x
  with Exit -> ()
     | e -> print_exception ?what e

let looks_like_true s =
  s = "1" || (
    String.length s > 1 &&
    let lc = Char.lowercase s.[0] in lc = 'y' || lc = 't')

let looks_like_null s =
  String.lowercase_ascii s = "null"

let with_time f k =
  let start = Unix.gettimeofday () in
  let res = f () in
  let dt = Unix.gettimeofday () -. start in
  k dt ;
  res

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

let is_empty_file fname =
  let open Unix in
  let s = stat fname in
  s.st_size = 0

let ensure_file_exists ?contents fname =
  mkdir_all ~is_file:true fname ;
  (* FIXME: race condition *)
  if not (file_exists fname) ||
    (* Check the file has the minimum contents: *)
    contents <> None && is_empty_file fname then (
    !logger.debug "Creating file fname" ;
    let open Unix in
    let fd = openfile fname [O_CREAT;O_WRONLY] 0o644 in
    Option.may (fun s ->
      single_write_substring fd s 0 (String.length s) |> ignore
    ) contents ;
    close fd)

let uniquify_filename fname =
  let rec loop n =
    let fname = fname ^"."^ string_of_int n in
    if file_exists fname then loop (n + 1) else fname
  in
  if file_exists fname then loop 0 else fname

let mtime_of_file fname =
  let open Unix in
  let s = stat fname in
  s.st_mtime

let mtime_of_file_def default fname =
  try mtime_of_file fname
  with Unix.Unix_error (Unix.ENOENT, _, _) -> default

let file_is_older_than age fname =
  try
    let mtime = mtime_of_file fname in
    let now = Unix.gettimeofday () in
    mtime > now -. age
  with _ -> false

(* Will call on_dir and on_file with both the absolute file name and then the
 * name from the given root. *)
let dir_subtree_iter ?on_dir ?on_file root =
  let open Unix in
  let rec loop_subtree path_from_root =
    let path =
      (* Avoid using "./" as we want path to correspond to program names: *)
      if path_from_root = "" then root else root ^"/"^ path_from_root in
    match opendir path with
    | exception Unix_error(ENOENT, _, _) ->
        (* A callback must have deleted it, no worries. *) ()
    | dh ->
      let rec loop_files () =
        match readdir dh with
        | exception End_of_file -> ()
        (* Ignore dotnames and any "hidden" dir or files: *)
        | s when s.[0] = '.' ->
            loop_files ()
        | fname_from_path ->
            let fname = path ^"/"^ fname_from_path in
            let fname_from_root =
              if path_from_root = "" then fname_from_path
              else path_from_root ^"/"^ fname_from_path in
            let may_run = function
              | None -> ()
              | Some f -> log_exceptions (f fname) fname_from_root in
            if is_directory fname then (
              may_run on_dir ;
              let path_from_root' =
                if path_from_root = "" then fname_from_path
                else path_from_root ^"/"^ fname_from_path in
              loop_subtree path_from_root'
            ) else
              may_run on_file ;
            loop_files ()
      in
      loop_files () ;
      closedir dh
  in
  loop_subtree ""

let has_dotnames s =
  s = "." || s = ".." ||
  String.starts_with s "./" ||
  String.starts_with s "../" ||
  String.ends_with s "/." ||
  String.ends_with s "/.." ||
  String.exists s "/./" ||
  String.exists s "/../"

let ensure_trailing_slash dirname =
  let len = String.length dirname in
  if len > 0 && dirname.[len-1] <> '/' then
    dirname ^"/" else dirname

let ensure_no_trailing_slash dirname =
  let len = String.length dirname in
  if len > 0 && dirname.[len-1] = '/' then
    String.rchop dirname else dirname

let change_ext new_ext fname =
  Filename.remove_extension fname ^ new_ext

let starts_with c f =
  String.length f > 0 && f.[0] = c

let is_virtual_field = starts_with '#'

let is_private_field = starts_with '_'

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

let set_signals sigs behavior =
  List.iter (fun s ->
    Sys.set_signal s behavior
  ) sigs

let string_of_process_status = function
  | Unix.WEXITED 127 -> "shell couldn't be executed"
  | Unix.WEXITED code -> Printf.sprintf "terminated with code %d" code
  | Unix.WSIGNALED sign -> Printf.sprintf "killed by signal %s" (name_of_signal sign)
  | Unix.WSTOPPED sign -> Printf.sprintf "stopped by signal %s" (name_of_signal sign)

exception RunFailure of Unix.process_status
let () = Printexc.register_printer (function
  | RunFailure st -> Some ("RunFailure "^ string_of_process_status st)
  | _ -> None)

(* Trick from LWT: how to exit without executing the at_exit hooks: *)
external sys_exit : int -> 'a = "caml_sys_exit"

let with_subprocess cmd args k =
  (* Got some Unix_error(EBADF, "close_process_in", "") suggesting the
   * fd is closed several times so limit the magic: *)
  let open Legacy.Unix in
  !logger.debug "Going to exec %s %a" cmd (Array.print String.print) args ;
  (* Check that the file is present before forking; as we are not going to
   * check the child exit status it will give us a better error message. *)
  if not (file_exists cmd) then
    failwith (Printf.sprintf "File %s does not exist" cmd) ;
  let env = environment () in
  let his_in, my_in = pipe ~cloexec:false ()
  and my_out, his_out = pipe ~cloexec:false ()
  and my_err, his_err = pipe ~cloexec:false () in
  match Unix.fork () with
  | 0 -> (* Child *)
    (try
      (* Move the fd in pos 0, 1 and 2: *)
      let move_fd s d =
        dup2 ~cloexec:false s d ;
        close s in
      move_fd his_in stdin ;
      move_fd his_out stdout ;
      move_fd his_err stderr ;
      execve cmd args env
    with e ->
      Printf.eprintf "Cannot execve: %s\n%!" (Printexc.to_string e) ;
      sys_exit 127)
  | pid -> (* Parent *)
    close his_in ; close his_out ; close his_err ;
    let close_all () =
      close my_in ;
      close my_out ;
      close my_err in
    finally close_all k (out_channel_of_descr my_in,
                         in_channel_of_descr my_out,
                         in_channel_of_descr my_err)

let with_stdout_from_command cmd args k =
  with_subprocess cmd args (fun (_ic, oc, _ec) -> k oc)
(*$= with_stdout_from_command & ~printer:identity
  "glop" (with_stdout_from_command "/bin/echo" [|"/bin/echo";"glop"|] \
          Legacy.input_line)
 *)

(* Low level stuff: run jobs and return lines: *)
let run ?timeout ?(to_stdin="") cmd =
  let open Lwt in
  let string_of_array a =
    Array.fold_left (fun s v ->
        s ^ (if String.length s > 0 then " " else "") ^ v
      ) "" a in
  if Array.length cmd < 1 then invalid_arg "cmd" ;
  !logger.debug "Running command %s" (string_of_array cmd) ;
  Lwt_process.with_process_full ?timeout (cmd.(0), cmd) (fun process ->
    (* What we write to stdin: *)
    let write_stdin =
      let%lwt () =
        try%lwt Lwt_io.write process#stdin to_stdin
        with (Unix.Unix_error (Unix.EPIPE, _, _)) -> return_unit in
      Lwt_io.close process#stdin in
    (* We need to read both stdout and stderr simultaneously or risk
     * interlocking: *)
    let stdout = ref [] and stderr = ref [] in
    let read_lines c k =
      try%lwt
        Lwt_io.read_lines c |>
        Lwt_stream.iter k
      with exn ->
        !logger.error "Error while running %s: %s"
          (string_of_array cmd) (Printexc.to_string exn) ;
        return_unit in
    let read_stdout = read_lines process#stdout (fun l ->
      stdout := l :: !stdout)
    and read_stderr = read_lines process#stderr (fun l ->
      stderr := l :: !stderr) in
    let%lwt () =
      join [ write_stdin ; read_stdout ; read_stderr ] in
    match%lwt process#status with
    | Unix.WEXITED 0 ->
      let to_str lst =
        String.concat "\n" (List.rev lst) in
      return (to_str !stdout, to_str !stderr)
    | x ->
      !logger.error "Command '%s' %s"
        (string_of_array cmd)
        (string_of_process_status x) ;
      fail (RunFailure x))
(*$= run & ~printer:(fun (out,err) -> Printf.sprintf "out=%s, err=%s" out err)
  ("glop", "")     (Lwt_main.run (run [|"/bin/sh";"-c";"echo glop"|]))
  ("", "pas glop") (Lwt_main.run (run [|"/bin/sh";"-c";"echo pas glop 1>&2"|]))
 *)

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

let read_whole_channel ic =
  let open Legacy in
  let read_chunk = 1000 in
  let rec loop buf o =
    if Bytes.length buf - o < read_chunk then
      loop (Bytes.extend buf 0 (5 * read_chunk)) o
    else
      let ret = input ic buf o read_chunk in
      if ret = 0 then Bytes.(sub buf 0 o |> to_string)
      else loop buf (o + ret)
  in
  loop (Bytes.create (5 * read_chunk)) 0

let file_print oc fname =
  let content = File.lines_of fname |> List.of_enum |> String.concat "\n" in
  String.print oc content

let rec simplified_path =
  let res =
    [ Str.regexp "/[^/]+/\\.\\./", "/" ;
      Str.regexp "/\\./", "/" ;
      Str.regexp "//", "/" ;
      Str.regexp "/\\.?$", "" ;
      Str.regexp "^\\./", "" ] in
  fun path ->
    let s =
      List.fold_left (fun s (re, repl) ->
        Str.global_replace re repl s
      ) path res in
    if s = path then s else simplified_path s
(*$= simplified_path & ~printer:identity
  "/glop/glop" (simplified_path "/glop/glop/")
  "/glop/glop" (simplified_path "/glop/glop")
  "glop/glop"  (simplified_path "./glop/glop/")
  "glop/glop"  (simplified_path "glop/glop/.")
  "/glop/glop" (simplified_path "/glop/pas glop/../glop")
  "/glop/glop" (simplified_path "/glop/./glop")
  "glop"       (simplified_path "glop/.")
  "glop"       (simplified_path "glop/./")
  "/glop"      (simplified_path "/./glop")
  "/glop/glop" (simplified_path "/glop//glop")
  "/glop/glop" (simplified_path "/glop/pas glop/..//pas glop/.././//glop//")
 *)

let absolute_path_of ?rel_to path =
  (if path <> "" && path.[0] = '/' then path else
   (rel_to |? Unix.getcwd ()) ^"/"^ path) |>
  simplified_path

let rel_path_from root_path path =
  (* If root path is null assume source file is already relative to root: *)
  if root_path = "" then path else
  let root = absolute_path_of root_path
  and path = absolute_path_of path in
  if String.starts_with path root then
    let rl = String.length root in
    String.sub path rl (String.length path - rl)
  else
    failwith ("Cannot locate "^ path ^" within "^ root_path)

let int_of_fd fd : int = Obj.magic fd

let really_read_fd fd size =
  let open Unix in
  let buf = Bytes.create size in
  let rec loop i =
    if i >= size then buf else
    let r = BatUnix.restart_on_EINTR (read fd buf i) (size - i) in
    if r > 0 then loop (i + r) else
      let e = Printf.sprintf "File smaller then %d bytes (EOF at %d)"
                size i in
      failwith e
  in
  loop 0

let read_whole_fd fd =
  let open Unix in
  let s = fstat fd in
  let size = s.st_size in
  lseek fd 0 SEEK_SET |> ignore ;
  really_read_fd fd size

let marshal_into_fd fd v =
  let open BatUnix in
  (* Leak memory for some reason / and do not write anything to the file
   * if we Marshal.to_channel directly. :-/ *)
  let bytes = Marshal.to_bytes v [] in
  let len = Bytes.length bytes in
  restart_on_EINTR (fun () ->
    lseek fd 0 SEEK_SET |> ignore ;
    write fd bytes 0 len) () |> ignore

let marshal_into_file fname v =
  mkdir_all ~is_file:true fname ;
  let fd = Unix.openfile fname [O_RDWR; O_CREAT; O_TRUNC] 0o640 in
  (* Same as above, must avoid Marshal.from_input (autoclose might
   * eventually close fd at some point - FIXME) *)
  marshal_into_fd fd v

let marshal_from_fd fd =
  let open Unix in
  (* Useful log statement in case the GC crashes right away: *)
  !logger.debug "Retrieving marshaled value from file" ;
  let bytes = read_whole_fd fd in
  Marshal.from_bytes bytes 0

let marshal_from_file fname =
  mkdir_all ~is_file:true fname ;
  let fd = Unix.openfile fname [O_RDWR] 0o640 in
  marshal_from_fd fd

let getenv ?def n =
  try Sys.getenv n
  with Not_found ->
    match def with
    | Some d -> d
    | None ->
      Printf.sprintf "Cannot find envvar %s" n |>
      failwith

let do_daemonize () =
  let open Unix in
  if fork () > 0 then sys_exit 0 ;
  setsid () |> ignore ;
  (* Close in/out, ignoring errors in case they have been closed already: *)
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
  max_simult ~max_count (fun () ->
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
      let%lwt () =
        join [ write_stdin ;
               read_lines process#stdout ;
               read_lines process#stderr ] in
      process#status))

let string_of_time ts =
  let open Unix in
  match localtime ts with
  | exception Unix_error (EINVAL, _, _) ->
      Printf.sprintf "Invalid date %f" ts
  | tm ->
      Printf.sprintf "%04d-%02d-%02d %02dh%02dm%02ds"
        (tm.tm_year + 1900) (tm.tm_mon + 1) tm.tm_mday
        tm.tm_hour tm.tm_min tm.tm_sec

let string_remove c s =
  let len = String.length s in
  let buf = Buffer.create len in
  for i = 0 to len-1 do
    if s.[i] <> c then Buffer.add_char buf s.[i]
  done ;
  Buffer.contents buf
(*$= string_remove & ~printer:identity
  "1234" (string_remove ':' "::12:34")
 *)

let udp_server ?(buffer_size=2000) ~inet_addr ~port
               ?(while_=(fun () -> true)) k =
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
    if while_ () then
      let%lwt recv_len, sockaddr =
        recvfrom sock buffer 0 (Bytes.length buffer) [] in
      let sender =
        match sockaddr with
        | ADDR_INET (addr, port) ->
          Printf.sprintf "%s:%d" (Unix.string_of_inet_addr addr) port
        | _ -> "??" in
      let%lwt () = k sender buffer recv_len in
      (forever [@tailcall]) ()
    else return_unit
  in
  forever ()

let hex_of =
  let zero = Char.code '0'
  and ten = Char.code 'a' - 10 in
  fun n ->
    if n < 10 then Char.chr (zero + n)
    else Char.chr (ten + n)

let rec restart_on_failure what f x =
  try%lwt f x
  with e -> (
    print_exception e ;
    !logger.error "Will restart %s..." what ;
    let%lwt () = Lwt_unix.sleep (0.5 +. Random.float 0.5) in
    (restart_on_failure [@tailcall]) what f x)

let md5 str = Digest.(string str |> to_hex)

(* Cohttp does not enforce any scheme but we want to be friendlier with
 * user entered urls so we add one if it's missing, assuming http: *)
let sure_is_http str =
  if match String.find str "://" with
     | exception Not_found -> true
     | n -> n > 10
  then "http://" ^ str
  else str
(*$= sure_is_http & ~printer:identity
  "http://blabla.com" (sure_is_http "http://blabla.com")
  "http://blabla.com" (sure_is_http "blabla.com")
  "https://blabla.com" (sure_is_http "https://blabla.com")
 *)

let packed_string_of_int n =
  let buf = Buffer.create 8 in
  let rec loop n =
    if n = 0 then Buffer.contents buf else (
      Buffer.add_char buf (Char.chr (n land 255)) ;
      loop (n / 256) (* Beware that n is signed *))
  in
  loop n
(*$= packed_string_of_int & ~printer:identity
  "abc" (packed_string_of_int 0x636261)
  "" (packed_string_of_int 0)
 *)

let hash_iter_s h f =
  Hashtbl.fold (fun k v thd ->
    let%lwt () = f k v in thd
  ) h Lwt.return_unit

let hash_map_s h f =
  let res = Hashtbl.create (Hashtbl.length h) in
  let%lwt () =
    hash_iter_s h (fun k v ->
      let%lwt v' = f k v in
      Hashtbl.add res k v' ;
      Lwt.return_unit) in
  Lwt.return res

let hash_fold_s h f i =
  Hashtbl.fold (fun k v thd ->
    let%lwt prev = thd in
    f k v prev
  ) h (Lwt.return i)

let age t = Unix.gettimeofday () -. t

let memoize f =
  let cached = ref None in
  fun () ->
    match !cached with
    | Some r -> r
    | None ->
        let r = f () in
        cached := Some r ;
        r

let cache_clean_after = 1200.
let cached2 cache_name reread time =
  (* Cache is a hash from some key to last access time, last data time,
   * and data. *)
  !logger.debug "Create a new cache for %s" cache_name ;
  let cache = Hashtbl.create 31 in
  let next_clean = ref (Unix.time () +. Random.float cache_clean_after) in
  fun k u ->
    let ret = ref None in
    let now = Unix.time () in
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
  let c = cached2 cache_name (fun k () -> reread k) (fun k () -> time k) in
  fun k -> c k ()

let save_in_file ~compute ~serialize ~deserialize fname =
  try read_whole_file fname |> deserialize
  with _ ->
    mkdir_all ~is_file:true fname ;
    File.with_file_out ~mode:[`create;`trunc;`text] fname (fun oc ->
      let v = compute () in
      String.print oc (serialize v) ;
      v)

let abbrev len s =
  if String.length s <= len then s else
  String.sub s 0 (len-3) ^"..."

let abbrev_path ?(max_length=20) ?(known_prefix="") path =
  let known_prefix =
    if String.length known_prefix > 0 &&
       known_prefix.[String.length known_prefix - 1] <> '/'
    then known_prefix ^"/"
    else known_prefix in
  let path =
    if String.starts_with path known_prefix then
      String.lchop ~n:(String.length known_prefix) path
    else path in
  let rec loop abb rest =
    if String.length rest < 1 || rest.[0] = '.' ||
       String.length abb + String.length rest <= max_length
    then
      abb ^ rest
    else
      if rest.[0] = '/' then loop (abb ^"/") (String.lchop rest)
      else
        match String.index rest '/' with
        | exception Not_found ->
            abb ^ rest
        | n ->
            loop (abb ^ String.of_char rest.[0]) (String.lchop ~n rest)
  in loop "" path
(*$= abbrev_path & ~printer:(fun x -> x)
  "/a/b/c/glop" (abbrev_path "/a very long name/before another very long one/could be reduced to/glop")
  "/a/b/c/glop" (abbrev_path ~known_prefix:"/tmp" "/a very long name/before another very long one/could be reduced to/glop")
  "a/b/c/glop" (abbrev_path ~known_prefix:"/tmp" "/tmp/a very long name/before another very long one/could be reduced to/glop")
  "a/b/c/glop" (abbrev_path ~known_prefix:"/tmp/" "/tmp/a very long name/before another very long one/could be reduced to/glop")
  "a/b/c/glop" (abbrev_path "a very long name/before another very long one/could be reduced to/glop")
 *)

(* Addition capped to min_int/max_int *)
let cap_add a b =
  if a > 0 && b > 0 then
    if max_int - b >= a then a + b else max_int
  else if a < 0 && b < 0 then
    if min_int - b <= a then a + b else min_int
  else a + b

(*$= cap_add & ~printer:string_of_int
  42 (cap_add 31 11)
  42 (cap_add 57 ~-15)
  42 (cap_add ~-17 59)
  max_int (cap_add (max_int - 3) 3)
  max_int (cap_add (max_int - 3) 4)
  max_int (cap_add (max_int - 3) 9)
  min_int (cap_add (min_int + 3) ~-3)
  min_int (cap_add (min_int + 3) ~-4)
  min_int (cap_add (min_int + 3) ~-9)
 *)

(* min_int cannot be negated without overflow *)
let cap_neg a = if a = min_int then max_int else ~-a

let uniquify () =
  let past = ref Set.empty in
  fun x ->
    if Set.mem x !past then false
    else (
      past := Set.add x !past ;
      true
    )
(*$= uniquify & ~printer:(IO.to_string (List.print Int.print))
  [1;2;3] (List.filter (uniquify ()) [1;1;2;3;3;2;1])
 *)

let jitter ?(amplitude=0.25) v =
  let r = (Random.float amplitude) -. amplitude /. 2. in
  v +. (v *. r)

let todo msg = failwith ("not implemented: "^ msg)

let ordinal_suffix n =
  let tens = n mod 100 in
  if tens >= 10 && tens < 20 then "th" else
  match n mod 10 with
  | 1 -> "st"
  | 2 -> "nd"
  | 3 -> "rd"
  | _ -> "th"

(* Given an array of floats, display an UTF-8 sparkline: *)
let sparkline vec =
  let stairs = [| "▁"; "▂"; "▃"; "▄"; "▅"; "▆"; "▇"; "█" |] in
  let mi, ma =
    Array.fold_left (fun (mi, ma) v ->
      min v mi, max v ma
    ) (infinity, neg_infinity) vec in
  let ratio =
    if ma > mi then
      float_of_int (Array.length stairs - 1) /. (ma -. mi)
    else 0. in
  let res = Buffer.create (Array.length vec * 4) in
  Array.iteri (fun i v ->
    let c = int_of_float ((v -. mi) *. ratio) in
    Buffer.add_string res stairs.(c)
  ) vec ;
  Buffer.contents res

(* All the time conversion functions below are taken from (my understanding of)
 * http://graphite-api.readthedocs.io/en/latest/api.html#from-until *)

let time_of_reltime s =
  let scale d s =
    try
      Some (
        Unix.gettimeofday () +. d *.
          (match s with
          | "s" -> 1.
          | "min" -> 60.
          | "h" -> 3600.
          | "d" -> 86400.
          | "w" -> 7. *. 86400.
          | "mon" -> 30. *. 86400.
          | "y" -> 365. *. 86400.
          | _ -> raise Exit))
    with Exit ->
      None
  in
  Scanf.sscanf s "%f%s%!" scale

(* String interpreted in the local time zone: *)
let time_of_abstime s =
  let s = String.lowercase s in
  let scan fmt recv =
    try Some (Scanf.sscanf s fmt recv)
    with Scanf.Scan_failure _ -> None
  and eq str recv =
    if s = str then Some (recv ()) else None
  and (|||) o1 o2 =
    if o1 <> None then o1 else o2 in
  let open Unix in
  let is_past h m tm =
    h < tm.tm_hour || h = tm.tm_hour && m < tm.tm_min in
  let time_of_hh_mm h m am_pm =
    let h = match String.lowercase am_pm with
      | "am" | "" -> h
      | "pm" -> h + 12
      | _ -> raise (Scanf.Scan_failure ("Invalid AM/PM: "^ am_pm)) in
    let now = time () in
    let tm = localtime now in
    (* "If that time is already past, the next day is assumed" *)
    if is_past h m tm then now +. 86400. else now in
  let time_of_dd_mm_yyyy d m y =
    let y = if y < 100 then y + 2000 (* ? *) else y in
    let tm =
      { tm_sec = 0 ; tm_min = 0 ; tm_hour = 0 ;
        tm_mday = d ; tm_mon = m - 1 ; tm_year = y - 1900 ;
        (* ignored: *) tm_wday = 0 ; tm_yday = 0 ; tm_isdst = false } in
    mktime tm |> fst
  in
  (* Extracts are from `man 1 at`:
   *
   * "It accepts times of the form HHMM or HH:MM to run a job at a specific
   * time of day.  (If that time is already past, the next day is assumed.)
   * (...) and time-of-day may be suffixed with AM or PM for running in the
   * morning or the evening." *)
  (scan "%2d%2d%s%!" time_of_hh_mm) |||
  (scan "%2d:%2d%s%!" time_of_hh_mm) |||
  (scan "%2d:%2d%s%!" time_of_hh_mm) |||
  (* "As an alternative, the following keywords may be specified: midnight,
   * noon, or teatime (4pm) (...)." *)
  (eq "midnight" (fun () -> time_of_hh_mm 0 0 "")) |||
  (eq "noon" (fun () -> time_of_hh_mm 12 00 "")) |||
  (eq "teatime" (* fuck you! *) (fun () -> time_of_hh_mm 16 00 "")) |||
  (* Not specified but that's actually the first Grafana will send: *)
  (eq "now" time) |||
  (* Also not specified but mere unix timestamps are actually frequent: *)
  (scan "%f%!" (fun f ->
    if f > 946681200. && f < 2208985200. then f
    else raise (Scanf.Scan_failure "Doesn't look like a timestamp"))) |||
  (* "The day on which the job is to be run may also be specified by giving a
   * date in the form month-name day with an optional year," *)
  (* TODO *)
  (* "or giving a date of the forms DD.MM.YYYY, DD.MM.YY, MM/DD/YYYY, MM/DD/YY,
   * MMDDYYYY, or MMDDYY." *)
  (scan "%2d.%2d.%4d%!" time_of_dd_mm_yyyy) |||
  (scan "%2d/%2d/%4d%!" (fun m d y -> time_of_dd_mm_yyyy d m y)) |||
  (scan "%2d%2d%4d%!" (fun m d y -> time_of_dd_mm_yyyy d m y)) |||
  (* "The specification of a date must follow the specification of the time of
   * day.  Time can also be specified as: [now] + count time-units, where the
   * time-units can be minutes, hours, days, weeks, months or years and at may
   * be told to run the job today by suffixing the time with today and to run
   * the job tomorrow by suffixing the time with tomorrow.  The shortcut next
   * can be used instead of + 1." *)
  (* TODO *)
  None

(* mktime tm struct "is interpreted in the local time zone". Work around this
 * by dividing by 24h. *)
(*$= time_of_abstime & ~printer:(function None -> "None" | Some f -> string_of_float f)
 (Some 2218.) (BatOption.map (fun ts -> ceil (ts /. 86400.)) (time_of_abstime "28.01.1976"))
 (time_of_abstime "28.01.1976") (time_of_abstime "01/28/1976")
 (Some 1523052000.) (time_of_abstime "1523052000")
 *)

(* Replace ${tuple.field} by the actual value the passed string: *)
let subst_tuple_fields =
  let open Str in
  let re =
    regexp "\\${\\(\\([_a-zA-Z0-9.]+\\)\\.\\)?\\([_a-zA-Z0-9]+\\)}" in
  fun tuples text ->
    let tuples = ([ "env" ], Sys.getenv) :: tuples in
    global_substitute re (fun s ->
      let tuple_name = try matched_group 2 s with Not_found -> "" in
      let field_name = matched_group 3 s in
      match List.find (fun (names, _finder) ->
              List.mem tuple_name names
            ) tuples with
      | exception Not_found ->
        !logger.error "Unknown tuple %S used in text substitution!"
          tuple_name ;
        "??"^ tuple_name ^"."^ field_name ^"??"
      | _, finder ->
        try finder field_name
        with Not_found ->
          !logger.error "Field \"%s.%s\" used in text substitution is not \
                         present in that tuple!" tuple_name field_name ;
          "??"^ tuple_name ^"."^ field_name ^"??"
    ) text

let reindent indent s =
  indent ^ String.nreplace (String.trim s) "\n" ("\n"^indent)

(* FIXME: this won't work but for the simplest types: *)
let split_string ~sep ~opn ~cls s =
  let open String in
  let s = trim s in
  if s.[0] <> opn || s.[length s - 1] <> cls then
    failwith (Printf.sprintf "Value must be delimited with %c and %c"
                opn cls) ;
  let s = sub s 1 (length s - 2) in
  split_on_char sep s |> List.map trim |> Array.of_list

(*$= split_string & ~printer:(IO.to_string (Array.print String.print))
  [| "glop" |] (split_string ~sep:';' ~opn:'(' ~cls:')' "(glop)")
  [| "glop" |] (split_string ~sep:';' ~opn:'(' ~cls:')' "  ( glop  )  ")
  [| "pas"; "glop" |] \
    (split_string ~sep:';' ~opn:'(' ~cls:')' "(pas;glop)")
  [| "pas"; "glop" |] \
    (split_string ~sep:';' ~opn:'(' ~cls:')' "(  pas ;  glop)  ")
*)

let ppp_of_file ?(error_ok=false) ppp =
  let reread fname =
    !logger.debug "Have to reread %S" fname ;
    let openflags = [ Open_rdonly; Open_text ] in
    match Legacy.open_in_gen openflags 0o644 fname with
    | exception e ->
        (if error_ok then !logger.debug else !logger.warning)
          "Cannot open %S for reading: %s" fname (Printexc.to_string e) ;
        raise e
    | ic ->
        finally
          (fun () -> Legacy.close_in ic)
          (PPP.of_in_channel_exc ppp) ic in
  let cache_name = "ppp_of_file ("^ (ppp ()).descr 0 ^")" in
  cached cache_name reread mtime_of_file

let ppp_to_file fname ppp v =
  mkdir_all ~is_file:true fname ;
  let openflags = [ Open_wronly; Open_creat; Open_trunc; Open_text ] in
  match Pervasives.open_out_gen openflags 0o644 fname with
  | exception e ->
      !logger.warning "Cannot open %S for writing: %s"
        fname (Printexc.to_string e) ;
      raise e
  | oc ->
      finally
        (fun () -> Pervasives.close_out oc)
        (PPP.to_out_channel ppp oc) v
