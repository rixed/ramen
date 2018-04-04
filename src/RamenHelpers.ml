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
          loop (nb_try + 1) x
        ) else (
          !logger.error "Non-retryable error: %s after %d attempt%s"
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

let list_existsi f l =
  match List.findi (fun i v -> f i v) l with
  | exception Not_found -> false
  | _ -> true

let print_exception ?(what="Exception:") e =
  !logger.error "%s: %s\n%s" what
    (Printexc.to_string e)
    (Printexc.get_backtrace ())

let log_exceptions ?what f x =
  try f x
  with e -> print_exception ?what e

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

let ensure_file_exists fname =
  mkdir_all ~is_file:true fname ;
  if not (file_exists fname) then (
    !logger.debug "Creating file fname" ;
    Unix.(openfile fname [O_CREAT] 0o644 |> close))

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
      let%lwt () = join [ write_stdin ; read_lines ; monitor_stderr ] in
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

let rec simplified_path =
  let res =
    [ Str.regexp "/[^/]+/\\.\\./" ;
      Str.regexp "/\\./" ;
      Str.regexp "//" ] in
  fun path ->
    let s =
      List.fold_left (fun s re ->
        Str.global_replace re "/" s
      ) path res in
    if s = path then s else simplified_path s
(*$= simplified_path & ~printer:identity
  "/glop/glop" (simplified_path "/glop/glop")
  "/glop/glop" (simplified_path "/glop/pas glop/../glop")
  "/glop/glop" (simplified_path "/glop/./glop")
  "/glop/glop" (simplified_path "/glop//glop")
  "/glop/glop" (simplified_path "/glop/pas glop/..//pas glop/.././//glop")
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

let string_starts_with sub s = String.starts_with s sub

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
      k sender buffer recv_len >>=
      forever
    else return_unit
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
  with e -> (
    print_exception e ;
    !logger.error "Will restart..." ;
    let%lwt () = Lwt_unix.sleep (0.5 +. Random.float 0.5) in
    restart_on_failure f x)

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

let abbrev len s =
  if String.length s <= len then s else
  String.sub s 0 (len-3) ^"..."

(* Addition capped to min_int/max_int *)
let cap_add a b =
  !logger.debug "cap_add %d %d" a b ;
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
