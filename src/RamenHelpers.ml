open Batteries
open RamenLog
module Atomic = RamenAtomic

(*$inject open Batteries *)

let max_int_for_random = 0x3FFFFFFF

(* Small helper to return the ith entry of an array, capped to the last one.
 * Useful when we reach the last defined attempt while escalating an alert. *)
let get_cap a i =
  let len = Array.length a in
  assert (len > 0) ;
  a.(min i (len - 1))

let align_float ?(round=floor) step v =
  round (v /. step) *. step

let round_to_int f =
  int_of_float (Float.round f)

(* The original Float.to_string adds a useless dot at the end of
 * round numbers, and likes to end with lots of zeroes: *)
let nice_string_of_float v =
  let s = Float.to_string v in
  assert (String.length s > 0) ;
  match String.index s '.' with
  | exception Not_found -> s
  | i ->
      let last_non_zero =
        let rec loop j =
          assert (j >= i) ;
          if s.[j] <> '0' then j else loop (j - 1) in
        loop (String.length s - 1) in
      let has_trailling_dot = s.[last_non_zero] = '.' in
      let n =
        (String.length s - last_non_zero) - 1 +
        (if has_trailling_dot then 1 else 0) in
      String.rchop ~n s

(*$= nice_string_of_float & ~printer:(fun x -> x)
  "1.234" (nice_string_of_float 1.234)
  "1.001" (nice_string_of_float 1.001)
  "1"     (nice_string_of_float 1.)
*)

let print_nice_float oc f =
  String.print oc (nice_string_of_float f)

exception Timeout

(* Avoid to create a new while_ at each call: *)
let always () = true

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
    let keep_going = while_ () in
    if not keep_going then raise Exit
    else if not (can_wait_longer ()) then raise Timeout
    else match f x with
      | exception e ->
        let retry_on_this = on e in
        let should_retry =
          Option.map_default (fun max -> num_try < max) true max_retry &&
          retry_on_this in
        if should_retry then (
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

let shell_quote s =
  "'"^ String.nreplace s "'" "'\\''" ^"'"

let sql_quote s =
  "'"^ String.nreplace s "'" "''" ^"'"

let ramen_quote = sql_quote

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

let list_iter_first_last f lst =
  let rec loop is_first = function
  | [] -> ()
  | [x] -> f is_first true x
  | x::lst ->
      f is_first false x ;
      loop false lst in
  loop true lst

let print_exception ?(what="Exception") e =
  !logger.error "%s: %s\n%s" what
    (Printexc.to_string e)
    (Printexc.get_backtrace ())

let result_print p_ok p_err oc = function
  | Result.Ok x -> Printf.fprintf oc "Ok(%a)" p_ok x
  | Result.Bad x -> Printf.fprintf oc "Bad(%a)" p_err x

let log_exceptions ?what f x =
  try f x
  with e ->
    if e <> Exit then print_exception ?what e ;
    raise e

let log_and_ignore_exceptions ?what f x =
  try f x
  with Exit -> ()
     | e -> print_exception ?what e

let print_dump oc x = dump x |> String.print oc

let looks_like_true s =
  s = "1" || (
    String.length s > 1 &&
    let lc = Char.lowercase s.[0] in lc = 'y' || lc = 't')

let looks_like_null s =
  String.lowercase_ascii s = "null"

let is_alpha s =
  try
    for i = 0 to String.length s - 1 do
      if (s.[i] < 'a' || s.[i] > 'z') &&
         (s.[i] < 'A' || s.[i] > 'Z') then raise Exit
    done ;
    true
  with Exit -> false

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

(*
 * Some file utilities
 *)

let rec restart_on_eintr ?(while_=always) f x =
  let open Unix in
  try f x
  with Unix_error (EINTR, _, _) ->
    if while_ () then restart_on_eintr ~while_ f x
    else raise Exit

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
        !logger.debug "Cannot mkdir as %s already exist" d
    ) in
  ensure_exist dir

type file_status = FileOk | FileMissing | FileTooSmall | FileBadPerms
let file_check ?(min_size=0) ?(has_perms=0) fname =
  let open Unix in
  match stat fname with
  | exception _ ->
    (* Be it the file or a directory, or a permission issue, we consider the
     * file to be missing: *)
    FileMissing
  | s ->
    if s.st_perm land has_perms <> has_perms then FileBadPerms else
    if s.st_size < min_size then FileTooSmall else
    FileOk

let file_exists fname = file_check fname = FileOk

let file_size fname =
  let open Unix in
  let s = stat fname in
  s.st_size

let is_empty_file fname =
  file_size fname = 0

let safe_unlink fname =
  try BatUnix.restart_on_EINTR Unix.unlink fname
  with Unix.(Unix_error (ENOENT, _, _)) -> ()

let move_file_away fname =
  let bad_file = fname ^".bad?" in
  ignore_exceptions safe_unlink bad_file ;
  (try restart_on_eintr (Unix.rename fname) bad_file
  with e ->
    !logger.warning "Cannot rename file %s to %s: %s"
      fname bad_file (Printexc.to_string e))

let mtime_of_file fname =
  let open Unix in
  let s = stat fname in
  s.st_mtime

let mtime_of_file_def default fname =
  try mtime_of_file fname
  with Unix.Unix_error (Unix.ENOENT, _, _) -> default

let file_is_older_than ~on_err age fname =
  try
    let mtime = mtime_of_file fname in
    let now = Unix.gettimeofday () in
    mtime < now -. age
  with e ->
    print_exception e ;
    on_err

let rec ensure_file_exists ?(contents="") ?min_size fname =
  mkdir_all ~is_file:true fname ;
  (* If needed, create the file with the initial content, atomically: *)
  let open Unix in
  (* But first, check if the file is already there with the proper size
   * (without opening it, or the following O_EXCL dance won't work!): *)
  match file_check ?min_size fname with
  | FileOk -> ()
  | FileMissing ->
      !logger.debug "File %s is still missing" fname ;
      (match openfile fname [O_CREAT; O_EXCL; O_WRONLY; O_CLOEXEC] 0o644 with
      | exception Unix_error (EEXIST, _, _) ->
          (* Wait for some other concurrent process to rebuild it: *)
          !logger.debug "File %s just appeared, give it time..." fname ;
          sleep 1 ;
          ensure_file_exists ~contents ?min_size fname
      | fd ->
          !logger.debug "Creating file %s with initial content %S"
            fname contents ;
          finally
            (fun () -> close fd)
            (fun () ->
              if contents <> "" then (
                let len = String.length contents in
                single_write_substring fd contents 0 len |> ignore)) ())
  | FileTooSmall ->
      (* Not my business, wait until the file length is at least that
       * of contents, which realistically should not take more than 1s: *)
      let redo () =
        move_file_away fname ;
        ensure_file_exists ~contents ?min_size fname
      in
      if file_is_older_than ~on_err:true 3. fname then (
        !logger.warning "File %s is an old left-over, let's redo it" fname ;
        redo ()
      ) else (
        (* Wait for some other concurrent process to rebuild it: *)
        !logger.debug "File %s is being worked on, give it time..." fname ;
        sleep 1 ;
        ensure_file_exists ~contents ?min_size fname)
  | FileBadPerms ->
      assert false (* We aren't checking that here *)

let uniquify_filename fname =
  let rec loop n =
    let fname = fname ^"."^ string_of_int n in
    if file_exists fname then loop (n + 1) else fname
  in
  if file_exists fname then loop 0 else fname

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
              | Some f -> log_and_ignore_exceptions (f fname) fname_from_root in
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

let file_ext fname =
  let e = Filename.extension fname in
  if e = "" then e else String.lchop e

let read_whole_file fname =
  File.with_file_in ~mode:[`text] fname IO.read_all

let read_whole_thing read =
  let read_chunk = 1000 in
  let rec loop buf o =
    if Bytes.length buf - o < read_chunk then
      loop (Bytes.extend buf 0 (5 * read_chunk)) o
    else
      let ret = read buf o read_chunk in
      if ret = 0 then Bytes.(sub buf 0 o |> to_string)
      else loop buf (o + ret)
  in
  loop (Bytes.create (5 * read_chunk)) 0

let read_whole_channel ic =
  read_whole_thing (Legacy.input ic)

let read_whole_fd fd =
  read_whole_thing (Unix.read fd)

let touch_file fname to_when =
  !logger.debug "Touching %s" fname ;
  Unix.utimes fname to_when to_when

let file_print oc fname =
  let content = File.lines_of fname |> List.of_enum |> String.concat "\n" in
  String.print oc content

let rec simplified_path =
  let open Str in
  let strip_final_slash s =
    let l = String.length s in
    if l > 1 && s.[l-1] = '/' then
      String.rchop s
    else s in
  let res =
    [ regexp "/[^/]+/\\.\\./", "/" ;
      regexp "/[^/]+/\\.\\.$", "" ;
      regexp "/\\./", "/" ;
      regexp "//", "/" ;
      regexp "/\\.?$", "" ;
      regexp "^\\./", "" ] in
  fun path ->
    let s =
      List.fold_left (fun s (re, repl) ->
        global_replace re repl s
      ) path res in
    if s = path then strip_final_slash s
    else simplified_path s

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
  "/glop"      (simplified_path "/glop/glop/..")
  "/glop"      (simplified_path "/glop/glop/../")
 *)

let absolute_path_of ?cwd path =
  (if path <> "" && path.[0] = '/' then path else
   (cwd |? Unix.getcwd ()) ^"/"^ path) |>
  simplified_path

(*$= absolute_path_of & ~printer:identity
  "/tmp/ramen_root/junkie/csv.x" \
    (absolute_path_of ~cwd:"/tmp" "ramen_root/junkie/csv.x")
 *)

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

let marshal_into_fd fd v =
  let open BatUnix in
  (* Leak memory for some reason / and do not write anything to the file
   * if we Marshal.to_channel directly. :-/ *)
  let bytes = Marshal.to_bytes v [] in
  let len = Bytes.length bytes in
  restart_on_EINTR (fun () ->
    lseek fd 0 SEEK_SET |> ignore ;
    write fd bytes 0 len) () |> ignore

let marshal_from_fd fd =
  let open Unix in
  (* Useful log statement in case the GC crashes right away: *)
  !logger.debug "Retrieving marshaled value from file" ;
  let bytes = read_whole_fd fd in
  Marshal.from_string bytes 0

let same_files a b =
  let same_file_size () =
    try
      file_size a = file_size b
    with Unix.(Unix_error (ENOENT, _, _)) -> false
  and same_content () =
    read_whole_file a = read_whole_file b
  in
  same_file_size () && same_content ()

(* Given two files, compare their content and if it differs rename
 * [src] into [dst] ; otherwise merely deletes [src].
 * This is to avoid touching the mtime of the destination when not needed.
 * [dst] might not exist but the directory does.
 * Returns true iff the file was moved. *)
let replace_if_different ~src ~dst =
  if same_files src dst then (
    Unix.unlink src ;
    false
  ) else (
    Unix.rename src dst ;
    true
  )

(*$R replace_if_different
  let open Batteries in
  let tmpdir = "/tmp/ramen_inline_test_"^ string_of_int (Unix.getpid ()) in
  mkdir_all tmpdir ;
  let src = tmpdir ^ "/replace_if_different.src"
  and dst = tmpdir ^ "/replace_if_different.dst" in
  let set_content f = function
    | Some c ->
        File.with_file_out f (fun oc -> String.print oc c)
    | None ->
        safe_unlink f
  in
  let test csrc cdst_opt =
    let asrt t =
      assert_bool (csrc ^"->"^ (cdst_opt |? "none") ^": "^ t) in
    set_content src (Some csrc) ;
    set_content dst cdst_opt ;
    let res = replace_if_different ~src ~dst in
    (* In any cases: *)
    asrt "src file must be gone" (not (file_exists src)) ;
    asrt "dst file must be present" (file_exists dst) ;
    if res then
      asrt "dest content must have changed"
        (csrc = read_whole_file dst)
    else
      asrt "dest content must not have changed"
        ((cdst_opt |? "ENOENT") = read_whole_file dst) ;
    res
  in
  assert_bool "Must not move identical content (1)" (not (test "bla" (Some "bla"))) ;
  assert_bool "Must not move identical content (2)" (not (test "" (Some ""))) ;
  assert_bool "Must move non-identical content (1)" (test "bla" (Some "")) ;
  assert_bool "Must move non-identical content (2)" (test "" (Some "bla")) ;
  (* Also dest file may not exist: *)
  assert_bool "Must move over non existing dest" (test "bla" None) ;
  (* Cleanup: *)
  safe_unlink src ; safe_unlink dst ; Unix.rmdir tmpdir
*)

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

let waitpid_log ?expected_status ~what pid =
  let open Unix in
  let _, status = restart_on_EINTR (waitpid [ WUNTRACED ]) pid in
  if (match status with
      | WEXITED c ->
          expected_status <> Some c && expected_status <> None
      | _ -> true)
  then
    !logger.error "%s %s"
      what (string_of_process_status status)

let with_subprocess ?expected_status ?env cmd args k =
  (* Got some Unix_error(EBADF, "close_process_in", "") suggesting the
   * fd is closed several times so limit the magic: *)
  let open Legacy.Unix in
  !logger.debug "Going to exec %s %a" cmd (Array.print String.print) args ;
  (* Check that the file is present before forking; as we are not going to
   * check the child exit status it will give us a better error message. *)
  if not (file_exists cmd) then
    failwith (Printf.sprintf "File %s does not exist" cmd) ;
  let env = env |? environment () in
  let his_in, my_in = pipe ~cloexec:false ()
  and my_out, his_out = pipe ~cloexec:false ()
  and my_err, his_err = pipe ~cloexec:false () in
  flush_all () ;
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
    let close_wait () =
      close_all () ;
      let what =
        IO.to_string
          (Array.print ~first:"" ~last:"" ~sep:" " String.print) args in
      waitpid_log ~what ?expected_status pid
      in
    finally close_wait
      k (out_channel_of_descr my_in,
         in_channel_of_descr my_out,
         in_channel_of_descr my_err)

let with_stdout_from_command ?expected_status ?env cmd args k =
  with_subprocess ?expected_status ?env cmd args (fun (_ic, oc, _ec) -> k oc)

(*$= with_stdout_from_command & ~printer:identity
  "glop" (with_stdout_from_command "/bin/echo" [|"/bin/echo";"glop"|] \
          Legacy.input_line)
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
  flush_all () ;
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

let read_lines fd =
  let open Legacy.Unix in
  let last_chunk = ref Bytes.empty in
  let buf = Buffer.create 1000 in
  let eof = ref false in
  (* Tells if we also had a newline after buf: *)
  let flush ends_with_nl =
    if Buffer.length buf = 0 && not ends_with_nl then
      raise Enum.No_more_elements ;
    let s = Buffer.contents buf in
    Buffer.clear buf ;
    s
  in
  Enum.from (fun () ->
    let rec loop () =
      if !eof then raise Enum.No_more_elements ;
      let chunk =
        if Bytes.length !last_chunk > 0 then (
          (* If we have some bytes left from previous run, use that: *)
          !last_chunk
        ) else (
          (* Get new bytes: *)
          let chunk = Bytes.create 1000 in
          let r = read fd chunk 0 (Bytes.length chunk) in
          Bytes.sub chunk 0 r
        ) in
      if Bytes.length chunk = 0 then (
        eof := true ;
        flush false
      ) else match Bytes.index chunk '\n' with
        | exception Not_found ->
            Buffer.add_bytes buf chunk ;
            last_chunk := Bytes.empty ;
            loop ()
        | l ->
            Buffer.add_bytes buf (Bytes.sub chunk 0 l) ;
            last_chunk :=
              (let l = l+1 in
              Bytes.sub chunk l (Bytes.length chunk - l)) ;
            flush true in
    loop ())

(* Run given command, logging its output in our log-file *)
let run_coprocess ~max_count ?(to_stdin="") cmd_name cmd =
  !logger.debug "Executing: %s" cmd ;
  max_simult ~what:cmd_name ~max_count (fun () ->
    let open Legacy.Unix in
    let (pstdout, pstdin, pstderr as chans) = open_process_full cmd [||] in
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

let udp_server ?(buffer_size=2000) ~inet_addr ~port ?(while_=always) k =
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
  !logger.debug "Listening for datagrams on port %d" port ;
  let buffer = Bytes.create buffer_size in
  let rec forever () =
    if while_ () then
      let recv_len, sockaddr =
        recvfrom sock buffer 0 (Bytes.length buffer) [] in
      let sender =
        match sockaddr with
        | ADDR_INET (addr, port) ->
          Printf.sprintf "%s:%d" (Unix.string_of_inet_addr addr) port
        | _ -> "??" in
      k sender buffer recv_len ;
      (forever [@tailcall]) ()
  in
  forever ()

let hex_of =
  let zero = Char.code '0'
  and ten = Char.code 'a' - 10 in
  fun n ->
    if n < 10 then Char.chr (zero + n)
    else Char.chr (ten + n)

let fail_for_good = ref false
let rec restart_on_failure ?(while_=always) what f x =
  if !fail_for_good then
    f x
  else
    try f x
    with e ->
      print_exception e ;
      if while_ () then (
        !logger.error "Will restart %s..." what ;
        Unix.sleepf (0.5 +. Random.float 0.5) ;
        (restart_on_failure ~while_ [@tailcall]) what f x)

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

let age t = Unix.gettimeofday () -. t

let option_get what = function
  | Some x -> x
  | None ->
      !logger.error "Forced the None value of %s" what ;
      invalid_arg "option_get"

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
  Array.iter (fun v ->
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
  let scan c recv =
    try Some (Scanf.sscanf s c recv)
    with Scanf.Scan_failure _ | End_of_file | Failure _ -> None
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
  let time_of_yyyy_mm_dd_h_m_s y mo d h mi s =
    let tm =
      { tm_sec = round_to_int s ; tm_min = mi ; tm_hour = h ;
        tm_mday = d ; tm_mon = mo - 1 ; tm_year = y - 1900 ;
        (* ignored: *) tm_wday = 0 ; tm_yday = 0 ; tm_isdst = false } in
    mktime tm |> fst in
  let time_of_dd_mm_yyyy d m y =
    let y = if y < 100 then y + 2000 (* ? *) else y in
    time_of_yyyy_mm_dd_h_m_s y m d 0 0 0.
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
  (* And now for the only sane formats: *)
  (scan "%4d-%2d-%2d%!" (fun y m d -> time_of_yyyy_mm_dd_h_m_s y m d 0 0 0.)) |||
  (scan "%4d-%2d-%2d%[ tT]%d:%d%!" (fun y mo d _ h mi -> time_of_yyyy_mm_dd_h_m_s y mo d h mi 0.)) |||
  (scan "%4d-%2d-%2d%[ tT]%d:%d:%f%!" (fun y mo d _ h mi s -> time_of_yyyy_mm_dd_h_m_s y mo d h mi s)) |||
  None

(* mktime tm struct "is interpreted in the local time zone". Work around this
 * by dividing by 24h. *)
(*$= time_of_abstime & ~printer:(function None -> "None" | Some f -> string_of_float f)
 (Some 2218.) (BatOption.map (fun ts -> ceil (ts /. 86400.)) (time_of_abstime "28.01.1976"))
 (time_of_abstime "28.01.1976") (time_of_abstime "01/28/1976")
 (time_of_abstime "28.01.1976") (time_of_abstime "1976-01-28")
 (BatOption.map ((+.) (12.*.3600.)) (time_of_abstime "28.01.1976")) \
    (time_of_abstime "1976-01-28 12:00")
 (time_of_abstime "1976-01-28 12:00") (time_of_abstime "1976-01-28T12:00:00")
 (time_of_abstime "1976-01-28 12:00") (time_of_abstime "1976-01-28T12:00:00.1")
 (time_of_abstime "1976-01-28 12:00:01") (time_of_abstime "1976-01-28 12:00:00.9")
 (Some 1523052000.) (time_of_abstime "1523052000")
 *)

(* Replace ${tuple.field} by the actual value the passed string: *)
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

(* Similarly, but for simpler identifiers without tuple prefix: a function
 * to replace a map of keys by their values in a string.
 * Keys are delimited in the string with "${" "}".
 * Used to replace notification parameters by their values. *)
let subst_dict =
  let open Str in
  let re =
    regexp "\\${\\([_a-zA-Z][-_a-zA-Z0-9]*\\)}" in
  fun dict ?(quote=identity) ?null text ->
    global_substitute re (fun s ->
      let var_name = matched_group 1 s in
      (try List.assoc var_name dict
      with Not_found ->
        !logger.debug "Unknown parameter %S" var_name ;
        null |? "??"^ var_name ^"??") |>
      quote
    ) text

(*$= subst_dict & ~printer:(fun x -> x)
  "glop 'pas' glop" \
      (subst_dict ~quote:shell_quote ["glop", "pas"] "glop ${glop} glop")
  "pas"           (subst_dict ["glop", "pas"] "${glop}")
  "??"            (subst_dict ~null:"??" ["glop", "pas"] "${gloup}")
 *)

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

let fail_with_context ctx f =
  try f () with e ->
    Printf.sprintf "While %s: %s"
      ctx (Printexc.to_string e) |>
    failwith

let ppp_of_fd ?(default="") ppp fd =
  Unix.(lseek fd 0 SEEK_SET) |> ignore ;
  let str = read_whole_fd fd in
  let str = if str = "" then default else str in
  fail_with_context "parsing a file descriptor"
    (fun () -> PPP.of_string_exc ppp str)

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
          (fun ic ->
            fail_with_context ("parsing file "^ fname)
              (fun () -> PPP.of_in_channel_exc ppp ic)) ic in
  let cache_name = "ppp_of_file ("^ (ppp ()).descr 0 ^")" in
  cached cache_name reread (mtime_of_file_def 0.)

let ppp_to_fd ?pretty fd ppp v =
  Unix.(lseek fd 0 SEEK_SET) |> ignore ;
  let str = PPP.to_string ?pretty ppp v in
  let len = String.length str in
  if len = Unix.write_substring fd str 0 len then
    Unix.ftruncate fd len
  else
    Printf.sprintf "Cannot write %d bytes into fd" len |>
    failwith

let ppp_to_file ?pretty fname ppp v =
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
        (PPP.to_out_channel ?pretty ppp oc) v

let rec reach_fixed_point ?max_try f =
  match max_try with Some n when n <= 0 -> false
  | _ ->
    if f () then (
      !logger.debug "Looping to reach fixed point" ;
      reach_fixed_point ?max_try:(Option.map pred max_try) f
    ) else true

(* How many ways to choose n undistinguishable things in a set of m *)
let comb n m =
  assert (n <= m) ;
  let rec loop num den i =
    if i > n then num /. den else
    loop
      (num *. float_of_int (m + 1 - i))
      (den *. float_of_int i)
      (i + 1) in
  loop 1. 1. 1
(*$= comb & ~printer:string_of_float
  1. (comb 0 10)
  1. (comb 10 10)
  2_598_960. (comb 5 52)
 *)

(* TODO: should go in batteries *)
let option_map2 f o1 o2 =
  match o1, o2 with
  | None, _ | _, None -> None
  | Some a, Some b -> Some (f a b)

(* To circumvent short-cuts *)
let (|||) = (||)

let pretty_enum_print p oc e =
  let rec loop first x =
    match Enum.get e with
    | None ->
        Printf.fprintf oc "%s%a" (if first then "" else " and ") p x
    | Some next ->
        Printf.fprintf oc "%s%a" (if first then "" else ", ") p x ;
        loop false next in
  match Enum.get e with
  | None -> String.print oc "<empty>"
  | Some x -> loop true x

let pretty_list_print p oc =
  pretty_enum_print p oc % List.enum

let pretty_array_print p oc =
  pretty_enum_print p oc % Array.enum

(* Return the distance (as a float) between two values of the same type: *)
module Distance = struct
  let float a b = abs_float (a -. b)

  let string a b =
    (* TODO *)
    String.length a - String.length b |> float_of_int
end

(* We need an accept that stops waiting whenever the while_ condition become
 * false. It is not enough to check while_ on EINTR since the actual signal handler
 * will be resumed and the OCaml signal handler might not be have run yet when
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
let forking_server ~while_ sockaddr server_fun =
  let open Legacy.Unix in
  (* Keep an eye on my sons pids: *)
  let sons = Atomic.Set.make () in
  let killer_thread =
    Thread.create (fun () ->
      let stop_since = ref 0. in
      while true do
        let now = gettimeofday () in
        let continue = while_ () in
        (* If we want to quit, kill the sons: *)
        if not continue then (
          if !stop_since = 0. then stop_since := now ;
          if not (Atomic.Set.is_empty sons) then (
            !logger.info "Killing httpd servers..." ;
            Atomic.Set.iter sons (fun (pid, _) ->
              !logger.debug "Killing %d" pid ;
              log_and_ignore_exceptions ~what:"stopping httpd servers"
                (kill pid)
                Sys.(if now -. !stop_since > 3. then sigkill else sigterm))
          ) else (
            !logger.debug "Quit servers killer" ;
            Thread.exit ()
          )
        ) ;
        (* Collect the sons statuses: *)
        Atomic.Set.filter sons
          (fun (pid, start) ->
            match BatUnix.restart_on_EINTR (waitpid [ WNOHANG ]) pid with
            | 0, _ -> true
            | _, status ->
                let dt = now -. start in
                (if status = WEXITED 0 then !logger.debug else !logger.error)
                  "httpd server %d %s after %fs"
                    pid (string_of_process_status status) dt ;
                false) ;
        sleep 1
      done
    ) () in
  (* Now fork a new server for each new connection: *)
  let sock =
    socket ~cloexec:true (domain_of_sockaddr sockaddr) SOCK_STREAM 0 in
  finally (fun () -> close sock)
    (fun () ->
      setsockopt sock SO_REUSEADDR true ;
      bind sock sockaddr ;
      listen sock 5 ;
      try
        while while_ () do
          let s, _caller = my_accept ~while_ sock in
          (* Before forking, advance the PRNG so that all children do not re-init
           * their own PRNG with the same number: *)
          let prng_init = Random.bits () in
          flush_all () ;
          match fork () with
          | 0 ->
              Random.init prng_init ;
              close sock ;
              server_fun s ;
              exit 0
          | pid ->
              close s ;
              Atomic.Set.add sons (pid, Unix.gettimeofday ())
        done
      with Exit ->
        !logger.debug "Stop accepting connections."
    ) () ;
  Thread.join killer_thread

let cap ?min ?max f =
  let f = Option.map_default (Pervasives.max f) f min in
  Option.map_default (Pervasives.min f) f max

(*$= cap & ~printer:string_of_int
  2 (cap ~min:1 ~max:3 2)
  1 (cap ~min:1 ~max:3 0)
  0 (cap ~max:3 0)
  3 (cap ~min:1 ~max:3 5)
  5 (cap ~min:1 5)
*)

let strip_control_chars =
  let open Str in
  let res =
    [ regexp "[\n\r] *", " " ;
      regexp "\t", "    " ;
      regexp "\027\\[[0-9];[0-9]+m", "" ;
      regexp "\027\\[0m", "" ] in
  fun msg ->
    List.fold_left (fun s (re, repl) ->
      Str.global_replace re repl s
    ) msg res

(* Return whether we are _below_ the rate limit *)
let rate_limit max_events duration =
  let last_period = ref 0
  and count = ref 0 in
  fun () ->
    let now = Unix.time () in
    let period = int_of_float (now /. duration) in
    if period = !last_period && !count >= max_events then false else (
      if period = !last_period then (
        incr count
      ) else (
        last_period := period ;
        count := 1
      ) ;
      true)

let string_same_pref l a b =
  if l > String.length a || l > String.length b then false
  else
    try
      for i = 0 to l - 1 do
        if a.[i] <> b.[i] then raise Exit
      done ;
      true
    with Exit -> false

let as_date ?rel t =
  let full = string_of_time t in
  match rel with
  | None -> full
  | Some rel ->
      let possible_cuts = [| 11; 14; 18 |] in
      let rec loop i =
        if i < 0 then
          full
        else (
          let pref_len = possible_cuts.(i) in
          if string_same_pref pref_len rel full then
            String.lchop ~n:pref_len full
          else
            loop (i - 1)
        ) in
      loop (Array.length possible_cuts - 1)

(*$= as_date & ~printer:(fun x -> x)
  "2018-11-14 22h13m20s" (as_date ~rel:"" 1542230000.)
  "2018-11-14 22h13m20s" (as_date ~rel:"1983-11-14 22h13m20s" 1542230000.)
  "22h13m20s" (as_date ~rel:"2018-11-14 08h12m32s" 1542230000.)
  "13m20s" (as_date ~rel:"2018-11-14 22h12m20s" 1542230000.)
*)

(* A pretty printer for timestamps, with the peculiarity that it tries to not
 * repeat the date components that have already been written, saved in [rel]. *)
let print_as_date ?rel oc t =
  let s = as_date ?rel:(Option.map (!) rel) t in
  Option.may (fun rel -> rel := s) rel ;
  String.print oc s
