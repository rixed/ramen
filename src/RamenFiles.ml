(* Helper functions manipulating files. *)
open Batteries
open RamenHelpers
open RamenLog
module N = RamenName

(*$inject
  open Batteries
  module N = RamenName
*)

let dirname (d : N.path) =
  N.path (Filename.dirname (d :> string))

let basename (d : N.path) =
  N.path (Filename.basename (d :> string))

let unlink (f : N.path) =
  Unix.unlink (f :> string)

let rmdir (d : N.path) =
  Unix.rmdir (d :> string)

let rename (s : N.path) (d : N.path) =
  Unix.rename (s :> string) (d :> string)

let opendir (d : N.path) =
  Unix.opendir (d :> string)

let readdir dh =
  N.path (Unix.readdir dh)

let getcwd () =
  N.path (Unix.getcwd ())

let chdir d =
  let what = Printf.sprintf2 "chdir to %a" N.path_print d in
  log_exceptions ~what (fun () -> Unix.chdir (d : N.path :> string))

(* We consider extensions as string for simplicity and also because it
 * gives us confidence we do not mix them with file names, as many functions
 * accept both types. *)
let ext (f : N.path) =
  let e = Filename.extension (f :> string) in
  if e = "" then e else String.lchop e

let remove_ext (f : N.path) =
  N.path (Filename.remove_extension (f :> string))

let add_ext (f : N.path) ext =
  if ext = "" then f else (
    assert (ext.[0] <> '.') ;
    N.cat f (N.path ("."^ ext)))

let has_ext ext (f : N.path) =
  assert (ext.[0] <> '.') ;
  String.ends_with (f :> string) ("."^ ext)

let has_any_ext f =
  String.contains (f : N.path :> string) '.'

let is_directory (f : N.path) =
  try Sys.is_directory (f :> string) with _ -> false

let files_of (d : N.path) =
  Sys.files_of (d :> string) /@ N.path

let mkdir_all ?(is_file=false) (dir : N.path) =
  let dir = if is_file then dirname dir else dir in
  let rec ensure_exist d =
    if not (N.is_empty d) && not (is_directory d) then (
      ensure_exist (dirname d) ;
      !logger.debug "mkdir %a" N.path_print_quoted d ;
      try Unix.mkdir (d :> string) 0o755
      (* Happens when we have "somepath//someother" (dirname should handle this IMHO) *)
      with Unix.(Unix_error (EEXIST, "mkdir", _)) -> ()
    ) in
  ensure_exist dir

let safe_stat (fname : N.path) =
  Unix.(restart_on_EINTR stat (fname :> string))

let safe_fstat fd =
  Unix.(restart_on_EINTR fstat fd)

type file_status = FileOk | FileMissing | FileTooSmall | FileBadPerms
let check ?(min_size=0) ?(has_perms=0) fname =
  let open Unix in
  match safe_stat fname with
  | exception _ ->
    (* Be it the file or a directory, or a permission issue, we consider the
     * file to be missing: *)
    FileMissing
  | s ->
    if s.st_perm land has_perms <> has_perms then FileBadPerms else
    if s.st_size < min_size then FileTooSmall else
    FileOk

let exists fname = check fname = FileOk

let size fname =
  let s = safe_stat fname in
  s.Unix.st_size

let is_empty_file fname =
  size fname = 0

let int_of_fd fd : int = Obj.magic fd

let safe_fileop f fname =
  try Unix.restart_on_EINTR f fname
  with Unix.(Unix_error (ENOENT, _, _)) -> ()

let safe_unlink fname =
  safe_fileop unlink fname

let safe_open (fname : N.path) flags perms =
  Unix.(restart_on_EINTR openfile (fname :> string) flags perms)

let safe_close fd =
  log_and_ignore_exceptions Unix.(restart_on_EINTR close) fd

let move_aside ?(ext="bad?") (fname : N.path) =
  let bad_file = N.cat fname (N.path ("."^ ext)) in
  ignore_exceptions safe_unlink bad_file ;
  (try restart_on_eintr (rename fname) bad_file
  with
    | Unix.(Unix_error (ENOENT, _, _)) ->
        () (* never mind *)
    | e ->
        !logger.warning "Cannot rename file %a to %a: %s"
          N.path_print fname
          N.path_print bad_file
          (Printexc.to_string e))

let rec rm_rf fname =
  assert (N.length fname > 2) ;
  if not (is_directory fname) then
    Printf.sprintf2 "rm_rf: %a must be a directory"
      N.path_print_quoted fname |>
    invalid_arg
  else (
    foreach (files_of fname) (fun rel ->
      let fname = N.path_cat [ fname ; rel ] in
      if is_directory fname then rm_rf fname
      else safe_unlink fname) ;
    safe_fileop rmdir fname)

let mtime fname =
  let s = safe_stat fname in
  s.Unix.st_mtime

let mtime_def default fname =
  try mtime fname
  with Unix.(Unix_error (ENOENT, _, _)) -> default

let mtime_of_fd fd =
  let open Unix in
  let s = safe_fstat fd in
  s.st_mtime

let age fname =
  let mtime = mtime fname
  and now = Unix.gettimeofday () in
  now -. mtime

let is_older_than ~on_err t fname =
  try
    t < age fname
  with e ->
    print_exception e ;
    on_err

let write_whole_string fd str =
  let len = String.length str in
  Unix.single_write_substring fd str 0 len |> ignore

let write_whole_file fname str =
  mkdir_all ~is_file:true fname ;
  File.with_file_out ~mode:[`text; `create; `trunc] (fname :> string) (fun oc ->
    String.print oc str)

let rec ensure_exists ?(contents="") ?min_size fname =
  mkdir_all ~is_file:true fname ;
  (* If needed, create the file with the initial content, atomically: *)
  let open Unix in
  (* But first, check if the file is already there with the proper size
   * (without opening it, or the following O_EXCL dance won't work!): *)
  match check ?min_size fname with
  | FileOk -> ()
  | FileMissing ->
      !logger.debug "File %a is still missing" N.path_print fname ;
      let flags = [O_CREAT; O_EXCL; O_WRONLY; O_CLOEXEC] in
      (match openfile (fname :> string) flags 0o644 with
      | exception Unix_error (EEXIST, _, _) ->
          (* Wait for some other concurrent process to rebuild it: *)
          !logger.debug "File %a just appeared, give it time..."
            N.path_print fname ;
          sleep 1 ;
          ensure_exists ~contents ?min_size fname
      | fd ->
          !logger.debug "Creating file %a with initial content %S"
            N.path_print fname contents ;
          finally
            (fun () -> close fd)
            (fun () ->
              if contents <> "" then (
                write_whole_string fd contents)) ())
  | FileTooSmall ->
      (* Not my business, wait until the file length is at least min_size,
       * which realistically should not take more than 1s: *)
      let redo () =
        move_aside fname ;
        ensure_exists ~contents ?min_size fname
      in
      if is_older_than ~on_err:true 3. fname then (
        !logger.warning "File %a is an old left-over, let's redo it"
          N.path_print fname ;
        redo ()
      ) else (
        (* Wait for some other concurrent process to rebuild it: *)
        !logger.debug "File %a is being worked on, give it time..."
          N.path_print fname ;
        sleep 1 ;
        ensure_exists ~contents ?min_size fname)
  | FileBadPerms ->
      assert false (* We aren't checking that here *)

let uniquify (fname : N.path) =
  let rec loop n =
    let fname = N.path ((fname :> string) ^"."^ string_of_int n) in
    if exists fname then loop (n + 1) else fname
  in
  if exists fname then loop 0 else fname

(* Will call on_dir and on_file with both the absolute file name and then the
 * name from the given root. *)
let dir_subtree_iter ?on_dir ?on_file root =
  let rec loop_subtree path_from_root =
    let path =
      (* Avoid using "./" as we want path to correspond to program names: *)
      if N.is_empty path_from_root then root
      else N.path_cat [ root ; path_from_root ] in
    match opendir path with
    | exception Unix.(Unix_error (ENOENT, _, _)) ->
        (* A callback must have deleted it, no worries. *) ()
    | dh ->
      let rec loop_files () =
        match readdir dh with
        | exception End_of_file -> ()
        (* Ignore dotnames and any "hidden" dir or files: *)
        | s when not (N.is_empty s) && (s :> string).[0] = '.' ->
            loop_files ()
        | fname_from_path ->
            let fname = N.path_cat [ path ; fname_from_path ] in
            let fname_from_root =
              if N.is_empty path_from_root then fname_from_path
              else N.path_cat [ path_from_root ; fname_from_path ] in
            let may_run = function
              | None -> ()
              | Some f -> log_and_ignore_exceptions (f fname) fname_from_root in
            if is_directory fname then (
              may_run on_dir ;
              let path_from_root' =
                if N.is_empty path_from_root then fname_from_path
                else N.path_cat [ path_from_root ; fname_from_path ] in
              loop_subtree path_from_root'
            ) else
              may_run on_file ;
            loop_files ()
      in
      loop_files () ;
      Unix.closedir dh
  in
  loop_subtree (N.path "")

let change_ext new_ext fname =
  assert (new_ext.[0] <> '.') ;
  add_ext (remove_ext fname) new_ext

let read_whole_file (fname : N.path) =
  File.with_file_in ~mode:[`text] (fname :> string) IO.read_all

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

(* FIXME: read_whole_channels that read several simultaneously! *)

let read_whole_channel ic =
  read_whole_thing (Legacy.input ic)

let read_whole_fd fd =
  read_whole_thing (Unix.read fd)

let touch fname to_when =
  !logger.debug "Touching %a" N.path_print fname ;
  Unix.utimes (fname :> string) to_when to_when

let file_print oc fname =
  let content = File.lines_of fname |> List.of_enum |> String.concat "\n" in
  String.print oc content

let is_absolute (p : N.path) =
  not (N.is_empty p) && (p :> string).[0] = '/'

let absolute_path_of ?cwd (path : N.path) =
  let cwd =
    match cwd with Some p -> p | None -> getcwd () in
  (if is_absolute path then path else N.path_cat [ cwd ; path ]) |>
  N.simplified_path

(*$= absolute_path_of & ~printer:(IO.to_string N.path_print)
  (N.path "/tmp/ramen_root/junkie/csv.x") \
    (absolute_path_of ~cwd:(N.path "/tmp") (N.path "ramen_root/junkie/csv.x"))
 *)

let rel_path_from lib_path path =
  (* If root path is null assume source file is already relative to root: *)
  if N.is_empty lib_path then path else
  let root = absolute_path_of lib_path
  and path = absolute_path_of path in
  if N.starts_with path root then
    let rl = N.length root in
    N.sub path rl (N.length path - rl)
  else
    Printf.sprintf2 "Cannot locate %a within %a"
      N.path_print path
      N.path_print lib_path |>
    failwith

let really_read_fd_into buf offs fd size =
  assert (Bytes.length buf >= offs + size) ;
  let open Unix in
  let rec loop i =
    if i < size then
      let r =
        Unix.restart_on_EINTR (read fd buf (offs + i)) (size - i) in
      if r > 0 then loop (i + r) else (
        if i > 0 then
          !logger.warning "File ended but only %d/%d bytes could be read"
            i size ;
        raise End_of_file)
  in
  loop 0

let really_read_fd fd size =
  let buf = Bytes.create size in
  really_read_fd_into buf 0 fd size

let marshal_into_fd ?(at_start=true) fd v =
  let open Unix in
  (* Leak memory for some reason / and do not write anything to the file
   * if we Marshal.to_channel directly. :-/ *)
  let bytes = Marshal.to_bytes v [] in
  let len = Bytes.length bytes in
  restart_on_EINTR (fun () ->
    if at_start then lseek fd 0 SEEK_SET |> ignore ;
    write fd bytes 0 len) () |> ignore

let marshal_into_file ?at_start fname v =
  mkdir_all ~is_file:true fname ;
  let flags = Unix.[ O_WRONLY ; O_CREAT ; O_CLOEXEC ] in
  let fd = safe_open fname flags 0o644 in
  finally
    (fun () ->safe_close fd)
    (marshal_into_fd ?at_start fd) v

let marshal_from_fd ?default fname fd =
  (* Useful log statement in case the GC crashes right away: *)
  !logger.debug "Retrieving marshaled value from file %a"
    N.path_print fname ;
  try
    let buf, data_sz =
      let capa = 8000 in
      let buf = Bytes.create (Marshal.header_size + capa) in
      really_read_fd_into buf 0 fd Marshal.header_size ;
      let data_size = Marshal.data_size buf 0 in
      if data_size <= capa then
        buf, data_size
      else
        let buf' = Bytes.create (Marshal.header_size + data_size) in
        Bytes.blit buf 0 buf' 0 Marshal.header_size ;
        buf', data_size in
    really_read_fd_into buf Marshal.header_size fd data_sz ;
    Marshal.from_bytes buf 0
  with e ->
    let bt = Printexc.get_raw_backtrace () in
    (match default with
    | None ->
        Printexc.raise_with_backtrace e bt
    | Some d ->
        !logger.error "Cannot unmarshal from file %a: %s"
          N.path_print fname (Printexc.to_string e) ;
        d)

let marshal_from_file ?default fname =
  let flags = Unix.[ O_RDONLY ; O_CLOEXEC ] in
  let fd = safe_open fname flags 0o644 in
  finally
    (fun () ->safe_close fd)
    (marshal_from_fd ?default fname) fd

let same_content a b =
  let same_size () =
    try size a = size b
    with Unix.(Unix_error (ENOENT, _, _)) -> false
  and same_content () =
    read_whole_file a = read_whole_file b
  in
  same_size () && same_content ()

(* Given two files, compare their content and if it differs rename
 * [src] into [dst] ; otherwise merely deletes [src].
 * This is to avoid touching the mtime of the destination when not needed.
 * [dst] might not exist but the directory does.
 * Returns true iff the file was moved. *)
let replace_if_different ~src ~dst =
  if same_content src dst then (
    unlink src ;
    false
  ) else (
    rename src dst ;
    true
  )

(*$R replace_if_different
  let open Batteries in
  let tmpdir =
    N.path ("/tmp/ramen_inline_test_"^ string_of_int (Unix.getpid ())) in
  mkdir_all tmpdir ;
  let src = N.path_cat [ tmpdir ; N.path "replace_if_different.src" ]
  and dst = N.path_cat [ tmpdir ; N.path "replace_if_different.dst" ] in
  let set_content (f : N.path) = function
    | Some c ->
        File.with_file_out (f :> string) (fun oc -> String.print oc c)
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
    asrt "src file must be gone" (not (RamenFiles.exists src)) ;
    asrt "dst file must be present" (RamenFiles.exists dst) ;
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
  safe_unlink src ; safe_unlink dst ; rmdir tmpdir
*)

let same a b =
  a = b || (
    let sa = safe_stat a
    and sb = safe_stat b in
    Unix.(sa.st_dev = sb.st_dev && sa.st_ino = sb.st_ino)
  )

let save ~compute ~serialize ~deserialize fname =
  try read_whole_file fname |> deserialize
  with _ ->
    mkdir_all ~is_file:true fname ;
    File.with_file_out ~mode:[`create;`trunc;`text] (fname :> string)
      (fun oc ->
        let v = compute () in
        String.print oc (serialize v) ;
        v)

(* [default] replaces a missing _or_empty_ file. *)
let ppp_of_fd ?default ppp =
  let reread _fname fd =
    let from_string s =
      let c = Printf.sprintf2 "parsing default value: %s" s in
      fail_with_context c (fun () -> PPP.of_string_exc ppp s)
    and from_fd fd =
      let c = Printf.sprintf2 "parsing file %d" (int_of_fd fd) in
      let s = read_whole_fd fd in
      fail_with_context c (fun () -> PPP.of_string_exc ppp s) in
    !logger.debug "Have to reread %d" (int_of_fd fd) ;
    match default with
    | Some d ->
        let s = safe_fstat fd in
        if s.st_size = 0 then from_string d else from_fd fd
    | None ->
        from_fd fd in
  let cache_name = "ppp_of_file ("^ (ppp ()).descr 0 ^")" in
  cached2 cache_name reread (fun _ fd -> mtime_of_fd fd)

(* [default] replaces a missing _or_empty_ file. *)
let ppp_of_file ?(errors_ok=false) ?default ppp =
  let reread fname =
    let from_string s =
      let c = Printf.sprintf2 "parsing default value for file %a: %s"
                N.path_print fname s in
      fail_with_context c (fun () -> PPP.of_string_exc ppp s)
    and from_in ic =
      let c = Printf.sprintf2 "parsing file %a" N.path_print fname in
      fail_with_context c (fun () -> PPP.of_in_channel_exc ppp ic) in
    !logger.debug "Have to reread %a" N.path_print_quoted fname ;
    let openflags = [ Open_rdonly; Open_text ] in
    match Legacy.open_in_gen openflags 0o644 (fname :> string) with
    | exception e ->
        (match default with
        | None ->
            (if errors_ok then !logger.debug else !logger.warning)
              "Cannot open %a for reading: %s"
              N.path_print_quoted fname (Printexc.to_string e) ;
            raise e
        | Some d -> from_string d)
    | ic ->
        finally
          (fun () -> Legacy.close_in ic)
          (fun () ->
            match default with
            | Some d ->
                let fd = Legacy.Unix.descr_of_in_channel ic in
                let s = safe_fstat fd in
                if s.st_size = 0 then from_string d else from_in ic
            | None -> from_in ic) () in
  let cache_name = "ppp_of_file ("^ (ppp ()).descr 0 ^")" in
  cached cache_name reread (mtime_def 0.)

(* We use the file mtime as a version number so we'd better make sure it does
 * not stay the same after a write: *)
let ensure_mtime_progress fname f =
  let before = mtime_def 0. fname in
  let res = f () in
  let after = mtime fname in
  if after <= before then
    (* Assuming we have at least 1ms acuracy for mtimes: *)
    touch fname (before +. 0.01) ;
  res

let ppp_to_fd ?pretty ppp fname fd v =
  Unix.restart_on_EINTR (Unix.lseek fd 0) SEEK_SET |> ignore ;
  let str = PPP.to_string ?pretty ppp v in
  let len = String.length str in
  ensure_mtime_progress fname (fun () ->
    if len = Unix.write_substring fd str 0 len then
      Unix.ftruncate fd len
    else
      Printf.sprintf "Cannot write %d bytes into fd" len |>
      failwith)

let ppp_to_file ?pretty fname ppp v =
  mkdir_all ~is_file:true fname ;
  let openflags = [ Open_wronly; Open_creat; Open_trunc; Open_text ] in
  match Pervasives.open_out_gen openflags 0o644 (fname :> string) with
  | exception e ->
      !logger.warning "Cannot open %a for writing: %s"
        N.path_print_quoted fname (Printexc.to_string e) ;
      raise e
  | oc ->
      ensure_mtime_progress fname (fun () ->
        finally
          (fun () -> Pervasives.close_out oc)
          (PPP.to_out_channel ?pretty ppp oc) v)

(*
 * Subprocesses
 *)

let with_subprocess ?expected_status ?env cmd args k =
  (* Got some Unix_error(EBADF, "close_process_in", "") suggesting the
   * fd is closed several times so limit the magic: *)
  let open Legacy.Unix in
  !logger.debug "Going to exec %a %a"
    N.path_print cmd
    (Array.print String.print) args ;
  (* Check that the file is present before forking; as we are not going to
   * check the child exit status it will give us a better error message. *)
  if not (exists cmd) then
    failwith (Printf.sprintf2 "File %a does not exist" N.path_print cmd) ;
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
      move_fd his_err stderr ;
      move_fd his_in stdin ;
      move_fd his_out stdout ;
      execve (cmd :> string) args env
    with e ->
      Printf.eprintf "Cannot execve: %s\n%!" (Printexc.to_string e) ;
      sys_exit 126)
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
  with_subprocess ?expected_status ?env cmd args (fun (_ic, oc, ec) ->
    let dump_stderr () =
      let last_words = read_whole_channel ec in
      if last_words <> "" then
        !logger.warning "%a: %s" N.path_print cmd last_words in
    finally dump_stderr k oc)

(*$= with_stdout_from_command & ~printer:identity
  "glop" (with_stdout_from_command (N.path "/bin/echo") \
            [|"/bin/echo";"glop"|] Legacy.input_line)
*)

let quote (s : N.path) =
  let s = String.nreplace (s :> string) "%" "%%" in
  String.nreplace s "/" "%2F" |>
  N.path

(*$= quote & ~printer:(IO.to_string N.path_print)
  (N.path "") (quote (N.path ""))
  (N.path "glop") (quote (N.path "glop"))
  (N.path "pas%2Fglop") (quote (N.path "pas/glop"))
  (N.path "%%%2Fglop%2Fpas%%pas%2Fglop%2F%%") \
      (quote (N.path "%/glop/pas%pas/glop/%"))
  (N.path "%2F%%glop%2Fpas%%pas%2Fglop%%%2F") \
      (quote (N.path "/%glop/pas%pas/glop%/"))
*)

let unquote (s : N.path) =
  let s = (s :> string) in
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
  loop 0 |> N.path

(*$= unquote & ~printer:(IO.to_string N.path_print)
  (N.path "") (unquote (N.path ""))
  (N.path "glop") (unquote (N.path "glop"))
  (N.path "pas/glop") (unquote (N.path "pas%2Fglop"))
  (N.path "%/glop/pas%pas/glop/%") \
      (unquote (N.path "%%%2Fglop%2Fpas%%pas%2Fglop%2F%%"))
  (N.path "/%glop/pas%pas/glop%/") \
      (unquote (N.path "%2F%%glop%2Fpas%%pas%2Fglop%%%2F"))
*)
(*$T unquote
  try ignore (unquote (N.path "%")) ; false with Invalid_argument _ -> true
  try ignore (unquote (N.path "%%%")) ; false with Invalid_argument _ -> true
  try ignore (unquote (N.path "%2")) ; false with Invalid_argument _ -> true
  try ignore (unquote (N.path "%2f")) ; false with Invalid_argument _ -> true
  try ignore (unquote (N.path "%2z")) ; false with Invalid_argument _ -> true
  try ignore (unquote (N.path "%3F")) ; false with Invalid_argument _ -> true
*)

(* Used as a placeholder for file names that are sockets: *)
let socket_path = N.path "socket"

let check_file_is_secure fname =
  let s = safe_stat fname in
  if s.Unix.st_perm land 0o77 <> 0 then
    Printf.sprintf2 "File %a is readable or writable by others"
      N.path_print fname |>
    failwith

let read_key ~secure fname =
  if secure then check_file_is_secure fname ;
  read_whole_file fname |>
  String.trim

let write_key ~secure fname key =
  mkdir_all ~is_file:true fname ;
  let open Unix in
  (try move_aside ~ext:"old" fname
  with Unix_error (ENOENT, _, _) -> ()) ;
  let perm = if secure then 0o400 else 0o444 in
  let flags = [ O_WRONLY ; O_CREAT ; O_EXCL ; O_CLOEXEC ] in
  let fd = openfile (fname :> string) flags perm in
  finally
    (fun () -> safe_close fd)
    (write_whole_string fd) key

let inode fname =
  let stats = safe_stat fname in
  stats.Unix.st_ino

(*
 * (Pre)Compilation creates a lot of temporary files. Here
 * we collect all their name so we delete them at the end:
 *)
let clean_temporary ~keep f =
  let temp_files = ref Set.empty in
  let add_single_temp_file f = temp_files := Set.add f !temp_files in
  let add_temp_file f =
    add_single_temp_file (change_ext "ml" f) ;
    add_single_temp_file (change_ext "cmx" f) ;
    add_single_temp_file (change_ext "cmi" f) ;
    add_single_temp_file (change_ext "o" f) ;
    add_single_temp_file (change_ext "s" f) ;
    add_single_temp_file (change_ext "cc" f) ;
    add_single_temp_file (change_ext "cpp" f) ;
    add_single_temp_file (change_ext "cmt" f) ;
    add_single_temp_file (change_ext "cmti" f) ;
    add_single_temp_file (change_ext "annot" f) in
  (* Keep everything unless all goes well: *)
  let keep' = ref true in
  let del_temp_files () =
    if not !keep' then
      Set.iter (fun fname ->
        !logger.debug "Deleting temp file %a" N.path_print fname ;
        log_and_ignore_exceptions safe_unlink fname
      ) !temp_files
  in
  finally del_temp_files (fun () ->
    let res = f add_single_temp_file add_temp_file in
    keep' := keep ;
    res)

let cp src dst =
  let status =
    Printf.sprintf2 "cp %s %s"
      (shell_quote (src : N.path :> string))
      (shell_quote (dst : N.path :> string)) |>
    Unix.system in
  match status with
  | Unix.WEXITED 0 -> ()
  | _ ->
      !logger.error "Cannot copy file %a to %a: cp returned %s"
        N.path_print src
        N.path_print dst
        (string_of_process_status status)

(* Linux, for some reason, has two names associated with a process: argv[0]
 * and /proc/self/comm. This is on top of its executable file name, which can
 * be though of another name for the process and indeed is by used so by
 * convention.
 * This confuses some users, who see different results in different tools
 * (typically: ps -o comm, ps -o args, htop, top...).
 * Here we try to reduce this confusion by setting comm to argv[0].
 * Keep in mind comm is limited to 16 bytes though. But for workers, those
 * 16 first bytes of argv[0] are still more informative than the executable
 * file name (which is the default comm).
 *)
let reset_process_name () =
  try
    let name = Sys.argv.(0) in
    let pid = Unix.getpid () in
    let fname = N.path ("/proc/"^ string_of_int pid ^"/comm") in
    let flags = Unix.[ O_WRONLY ] in
    let fd = safe_open fname flags 0o000 in
    write_whole_string fd name ;
    safe_close fd ;
    !logger.debug "Set process name to %S" name
  with e ->
    !logger.debug "Cannot set_process_name: %s" (Printexc.to_string e)
