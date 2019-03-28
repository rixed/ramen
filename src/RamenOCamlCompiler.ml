open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module N = RamenName
module Files = RamenFiles

let max_simult_compilations = Atomic.Counter.make 4
let warnings = "-58-26@5"

(* Return a formatter outputting in the logger *)
let ppf () =
  let oc =
    match !logger.output with
    | Syslog ->
        (match RamenLog.syslog with
        | None -> stdnull
        | Some slog ->
            let buf = Buffer.create 100 in
            let write c =
              if c = '\n' then (
                Syslog.syslog slog `LOG_ERR (Buffer.contents buf);
                Buffer.clear buf
              ) else Buffer.add_char buf c in
            let output b s l =
              for i = 0 to l-1 do
                write (Bytes.get b (s + i))
              done ;
              l
            and flush () = ()
            and close () = () in
            BatIO.create_out ~write ~output ~flush ~close)
    | output ->
        let tm = Unix.(gettimeofday () |> localtime) in
        RamenLog.do_output output tm true in
  BatFormat.formatter_of_output oc

let cannot_compile what status =
  Printf.sprintf "Cannot generate code for %s: %s"
    what status |>
  failwith

let cannot_link what status =
  Printf.sprintf "Cannot generate binary for %s: %s" what status |>
  failwith

(* Takes a source file and produce an object file: *)

let ocamlpath () =
  let open RamenCompilConfig in
  (
    if N.is_empty ocamlfind_destdir then ""
    else (ocamlfind_destdir :> string) ^ ":"
  ) ^ ocamlpath

let compile_external ~debug ~keep_temp_files what
                     (src_file : N.path) (obj_file : N.path) =
  let cmd =
    Printf.sprintf
      "env -i PATH=%s OCAMLPATH=%s \
         nice -n 1 \
           %s ocamlopt%s%s -linscan -thread -bin-annot -w %s \
                     -o %s -package ramen -I %s -c %s"
      (shell_quote RamenCompilConfig.build_path)
      (shell_quote (ocamlpath ()))
      (shell_quote (RamenCompilConfig.ocamlfind :> string))
      (if debug then " -g" else "")
      (if keep_temp_files then " -S" else "")
      (shell_quote warnings)
      (shell_quote (obj_file :> string))
      (shell_quote ((Files.dirname obj_file) :> string))
      (shell_quote (src_file :> string)) in
  (* TODO: return an array of arguments and get rid of the shell *)
  let cmd_name = "Compilation of "^ what in
  match run_coprocess ~max_count:max_simult_compilations cmd_name cmd with
  | None ->
      cannot_compile what "Cannot run command"
  | Some (Unix.WEXITED 0) ->
      !logger.debug "Compiled %s with: %s" what cmd
  | Some status ->
      (* As this might well be an installation problem, makes this error
       * report to the GUI: *)
      cannot_compile what (string_of_process_status status)

let compile ?(debug=false) ?(keep_temp_files=false) what src_file obj_file =
  Files.mkdir_all ~is_file:true obj_file ;
  compile_external ~debug ~keep_temp_files what src_file obj_file

(* Function to take some object files, a source file, and produce an
 * executable: *)

let is_ocaml_objfile (fname : N.path) =
  String.ends_with (fname :> string) ".cmx" ||
  String.ends_with (fname :> string) ".cmxa"

let link_external ~debug ~keep_temp_files
                  ~what ~inc_dirs ~obj_files
                  ~(src_file : N.path) ~(exec_file : N.path) =
  let cmd =
    Printf.sprintf
      "env -i PATH=%s OCAMLPATH=%s \
         nice -n 1 \
           %s ocamlopt%s%s %s -thread -annot \
                       -o %s -package ramen -linkpkg %s %s"
      (shell_quote RamenCompilConfig.build_path)
      (shell_quote (ocamlpath ()))
      (shell_quote (RamenCompilConfig.ocamlfind :> string))
      (if debug then " -g" else "")
      (if keep_temp_files then " -S" else "")
      (IO.to_string
        (Set.print ~first:"" ~last:"" ~sep:" " (fun oc (f : N.path) ->
          Printf.fprintf oc "-I %s" (shell_quote (f :> string)))) inc_dirs)
      (shell_quote (exec_file :> string))
      (IO.to_string
        (List.print ~first:"" ~last:"" ~sep:" " (fun oc (f : N.path) ->
          Printf.fprintf oc "%s" (shell_quote (f :> string)))) obj_files)
      (shell_quote (src_file :> string)) in
  (* TODO: return an array of arguments and get rid of the shell *)
  let cmd_name = "Compilation+Link of "^ what in
  match run_coprocess ~max_count:max_simult_compilations cmd_name cmd with
  | None ->
      cannot_link what "Cannot run command"
  | Some (Unix.WEXITED 0) ->
      !logger.debug "Compiled %s with: %s" what cmd ;
  | Some status ->
      (* As this might well be an installation problem, makes this error
       * report to the GUI: *)
      cannot_link what (string_of_process_status status)

let link ?(debug=false) ?(keep_temp_files=false)
         ~what ~obj_files ~src_file ~exec_file =
  Files.mkdir_all ~is_file:true exec_file ;
  (* Look for cmi files in the same dirs where the cmx are: *)
  let inc_dirs =
    String.nsplit ~by:"," RamenCompilConfig.runtime_libdirs |>
    List.filter ((<>) "") |>
    List.map N.path |>
    Set.of_list in
  let inc_dirs, obj_files =
    List.fold_left (fun (s, l) obj_file ->
      if is_ocaml_objfile obj_file then
        Set.add (Files.dirname obj_file) s,
        Files.basename obj_file :: l
      else
        s, obj_file :: l
    ) (inc_dirs, []) obj_files in
  link_external
    ~debug ~keep_temp_files
    ~what ~inc_dirs ~obj_files ~src_file ~exec_file


(* Helpers: *)

(* Accepts a filename (without directory) and change it into something valid
 * as an ocaml compilation unit: *)
let to_module_name =
  let re = Str.regexp "[^a-zA-Z0-9_]" in
  fun fname ->
    let ext = Files.ext fname in
    let s = Files.remove_ext fname in
    let s =
      if N.is_empty s then "_" else
      (* Encode all chars not allowed in OCaml modules: *)
      let s =
        Str.global_substitute re (fun s ->
          let c = Str.matched_string s in
          assert (String.length c = 1) ;
          let i = Char.code c.[0] in
          "_" ^ string_of_int i ^ "_"
        ) (s :> string) in
      (* Then make sure we start with a letter: *)
      if Char.is_letter s.[0] then s else "m"^ s
    in
    Files.add_ext (N.path s) ext

(* Given a file name, make it a valid module name: *)
let make_valid_for_module (fname : N.path) =
  let dirname, basename =
    try String.rsplit ~by:"/" (fname :> string) |>
        fun (d, b) -> N.path d, N.path b
    with Not_found -> N.path ".", fname in
  let basename = to_module_name basename in
  N.path_cat [ dirname ; basename ]

(* obj name must not conflict with any external module. *)
let with_code_file_for obj_name keep_temp_files f =
  assert (not (N.is_empty obj_name)) ;
  let basename =
    Files.(change_ext "ml" (basename obj_name)) in
  (* Make sure this will result in a valid module name: *)
  let basename = to_module_name basename in
  let fname = N.path_cat [ Files.dirname obj_name ; basename ] in
  Files.mkdir_all ~is_file:true fname ;
  (* If keep-temp-file is set, reuse preexisting source code : *)
  if keep_temp_files &&
     Files.check ~min_size:1 ~has_perms:0o400 fname = FileOk
  then
    !logger.info "Reusing source file %a" N.path_print_quoted fname
  else
    File.with_file_out ~mode:[`create; `text; `trunc] (fname :> string) f ;
  fname

let make_valid_ocaml_identifier s =
  let is_letter c = (c >= 'a' && c <= 'z') ||
                    (c >= 'A' && c <= 'Z')
  and is_digit c = c >= '0' && c <= '9'
  in
  if s = "" then invalid_arg "make_valid_ocaml_identifier: empty" ;
  String.fold_lefti (fun s i c ->
    s ^ (
      if is_letter c || c = '_' ||
         (i > 0 && (c = '\'' || is_digit c))
      then
        if i > 0 then String.of_char c
        else String.of_char (Char.lowercase c)
      else
        (if i > 0 then "'" else "x'") ^ string_of_int (Char.code c))
        (* Here we use the single quote as an escape char, given the single
         * quote is not usable in quoted identifiers on ramen's side. *)
  ) "" s

(* Test that [make_valid_ocaml_identifier] is a projection: *)
(*$Q make_valid_ocaml_identifier
  Q.small_string (fun s -> s = "" || ( \
    let f = make_valid_ocaml_identifier in \
    let i1 = f s in let i2 = f i1 in i1 = i2))
 *)

let module_name_of_file_name fname =
  (Files.(basename fname |> remove_ext) :> string) |>
  make_valid_ocaml_identifier |>
  String.capitalize_ascii
