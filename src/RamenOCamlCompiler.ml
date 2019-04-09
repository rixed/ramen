open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module N = RamenName
module Files = RamenFiles

let max_simult_compilations = Atomic.Counter.make 4
let use_external_compiler = ref false
let warnings = "-58-26@5"

(* Mostly copied from ocaml source code driver/optmain.ml *)

module Backend = struct
  let symbol_for_global' = Compilenv.symbol_for_global'
  let closure_symbol = Compilenv.closure_symbol
  let really_import_approx = Import_approx.really_import_approx
  let import_symbol = Import_approx.import_symbol
  let size_int = Arch.size_int
  let big_endian = Arch.big_endian
  let max_sensible_number_of_arguments = Proc.max_arguments_for_tailcalls - 1
end

(* Compiler accumulate some options in there so we have to manually clean
 * it in between two compilations :-< *)
let reset () =
  Clflags.objfiles := [] ;
  Clflags.ccobjs := [] ;
  Clflags.dllibs := [] ;
  Clflags.all_ccopts := [] ;
  Clflags.all_ppx := [] ;
  Clflags.open_modules := [] ;
  Clflags.dllpaths := []

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

let compile_internal conf ~keep_temp_files what src_file obj_file =
  let debug = conf.C.log_level = Debug in
  let backend = (module Backend : Backend_intf.S) in
  !logger.info "Compiling %a" N.path_print_quoted src_file ;
  reset () ;
  Clflags.native_code := true ;
  Clflags.binary_annotations := true ;
  Clflags.annotations := true ;
  Clflags.use_linscan := true ; (* https://caml.inria.fr/mantis/view.php?id=7899 *)
  Clflags.debug := debug ;
  Clflags.verbose := debug ;
  Clflags.no_std_include := true ;
  !logger.debug "Use bundled libraries from %a"
    N.path_print_quoted conf.C.bundle_dir ;
  (* Also include in incdir the directory where the obj_file will be,
   * since other modules (params...) might have been compiled there
   * already: *)
  let obj_dir = Files.dirname obj_file in
  let inc_dirs =
    obj_dir ::
    List.map (fun d ->
      N.path_cat [ conf.C.bundle_dir ; d ]
    ) RamenDepLibs.incdirs in
  Clflags.include_dirs := (inc_dirs :> string list) ;
  Clflags.dlcode := true ;
  Clflags.keep_asm_file := keep_temp_files ;
  (* equivalent to -O2: *)
  if debug then (
    Clflags.default_simplify_rounds := 1 ;
    Clflags.(use_inlining_arguments_set o1_arguments)
  ) else (
    Clflags.default_simplify_rounds := 2 ;
    Clflags.(use_inlining_arguments_set o2_arguments) ;
    Clflags.(use_inlining_arguments_set ~round:0 o1_arguments)
  ) ;
  Clflags.compile_only := true ;
  Clflags.link_everything := false ;
  Warnings.parse_options false warnings ;

  Clflags.output_name := Some (obj_file :> string) ;

  Asmlink.reset () ;
  try
    Optcompile.implementation ~backend (ppf ()) (src_file :> string)
      ((Files.remove_ext src_file) :> string)
  with exn ->
    Location.report_exception (ppf ()) exn ;
    cannot_compile what (Printexc.to_string exn)

let compile_external conf ~keep_temp_files what (src_file : N.path) obj_file =
  let debug = conf.C.log_level = Debug in
  let cmd =
    Printf.sprintf
      "env -i PATH=%s OCAMLPATH=%s \
         nice -n 1 \
           %s ocamlopt%s%s -linscan -thread -annot -w %s \
                     -o %s -package ramen -I %s -c %s"
      (shell_quote RamenCompilConfig.build_path)
      (shell_quote RamenCompilConfig.ocamlpath)
      (shell_quote (RamenCompilConfig.ocamlfind :> string))
      (if debug then " -g" else "")
      (if keep_temp_files then " -S" else "")
      (shell_quote warnings)
      (shell_quote ((Files.dirname obj_file) :> string))
      (shell_quote (obj_file :> string))
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

let compile conf ?(keep_temp_files=false) what src_file obj_file =
  Files.mkdir_all ~is_file:true obj_file ;
  (if !use_external_compiler then compile_external else compile_internal)
    conf ~keep_temp_files what src_file obj_file

(* Function to take some object files, a source file, and produce an
 * executable: *)

let is_ocaml_objfile (fname : N.path) =
  String.ends_with (fname :> string) ".cmx" ||
  String.ends_with (fname :> string) ".cmxa"

let link_internal conf ~keep_temp_files
                  ~what ~inc_dirs ~obj_files
                  ~src_file ~(exec_file : N.path) =
  let debug = conf.C.log_level = Debug in
  let backend = (module Backend : Backend_intf.S) in
  !logger.info "Linking %a" N.path_print_quoted src_file ;
  reset () ;
  Clflags.native_code := true ;
  Clflags.binary_annotations := true ;
  Clflags.use_linscan := true ;
  Clflags.debug := debug ;
  Clflags.verbose := debug ;
  Clflags.no_std_include := true ;
  !logger.debug "Use bundled libraries from %a"
    N.path_print_quoted conf.C.bundle_dir ;
  let inc_dirs =
    List.map (fun d ->
      N.path_cat [ conf.C.bundle_dir ; d ]
    ) RamenDepLibs.incdirs @
    Set.to_list inc_dirs in
  Clflags.include_dirs := (inc_dirs :> string list) ;
  Clflags.dlcode := true ;
  Clflags.keep_asm_file := keep_temp_files ;
  (* equivalent to -O2: *)
  if debug then (
    Clflags.default_simplify_rounds := 1 ;
    Clflags.(use_inlining_arguments_set o1_arguments)
  ) else (
    Clflags.default_simplify_rounds := 2 ;
    Clflags.(use_inlining_arguments_set o2_arguments) ;
    Clflags.(use_inlining_arguments_set ~round:0 o1_arguments)
  ) ;
  Clflags.compile_only := false ;
  Clflags.link_everything := false ;
  Warnings.parse_options false warnings ;

  Clflags.output_name := Some (exec_file :> string) ;

  (* Internal compiler wants .o files elsewhere then in objfiles: *)
  let objfiles, ccobjs =
    List.fold_left (fun (mls, cs) obj_file ->
      if is_ocaml_objfile obj_file then
        obj_file :: mls, cs
      else (* Let's assume it's then a legitimate object file *)
        mls, obj_file :: cs
    ) ([], []) obj_files in
  let objfiles = List.rev objfiles
  and ccobjs = List.rev ccobjs in

  (* Now add the bundled libs and finally the main cmx: *)
  let cmx_file = Files.change_ext "cmx" src_file in
  let objfiles =
    List.map (fun d ->
      N.path_cat [ conf.C.bundle_dir ; d ]
    ) RamenDepLibs.objfiles @
    objfiles @ [ cmx_file ] in

  !logger.debug "objfiles = %a" (List.print N.path_print) objfiles ;
  !logger.debug "ccobjs = %a" (List.print N.path_print) ccobjs ;
  Clflags.ccobjs := (ccobjs :> string list) ;

  Asmlink.reset () ;
  try
    Optcompile.implementation ~backend (ppf ()) (src_file :> string)
      ((Files.remove_ext src_file) :> string) ;
    (* Now link *)
    Compmisc.init_path true ;
    Asmlink.link (ppf ()) (objfiles :> string list) (exec_file :> string)
  with exn ->
    Location.report_exception (ppf ()) exn ;
    cannot_link what (Printexc.to_string exn)

let link_external conf ~keep_temp_files
                  ~what ~inc_dirs ~obj_files
                  ~(src_file : N.path) ~(exec_file : N.path) =
  let debug = conf.C.log_level = Debug in
  let path = getenv ~def:"/usr/bin:/usr/sbin" "PATH"
  and ocamlpath = getenv ~def:"" "OCAMLPATH" in
  let cmd =
    Printf.sprintf
      "env -i PATH=%s OCAMLPATH=%s \
         nice -n 1 \
           ocamlfind ocamlopt%s%s %s -thread -annot \
                       -o %s -package ramen -linkpkg %s %s"
      (shell_quote path)
      (shell_quote ocamlpath)
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

let link conf ?(keep_temp_files=false)
         ~what ~obj_files ~src_file ~exec_file =
  Files.mkdir_all ~is_file:true exec_file ;
  (* We have a few C libraries in bundle_dir/lib that will be searched by the
   * C linker: *)
  let inc_dirs =
    Set.singleton (N.path_cat [ conf.C.bundle_dir ; N.path "lib" ]) in
  (* Look for cmi files in the same dirs where the cmx are: *)
  let inc_dirs, obj_files =
    List.fold_left (fun (s, l) obj_file ->
      if is_ocaml_objfile obj_file then
        Set.add (Files.dirname obj_file) s,
        Files.basename obj_file :: l
      else
        s, obj_file :: l
    ) (inc_dirs, []) obj_files in
  (if !use_external_compiler then link_external else link_internal)
    conf ~keep_temp_files
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
