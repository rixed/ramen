open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf

let max_simult_compilations = Atomic.Counter.make 4
let use_external_compiler = ref false
let bundle_dir = ref ""
let warnings = "-58-26"

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
  let open Clflags in
  objfiles := [] ;
  ccobjs := [] ;
  dllibs := [] ;
  all_ccopts := [] ;
  all_ppx := [] ;
  open_modules := [] ;
  dllpaths := []

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

let cannot_compile func_name status =
  Printf.sprintf "Cannot generate code for function %s: %s"
    (RamenName.func_color func_name) status |>
  failwith

let cannot_link program_name status =
  Printf.sprintf "Cannot generate binary for program %s: %s"
    (RamenName.program_color program_name) status |>
  failwith

(* Takes a source file and produce an object file: *)

let compile_internal conf func_name src_file obj_file =
  let open Clflags in
  let backend = (module Backend : Backend_intf.S) in
  !logger.info "Compiling %S" src_file ;
  reset () ;
  native_code := true ;
  annotations := true ;
  debug := conf.C.log_level = Debug ;
  verbose := !debug ;
  no_std_include := true ;
  !logger.debug "Use bundled libraries from %S" !bundle_dir ;
  include_dirs :=
    List.map (fun d -> !bundle_dir ^"/"^ d) RamenDepLibs.incdirs ;
  dlcode := true ;
  keep_asm_file := conf.C.keep_temp_files ;
  (* equivalent to -O2: *)
  if conf.C.log_level = Debug then (
    default_simplify_rounds := 1 ;
    use_inlining_arguments_set o1_arguments
  ) else (
    default_simplify_rounds := 2 ;
    use_inlining_arguments_set o2_arguments ;
    use_inlining_arguments_set ~round:0 o1_arguments
  ) ;
  compile_only := true ;
  link_everything := false ;
  Warnings.parse_options false warnings ;

  output_name := Some obj_file ;

  Asmlink.reset () ;
  try
    Optcompile.implementation ~backend (ppf ()) src_file
      (Filename.remove_extension src_file)
  with exn ->
    Location.report_exception (ppf ()) exn ;
    cannot_compile func_name (Printexc.to_string exn)

let compile_external conf func_name src_file obj_file =
  let path = getenv ~def:"/usr/bin:/usr/sbin" "PATH"
  and ocamlpath = getenv ~def:"" "OCAMLPATH" in
  let cmd =
    Printf.sprintf
      "env -i PATH=%s OCAMLPATH=%s \
         nice -n 1 \
           ocamlfind ocamlopt%s%s -thread -annot -w '%s' \
                     -o %s -package ramen -c %s"
      (shell_quote path)
      (shell_quote ocamlpath)
      (if conf.C.log_level = Debug then " -g" else "")
      (if conf.C.keep_temp_files then " -S" else "")
      warnings
      (shell_quote obj_file)
      (shell_quote src_file) in
  (* TODO: return an array of arguments and get rid of the shell *)
  let cmd_name = "Compilation of "^ RamenName.string_of_func func_name in
  match run_coprocess ~max_count:max_simult_compilations cmd_name cmd with
  | None ->
      cannot_compile func_name "Cannot run command"
  | Some (Unix.WEXITED 0) ->
      !logger.debug "Compiled %s with: %s"
        (RamenName.string_of_func func_name) cmd
  | Some status ->
      (* As this might well be an installation problem, makes this error
       * report to the GUI: *)
      cannot_compile func_name (string_of_process_status status)

let compile conf func_name src_file obj_file =
  mkdir_all ~is_file:true obj_file ;
  (if !use_external_compiler then compile_external else compile_internal)
    conf func_name src_file obj_file

(* Function to take some object files, a source file, and produce an
 * executable: *)

let link_internal conf program_name inc_dirs obj_files src_file bin_file =
  let open Clflags in
  let backend = (module Backend : Backend_intf.S) in
  !logger.info "Linking %S" src_file ;
  reset () ;
  native_code := true ;
  annotations := true ;
  debug := conf.C.log_level = Debug ;
  verbose := !debug ;
  no_std_include := true ;
  !logger.debug "Use bundled libraries from %S" !bundle_dir ;
  include_dirs :=
    List.map (fun d -> !bundle_dir ^"/"^ d) RamenDepLibs.incdirs @
    Set.to_list inc_dirs ;
  dlcode := true ;
  keep_asm_file := conf.C.keep_temp_files ;
  (* equivalent to -O2: *)
  if conf.C.log_level = Debug then (
    default_simplify_rounds := 1 ;
    use_inlining_arguments_set o1_arguments
  ) else (
    default_simplify_rounds := 2 ;
    use_inlining_arguments_set o2_arguments ;
    use_inlining_arguments_set ~round:0 o1_arguments
  ) ;
  compile_only := false ;
  link_everything := false ;
  Warnings.parse_options false warnings ;

  output_name := Some bin_file ;

  let cmx_file = Filename.remove_extension src_file ^ ".cmx" in
  let objfiles =
    List.map (fun d -> !bundle_dir ^"/"^ d) RamenDepLibs.objfiles @
    obj_files @ [ cmx_file ] in

  !logger.debug "objfiles = %a" (List.print String.print) objfiles ;

  Asmlink.reset () ;
  try
    Optcompile.implementation ~backend (ppf ()) src_file
      (Filename.remove_extension src_file) ;
    (* Now link *)
    Compmisc.init_path true ;
    Asmlink.link (ppf ()) objfiles bin_file
  with exn ->
    Location.report_exception (ppf ()) exn ;
    cannot_link program_name (Printexc.to_string exn)

let link_external conf program_name inc_dirs obj_files src_file bin_file =
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
      (if conf.C.log_level = Debug then " -g" else "")
      (if conf.C.keep_temp_files then " -S" else "")
      (IO.to_string
        (Set.print ~first:"" ~last:"" ~sep:" " (fun oc f ->
          Printf.fprintf oc "-I %s" (shell_quote f))) inc_dirs)
      (shell_quote bin_file)
      (IO.to_string
        (List.print ~first:"" ~last:"" ~sep:" " (fun oc f ->
          Printf.fprintf oc "%s" (shell_quote f))) obj_files)
      (shell_quote src_file) in
  (* TODO: return an array of arguments and get rid of the shell *)
  let cmd_name =
    "Compilation+Link of "^ RamenName.string_of_program program_name in
  match run_coprocess ~max_count:max_simult_compilations cmd_name cmd with
  | None ->
      cannot_link program_name "Cannot run command"
  | Some (Unix.WEXITED 0) ->
      !logger.debug "Compiled %s with: %s"
        (RamenName.string_of_program program_name) cmd ;
  | Some status ->
      (* As this might well be an installation problem, makes this error
       * report to the GUI: *)
      cannot_link program_name (string_of_process_status status)

let link conf program_name obj_files src_file bin_file =
  mkdir_all ~is_file:true bin_file ;
  (* Look for cmi files in the same dirs where the cmx are: *)
  let inc_dirs, obj_files =
    List.fold_left (fun (s, l) cmx ->
      Set.add (Filename.dirname cmx) s,
      Filename.basename cmx :: l
    ) (Set.empty, []) obj_files in
  (if !use_external_compiler then link_external else link_internal)
    conf program_name inc_dirs obj_files src_file bin_file


(* Helpers: *)

let to_module_name s =
  if Char.is_letter s.[0] then s else "m"^ s

(* obj name must not conflict with any external module. *)
let with_code_file_for obj_name conf f =
  assert (obj_name <> "") ;
  let basename =
    Filename.(remove_extension (basename obj_name)) ^".ml" in
  (* Make sure this will result in a valid module name: *)
  let basename = to_module_name basename in
  let fname = Filename.dirname obj_name ^"/"^ basename in
  mkdir_all ~is_file:true fname ;
  (* If keep-temp-file is set, reuse preexisting source code : *)
  if conf.C.keep_temp_files &&
     file_check ~min_size:1 ~has_perms:0o400 fname = FileOk
  then
    !logger.info "Reusing source file %S" fname
  else
    File.with_file_out ~mode:[`create; `text; `trunc] fname f ;
  fname
