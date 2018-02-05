open Lwt
open RamenLog
open Helpers
module C = RamenConf

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

let compile_internal conf func_name src_file bin_file =
  let open Clflags in
  let backend = (module Backend : Backend_intf.S) in
  !logger.info "Compiling %S" src_file ;
  reset () ;
  native_code := true;
  binary_annotations := conf.C.debug ;
  debug := conf.C.debug ;
  verbose := conf.C.debug ;
  no_std_include := true ;
  !logger.debug "Use bundled libraries from %S" conf.C.bundle_dir ;
  include_dirs :=
    List.map (fun d -> conf.C.bundle_dir ^"/"^ d) RamenDepLibs.incdirs ;
  dlcode := false ;
  keep_asm_file := conf.C.debug ;
  (* equivalent to -O2: *)
  if conf.C.debug then (
    default_simplify_rounds := 1 ;
    use_inlining_arguments_set o1_arguments
  ) else (
    default_simplify_rounds := 2 ;
    use_inlining_arguments_set o2_arguments ;
    use_inlining_arguments_set ~round:0 o1_arguments
  ) ;
  output_name := Some bin_file ;
  let cmx_file = Filename.remove_extension src_file ^ ".cmx" in

  let objfiles =
    List.map (fun d -> conf.C.bundle_dir ^"/"^ d) RamenDepLibs.objfiles @
    [ cmx_file ] in

  let ppf = Format.err_formatter in (* FIXME: direct this to logs *)
  Asmlink.reset () ;
  try
    Optcompile.implementation ~backend ppf src_file
      (Filename.remove_extension src_file) ;
    (* Now link *)
    Compmisc.init_path true ;
    Asmlink.link ppf objfiles bin_file ;
    return_unit
  with exn ->
    Location.report_exception ppf exn ;
    let e = Lang.CannotGenerateCode {
      func = func_name ; cmd = "embedded compiler" ;
      status = Printexc.to_string exn } in
    fail (Lang.SyntaxError e)

let compile_external conf func_name src_file bin_file =
  let path = getenv ~def:"/usr/bin:/usr/sbin" "PATH"
  and ocamlpath = getenv ~def:"" "OCAMLPATH" in
  let comp_cmd =
    Printf.sprintf
      "env -i PATH=%s OCAMLPATH=%s \
         nice -n 20 \
           ocamlfind ocamlopt%s \
                     -o %s -package ramen -linkpkg %s"
      (shell_quote path)
      (shell_quote ocamlpath)
      (if conf.C.debug then " -S -g -annot" else "")
      (shell_quote bin_file)
      (shell_quote src_file) in
  (* TODO: return an array of arguments and get rid of the shell *)
  let cmd = Lwt_process.shell comp_cmd in
  let cmd_name = "compilation of "^ func_name in
  let%lwt status =
    run_coprocess ~max_count:conf.max_simult_compilations cmd_name cmd in
  if status = Unix.WEXITED 0 then (
    !logger.debug "Compiled %s with: %s" func_name comp_cmd ;
    return_unit
  ) else (
    (* As this might well be an installation problem, makes this error
     * report to the GUI: *)
    let e = Lang.CannotGenerateCode {
      func = func_name ; cmd = comp_cmd ;
      status = string_of_process_status status } in
    fail (Lang.SyntaxError e)
  )

let compile conf func_name src_file bin_file =
  (if conf.C.use_embedded_compiler then compile_internal else compile_external)
    conf func_name src_file bin_file
