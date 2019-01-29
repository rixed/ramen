(* AST for the stream processor graph *)
open Batteries
open Stdint
open RamenHelpers
open RamenLog

open RamenParsing

(* Defined here as both RamenProgram and RamenOperation need to parse/print
 * function and program names: *)

let program_name ?(quoted=false) m =
  let not_quote =
    cond "quoted identifier" ((<>) '\'') 'x' in
  let quoted_quote = string "''" >>: fun () -> '\'' in
  let what = "program name" in
  let m = what :: m in
  let first_char =
    if quoted then not_quote ||| quoted_quote
    else letter ||| underscore ||| dot ||| slash in
  let any_char = if quoted then not_quote
                 else first_char ||| decimal_digit in
  (
    first_char ++ repeat ~sep:none ~what any_char >>:
    fun (c, s) -> RamenName.rel_program_of_string (String.of_list (c :: s))
  ) m

let func_name ?(quoted=false) m =
  let what = "function name" in
  let m = what :: m in
  let not_quote =
    cond "quoted identifier" (fun c -> c <> '\'' && c <> '/') 'x' in
  let first_char = if quoted then not_quote
                   else letter ||| underscore in
  let any_char = if quoted then not_quote
                 else first_char ||| decimal_digit in
  (
    first_char ++ repeat_greedy ~sep:none ~what any_char >>:
    fun (c, s) -> RamenName.func_of_string (String.of_list (c :: s))
  ) m

let function_name =
  let unquoted = func_name
  and quoted =
    id_quote -+ func_name ~quoted:true +- id_quote in
  (quoted ||| unquoted)

let func_identifier m =
  let m = "function identifier" :: m in
  let unquoted =
    optional ~def:None
      (some program_name +- char '/') ++
    func_name
  and quoted =
    id_quote -+
    optional ~def:None
       (some (program_name ~quoted:true) +- char '/') ++
    func_name ~quoted:true +-
    id_quote
  in (quoted ||| unquoted) m
