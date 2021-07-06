(* AST for the stream processor graph *)
open Batteries
open Stdint

open RamenHelpers
open RamenLog
module T = RamenTypes
module N = RamenName

open RamenParsing

(* Defined here as both RamenProgram and RamenOperation need to parse/print
 * function and program names: *)

let program_name ?(quoted=false) m =
  let quoted_quote = id_quote_escaped >>: fun () -> id_quote_char in
  let what = "program name" in
  let m = what :: m in
  let first_char =
    if quoted then not_id_quote |<| quoted_quote
    else letter |<| underscore |<| dot |<| slash in
  let any_char =
    if quoted then not_id_quote
              else first_char |<| decimal_digit |<| pound in
  (
    first_char ++ repeat ~sep:none ~what any_char >>:
    fun (c, s) -> N.rel_program (String.of_list (c :: s))
  ) m

let func_name ?(quoted=false) m =
  let what = "function name" in
  let m = what :: m in
  let not_quote =
    cond "quoted identifier" (fun c -> c <> id_quote_char && c <> '/') '_' in
  let first_char = if quoted then not_quote
                   else letter |<| underscore in
  let any_char = if quoted then not_quote
                 else first_char |<| decimal_digit in
  (
    first_char ++ repeat_greedy ~sep:none ~what any_char >>:
    fun (c, s) -> N.func (String.of_list (c :: s))
  ) m

let function_name =
  let unquoted = func_name
  and quoted =
    id_quote -+ func_name ~quoted:true +- id_quote in
  (quoted |<| unquoted)

let func_identifier m =
  let m = "function identifier" :: m in
  let unquoted =
    optional ~def:None
      (some program_name +- slash) ++
    func_name
  and quoted =
    id_quote -+
    optional ~def:None
       (some (program_name ~quoted:true) +- slash) ++
    func_name ~quoted:true +-
    id_quote in
  (quoted |<| unquoted) m

let site_identifier m =
  let what = "site identifier" in
  let m = what :: m in
  let site_char =
    letter |<| decimal_digit |<| minus |<|
    underscore |<| star in
  let unquoted =
    repeat_greedy ~sep:none ~what site_char
  and quoted =
    id_quote -+ repeat_greedy ~sep:none ~what not_id_quote +- id_quote in
  (
    quoted |<| unquoted >>: String.of_list
  ) m
