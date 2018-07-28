(* See RamenNames.mli *)
open Batteries
open RamenHelpers

type 'a t = string [@@ppp PPP_OCaml]

(* Function names *)

type func = [`Function] t

let func_ppp_ocaml = t_ppp_ocaml

let func_of_string s =
  (* New lines have to be forbidden because of the out_ref ringbuf files.
   * slashes have to be forbidden because we rsplit to get program names.
   *)
  if s = "" ||
     String.fold_left (fun bad c ->
       bad || c = '\n' || c = '\r' || c = '/') false s then
    invalid_arg "operation name" ;
  s

external string_of_func : func -> string = "%identity"


(* Program names *)

type program = [`Program] t

let program_ppp_ocaml = t_ppp_ocaml

let program_of_string s =
  (* Curly braces are reserved to enclose parameter expansion, which can
   * have any character (in between the first '{' of the program name and
   * the last '}' of the program name: *)
  if String.contains s '{' || String.contains s '}' then
    failwith "Program names cannot use curly braces" ;
  let rec remove_heading_slashes s =
    if String.length s > 0 && s.[0] = '/' then
      remove_heading_slashes (String.lchop s)
    else s in
  let s = remove_heading_slashes s in
  if s = "" then
    failwith "Program names must not be empty" else
  if has_dotnames s then
    failwith "Program names cannot include directory dotnames" else
  simplified_path s

external string_of_program : program -> string = "%identity"

(* Make sure a path component is shorter that max_dir_len: *)
let max_dir_len = 255
let abbrev s =
  if String.length s <= max_dir_len then s else md5 s

let path_of_program prog =
  String.split_on_char '/' prog |>
  List.map abbrev |>
  String.join "/"

(* Relative Program Names: "../" are allowed, and conversion to a normal
 * program name requires the location from which the program is relative: *)

type rel_program = [`RelProgram] t

let rel_program_ppp_ocaml = t_ppp_ocaml

let rel_program_of_string s =
  if s = "" then invalid_arg "relative program name"
  else s

external string_of_rel_program : rel_program -> string = "%identity"

let program_of_rel_program start rel_program =
  (* TODO: for now we just support "../" prefix: *)
  if rel_program = ".." || String.starts_with rel_program "../" then
    simplified_path (start ^"/"^ rel_program)
  else rel_program


(* Program parameters
 *
 * String representation is either:
 * - The value of the parameter uniq_name if present, a string, and unique;
 * - The printed out values of all parameters ("{p1=n1;p2=v2;...}"), if short
 *   enough;
 * - The MD5 hash of the above, otherwise.
 *
 * How do we know if uniq_name is indeed unique? We do not, that's the
 * supervisor responsibility to make this variable unique (by appending a
 * sequence number).
 * *)

type param = string * RamenTypes.value [@@ppp PPP_OCaml]
type params = param list [@@ppp PPP_OCaml]

(* FIXME: make those params a Map so names are unique and it's faster to look
 * for uniq_name. *)
let param_compare (a, _) (b, _) = String.compare a b

let params_sort = List.fast_sort param_compare

let print_param oc (n, v) =
  Printf.fprintf oc "%s=%a" n RamenTypes.print v

let string_of_params params =
  try
    List.find_map (function
      | "uniq_name", RamenTypes.VString s -> Some s
      | _ -> None
    ) params
  with Not_found ->
    params_sort params |>
    IO.to_string (List.print ~first:"" ~last:"" ~sep:"," print_param) |>
    abbrev

type params_exp = [`Params] t

let params_exp_ppp_ocaml = t_ppp_ocaml

external params_exp_of_string : string -> params_exp = "%identity"
external string_of_params_exp : params_exp -> string = "%identity"

let params_exp_of_params = function
  | [] -> ""
  | params -> "{"^ string_of_params params ^"}"


(* Program name - expansed ; with the params expansion *)

type program_exp = [`ProgramExp] t

let program_exp_ppp_ocaml = t_ppp_ocaml

external string_of_program_exp : program_exp -> string = "%identity"

external program_exp_of_string : string -> program_exp = "%identity"

let split_program_exp s =
  match String.index s '{' with
  | exception Not_found -> s, ""
  | i -> String.sub s 0 i, String.sub s i (String.length s - i)

let make_program_exp program params =
  program ^ params_exp_of_params params

let program_of_program_exp = fst % split_program_exp

let path_of_program_exp s =
  let prog, params = split_program_exp s in
  let dirs = String.split_on_char '/' prog in
  let rec loop path = function
    | [] -> path
    | [ last ] ->
        let exp = path_quote params in
        let comp = abbrev (last ^ exp) in
        loop (path ^"/"^ comp) []
    | comp :: rest ->
        loop (path ^"/"^ abbrev comp) rest
  in
  loop "" dirs

let program_exp_of_path s =
  let prog, params = split_program_exp s in
  prog ^ path_unquote params

external program_exp_of_program : program -> program_exp = "%identity"

(* Relative programs with expansion: *)

type rel_program_exp = [`RelProgramExp] t

let rel_program_exp_ppp_ocaml = t_ppp_ocaml

external rel_program_exp_of_string : string -> rel_program_exp = "%identity"
external string_of_rel_program_exp : rel_program_exp -> string = "%identity"

let program_exp_of_rel_program_exp start rel_program_exp =
  (* TODO: for now we just support "../" prefix: *)
  if rel_program_exp = ".." ||
     String.starts_with rel_program_exp "..{" ||
     String.starts_with rel_program_exp "../" then
    (* Remove the params since they might contain slash and dots, and we
     * know the last component of the path is going to be stripped anyway: *)
    let start_noexp, _ = split_program_exp start in
    (* Same goes for the rel_program, but those params we will preserve: *)
    let rel_noexp, param_exp = split_program_exp rel_program_exp in
    program_of_rel_program start_noexp rel_noexp ^ param_exp
  else rel_program_exp

let make_rel_program_exp rel_program params =
  rel_program ^ params_exp_of_params params

let split_rel_program_exp = split_program_exp

(* Fully Qualified function names *)

type fq = [`FQ] t

let fq_ppp_ocaml = t_ppp_ocaml

external fq_of_string : string -> fq = "%identity"

external string_of_fq : fq -> string = "%identity"

let fq prog func =
  string_of_program_exp prog ^"/"^ func

let fq_print oc = String.print oc % string_of_fq
