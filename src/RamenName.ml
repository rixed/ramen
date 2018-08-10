(* See RamenNames.mli *)
open Batteries
open RamenHelpers

type 'a t = string [@@ppp PPP_OCaml]

(* Function names *)

type func = [`Function] t

let func_ppp_ocaml = t_ppp_ocaml

let func_of_string s =
  (* New lines have to be forbidden because of the out_ref ringbuf files.
   * Slashes have to be forbidden because we rsplit to get program names. *)
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

(* Fully Qualified function names *)

type fq = [`FQ] t

let fq_ppp_ocaml = t_ppp_ocaml

external fq_of_string : string -> fq = "%identity"

external string_of_fq : fq -> string = "%identity"

let fq prog func =
  string_of_program prog ^"/"^ func

let fq_print = String.print

(* Base units *)

type base_unit = [`BaseUnit] t

let base_unit_ppp_ocaml = t_ppp_ocaml

external base_unit_of_string : string -> base_unit = "%identity"
external string_of_base_unit : base_unit -> string = "%identity"
let base_unit_print = String.print
