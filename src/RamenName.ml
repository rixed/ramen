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
let abbrev_fname s =
  if String.length s <= max_dir_len then s else md5 s

let path_of_program prog =
  String.split_on_char '/' prog |>
  List.map abbrev_fname |>
  String.join "/"

(* Program name - expansed *)

type param = string * RamenTypes.value [@@ppp PPP_OCaml]
type params = param list [@@ppp PPP_OCaml]

(* FIXME: make those params a Map so names are unique *)
let param_compare (a, _) (b, _) = String.compare a b

let params_sort = List.fast_sort param_compare

let print_param oc (n, v) =
  Printf.fprintf oc "%s=%a" n RamenTypes.print v

let string_of_params params =
  params_sort params |>
  IO.to_string (List.print ~first:"" ~last:"" ~sep:"," print_param)

type params_exp = [`Params] t

let params_exp_ppp_ocaml = t_ppp_ocaml

external params_exp_of_string : string -> params_exp = "%identity"
external string_of_params_exp : params_exp -> string = "%identity"

let params_exp_of_params = function
  | [] -> ""
  | params -> "{"^ string_of_params params ^"}"


(* Program name - expansed *)

type program_exp = [`ProgramExp] t

let program_exp_ppp_ocaml = t_ppp_ocaml

external string_of_program_exp : program_exp -> string = "%identity"

external program_exp_of_string : string -> program_exp = "%identity"

let split_program_exp s =
  match String.index s '{' with
  | exception Not_found -> s, ""
  | i -> String.sub s 0 i, String.sub s i (String.length s - i)

let program_of_program_exp = fst % split_program_exp

let path_of_program_exp s =
  let prog, params = split_program_exp s in
  let dirs = String.split_on_char '/' prog in
  let rec loop path = function
    | [] -> path
    | [ last ] ->
        let exp = path_quote params in
        let exp =
          if String.length last + String.length exp <= max_dir_len then exp
          else "{"^ md5 exp ^"}" in
        let comp = abbrev_fname (last ^ exp) in
        loop (path ^"/"^ comp) []
    | comp :: rest ->
        loop (path ^"/"^ abbrev_fname comp) rest
  in
  loop "" dirs

let program_exp_of_path s =
  let prog, params = split_program_exp s in
  prog ^ path_unquote params

external program_exp_of_program : program -> program_exp = "%identity"

(* Fully Qualified function names *)

type fq = [`FQ] t

let fq_ppp_ocaml = t_ppp_ocaml

external fq_of_string : string -> fq = "%identity"

external string_of_fq : fq -> string = "%identity"

let fq prog func =
  string_of_program_exp prog ^"/"^ func

let fq_print oc = String.print oc % string_of_fq
