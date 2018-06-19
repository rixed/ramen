(* As many objects have names, too many different things end up being
 * indistinguishable strings. But we'd like the compiler to help us not
 * mix up program names with function names, etc. Therefore this module
 * using phantom types to help build different but costless string types *)

type 'a t
val t_ppp_ocaml : 'a t PPP.t

type func = [`Function] t
val func_ppp_ocaml : func PPP.t
val func_of_string : string -> func
val string_of_func : func -> string

type program = [`Program] t
val program_ppp_ocaml : program PPP.t
val program_of_string : string -> program
val string_of_program : program -> string

(* We also need param expansion as strings, since that's what the user
 * gives us and what we use to identify an instance of a program: *)
type param = string * RamenTypes.value
val param_ppp_ocaml : param PPP.t
type params = param list
val params_ppp_ocaml : params PPP.t
val params_sort : params -> params
val string_of_params : params -> string
val params_signature : params -> string
(* A string formatted like "{name1=val1,name2=val,...}": *)
type params_exp = [`Params] t
val params_exp_ppp_ocaml : params_exp PPP.t
val params_exp_of_string : string -> params_exp
val string_of_params_exp : params_exp -> string
val params_exp_of_params : params -> params_exp

type program_exp = [`ProgramExp] t
val program_exp_ppp_ocaml : program_exp PPP.t
val string_of_program_exp : program_exp -> string
val program_exp_of_string : string -> program_exp
val path_of_program_exp : program_exp -> string
val program_exp_of_path : string -> program_exp
val program_exp_of_program : program -> program_exp
val split_program_exp : program_exp -> program * params_exp

(* For logs, not paths! *)
type fq = [`FQ] t
val fq_ppp_ocaml : fq PPP.t
val fq_of_string : string -> fq
val string_of_fq : fq -> string
val fq : program_exp -> func -> fq
val fq_print : 'a BatInnerIO.output -> fq -> unit

(* TODO: field names, bin names, notifs... *)
