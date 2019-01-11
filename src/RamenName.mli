(* As many objects have names, too many different things end up being
 * indistinguishable strings. But we'd like the compiler to help us not
 * mix up program names with function names, etc. Therefore this module
 * using phantom types to help build different but costless string types *)

type 'a t
val t_ppp_ocaml : 'a t PPP.t
val t_ppp_json : 'a t PPP.t

type field = [`Field] t
val field_ppp_ocaml : field PPP.t
val field_ppp_json : field PPP.t
val field_of_string : string -> field
val field_print : 'a BatInnerIO.output -> field -> unit
val string_of_field : field -> string
val field_color : field -> string
val is_private : field -> bool
val is_virtual : field -> bool

type func = [`Function] t
val func_ppp_ocaml : func PPP.t
val func_of_string : string -> func
val func_print : 'a BatInnerIO.output -> func -> unit
val string_of_func : func -> string
val func_color : func -> string

type program = [`Program] t
val program_ppp_ocaml : program PPP.t
val program_of_string : string -> program
val string_of_program : program -> string
val program_color : program -> string
val program_print : 'a BatInnerIO.output -> program -> unit

type rel_program = [`RelProgram] t
val rel_program_ppp_ocaml : rel_program PPP.t
val rel_program_of_string : string -> rel_program
val string_of_rel_program : rel_program -> string
val program_of_rel_program : program -> rel_program -> program
val rel_program_print : 'a BatInnerIO.output -> rel_program -> unit
val rel_program_color : rel_program -> string

(* We also need param expansion as strings, since that's what the user
 * gives us and what we use to identify an instance of a program: *)
type param = string * RamenTypes.value
val param_ppp_ocaml : param PPP.t

type params = (field, RamenTypes.value) Hashtbl.t
val params_ppp_ocaml : params PPP.t
val string_of_params : params -> string
val signature_of_params : params -> string

val path_of_program : program -> string

(* For logs, not paths! *)
type fq = [`FQ] t
val fq_ppp_ocaml : fq PPP.t
val fq_of_string : string -> fq
val string_of_fq : fq -> string
val fq : program -> func -> fq
val fq_print : 'a BatInnerIO.output -> fq -> unit
val fq_parse : ?default_program:program -> fq -> program * func
val fq_color : fq -> string

(* Base units for composing values units.
 * For dimensional analysis to work, all defined base units must be independent
 * (not reducible to others) *)
type base_unit = [`BaseUnit] t
val base_unit_ppp_ocaml : base_unit PPP.t
val base_unit_of_string : string -> base_unit
val string_of_base_unit : base_unit -> string
val base_unit_print : 'a BatInnerIO.output -> base_unit -> unit

(* Compare two strings together as long as they are of the same (phantom)
 * type: *)
val compare :
  ([< `Field|`Function|`Program|`RelProgram|`FQ|`BaseUnit|`Url] as 'a) t ->
  'a t -> int

(* Misc: *)
val expr_color : string -> string

(* TODO: bin names, notif names... *)
