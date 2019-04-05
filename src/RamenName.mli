(* As many objects have names, too many different things end up being
 * indistinguishable strings. But we'd like the compiler to help us not
 * mix up program names with function names, etc. Therefore this module
 * using phantom types to help build different but costless string types *)

type +'a t = private string
val t_ppp_ocaml : 'a t PPP.t
val t_ppp_json : 'a t PPP.t

type field = [`Field] t
val field_ppp_ocaml : field PPP.t
val field_ppp_json : field PPP.t
val field : string -> field
val field_print : 'a BatInnerIO.output -> field -> unit
val field_print_quoted : 'a BatInnerIO.output -> field -> unit
val field_color : field -> string
val is_virtual : field -> bool
val is_private : field -> bool

type func = [`Function] t
val func_ppp_ocaml : func PPP.t
val func : string -> func
val func_print : 'a BatInnerIO.output -> func -> unit
val func_print_quoted : 'a BatInnerIO.output -> func -> unit
val func_color : func -> string

type program = [`Program] t
val program_ppp_ocaml : program PPP.t
val program : string -> program
val program_color : program -> string
val program_print : 'a BatInnerIO.output -> program -> unit
val program_print_quoted : 'a BatInnerIO.output -> program -> unit

type rel_program = [`RelProgram] t
val rel_program_ppp_ocaml : rel_program PPP.t
val rel_program : string -> rel_program
val program_of_rel_program : program -> rel_program -> program
val rel_program_print : 'a BatInnerIO.output -> rel_program -> unit
val rel_program_print_quoted : 'a BatInnerIO.output -> rel_program -> unit
val rel_program_color : rel_program -> string

(* For logs, not paths! *)
type fq = [`FQ] t
val fq_ppp_ocaml : fq PPP.t
val fq : string -> fq
val fq_of_program : program -> func -> fq
val fq_print : 'a BatInnerIO.output -> fq -> unit
val fq_print_quoted : 'a BatInnerIO.output -> fq -> unit
val fq_parse : ?default_program:program -> fq -> program * func
val fq_color : fq -> string

(* Base units for composing values units.
 * For dimensional analysis to work, all defined base units must be independent
 * (not reducible to others) *)
type base_unit = [`BaseUnit] t
val base_unit_ppp_ocaml : base_unit PPP.t
val base_unit : string -> base_unit
val base_unit_print : 'a BatInnerIO.output -> base_unit -> unit
val base_unit_print_quoted : 'a BatInnerIO.output -> base_unit -> unit

(* File names: *)
type path = [`Path] t
val path_ppp_ocaml : path PPP.t
val path : string -> path
val path_print : 'a BatInnerIO.output -> path -> unit
val path_print_quoted : 'a BatInnerIO.output -> path -> unit
val path_cat : path list -> path

val path_of_program : program -> path
val simplified_path : path -> path

(* Host names (or IP as strings): *)
type host = [`Host] t
val host_ppp_ocaml : host PPP.t
val host : string -> host
val host_print : 'a BatInnerIO.output -> host -> unit
val host_print_quoted : 'a BatInnerIO.output -> host -> unit

(* Site names:
 * A site is an instance of Ramen. Various sites can share a single RC
 * and communicate through the tunneld service. *)
type site = [`Site] t
val site_ppp_ocaml : site PPP.t
val site : string -> site
val site_print : 'a BatInnerIO.output -> site -> unit
val site_print_quoted : 'a BatInnerIO.output -> site -> unit

(* Service names:
 * For now the only service is the "tunneld" service, forwarding remote
 * tuples to local workers. *)
type service = [`Service] t
val service_ppp_ocaml : service PPP.t
val service : string -> service
val service_print : 'a BatInnerIO.output -> service -> unit
val service_print_quoted : 'a BatInnerIO.output -> service -> unit

(* Compare two strings together as long as they are of the same (phantom)
 * type: *)
type 'a any =
  [< `Field | `Function | `Program | `RelProgram | `FQ | `BaseUnit | `Url
   | `Path | `Host | `Site | `Service ] as 'a
val compare : ('a any as 'a) t -> 'a t -> int
val eq : ('a any as 'a) t -> 'a t -> bool
val cat : ('a any as 'a) t -> 'a t -> 'a t
val length : 'a t -> int
val is_empty : 'a  t -> bool
val lchop : ?n:int -> ('a any as 'a) t -> 'a t
val starts_with : ('a any as 'a) t -> 'a t -> bool
val sub : ('a any as 'a) t -> int -> int -> 'a t

(* Misc: *)
val expr_color : string -> string

val md5 : string -> string (* used internally but others might want this *)

(* TODO: notif names... *)
