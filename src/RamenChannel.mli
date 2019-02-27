type t = private int
val t_ppp_ocaml : t PPP.t

val live : t
val print : 'a BatInnerIO.output -> t -> unit
val make : unit -> t
val of_string : string -> t
val to_string : t -> string
val of_int : int -> t
