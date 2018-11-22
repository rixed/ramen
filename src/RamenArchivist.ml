(* A service of its own, the archivist job is to monitor everything
 * that's running and, guided by some user configuration, to find out
 * which function should be asked to archive its history and for all
 * long (this being used by the GC eventually). *)

(* We want to serialize globs as strings: *)
type glob = Globs.pattern
let glob_ppp_ocaml = PPP.OCaml.string

type user_config = per_node_config array [@@ppp PPP_OCaml]

and per_node_config =
  { (* Functions which FQ match [pattern] will be collectively allowed to
       use [size] bytes of storage. If nothing match then no size
       restriction applies. *)
    pattern : glob [@@ppp_default = ""] ;
    size : int }

