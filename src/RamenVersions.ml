(* Version Numbers used to identify anything that's saved on disc or
 * transferred via network. *)

(* Release tag just for information purpose but not actually used to version
 * anything. *)
let release_tag = "v2.0.0"

(* Ramen graph configuration *)
let graph_config = "v9" (* last: stripped down version *)

(* Code generation: sources, binaries... *)
let codegen = "v7" (* last: no more dedicated yield *)

(* Instrumentation data sent from workers to Ramen *)
let instrumentation_tuple = "v1"

(* Notifications sent from workers to Ramen *)
let notify_tuple = "v1"

(* Ringbuf formats *)
let ringbuf = "v2" (* last: non-wrapping ringbufs *)

(* Ref-ringbuf format *)
let out_ref = "v2" (* last: added timeout *)

(* Workers state format *)
let worker_state = "v3" (* last: stripped down version *)
