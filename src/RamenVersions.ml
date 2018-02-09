(* Version Numbers used to identify anything that's saved on disc or
 * transferred via network. *)

(* Release tag just for information purpose but not actually used to version
 * anything. *)
let release_tag = "v1.2.0"

(* Ramen graph configuration *)
let graph_config = "v4"

(* Code generation: sources, binaries... *)
let codegen = "v4"

(* Alerting state *)
let alerting_state = "v1"

(* Instrumentation data sent from workers to Ramen *)
let instrumentation_tuple = "v1"

(* Notifications sent from workers to Ramen *)
let notify_tuple = "v1"

(* History file format *)
let history = "v1"

(* Ringbuf formats *)
let ringbuf = "v1"

(* Ref ringbuf format *)
let out_ref = "v1"

(* Workers state format *)
let worker_state = "v2"
