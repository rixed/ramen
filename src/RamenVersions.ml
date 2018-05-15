(* Version Numbers used to identify anything that's saved on disc or
 * transferred via network. *)

(* Release tag just for information purpose but not actually used to version
 * anything. *)
let release_tag = "v2.0.0"

(* Format of the list of running programs: *)
let graph_config = "v9" (* last: stripped down version *)

(* Code generation: sources, binaries, marshaled types... *)
let codegen = "v12" (* last: addition of generic IP types *)

(* Instrumentation data sent from workers to Ramen *)
let instrumentation_tuple = "v2" (* last: addition of last_out *)

(* Notifications sent from workers to Ramen *)
let notify_tuple = "v2" (* last: new notification type *)

(* Ringbuf formats *)
let ringbuf = "v3" (* last: merge seq and time archives *)

(* Ref-ringbuf format *)
let out_ref = "v2" (* last: added timeout *)

(* Workers state format *)
let worker_state = "v3" (* last: stripped down version *)
