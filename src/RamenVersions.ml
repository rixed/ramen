(* Version Numbers used to identify anything that's saved on disc or
 * transferred via network. *)

(* Release tag just for information purpose but not actually used to version
 * anything. *)
let release_tag = "v2.2.3"

(* Format of the list of running programs: *)
let graph_config = "v10" (* last: params in RC key *)

(* Code generation: sources, binaries, marshaled types... *)
let codegen = "v24" (* last: changed input_ringbuf transmission *)

(* Instrumentation data sent from workers to Ramen *)
let instrumentation_tuple = "v2" (* last: addition of last_out *)

(* Notifications sent from workers to Ramen *)
let notify_tuple = "v3" (* last: removed non-generic notifications *)

(* Pending notification format (marshalled on disc) *)
let pending_notify = "v1"

(* Ringbuf formats *)
let ringbuf = "v3" (* last: merge seq and time archives *)

(* Ref-ringbuf format *)
let out_ref = "v2" (* last: added timeout *)

(* Workers state format *)
let worker_state = "v6" (* last: not sure why *)
