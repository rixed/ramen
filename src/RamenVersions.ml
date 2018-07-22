(* Version Numbers used to identify anything that's saved on disc or
 * transferred via network. *)

(* Release tag just for information purpose but not actually used to version
 * anything. *)
let release_tag = "v2.3.0"

(* Format of the list of running programs: *)
let graph_config = "v10" (* last: params in RC key *)

(* Code generation: sources, binaries, marshaled types... *)
let codegen = "v28" (* last: new type TEmpty *)

(* Instrumentation data sent from workers to Ramen *)
let instrumentation_tuple = "v2" (* last: addition of last_out *)

(* Notifications sent from workers to Ramen *)
let notify_tuple = "v4" (* last: parameter values are expressions *)

(* Pending notification format (marshalled on disc) *)
let pending_notify = "v2" (* last: new notification format with event_time etc... *)

(* Ringbuf formats *)
let ringbuf = "v4" (* last: nullable in RamenTypes.t *)

(* Ref-ringbuf format *)
let out_ref = "v3" (* last: use PPP *)

(* Workers state format *)
let worker_state = "v6" (* last: not sure why *)
