(* Version Numbers used to identify anything that's saved on disc or
 * transferred via network. *)

(* Release tag just for information purpose but not actually used to version
 * anything. *)
let release_tag = "v3.0.7"

(* Format of the list of running programs: *)
let graph_config = "v11" (* last: params is now a hash *)

(* Code generation: sources, binaries, marshaled types... *)
let codegen = "v37" (* last: commit before *)

(* Instrumentation data sent from workers to Ramen *)
let instrumentation_tuple = "v3" (* last: addition of max event time *)

(* Notifications sent from workers to Ramen *)
let notify_tuple = "v6" (* last: firing is now optional *)

(* Pending notification format (marshalled on disc) *)
let pending_notify = "v4" (* last: max FPR *)

(* Ringbuf formats *)
let ringbuf = "v5" (* last: Cache-aware alignment *)

(* Ref-ringbuf format *)
let out_ref = "v3" (* last: use PPP *)

(* Workers state format *)
let worker_state = "v10" (* last: remove dead state member *)
