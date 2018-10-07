(* Version Numbers used to identify anything that's saved on disc or
 * transferred via network. *)

(* Release tag just for information purpose but not actually used to version
 * anything. *)
let release_tag = "v3.0.12"

(* Format of the list of running programs: *)
let graph_config = "v14" (* last: per worker report_period *)

(* Code generation: sources, binaries, marshaled types... *)
let codegen = "v39" (* last: remove default program name from binary *)

(* Instrumentation data sent from workers to Ramen *)
let instrumentation_tuple = "v5" (* last: addition of startup time *)

(* Notifications sent from workers to Ramen *)
let notify_tuple = "v6" (* last: firing is now optional *)

(* Pending notification format (marshalled on disc) *)
let pending_notify = "v5" (* last: use PPP *)

(* Ringbuf formats *)
let ringbuf = "v6" (* last: Addition of version field *)

(* Ref-ringbuf format *)
let out_ref = "v3" (* last: use PPP *)

(* Workers state format *)
let worker_state = "v10" (* last: remove dead state member *)

(* Format of the binocle save files *)
let binocle = Binocle.version
