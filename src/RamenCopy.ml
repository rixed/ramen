module N = RamenName

(*
 * Message types used by the copy service.
 *)

(* This is supposed to be the first message sent by the client. *)
type set_target_msg = N.fq * int

(* Append the given tuples to the proper ringbuffer
 * (once target has been set): *)
type append_msg = Bytes.t
