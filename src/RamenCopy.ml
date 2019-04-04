module N = RamenName

(*
 * Message types used by the copy service.
 *)

(* This is supposed to be the first message sent by the client. *)
(* TODO: versioned variant type *)
type set_target_msg =
  { client_site : N.site ;
    child : N.fq ;
    parent_num : int }

(* Append the given tuples to the proper ringbuffer
 * (once target has been set): *)
 (* TODO: versioned variant type *)
type append_msg = Bytes.t
