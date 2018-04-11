(*
 * Notifications:
 * To alleviate workers from the hassle to send HTTP notifications, those are
 * sent to Ramen via a ringbuffer. Advantages are many:
 * Workers do not need an HTTP client and are therefore smaller, faster to
 * link, and easier to port to another language. Also, other notification
 * mechanisms are easier to implement in a single location.
 *
 * TODO: quarantine workers when command fails?
 *)
open Batteries
open Lwt
open RamenLog
open RamenHelpers

let start conf rb =
  RamenSerialization.read_notifs rb (fun (worker, cmd) ->
    !logger.info "Received execute instruction from %s: %s"
      worker cmd ;
    match%lwt run ~timeout:5. [| "/bin/sh"; "-c"; cmd |] with
    | exception e ->
        !logger.error "While executing command %S from %s: %s"
          cmd worker
          (Printexc.to_string e) ;
        return_unit
    | stdout, stderr ->
        if stdout <> "" then !logger.debug "cmd: %s" stdout ;
        if stderr <> "" then !logger.error "cmd: %s" stderr ;
        return_unit)
