open RamenLog
module N = RamenName

type t =
  { log_level : log_level ;
    state_file : N.path ;
    is_test : bool ;
    site : N.site ;
    mutable zock : [`Dealer ] Zmq.Socket.t option }

let make log_level state_file is_test site =
  { log_level ; state_file ; is_test ; site ; zock = None }
