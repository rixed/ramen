(* Part of RamenNetflow requiring external libs: *)
open Batteries
open RamenLog
open RamenHelpers
open RamenNullable
open RamenNetflow

external decode :
  Bytes.t -> int -> RamenIp.t nullable -> netflow_metric array =
  "wrap_netflow_v5_decode"

let collector ~inet_addr ~port ?while_ k =
  (* Listen to incoming UDP datagrams on given port: *)
  let serve ?sender buffer recv_len =
    let sender = Option.map RamenIp.of_unix_addr sender in
    !logger.debug "Received %d bytes from netflow source @ %a"
      recv_len
      (Option.print RamenIp.print) sender ;
    decode buffer recv_len (nullable_of_option sender) |>
    Array.iter k
  in
  udp_server
    ~what:"netflow sink" ~inet_addr ~port ?while_ serve

let test ?(port=2055) () =
  init_logger Normal ;
  let display_tuple _t = () in
  collector ~inet_addr:Unix.inet_addr_any ~port display_tuple
