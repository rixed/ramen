(* All events should have a proper type to make field access faster and detect
 * any typing bug when the configuration is compiled.  Also helps to make
 * events as sophisticated as we need/like.
 *
 * This is rather ugly due to the heavy "ppp" things, which are
 * pretty-printer and parsers. This should come as a compiler plugin and
 * disappear from here at some point.
 *)

type time = float
let time_ppp = PPP_OCaml.float

type ip_port = int
let ip_port_ppp = PPP_OCaml.int

(* We put the server first *)
type ip_endpoints = IPv4s of (Int32.t * Int32.t)
                  | IPv6s of (Int128.t * Int128.t)
let ip_endpoints_ppp = PPP_OCaml.(union (
  variant "IPv4" (pair int32 int32) |||
  variant "IPv6" (pair Int128.ppp Int128.ppp)) >>:
  ((function IPv4s (i1, i2) -> Some (i1, i2), None
           | IPv6s (i1, i2) -> None, Some (i1, i2)),
   (function Some (i1, i2), _ -> IPv4s (i1, i2)
           | _, Some (i1, i2) -> IPv6s (i1, i2)
           | _ -> assert false)))

type socket = {
  (* client * server *)
  endpoints : ip_endpoints ;
  ports : ip_port * ip_port
}
let socket_ppp = PPP_OCaml.(record (
  field "endpoints" ip_endpoints_ppp <->
  field "ports" (pair ip_port_ppp ip_port_ppp)) >>:
  ((fun { endpoints ; ports } -> Some endpoints, Some ports),
   (function Some endpoints, Some ports -> { endpoints ; ports }
           | _ -> raise PPP.MissingRequiredField)))

module TCP_v29 =
struct
  (*$< TCP_v29 *)

  (* This is weird and should go away! *)
  type itf = CltOnly of int
           | SrvOnly of int
           | Same of int
           | Split of int * int

  let itf_ppp = PPP_OCaml.(union (
    variant "Clt" int |||
    variant "Srv" int |||
    variant "Same" int |||
    variant "Split" (pair int int)) >>:
    ((function CltOnly i -> Some (Some (Some i, None), None), None
             | SrvOnly i -> Some (Some (None, Some i), None), None
             | Same i -> Some (None, Some i), None
             | Split (i, j) -> None, Some (i, j)),
     (function Some (Some (Some i, None), None), None -> CltOnly i
             | Some (Some (None, Some i), None), None -> SrvOnly i
             | Some (None, Some i), None -> Same i
             | None, Some (i, j) -> Split (i, j)
             | _ -> assert false)))

  type zone = int
  let zone_ppp = PPP_OCaml.int

  type t =
    { mutable start : time ;
      mutable stop : time ;
      itf : itf ;
      zone_clt : zone option ;
      zone_srv : zone option ;
      socket : socket option ; (* unset for "others" *)
      mutable packets_clt : int ;
      mutable packets_srv : int ;
      mutable max_missed : int ; (* max number of uncounted packets *)
      mutable bytes_clt : int ;
      mutable bytes_srv : int }

  let ppp = PPP_OCaml.(record (
    field "start" time_ppp <->
    field "stop" time_ppp <->
    field "itf" itf_ppp <->
    field "zone_clt" (option zone_ppp) <->
    field "zone_srv" (option zone_ppp) <->
    field "socket" (option socket_ppp) <->
    field "bytes_clt" int <->
    field "bytes_srv" int <->
    field "packets_clt" int <->
    field "packets_srv" int <->
    field "max_missed" int) >>:
    ((fun { start ; stop ; itf ; zone_clt ; zone_srv ; socket ; bytes_clt ; bytes_srv ;
            packets_clt ; packets_srv ; max_missed } ->
       Some (Some (Some (Some (Some (Some (Some (Some (Some (Some start, Some stop), Some itf),
       Some zone_clt), Some zone_srv), Some socket), Some bytes_clt), Some bytes_srv),
       Some packets_clt), Some packets_srv), Some max_missed),
     (function
       | (Some (Some (Some (Some (Some (Some (Some (Some (Some (Some start, Some stop), Some itf),
         Some zone_clt), Some zone_srv), Some socket), Some bytes_clt), Some bytes_srv),
         Some packets_clt), Some packets_srv), Some max_missed) ->
         { start ; stop ; itf ; zone_clt ; zone_srv ; socket ;
           packets_clt ; packets_srv ; max_missed ; bytes_clt ; bytes_srv }
       | _ -> raise PPP.MissingRequiredField)))

  (* TODO: Int128.ppp would not parse the input, better not have ipv6! *)
  (* TODO: PPP_CSV that print/scan everything from a one line tuple, so we
   * can reuse the above ppp *)
  let ppp_csv = PPP.(
    let microsecs_ppp = PPP.(int64 >>:
      ((fun t -> t *. 1_000_000. |> Int64.of_float),
       (fun m -> (Int64.to_float m) *. 0.000_001))) in
    let c = cst "|" in
    microsecs_ppp +- c ++ microsecs_ppp +- c ++
    optional int +- c ++ optional int +- c ++
    optional int +- c ++ optional int +- c ++
    optional uint32 +- c ++ optional Int128.ppp +- c ++
    optional uint32 +- c ++ optional Int128.ppp +- c ++
    int +- c ++ int +- c ++
    int +- c ++ int +- c ++ int +- c ++ int +- newline >>:
    ((fun e ->
        let socket = BatOption.get e.socket in
        let itf_clt, itf_srv =
          match e.itf with
          | CltOnly i -> Some i, None
          | SrvOnly i -> None, Some i
          | Same i -> Some i, Some i
          | Split (i, j) -> Some i, Some j in
        let ip4_clt_opt, ip6_clt_opt, ip4_srv_opt, ip6_srv_opt =
          match socket.endpoints with
          | IPv4s (c4, s4) -> Some c4, None, Some s4, None
          | IPv6s (c4, s4) -> None, Some c4, None, Some s4 in
        let port_clt, port_srv = socket.ports in
        (((((((((((((((e.start, e.stop), itf_clt), itf_srv), e.zone_clt), e.zone_srv),
        ip4_clt_opt), ip6_clt_opt), ip4_srv_opt), ip6_srv_opt),
        port_clt), port_srv), e.bytes_clt), e.bytes_srv), e.packets_clt), e.packets_srv)),
     (fun (((((((((((((((start, stop), itf_clt), itf_srv), zone_clt), zone_srv),
          ip4_clt_opt), ip6_clt_opt), ip4_srv_opt), ip6_srv_opt),
          port_clt), port_srv), bytes_clt), bytes_srv),
          packets_clt), packets_srv) ->
        let itf =
          match itf_clt, itf_srv with
          | Some i, None -> CltOnly i
          | None, Some i -> SrvOnly i
          | Some i, Some j -> if i = j then Same i else Split (i, j)
          | None, None -> invalid_arg "device" in
        let endpoints =
          match ip4_clt_opt, ip6_clt_opt, ip4_srv_opt, ip6_srv_opt with
          | Some c4, None, Some s4, None -> IPv4s (c4, s4)
          | None, Some c4, None, Some s4 -> IPv6s (c4, s4)
          | _ -> invalid_arg "IPs" in
        { start ; stop ; itf ;
          zone_clt ; zone_srv ;
          socket = Some { endpoints ; ports = port_clt, port_srv } ;
          packets_clt ; packets_srv ; max_missed = 0 ;
          bytes_clt ; bytes_srv })))
  (*$= ppp_csv & ~printer:(function None -> "None" | Some (e, o) -> Printf.sprintf "Some %s, len %d" (PPP.to_string ppp e) o)
    (Some ({ start = 0.000001 ; stop = 0.000002 ; itf = Split (3,4) ; \
             zone_clt = Some 5 ; zone_srv = Some 6 ; \
             socket = Some { endpoints = IPv4s (7l, 9l) ; ports = 11, 12 } ; \
             bytes_clt = 13 ; bytes_srv = 14 ; packets_clt = 15 ; packets_srv = 16 ; \
             max_missed = 0 }, 36)) \
      (PPP.of_string ppp_csv "1|2|3|4|5|6|7||9||11|12|13|14|15|16\n" 0)
   *)
  (*$>*)
end
