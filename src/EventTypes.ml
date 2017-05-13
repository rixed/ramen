(* All events should have a proper type to make field access faster and detect
 * any typing bug when the configuration is compiled.  Also helps to make
 * events as sophisticated as we need/like.
 *
 * This is rather ugly due to the heavy "ppp" things, which are
 * pretty-printer and parsers. This should come as a compiler plugin and
 * disappear from here at some point.
 *)

type time = float [@@ppp PPP_OCaml]

type ip_port = int [@@ppp PPP_OCaml]

(* We put the server first *)
type ip_endpoints = IPv4s of (int32 * int32)
                  | IPv6s of (Int128.t * Int128.t) [@@ppp PPP_OCaml]

type socket = {
  (* client * server *)
  endpoints : ip_endpoints ;
  ports : ip_port * ip_port
} [@@ppp PPP_OCaml]

module TCP_v29 =
struct
  (*$< TCP_v29 *)

  (* This is weird and should go away! *)
  type itf = CltOnly of int
           | SrvOnly of int
           | Same of int
           | Split of int * int [@@ppp PPP_OCaml]

  type zone = int [@@ppp PPP_OCaml]

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
      mutable bytes_srv : int } [@@ppp PPP_OCaml]

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
    optional uint32 +- c ++ optional Int128.t_ppp +- c ++
    optional uint32 +- c ++ optional Int128.t_ppp +- c ++
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
