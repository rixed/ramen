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

  type csv =
    int64 * int64 * int option * int option * int option * int option *
    PPP.uint32 option * Int128.t option * PPP.uint32 option * Int128.t option *
    int * int * int * int * int * int [@@ppp PPP_CSV]

  (*$= t_ppp & ~printer:(function None -> "None" | Some (e, o) -> Printf.sprintf "Some %s, len %d" (PPP.to_string t_ppp e) o)
    (Some ((1L, 2L, Some 3, Some 4, Some 5, Some 6, Some 7l, \
            None, Some 9l, None, 11, 12, 13, 14, 15, 16 ), 36)) \
      (PPP.of_string t_ppp "1|2|3|4|5|6|7||9||11|12|13|14|15|16\n" 0)
   *)
  (*$>*)

  (* Convert a CSV value into our nicer structure: *)
  let of_csv (start, stop, itf_clt, itf_srv, zone_clt, zone_srv,
              ip4_clt_opt, ip6_clt_opt, ip4_srv_opt, ip6_srv_opt,
              port_clt, port_srv, bytes_clt, bytes_srv,
              packets_clt, packets_srv) =
    let to_usec x = 0.000_001 *. Int64.to_float x in
    let start = to_usec start and stop = to_usec stop in
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
      bytes_clt ; bytes_srv }

  let to_csv _ = failwith "No reason to do that"

  (*$= of_csv & ~printer:(function None -> "None" | Some (e, o) -> Printf.sprintf "Some %s, len %d" (PPP.to_string t_ppp e) o)
    (Some ({ start = 0.000001 ; stop = 0.000002 ; itf = Split (3,4) ; \
             zone_clt = Some 5 ; zone_srv = Some 6 ; \
             socket = Some { endpoints = IPv4s (7l, 9l) ; ports = 11, 12 } ; \
             bytes_clt = 13 ; bytes_srv = 14 ; packets_clt = 15 ; packets_srv = 16 ; \
             max_missed = 0 }, 36)) \
      (PPP.of_string ppp_csv "1|2|3|4|5|6|7||9||11|12|13|14|15|16\n" 0)
   *)
  (*$>*)
end
