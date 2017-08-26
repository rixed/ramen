open Batteries
open Log
module N = RamenSharedTypes.Node

type options = { debug : bool ; monitor : bool }

let options debug monitor = { debug ; monitor }

let rebase dataset_name name = dataset_name ^"/"^ name

(* Get the operations to import the dataset and do basic transformations.
 * Named streams belonging to this base layer:
 * - ${dataset_name}/csv: Raw imported tuples straight from the CSV;
 * - ${dataset_name}/c2s: The first of a pair of streams of traffic info
 *                        (source to dest rather than client to server);
 * - ${dataset_name}/s2c: The second stream of traffic info;
 *
 * If you wish to process traffic info you must feed on both c2s and s2c.
 *)
let base_layer dataset_name delete csv_dir =
  let make_node ?(parents=[]) name operation =
    let parents = List.map (rebase dataset_name) parents in
    N.{ name ; operation ; parents }
  in
  let csv =
    let op =
      Printf.sprintf
        "READ%s CSV FILES \"%s/tcp_v29.*.csv.gz\"\n    \
                SEPARATOR \"\\t\" NULL \"<NULL>\"\n    \
                PREPROCESS WITH \"zcat\" (\n  \
           poller string not null,\n  \
           capture_begin u64 not null,\n  \
           capture_end u64 not null,\n  \
           device_client u8 null,\n  \
           device_server u8 null,\n  \
           vlan_client u32 null,\n  \
           vlan_server u32 null,\n  \
           mac_client u64 null,\n  \
           mac_server u64 null,\n  \
           zone_client u32 not null,\n  \
           zone_server u32 not null,\n  \
           ip4_client u32 null,\n  \
           ip6_client i128 null,\n  \
           ip4_server u32 null,\n  \
           ip6_server i128 null,\n  \
           ip4_external u32 null,\n  \
           ip6_external i128 null,\n  \
           port_client u16 not null,\n  \
           port_server u16 not null,\n  \
           diffserv_client u8 not null,\n  \
           diffserv_server u8 not null,\n  \
           os_client u8 null,\n  \
           os_server u8 null,\n  \
           mtu_client u16 null,\n  \
           mtu_server u16 null,\n  \
           captured_pcap string null,\n  \
           application u32 not null,\n  \
           protostack string null,\n  \
           uuid string null,\n  \
           traffic_bytes_client u64 not null,\n  \
           traffic_bytes_server u64 not null,\n  \
           traffic_packets_client u64 not null,\n  \
           traffic_packets_server u64 not null,\n  \
           payload_bytes_client u64 not null,\n  \
           payload_bytes_server u64 not null,\n  \
           payload_packets_client u64 not null,\n  \
           payload_packets_server u64 not null,\n  \
           retrans_traffic_bytes_client u64 null,\n  \
           retrans_traffic_bytes_server u64 null,\n  \
           retrans_payload_bytes_client u64 null,\n  \
           retrans_payload_bytes_server u64 null,\n  \
           syn_count_client u64 null,\n  \
           fin_count_client u64 null,\n  \
           fin_count_server u64 null,\n  \
           rst_count_client u64 null,\n  \
           rst_count_server u64 null,\n  \
           timeout_count u64 not null,\n  \
           close_count u64 null,\n  \
           dupack_count_client u64 null,\n  \
           dupack_count_server u64 null,\n  \
           zero_window_count_client u64 null,\n  \
           zero_window_count_server u64 null,\n  \
           ct_count u64 null,\n  \
           ct_sum u64 not null,\n  \
           ct_square_sum u64 not null,\n  \
           rt_count_server u64 null,\n  \
           rt_sum_server u64 not null,\n  \
           rt_square_sum_server u64 not null,\n  \
           rtt_count_client u64 null,\n  \
           rtt_sum_client u64 not null,\n  \
           rtt_square_sum_client u64 not null,\n  \
           rtt_count_server u64 null,\n  \
           rtt_sum_server u64 not null,\n  \
           rtt_square_sum_server u64 not null,\n  \
           rd_count_client u64 null,\n  \
           rd_sum_client u64 not null,\n  \
           rd_square_sum_client u64 not null,\n  \
           rd_count_server u64 null,\n  \
           rd_sum_server u64 not null,\n  \
           rd_square_sum_server u64 not null,\n  \
           dtt_count_client u64 null,\n  \
           dtt_sum_client u64 not null,\n  \
           dtt_square_sum_client u64 not null,\n  \
           dtt_count_server u64 null,\n  \
           dtt_sum_server u64 not null,\n  \
           dtt_square_sum_server u64 not null,\n  \
           dcerpc_uuid string null\n\
         )"
      (if delete then " AND DELETE" else "") csv_dir in
    make_node "csv" op in
  let to_unidir ~src ~dst name =
    let cs_fields = [
         "device", "" ; "vlan", "" ; "mac", "" ; "zone", "" ; "ip4", "" ;
         "ip6", "" ; "port", "" ; "diffserv", "" ; "os", "" ; "mtu", "" ;
         "traffic_packets", "packets" ; "traffic_bytes", "bytes" ;
         "payload_bytes", "payload" ;
         "payload_packets", "packets_with_payload" ;
         "retrans_traffic_bytes", "retrans_bytes" ;
         "retrans_payload_bytes", "retrans_payload" ;
         "fin_count", "fins" ; "rst_count", "rsts" ;
         "dupack_count", "dupacks" ; "zero_window_count", "zero_windows" ;
         "rtt_count", "" ; "rtt_sum", "" ; "rtt_square_sum", "rtt_sum2" ;
         "rd_count", "" ; "rd_sum", "" ; "rd_square_sum", "rd_sum2" ;
         "dtt_count", "" ; "dtt_sum", "" ; "dtt_square_sum", "dtt_sum2" ] |>
      List.fold_left (fun s (field, alias) ->
        let alias = if alias <> "" then alias else field in
        s ^"  "^ field ^"_"^ src ^" AS "^ alias ^"_src, "
               ^ field ^"_"^ dst ^" AS "^ alias ^"_dst,\n") ""
    in
    let op =
      "SELECT\n  \
         poller, capture_begin, capture_end,\n  \
         ip4_external, ip6_external,\n  \
         captured_pcap, application, protostack, uuid,\n  \
         timeout_count AS timeouts, close_count AS closes,\n  \
         ct_count AS connections, ct_sum AS connections_time,\n  \
         ct_square_sum AS connections_time2, syn_count_client AS syns,\n"^
         cs_fields ^
      "  dcerpc_uuid\n\
       WHERE traffic_packets_"^ src ^" > 0" in
    make_node ~parents:["csv"] name op in
  let traffic name dt =
    let dt_us = dt * 1_000_000 in
    let op =
      Printf.sprintf
        "SELECT\n  \
           (capture_begin // %d) AS start,\n  \
           min of capture_begin, max of capture_end,\n  \
           sum of packets_src / %d AS packets_per_secs,\n  \
           sum of bytes_src / %d AS bytes_per_secs\n\
         EXPORT EVENT STARTING AT start * %d\n         \
                 WITH DURATION %d\n\
         GROUP BY capture_begin // %d\n\
         COMMIT AND FLUSH WHEN\n  \
           in.capture_begin > out.min_capture_begin + 2 * %d"
        dt_us dt dt dt dt dt_us dt_us
        (* Note: Ideally we would want to compute the max of all.capture_begin *)
    in
    make_node ~parents:["c2s"; "s2c"] name op
  in
  RamenSharedTypes.{
    name = dataset_name ;
    nodes = [
      csv ;
      to_unidir ~src:"client" ~dst:"server" "c2s" ;
      to_unidir ~src:"server" ~dst:"client" "s2c" ;
      traffic "minutely traffic" 60 ;
      traffic "hourly traffic" 3600 ;
      traffic "daily traffic" (3600 * 24) ] }

(* Build the node infos corresponding to the BCN configuration *)
let layer_of_bcns bcns dataset_name =
  let layer_name = rebase dataset_name "BCN" in
  let name_of_zones = function
    | [] -> "any"
    | main::_ -> string_of_int main in
  let all_nodes = ref [] in
  let make_node ?(parents=[]) name operation =
    let parents = List.map (rebase dataset_name) parents in
    let node = N.{ name ; operation ; parents } in
    all_nodes := node :: !all_nodes
  in
  let alert_conf_of_bcn bcn =
    (* bcn.min_bps, bcn.max_bps, bcn.obs_window, bcn.avg_window, bcn.percentile, bcn.source bcn.dest *)
    let open Conf_of_sqlite in
    let name_prefix = Printf.sprintf "%s to %s"
      (name_of_zones bcn.source) (name_of_zones bcn.dest) in
    let avg_window = int_of_float (bcn.avg_window *. 1_000_000.0) in
    let avg_per_zones_name =
      Printf.sprintf "%s: avg traffic every %gs"
        name_prefix bcn.avg_window in
    let in_zone what_zone = function
      | [] -> "true"
      | lst ->
        "("^ List.fold_left (fun s z ->
          s ^ (if s <> "" then " OR " else "") ^
          what_zone ^ " = " ^ string_of_int z) "" lst ^")" in
    let op =
      Printf.sprintf
        "SELECT\n  \
           (capture_begin // %d) AS start,\n  \
           min of capture_begin, max of capture_end,\n  \
           sum of packets_src / %g AS packets_per_secs,\n  \
           sum of bytes_src / %g AS bytes_per_secs,\n  \
           %S AS zone_src, %S AS zone_dst\n\
         WHERE %s AND %s\n\
         EXPORT EVENT STARTING AT start * %g\n         \
                 WITH DURATION %g\n\
         GROUP BY capture_begin // %d\n\
         COMMIT AND FLUSH WHEN in.capture_begin > out.min_capture_begin + 2 * %d"
        avg_window
        bcn.avg_window bcn.avg_window
        (name_of_zones bcn.source)
        (name_of_zones bcn.dest)
        (in_zone "zone_src" bcn.source)
        (in_zone "zone_dst" bcn.dest)
        bcn.avg_window bcn.avg_window
        avg_window
        avg_window
        (* Note: Ideally we would want to compute the max of all.capture_begin *)
    in
    make_node ~parents:["c2s"; "s2c"] avg_per_zones_name op ;
    let perc_per_obs_window_name =
      Printf.sprintf "%s: %gth perc on last %gs"
        name_prefix bcn.percentile bcn.obs_window in
    let op =
      let nb_items_per_groups =
        Helpers.round_to_int (bcn.obs_window /. bcn.avg_window) in
      (* Note: The event start at the end of the observation window and lasts
       * for one avg window! *)
      Printf.sprintf
        "SELECT\n  \
           group.#count AS group_count,\n  \
           min start, max start,\n  \
           min of min_capture_begin AS min_capture_begin,\n  \
           max of max_capture_end AS max_capture_end,\n  \
           %gth percentile of bytes_per_secs AS bytes_per_secs,\n  \
           zone_src, zone_dst\n\
         EXPORT EVENT STARTING AT max_capture_end * 0.000001\n        \
                 WITH DURATION %g\n\
         COMMIT AND SLIDE 1 WHEN\n  \
           group.#count >= %d\n  OR \
           in.start > out.max_start + 5"
         bcn.percentile bcn.avg_window nb_items_per_groups in
    make_node ~parents:["BCN/"^ avg_per_zones_name] perc_per_obs_window_name op ;
    let enc = Uri.pct_encode in
    (* TODO: we need an hysteresis here! *)
    Option.may (fun min_bps ->
        let subject = Printf.sprintf "Too little traffic from zone %s to %s"
                        (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = Printf.sprintf
                     "The traffic from zone %s to %s has sunk below \
                      the configured minimum of %d \
                      for the last %g minutes.\n"
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      min_bps (bcn.obs_window /. 60.) in
        let ops = Printf.sprintf
          "WHEN bytes_per_secs < %d\n  \
           NOTIFY \"http://localhost:876/notify?name=Low%%20traffic&firing=1&subject=%s&text=%s\""
            min_bps
            (enc subject) (enc text) in
        let name = Printf.sprintf "%s: alert traffic too low" name_prefix in
        make_node ~parents:["BCN"^ perc_per_obs_window_name] name ops
      ) bcn.min_bps ;
    Option.may (fun max_bps ->
        let subject = Printf.sprintf "Too much traffic from zone %s to %s"
                        (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = Printf.sprintf
                     "The traffic from zones %s to %s has raised above \
                      the configured maximum of %d \
                      for the last %g minutes.\n"
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      max_bps (bcn.obs_window /. 60.) in
        let ops = Printf.sprintf
          "WHEN bytes_per_secs > %d\n  \
           NOTIFY \"http://localhost:876/notify?name=High%%20traffic&firing=1&subject=%s&text=%s\""
            max_bps
            (enc subject) (enc text) in
        let name = Printf.sprintf "%s: alert traffic too high" name_prefix in
        make_node ~parents:["BCN/"^ perc_per_obs_window_name] name ops
      ) bcn.max_bps
  in
  List.iter alert_conf_of_bcn bcns ;
  RamenSharedTypes.{ name = layer_name ; nodes = !all_nodes }

let get_bcns_from_db db =
  let open Conf_of_sqlite in
  get_config db

(* Daemon *)

open Lwt
open Cohttp
open Cohttp_lwt_unix

let put_layer ramen_url layer =
  let req = PPP.to_string RamenSharedTypes.put_layer_req_ppp layer in
  let url = ramen_url ^"/graph/" in
  !logger.debug "Will send %S to %S" req url ;
  let body = `String req in
  (* TODO: but also fix the server never timeouting! *)
  let headers = Header.init_with "Connection" "close" in
  let%lwt (resp, body) =
    Helpers.retry ~on:(fun _ -> true) ~min_delay:1.
      (Client.put ~headers ~body) (Uri.of_string url) in
  let code = resp |> Response.status |> Code.code_of_status in
  !logger.debug "Response code: %d" code ;
  let%lwt body = Cohttp_lwt_body.to_string body in
  !logger.debug "Body: %S\n" body ;
  return_unit

let start conf ramen_url db_name dataset_name delete csv_dir =
  logger := make_logger conf.debug ;
  let open Conf_of_sqlite in
  let db = get_db db_name in
  let update () =
    (* TODO: The base layer for this client *)
    let base = base_layer dataset_name delete csv_dir in
    let%lwt () = put_layer ramen_url base in
    (* TODO: A layer per BCN? Pro: easier to update and set from cmdline
     * without a DB. Cons: Easier to remove/add all at once; more manual labor
     * if we do have a DB *)
    let bcns = get_bcns_from_db db in
    let bcns = layer_of_bcns bcns dataset_name in
    put_layer ramen_url bcns
  in
  let%lwt () = update () in
  if conf.monitor then
    while%lwt true do
      check_config_changed db ;
      if must_reload db then (
        !logger.info "Must reload configuration" ;
        update ()
      ) else (
        Lwt_unix.sleep 1.0
      )
    done
  else return_unit

(* Args *)

open Cmdliner

let common_opts =
  let debug =
    Arg.(value (flag (info ~doc:"increase verbosity" ["d"; "debug"])))
  and monitor =
    Arg.(value (flag (info ~doc:"keep running and update conf when DB changes"
                           ["m"; "monitor"])))
  in
  Term.(const options $ debug $ monitor)

let ramen_url =
  let i = Arg.info ~doc:"URL to reach ramen" [ "ramen-url" ] in
  Arg.(value (opt string "http://127.0.0.1:29380" i))

let db_name =
  let i = Arg.info ~doc:"Path of the SQLite file"
                   [ "db" ] in
  Arg.(required (opt (some string) None i))

let dataset_name =
  let i = Arg.info ~doc:"Name identifying this data set"
                        [ "name" ] in
  Arg.(value (opt string "" i))

let delete_opt =
  let i = Arg.info ~doc:"Delete CSV files once injected"
                   [ "delete" ] in
  Arg.(value (flag i))

let csv_dir =
  let i = Arg.info ~doc:"Path where the CSV files are stored"
                   [ "csv-dir" ] in
  Arg.(required (opt (some string) None i))

let start_cmd =
  Term.(
    (const start
      $ common_opts
      $ ramen_url
      $ db_name
      $ dataset_name
      $ delete_opt
      $ csv_dir),
    info "ramen_configurator")

let () =
  match Term.eval start_cmd with
  | `Ok th -> Lwt_main.run th
  | x -> Term.exit x
