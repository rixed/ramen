open Batteries
open RamenLog
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

let make_node name operation = N.{ name ; operation }

let rep sub by str = String.nreplace ~str ~sub ~by

let print_squoted oc = Printf.fprintf oc "'%s'"

let traffic_op ?where dataset_name name dt =
  let dt_us = dt * 1_000_000 in
  let parents =
    List.map (rebase dataset_name) ["c2s"; "s2c"] |>
    IO.to_string (List.print ~first:"" ~last:"" ~sep:","
                    print_squoted) in
  let op =
    {|FROM $PARENTS$ SELECT
       (capture_begin // $DT_US$) AS start,
       min capture_begin, max capture_end,
       sum packets_src / $DT$ AS packets_per_secs,
       sum bytes_src / $DT$ AS bytes_per_secs,
       sum payload_src / $DT$ AS payload_per_secs,
       sum packets_with_payload_src / $DT$ AS packets_with_payload_per_secs,
       sum retrans_bytes_src / $DT$ AS retrans_bytes_per_secs,
       sum retrans_payload_src / $DT$ AS retrans_payload_per_secs,
       sum fins_src / $DT$ AS fins_per_secs,
       sum rsts_src / $DT$ AS rsts_per_secs,
       sum timeouts / $DT$ AS timeouts_per_secs,
       sum syns / $DT$ AS syns_per_secs,
       sum closes / $DT$ AS closes_per_secs,
       sum connections / $DT$ AS connections_per_secs,
       sum dupacks_src / $DT$ AS dupacks_per_secs,
       sum zero_windows_src / $DT$ AS zero_windows_per_secs,
       (sum rtt_sum_src / sum rtt_count_src) / 1e6 AS rtt_avg,
       ((sum rtt_sum2_src - float(sum rtt_sum_src)^2 / sum rtt_count_src) /
           sum rtt_count_src) / 1e12 AS rtt_var,
       (sum rd_sum_src / sum rd_count_src) / 1e6 AS rd_avg,
       ((sum rd_sum2_src - float(sum rd_sum_src)^2 / sum rd_count_src) /
           sum rd_count_src) / 1e12 AS rd_var,
       (sum dtt_sum_src / sum dtt_count_src) / 1e6 AS dtt_avg,
       ((sum dtt_sum2_src - float(sum dtt_sum_src)^2 / sum dtt_count_src) /
           sum dtt_count_src) / 1e12 AS dtt_var,
       (sum connections_time / sum connections) / 1e6 AS connection_time_avg,
       ((sum connections_time2 - float(sum connections_time)^2 / sum connections) /
           sum connections) / 1e12 AS connection_time_var
     EXPORT EVENT STARTING AT start * $DT$
             WITH DURATION $DT$
     GROUP BY capture_begin // $DT_US$
     COMMIT AND FLUSH WHEN
       in.capture_begin > out.min_capture_begin + 2 * u64($DT_US$)|} |>
    rep "$DT$" (string_of_int dt) |>
    rep "$DT_US$" (string_of_int dt_us) |>
    rep "$PARENTS$" parents
  in
  let op =
    match where with None -> op
                   | Some w -> op ^"\nWHERE "^ w in
  make_node name op

let base_layer dataset_name delete csv_dir =
  let csv =
    let op =
      Printf.sprintf {|
        READ%s FILES "%s/tcp_v29.*.csv.lz4"
                 SEPARATOR "\t" NULL "<NULL>"
                 PREPROCESS WITH "lz4 -d -c" (
           poller string not null,
           capture_begin u64 not null,
           capture_end u64 not null,
           device_client u8 null,
           device_server u8 null,
           vlan_client u32 null,
           vlan_server u32 null,
           mac_client u64 null,
           mac_server u64 null,
           zone_client u32 not null,
           zone_server u32 not null,
           ip4_client u32 null,
           ip6_client i128 null,
           ip4_server u32 null,
           ip6_server i128 null,
           ip4_external u32 null,
           ip6_external i128 null,
           port_client u16 not null,
           port_server u16 not null,
           diffserv_client u8 not null,
           diffserv_server u8 not null,
           os_client u8 null,
           os_server u8 null,
           mtu_client u16 null,
           mtu_server u16 null,
           captured_pcap string null,
           application u32 not null,
           protostack string null,
           uuid string null,
           traffic_bytes_client u64 not null,
           traffic_bytes_server u64 not null,
           traffic_packets_client u64 not null,
           traffic_packets_server u64 not null,
           payload_bytes_client u64 not null,
           payload_bytes_server u64 not null,
           payload_packets_client u64 not null,
           payload_packets_server u64 not null,
           retrans_traffic_bytes_client u64 null,
           retrans_traffic_bytes_server u64 null,
           retrans_payload_bytes_client u64 null,
           retrans_payload_bytes_server u64 null,
           syn_count_client u64 null,
           fin_count_client u64 null,
           fin_count_server u64 null,
           rst_count_client u64 null,
           rst_count_server u64 null,
           timeout_count u64 not null,
           close_count u64 null,
           dupack_count_client u64 null,
           dupack_count_server u64 null,
           zero_window_count_client u64 null,
           zero_window_count_server u64 null,
           ct_count u64 null,
           ct_sum u64 not null,
           ct_square_sum u64 not null,
           rt_count_server u64 null,
           rt_sum_server u64 not null,
           rt_square_sum_server u64 not null,
           rtt_count_client u64 null,
           rtt_sum_client u64 not null,
           rtt_square_sum_client u64 not null,
           rtt_count_server u64 null,
           rtt_sum_server u64 not null,
           rtt_square_sum_server u64 not null,
           rd_count_client u64 null,
           rd_sum_client u64 not null,
           rd_square_sum_client u64 not null,
           rd_count_server u64 null,
           rd_sum_server u64 not null,
           rd_square_sum_server u64 not null,
           dtt_count_client u64 null,
           dtt_sum_client u64 not null,
           dtt_square_sum_client u64 not null,
           dtt_count_server u64 null,
           dtt_sum_server u64 not null,
           dtt_square_sum_server u64 not null,
           dcerpc_uuid string null
         )|}
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
      {|FROM '|}^ rebase dataset_name "csv" ^{|' SELECT
         poller, capture_begin, capture_end,
         ip4_external, ip6_external,
         captured_pcap, application, protostack, uuid,
         timeout_count AS timeouts, close_count AS closes,
         ct_count AS connections, ct_sum AS connections_time,
         ct_square_sum AS connections_time2, syn_count_client AS syns,|}^
         cs_fields ^{|
         dcerpc_uuid
       WHERE traffic_packets_|}^ src ^" > 0" in
    make_node name op in
  RamenSharedTypes.{
    name = dataset_name ;
    nodes = [
      csv ;
      to_unidir ~src:"client" ~dst:"server" "c2s" ;
      to_unidir ~src:"server" ~dst:"client" "s2c" ;
      traffic_op dataset_name "minutely traffic" 60 ;
      traffic_op dataset_name "hourly traffic" 3600 ;
      traffic_op dataset_name "daily traffic" (3600 * 24) ] }

(* Build the node infos corresponding to the BCN configuration *)
let layer_of_bcns bcns dataset_name =
  let layer_name = rebase dataset_name "BCN" in
  let name_of_zones = function
    | [] -> "any"
    | main::_ -> string_of_int main in
  let all_nodes = ref [] in
  let make_node name operation =
    let node = make_node name operation in
    all_nodes := node :: !all_nodes
  in
  let conf_of_bcn bcn =
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
    let where =
      (in_zone "zone_src" bcn.source) ^" AND "^
      (in_zone "zone_dst" bcn.dest) in
    (* FIXME: this operation is exactly like minutely, except that:
     * - it add zone_src and zone_dst names, which can be useful indeed
     * - it works for whatever avg_window not necessarily minutely.
     * All in all a waste of resources. We could add custom fields to
     * traffic_op and force a minutely averaging window for the alerts. *)
    let op =
      Printf.sprintf
        {|FROM '%s', '%s' SELECT
            (capture_begin // %d) AS start,
            min capture_begin, max capture_end,
            sum packets_src / %g AS packets_per_secs,
            sum bytes_src / %g AS bytes_per_secs,
            %S AS zone_src, %S AS zone_dst
          WHERE %s
          EXPORT EVENT STARTING AT start * %g
                 WITH DURATION %g
          GROUP BY capture_begin // %d
          COMMIT AND FLUSH WHEN
            in.capture_begin > out.min_capture_begin + 2 * u64(%d)|}
        (rebase dataset_name "c2s") (rebase dataset_name "s2c")
        avg_window
        bcn.avg_window bcn.avg_window
        (name_of_zones bcn.source)
        (name_of_zones bcn.dest)
        where
        bcn.avg_window bcn.avg_window
        avg_window
        avg_window
        (* Note: Ideally we would want to compute the max of all.capture_begin *)
    in
    make_node avg_per_zones_name op ;
    let perc_per_obs_window_name =
      Printf.sprintf "%s: %gth perc on last %gs"
        name_prefix bcn.percentile bcn.obs_window in
    let op =
      let nb_items_per_groups =
        Helpers.round_to_int (bcn.obs_window /. bcn.avg_window) in
      (* Note: The event start at the end of the observation window and lasts
       * for one avg window! *)
      Printf.sprintf
        {|FROM '%s' SELECT
           group.#count AS group_count,
           min start, max start,
           min min_capture_begin AS min_capture_begin,
           max max_capture_end AS max_capture_end,
           %gth percentile bytes_per_secs AS bytes_per_secs,
           zone_src, zone_dst
         EXPORT EVENT STARTING AT max_capture_end * 0.000001
                 WITH DURATION %g
         COMMIT AND SLIDE 1 WHEN
           group.#count >= %d OR
           in.start > out.max_start + 5|}
         avg_per_zones_name
         bcn.percentile bcn.avg_window nb_items_per_groups in
    make_node perc_per_obs_window_name op ;
    let enc = Uri.pct_encode in
    (* TODO: we need an hysteresis here! *)
    Option.may (fun min_bps ->
        let subject = Printf.sprintf "Too little traffic from zone %s to %s"
                        (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = Printf.sprintf
                     {|The traffic from zone %s to %s has sunk below
                       the configured minimum of %d for the last %g minutes.|}
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      min_bps (bcn.obs_window /. 60.) in
        let ops = Printf.sprintf
          {|WHEN bytes_per_secs < %d
            FROM '%s'
            NOTIFY "http://localhost:876/notify?name=Low%%20traffic&firing=1&subject=%s&text=%s"|}
            min_bps
            perc_per_obs_window_name
            (enc subject) (enc text) in
        let name = Printf.sprintf "%s: alert traffic too low" name_prefix in
        make_node name ops
      ) bcn.min_bps ;
    Option.may (fun max_bps ->
        let subject = Printf.sprintf "Too much traffic from zone %s to %s"
                        (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = Printf.sprintf
                     {|The traffic from zones %s to %s has raised above
                       the configured maximum of %d for the last %g minutes.|}
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      max_bps (bcn.obs_window /. 60.) in
        let ops = Printf.sprintf
          {|WHEN bytes_per_secs > %d
            FROM '%s'
            NOTIFY "http://localhost:876/notify?name=High%%20traffic&firing=1&subject=%s&text=%s"|}
            max_bps
            perc_per_obs_window_name
            (enc subject) (enc text) in
        let name = Printf.sprintf "%s: alert traffic too high" name_prefix in
        make_node name ops
      ) bcn.max_bps ;
    all_nodes :=
      traffic_op ~where dataset_name (name_prefix ^": minutely traffic") 60 ::
      traffic_op ~where dataset_name (name_prefix ^": hourly traffic") 3600 ::
      traffic_op ~where dataset_name (name_prefix ^": daily traffic") (3600 * 24) :: !all_nodes
  in
  List.iter conf_of_bcn bcns ;
  RamenSharedTypes.{ name = layer_name ; nodes = !all_nodes }

let get_bcns_from_db db =
  let open Conf_of_sqlite in
  get_config db

let ddos_layer dataset_name =
  let layer_name = rebase dataset_name "DDoS" in
  let op_new_peers avg_win rem_win =
    let avg_win_us = avg_win * 1_000_000 in
    {|FROM '$CSV$' SELECT
       (capture_begin // $AVG_WIN_US$) AS start,
       min capture_begin, max capture_end,
       sum (
         u32(remember globally (
              capture_begin // 1_000_000, $REM_WIN$,
              (hash (coalesce (ip4_client, ip6_client, 0)) +
               hash (coalesce (ip4_server, ip6_server, 0)))))) /
         $AVG_WIN$
         AS nb_new_peers_per_secs
     GROUP BY capture_begin // $AVG_WIN_US$
     COMMIT AND FLUSH WHEN
       in.capture_begin > out.min_capture_begin + 2 * u64($AVG_WIN_US$)
     EXPORT EVENT STARTING AT start * $AVG_WIN$
                  WITH DURATION $AVG_WIN$|} |>
    rep "$AVG_WIN_US$" (string_of_int avg_win_us) |>
    rep "$AVG_WIN$" (string_of_int avg_win) |>
    rep "$REM_WIN$" (string_of_int rem_win) |>
    rep "$CSV$" (rebase dataset_name "csv") in
  let global_new_peers =
    make_node "new peers" (op_new_peers 60 3600)
  in
  RamenSharedTypes.{
    name = layer_name ;
    nodes = [ global_new_peers ] }

(* Daemon *)

open Lwt
open Cohttp
open Cohttp_lwt_unix

let put_layer ramen_url layer =
  let req = PPP.to_string RamenSharedTypes.put_layer_req_ppp layer in
  let url = ramen_url ^"/graph/" in
  !logger.debug "Will send %s to %S" req url ;
  let body = `String req in
  (* TODO: but also fix the server never timeouting! *)
  let headers = Header.init_with "Connection" "close" in
  let%lwt (resp, body) =
    Helpers.retry ~on:(fun _ -> true) ~min_delay:1.
      (Client.put ~headers ~body) (Uri.of_string url) in
  let code = resp |> Response.status |> Code.code_of_status in
  let%lwt body = Cohttp_lwt_body.to_string body in
  if code <> 200 then (
    !logger.error "Error code %d: %S" code body ;
    exit 1) ;
  !logger.debug "Response code: %d" code ;
  !logger.debug "Body: %s\n" body ;
  return_unit

let start conf ramen_url db_name dataset_name delete csv_dir
          with_base with_bcns with_ddos =
  logger := make_logger conf.debug ;
  let open Conf_of_sqlite in
  let db = get_db db_name in
  let update () =
    (* TODO: The base layer for this client *)
    let%lwt () = if with_base then (
        let base = base_layer dataset_name delete csv_dir in
        put_layer ramen_url base
      ) else return_unit in
    let%lwt () = if with_bcns then (
        (* TODO: A layer per BCN? Pro: easier to update and set from cmdline
         * without a DB. Cons: Easier to remove/add all at once; more manual
         * labor if we do have a DB *)
        let bcns = get_bcns_from_db db in
        let bcns = layer_of_bcns bcns dataset_name in
        put_layer ramen_url bcns
      ) else return_unit in
    if with_ddos then (
      (* Several DDoS detection approaches, regrouped in a "DDoS" layer. *)
      let ddos = ddos_layer dataset_name in
      put_layer ramen_url ddos
    ) else return_unit
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
  let env = Term.env_info "RAMEN_URL" in
  let i = Arg.info ~doc:"URL to reach ramen"
                   ~env [ "ramen-url" ] in
  Arg.(value (opt string "http://127.0.0.1:29380" i))

let db_name =
  let i = Arg.info ~doc:"Path of the SQLite file"
                   [ "db" ] in
  Arg.(required (opt (some string) None i))

let dataset_name =
  let i = Arg.info ~doc:"Name identifying this data set" [] in
  Arg.(required (pos 0 (some string) None i))

let delete_opt =
  let i = Arg.info ~doc:"Delete CSV files once injected"
                   [ "delete" ] in
  Arg.(value (flag i))

let csv_dir =
  let i = Arg.info ~doc:"Path where the CSV files are stored"
                   [ "csv-dir" ] in
  Arg.(required (opt (some string) None i))

let with_base =
  let i = Arg.info ~doc:"Output the base layer with CSV input and first \
                         operations"
                   [ "with-base" ; "base" ] in
  Arg.(value (flag i))

let with_bcns =
  let i = Arg.info ~doc:"Also output the layer with BCN configuration"
                   [ "with-bcns" ; "with-bcn" ; "bcns" ; "bcn" ] in
  Arg.(value (flag i))

let with_ddos =
  let i = Arg.info ~doc:"Also output the layer with DDoS detection"
                   [ "with-ddos" ; "with-dos" ; "ddos" ; "dos" ] in
  Arg.(value (flag i))

let start_cmd =
  Term.(
    (const start
      $ common_opts
      $ ramen_url
      $ db_name
      $ dataset_name
      $ delete_opt
      $ csv_dir
      $ with_base
      $ with_bcns
      $ with_ddos),
    info "ramen_configurator")

let () =
  match Term.eval start_cmd with
  | `Ok th -> Lwt_main.run th
  | x -> Term.exit x
