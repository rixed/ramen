open Batteries
open RamenLog
module N = RamenSharedTypes.Node

type options = { debug : bool ; monitor : bool }

let options debug monitor = { debug ; monitor }

let rebase dataset_name name = dataset_name ^"/"^ name

let enc = Uri.pct_encode

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

let traffic_node ?where dataset_name name dt =
  let dt_us = dt * 1_000_000 in
  let parents =
    List.map (rebase dataset_name) ["c2s"; "s2c"] |>
    IO.to_string (List.print ~first:"" ~last:"" ~sep:","
                    print_squoted) in
  let op =
    {|FROM $PARENTS$ SELECT
       (capture_begin // $DT_US$) * $DT$ AS start,
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
       sum connections AS _sum_connections,
       _sum_connections / $DT$ AS connections_per_secs,
       sum dupacks_src / $DT$ AS dupacks_per_secs,
       sum zero_windows_src / $DT$ AS zero_windows_per_secs,
       sum rtt_count_src AS _sum_rtt_count_src,
       sum rtt_sum_src AS _sum_rtt_sum_src,
       (_sum_rtt_sum_src / _sum_rtt_count_src) / 1e6 AS rtt_avg,
       ((sum rtt_sum2_src - float(_sum_rtt_sum_src)^2 / _sum_rtt_count_src) /
           _sum_rtt_count_src) / 1e12 AS rtt_var,
       sum rd_count_src AS _sum_rd_count_src,
       sum rd_sum_src AS _sum_rd_sum_src,
       (_sum_rd_sum_src / _sum_rd_count_src) / 1e6 AS rd_avg,
       ((sum rd_sum2_src - float(_sum_rd_sum_src)^2 / _sum_rd_count_src) /
           _sum_rd_count_src) / 1e12 AS rd_var,
       sum dtt_count_src AS _sum_dtt_count_src,
       sum dtt_sum_src AS _sum_dtt_sum_src,
       (_sum_dtt_sum_src / _sum_dtt_count_src) / 1e6 AS dtt_avg,
       ((sum dtt_sum2_src - float(_sum_dtt_sum_src)^2 / _sum_dtt_count_src) /
           _sum_dtt_count_src) / 1e12 AS dtt_var,
       sum connections_time AS _sum_connections_time,
       (_sum_connections_time / _sum_connections) / 1e6 AS connection_time_avg,
       ((sum connections_time2 - float(_sum_connections_time)^2 / _sum_connections) /
           _sum_connections) / 1e12 AS connection_time_var
     EXPORT EVENT STARTING AT start
             WITH DURATION $DT$
     GROUP BY capture_begin // $DT_US$
     COMMIT WHEN
       in.capture_begin > out.min_capture_begin + 2 * u64($DT_US$)|} |>
    rep "$DT$" (string_of_int dt) |>
    rep "$DT_US$" (string_of_int dt_us) |>
    rep "$PARENTS$" parents
  in
  let op =
    match where with None -> op
                   | Some w -> op ^"\nWHERE "^ w in
  make_node name op

(* Anomaly Detection
 *
 * For any node which output interesting timeseries (interesting = that we hand
 * pick) we will add a node that compute all predictions we can compute for the
 * series. We will notify when x out of y predictions are "off". for perf
 * reasons we want the number of such nodes minimal, but as we have only one
 * possible notify per node we also can't have one node for different unrelated
 * things. A good trade-off is to have one node per BCN/BCA.
 * For each timeseries to predict, we also pass a list of other timeseries that
 * we think are good predictors. *)
let anomaly_detection_nodes avg_window from timeseries =
  assert (timeseries <> []) ;
  let stand_alone_predictors = [ "smooth(" ; "fit(5, " ; "5-ma(" ; "lag(" ]
  and multi_predictors = [ "fit_multi(5, " ] in
  let predictor_name = from ^": predictions" in
  let predictor_node =
    let predictions =
      List.fold_left (fun fields (ts, nullable, preds) ->
          let preds_str = String.concat ", " preds in
          (* Add first the timeseries itself: *)
          let fields = ts :: fields in
          (* Then the predictors: *)
          let i, fields =
            List.fold_left (fun (i, fields) predictor ->
                i+1,
                (Printf.sprintf "%s%s) AS pred_%d_%s" predictor ts i ts) :: fields
              ) (0, fields) stand_alone_predictors in
          let nb_preds, fields =
            if preds <> [] then
              List.fold_left (fun (i, fields) multi_predictor ->
                  i + 1,
                  (Printf.sprintf "%s%s, %s) AS pred_%d_%s" multi_predictor ts preds_str i ts) :: fields
                ) (i, fields) multi_predictors
            else i, fields in
          (* Then the "abnormality" of this timeseries: *)
          let abnormality =
            let rec loop sum i =
              if i >= nb_preds then sum else
              let pred = "pred_"^ string_of_int i ^"_"^ ts in
              let abno = "abs("^ pred ^" - "^ ts ^") /\n     \
                            max(abs "^ pred ^", abs "^ ts ^")" in
              let abno = if nullable then "coalesce("^ abno ^", 0)"
                         else abno in
              loop (abno :: sum) (i + 1) in
            loop [] 0 in
          let abnormality =
            "("^ String.concat " +\n   " abnormality ^") / "^
            string_of_int nb_preds ^" AS abnormality_"^ ts in
          abnormality :: fields
        ) [] timeseries
    in
    let op =
      Printf.sprintf
        "SELECT\n  \
           start,\n  \
           %s\n\
         FROM '%s'\n\
         EXPORT EVENT STARTING AT start\n        \
                 WITH DURATION %f"
        (String.concat ",\n  " (List.rev predictions))
        from
        avg_window in
    make_node predictor_name op in
  let anomaly_node =
    let conditions =
      List.fold_left (fun cond (ts, _nullable, _preds) ->
          ("abnormality_"^ ts ^" > 0.75") :: cond
        ) [] timeseries in
    let condition = String.concat " OR\n     " conditions in
    let title = Printf.sprintf "%s is off" from
    and alert_name = from ^" looks abnormal"
    and text = Printf.sprintf "Some metrics from %s seam to be off." from in
    let op =
      Printf.sprintf
        {|FROM '%s'
          SELECT start, (%s) AS abnormality, 3-ma abnormality >= 2/3 AS firing
          COMMIT WHEN firing != previous.firing
          NOTIFY "http://localhost:29382/notify?name=%s&firing=${firing}&time=${start}&title=%s&text=%s"|}
        predictor_name
        condition
        (enc alert_name) (enc title) (enc text) in
    make_node (from ^": anomalies") op in
  predictor_node, anomaly_node

let base_layer dataset_name delete csv_dir =
  let csv =
    let op =
      Printf.sprintf {|
        READ%s FILES "%s/tcp_v29.*.csv.lz4"
               PREPROCESS WITH "lz4 -d -c"
               SEPARATOR "\t" NULL "<NULL>" (
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
           retrans_traffic_bytes_client u64 not null,
           retrans_traffic_bytes_server u64 not null,
           retrans_payload_bytes_client u64 not null,
           retrans_payload_bytes_server u64 not null,
           syn_count_client u64 not null,
           fin_count_client u64 not null,
           fin_count_server u64 not null,
           rst_count_client u64 not null,
           rst_count_server u64 not null,
           timeout_count u64 not null,
           close_count u64 not null,
           dupack_count_client u64 not null,
           dupack_count_server u64 not null,
           zero_window_count_client u64 not null,
           zero_window_count_server u64 not null,
           ct_count u64 not null,
           ct_sum u64 not null,
           ct_square_sum u64 not null,
           rt_count_server u64 not null,
           rt_sum_server u64 not null,
           rt_square_sum_server u64 not null,
           rtt_count_client u64 not null,
           rtt_sum_client u64 not null,
           rtt_square_sum_client u64 not null,
           rtt_count_server u64 not null,
           rtt_sum_server u64 not null,
           rtt_square_sum_server u64 not null,
           rd_count_client u64 not null,
           rd_sum_client u64 not null,
           rd_square_sum_client u64 not null,
           rd_count_server u64 not null,
           rd_sum_server u64 not null,
           rd_square_sum_server u64 not null,
           dtt_count_client u64 not null,
           dtt_sum_client u64 not null,
           dtt_square_sum_client u64 not null,
           dtt_count_server u64 not null,
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
      traffic_node dataset_name "minutely traffic" 60 ;
      traffic_node dataset_name "hourly traffic" 3600 ;
      traffic_node dataset_name "daily traffic" (3600 * 24) ] }

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
    let open Conf_of_sqlite.BCN in
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
     * - it adds zone_src and zone_dst names, which can be useful indeed
     * - it works for whatever avg_window not necessarily minutely.
     * All in all a waste of resources. We could add custom fields to
     * traffic_node and force a minutely averaging window for the alerts. *)
    let op =
      Printf.sprintf
        {|FROM '%s', '%s' SELECT
            (capture_begin // %d) * %g AS start,
            min capture_begin, max capture_end,
            sum packets_src / %g AS packets_per_secs,
            sum bytes_src / %g AS bytes_per_secs,
            %S AS zone_src, %S AS zone_dst
          WHERE %s
          EXPORT EVENT STARTING AT start
                 WITH DURATION %g
          GROUP BY capture_begin // %d
          COMMIT WHEN
            in.capture_begin > out.min_capture_begin + 2 * u64(%d)|}
        (rebase dataset_name "c2s") (rebase dataset_name "s2c")
        avg_window bcn.avg_window
        bcn.avg_window bcn.avg_window
        (name_of_zones bcn.source)
        (name_of_zones bcn.dest)
        where
        bcn.avg_window
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
    (* TODO: we need an hysteresis here! *)
    Option.may (fun min_bps ->
        let title = Printf.sprintf "Too little traffic from zone %s to %s"
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = Printf.sprintf
                     "The traffic from zone %s to %s has sunk below \
                      the configured minimum of %d for the last %g minutes."
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      min_bps (bcn.obs_window /. 60.) in
        let ops = Printf.sprintf
          {|SELECT max_start, bytes_per_secs > %d AS firing
            FROM '%s'
            COMMIT WHEN firing != previous.firing
            NOTIFY "http://localhost:29382/notify?name=Low%%20traffic&firing=${firing}&time=${max_start}&title=%s&text=%s"|}
            min_bps
            perc_per_obs_window_name
            (enc title) (enc text) in
        let name = Printf.sprintf "%s: alert traffic too low" name_prefix in
        make_node name ops
      ) bcn.min_bps ;
    Option.may (fun max_bps ->
        let title = Printf.sprintf "Too much traffic from zone %s to %s"
                        (name_of_zones bcn.source) (name_of_zones bcn.dest)
        and text = Printf.sprintf
                     "The traffic from zones %s to %s has raised above \
                      the configured maximum of %d for the last %g minutes."
                      (name_of_zones bcn.source) (name_of_zones bcn.dest)
                      max_bps (bcn.obs_window /. 60.) in
        let ops = Printf.sprintf
          {|SELECT max_start, bytes_per_secs > %d AS firing
            FROM '%s'
            COMMIT WHEN firing != previous.firing
            NOTIFY "http://localhost:29382/notify?name=High%%20traffic&firing=${firing}&time=${max_start}&title=%s&text=%s"|}
            max_bps
            perc_per_obs_window_name
            (enc title) (enc text) in
        let name = Printf.sprintf "%s: alert traffic too high" name_prefix in
        make_node name ops
      ) bcn.max_bps ;
    let minutely = traffic_node ~where dataset_name (name_prefix ^": minutely traffic") 60 in
    let pred_node, anom_node =
      anomaly_detection_nodes
        bcn.avg_window
        minutely.N.name
        [ "packets_per_secs", false, [ "bytes_per_secs" ;
                                       "packets_with_payload_per_secs" ] ;
          "bytes_per_secs", false, [ "packets_per_secs" ;
                                     "payload_per_secs" ] ;
          "payload_per_secs", false, [ "bytes_per_secs" ] ;
          "packets_with_payload_per_secs", false, [ "packets_per_secs" ] ;
          "retrans_bytes_per_secs", false, [ "retrans_payload_per_secs" ] ;
          "fins_per_secs", false, [ "packets_per_secs" ] ;
          "rsts_per_secs", false, [ "packets_per_secs" ] ;
          "syns_per_secs", false, [ "packets_per_secs" ] ;
          "connections_per_secs", false, [] ;
          "dupacks_per_secs", false, [] ;
          "zero_windows_per_secs", false, [] ;
          "rtt_avg", false, [] ;
          "rd_avg", false, [] ;
          "dtt_avg", false, [] ;
          "connection_time_avg", false, [] ] in
    all_nodes := pred_node :: anom_node :: minutely :: !all_nodes

  in
  List.iter conf_of_bcn bcns ;
  RamenSharedTypes.{ name = layer_name ; nodes = !all_nodes }

(* Build the node infos corresponding to the BCA configuration *)
let layer_of_bcas bcas dataset_name =
  let layer_name = rebase dataset_name "BCA" in
  let all_nodes = ref [] in
  let make_node name operation =
    let node = make_node name operation in
    all_nodes := node :: !all_nodes
  in
  let conf_of_bca bca =
    let open Conf_of_sqlite.BCA in
    let avg_window_int = int_of_float bca.avg_window in
    let avg_per_app = bca.name in
    let csv = rebase dataset_name "csv" in
    let op =
      {|FROM '$CSV$' SELECT
          -- Key
          (capture_begin * 0.000001 // $AVG_INT$) * $AVG_INT$ AS start,
          -- Traffic
          sum traffic_bytes_client / $AVG$ AS c2s_bytes_per_secs,
          sum traffic_bytes_server / $AVG$ AS s2c_bytes_per_secs,
          sum traffic_packets_client / $AVG$ AS c2s_packets_per_secs,
          sum traffic_packets_server / $AVG$ AS s2c_packets_per_secs,
          -- Retransmissions
          sum retrans_traffic_bytes_client / $AVG$
            AS c2s_retrans_bytes_per_secs,
          sum retrans_traffic_bytes_server / $AVG$
            AS s2c_retrans_bytes_per_secs,
          -- TCP flags
          sum syn_count_client / $AVG$ AS c2s_syns_per_secs,
          sum fin_count_client / $AVG$ AS c2s_fins_per_secs,
          sum fin_count_server / $AVG$ AS s2c_fins_per_secs,
          sum rst_count_client / $AVG$ AS c2s_rsts_per_secs,
          sum rst_count_server / $AVG$ AS s2c_rsts_per_secs,
          sum close_count / $AVG$ AS close_per_secs,
          -- TCP issues
          sum dupack_count_client / $AVG$ AS c2s_dupacks_per_secs,
          sum dupack_count_server / $AVG$ AS s2c_dupacks_per_secs,
          sum zero_window_count_client / $AVG$ AS c2s_0wins_per_secs,
          sum zero_window_count_server / $AVG$ AS s2c_0wins_per_secs,
          -- Connection Time
          sum ct_count AS _sum_ct_count,
          sum ct_sum AS _sum_ct_sum,
          _sum_ct_count / $AVG$ AS ct_per_secs,
          IF _sum_ct_count = 0 THEN 0 ELSE
            (_sum_ct_sum / _sum_ct_count) / 1e6 AS ct_avg,
          IF _sum_ct_count = 0 THEN 0 ELSE
            sqrt (((sum ct_square_sum - (_sum_ct_sum)^2) /
                   _sum_ct_count) / 1e12) AS ct_stddev,
          -- Server Response Time
          sum rt_count_server AS _sum_rt_count_server,
          sum rt_sum_server AS _sum_rt_sum_server,
          _sum_rt_count_server / $AVG$ AS srt_per_secs,
          IF _sum_rt_count_server = 0 THEN 0 ELSE
            (_sum_rt_sum_server / _sum_rt_count_server) / 1e6 AS srt_avg,
          IF _sum_rt_count_server = 0 THEN 0 ELSE
            sqrt (((sum rt_square_sum_server - (_sum_rt_sum_server)^2) /
                   _sum_rt_count_server) / 1e12) AS srt_stddev,
          -- Round Trip Time CSC
          sum rtt_count_server AS _sum_rtt_count_server,
          sum rtt_sum_server AS _sum_rtt_sum_server,
          _sum_rtt_count_server / $AVG$ AS crtt_per_secs,
          IF _sum_rtt_count_server = 0 THEN 0 ELSE
            (_sum_rtt_sum_server / _sum_rtt_count_server) / 1e6 AS crtt_avg,
          IF _sum_rtt_count_server = 0 THEN 0 ELSE
            sqrt (((sum rtt_square_sum_server - (_sum_rtt_sum_server)^2) /
                   _sum_rtt_count_server) / 1e12) AS crtt_stddev,
          -- Round Trip Time SCS
          sum rtt_count_client AS _sum_rtt_count_client,
          sum rtt_sum_client AS _sum_rtt_sum_client,
          _sum_rtt_count_client / $AVG$ AS srtt_per_secs,
          IF _sum_rtt_count_client = 0 THEN 0 ELSE
            (_sum_rtt_sum_client / _sum_rtt_count_client) / 1e6 AS srtt_avg,
          IF _sum_rtt_count_client = 0 THEN 0 ELSE
            sqrt (((sum rtt_square_sum_client - (_sum_rtt_sum_client)^2) /
                   _sum_rtt_count_client) / 1e12) AS srtt_stddev,
          -- Retransmition Delay C2S
          sum rd_count_client AS _sum_rd_count_client,
          sum rd_sum_client AS _sum_rd_sum_client,
          _sum_rd_count_client / $AVG$ AS crd_per_secs,
          IF _sum_rd_count_client = 0 THEN 0 ELSE
            (_sum_rd_sum_client / _sum_rd_count_client) / 1e6 AS crd_avg,
          IF _sum_rd_count_client = 0 THEN 0 ELSE
            sqrt (((sum rd_square_sum_client - (_sum_rd_sum_client)^2) /
                   _sum_rd_count_client) / 1e12) AS crd_stddev,
          -- Retransmition Delay S2C
          sum rd_count_server AS _sum_rd_count_server,
          sum rd_sum_server AS _sum_rd_sum_server,
          _sum_rd_count_server / $AVG$ AS srd_per_secs,
          IF _sum_rd_count_server = 0 THEN 0 ELSE
            (_sum_rd_sum_server / _sum_rd_count_server) / 1e6 AS srd_avg,
          IF _sum_rd_count_server = 0 THEN 0 ELSE
            sqrt (((sum rd_square_sum_server - (_sum_rd_sum_server)^2) /
                   _sum_rd_count_server) / 1e12) AS srd_stddev,
          -- Data Transfer Time C2S
          sum dtt_count_client AS _sum_dtt_count_client,
          sum dtt_sum_client AS _sum_dtt_sum_client,
          _sum_dtt_count_client / $AVG$ AS cdtt_per_secs,
          IF _sum_dtt_count_client = 0 THEN 0 ELSE
            (_sum_dtt_sum_client / _sum_dtt_count_client) / 1e6 AS cdtt_avg,
          IF _sum_dtt_count_client = 0 THEN 0 ELSE
            sqrt (((sum dtt_square_sum_client - (_sum_dtt_sum_client)^2) /
                   _sum_dtt_count_client) / 1e12) AS cdtt_stddev,
          -- Data Transfer Time S2C
          sum dtt_count_server AS _sum_dtt_count_server,
          sum dtt_sum_server AS _sum_dtt_sum_server,
          _sum_dtt_count_server / $AVG$ AS sdtt_per_secs,
          IF _sum_dtt_count_server = 0 THEN 0 ELSE
            (_sum_dtt_sum_server / _sum_dtt_count_server) / 1e6 AS sdtt_avg,
          IF _sum_dtt_count_server = 0 THEN 0 ELSE
            sqrt (((sum dtt_square_sum_server - (_sum_dtt_sum_server)^2) /
                   _sum_dtt_count_server) / 1e12) AS sdtt_stddev
        WHERE application = $ID$
        GROUP BY capture_begin * 0.000001 // $AVG_INT$
        COMMIT WHEN
          in.capture_begin * 0.000001 > out.start + 2 * $AVG$
        EXPORT EVENT STARTING AT start
               WITH DURATION $AVG$|} |>
      rep "$CSV$" csv |>
      rep "$ID$" (string_of_int bca.id) |>
      rep "$AVG_INT$" (string_of_int avg_window_int) |>
      rep "$AVG$" (string_of_float bca.avg_window)
    in
    make_node avg_per_app op ;
    let perc_per_obs_window_name =
      Printf.sprintf "%s: %gth perc on last %gs"
        bca.name bca.percentile bca.obs_window in
    let op =
      let nb_items_per_groups =
        Helpers.round_to_int (bca.obs_window /. bca.avg_window) in
      (* Note: The event start at the end of the observation window and lasts
       * for one avg window! *)
      (* EURT = RTTs + SRT + DTTs (DTT server to client being optional *)
      (* FIXME: sum of percentiles rather than percentiles of avg *)
      Printf.sprintf
        {|FROM '%s' SELECT
           min start, max start,
           %gth percentile (
            srtt_avg + crtt_avg + srt_avg + cdtt_avg + sdtt_avg) AS eurt
         EXPORT EVENT STARTING AT max_start WITH DURATION %g
         COMMIT AND SLIDE 1 WHEN
           group.#count >= %d OR
           in.start > out.max_start + 5|}
         avg_per_app bca.percentile
         bca.obs_window
         nb_items_per_groups in
    make_node perc_per_obs_window_name op ;
    (* TODO: we need an hysteresis here! *)
    let title =
      Printf.sprintf "EURT to %s is too large" bca.name
    and text =
      Printf.sprintf
        "The average end user response time to application %s has raised \
         above the configured maximum of %gs for the last %g minutes."
         bca.name bca.max_eurt (bca.obs_window /. 60.) in
    let ops =
      Printf.sprintf
        {|SELECT max_start, eurt > %g AS firing
          FROM '%s'
          COMMIT WHEN firing != previous.firing
          NOTIFY "http://localhost:29382/notify?name=EURT%%20%s&firing=${firing}&time=${max_start}&title=%s&text=%s"|}
          bca.max_eurt
          perc_per_obs_window_name
          (enc bca.name) (enc title) (enc text)
    and name = bca.name ^": EURT too high" in
    make_node name ops ;
    let pred_node, anom_node =
      anomaly_detection_nodes
        bca.avg_window
        avg_per_app
        [ "c2s_bytes_per_secs", false, [] ;
          "s2c_bytes_per_secs", false, [] ;
          "c2s_packets_per_secs", false, [] ;
          "s2c_packets_per_secs", false, [] ;
          "c2s_retrans_bytes_per_secs", false, [] ;
          "s2c_retrans_bytes_per_secs", false, [] ;
          "c2s_syns_per_secs", false, [] ;
          "s2c_rsts_per_secs", false, [] ;
          "close_per_secs", false, [] ;
          "c2s_dupacks_per_secs", false, [] ;
          "s2c_dupacks_per_secs", false, [] ;
          "c2s_0wins_per_secs", false, [] ;
          "s2c_0wins_per_secs", false, [] ;
          "ct_avg", false, [] ;
          "srt_avg", false, [] ;
          "crtt_avg", false, [] ;
          "srtt_avg", false, [] ;
          "crd_avg", false, [] ;
          "srd_avg", false, [] ;
          "cdtt_avg", false, [] ;
          "sdtt_avg", false, [] ] in
    all_nodes := pred_node :: anom_node :: !all_nodes
  in
  List.iter conf_of_bca bcas ;
  RamenSharedTypes.{ name = layer_name ; nodes = !all_nodes }

let get_config_from_db db =
  Conf_of_sqlite.get_config db

let ddos_layer dataset_name =
  let layer_name = rebase dataset_name "DDoS" in
  let avg_win = 60 and rem_win = 3600 in
  let op_new_peers =
    let avg_win_us = avg_win * 1_000_000 in
    {|FROM '$CSV$' SELECT
       (capture_begin // $AVG_WIN_US$) * $AVG_WIN$ AS start,
       min capture_begin, max capture_end,
       -- Traffic (of any kind) we haven't seen in the last $REM_WIN$ secs
       sum (0.9 * float(not remember (
              0.1, -- 10% of false positives
              capture_begin // 1_000_000, $REM_WIN$,
              (hash (coalesce (ip4_client, ip6_client, 0)) +
               hash (coalesce (ip4_server, ip6_server, 0)))))) /
         $AVG_WIN$
         AS nb_new_cnxs_per_secs,
       -- Clients we haven't seen in the last $REM_WIN$ secs
       sum (0.9 * float(not remember (
              0.1,
              capture_begin // 1_000_000, $REM_WIN$,
              hash (coalesce (ip4_client, ip6_client, 0))))) /
          $AVG_WIN$
          AS nb_new_clients_per_secs
     GROUP BY capture_begin // $AVG_WIN_US$
     COMMIT WHEN
       in.capture_begin > out.min_capture_begin + 2 * u64($AVG_WIN_US$)
     EXPORT EVENT STARTING AT start
                  WITH DURATION $AVG_WIN$|} |>
    rep "$AVG_WIN_US$" (string_of_int avg_win_us) |>
    rep "$AVG_WIN$" (string_of_int avg_win) |>
    rep "$REM_WIN$" (string_of_int rem_win) |>
    rep "$CSV$" (rebase dataset_name "csv") in
  let global_new_peers =
    make_node "new peers" op_new_peers in
  let pred_node, anom_node =
    anomaly_detection_nodes
      (float_of_int avg_win) "new peers"
      [ "nb_new_cnxs_per_secs", false, [] ;
        "nb_new_clients_per_secs", false, [] ] in
  RamenSharedTypes.{
    name = layer_name ;
    nodes = [ global_new_peers ; pred_node ; anom_node ] }

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
          with_base with_bcns with_bcas with_ddos =
  logger := make_logger conf.debug ;
  let open Conf_of_sqlite in
  let db = get_db db_name in
  let update () =
    (* TODO: The base layer for this client *)
    let%lwt () = if with_base then (
        let base = base_layer dataset_name delete csv_dir in
        put_layer ramen_url base
      ) else return_unit in
    let%lwt () = if with_bcns || with_bcas then (
        let bcns, bcas = get_config_from_db db in
        let%lwt () = if with_bcns then (
            let bcns = layer_of_bcns bcns dataset_name in
            put_layer ramen_url bcns
          ) else return_unit in
        if with_bcas then (
          let bcas = layer_of_bcas bcas dataset_name in
          put_layer ramen_url bcas
        ) else return_unit
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

let with_bcas =
  let i = Arg.info ~doc:"Also output the layer with BCA configuration"
                   [ "with-bcas" ; "with-bca" ; "bcas" ; "bca" ] in
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
      $ with_bcas
      $ with_ddos),
    info "ramen_configurator")

let () =
  match Term.eval start_cmd with
  | `Ok th -> Lwt_main.run th
  | x -> Term.exit x
