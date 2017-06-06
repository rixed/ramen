(* Simple description of a configuration so that we can then generate
 * all corresponding files:
 * The alerting configuration, the alert manager configuration, hosts
 * communications and the resulting CSV... *)
open TestGen

module T : TEST =
struct
  let alerting_config () =
    [ { zone_from = 1 ; zone_to = 2 ;
        (* We need at least 20 slices of 5 mins to compute a 95 percentile: *)
        (* BUT current bcnthreshold can't store this so we use only the
         * default: avg 5mins, obs 2h and 95 percentile. *)
        avg_window = (mins 5) ; obs_window = (mins 120) ; percentile = 0.95 ; 
        min = None ; max = Some (kbs 1000) ;
        relevancy = None ; max_rtt = None ; max_rr = None } ;
    ]

  let traffic () =
    (* Force evaluation order because of TestGen clock: *)
    traffic_between_zones 1 2 ~bandw_asc:(kbs 500) ~duration:(mins 100) [] |>
    traffic_between_zones 1 2 ~bandw_asc:(kbs 1500) ~duration:(mins 200)

  let expected_alerts () =
    [ alert ~at:(mins 100) ~name:"traffic pas glop" ]
end

let () =
  register_test "simple" (module T : TEST)
