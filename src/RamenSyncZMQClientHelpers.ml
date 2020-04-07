open RamenHelpersNoLog
open RamenSync
open RamenSyncHelpers

let send_service_conf ~while_ conf service_name port session =
    let host_key = RamenSync.Key.(PerSite (conf.RamenConf.site, (PerService (service_name, Host)))) in
    let port_key = RamenSync.Key.(PerSite (conf.RamenConf.site, (PerService (service_name, Port)))) in
    let host_value = Value.RamenValue (T.VString (conf.RamenConf.site :> string)) in
    let port_value = Value.RamenValue (T.VU16 (Stdint.Uint16.of_int port)) in
    ZMQClient.send_cmd session ~while_ (Client.CltMsg.SetKey (host_key, host_value)) ;
    ZMQClient.send_cmd session ~while_ (Client.CltMsg.SetKey (port_key, port_value))

let send_healthcheck ~while_ ~now conf service_name healthchecks_per_sec session =
  let hearth_key = Key.(PerSite (conf.C.site, (PerService (service_name, Health)))) in
  let hearth_value = Value.RamenValue (T.VFloat now) in
  ZMQClient.send_cmd session ~while_ (Client.CltMsg.SetKey (hearth_key, hearth_value)) ;
  let next_hearth_key = Key.(PerSite (conf.C.site, (PerService (service_name, NextHealth)))) in
  let next_hearth_value = Value.RamenValue (T.VFloat (now+.60./.healthchecks_per_sec)) in
  ZMQClient.send_cmd session ~while_ (Client.CltMsg.SetKey (next_hearth_key, next_hearth_value))
