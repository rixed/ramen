module C = RamenConf
module N = RamenName

let archivist conf loop daemonize stats allocs reconf =
  if conf.C.sync_url <> "" then (
    if stats then
      failwith "The --stats command makes no sense with confserver." ;
    if not allocs && not reconf then
      failwith "Must specify at least one of --allocs or --reconf-workers."
  ) else (
  if not stats && not allocs && not reconf then
    failwith "Must specify at least one of --stats, --allocs or --reconf-workers."
  ) ;
  if daemonize && loop = Some 0. then
    failwith "It makes no sense to --daemonize without --loop."

let gc daemonize loop =
  if daemonize && loop = Some 0. then
    failwith "It makes no sense to --daemonize without --loop."

let choreographer conf =
  if conf.C.sync_url = "" then
    failwith "Cannot start the choreographer without --confserver."

let confserver ports ports_sec srv_pub_key_file srv_priv_key_file =
  if ports = [] && ports_sec = [] then
    failwith "You must specify some ports to listen to with --secure and/or \
             --insecure" ;
  if ports_sec = [] && not (N.is_empty srv_pub_key_file) then
    failwith "--public-key makes no sense without --secure" ;
  if ports_sec = [] && not (N.is_empty srv_priv_key_file) then
    failwith "--private-key makes no sense without --secure"

let execompserver conf =
  if conf.C.sync_url = "" then
    failwith "Cannot start the compilation service without --confserver."

let precompserver conf =
  if conf.C.sync_url = "" then
    failwith "Cannot start the precompilation service without --confserver."

let replayer conf =
  if conf.C.sync_url = "" then
    failwith "Cannot start the replay service without --confserver."

let start _conf ports =
  if ports = [] then
    failwith "Start must be run with --insecure option."

let confclient key_opt value =
  if value <> "" && key_opt = "" then
    failwith "Cannot set a value without a key."

let alerter max_fpr =
  if max_fpr < 0. || max_fpr > 1. then
    failwith "False-positive rate is a rate is a rate."
