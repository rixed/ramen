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

let confserver ports ports_sec srv_pub_key_file srv_priv_key_file
               incidents_history_length =
  if ports = [] && ports_sec = [] then
    failwith "You must specify some ports to listen to with --secure and/or \
             --insecure." ;
  if ports_sec = [] && not (N.is_empty srv_pub_key_file) then
    failwith "--public-key makes no sense without --secure." ;
  if ports_sec = [] && not (N.is_empty srv_priv_key_file) then
    failwith "--private-key makes no sense without --secure." ;
  if incidents_history_length < 0 then
    failwith "--incidents-history-length must be positive."

let execompserver conf max_simult_compils quarantine force_dessser
                  force_legacy =
  if conf.C.sync_url = "" then
    failwith "Cannot start the compilation service without --confserver." ;
  if max_simult_compils <= 0 then
    failwith "--max-simult-compilations must be positive." ;
  if quarantine < 0. then
    failwith "--quarantine must be positive." ;
  if force_dessser && force_legacy then
    failwith "--force-dessser and --force-legacy are incompatible"

let compile source_files src_path_opt force_dessser force_legacy =
  let many_source_files = List.length source_files > 1 in
  if many_source_files && src_path_opt <> None then
    failwith "Cannot specify the program name for several source files." ;
  if force_dessser && force_legacy then
    failwith "--force-dessser and --force-legacy are incompatible"

let precompserver conf =
  if conf.C.sync_url = "" then
    failwith "Cannot start the precompilation service without --confserver."

let replayer conf =
  if conf.C.sync_url = "" then
    failwith "Cannot start the replay service without --confserver."

let start _conf = ()

let confclient key value del if_exists follow =
  if value <> "" && key = "" then
    failwith "Cannot set a value without a key." ;
  if del && key = "" then
    failwith "Cannot delete a value without a key." ;
  if del && value <> "" then
    failwith "--value and --delete are incompatible." ;
  if follow && (del || value <> "") then
    failwith "Cannot --follow when editing/deleting." ;
  if if_exists && not del then
    failwith "--if-exists makes no sense without --delete"

let alerter max_fpr =
  if max_fpr < 0. || max_fpr > 1. then
    failwith "False-positive rate is a rate is a rate."

let non_empty what = function
  | "" ->
      failwith (what ^" is missing")
  | _ ->
      ()
