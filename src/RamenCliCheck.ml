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

let confserver conf ports ports_sec srv_pub_key_file srv_priv_key_file
               incidents_history_length =
  (* Some not-so-common options makes no sense for confserver and are likely
   * a user error: *)
  let check_unset what suggestion s =
    if s <> "" then
      Printf.sprintf "Option %s makes no sense for confserver (do you mean %s?)"
        what suggestion |>
      failwith in
  check_unset "--client-public-key" "--server-public-key" conf.C.clt_pub_key ;
  check_unset "--client-private-key" "--server-private-key" conf.clt_priv_key ;
  (* Options confserver/confserver-key/identity also makes no sense, but could
   * be passed as envvars not specifically for confserver and thus should not
   * generate any errors. *)
  if ports = [] && ports_sec = [] then
    failwith "You must specify some ports to listen to with --secure and/or \
             --insecure." ;
  if ports_sec = [] && not (N.is_empty srv_pub_key_file) then
    failwith "--public-key makes no sense without --secure." ;
  if ports_sec = [] && not (N.is_empty srv_priv_key_file) then
    failwith "--private-key makes no sense without --secure." ;
  if incidents_history_length < 0 then
    failwith "--incidents-history-length must be positive."

let check_opt_level = function
  | 0 | 1 | 2 | 3 -> ()
  | _ ->
      failwith "Invalid optimization level: must be between 0 and 3 \
                (inclusive)"

let execompserver conf max_simult_compils quarantine opt_level =
  if conf.C.sync_url = "" then
    failwith "Cannot start the compilation service without --confserver." ;
  if max_simult_compils <= 0 then
    failwith "--max-simult-compilations must be positive." ;
  if quarantine < 0. then
    failwith "--quarantine must be positive." ;
  check_opt_level opt_level

let compile source_files src_path_opt opt_level =
  let many_source_files = List.length source_files > 1 in
  if many_source_files && src_path_opt <> None then
    failwith "Cannot specify the program name for several source files." ;
  check_opt_level opt_level

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
