(* Bits of configuration shared by all modules in a worker *)
open RamenConsts
open RamenHelpers
open RamenLog
module Files = RamenFiles
module N = RamenName

type conf =
  { log_level : log_level ;
    state_file : N.path ;
    is_test : bool ;
    is_top_half : bool ;
    is_replayer : bool ;
    site : N.site ;
    fq : N.fq ;
    instance : string ;
    report_period : float }

let make_conf ?(is_replayer=false) ?(is_top_half=false) () =
  let log_level =
    getenv ~def:"normal" "log_level" |> log_level_of_string in
  let report_period =
    getenv ~def:(string_of_float Default.report_period)
           "report_period" |> float_of_string in
  let is_test =
    getenv ~def:"false" "is_test" |> bool_of_string in
  let site =
    N.site (getenv ~def:"" "site") in
  let fq =
    N.fq (getenv ~def:"?fq_name?" "fq_name") in
  let instance =
    getenv ~def:"?instance?" "instance" in
  let prefix =
    (fq :> string) ^
    (if is_replayer then " (REPLAY)" else "") ^
    (if is_top_half then " (TOP-HALF)" else "") ^": " in
  (match getenv "log" with
  | exception _ ->
      init_logger ~prefix log_level
  | logdir ->
      if logdir = "syslog" then
        init_syslog ~prefix log_level
      else (
        Files.mkdir_all (N.path logdir) ;
        init_logger ~logdir log_level
      )) ;
  let default_persist_dir =
    "/tmp/worker_"^ (fq :> string) ^"_"^
    (if is_top_half then "TOP_HALF_" else "")^
    string_of_int (Unix.getpid ()) in
  let state_file =
    let def = default_persist_dir ^"/state" in
    N.path (getenv ~def "state_file") in
  { log_level ; state_file ; is_test ; is_replayer ; is_top_half ;
    site ; fq ; instance ; report_period }
