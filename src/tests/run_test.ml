(* This is for testing as much as we can by runing the actual programs with actual data.
 * Another tool is needed to help users test their configuration, that would just mock
 * everything and use ramen as a library.
 * So, this program first create a networking alert configuration DB, then an
 * alert management configuration DB, then create a CSV with some traffic, then
 * run Ramen and check received alerts (with or without acknowledging them. *)
open Batteries
open Helpers

let run_cmd debug cmd =
  if debug then Printf.eprintf "Running: %s\n" cmd ;
  IO.flush_all () ;
  Sys.command cmd

let must_run_ok = must_be string_of_int 0

let opt_print optname () = function
  | None -> ""
  | Some v -> Printf.sprintf "%s=%S" optname v

let run_ramen debug ramen_bin alerting_cmxs alerter_conf_db alerting_conf_db csv =
  let entry_point = "to unidir volumes" in
  run_cmd debug "rm -f /tmp/alerter_conf.db" |> must_run_ok ;
  let cmd =
    Printf.sprintf "%a %S exec%s %S %a --set '{node=Name %S; settings=[ReadFile %S%s]}'"
      (opt_print "CONFIG_DB") alerting_conf_db
      ramen_bin
      (if debug then " --debug" else "")
      alerting_cmxs
      (opt_print "--alert-mgmt-db") alerter_conf_db
      entry_point csv (if debug then ";Debug" else "") in
  run_cmd debug cmd |> must_run_ok

let clean_all debug =
  if debug then Printf.eprintf "Cleaning...\n" ;
  (* TODO *)
  ()

let () =
  let ramen_bin = ref "../ramen/rigati"
  and alerting_cmxs = ref "../configurations/alerting_conf.cmxs"
  and alerter_conf_db = ref None
  and alerting_conf_db = ref None
  and csv = ref ""
  and debug = ref false
  and keep_temps = ref false in
  let arg_specs = Arg.[
    "--ramen-path", Set_string ramen_bin, "ramen binary location" ;
    "--alerting-cmxs", Set_string alerting_cmxs, "Alerting configuration module" ;
    "--alert-mgmt-conf", String (fun x -> alerter_conf_db := Some x), "Alert manager configuration to use" ;
    "--alerting-conf", String (fun x -> alerting_conf_db := Some x), "Alerting configuration to use" ;
    "--keep-temps", Set keep_temps, "Do not delete temporary files" ;
    "--debug", Set debug, "More verbose output" ]
  and usage_msg = "run_test [options] file.csv" in
  Arg.(parse arg_specs (fun x -> csv := x) usage_msg) ;
  if !csv = "" then Arg.usage arg_specs usage_msg
  else ( 
    run_ramen !debug !ramen_bin !alerting_cmxs !alerter_conf_db !alerting_conf_db !csv ;
    if not !keep_temps then clean_all !debug
  )

