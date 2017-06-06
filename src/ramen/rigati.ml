(* The event-processor binary.
 * Can execute a configuration given on the command line.
 *)
open Cmdliner
open Model

let () =
  let open Dynlink in
  let string_of_linking_error = function
    | Undefined_global f -> Printf.sprintf "Undefined global %S" f
    | Unavailable_primitive f -> Printf.sprintf "Unavailable primitive %S" f
    | Uninitialized_global f -> Printf.sprintf "Uninitialized global %S" f
  in
  Printexc.register_printer (function
    | Error (Not_a_bytecode_file f) ->
      Some (Printf.sprintf "Not a bytecode file: %S" f)
    | Error (Inconsistent_import f) ->
      Some (Printf.sprintf "Inconsistent import: %S" f)
    | Error (Unavailable_unit f) ->
      Some (Printf.sprintf "Unavailable unit: %S" f)
    | Error Unsafe_file ->
      Some "Unsafe file"
    | Error (Linking_error (f, le)) ->
      Some (Printf.sprintf "Linking error: %S, %s"
              f (string_of_linking_error le))
    | Error (Corrupted_interface f) ->
      Some (Printf.sprintf "Corrupted interface: %S" f)
    | Error (File_not_found f) ->
      Some (Printf.sprintf "File not found: %S" f)
    | Error (Cannot_open_dll f) ->
      Some (Printf.sprintf "Cannot open dll: %S" f)
    | Error (Inconsistent_implementation f) ->
      Some (Printf.sprintf "Inconsistent implementation: %S" f)
    | _ -> None)

type options =
  { debug : bool ; prefer_long_names : bool }

let options debug prefer_long_names _configs =
  { debug ; prefer_long_names }

let plugin_conv =
  Dynlink.allow_unsafe_modules true ; (* Or bytecode would fail (NOP on native code) *)
  let parse str =
    try `Ok (Dynlink.loadfile str)
    with exn ->
      Printf.eprintf "Cannot load configuration: %s\n%!"
        (Printexc.to_string exn) ;
      `Error (Printexc.to_string exn)
  and print _fmt () = () in
  parse, print

let common_opts =
  let debug =
    Arg.(value (flag (info ~doc:"increase verbosity" ["d"; "debug"])))
  and configs =
    Arg.(value (pos_all plugin_conv [] (info ~docv:"CMXS" ~doc:"configuration to load" [])))
  and prefer_long_names =
    Arg.(value (flag (info ~doc:"use full node path as names (deduplicate nodes \
                                 with same name but various call stack)"
                           ["long-names"])))
  in
  Term.(const options $ debug $ prefer_long_names $ configs)

let cmdliner_conv_of_ppp ppp =
  (fun str -> match PPP.of_string ppp str 0 with
    | Some (x, l) when l = String.length str -> `Ok x
    | Some (_, l) -> `Error ("Garbage at pos "^ string_of_int l)
    | _ -> `Error (Printf.sprintf "Cannot parse %S" str)),
  (fun fmt x -> Format.fprintf fmt "%s" (PPP.to_string ppp x))

type setting_change =
  { node : Graph.node_spec ; settings : Setting.t list } [@@ppp PPP_OCaml]

let setting_change_opt =
  let i = Arg.(info ~docv:"SETTING"
                    ~doc:("Set the specific setting of a node. "^
                          setting_change_ppp.PPP.descr)
                    ["set"]) in
  Arg.(value (opt_all (cmdliner_conv_of_ppp setting_change_ppp) [] i))

(*
 * Pretty Printer
 *)

let apply_setting_change graph setting_change =
  match Graph.lookup_node graph setting_change.node with
  | exception Not_found ->
    failwith (Printf.sprintf "Cannot find node id %s\n%!"
                (PPP.to_string Graph.node_spec_ppp setting_change.node))
  | node ->
    node.Node.settings <-
      List.rev_append setting_change.settings node.Node.settings

let get_config_graph options name setting_changes =
  let g = Graph.empty name in
  let m = Hashtbl.find Configuration.registered_configs name in
  let module ConfMaker = (val m : Configuration.MAKER) in
  if options.debug then Printf.eprintf "Building config graph.\n%!" ;
  let module ConfModule = ConfMaker (Engine.AddId (ReifyEngine.Impl)) in
  let configuration = ConfModule.configuration () in
  let g = configuration ?from_node:None g in
  List.iter (apply_setting_change g) setting_changes ;
  if options.debug then Printf.eprintf "Got config graph.\n%!" ;
  g

let print_graph ~as_dot ~prefer_long_names graph =
  let print_dot graph =
    let p = Printf.printf in
    p "digraph %S {\n" graph.Graph.name ;
    let node_name node =
      if prefer_long_names then
        Node.long_name node
      else node.Node.name in
    List.iter (fun node ->
        p "  \"#%d\" [label=\"%s(#%d)\"]\n"
          node.Node.id (node_name node) node.Node.id
      ) graph.Graph.nodes ;
    List.iter (fun pipe ->
        let open Pipe in
        p "  \"#%d\" -> \"#%d\"\n"
          pipe.from_node.Node.id
          pipe.to_node.Node.id
      ) graph.Graph.pipes ;
    p "}\n"
  in
  if as_dot then print_dot graph
  else Printf.printf "%s\n"
         (PPP.to_string Graph.t_ppp graph)

let print options setting_changes as_dot () =
  Hashtbl.iter (fun name _ ->
      if options.debug then
        Printf.eprintf "Printing configuration %S\n%!" name ;
      let graph = get_config_graph options name setting_changes in
      print_graph ~as_dot ~prefer_long_names:options.prefer_long_names graph
    ) Configuration.registered_configs

let as_dot =
  let i = Arg.info ~doc:"print the graph in dotty format" ["as-dot"] in
  Arg.(value (flag i))

let print_cmd =
  Term.(
    (const print
      $ common_opts
      $ setting_change_opt
      $ as_dot),
    info "print")

(*
 * Executor
 *)

let exec options setting_changes timestep alerter_conf_db trace with_http () =
  Alarm.timestep := timestep ;
  ExecuteEngine.init ?alerter_conf_db () ;
  let exec_conf graph m =
    let module ExecConfig = struct let graph = graph end in
    let module ConfMaker = (val m : Configuration.MAKER) in
    if not trace then (
      let module ConfModule =
        ConfMaker (Engine.AddId (ExecuteEngine.AddSettings (ExecConfig) (ExecuteEngine.Impl))) in
      (* We have no event to send to the toplevel operation but we have
       * to call ConfModule.configuration for its side effects: creating
       * the actual operational configuration (aka spawning new file readers
       * etc.) *)
      ConfModule.configuration () |> ignore ;
      (* Returns the function that tells when we must rebuild everything: *)
      ConfModule.must_reload
    ) else (
      (* It is important to wrap into Tracing before wrapping into setting,
       * as we start to inject events from the settings. *)
      let module ConfModule =
        ConfMaker (Engine.AddId (ExecuteEngine.AddSettings (ExecConfig) (ExecuteEngine.AddTracing (ExecuteEngine.Impl)))) in
      ConfModule.configuration () |> ignore ;
      (* Returns the function that tells when we must rebuild everything: *)
      ConfModule.must_reload
    )
  in
  Hashtbl.iter (fun name m ->
      if options.debug then Printf.printf "Executing: %s\n" name ;
      let current_graph =
        ref (get_config_graph options name setting_changes) in
      let must_reload =
        ref (exec_conf !current_graph m) in
      (* Check if this configuration must be updated from time to time: *)
      Alarm.every 1. (fun () ->
        if !must_reload () then (
          if options.debug then Printf.eprintf "Reloading %s\n%!" name ;
          let new_graph = get_config_graph options name setting_changes in
          if options.debug then print_graph ~as_dot:true ~prefer_long_names:false new_graph ;
          current_graph := Graph.update !current_graph new_graph ;
          must_reload := exec_conf !current_graph m
        ))
    ) Configuration.registered_configs ;
  IO.start options.debug with_http

let timestep_opt =
  let i = Arg.info ~doc:"How frequently to update the internal clock."
                   ["timestep"; "time-step"] in
  Arg.(value (opt float 1. i))

let alerter_conf_db_opt =
  let i = Arg.info ~doc:"Where to find the alert-manager configuration db."
                   ["alert-mgmt-db"] in
  Arg.(value (opt (some string) None i))

let trace =
  let i = Arg.info ~doc:"Display log of each operation for every event."
                   ["t"; "trace"] in
  Arg.(value (flag i))

let with_http =
  let i = Arg.info ~doc:"Also start an HTTP server on the given port \
                         to report/display internal state."
                   ["with-http"; "http"] in
  Arg.(value (opt int 0 i))

let exec_cmd =
  Term.(
    (const exec
      $ common_opts
      $ setting_change_opt
      $ timestep_opt
      $ alerter_conf_db_opt
      $ trace
      $ with_http),
    info "exec")

(*
 * Main
 *)

let default_cmd =
  let doc = "Event Processor Swiss-army knife" in
  Term.((ret (app (const (fun _ -> `Help (`Pager, None))) common_opts)),
        info "rigati" ~doc)

let () =
  match Term.eval_choice default_cmd [
    print_cmd ;
    exec_cmd
  ] with `Error _ -> exit 1
       | `Version | `Help -> exit 42
       | `Ok f -> f ()
