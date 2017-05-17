(* The event-processor binary.
 * Can execute a configuration given on the command line.
 *)
open Cmdliner
open Model

type options =
  { debug : bool ; prefer_long_names : bool }

let options debug prefer_long_names _configs =
  { debug ; prefer_long_names }

let plugin_conv =
  let parse str =
    try `Ok (Dynlink.loadfile str)
    with exn -> `Error (Printexc.to_string exn)
  and print _fmt () = () in
  parse, print

let common_opts =
  let debug =
    Arg.(value (flag (info ~doc:"increase verbosity" ["d"; "debug"])))
  and configs =
    Arg.(value (pos_all plugin_conv [] (info ~doc:"configuration to load" [])))
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
  { node_id : int ; settings : Setting.t list } [@@ppp PPP_OCaml]

let setting_change_opts =
  let i = Arg.(info ~docv:"SETTING"
                    ~doc:("Set the specific setting of a node. "^
                          setting_change_ppp.PPP.descr)
                    ["set"]) in
  Arg.(value (opt_all (cmdliner_conv_of_ppp setting_change_ppp) [] i))

(*
 * Pretty Printer
 *)

let apply_setting_change graph setting_change =
  let node = Graph.lookup_node graph setting_change.node_id in
  node.Node.settings <-
    List.rev_append setting_change.settings node.Node.settings

let get_config_graph name setting_changes =
  let m = Hashtbl.find Configuration.registered_configs name in
  let module ConfMaker = (val m : Configuration.MAKER) in
  let module ConfModule = ConfMaker (Engine.AddId (ReifyEngine.Impl)) in
  let g = Graph.empty name in
  let g = ConfModule.configuration ?from_node:None g in
  List.iter (apply_setting_change g) setting_changes ;
  g

let print options setting_changes as_dot () =
  let print_dot graph =
    let p = Printf.printf in
    p "digraph %S {\n" graph.Graph.name ;
    let node_name node =
      if options.prefer_long_names then
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
  Hashtbl.iter (fun name _ ->
      let graph = get_config_graph name setting_changes in
      if as_dot then print_dot graph
      else (
        Printf.printf "%s\n"
          (PPP.to_string Graph.t_ppp graph)
      )
    ) Configuration.registered_configs

let as_dot =
  let i = Arg.info ~doc:"print the graph in dotty format" ["as-dot"] in
  Arg.(value (flag i))

let print_cmd =
  Term.(
    (const print
      $ common_opts
      $ setting_change_opts
      $ as_dot),
    info "print")

(*
 * Executor
 *)

let exec options setting_changes timestep () =
  Alarm.timestep := timestep ;
  Hashtbl.iter (fun name m ->
      let graph = get_config_graph name setting_changes in
      let module ExecConfig = struct let graph = graph end in
      let module ConfMaker = (val m : Configuration.MAKER) in
      let module ConfModule =
        ConfMaker (Engine.AddId (ExecuteEngine.Impl (ExecConfig))) in
      if options.debug then Printf.printf "Executing: %s\n" name ;
      IO.start (Alarm.main_loop ())
    ) Configuration.registered_configs

let timestep_opt =
  let i = Arg.info ~doc:"How frequently to update the internal clock."
                   ["timestep"; "time-step"] in
  Arg.(value (opt float 1. i))

let exec_cmd =
  Term.(
    (const exec
      $ common_opts
      $ setting_change_opts
      $ timestep_opt),
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
