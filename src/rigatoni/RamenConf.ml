(* Global configuration for rigatoni daemon *)
open Batteries
open Log

type node =
  { name : string ;
    operation : Lang.Operation.t ;
    mutable parents : node list ;
    mutable children : node list ;
    command : string }

type graph =
  { nodes : (string, node) Hashtbl.t }

type conf =
  { running_graph : graph ;
    save_file : string }

let make_node name operation =
  !logger.debug "Creating node %s" name ;
  let command = CodeGen_OCaml.gen_operation name operation in
  { name ; operation ; parents = [] ; children = [] ; command }

let make_new_graph () =
  { nodes = Hashtbl.create 17 }

let make_graph save_file =
  try
    File.with_file_in save_file (fun ic -> Marshal.input ic)
  with
    | Sys_error err ->
      !logger.debug "Cannot read state from file %S: %s. Starting anew" save_file err ;
      make_new_graph ()
    | BatInnerIO.No_more_input ->
      !logger.debug "Cannot read state from file %S: not enough input. Starting anew" save_file ;
      make_new_graph ()

let save_graph conf graph =
  !logger.debug "Saving graph in %S" conf.save_file ;
  File.with_file_out ~mode:[`create; `trunc] conf.save_file (fun oc ->
    Marshal.output oc graph)

let has_node _conf graph id =
  Hashtbl.mem graph.nodes id

let find_node _conf graph id =
  Hashtbl.find graph.nodes id

let add_node conf graph id node =
  Hashtbl.add graph.nodes id node ;
  save_graph conf graph

let remove_node conf graph id =
  let node = Hashtbl.find graph.nodes id in
  List.iter (fun p ->
      p.children <- List.filter ((!=) node) p.children
    ) node.parents ;
  List.iter (fun p ->
      p.parents <- List.filter ((!=) node) p.parents
    ) node.children ;
  Hashtbl.remove_all graph.nodes id ;
  save_graph conf graph

let has_link _conf src dst =
  List.exists ((==) dst) src.children

let make_link conf graph src dst =
  !logger.debug "Create link between nodes %s and %s" src.name dst.name ;
  src.children <- dst :: src.children ;
  dst.parents <- src :: dst.parents ;
  save_graph conf graph

let remove_link conf graph src dst =
  !logger.debug "Delete link between nodes %s and %s" src.name dst.name ;
  src.children <- List.filter ((!=) dst) src.children ;
  dst.parents <- List.filter ((!=) src) dst.parents ;
  save_graph conf graph

let make_conf debug save_file =
  logger := Log.make_logger debug ;
  { running_graph = make_graph save_file ; save_file }
