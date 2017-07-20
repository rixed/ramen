(* Global configuration for rigatoni daemon *)
open Batteries
open Log
open RamenSharedTypes

type temp_tup_typ =
  { mutable complete : bool ;
    (* Not sure we need the rank for anything, actually *)
    fields : (string, int option ref * Lang.Expr.typ) Hashtbl.t }

let print_temp_tup_typ fmt t =
  Printf.fprintf fmt "%a (%s)"
    (Hashtbl.print ~first:"{" ~last:"}" ~sep:", " ~kvsep:""
                   (fun _fmt _ -> ())
                   (fun fmt (rank, expr_typ) ->
                     Printf.fprintf fmt "[%s] %a"
                      (match !rank with
                      | Some r -> string_of_int r
                      | None -> "??")
                      Lang.Expr.print_typ expr_typ)) t.fields
    (if t.complete then "complete" else "incomplete")

let make_temp_tup_typ () =
  { complete = false ;
    fields = Hashtbl.create 7 }

let temp_tup_typ_of_tup_typ complete tup_typ =
  let t = make_temp_tup_typ () in
  t.complete <- complete ;
  List.iteri (fun i f ->
      let expr_typ =
        Lang.Expr.make_typ ~nullable:f.nullable
                           ~typ:f.typ f.typ_name in
      Hashtbl.add t.fields f.typ_name (ref (Some i), expr_typ)
    ) tup_typ ;
  t

let list_of_temp_tup_type ttt =
  Hashtbl.values ttt.fields |>
  List.of_enum |>
  List.fast_sort (fun (r1, _) (r2, _) -> compare r1 r2) |>
  List.map (fun (r, f) -> !r, f)

let tup_typ_of_temp_tup_type ttt =
  let open Lang in
  assert ttt.complete ;
  list_of_temp_tup_type ttt |>
  List.map (fun (_, typ) ->
    { typ_name = typ.Expr.expr_name ;
      nullable = Option.get typ.Expr.nullable ;
      typ = Option.get typ.Expr.scalar_typ })

type node =
  { name : string ;
    mutable operation : Lang.Operation.t ;
    (* Also keep the string as defined by the client so we do not loose
     * formatting *)
    mutable op_text : string ;
    mutable parents : node list ;
    mutable children : node list ;
    mutable in_type : temp_tup_typ ;
    mutable out_type : temp_tup_typ ;
    mutable command : string option ;
    mutable pid : int option ;
    mutable last_report : Binocle.metric list }

type graph_persist =
  { nodes : (string, node) Hashtbl.t }

type graph =
  { mutable status : graph_status ;
    mutable last_started : float option ;
    mutable last_stopped : float option ;
    mutable importing_threads : unit Lwt.t list ;
    persist : graph_persist }

type conf =
  { mutable building_graph : graph ;
    save_file : string ;
    report_url_prefix : string }

exception InvalidCommand of string

(* Graph edition: only when stopped *)

let set_graph_editable graph =
  match graph.status with
  | Edition -> ()
  | Compiled ->
    graph.status <- Edition ;
    (* Also reset the info we kept from the last cmopilation *)
    Hashtbl.iter (fun _ node ->
       node.in_type <- make_temp_tup_typ () ;
       node.out_type <- make_temp_tup_typ () ;
       node.command <- None ;
       node.pid <- None) graph.persist.nodes
  | Running ->
    raise (InvalidCommand "Graph is running")

let parse_operation operation =
  let open Lang.P in
  let p = Lang.(opt_blanks -+ Operation.Parser.p +- opt_blanks +- eof) in
  (* TODO: enable error correction *)
  match p [] None Parsers.no_error_correction (stream_of_string operation) |>
        to_result with
  | Bad e ->
    let err =
      IO.to_string (Lang.P.print_bad_result Lang.Operation.print) e in
    raise (Lang.SyntaxError ("Parse error: "^ err))
  | Ok (op, _) -> (* Since we force EOF, no need to keep what's left to parse *)
    Lang.Operation.Parser.check op ;
    op

let make_node graph name op_text =
  !logger.debug "Creating node %s" name ;
  set_graph_editable graph ;
  let operation = parse_operation op_text in
  { name ; operation ; op_text ; parents = [] ; children = [] ;
    (* Set once the whole graph is known and reset each time the graph is
     * edited: *)
    in_type = make_temp_tup_typ () ; out_type = make_temp_tup_typ () ;
    command = None ; pid = None ; last_report = [] }

let update_node graph node op_text =
  !logger.debug "Modifying node %s" node.name ;
  set_graph_editable graph ;
  let operation = parse_operation op_text in
  node.operation <- operation ;
  node.op_text <- op_text ;
  node.command <- None ; node.pid <- None

let make_graph ?persist () =
  let persist =
    Option.default_delayed (fun () -> { nodes = Hashtbl.create 17 }) persist in
  { status = Edition ;
    importing_threads = [] ;
    last_started = None ; last_stopped = None ;
    persist }

let load_graph save_file =
  let persist =
    try
      File.with_file_in save_file (fun ic ->
        Some (Marshal.input ic))
    with
    | Sys_error err ->
      !logger.debug "Cannot read state from file %S: %s. Starting anew" save_file err ;
      None
    | BatInnerIO.No_more_input ->
      !logger.debug "Cannot read state from file %S: not enough input. Starting anew" save_file ;
      None
  in
  make_graph ?persist ()

let save_graph conf graph =
  !logger.debug "Saving graph in %S" conf.save_file ;
  File.with_file_out ~mode:[`create; `trunc] conf.save_file (fun oc ->
    Marshal.output oc graph.persist)

let has_node _conf graph id =
  Hashtbl.mem graph.persist.nodes id

let find_node _conf graph id =
  Hashtbl.find graph.persist.nodes id

let add_node conf graph node =
  if has_node conf graph node.name then
    raise (InvalidCommand ("Node "^ node.name ^" already exists")) ;
  Hashtbl.add graph.persist.nodes node.name node ;
  save_graph conf graph

let remove_node conf graph name =
  !logger.debug "Removing node %s" name ;
  set_graph_editable graph ;
  let node = Hashtbl.find graph.persist.nodes name in
  List.iter (fun p ->
      p.children <- List.filter ((!=) node) p.children
    ) node.parents ;
  List.iter (fun p ->
      p.parents <- List.filter ((!=) node) p.parents
    ) node.children ;
  Hashtbl.remove_all graph.persist.nodes name ;
  save_graph conf graph

let has_link _conf src dst =
  List.exists ((==) dst) src.children

let make_link conf graph src dst =
  !logger.debug "Create link between nodes %s and %s" src.name dst.name ;
  set_graph_editable graph ;
  src.children <- dst :: src.children ;
  dst.parents <- src :: dst.parents ;
  save_graph conf graph

let remove_link conf graph src dst =
  !logger.debug "Delete link between nodes %s and %s" src.name dst.name ;
  set_graph_editable graph ;
  src.children <- List.filter ((!=) dst) src.children ;
  dst.parents <- List.filter ((!=) src) dst.parents ;
  save_graph conf graph

let make_conf debug save_file =
  logger := Log.make_logger debug ;
  { building_graph = load_graph save_file ; save_file ;
    report_url_prefix = "http://127.0.0.1:29380/report" }
