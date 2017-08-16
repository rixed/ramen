(* Global configuration *)
open Batteries
open Log
open RamenSharedTypes

type temp_tup_typ =
  { mutable finished_typing : bool ;
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
    (if t.finished_typing then "finished typing" else "to be typed")

let type_signature t =
  let tag_of_rank r = "["^ string_of_int r ^"]" in
  let keys = Hashtbl.keys t.fields |> Array.of_enum in
  Array.fast_sort String.compare keys ;
  Array.fold_left (fun s k ->
      let rank, typ = Hashtbl.find t.fields k in
      (if s = "" then "" else s ^ "_") ^
      k ^ ":" ^ Lang.Expr.signature_of_typ typ ^ tag_of_rank (Option.get !rank)
    ) "" keys

let make_temp_tup_typ () =
  { finished_typing = false ;
    fields = Hashtbl.create 7 }

let temp_tup_typ_of_tup_typ tup_typ =
  let t = make_temp_tup_typ () in
  t.finished_typing <- true ;
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
  List.fast_sort (fun (r1, _) (r2, _) -> compare !r1 !r2) |>
  List.map (fun (r, f) -> !r, f)

let tup_typ_of_temp_tup_type ttt =
  let open Lang in
  assert ttt.finished_typing ;
  list_of_temp_tup_type ttt |>
  List.map (fun (_, typ) ->
    { typ_name = typ.Expr.expr_name ;
      nullable = Option.get typ.Expr.nullable ;
      typ = Option.get typ.Expr.scalar_typ })

let make_node_name =
  let seq = ref 0 in
  fun () ->
    incr seq ;
    string_of_int !seq

type node =
  { (* We identify nodes by a unique name that can be optionally provided
       automatically if its not meant to be referenced. *)
    name : string ;
    (* Nodes are added/removed to/from the graph in group called layers.
     * Layers can connect to nodes from any other layers and non existing
     * nodes, but there is still the notion of layers "depending" (or lying on)
     * others; we must prevent change of higher level layers to restart lower
     * level layers, while update of a low level layers can trigger the
     * recompilation of upper layers.  The idea is that there are some
     * ephemeral layers that answer specific queries on top of more fundamental
     * layers that compute generally useful data, a bit like functions calling
     * each others. *)
    layer : layer ;
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

let signature =
  let blanks = Str.regexp "[ \n\t\b\r]\\+" in
  fun op_text in_type out_type ->
    let op_text = Str.global_replace blanks " " op_text in
    "OP="^ op_text ^
    "IN="^ type_signature in_type ^
    "OUT="^ type_signature out_type |>
    Cryptohash_md4.string |>
    Cryptohash_md4.to_hex

type graph_persist =
  { mutable status : graph_status ;
    nodes : (string, node) Hashtbl.t }

(* FIXME: have a subgraph per layer, and relax node name uniqueness to the
 * layer they are in. *)
type graph =
  { mutable last_started : float option ;
    mutable last_stopped : float option ;
    mutable importing_threads : unit Lwt.t list ;
    persist : graph_persist }

type conf =
  { mutable building_graph : graph ;
    save_file : string option ;
    ramen_url : string }

exception InvalidCommand of string

(* Graph edition: only when stopped *)

let set_graph_editable graph =
  match graph.persist.status with
  | Edition -> ()
  | Compiled ->
    graph.persist.status <- Edition ;
    (* Also reset the info we kept from the last compilation *)
    Hashtbl.iter (fun _ node ->
       node.in_type <- make_temp_tup_typ () ;
       node.out_type <- make_temp_tup_typ () ;
       node.command <- None ;
       node.pid <- None) graph.persist.nodes
  | Running ->
    raise (InvalidCommand "Graph is running")

let parse_operation operation =
  let open RamenParsing in
  let what = Printf.sprintf "Parsing node operation %S" operation in
  Helpers.time what (fun () ->
    let p = Lang.(opt_blanks -+ Operation.Parser.p +- opt_blanks +- eof) in
    (* TODO: enable error correction *)
    match p ["operation"] None Parsers.no_error_correction (stream_of_string operation) |>
          to_result with
    | Bad e ->
      let err =
        IO.to_string (print_bad_result Lang.Operation.print) e in
      raise (Lang.SyntaxError ("Parse error: "^ err ^" while parsing:\n" ^ operation))
    | Ok (op, _) -> (* Since we force EOF, no need to keep what's left to parse *)
      op)

let make_node graph name layer op_text =
  !logger.debug "Creating node %s" name ;
  set_graph_editable graph ;
  let operation = parse_operation op_text in
  { name ; layer ;
    operation ; op_text ;
    parents = [] ; children = [] ;
    (* Set once the whole graph is known and reset each time the graph is
     * edited: *)
    in_type = make_temp_tup_typ () ; out_type = make_temp_tup_typ () ;
    command = None ; pid = None ; last_report = [] }

let make_graph ?persist () =
  let persist =
    Option.default_delayed (fun () ->
      { status = Edition ; nodes = Hashtbl.create 17 }) persist in
  { importing_threads = [] ;
    last_started = None ; last_stopped = None ;
    persist }

let load_graph save_file =
  let persist =
    match save_file with
    | None -> None
    | Some fname ->
      (try
        File.with_file_in fname (fun ic ->
          let persist = Marshal.input ic in
          if persist.status = Running then persist.status <- Compiled ;
          (* TODO: check the binaries are still there!
           * If so, then we should restart the graph. *)
          Some persist)
      with
      | Sys_error err ->
        !logger.debug "Cannot read state from file %S: %s. Starting anew" fname err ;
        None
      | BatInnerIO.No_more_input ->
        !logger.debug "Cannot read state from file %S: not enough input. Starting anew" fname ;
        None)
  in
  make_graph ?persist ()

let save_graph conf graph =
  Option.may (fun save_file ->
      !logger.debug "Saving graph in %S" save_file ;
      File.with_file_out ~mode:[`create; `trunc] save_file (fun oc ->
        Marshal.output oc graph.persist)
    ) conf.save_file

let has_node _conf graph id =
  Hashtbl.mem graph.persist.nodes id

let find_node _conf graph id =
  Hashtbl.find graph.persist.nodes id

let add_node conf graph node =
  assert (node.name <> "") ;
  if has_node conf graph node.name then
    raise (InvalidCommand ("Node "^ node.name ^" already exists")) ;
  Hashtbl.add graph.persist.nodes node.name node ;
  save_graph conf graph

let make_link conf graph src dst =
  !logger.debug "Create link between nodes %s and %s" src.name dst.name ;
  set_graph_editable graph ;
  src.children <- dst :: src.children ;
  dst.parents <- src :: dst.parents ;
  save_graph conf graph

let make_conf save_file ramen_url =
  { building_graph = load_graph save_file ; save_file ; ramen_url }


(* AutoCompletion of node/field names *)

(* Autocompletion of *all* nodes; not only exporting ones since we might want
 * to graph some meta data. Also maybe we should export on demand? *)

let complete_node_name conf s =
  let s = String.(lowercase (trim s)) in
  Hashtbl.fold (fun node_name _ lst ->
      (* TODO: a better search structure for case-insensitive prefix search *)
      if String.starts_with (String.lowercase node_name) s then
        node_name :: lst
      else lst
    ) conf.building_graph.persist.nodes []

let complete_field_name conf node_name s =
  match find_node conf conf.building_graph (String.trim node_name) with
  | exception Not_found -> []
  | node ->
    let s = String.(lowercase (trim s)) in
    Hashtbl.fold (fun field_name _ lst ->
        if String.starts_with (String.lowercase field_name) s then
          field_name :: lst
        else lst
      ) node.out_type.fields []
