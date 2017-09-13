(* Global configuration *)
open Batteries
open RamenLog
open RamenSharedTypes
module SL = RamenSharedTypes.Layer

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

type history =
  { (* Store arrays of Scalar.values not hash of names to values !
     * TODO: ideally storing scalar_columns would be even better *)
    tuples : scalar_value array array ;
    (* Gives us both the position of the last tuple in the array and an index
     * in the stream of tuples to help polling (once added to this block seqnum) *)
    mutable count : int ;
    dir : string ;
    (* A cache to save first/last timestamps of each archive file we've visited. *)
    ts_cache : (int, float * float) Hashtbl.t ;
    mutable min_filenum : int ; (* Not necessarily up to date but gives a lower bound *)
    mutable max_filenum : int }

module Node =
struct
  type t =
    { (* Nodes are added/removed to/from the graph in group called layers.
       * Layers can connect to nodes from any other layers and non existing
       * nodes, but there is still the notion of layers "depending" (or lying on)
       * others; we must prevent change of higher level layers to restart lower
       * level layers, while update of a low level layers can trigger the
       * recompilation of upper layers.  The idea is that there are some
       * ephemeral layers that answer specific queries on top of more fundamental
       * layers that compute generally useful data, a bit like functions calling
       * each others.
       * Of course to compile a layer, all layers it depends on must have been
       * compiled. *)
      layer : string ;
      (* within a layer, nodes are identified by a name that can be optionally
       * provided automatically if its not meant to be referenced. *)
      name : string ;
      (* Parsed operation and its in/out types: *)
      mutable operation : Lang.Operation.t ;
      mutable in_type : temp_tup_typ ;
      mutable out_type : temp_tup_typ ;
      (* The signature identifies the operation and therefore the binary.
       * It does not identifies a node! Only layer name + node name identifies
       * a node. Indeed, it is frequent that different nodes in the graph have
       * the same signature (they perform the same operation, but with a
       * different internal state and different environment (ie. different
       * ringbufs and different parameters).
       * This field is computed as soon as the node is typed, and is otherwise
       * empty. *)
      mutable signature : string ;
      (* Also keep the string as defined by the client to preserve formatting: *)
      mutable op_text : string ;
      (* Parents are either in this layer or a layer _below_. *)
      mutable parents : t list ;
      (* Children are either in this layer or in a layer _above_ *)
      mutable children : t list ;
      (* List of tuples exported from that node: *)
      (* FIXME: a pretty bad name *)
      mutable history : history option ;
      (* Worker info, only relevant if it is running: *)
      mutable pid : int option ;
      mutable last_report : Binocle.metric list }

  let fq_name node = node.layer ^"/"^ node.name

  let make_name =
    let seq = ref 0 in
    fun () ->
      incr seq ;
      string_of_int !seq

  (* We need the conf because we want to add in the signature the version of
   * ramen that generated those binaries. *)
  let signature version_tag node =
    (* We'd like to be formatting independent so that operation text can be
     * reformatted without ramen recompiling it. For this it is not OK to
     * strip redundant white spaces as some of those might be part of literal
     * string values. So we print it, trusting the printer to be exhaustive.
     * This is not enough to print the expression with types, as those do not
     * contain relevant info such as field rank. We therefore print without
     * types and encode input/output types explicitly below: *)
    "OP="^ IO.to_string Lang.Operation.print node.operation ^
    "IN="^ type_signature node.in_type ^
    "OUT="^ type_signature node.out_type ^
    "V="^ version_tag |>
    Cryptohash_md4.string |>
    Cryptohash_md4.to_hex
end

exception InvalidCommand of string

module Layer =
struct
  type persist =
    { nodes : (string, Node.t) Hashtbl.t ;
      (* How long can this layer can stays without dependent nodes before it's
       * reclaimed. Set to 0 for no timeout. *)
      timeout : float ;
      mutable last_used : float ;
      mutable status : SL.status ;
      mutable last_status_change : float ;
      mutable last_started : float option ;
      mutable last_stopped : float option }

  type t =
    { name : string ;
      persist : persist ;
      mutable importing_threads : unit Lwt.t list }

  let set_status layer status =
    layer.persist.status <- status ;
    layer.persist.last_status_change <- Unix.gettimeofday ()

  let make ?persist ?(timeout=0.) name =
    let persist =
      let now = Unix.gettimeofday () in
      Option.default_delayed (fun () ->
        { nodes = Hashtbl.create 17 ;
          timeout ; last_used = now ;
          status = SL.Edition ;
          last_status_change = now ;
          last_started = None ; last_stopped = None }) persist in
    let layer = { name ; persist ; importing_threads = [] } in
    (* Downgrade the status to compiled since the workers can't be running
     * anymore.  TODO: check the binaries are still there or downgrade to
     * edition. *)
    if persist.status = SL.Running then set_status layer SL.Compiled ;
    (* FIXME: also, as a precaution, delete any temporary layer (maybe we
     * crashed because of it? *)
    layer

  let is_typed layer =
    match layer.persist.status with
    | SL.Edition | SL.Compiling -> false
    | SL.Compiled | SL.Running -> true

  (* Layer edition: only when stopped *)
  let set_editable layer =
    match layer.persist.status with
    | SL.Edition -> ()
    | SL.Compiling ->
      (* FIXME: rather discard the compilation, and change the compiler to
       * check the status in between compilations and before changing any value
       * (needs a mutex.) *)
      raise (InvalidCommand "Graph is compiling")
    | SL.Compiled ->
      set_status layer SL.Edition ;
      (* Also reset the info we kept from the last compilation *)
      Hashtbl.iter (fun _ node ->
        let open Node in
        node.in_type <- make_temp_tup_typ () ;
        node.out_type <- make_temp_tup_typ () ;
        node.pid <- None) layer.persist.nodes
    | SL.Running ->
      raise (InvalidCommand "Graph is running")

  let iter_dependencies layer f =
    (* One day we will have a lock on the configuration and we will be able to
     * mark visited nodes *)
    Hashtbl.fold (fun _node_name node called ->
        List.fold_left (fun called parent ->
            let dependency = parent.Node.layer in
            if Set.mem dependency called then called else (
              f dependency ;
              Set.add dependency called)
          ) called node.Node.parents
      ) layer.persist.nodes (Set.singleton layer.name) |>
    ignore

  (* Order layers according to dependency, depended upon first. *)
  let order layers =
    let rec loop ordered = function
      | [] -> List.rev ordered
      | layers ->
        let progress, ordered, later =
          List.fold_left (fun (progress, ordered, later) l ->
              try
                iter_dependencies l (fun dep ->
                  if not (List.exists (fun o -> o.name = dep) ordered) then
                    raise Exit) ;
                true, l::ordered, later
              with Exit ->
                progress, ordered, l::later
            ) (false, ordered, []) layers in
        if not progress then raise (InvalidCommand "Dependency loop") ;
        loop ordered later
    in
    loop [] layers
end

type graph =
  { layers : (string, Layer.t) Hashtbl.t }

type persisted = (string, Layer.persist) Hashtbl.t

type conf =
  { mutable graph : graph ;
    debug : bool ;
    ramen_url : string ;
    version_tag : string ;
    persist_dir : string ;
    do_persist : bool }

let parse_operation operation =
  let open RamenParsing in
  let p = Lang.(opt_blanks -+ Operation.Parser.p +- opt_blanks +- eof) in
  (* TODO: enable error correction *)
  match p ["operation"] None Parsers.no_error_correction (stream_of_string operation) |>
        to_result with
  | Bad e ->
    let err =
      IO.to_string (print_bad_result Lang.Operation.print) e in
    raise (Lang.SyntaxError ("Parse error: "^ err ^" while parsing:\n" ^ operation))
  | Ok (op, _) -> (* Since we force EOF, no need to keep what's left to parse *)
    op

let add_layer ?timeout conf name =
  let layer = Layer.make ?timeout name in
  Hashtbl.add conf.graph.layers name layer ;
  layer

let save_file_of persist_dir =
  persist_dir ^"/configuration/1" (* TODO: versioning *)

let save_graph conf =
  if conf.do_persist then
    let persist =
      Hashtbl.map (fun _ l -> l.Layer.persist) conf.graph.layers in
    let save_file = save_file_of conf.persist_dir in
    !logger.debug "Saving graph in %S" save_file ;
    Helpers.mkdir_all ~is_file:true save_file ;
    File.with_file_out ~mode:[`create; `trunc] save_file (fun oc ->
      Marshal.output oc persist)

(* Store history of past tuple output by a given node: *)
let history_block_length = 10000 (* TODO: make it a parameter? *)
(* We use filenum * max_history_block_length + index as a cursor *)
let max_history_block_length = 1000000
let max_history_archives = 200

let make_history dir =
  Helpers.mkdir_all ~is_file:false dir ;
  (* Note: this is OK to share this [||] since we use it only as a placeholder *)
  let tuples = Array.make history_block_length [||] in
  let min_filenum, max_filenum =
    Sys.readdir dir |>
    Array.fold_left (fun (mi, ma as prev) fname ->
      match int_of_string fname with
      | exception _ -> prev
      | n -> min mi n, max ma n) (max_int, min_int) in
  let min_filenum, max_filenum =
    if min_filenum > max_filenum then -1, -1
    else min_filenum, max_filenum in
  !logger.debug "History files from %d to %d" min_filenum max_filenum ;
  { tuples ; count = 0 ; dir ; min_filenum ; max_filenum ;
    ts_cache = Hashtbl.create (max_history_archives / 8) }

let add_parsed_node ?timeout conf node_name layer_name op_text operation =
  let layer =
    try Hashtbl.find conf.graph.layers layer_name
    with Not_found -> add_layer ?timeout conf layer_name in
  if Hashtbl.mem layer.Layer.persist.Layer.nodes node_name then
    raise (InvalidCommand (
             "Node "^ node_name ^" already exists in layer "^ layer_name)) ;
  let history =
    if Lang.Operation.is_exporting operation then (
      let dir = conf.persist_dir ^"/workers/"^ layer_name ^"/"^
                node_name ^"/history" in
      Some (make_history dir)
    ) else None in
  let node = Node.{
    layer = layer_name ; name = node_name ;
    operation ; signature = "" ; op_text ;
    parents = [] ; children = [] ; history ;
    (* Set once the whole graph is known and reset each time the graph is
     * edited: *)
    in_type = make_temp_tup_typ () ; out_type = make_temp_tup_typ () ;
    pid = None ; last_report = [] } in
  Layer.set_editable layer ;
  Hashtbl.add layer.Layer.persist.Layer.nodes node_name node ;
  layer

let add_node conf node_name layer_name op_text =
  !logger.debug "Creating node %s / %s" layer_name node_name ;
  (* New lines have to be forbidden because of the out_ref ringbuf files.
   * slashes have to be forbidden because we rsplit to get layer names. *)
  if node_name = "" ||
     String.fold_left (fun bad c ->
       bad || c = '\n' || c = '\r' || c = '/') false node_name then
    invalid_arg "node name" ;
  assert (node_name <> "") ;
  let operation = parse_operation op_text in
  let _layer = add_parsed_node conf node_name layer_name op_text operation in
  (* FIXME: Delay this with a dirty flag, and save_if_dirty after every
   * HTTP query *)
  save_graph conf

let exec_of_node conf node =
  conf.persist_dir ^"/workers/bin/"^ node.Node.signature

let make_graph ?persist () =
  let persist =
    Option.default_delayed (fun () -> Hashtbl.create 11) persist in
  { layers = Hashtbl.map (fun name persist ->
               Layer.make ~persist name) persist }

let load_graph do_persist persist_dir =
  let save_file = save_file_of persist_dir in
  let persist : persisted option =
    if do_persist then
      try
        File.with_file_in save_file (fun ic ->
          let persist = Marshal.input ic in
          Some persist)
      with
      | e ->
        !logger.error "Cannot read state from file %S: %s. Starting anew."
          save_file (Printexc.to_string e) ;
        None
    else None
  in
  make_graph ?persist ()

let find_node conf layer name =
  let layer = Hashtbl.find conf.graph.layers layer in
  layer, Hashtbl.find layer.Layer.persist.Layer.nodes name

(* Both src and dst have to exist already. *)
let add_link conf src dst =
  let open Node in
  !logger.debug "Create link between nodes %s/%s and %s/%s"
    src.layer src.name dst.layer dst.name ;
  (* Leave the src layer as it is if <> dst layer, since we must not
   * have to recompile an underlying layer. The dependent dst, though, must
   * be recompiled to take into account this (possible) change in it's input
   * type. *)
  Layer.set_editable (Hashtbl.find conf.graph.layers dst.layer) ;
  src.children <- dst :: src.children ;
  dst.parents <- src :: dst.parents ;
  save_graph conf

let make_conf do_persist ramen_url debug version_tag persist_dir =
  { graph = load_graph do_persist persist_dir ; do_persist ;
    ramen_url ; debug ; version_tag ; persist_dir }

(* AutoCompletion of node/field names *)

(* Autocompletion of *all* nodes; not only exporting ones since we might want
 * to graph some meta data. Also maybe we should export on demand? *)

let fold_nodes conf init f =
  Hashtbl.fold (fun _ layer prev ->
      Hashtbl.fold (fun _ node prev -> f prev node) layer.Layer.persist.Layer.nodes prev
    ) conf.graph.layers init

let complete_node_name conf s =
  let s = String.(lowercase (trim s)) in
  (* TODO: a better search structure for case-insensitive prefix search *)
  fold_nodes conf [] (fun lst node ->
      let lc_name = String.lowercase node.Node.name in
      let fq_name = Node.fq_name node in
      let lc_fq_name = String.lowercase fq_name in
      if String.(starts_with lc_fq_name s || starts_with lc_name s) then
        fq_name :: lst
      else lst
    )

let complete_field_name conf name s =
  (* rsplit because we might want to have '/'s in the layer name. *)
  match String.rsplit ~by:"/" (String.trim name) with
  | exception Not_found -> []
  | layer_name, node_name ->
    match find_node conf layer_name node_name with
    | exception Not_found -> []
    | _layer, node ->
      let s = String.(lowercase (trim s)) in
      Hashtbl.fold (fun field_name _ lst ->
          if String.starts_with (String.lowercase field_name) s then
            field_name :: lst
          else lst
        ) node.Node.out_type.fields []
