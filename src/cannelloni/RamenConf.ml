(* Global configuration *)
open Batteries
open Log
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
      mutable signature : string ; (* Lazily computed signature *)
      (* Also keep the string as defined by the client to preserve formatting: *)
      mutable op_text : string ;
      (* Parents are either in this layer or a layer _below_. *)
      mutable parents : t list ;
      (* Children are either in this layer or in a layer _above_ *)
      mutable children : t list ;
      (* Worker info, only relevant if it is running: *)
      mutable command : string option ;
      mutable pid : int option ;
      mutable last_report : Binocle.metric list }

  let fq_name node = node.layer ^"/"^ node.name

  let make_name =
    let seq = ref 0 in
    fun () ->
      incr seq ;
      string_of_int !seq

  let signature =
    let blanks = Str.regexp "[ \n\t\b\r]\\+" in
    fun node ->
      let op_text = Str.global_replace blanks " " node.op_text in
      "OP="^ op_text ^
      "IN="^ type_signature node.in_type ^
      "OUT="^ type_signature node.out_type |>
      Cryptohash_md4.string |>
      Cryptohash_md4.to_hex
end

exception InvalidCommand of string

module Layer =
struct
  type persist =
    { nodes : (string, Node.t) Hashtbl.t ;
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

  let make ?persist name =
    let persist =
      Option.default_delayed (fun () ->
        { nodes = Hashtbl.create 17 ;
          status = SL.Edition ;
          last_status_change = Unix.gettimeofday () ;
          last_started = None ; last_stopped = None }) persist in
    let layer = { name ; persist ; importing_threads = [] } in
    (* downgrade the status to compiled since the workers can't be running
     * anymore.  TODO: check the binaries are still there or downgrade to
     * edition. *)
    if persist.status = SL.Running then set_status layer SL.Compiled ;
    layer

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
        node.command <- None ;
        node.pid <- None) layer.persist.nodes
    | SL.Running ->
      raise (InvalidCommand "Graph is running")
end

type graph =
  { layers : (string, Layer.t) Hashtbl.t }

type persisted = (string, Layer.persist) Hashtbl.t

type conf =
  { mutable graph : graph ;
    debug : bool ;
    save_file : string option ;
    ramen_url : string }

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

let add_layer conf name =
  let layer = Layer.make name in
  Hashtbl.add conf.graph.layers name layer ;
  layer

let save_graph conf =
  let persist =
    Hashtbl.map (fun _ l -> l.Layer.persist) conf.graph.layers in
  Option.may (fun save_file ->
      !logger.debug "Saving graph in %S" save_file ;
      File.with_file_out ~mode:[`create; `trunc] save_file (fun oc ->
        Marshal.output oc persist)
    ) conf.save_file

let add_node conf node_name layer_name op_text =
  !logger.debug "Creating node %s / %s" layer_name node_name ;
  assert (node_name <> "") ;
  let layer =
    try Hashtbl.find conf.graph.layers layer_name
    with Not_found -> add_layer conf layer_name in
  if Hashtbl.mem layer.Layer.persist.Layer.nodes node_name then
    raise (InvalidCommand (
             "Node "^ node_name ^" already exists in layer "^ layer_name)) ;
  let operation = parse_operation op_text in
  let node = Node.{
    layer = layer_name ; name = node_name ;
    operation ; signature = "" ; op_text ;
    parents = [] ; children = [] ;
    (* Set once the whole graph is known and reset each time the graph is
     * edited: *)
    in_type = make_temp_tup_typ () ; out_type = make_temp_tup_typ () ;
    command = None ; pid = None ; last_report = [] } in
  Layer.set_editable layer ;
  Hashtbl.add layer.Layer.persist.Layer.nodes node_name node ;
  (* FIXME: Delay this with a dirty flag, and save_if_dirty after every
   * HTTP query *)
  save_graph conf

let make_graph ?persist () =
  let persist =
    Option.default_delayed (fun () -> Hashtbl.create 11) persist in
  { layers = Hashtbl.map (fun name persist ->
               Layer.make ~persist name) persist }

let load_graph save_file =
  let persist : persisted option =
    match save_file with
    | None -> None
    | Some fname ->
      (try
        File.with_file_in fname (fun ic ->
          let persist = Marshal.input ic in
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

let find_node conf layer name =
  let layer = Hashtbl.find conf.graph.layers layer in
  Hashtbl.find layer.Layer.persist.Layer.nodes name

(* Both src and dst have to exist already. *)
let make_link conf src dst =
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

let make_conf save_file ramen_url debug =
  { graph = load_graph save_file ; save_file ; ramen_url ; debug }


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
    | node ->
      let s = String.(lowercase (trim s)) in
      Hashtbl.fold (fun field_name _ lst ->
          if String.starts_with (String.lowercase field_name) s then
            field_name :: lst
          else lst
        ) node.Node.out_type.fields []
