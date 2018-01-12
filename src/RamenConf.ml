open Batteries
open RamenLog
open Helpers
open RamenSharedTypes
open RamenSharedTypesJS
open AlerterSharedTypesJS

(* Used to type the input/output of nodes. Of course a compiled/
 * running node must have finished_typing to true and all optional
 * values set, but we keep that type even for typed nodes so that
 * the typing code, which has to use both typed and untyped nodes,
 * has to deal with only one case. We will sometime Option.get those
 * values when we know the node is typed.
 * The other tuple type, Lang.Tuple.typ, is used to describe tuples
 * outside of this context (for instance, when describing a CSV or other
 * serialization format). *)
(* FIXME: rename this type *)
type temp_tup_typ =
  { mutable finished_typing : bool ;
    mutable fields : (string * Lang.Expr.typ) List.t }

let print_temp_tup_typ_fields fmt fs =
  List.print ~first:"{" ~last:"}" ~sep:", "
    (fun fmt (name, expr_typ) ->
      Printf.fprintf fmt "%s: %a"
        name
        Lang.Expr.print_typ expr_typ) fmt fs

let print_temp_tup_typ fmt t =
  Printf.fprintf fmt "%a (%s)"
    print_temp_tup_typ_fields t.fields
    (if t.finished_typing then "finished typing" else "to be typed")

let temp_tup_typ_copy t =
  { t with fields =
      List.map (fun (name, typ) -> name, Lang.Expr.copy_typ typ) t.fields }

type tuple_type = UntypedTuple of temp_tup_typ
                | TypedTuple of { user : field_typ list ;
                                  ser : field_typ list }

let tuple_is_typed = function
  | TypedTuple _ -> true
  | UntypedTuple _ -> false

let print_tuple_type fmt = function
  | UntypedTuple temp_tup_typ ->
      print_temp_tup_typ fmt temp_tup_typ
  | TypedTuple { user ; _ } ->
      Lang.Tuple.print_typ fmt user

exception BadTupleTypedness of string
let typed_tuple_type = function
  | TypedTuple { user ; ser } -> user, ser
  | UntypedTuple _ ->
      raise (BadTupleTypedness "Node should be typed by now!")

let untyped_tuple_type = function
  | TypedTuple _ ->
      raise (BadTupleTypedness "This node should not be typed!")
  | UntypedTuple temp_tup_typ -> temp_tup_typ

let tuple_ser_type = snd % typed_tuple_type
let tuple_user_type = fst % typed_tuple_type

let type_signature tuple_type =
  let ser = tuple_ser_type tuple_type in
  List.fold_left (fun s field ->
      (if s = "" then "" else s ^ "_") ^
      field.typ_name ^ ":" ^
      Lang.Scalar.string_of_typ field.typ ^
      if field.nullable then " null" else " notnull"
    ) "" ser

let md4 s =
  Cryptohash_md4.string s |> Cryptohash_md4.to_hex

let type_signature_hash = md4 % type_signature

let make_temp_tup_typ () =
  { finished_typing = false ; fields = [] }

let finish_typing t =
  t.finished_typing <- true

let temp_tup_typ_of_tup_typ tup_typ =
  let t = make_temp_tup_typ () in
  List.iter (fun f ->
      let expr_typ =
        Lang.Expr.make_typ ~nullable:f.nullable
                           ~typ:f.typ f.typ_name in
      t.fields <- t.fields @ [f.typ_name, expr_typ]
    ) tup_typ ;
  finish_typing t ;
  t

let info_of_tuple_type = function
  | UntypedTuple ttt ->
      (* Is it really useful to send unfinished types? *)
      List.map (fun (name, typ) ->
          { name_info = name ;
            nullable_info = typ.Lang.Expr.nullable ;
            typ_info = typ.scalar_typ }) ttt.fields
  | TypedTuple { user ; _ } ->
      List.filter_map (fun f ->
          if is_private_field f.typ_name then None else Some (
            { name_info = f.typ_name ;
              nullable_info = Some f.nullable ;
              typ_info = Some f.typ })) user

let temp_tup_typ_of_tuple_type = function
  | UntypedTuple temp_tup_typ -> temp_tup_typ
  | TypedTuple { ser ; _ } -> temp_tup_typ_of_tup_typ ser

let tup_typ_of_temp_tup_type ttt =
  let open Lang in
  assert ttt.finished_typing ;
  List.map (fun (name, typ) ->
    { typ_name = name ;
      nullable = Option.get typ.Expr.nullable ;
      typ = Option.get typ.Expr.scalar_typ }) ttt.fields

let archive_file dir (block_start, block_stop) =
  dir ^"/"^ string_of_int block_start ^"-"^ string_of_int block_stop

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
      mutable in_type : tuple_type ;
      mutable out_type : tuple_type ;
      (* The signature identifies the operation and therefore the binary.
       * It does not identifies a node! Only layer name + node name identifies
       * a node. Indeed, it is frequent that different nodes in the graph have
       * the same signature; they perform the same operation, but with a
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
      (* Worker info, only relevant if it is running: *)
      mutable pid : int option }

  let fq_name node = node.layer ^"/"^ node.name

  let make_name =
    let seq = ref 0 in
    fun () ->
      incr seq ;
      string_of_int !seq

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
    md4
end

let exec_of_node persist_dir node =
  persist_dir ^"/workers/bin/worker_"^ node.Node.signature

let tmp_input_of_node persist_dir node =
  persist_dir ^"/workers/inputs/"^ Node.fq_name node ^"/"
              ^ type_signature node.Node.in_type

let upload_dir_of_node persist_dir node =
  tmp_input_of_node persist_dir node ^"/uploads"

exception InvalidCommand of string

module Layer =
struct
  type persist =
    { nodes : (string, Node.t) Hashtbl.t ;
      (* How long can this layer can stays without dependent nodes before it's
       * reclaimed. Set to 0 for no timeout. *)
      timeout : float ;
      mutable last_used : float ;
      mutable status : layer_status ;
      mutable last_status_change : float ;
      mutable last_started : float option ;
      mutable last_stopped : float option }

  type t =
    { name : string ;
      persist : persist ;
      mutable importing_threads : unit Lwt.t list }

  let set_status layer status =
    !logger.debug "Layer %s status %s -> %s"
      layer.name
      (Layer.string_of_status status)
      (Layer.string_of_status layer.persist.status) ;
    layer.persist.status <- status ;
    layer.persist.last_status_change <- Unix.gettimeofday () ;
    (* If we are not running, clean pid info *)
    if status <> Running then
      Hashtbl.iter (fun _ n -> n.Node.pid <- None) layer.persist.nodes ;
    (* If we are now in Edition _untype_ the nodes *)
    match status with Edition _ ->
      Hashtbl.iter (fun _ n ->
        let open Node in
        n.in_type <- UntypedTuple (make_temp_tup_typ ()) ;
        n.out_type <- UntypedTuple (make_temp_tup_typ ()) ;
        n.pid <- None) layer.persist.nodes
    | _ -> ()

  let make persist_dir version_tag ?persist ?(timeout=0.) ?(restart=false)
           name =
    assert (String.length name > 0) ;
    let persist =
      let now = Unix.gettimeofday () in
      Option.default_delayed (fun () ->
        { nodes = Hashtbl.create 17 ;
          timeout ; last_used = now ;
          status = Edition "" ;
          last_status_change = now ;
          last_started = None ; last_stopped = None }) persist in
    let layer = { name ; persist ; importing_threads = [] } in
    if restart then (
      (* Demote the status to compiled since the workers can't be running
       * anymore. *)
      if persist.status = Running then set_status layer Compiled ;
      (* Recompute the node signatures if the version_tag changed, so that
       * we won't reuse former paths: *)
      Hashtbl.iter (fun name node ->
        let new_sign =
          try Node.signature version_tag node
          with BadTupleTypedness _ -> "" in
        if node.Node.signature <> new_sign then (
          !logger.debug "Node %s is from a previous version" name ;
          node.signature <- new_sign)) persist.nodes ;
      (* Further demote to edition if the binaries are not there anymore
       * (which will be the case if we just updated the node signatures): *)
      if persist.status = Compiled &&
         Hashtbl.values persist.nodes |> Enum.exists (fun n ->
           not (file_exists ~has_perms:0o100 (exec_of_node persist_dir n)))
      then set_status layer (Edition "") ;
      (* Also, we cannot be compiling anymore: *)
      if persist.status = Compiling then set_status layer (Edition "") ;
      (* FIXME: also, as a precaution, delete any temporary layer (maybe we
       * crashed because of it? *)) ;
    layer

  let is_typed layer =
    match layer.persist.status with
    | Edition _ | Compiling -> false
    | Compiled | Running -> true

  (* Layer edition: only when stopped *)
  let set_editable layer reason =
    match layer.persist.status with
    | Edition _ ->
      (* Update the error message *)
      if reason <> "" then
        set_status layer (Edition reason)
    | Compiling ->
      (* FIXME: rather discard the compilation, and change the compiler to
       * check the status in between compilations and before changing any value
       * (needs a mutex.) *)
      raise (InvalidCommand "Graph is compiling")
    | Compiled ->
      set_status layer (Edition reason) ;
    | Running ->
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
                  !logger.debug "Layer %S depends on %S" l.name dep ;
                  let in_list = List.exists (fun o -> o.name = dep) in
                  if in_list layers && not (in_list ordered) then
                    raise Exit) ;
                true, l::ordered, later
              with Exit ->
                !logger.debug "Will do %S later" l.name ;
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

module Alerter =
struct
  (* The part of the internal state that we persist on disk: *)
  type t =
    { (* Ongoing incidents stays there until they are manually closed (with
        * comment, reason, etc...) *)
      ongoing_incidents : (int, Incident.t) Hashtbl.t ;

      (* Used to assign an id to any escalation so that we can ack them. *)
      mutable next_alert_id : int ;

      (* Same for incidents: *)
      mutable next_incident_id : int ;

      (* And inhibitions: *)
      mutable next_inhibition_id : int ;

      mutable static : StaticConf.t }

  let save_file persist_dir alerting_version =
    persist_dir ^"/alerting/"^ alerting_version

  (* TODO: write using Lwt *)
  let save_state persist_dir alerting_version state =
    let fname = save_file persist_dir alerting_version in
    mkdir_all ~is_file:true fname ;
    !logger.debug "Saving state in file %S." fname ;
    File.with_file_out ~mode:[`create; `trunc] fname (fun oc ->
      Marshal.output oc state)

  let get_state do_persist persist_dir alerting_version =
    let get_new () =
      { ongoing_incidents = Hashtbl.create 5 ;
        next_alert_id = 0 ;
        next_incident_id = 0 ;
        next_inhibition_id = 0 ;
        static =
          { oncallers = [ OnCaller.{ name = "John Doe" ;
                                     contacts = [| Contact.Console |] } ] ;
            teams = [
              Team.{ name = "firefighters" ;
                     members = [ "John Doe" ] ;
                     escalations = [
                       { importance = 0 ;
                         steps = Escalation.[
                           { victims = [| 0 |] ; timeout = 350. } ;
                           { victims = [| 0; 1 |] ; timeout = 350. } ] } ] ;
                     inhibitions = [] } ] ;
            default_team = "firefighters" ;
            schedule =
              [ { rank = 0 ; from = 0. ; oncaller = "John Doe" } ] } }
    in
    if do_persist then (
      let fname = save_file persist_dir alerting_version in
      mkdir_all ~is_file:true fname ;
      try
        File.with_file_in fname (fun ic -> Marshal.input ic)
      with Sys_error err ->
        !logger.debug "Cannot read state from file %S: %s. Starting anew."
            fname err ;
        get_new ()
      | BatInnerIO.No_more_input ->
        !logger.debug "Cannot read state from file %S: not enough input. Starting anew."
           fname ;
        get_new ()
    ) else get_new ()
end

type conf =
  { mutable graph : graph ;
    graph_lock : RWLock.t ; (* Protects the above graph *)
    alerts : Alerter.t ;
    (* TODO: use the RWLock and forget about that dirty flag: *)
    alerts_lock : RWLock.t ; (* Protects the above alerts *)
    (* TODO: a file *)
    mutable archived_incidents : Incident.t list ;

    debug : bool ;
    ramen_url : string ;
    version_tag : string ;
    alerting_version : string ;
    instrumentation_version : string ;
    persist_dir : string ;
    do_persist : bool ; (* false for tests *)
    max_simult_compilations : int ref ;
    max_history_archives : int }

let parse_operation operation =
  let open RamenParsing in
  let p = Lang.(opt_blanks -+ Operation.Parser.p +- opt_blanks +- eof) in
  (* TODO: enable error correction *)
  match p ["operation"] None Parsers.no_error_correction (stream_of_string operation) |>
        to_result with
  | Bad e ->
    let error =
      IO.to_string (print_bad_result Lang.Operation.print) e in
    let open Lang in
    raise (SyntaxError (ParseError { error ; text = operation }))
  | Ok (op, _) -> (* Since we force EOF, no need to keep what's left to parse *)
    Lang.Operation.check op ;
    op

let add_layer ?timeout conf name =
  let layer = Layer.make conf.persist_dir conf.version_tag ?timeout name in
  Hashtbl.add conf.graph.layers name layer ;
  layer

let del_layer conf layer =
  let open Layer in
  !logger.info "Deleting layer %S" layer.name ;
  if layer.persist.status = Running then
    raise (InvalidCommand "Layer is running") ;
  if layer.importing_threads <> [] then
    raise (InvalidCommand "Layer has running threads") ;
  (* OK so now we should be able to restart on error *)
  (* Remove the nodes and links *)
  Hashtbl.iter (fun _ node ->
      let open Node in
      List.iter (fun parent ->
          parent.children <-
            List.filter (fun c -> c != node) parent.children
        ) node.parents ;
      List.iter (fun child ->
          if child.layer <> layer.name then
            !logger.info "Node %S will miss layer %S"
              (Node.fq_name child) layer.name ;
          child.parents <-
            List.filter (fun p -> p != node) child.parents
        ) node.children
    ) layer.persist.nodes ;
  Hashtbl.remove conf.graph.layers layer.name

let save_file_of persist_dir =
  persist_dir ^"/configuration/1" (* TODO: versioning *)

let add_parsed_node ?timeout conf node_name layer_name op_text operation =
  let layer =
    try Hashtbl.find conf.graph.layers layer_name
    with Not_found -> add_layer ?timeout conf layer_name in
  if Hashtbl.mem layer.Layer.persist.Layer.nodes node_name then
    raise (InvalidCommand (
             "Node "^ node_name ^" already exists in layer "^ layer_name)) ;
  let node = Node.{
    layer = layer_name ; name = node_name ;
    operation ; signature = "" ; op_text ;
    parents = [] ; children = [] ;
    (* Set once the whole graph is known and reset each time the graph is
     * edited: *)
    in_type = UntypedTuple (make_temp_tup_typ ()) ;
    out_type = UntypedTuple (make_temp_tup_typ ()) ;
    pid = None } in
  Hashtbl.add layer.Layer.persist.Layer.nodes node_name node ;
  layer, node

(* Create the node but not the links to parents (this is so we can have
 * loops) *)
let add_node conf node_name layer_name op_text =
  !logger.debug "Creating node %s/%s" layer_name node_name ;
  (* New lines have to be forbidden because of the out_ref ringbuf files.
   * slashes have to be forbidden because we rsplit to get layer names. *)
  if node_name = "" ||
     String.fold_left (fun bad c ->
       bad || c = '\n' || c = '\r' || c = '/') false node_name then
    invalid_arg "node name" ;
  assert (node_name <> "") ;
  let operation = parse_operation op_text in
  add_parsed_node conf node_name layer_name op_text operation

let make_graph persist_dir version_tag ?persist ?restart () =
  let persist =
    Option.default_delayed (fun () -> Hashtbl.create 11) persist in
  { layers = Hashtbl.map (fun name persist ->
               Layer.make persist_dir version_tag
                          ~persist ?restart name) persist }

let load_graph ?restart do_persist persist_dir version_tag =
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
  make_graph persist_dir version_tag ?persist ?restart ()

let save_graph conf =
  if conf.do_persist then
    let persist =
      Hashtbl.map (fun _ l ->
        l.Layer.persist) conf.graph.layers in
    let save_file = save_file_of conf.persist_dir in
    !logger.debug "Saving graph in %S" save_file ;
    mkdir_all ~is_file:true save_file ;
    File.with_file_out ~mode:[`create; `trunc] save_file (fun oc ->
      Marshal.output oc persist)

let reload_graph conf =
  conf.graph <- load_graph ~restart:false conf.do_persist conf.persist_dir
                           conf.version_tag

let with_rlock conf f =
  RWLock.with_r_lock conf.graph_lock (fun () ->
    !logger.debug "Took graph lock (read)" ;
    reload_graph conf ;
    let%lwt x = f () in
    !logger.debug "Release graph lock (read)" ;
    Lwt.return x)

let with_wlock conf f =
  RWLock.with_w_lock conf.graph_lock (fun () ->
    !logger.debug "Took graph lock (write)" ;
    reload_graph conf ;
    let%lwt res = f () in
    (* Save the config only if f did not fail: *)
    save_graph conf ;
    !logger.debug "Release graph lock (write)" ;
    Lwt.return res)

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
  let l = Hashtbl.find conf.graph.layers dst.layer in
  Layer.set_editable l "" ;
  src.children <- dst :: src.children ;
  dst.parents <- src :: dst.parents

let make_conf do_persist ramen_url debug version_tag persist_dir
              max_simult_compilations max_history_archives =
  let alerting_version = "v0" and instrumentation_version = "v1" in
  { graph = load_graph ~restart:true do_persist persist_dir version_tag ;
    graph_lock = RWLock.make () ; alerts_lock = RWLock.make () ;
    alerts = Alerter.get_state do_persist persist_dir alerting_version ;
    archived_incidents = [] ;
    alerting_version ; instrumentation_version ;
    do_persist ; ramen_url ; debug ; version_tag ; persist_dir ;
    max_simult_compilations = ref max_simult_compilations ;
    max_history_archives }

(* AutoCompletion of node/field names *)

(* Autocompletion of *all* nodes; not only exporting ones since we might want
 * to graph some meta data. Also maybe we should export on demand? *)

let fold_nodes conf init f =
  Hashtbl.fold (fun _ layer prev ->
      Hashtbl.fold (fun _ node prev -> f prev node) layer.Layer.persist.Layer.nodes prev
    ) conf.graph.layers init

let complete_node_name conf s only_exporting =
  let s = String.(lowercase (trim s)) in
  (* TODO: a better search structure for case-insensitive prefix search *)
  fold_nodes conf [] (fun lst node ->
      let lc_name = String.lowercase node.Node.name in
      let fq_name = Node.fq_name node in
      let lc_fq_name = String.lowercase fq_name in
      if String.(starts_with lc_fq_name s || starts_with lc_name s) &&
         (not only_exporting || Lang.Operation.is_exporting node.Node.operation)
      then fq_name :: lst
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
      (match node.Node.out_type with
      | UntypedTuple _ -> []
      (* Avoid private fields by looking only at ser: *)
      | TypedTuple { ser ; _ } ->
        let s = String.(lowercase (trim s)) in
        List.fold_left (fun lst field ->
            let n = field.typ_name in
            if String.starts_with (String.lowercase n) s then
              n :: lst
            else lst
          ) [] ser)
