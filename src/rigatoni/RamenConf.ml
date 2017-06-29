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
        Lang.Expr.make_typ ~nullable:f.Lang.Tuple.nullable
                           ~typ:f.Lang.Tuple.typ f.Lang.Tuple.name in
      Hashtbl.add t.fields f.Lang.Tuple.name (ref (Some i), expr_typ)
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
    { Tuple.name = typ.Expr.expr_name ;
      Tuple.nullable = Option.get typ.Expr.nullable ;
      Tuple.typ = Option.get typ.Expr.scalar_typ })

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
    mutable pid : int option }

type graph =
  { nodes : (string, node) Hashtbl.t ;
    mutable status : graph_status }

type conf =
  { building_graph : graph ;
    save_file : string }

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
       node.pid <- None) graph.nodes
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
    command = None ; pid = None }

let update_node graph node op_text =
  !logger.debug "Modifying node %s" node.name ;
  set_graph_editable graph ;
  let operation = parse_operation op_text in
  node.operation <- operation ;
  node.op_text <- op_text ;
  node.command <- None ; node.pid <- None

let make_new_graph () =
  { nodes = Hashtbl.create 17 ;
    status = Edition }

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

let add_node conf graph node =
  Hashtbl.add graph.nodes node.name node ;
  save_graph conf graph

let remove_node conf graph name =
  !logger.debug "Removing node %s" name ;
  set_graph_editable graph ;
  let node = Hashtbl.find graph.nodes name in
  List.iter (fun p ->
      p.children <- List.filter ((!=) node) p.children
    ) node.parents ;
  List.iter (fun p ->
      p.parents <- List.filter ((!=) node) p.parents
    ) node.children ;
  Hashtbl.remove_all graph.nodes name ;
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
  { building_graph = make_graph save_file ; save_file }

(*
 * Compilation of a graph
 *
 * We must first check all input/output tuple types.  For this we have two
 * temp_tuple_typ per node, one for input tuple and one for output tuple.  Each
 * check operation takes those as input and returns true if they completed any
 * of those.  Beware that those lists are completed bit by bit, since one
 * iteration of the loop might reveal only some info of some field.
 *
 * Once the fixed point is reached we check if we have all the fields we should
 * have.
 *
 * Types propagate to parents output to node input, then from operations to
 * node output, via the expected_type of each expression.
 *)

let can_cast ~from_scalar_type ~to_scalar_type =
  let compatible_types =
    match from_scalar_type with
    | TNum -> [ TU8 ; TU16 ; TU32 ; TU64 ; TU128 ; TI8 ; TI16 ; TI32 ; TI64 ; TI128 ; TFloat ]
    | TU8 -> [ TU8 ; TU16 ; TU32 ; TU64 ; TU128 ; TI16 ; TI32 ; TI64 ; TI128 ; TFloat ]
    | TU16 -> [ TU16 ; TU32 ; TU64 ; TU128 ; TI32 ; TI64 ; TI128 ; TFloat ]
    | TU32 -> [ TU32 ; TU64 ; TU128 ; TI64 ; TI128 ; TFloat ]
    | TU64 -> [ TU64 ; TU128 ; TI128 ; TFloat ]
    | TU128 -> [ TU128 ; TFloat ]
    | TI8 -> [ TI8 ; TI16 ; TI32 ; TI64 ; TI128 ; TU16 ; TU32 ; TU64 ; TU128 ; TFloat ]
    | TI16 -> [ TI16 ; TI32 ; TI64 ; TI128 ; TU32 ; TU64 ; TU128 ; TFloat ]
    | TI32 -> [ TI32 ; TI64 ; TI128 ; TU64 ; TU128 ; TFloat ]
    | TI64 -> [ TI64 ; TI128 ; TU128 ; TFloat ]
    | TI128 -> [ TI128 ; TFloat ]
    | x -> [ x ] in
  List.mem to_scalar_type compatible_types

(* Improve to rank with from *)
let check_rank ~from ~to_ =
  match !to_, !from with
  | None, Some _ ->
    to_ := !from ;
    true
  (* Contrary to type, once to_ is set it is better than from.
   * Example: "select 42, *" -> the star will add all fields from input into
   * output with a larger rank than on input. Then checking the rank again
   * would complain. *)
  | _ -> false

(* Improve to_ while checking compatibility with from.
 * Numerical types of to_ can be enlarged to match those of from. *)
let check_expr_type ~from ~to_ =
  let open Lang in
  let changed =
    match to_.Expr.scalar_typ, from.Expr.scalar_typ with
    | None, Some _ ->
      to_.Expr.scalar_typ <- from.Expr.scalar_typ ;
      true
    | Some to_typ, Some from_typ when to_typ <> from_typ ->
      if can_cast ~from_scalar_type:to_typ ~to_scalar_type:from_typ then (
        to_.Expr.scalar_typ <- from.Expr.scalar_typ ;
        true
      ) else (
        let m = Printf.sprintf "%s must have type %s but got %s of type %s"
                    to_.Expr.expr_name (IO.to_string Scalar.print_typ to_typ)
                    from.Expr.expr_name (IO.to_string Scalar.print_typ from_typ) in
        raise (SyntaxError m)
      )
    | _ -> false in
  let changed =
    match to_.Expr.nullable, from.Expr.nullable with
    | None, Some _ ->
      to_.Expr.nullable <- from.Expr.nullable ;
      true
    | Some to_null, Some from_null when to_null <> from_null ->
      let m = Printf.sprintf "%s must%s be nullable but %s is%s"
                to_.Expr.expr_name (if to_null then "" else " not")
                from.Expr.expr_name (if from_null then "" else " not") in
      raise (SyntaxError m)
    | _ -> changed in
  changed

(* Check that this expression fulfill the type expected by the caller (exp_type).
 * Also, improve exp_type (set typ and nullable, enlarge numerical types ...).
 * When we recurse from an operator to its operand we set the exp_type to the one
 * in the operator so we improve typing of the AST along the way. *)
let rec check_expr ~in_type ~out_type ~exp_type =
  let open Lang in
  let open Expr in
  let check_operand op_typ sub_typ ?exp_sub_typ ?exp_sub_nullable sub_expr=
    (* Start by recursing into the sub-expression to know its real type: *)
    let changed = check_expr ~in_type ~out_type ~exp_type:sub_typ sub_expr in
    (* Now we check this comply with the operator expectations about its operand : *)
    (match sub_typ.scalar_typ, exp_sub_typ with
    | Some actual_typ, Some exp_sub_typ ->
      if not (can_cast ~from_scalar_type:actual_typ ~to_scalar_type:exp_sub_typ) then
        let m = Printf.sprintf "Operand of %s is supposed to have type compatible with %s, not %s"
          op_typ.expr_name
          (IO.to_string Scalar.print_typ exp_sub_typ)
          (IO.to_string Scalar.print_typ actual_typ) in
        raise (SyntaxError m)
    | _ -> ()) ;
    (match exp_sub_nullable, sub_typ.nullable with
    | Some n1, Some n2 when n1 <> n2 ->
      let m = Printf.sprintf "Operand of %s is%s supposed to be NULLable"
        op_typ.expr_name (if n1 then "" else " not") in
      raise (SyntaxError m)
    | _ -> ()) ;
    changed
  in
  (* Check that actual_typ is a better version of op_typ and improve op_typ,
   * then check that the resulting op_type fulfill exp_type. *)
  let check_operator op_typ actual_typ nullable =
    let from = make_typ ~typ:actual_typ ?nullable op_typ.expr_name in
    let changed = check_expr_type ~from ~to_:op_typ in
    check_expr_type ~from:op_typ ~to_:exp_type || changed
  in
  let check_unary_op op_typ make_op_typ ?(propagate_null=true) ?exp_sub_typ ?exp_sub_nullable sub_expr =
    (* First we check the operand: does it comply with the expected type
     * (enlarging it if necessary)? *)
    let sub_typ = typ_of sub_expr in
    let changed = check_operand op_typ sub_typ ?exp_sub_typ ?exp_sub_nullable sub_expr in
    (* So far so good. So, given the type of the operand, what is the type of the operator? *)
    match sub_typ.scalar_typ with
    | Some sub_typ_typ ->
      let actual_typ = make_op_typ sub_typ_typ in
      (* We propagate nullability automatically for most operator *)
      let nullable =
        if propagate_null then sub_typ.nullable else None in
      (* Now check that this is OK with this operator type, enlarging it if required: *)
      check_operator op_typ actual_typ nullable || changed
    | None -> changed (* try again later *)
  in
  let check_binary_op op_typ make_op_typ ?(propagate_null=true)
                      ?exp_sub_typ1 ?exp_sub_nullable1 sub_expr1
                      ?exp_sub_typ2 ?exp_sub_nullable2 sub_expr2 =
    let sub_typ1 = typ_of sub_expr1 in
    let changed =
        check_operand op_typ sub_typ1 ?exp_sub_typ:exp_sub_typ1 ?exp_sub_nullable:exp_sub_nullable1 sub_expr1 in
    let sub_typ2 = typ_of sub_expr2 in
    let changed =
        check_operand op_typ sub_typ2 ?exp_sub_typ:exp_sub_typ2 ?exp_sub_nullable:exp_sub_nullable2 sub_expr2 || changed in
    match sub_typ1.scalar_typ, sub_typ2.scalar_typ with
    | Some sub_typ1_typ, Some sub_typ2_typ ->
      let actual_typ = make_op_typ (sub_typ1_typ, sub_typ2_typ) in
      let nullable = if propagate_null then
          match sub_typ1.nullable, sub_typ2.nullable with
          | Some true, _ | _, Some true -> Some true
          | Some false, Some false -> Some false
          | _ -> None
        else None in
      check_operator op_typ actual_typ nullable || changed
    | _ -> changed
  in
  (* Useful helpers for make_op_typ above: *)
  let return_bool _ = TBool
  and return_float _ = TFloat
  and return_i128 _ = TI128
  in
  function
  | Const (op_typ, _) ->
    (* op_typ is already optimal. But is it compatible with exp_type? *)
    check_expr_type ~from:op_typ ~to_:exp_type
  | Field (op_typ, tuple, field) ->
    if same_tuple_as_in !tuple then (
      (* Check that this field is, or could be, in in_type *)
      match Hashtbl.find in_type.fields field with
      | exception Not_found ->
        !logger.debug "Cannot find field %s in in-tuple" field ;
        if in_type.complete then (
          if Hashtbl.mem out_type.fields field then (
            !logger.debug "Field %s appears to belongs to out!" field ;
            tuple := "out" ;
            true
          ) else if out_type.complete then (
            let m = Printf.sprintf "field %s not in %S tuple" field !tuple in
            raise (SyntaxError m)
          ) else false
        ) else false
      | _, from ->
        if in_type.complete then ( (* Save the type *)
          op_typ.nullable <- from.nullable ;
          op_typ.scalar_typ <- from.scalar_typ
        ) ;
        check_expr_type ~from ~to_:exp_type
    ) else if !tuple = "out" then (
      (* If we already have this field in out then check it's compatible (or
       * enlarge out or exp). If we don't have it then add it. *)
      match Hashtbl.find out_type.fields field with
      | exception Not_found ->
        !logger.debug "Cannot find field %s in out-tuple" field ;
        if out_type.complete then (
          let m = Printf.sprintf "field %s not in %S tuple" field !tuple in
          raise (SyntaxError m)) ;
        Hashtbl.add out_type.fields field (ref None, exp_type) ;
        true
      | _, out ->
        if out_type.complete then ( (* Save the type *)
          op_typ.nullable <- out.nullable ;
          op_typ.scalar_typ <- out.scalar_typ
        ) ;
        check_expr_type ~from:out ~to_:exp_type
    ) else (
      let m = Printf.sprintf "unknown tuple %S" !tuple in
      raise (SyntaxError m)
    )
  | Param (_op_typ, _pname) ->
    (* TODO: one day we will know the type or value of params *)
    false
  | AggrMin (op_typ, e) | AggrMax (op_typ, e)
  | AggrFirst (op_typ, e) | AggrLast (op_typ, e) ->
    check_unary_op op_typ identity e
  | AggrSum (op_typ, e) | AggrAnd (op_typ, e)
  | AggrOr (op_typ, e) | Age (op_typ, e)
  | Not (op_typ, e) ->
    check_unary_op op_typ identity ~exp_sub_typ:TFloat e
  | Defined (op_typ, e) ->
    check_unary_op op_typ return_bool ~exp_sub_nullable:true ~propagate_null:false e
  | AggrPercentile (op_typ, e1, e2) ->
    check_binary_op op_typ snd ~exp_sub_typ1:TFloat e1 ~exp_sub_typ2:TFloat e2
  | Add (op_typ, e1, e2) | Sub (op_typ, e1, e2)
  | Mul (op_typ, e1, e2) | IDiv (op_typ, e1, e2)
  | Exp (op_typ, e1, e2) ->
    check_binary_op op_typ Scalar.larger_type ~exp_sub_typ1:TFloat e1 ~exp_sub_typ2:TFloat e2
  | Sequence (op_typ, e1, e2) ->
    check_binary_op op_typ return_i128 ~exp_sub_typ1:TI128 e1 ~exp_sub_typ2:TI128 e2
  | Ge (op_typ, e1, e2) | Gt (op_typ, e1, e2)
  | Eq (op_typ, e1, e2) ->
    check_binary_op op_typ return_bool ~exp_sub_typ1:TFloat e1 ~exp_sub_typ2:TFloat e2
  | Div (op_typ, e1, e2) ->
    check_binary_op op_typ return_float ~exp_sub_typ1:TU128 e1 ~exp_sub_typ2:TU128 e2
  | And (op_typ, e1, e2) | Or (op_typ, e1, e2) ->
    check_binary_op op_typ return_bool ~exp_sub_typ1:TBool e1 ~exp_sub_typ2:TBool e2

(* Given two tuple types, transfer all fields from the parent to the child,
 * while checking those already in the child are compatible.
 * If autorank is true, do not try to reuse from rank but add them instead.
 * This is meant to be used when transferring input to output due to "select *"
 *)
let check_inherit_tuple ~including_complete ~is_subset ~from_tuple ~to_tuple ~autorank =
  let max_rank fields =
    Hashtbl.fold (fun _ (rank, _) max_rank ->
      match !rank with
      | None -> max_rank
      | Some r -> max max_rank r) fields ~-1 (* start at -1 so that max+1 starts at 0 *)
  in
  (* Check that to_tuple is included in from_tuple (is is_subset) and if so
   * that they are compatible. Improve child type using parent type. *)
  let changed =
    Hashtbl.fold (fun n (child_rank, child_field) changed ->
        match Hashtbl.find from_tuple.fields n with
        | exception Not_found ->
          if is_subset && from_tuple.complete then (
            let m = Printf.sprintf "Unknown field %s" n in
            raise (Lang.SyntaxError m)) ;
          changed (* no-op *)
        | parent_rank, parent_field ->
          let c1 = check_expr_type ~from:parent_field ~to_:child_field
          and c2 = check_rank ~from:parent_rank ~to_:child_rank in
          c1 || c2 || changed
      ) to_tuple.fields false in
  (* Add new fields into children. *)
  let changed =
    Hashtbl.fold (fun n (parent_rank, parent_field) changed ->
        match Hashtbl.find to_tuple.fields n with
        | exception Not_found ->
          if to_tuple.complete then (
            let m = Printf.sprintf "Field %s is not in output tuple" n in
            raise (Lang.SyntaxError m)) ;
          let copy = Lang.Expr.copy_typ parent_field in
          let rank =
            if autorank then ref (Some (max_rank to_tuple.fields + 1))
            else ref !parent_rank in
          Hashtbl.add to_tuple.fields n (rank, copy) ;
          true
        | _ ->
          changed (* We already checked those types above. All is good. *)
      ) from_tuple.fields changed in
  (* If from_tuple is complete then so is to_tuple *)
  let changed =
    if including_complete && from_tuple.complete && not to_tuple.complete then (
      !logger.debug "Completing to_tuple from check_inherit_tuple" ;
      to_tuple.complete <- true ;
      true
    ) else changed in
  changed

let check_selected_fields ~in_type ~out_type fields =
  let open Lang in
  List.fold_lefti (fun changed i sf ->
      changed || (
        let name = List.hd sf.Operation.alias in
        let exp_type =
          match Hashtbl.find out_type.fields name with
          | exception Not_found ->
            let expr_typ = Expr.make_typ name in
            !logger.debug "Adding out field %s" name ;
            Hashtbl.add out_type.fields name (ref (Some i), expr_typ) ;
            expr_typ
          | rank, exp_typ ->
            if !rank = None then rank := Some i ;
            exp_typ in
        check_expr ~in_type ~out_type ~exp_type sf.Operation.expr)
    ) false fields

let check_yield ~in_type ~out_type fields =
  (
    check_selected_fields ~in_type ~out_type fields
  ) || (
    (* If nothing changed so far then we are done *)
    if not out_type.complete then (
      !logger.debug "Completing out_type because it won't change any more." ;
      out_type.complete <- true ;
      true
    ) else false
  )

let check_select ~in_type ~out_type fields and_all_others where =
  (
    (* Check the expression, improving out_type and checking against in_type: *)
    let exp_type =
      (* That where expressions cannot be null seems a nice improvement
       * over SQL. *)
      Lang.Expr.make_bool_typ ~nullable:false "where clause" in
    check_expr ~in_type ~out_type ~exp_type where
  ) || (
    (* Also check other expression and make use of them to improve out_type.
     * Everything that's selected must be (added) in out_type. *)
    check_selected_fields ~in_type ~out_type fields
  ) || (
    (* If all other fields are selected, add them *)
    if and_all_others then (
      check_inherit_tuple ~including_complete:false ~is_subset:false ~from_tuple:in_type ~to_tuple:out_type ~autorank:true
    ) else false
  ) || (
    (* If nothing changed so far and our input is complete, then our output is. *)
    if in_type.complete && not out_type.complete then (
      !logger.debug "Completing out_type because it won't change any more." ;
      out_type.complete <- true ;
      true
    ) else false
  )

let check_aggregate ~in_type ~out_type fields and_all_others
                    where key commit_when flush_when =
  let open Lang in
  (
    (* Improve out_type using all expressions. Check we satisfy in_type. *)
    List.fold_left (fun changed k ->
        (* The key can be anything *)
        let exp_type = Expr.typ_of k in
        check_expr ~in_type ~out_type ~exp_type k || changed
      ) false key
  ) || (
    let exp_type = Expr.make_bool_typ ~nullable:false "commit-when clause" in
    check_expr ~in_type ~out_type ~exp_type commit_when
  ) || (
    match flush_when with
    | None -> false
    | Some flush_when ->
      let exp_type = Expr.make_bool_typ ~nullable:false "flush-when clause" in
      check_expr ~in_type ~out_type ~exp_type flush_when
  ) || (
    (* Check select must come last since it completes out_type *)
    check_select ~in_type ~out_type fields and_all_others where
  )

(*
 * Improve out_type using in_type and this node operation.
 * in_type is a given, don't modify it!
 *)
let check_operation ~in_type ~out_type =
  let open Lang in
  function
  | Operation.Yield fields ->
    check_yield ~in_type ~out_type fields
  | Operation.Select { fields ; and_all_others ; where } ->
    check_select ~in_type ~out_type fields and_all_others where
  | Operation.Aggregate { fields ; and_all_others ; where ;
                          key ; commit_when ; flush_when } ->
    check_aggregate ~in_type ~out_type fields and_all_others where
                    key commit_when flush_when
  | Operation.OnChange expr ->
    (
      (* Start by transmitting the field so that the expression can
       * sooner use out tuple: *)
      check_inherit_tuple ~including_complete:true ~is_subset:true ~from_tuple:in_type ~to_tuple:out_type ~autorank:false
    ) || (
      (* Then check the expression: *)
      let exp_type =
        Expr.make_bool_typ ~nullable:false "on-change clause" in
      check_expr ~in_type ~out_type ~exp_type expr
    )
  | Operation.Alert _ ->
    check_inherit_tuple ~including_complete:true ~is_subset:true ~from_tuple:in_type ~to_tuple:out_type ~autorank:false
  | Operation.ReadCSVFile { fields ; _ } ->
    let from_tuple = temp_tup_typ_of_tup_typ true fields in
    check_inherit_tuple ~including_complete:true ~is_subset:true ~from_tuple ~to_tuple:out_type ~autorank:false

(*
 * Type inference for the graph
 *)

let check_node_types node =
  try ( (* Prepend the node name to any SyntaxError *)
    (* Try to improve the in_type using the out_type of parents: *)
    (
      if node.in_type.complete then false
      else if node.parents = [] then (
        !logger.debug "Completing node %s in-type since we have no parents" node.name ;
        node.in_type.complete <- true ; true
      ) else List.fold_left (fun changed par ->
            (* This is supposed to propagate parent completeness into in-tuple. *)
            check_inherit_tuple ~including_complete:true ~is_subset:true ~from_tuple:par.out_type ~to_tuple:node.in_type ~autorank:false || changed
          ) false node.parents
    ) || (
    (* Try to improve out_type and the AST types using the in_type and the
     * operation: *)
      check_operation ~in_type:node.in_type ~out_type:node.out_type node.operation
    )
  ) with Lang.SyntaxError e ->
    !logger.debug "Compilation error: %s, %s"
      e (Printexc.get_backtrace ()) ;
    let e' = Printf.sprintf "node %S: %s" node.name e in
    raise (Lang.SyntaxError e')

let node_is_complete node =
  node.in_type.complete && node.out_type.complete

let set_all_types graph =
  let rec loop pass =
    if pass < 0 then (
      let bad_nodes =
        Hashtbl.values graph.nodes //
        (fun n -> not (node_is_complete n)) in
      let print_bad_node fmt node =
        Printf.fprintf fmt "%s: %a"
          node.name
          (List.print ~sep:" and " ~first:"" ~last:"" String.print)
            ((if node.in_type.complete then [] else ["cannot type input"]) @
            (if node.out_type.complete then [] else ["cannot type output"])) in
      let msg = IO.to_string (Enum.print ~sep:", " print_bad_node) bad_nodes in
      raise (Lang.SyntaxError msg)) ;
    if Hashtbl.fold (fun _ node changed ->
          check_node_types node || changed
        ) graph.nodes false
    then loop (pass - 1)
  in
  let max_pass = 50 (* TODO: max number of field for a node times number of nodes? *) in
  loop max_pass
  (* TODO:
   * - check that input type empty <=> no parents
   * - check that output type empty <=> no children
   * - Move all typing in a separate module
   *)

  (*$inject
    let test_type_single_node op_text =
      try
        let conf = make_conf false "/dev/null" in
        let graph = conf.building_graph in
        let node = make_node graph "test" op_text in
        add_node conf graph node ;
        set_all_types graph ;
        "ok"
      with e ->
        Printf.sprintf "Exception in set_all_types: %s at\n%s"
          (Printexc.to_string e)
          (Printexc.get_backtrace ())
   *)

  (*$= & ~printer:BatPervasives.identity
     "ok" (test_type_single_node "SELECT 1-1 AS x")
     "ok" (test_type_single_node "SELECT 1-200 AS x")
     "ok" (test_type_single_node "SELECT 1-4000000000 AS x")
     "ok" (test_type_single_node "SELECT SEQUENCE AS x")
     "ok" (test_type_single_node "SELECT 0 AS zero, 1 AS one, SEQUENCE AS seq")
   *)

let compile_node node =
  assert node.in_type.complete ;
  assert node.out_type.complete ;
  let in_typ = tup_typ_of_temp_tup_type node.in_type
  and out_typ = tup_typ_of_temp_tup_type node.out_type in
  node.command <- Some (
    CodeGen_OCaml.gen_operation node.name in_typ out_typ node.operation)

let compile conf graph =
  match graph.status with
  | Compiled ->
    raise (InvalidCommand "Graph is already compiled")
  | Running ->
    raise (InvalidCommand "Graph is already compiled and is running")
  | Edition ->
    set_all_types graph ;
    let complete =
      Hashtbl.fold (fun _ node complete ->
          !logger.debug "node %S:\n\tinput type: %a\n\toutput type: %a\n\n"
            node.name
            print_temp_tup_typ node.in_type
            print_temp_tup_typ node.out_type ;
          complete && node_is_complete node
        ) graph.nodes true in
    (* TODO: better reporting *)
    if not complete then raise (Lang.SyntaxError "Cannot complete typing") ;
    Hashtbl.iter (fun _ node ->
        try compile_node node
        with Failure m ->
          raise (Failure ("While compiling "^ node.name ^": "^ m))
      ) graph.nodes ;
    graph.status <- Compiled ;
    save_graph conf graph

let run_background cmd args env =
  let open Unix in
  (* prog name should be first arg *)
  let prog_name = Filename.basename cmd in
  let args = Array.init (Array.length args + 1) (fun i ->
      if i = 0 then prog_name else args.(i-1))
  in
  !logger.info "Running %s with args %a and env %a"
    cmd
    (Array.print String.print) args
    (Array.print String.print) env ;
  match fork () with
  | 0 -> execve cmd args env
  | pid -> pid
    (* TODO: A monitoring thread that report the error in the node structure *)

let run conf graph =
  match graph.status with
  | Edition ->
    raise (InvalidCommand "Cannot run if not compiled")
  | Running ->
    raise (InvalidCommand "Graph is already running")
  | Compiled ->
    (* For now each node creates its own output ringbuf itself but we still have
     * to set the names so that we can pass it to its children. *)
    Hashtbl.iter (fun _ node ->
        let command = Option.get node.command
        and rb_name_of node = "/tmp/ringbuf_"^ node.name ^"_in" in
        let env = [|
          "input_ringbuf="^ rb_name_of node ;
          "output_ringbufs="^ String.concat "," (List.map rb_name_of node.children)
        |] in
        node.pid <- Some (run_background command [||] env)
      ) graph.nodes ;
    graph.status <- Running ;
    save_graph conf graph

let string_of_process_status = function
  | Unix.WEXITED code -> Printf.sprintf "terminated with code %d" code
  | Unix.WSIGNALED sign -> Printf.sprintf "killed by signal %d" sign
  | Unix.WSTOPPED sign -> Printf.sprintf "stopped by signal %d" sign

let stop conf graph =
  match graph.status with
  | Edition | Compiled ->
    raise (InvalidCommand "Graph is not running")
  | Running ->
    Hashtbl.iter (fun _ node ->
        !logger.debug "Stopping node %s" node.name ;
        match node.pid with
        | None ->
          !logger.error "Node %s has no pid?!" node.name
        | Some pid ->
          let open Unix in
          (try kill pid Sys.sigterm
          with Unix_error _ -> ()) ;
          (try
            let _, status = restart_on_EINTR (waitpid []) pid in
            !logger.info "Node %s %s"
              node.name (string_of_process_status status) ;
           with exn ->
            !logger.error "Cannot wait for pid %d: %s"
              pid (Printexc.to_string exn)) ;
          node.pid <- None
      ) graph.nodes ;
    graph.status <- Compiled ;
    save_graph conf graph
