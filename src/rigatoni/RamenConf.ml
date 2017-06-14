(* Global configuration for rigatoni daemon *)
open Batteries
open Log

type temp_field_typ =
  { name : string ;
    mutable rank : int option ; (* not sure we need to update them ever *)
    mutable nullable : bool option ;
    mutable typ : Lang.Scalar.typ option } [@@ppp PPP_JSON]

let print_temp_field_typ fmt t =
  Printf.fprintf fmt "[%s] %s%s"
    (match t.rank with
    | None -> "??"
    | Some d -> string_of_int d)
    (match t.nullable with
    | None -> "maybe nullable "
    | Some true -> "nullable "
    | Some false -> "")
    (match t.typ with
    | None -> "unknown type"
    | Some typ -> IO.to_string Lang.Scalar.print_typ typ)

let make_temp_field_typ ?rank ?nullable ?typ name =
  { name ; nullable ; rank ; typ }

type temp_tup_typ =
  { mutable complete : bool ;
    fields : (string, temp_field_typ) Hashtbl.t }

let print_temp_tup_typ fmt t =
  Printf.fprintf fmt "%a (%s)"
    (Hashtbl.print ~first:"{" ~last:"}" ~sep:", " ~kvsep:":"
                   String.print print_temp_field_typ) t.fields
    (if t.complete then "complete" else "incomplete")

let make_temp_tup_typ () =
  { complete = false ;
    fields = Hashtbl.create 7 }

let temp_tup_typ_of_tup_typ complete tup_typ =
  let t = make_temp_tup_typ () in
  t.complete <- complete ;
  List.iteri (fun i f ->
      let temp_field =
        { name = f.Lang.Tuple.name ;
          rank = Some i ;
          nullable = Some f.Lang.Tuple.nullable ;
          typ = Some f.Lang.Tuple.typ } in
      Hashtbl.add t.fields f.Lang.Tuple.name temp_field
    ) tup_typ ;
  t

let list_of_temp_tup_type ttt =
  Hashtbl.values ttt.fields |>
  List.of_enum |>
  List.fast_sort (fun f1 f2 -> compare f1.rank f2.rank)

let tup_typ_of_temp_tup_type ttt =
  let open Lang in
  assert ttt.complete ;
  list_of_temp_tup_type ttt |>
  List.map (fun tf ->
    { Tuple.name = tf.name ;
      Tuple.nullable = Option.get tf.nullable ;
      Tuple.typ = Option.get tf.typ })

type node =
  { name : string ;
    operation : Lang.Operation.t ;
    mutable parents : node list ;
    mutable children : node list ;
    mutable in_type : temp_tup_typ ;
    mutable out_type : temp_tup_typ ;
    mutable command : string }

type graph =
  { nodes : (string, node) Hashtbl.t }

type conf =
  { building_graph : graph ;
    save_file : string }

let make_node name operation =
  !logger.debug "Creating node %s" name ;
  { name ; operation ; parents = [] ; children = [] ;
    (* Set once the all graph is known: *)
    in_type = make_temp_tup_typ () ; out_type = make_temp_tup_typ () ;
    command = "" }

let compile_node node =
  assert node.in_type.complete ;
  assert node.out_type.complete ;
  let in_typ = tup_typ_of_temp_tup_type node.in_type in
  node.command <-
    CodeGen_OCaml.gen_operation node.name in_typ node.operation

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

exception CompilationError of string

let can_cast ~from_scalar_type ~to_scalar_type =
  let open Lang.Scalar in
  let compatible_types =
    match from_scalar_type with
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

(* Improve to_field while checking compatibility with from_field *)
let check_field ~from_field ~to_field =
  let open Lang in
  let changed =
    match to_field.typ, from_field.typ with
    | None, Some _ -> to_field.typ <- from_field.typ ; true
    | Some to_typ, Some from_typ when to_typ <> from_typ ->
      if can_cast ~from_scalar_type:to_typ ~to_scalar_type:from_typ then (
        to_field.typ <- from_field.typ ; true
      ) else (
        let m = Printf.sprintf "%s must have type %s but got %s of type %s"
                    to_field.name (IO.to_string Scalar.print_typ to_typ)
                    from_field.name (IO.to_string Scalar.print_typ from_typ) in
        raise (CompilationError m)
      )
    | _ -> false in
  let changed =
    match to_field.nullable, from_field.nullable with
    | None, Some _ -> to_field.nullable <- from_field.nullable ; true
    | Some to_null, Some from_null when to_null <> from_null ->
      let m = Printf.sprintf "%s must%s be nullable but %s is%s"
                to_field.name (if to_null then "" else " not")
                from_field.name (if from_null then "" else " not") in
      raise (CompilationError m)
    | _ -> changed in
  let changed =
    match to_field.rank, from_field.rank with
    | None, Some _ -> to_field.rank <- from_field.rank ; true
    (* Contrary to type, to_field is authoritative for the rank *)
    | _ -> changed in
  changed

(* Add all types from from_type into to_type. Check that those already in
 * to_type are compatible. We must never copy the fields from in to out because
 * some fields are editable while others must not be changed, therefore we
 * never transfer fields here. *)
let check_add_type ~autorank ~including_complete ~from_type ~to_type =
  let max_rank fields =
    Hashtbl.fold (fun _ f max_rank ->
      match f.rank with
      | None -> max_rank
      | Some r -> max max_rank r) fields 0 in
  let changed =
    Hashtbl.fold (fun n from_field changed ->
        match Hashtbl.find to_type.fields n with
        | exception Not_found ->
          let from_field_copy =
            { from_field with
              rank = if autorank then Some (max_rank to_type.fields)
                     else from_field.rank } in
          Hashtbl.add to_type.fields n from_field_copy ;
          true
        | to_field ->
          check_field ~from_field ~to_field || changed
      ) from_type.fields false in
  if including_complete && from_type.complete then (
    to_type.complete <- true ; true
  ) else changed

let check_against_in_typ = function
  | "in" | "others" | "any" -> true
  | _ -> false

(* Check that the scalar type we have is compatible with the expected_type.  We
 * may want to improve/enlarge exp_type, unless it's complete *)
let check_scalar ~exp_type field_type =
  check_field ~from_field:field_type ~to_field:exp_type

(* Check that this expression fulfill the exp_type. Also, improve exp_type.
 * For instance if it's not set, set it to the narrowest type possible.
 * It it is set too narrow, enlarge it. *)
let rec check_expr ~in_type ~out_type ~exp_type =
  let open Lang in
  let check_unary_op what make_typ sub_typ sub_expr =
    (* We should check that the sub expression comply with the expected type
     * (enlarging it if necessary), and then determine the type of the global
     * operation as a function of the enlarged type of the operand, and check
     * that it is compatible with the expected type. *)
    let sub_exp_type = make_temp_field_typ ~typ:sub_typ ("argument of "^ what) in
    (* Note: we expect the sub_exp_type to be changed since we always try a
     * constant one. We care if the operator itself changed exp_type. *)
    let _ = check_expr ~in_type ~out_type ~exp_type:sub_exp_type sub_expr in
    let typ = make_typ (Option.get sub_exp_type.typ) in
    check_scalar ~exp_type (make_temp_field_typ ?nullable:sub_exp_type.nullable
                                                ~typ what)
  in
  let check_binary_op what make_typ sub_typ1 sub_typ2 sub_expr1 sub_expr2 =
    !logger.debug "check_binary_op for %s" what ;
    let sub_exp_type1 =
      make_temp_field_typ ~typ:sub_typ1 ("first argument of "^ what)
    and sub_exp_type2 =
      make_temp_field_typ ~typ:sub_typ2 ("second argument of "^ what) in
    let _ = check_expr ~in_type ~out_type ~exp_type:sub_exp_type1 sub_expr1 in
    !logger.debug "... first arg check! (field_type=%a)"
      print_temp_field_typ sub_exp_type1 ;
    let _ = check_expr ~in_type ~out_type ~exp_type:sub_exp_type2 sub_expr2 in
    !logger.debug "... second arg check! (field_type=%a)"
      print_temp_field_typ sub_exp_type2 ;
    let typ = make_typ (Option.get sub_exp_type1.typ,
                        Option.get sub_exp_type2.typ) in
    let nullable = match sub_exp_type1.nullable, sub_exp_type2.nullable with
                   | Some true, _ | _, Some true -> Some true
                   | Some false, Some false -> Some false
                   | _ -> None in
    let changed =
      check_scalar ~exp_type (make_temp_field_typ ?nullable ~typ what) in
    !logger.debug "... global operation check! (changed=%b, exp_type=%a)"
      changed print_temp_field_typ exp_type ;
    changed
  in
  (* Useful helpers for make_typ above: *)
  let larger_type (t1, t2) =
    if Scalar.compare_typ t1 t2 >= 0 then t1 else t2 in
  function
  | Expr.Const v ->
    let typ = Scalar.type_of v in
    check_scalar ~exp_type (make_temp_field_typ ~nullable:false ~typ "constant")
  | Expr.Field { tuple ; field } ->
    if check_against_in_typ tuple then (
      (* Check that this field is, or could be, in in_type *)
      match Hashtbl.find in_type.fields field with
      | exception Not_found ->
        if in_type.complete then (
          let m = Printf.sprintf "field %s not in %S tuple" field tuple in
          raise (CompilationError m)) ;
        false
      | in_field ->
        check_scalar ~exp_type in_field
    ) else if tuple = "out" then (
      (* If we already have this field in out then check it's compatible (or
       * enlarge out or exp). If we don't, add it. *)
      match Hashtbl.find out_type.fields field with
      | exception Not_found ->
        assert (not out_type.complete) ;
        Hashtbl.add out_type.fields field { exp_type with name = field } ;
        true
      | out_field ->
        check_scalar ~exp_type out_field
    ) else (
      let m = Printf.sprintf "unknown tuple %S" tuple in
      raise (CompilationError m)
    )
  | Expr.Param _pname ->
    (* TODO: one day we will know the definition of params *)
    false
  | Expr.AggrFun (Expr.AggrMin e) ->
    (* Try to be conservative about int types since they will be promoted when
     * required. *)
    check_unary_op "min aggregation function" identity Scalar.TI8 e
  | Expr.AggrFun (Expr.AggrMax e) ->
    check_unary_op "max aggregation function" identity Scalar.TI8 e
  | Expr.AggrFun (Expr.AggrSum e) ->
    check_unary_op "sum aggregation function" identity Scalar.TI8 e
  | Expr.AggrFun (Expr.AggrAnd e) ->
    check_unary_op "and aggregation function" identity Scalar.TI8 e
  | Expr.AggrFun (Expr.AggrOr e) ->
    check_unary_op "or aggregation function" identity Scalar.TI8 e
  | Expr.AggrFun (Expr.AggrPercentile (e1, e2)) ->
    check_binary_op "percentile aggregation function" snd Scalar.TI8 Scalar.TI8 e1 e2
  | Expr.Age e ->
    check_unary_op "age function" identity Scalar.TFloat e
  | Expr.Not e ->
    check_unary_op "logical not" identity Scalar.TBool e
  | Expr.Defined e ->
    (* This one is special: it takes anything nullable as an argument and the
     * result is always a non null boolean. Notice that we will get an error if
     * we try to test null on a non-nullable thing, which is nice I think. *)
    let sub_exp_type =
      make_temp_field_typ ~nullable:true "argument of is not null function" in
    let _ = check_expr ~in_type ~out_type ~exp_type:sub_exp_type e in
    let field_type = make_temp_field_typ ~nullable:false ~typ:Scalar.TBool
                                         "is not null function" in
    check_scalar ~exp_type field_type
  | Expr.Add (e1, e2) ->
    check_binary_op "addition" larger_type Scalar.TI8 Scalar.TI8 e1 e2
  | Expr.Sub (e1, e2) ->
    check_binary_op "subtraction" larger_type Scalar.TI8 Scalar.TI8 e1 e2
  | Expr.Mul (e1, e2) ->
    check_binary_op "multiplication" larger_type Scalar.TI8 Scalar.TI8 e1 e2
  | Expr.Div (e1, e2) ->
    check_binary_op "division" larger_type Scalar.TI8 Scalar.TI8 e1 e2
  | Expr.Exp (e1, e2) ->
    check_binary_op "exponentiation" larger_type Scalar.TI8 Scalar.TI8 e1 e2
  | Expr.And (e1, e2) ->
    check_binary_op "logical and" fst Scalar.TBool Scalar.TBool e1 e2
  | Expr.Or (e1, e2) ->
    check_binary_op "logical or" fst Scalar.TBool Scalar.TBool e1 e2
  | Expr.Ge (e1, e2) ->
    check_binary_op "greater or equal operator" larger_type Scalar.TI8 Scalar.TI8 e1 e2
  | Expr.Gt (e1, e2) ->
    check_binary_op "greater operator" larger_type Scalar.TI8 Scalar.TI8 e1 e2
  | Expr.Eq (e1, e2) ->
    check_binary_op "equal operator" larger_type Scalar.TI8 Scalar.TI8 e1 e2

(* Improve out_type but do not touch in_type which is a given *)
let check_operation ~in_type ~out_type =
  let open Lang in
  let make_bool_exp_type name =
    { name ; rank = None ;
      nullable = Some false ; typ = Some Scalar.TBool } in
  function
  | Operation.Select { fields ; and_all_others ; where } ->
    (* First, improve out_type using all expressions. Check we satisfy in_type. *)
    let changed =
      let exp_type = make_bool_exp_type "where clause" in
      check_expr ~in_type ~out_type ~exp_type where in
    (* If we have set out_type to some small type because of the where expression
     * above, we may need to enlarge it now *)
    let changed =
      List.fold_lefti (fun changed i selfield ->
          let name = List.hd selfield.Operation.alias in
          let exp_type =
            match Hashtbl.find out_type.fields name with
            | exception Not_found ->
              let x = { name ; rank = Some i (* This is certain *) ;
                        nullable = None ; typ = None } in
              Hashtbl.add out_type.fields name x ;
              x
            | x -> x in
          check_expr ~in_type ~out_type ~exp_type selfield.Operation.expr || changed
        ) changed fields in
    (* Then if all other fields are selected, add them *)
    let changed =
      if and_all_others then (
        check_add_type ~including_complete:false ~autorank:true ~from_type:in_type ~to_type:out_type || changed
      ) else changed in
    changed
  | Operation.Aggregate _ -> failwith "TODO: check_operation Aggregate"
  | Operation.OnChange expr ->
    let changed =
      check_add_type ~autorank:false ~including_complete:true ~from_type:in_type ~to_type:out_type in
    let exp_type = make_bool_exp_type "on-change clause" in
    check_expr ~in_type ~out_type ~exp_type expr || changed
  | Operation.Alert _ ->
    check_add_type ~autorank:false ~including_complete:true ~from_type:in_type ~to_type:out_type
  | Operation.ReadCSVFile { fields ; _ } ->
    check_add_type ~autorank:false
                   ~including_complete:true
                   ~from_type:(temp_tup_typ_of_tup_typ true fields)
                   ~to_type:out_type

(*
 * Types propagation from out to in
 *)

let check_merge_type ~from_type ~to_type =
  assert (not to_type.complete) ;
  (* Check that to_type is included in from_type *)
  let changed =
    Hashtbl.fold (fun n to_field changed ->
        match Hashtbl.find from_type.fields n with
        | exception Not_found ->
          if from_type.complete then (
            let m = Printf.sprintf "Unknown field %s" n in
            raise (CompilationError m)) ;
          changed
        | from_field ->
          check_field ~from_field ~to_field
      ) to_type.fields false in
  (* Add new from_fields into to_fields. Notice that from_fields and to_fields
   * can have different types as long as we can cast from from_field to
   * to_field. *)
  let changed =
    Hashtbl.fold (fun n from_field changed ->
        match Hashtbl.find to_type.fields n with
        | exception Not_found ->
          Hashtbl.add to_type.fields n from_field ; true
        | _to_field ->
          changed (* We already checked the types above. All is good. *)
      ) from_type.fields changed in
  (* If from_type is complete then so is to_type *)
  let changed =
    if from_type.complete then (
      to_type.complete <- true ;
      true
    ) else changed in
  changed

(*
 * Type inference for the graph
 *)

let check_node_types node =
  try ( (* Prepend the node name to any CompilationError *)
    (* Try to improve the in_type using the out_type of parents: *)
    let changed =
      if node.in_type.complete then false
      else if node.parents = [] then (
        node.in_type.complete <- true ; true
      ) else List.fold_left (fun changed par ->
            check_merge_type ~from_type:par.out_type ~to_type:node.in_type || changed
          ) false node.parents in
    (* Now try to improve out_type using the in_type and the operation: *)
    let changed =
      if node.out_type.complete then changed else (
        check_operation ~in_type:node.in_type ~out_type:node.out_type node.operation ||
        changed
      ) in
    changed
  ) with CompilationError e ->
    let e' = Printf.sprintf "node %S: %s" node.name e in
    raise (CompilationError e')

let set_all_types graph =
  let rec loop pass =
    if pass < 0 then (
      let msg = "Cannot type" in (* TODO *)
      raise (CompilationError msg)) ;
    if Hashtbl.fold (fun _ node changed ->
          check_node_types node || changed
        ) graph.nodes false then loop (pass - 1)
  in
  let max_pass = 50 (* TODO: max number of field for a node times number of nodes? *) in
  loop max_pass

let compile conf graph =
  set_all_types graph ;
  save_graph conf graph ;
  (* For now we merely print the nodes *)
  Hashtbl.iter (fun _ node ->
    !logger.debug "node %S:\n\tinput type: %a\n\toutput type: %a\n\n"
      node.name
      print_temp_tup_typ node.in_type
      print_temp_tup_typ node.out_type) graph.nodes ;
  failwith "TODO: actually compile the graph"
