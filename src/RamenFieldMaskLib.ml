(* Recursive fieldmasks.
 *
 * When serializing a function output a parent pass only the fields that are
 * actually required by its children. Therefore, the out_ref files need to
 * specify, for each target, which fields to copy and which to pass. As any
 * field can itself be composed of several subfields, this field-mask must
 * be recursive.
 *
 * For scalar values, we only can either Skip or Copy the value.
 * For records/tuples, we can also copy only some of the fields.
 * Vectors/list have no value of their own; rather, we need to know which
 * elements must be copied, and what fields of these elements must be copied.
 * We support to copy the same subfields of a selection of elements or a
 * custom fieldmask per element.
 *)
open Batteries
open RamenHelpers
open RamenLog
open RamenLang
module E = RamenExpr
open RamenFieldMask (* shared with codegen lib *)

(* The expression recorded here is any one instance of its occurrence.
 * Typing will make sure all have the same type. *)
type path = (E.path_comp * E.t) list

let print_path oc =
  List.print ~first:"" ~last:"" ~sep:"."
    (fun oc (p, _e) -> E.print_path_comp oc p) oc

let path_comp_of_constant_expr e =
  let fail () =
    Printf.sprintf2
      "Cannot find out path component of %a: \
       Only immediate integers and field names are supported"
      (E.print false) e |>
    failwith in
  match e.E.text with
  | Const _ ->
      (match E.int_of_const e with
      | Some i -> E.Int i
      | None ->
          (match E.string_of_const e with
          | Some n -> E.Name n
          | None -> fail ()))
  | _ -> fail ()

(*$< Batteries *)
(*$< RamenFieldMask *)

(* Return the optional path at the top level, and all the paths present in
 * this expression. Do not also return subpaths. *)
let rec paths_of_expression_rev =
  let add_paths_to lst e =
    match paths_of_expression e with
    | [], o -> o @ lst
    | t, o -> t :: (o @ lst) in
  fun e ->
    let add_get_name n p others =
      let others = add_paths_to others n in
      match path_comp_of_constant_expr n with
      | exception exn ->
          !logger.debug "Cannot evaluate constant value of %a: %s"
            (E.print false) n (Printexc.to_string exn) ;
          p, others
      | c ->
          ((c, e) :: p), others
    in
    match e.E.text with
    | Stateless (SL2 (Get, n, { text = Variable tuple ; _ }))
      when tuple_has_type_input tuple ->
        add_get_name n [] []
    | Stateless (SL2 (Get, n, x)) ->
        (match paths_of_expression_rev x with
        | [], others ->
            let others = add_paths_to others n in
            (* [e] is actually not a pure piece of input *)
            [], others
        | p, others ->
            add_get_name n p others)
    | Field (tuple, name) when tuple_has_type_input !tuple ->
        if RamenName.is_private name then
          Printf.sprintf2 "Can not use input field %a, which is private."
            RamenName.field_print name |>
          failwith ;
        [ E.Name (RamenName.string_of_field name), e ], []
    | Path path ->
        (* As the expression is only useful for the leaf element we can
         * repeat it for each component of the path: *)
        List.rev_map (fun comp -> comp, e) path, []
    | _ ->
        [],
        E.fold_subexpressions (fun _ others e ->
          add_paths_to others e
        ) [] [] e

and paths_of_expression e =
  let t, o = paths_of_expression_rev e in
  List.rev t, o

(*$inject
  module E = RamenExpr
  let print_path_ oc =
    List.print ~first:"" ~last:"" ~sep:"." E.print_path_comp oc
  let string_of_paths_ (t, o) =
    Printf.sprintf2 "%a, %a"
      print_path_ t (List.print print_path_) o
  let strip_expr (t, o) =
    List.map fst t, List.map (List.map fst) o
 *)
(*$= paths_of_expression & ~printer:string_of_paths_
  ([ E.Name "foo" ], []) \
    (RamenExpr.parse "in.foo" |> paths_of_expression |> strip_expr)
  ([ E.Name "foo" ; E.Int 3 ], []) \
    (RamenExpr.parse "get(3, in.foo)" |> paths_of_expression |> strip_expr)
  ([ E.Name "foo" ; E.Name "bar" ], []) \
    (RamenExpr.parse "get(\"bar\", in.foo)" |> paths_of_expression |> strip_expr)
  ([ E.Name "foo" ], [ [ E.Name "some_index" ; E.Int 2 ] ]) \
    (RamenExpr.parse "get(get(2, in.some_index), in.foo)" |> paths_of_expression |> strip_expr)
  ([], []) (RamenExpr.parse "0+0" |> paths_of_expression |> strip_expr)
 *)

let paths_of_operation =
  RamenOperation.fold_top_level_expr [] (fun ps _what e ->
    let t, o = paths_of_expression e in
    let o = if t = [] then o else t :: o in
    List.rev_append o ps)

(* All paths are merged into a tree of components: *)
type tree =
  | Empty (* means uninitialized *)
  (* Either a scalar or a compound type that is manipulated as a whole: *)
  | Leaf of E.t
  | Indices of tree Map.Int.t
  | Subfields of tree Map.String.t

(* generic compare does not work on maps: *)
let rec compare_tree t1 t2 =
  match t1, t2 with
  | Empty, _ -> -1
  | Leaf _, Leaf _ -> 0
  | Leaf _, (Indices _ | Subfields _) -> -1
  | Indices _, Subfields _ -> -1
  | Indices i1, Indices i2 ->
      Map.Int.compare compare_tree i1 i2
  | Subfields m1, Subfields m2 ->
      Map.String.compare compare_tree m1 m2
  | _ -> 1

let eq t1 t2 = compare_tree t1 t2 = 0

(* More compact than the default: *)
let rec map_print oc m =
  Map.String.print ~first:"{" ~last:"}" ~sep:"," ~kvsep:":"
    String.print print_tree oc m

and print_indices oc m =
  Map.Int.print ~first:"{" ~last:"}" ~sep:"," ~kvsep:":"
    Int.print print_tree oc m

and print_subfields oc m =
  Map.String.print ~first:"{" ~last:"}" ~sep:"," ~kvsep:":"
    String.print print_tree oc m

and print_tree oc = function
  | Empty -> String.print oc "empty"
  | Leaf e -> Printf.fprintf oc "<%a>" (E.print ~max_depth:1 false) e
  | Indices m -> print_indices oc m
  | Subfields m -> print_subfields oc m

let rec merge_tree t1 t2 =
  match t1, t2 with
  | Empty, t | t, Empty -> t
  (* If a value is used as a whole and also with subfields, forget about the
   * subfields: *)
  | (Leaf _ as l), _ | _, (Leaf _ as l) -> l
  | Indices m1, Indices m2 ->
      Indices (
        Map.Int.fold (fun i1 s1 m ->
          Map.Int.modify_opt i1 (function
            | None -> Some s1
            | Some s2 -> Some (merge_tree s1 s2)
          ) m
        ) m1 m2)
  | Subfields m1, Subfields m2 ->
      Subfields (
        Map.String.fold (fun n1 s1 m ->
          Map.String.modify_opt n1 (function
            | None -> Some s1
            | Some s2 -> Some (merge_tree s1 s2)
          ) m
        ) m1 m2)
  | Indices i, Subfields n
  | Subfields n, Indices i ->
      Printf.sprintf2
        "Invalid mixture of numeric indices (%a) with subfield names (%a)"
        print_indices i print_subfields n |>
      failwith

let rec tree_of_path e = function
  | [] -> Leaf e
  | (E.Name n, e) :: p -> Subfields (Map.String.singleton n (tree_of_path e p))
  | (E.Int i, e) :: p -> Indices (Map.Int.singleton i (tree_of_path e p))

let tree_of_paths ps =
  (* Return the tree of all input access paths mentioned: *)
  let root_expr =
    E.make (Field (ref TupleIn, RamenName.field_of_string "in")) in
  List.fold_left (fun t p ->
    merge_tree t (tree_of_path root_expr p)
  ) Empty ps

(*$inject
  let tree_of s =
    let t, o = RamenExpr.parse s |> paths_of_expression in
    let paths = if t = [] then o else t :: o in
    tree_of_paths paths
  let str_tree_of s =
    IO.to_string print_tree (tree_of s)
 *)
(*$= str_tree_of & ~printer:identity
  "empty" \
    (str_tree_of "0+0")
  "{bar:<in.bar>,foo:<in.foo>}" \
    (str_tree_of "in.foo + in.bar")
  "{foo:{0:<GET(..., ...)>,1:<GET(..., ...)>}}" \
    (str_tree_of "get(0,in.foo) + get(1,in.foo)")
  "{bar:{1:<GET(..., ...)>},foo:{0:<GET(..., ...)>}}" \
    (str_tree_of "get(0,in.foo) + get(1,in.bar)")
  "{bar:{1:<GET(..., ...)>,2:<GET(..., ...)>},foo:{0:<GET(..., ...)>}}" \
    (str_tree_of "get(0,in.foo) + get(1,in.bar) + get(2,in.bar)")
  "{foo:{bar:<GET(..., ...)>,baz:<GET(..., ...)>}}" \
    (str_tree_of "get(\"bar\",in.foo) + get(\"baz\",in.foo)")
  "{foo:{bar:{1:<GET(..., ...)>},baz:<GET(..., ...)>}}" \
    (str_tree_of "get(1,get(\"bar\",in.foo)) + get(\"baz\",in.foo) + \
                  get(\"glop\",get(\"baz\",in.foo))")
 *)

(* Iter over a tree in serialization order: *)
let fold_tree u f t =
  let rec loop u p = function
    | Empty -> u
    | Leaf e -> f u p e
    | Indices m ->
        (* Note: Map will fold the keys in increasing order: *)
        Map.Int.fold (fun i t u -> loop u (E.Int i :: p) t) m u
    | Subfields m ->
        Map.String.fold (fun n t u -> loop u (E.Name n :: p) t) m u in
  loop u [] t

(* Given the type of the parent output and the tree of paths used in the
 * child, compute the field mask.
 * For now the input type is a RamenTuple.typ but in the future it should
 * be a RamenTypes.t, and RamenTuple moved into a proper RamenTypes.TRecord *)
let rec fieldmask_for_output typ t =
  match t with
  | Empty -> List.enum typ /@ (fun _ -> Skip) |> Array.of_enum
  | Subfields m -> fieldmask_for_output_subfields typ m
  | _ -> failwith "Input type must be a record"

and rec_fieldmask : 'b 'c. RamenTypes.t -> ('b -> 'c -> tree) -> 'b -> 'c ->
                           RamenFieldMask.mask =
  fun typ finder key map ->
    match finder key map with
    | exception Not_found -> Skip
    | Empty -> assert false
    | Leaf _ -> Copy
    | Indices i -> fieldmask_of_indices typ i
    | Subfields m -> fieldmask_of_subfields typ m

(* [typ] gives us all possible fields ordered, so we can build a mask.
 * For each subfields we have to pick a mask among Skip (not used), Copy
 * (used _fully_, such as scalars), and Rec if that field also has subfields.
 * For now we consider long vectors and lists to not have subfields. Short
 * vectors have subfields though; for indexed structured types such as
 * those, the mask is trivially ordered by index. *)
and fieldmask_for_output_subfields typ m =
  (* TODO: check if we should copy the whole thing *)
  List.enum typ /@ (fun ft ->
    let name = RamenName.string_of_field ft.RamenTuple.name in
    rec_fieldmask ft.typ Map.String.find name m) |>
  Array.of_enum

and fieldmask_of_subfields typ m =
  let open RamenTypes in
  match typ.structure with
  | TRecord kts ->
      let ser_kts = RingBufLib.ser_array_of_record kts in
      Rec (
        Array.map (fun (k, typ) ->
          rec_fieldmask typ Map.String.find k m
        ) ser_kts)
  | _ ->
      Printf.sprintf2 "Type %a does not allow subfields %a"
        print_typ typ
        (pretty_enum_print String.print_quoted) (Map.String.keys m) |>
      failwith

and fieldmask_of_indices typ m =
  let fm_of_vec d typ =
    Rec (Array.init d (fun i -> rec_fieldmask typ Map.Int.find i m))
  in
  (* TODO: make an honest effort to find out if its cheaper to copy the
   * whole thing. *)
  let open RamenTypes in
  match typ.structure with
  | TTuple ts ->
      Rec (Array.mapi (fun i typ -> rec_fieldmask typ Map.Int.find i m) ts)
  | TVec (d, typ) -> fm_of_vec d typ
  | TList typ ->
      (match Map.Int.keys m |> Enum.reduce max with
      | exception Not_found -> (* no indices *) Skip
      | ma -> fm_of_vec (ma+1) typ)
  | _ ->
      Printf.sprintf2 "Type %a does not allow indexed accesses %a"
        print_typ typ
        (pretty_enum_print Int.print) (Map.Int.keys m) |>
      failwith

(*$inject
  let make_typ t = RamenTypes.{ structure = t ; nullable = false }
  let make_tup_typ =
    List.map (fun (n, t) ->
      RamenTuple.{ name = RamenName.field_of_string n ;
                   typ = RamenTypes.{ structure = t ; nullable = false } ;
                   units = None ; doc = "" ; aggr = None })
  let tup1 = make_tup_typ [ "f1", RamenTypes.TString ;
                            "f2", RamenTypes.(TVec (3, make_typ TU8)) ] *)
(*$= fieldmask_for_output & ~printer:identity
  "__" (fieldmask_for_output tup1 (tree_of "0 + 0") |> to_string)
  "X_" (fieldmask_for_output tup1 (tree_of "in.f1") |> to_string)
  "_X" (fieldmask_for_output tup1 (tree_of "in.f2") |> to_string)
  "_(_X_)" (fieldmask_for_output tup1 (tree_of "get(1,in.f2)") |> to_string)
 *)

let tree_of_operation =
  tree_of_paths % paths_of_operation

(* The fields send to a function might be seen as a kind of record
 * with the difference that some field names have access paths to deep
 * fields in parent output record, for values extracted from a compound
 * type that's not transmitted as a whole): *)
type in_field =
  { path : E.path_comp list (* from root to leaf *);
    mutable typ : RamenTypes.t ;
    mutable units : RamenUnits.t option }

type in_type = in_field list

let id_of_path p =
  List.fold_left (fun id p ->
    id ^(
      match p with
      | E.Int i -> "["^ string_of_int i ^"]"
      | E.Name s -> if id = "" then s else "."^ s)
  ) "" p |>
  RamenName.field_of_string

let in_type_signature =
  List.fold_left (fun s f ->
    (if s = "" then "" else s ^ "_") ^
    RamenName.string_of_field (id_of_path f.path) ^ ":" ^
    RamenTypes.string_of_typ f.typ
  ) ""

let print_in_field oc f =
  Printf.fprintf oc "%a %a"
    RamenName.field_print (id_of_path f.path)
    RamenTypes.print_typ f.typ ;
  Option.may (RamenUnits.print oc) f.units

let print_in_type oc =
  (List.print ~first:"(" ~last:")" ~sep:", "
    (fun oc t -> print_in_field oc t)) oc

let in_type_of_operation op =
  let tree = tree_of_operation op in
  fold_tree [] (fun lst path e ->
    { path = List.rev path ;
      typ = e.E.typ ;
      units = e.E.units } :: lst
  ) tree |>
  List.rev

let find_type_of_path parent_out path =
  let rec locate_type typ = function
    | [] -> typ
    | E.Int i :: rest ->
        let invalid () =
          Printf.sprintf2 "Invalid path index %d into %a"
            i RamenTypes.print_typ typ |>
          failwith in
        (match typ.RamenTypes.structure with
        | TVec (d, t) ->
            if i >= d then invalid () ;
            locate_type t rest
        | TList t ->
            locate_type t rest
        | TTuple ts ->
            if i >= Array.length ts then invalid () ;
            locate_type ts.(i) rest
        | _ ->
            invalid ())
    | E.Name n :: rest ->
        let invalid () =
          Printf.sprintf2 "Invalid subfield %S into %a"
            n RamenTypes.print_typ typ |>
          failwith in
        (match typ.structure with
        | TRecord ts ->
            (match array_rfind (fun (k, _) -> k = n) ts with
            | exception Not_found -> invalid ()
            | _, t -> locate_type t rest)
        | _ -> invalid ())
  in
  !logger.debug "find_type_of_path %a" E.print_path path ;
  match path with
  | [] ->
      invalid_arg "find_type_of_path: empty path"
  | E.Name name :: rest ->
      let name = RamenName.field_of_string name in
      (match List.find (fun ft -> ft.RamenTuple.name = name) parent_out with
      | exception Not_found ->
          Printf.sprintf2 "Cannot find field %a in %a"
            RamenName.field_print name
            RamenTuple.print_typ parent_out |>
          failwith
      | ft ->
          locate_type ft.typ rest)
  | E.Int i :: _ ->
      Printf.sprintf "Invalid index %d at beginning of input type path" i |>
      failwith

(* Optimize the op to replace all the Gets by any deep fields readily
 * available in in_type (that's actually a requirement to be able to compile
 * this operation: *)
let subst_deep_fields in_type =
  (* As we are going to find the deeper dereference first in the AST, we
   * start by matching the end of the path (which has been inverted): *)
  let rec matches_expr path e =
    match path, e.E.text with
    | [ E.Name n ],
      Field (tup, n')
        when tuple_has_type_input !tup &&
             n' = RamenName.field_of_string n ->
        true
    | [ E.Name n ],
      Stateless (SL2 (Get, s, { text = Variable TupleIn ; _ })) ->
        (match E.string_of_const s with
        | Some n' when n' = n -> true
        | _ -> false)
    | E.Name n :: path',
        (* Here we assume that to deref a record one uses Get with a string
         * index. *)
        Stateless (SL2 (Get, s, e')) ->
          (match E.string_of_const s with
          | Some n' when n' = n -> matches_expr path' e'
          | _ -> false)
    | E.Int i :: path',
        Stateless (SL2 (Get, n, e')) ->
          (match E.int_of_const n with
          | Some i' when i' = i -> matches_expr path' e'
          | _ -> false)
    | _ -> false
  in
  RamenOperation.map_expr (fun e ->
    match List.find (fun in_field ->
            assert (in_field.path <> []) ;
            matches_expr (List.rev in_field.path) e
          ) in_type with
    | exception Not_found -> e
    | in_field ->
        !logger.debug "Substituting expression %a with Path %a"
          (E.print ~max_depth:2 false) e
          (List.print E.print_path_comp) in_field.path ;
        { e with text = Path in_field.path })

(* Return the fieldmask required to send out_typ to this operation: *)
let fieldmask_of_operation ~out_typ op =
  tree_of_operation op |>
  fieldmask_for_output out_typ

(* Return the fieldmask required to copy everything (useful for archiving): *)
let fieldmask_all ~out_typ =
  (* Assuming for now that out_typ is a record: *)
  let nb_fields = List.length out_typ in
  Array.make nb_fields Copy

(*$>*)
(*$>*)
