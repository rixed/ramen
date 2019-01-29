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
open RamenFieldMask (* shared with codegen lib *)
module T = RamenTypes
module E = RamenExpr
module U = RamenUnits

(* The expression recorded here is any one instance of its occurrence.
 * Typing will make sure all have the same type. *)
type path = (path_comp * E.t) list
and path_comp = Int of int | Name of string

let print_path oc =
  let print_path_comp oc = function
    | Int i -> Printf.fprintf oc "[%d]" i
    | Name n -> String.print oc n
  in
  List.print ~first:"" ~last:"" ~sep:"." print_path_comp oc

let path_comp_of_constant_expr e =
  let fail () =
    Printf.sprintf2
      "Cannot find out path component of %a: \
       Only immediate integers and field names are supported"
      (E.print false) e |>
    failwith in
  match e.text with
  | E.Const _ ->
      (match E.int_of_const e with
      | Some i -> Int i
      | None ->
          (match E.string_of_const e with
          | Some n -> Name n
          | None -> fail ()))
  | _ -> fail ()

(*$< Batteries *)
(*$< RamenFieldMask *)

(* Return the optional path (to reach input) at the top level, and all the
 * paths present in this expression. Do not also return subpaths.
 * Environment is not enriched along the way since we are only interrested
 * in input not local records. *)
let rec paths_of_expression_rev =
  let add_paths_to lst env e =
    match paths_of_expression env e with
    | [], o -> o @ lst
    | t, o -> t :: (o @ lst) in
  let recurs env e =
    [],
    E.fold_subexpressions (fun others env e ->
      add_paths_to others env e
    ) [] env e
  in
  fun env e ->
  match e.E.text with
  | E.(Stateless (SL2 (Get, n, x))) ->
      (match paths_of_expression_rev env x with
      | [], others ->
          let others = add_paths_to others env n in
          (* [e] is actually not a pure piece of input *)
          [], others
      | p, others ->
          let others = add_paths_to others env n in
          match path_comp_of_constant_expr n with
          | exception exn ->
              !logger.debug "Cannot evaluate constant value of %a: %s"
                (E.print false) n (Printexc.to_string exn) ;
              p, others
          | c ->
              ((c, e) :: p), others)
  | E.Variable name ->
      let what = "paths_of_expression_rev" in
      (match E.Env.lookup what env name with
      | exception Not_found ->
          E.Env.unbound_var what env name
      | tup_pref ->
          if E.tuple_has_type_input tup_pref then
            [ Name (RamenName.string_of_field name), e ], []
          else
            recurs env e)
  | _ ->
      recurs env e

and paths_of_expression env e =
  let t, o = paths_of_expression_rev env e in
  List.rev t, o

(*$inject
  let string_of_paths (t, o) =
    Printf.sprintf2 "%a, %a"
      print_path t (List.print print_path) o
  let strip_expr (t, o) =
    List.map fst t, List.map (List.map fst) o
 *)
(*$= paths_of_expression & ~printer:string_of_paths
  ([ Name "foo" ], []) \
    (RamenExpr.parse "in.foo" |> paths_of_expression |> strip_expr)
  ([ Name "foo" ; Int 3 ], []) \
    (RamenExpr.parse "get(3, in.foo)" |> paths_of_expression |> strip_expr)
  ([ Name "foo" ; Name "bar" ], []) \
    (RamenExpr.parse "get(\"bar\", in.foo)" |> paths_of_expression |> strip_expr)
  ([ Name "foo" ], [ [ Name "some_index" ; Int 2 ] ]) \
    (RamenExpr.parse "get(get(2, in.some_index), in.foo)" |> paths_of_expression |> strip_expr)
  ([], []) (RamenExpr.parse "0+0" |> paths_of_expression |> strip_expr)
 *)

let paths_of_operation op =
  RamenOperation.Env.fold_top (fun _what ps env e ->
    let t, o = paths_of_expression env e in
    let o = if t = [] then o else t :: o in
    List.rev_append o ps
  ) [] [] op

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
  | (Name n, e) :: p -> Subfields (Map.String.singleton n (tree_of_path e p))
  | (Int i, e) :: p -> Indices (Map.Int.singleton i (tree_of_path e p))

let tree_of_paths ps =
  (* Return the tree of all input access paths mentioned: *)
  let root_expr =
    E.(make (Variable (RamenName.field_of_string "in"))) in
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
  "empty" (str_tree_of "0+0")
  "{bar:<bar>,foo:<foo>}" (str_tree_of "in.foo + in.bar")
  "{foo:{0:<get>,1:<get>}}" (str_tree_of "get(0,in.foo) + get(1,in.foo)")
  "{bar:{1:<get>},foo:{0:<get>}}" (str_tree_of "get(0,in.foo) + get(1,in.bar)")
  "{bar:{1:<get>,2:<get>},foo:{0:<get>}}" \
    (str_tree_of "get(0,in.foo) + get(1,in.bar) + get(2,in.bar)")
  "{foo:{bar:<get>,baz:<get>}}" \
    (str_tree_of "get(\"bar\",in.foo) + get(\"baz\",in.foo)")
  "{foo:{bar:{1:<get>},baz:<get>}}" \
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
        Map.Int.fold (fun i t u -> loop u (Int i :: p) t) m u
    | Subfields m ->
        Map.String.fold (fun n t u -> loop u (Name n :: p) t) m u in
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

and rec_fieldmask : 'b 'c. T.t -> ('b -> 'c -> tree) -> 'b -> 'c ->
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
  let open T in
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
  let open T in
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

let tree_of_operation op =
  let paths = paths_of_operation op in
  tree_of_paths paths

(* The fields send to a function might be seen as a kind of record
 * with the difference that some field names have access paths to deep
 * fields in parent output record, for values extracted from a compound
 * type that's not transmitted as a whole): *)
type in_field =
  { path : path_comp list (* from root to leaf *);
    mutable typ : T.t ;
    mutable units : U.t option }

type in_type = in_field list

let id_of_path p =
  List.fold_left (fun id p ->
    match p with
    | Int i -> id ^"["^ string_of_int i ^"]"
    | Name s -> if id = "" then s else "."^ s
  ) "" p

let in_type_signature =
  List.fold_left (fun s f ->
    (if s = "" then "" else s ^ "_") ^
    id_of_path f.path ^ ":" ^
    T.string_of_typ f.typ
  ) ""

let print_in_field oc f =
  Printf.fprintf oc "%s %a"
    (id_of_path f.path)
    T.print_typ f.typ ;
  Option.may (U.print oc) f.units

let print_in_type oc =
  (List.print ~first:"(" ~last:")" ~sep:", "
    (fun oc t -> print_in_field oc t)) oc

let in_type_of_operation op =
  let tree = tree_of_operation op in
  fold_tree [] (fun lst path e ->
    { path = List.rev path ;
      typ = e.E.typ ;
      units = e.typ.units } :: lst
  ) tree |>
  List.rev

let find_type_of_path parent_typ path =
  let rec locate_type typ = function
    | [] -> typ
    | Int i :: rest ->
        let invalid () =
          Printf.sprintf2 "Invalid path index %d into %a"
            i T.print_typ typ |>
          failwith in
        (match typ.T.structure with
        | TVec (_, t) | TList t -> locate_type t rest
        | TTuple ts ->
            if i >= Array.length ts then invalid () ;
            locate_type ts.(i) rest
        | _ ->
            invalid ())
    | Name n :: rest ->
        let invalid () =
          Printf.sprintf2 "Invalid subfield %S into %a"
            n T.print_typ typ |>
          failwith in
        (match typ.structure with
        | TRecord ts ->
            (match array_rfind (fun (k, _) -> k = n) ts with
            | exception Not_found -> invalid ()
            | _, t -> locate_type t rest)
        | _ -> invalid ())
  in
  match path with
  | [] ->
      invalid_arg "find_type_of_path: empty path"
  | Name name :: rest ->
      let name = RamenName.field_of_string name in
      (match T.fields_of_type parent_typ |>
             enum_rfind (fun (n, _) -> n = RamenName.string_of_field name)
      with
      | exception Not_found ->
          Printf.sprintf2 "Cannot find field %a in %a"
            RamenName.field_print name
            T.print_typ parent_typ |>
          failwith
      | _, t ->
          locate_type t rest)
  | Int i :: _ ->
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
    | [ Name n ],
      E.(Variable n') when n' = RamenName.field_of_string n ->
        true
    | Name n :: path',
      E.(Stateless (SL2 (Get, s, e'))) ->
        (match E.string_of_const s with
        | Some n' when n' = n -> matches_expr path' e'
        | _ -> false)
    | Int i :: path',
      E.(Stateless (SL2 (Get, n, e'))) ->
        (match E.int_of_const n with
        | Some i' when i' = i -> matches_expr path' e'
        | _ -> false)
    | _ -> false
  in
  RamenOperation.Env.map (fun what env e ->
    match e.E.text with
    | Stateless (SL2 (Get, _, ({ text = Variable var_name ; _ } as var))) ->
        (match E.Env.lookup what env var_name with
        | exception Not_found -> e
        | tup_pref when E.tuple_has_type_input tup_pref ->
            (* So here we are accessing something from input: shall we
             * shortcut? *)
            (match List.find (fun in_field ->
                     assert (in_field.path <> []) ;
                     matches_expr (List.rev in_field.path) e
                   ) in_type with
            | exception Not_found -> e
            | in_field ->
                let name = id_of_path in_field.path in
                { e with
                  text = Stateless (SL2 (Get, E.of_string name, var)) })
        | _ -> e)
    | _ -> e
  ) []

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
