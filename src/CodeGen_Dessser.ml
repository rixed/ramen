(* Make use of external library Dessser for code generation *)
open Batteries
open Stdint
open RamenHelpers
open RamenHelpersNoLog
open RamenLang
open RamenLog
open Dessser
module C = RamenConf
module DE = DessserExpressions
module DT = DessserTypes
module E = RamenExpr
module EntryPoints = RamenConstsEntryPoints
module Files = RamenFiles
module Helpers = CodeGen_Helpers
module N = RamenName
module O = RamenOperation
module T = RamenTypes
module RaQL2DIL = CodeGen_RaQL2DIL

module Value2RingBuf = HeapValue.Serialize (RamenRingBuffer.Ser)
module RingBuf2Value = HeapValue.Materialize (RamenRingBuffer.Des)

module RowBinary2Value = HeapValue.Materialize (RowBinary.Des)
let rowbinary_to_value ?config mn =
  let open DE.Ops in
  comment "Function deserializing the rowbinary into a heap value:"
    (DE.func1 DataPtr (fun _l src -> RowBinary2Value.make ?config mn src))

module Csv2Value = HeapValue.Materialize (Csv.Des)
let csv_to_value ?config mn =
  let open DE.Ops in
  comment "Function deserializing the CSV into a heap value:"
    (DE.func1 DataPtr (fun _l src -> Csv2Value.make ?config mn src))

let sersize_of_type mn =
  let cmt =
    Printf.sprintf2 "Compute the serialized size of values of type %a"
      DT.print_maybe_nullable mn in
  let open DE.Ops in
  let ma = copy_field in
  DE.func1 (Value mn) (fun _l v -> Value2RingBuf.sersize mn ma v) |>
  comment cmt

(* Takes a fieldmask, a tx, an offset and a heap value, and returns the new
 * offset. *)
let serialize mn =
  let cmt =
    Printf.sprintf2 "Serialize a value of type %a"
      DT.print_maybe_nullable mn in
  let open DE.Ops in
  let tx_t = DT.(Value (make (get_user_type "tx"))) in
  DE.func4 DT.Mask tx_t DT.Size (DT.Value mn) (fun _l ma tx start_offs v ->
    let dst = apply (identifier "CodeGenLib_Dessser.pointer_of_tx") [ tx ] in
    let dst' = data_ptr_add dst start_offs in
    let dst' = Value2RingBuf.serialize mn ma v dst' in
    data_ptr_sub dst' dst) |>
  comment cmt

(* The [generate_tuples_] function is the final one that's called after the
 * decision is taken to output a tuple. What we had so far as out [out_type]
 * was in fact [CodeGenLib_Skeleton]'s [`generator_out], which could still
 * have generators instead of proper values in some places (a generator is a
 * function generating several outputs that we want on distinct tuples.
 * [generate_tuples_] function takes an external callback function taking the
 * generated tuple_out and returning nothing, and a tuple_in and the
 * generator_out, and returns nothing. The callback is therefore in charge of
 * actually sending the generated tuples. *)
let generate_tuples in_type out_type out_fields =
  let has_generator =
    List.exists (fun sf ->
      E.is_generator sf.O.expr)
      out_fields in
  let callback_t = DT.Function ([| DT.Value out_type |], DT.Void) in
  DE.func3 callback_t (DT.Value in_type) (DT.Value out_type) (fun _l f _it ot ->
    let open DE.Ops in
    if not has_generator then apply f [ ot ]
    else todo "generators")

(* Wrap around add_identifier_of_expression to display the full expression in case
 * type_check fails: *)
let add_identifier_of_expression ?name f code e =
  try
    f code ?name e
  with exn ->
    let fname = Filename.get_temp_dir_name () ^"/dessser_type_error.last" in
    ignore_exceptions (fun () ->
      let mode = [ `create ; `trunc ; `text ] in
      File.with_file_out fname ~mode (fun oc ->
        DE.print ?max_depth:None oc e)) () ;
    !logger.error "Invalid expression: %a (see complete expression in %s), %s"
      (DE.print ~max_depth:3) e
      fname
      (Printexc.to_string exn) ;
    raise exn

module OCaml =
struct
  module BE = BackEndOCaml

  (* Here we rewrite Dessser heap values as internal value, converting
   * records into tuples, user types into our owns, etc, recursively,
   * and also converting from options to nullable.
   * FIXME: Use the same representation for records than desser
   *        and keep heap_value as is. Also maybe dessser could use proper
   *        tuples instead of fake records? *)
  let rec emit_ramen_of_dessser_value ?(depth=0) mn oc vname =
    (* Not all values need conversion though. For performance, reuse as
     * much as the heap value as possible: *)
    let rec need_conversion mn =
      match mn.DT.vtyp with
      | DT.Unknown | Unit | Mac _ ->
          false
      | Usr { name = ("Ip" | "Cidr") ; _ } ->
          true
      | Usr { def ; _ } ->
          need_conversion DT.{ nullable = false ; vtyp = def }
      | Vec (_, mn) | Lst mn | Set mn ->
          need_conversion mn
      | Tup _ | Rec _ ->
          (* Represented as records in Dessser but tuples in Ramen (FIXME): *)
          true
      | Sum mns ->
          Array.exists (need_conversion % snd) mns
      | Map (k, v) ->
          need_conversion k || need_conversion v
    in
    if need_conversion mn then (
      emit oc depth "(" ;
      let vname', depth' =
        if mn.nullable then (
          emit oc (depth+1) "Nullable.map (fun x ->" ;
          "x", depth + 2
        ) else (
          vname, depth + 1
        ) in
      (* Emit an array of maybe_nullable. [mns] has the field names that must
       * be prefixed with the module name to reach the fields in Dessser
       * generated code. *)
      let emit_tuple mod_name mns =
        Array.iteri (fun i (field_name, mn) ->
          let field_name = BE.Config.valid_identifier field_name in
          let n = vname' ^"."^ mod_name ^"."^ field_name in
          let v =
            Printf.sprintf2 "%a" (emit_ramen_of_dessser_value ~depth:depth' mn) n in
          (* Remove the last newline for cosmetic: *)
          let v =
            let l = String.length v in
            if l > 0 && v.[l-1] = '\n' then String.rchop v else v in
          emit oc 0 "%s%s" v (if i < Array.length mns - 1 then "," else "")
        ) mns in
      let mod_name =
        "DessserGen." ^
        (* Types are defined as non-nullable and the option is added afterward
         * as required: *)
        BE.Config.module_of_type (DT.Value { mn with nullable = false }) in
      (match mn.vtyp with
      (* Convert Dessser makeshift type into Ramen's: *)
      | Usr { name = "Ip" ; _ } ->
          emit oc depth' "match %s with %s.V4 x -> RamenIp.V4 x \
             | V6 x -> RamenIp.V6 x" vname' mod_name
      | Usr { name = "Cidr" ; _ } ->
          emit oc depth' "match %s with %s.V4 x -> RamenIp.Cidr.V4 (x.ip, x.mask) \
             | V6 x -> RamenIp.Cidr.V6 (x.ip, x.mask)" vname' mod_name
      | Usr { def ; _ } ->
          let mn = DT.{ vtyp = def ; nullable = false } in
          emit_ramen_of_dessser_value ~depth mn oc vname'
      | Vec (_, mn) | Lst mn ->
          emit oc depth' "Array.map (fun x ->" ;
          emit_ramen_of_dessser_value ~depth:depth' mn oc "x" ;
          emit oc depth' ") %s" vname'
      | Tup mns ->
          Array.mapi (fun i t ->
            BE.Config.tuple_field_name i, t
          ) mns |>
          emit_tuple mod_name
      | Rec mns ->
          emit_tuple mod_name mns
      | Sum _ ->
          (* No values of type Sum yet *)
          assert false
      | _ ->
          emit oc depth' "%s" vname') ;
      if mn.DT.nullable then emit oc depth' ") %s" vname ;
      emit oc depth ")"
    ) else ( (* No need_conversion *)
      emit oc depth "%s" vname
    )

  let emit deserializer mn oc =
    let p fmt = emit oc 0 fmt in
    let code = BE.make_state () in
    let code, _, value_of_ser =
      deserializer mn |>
      add_identifier_of_expression ~name:"value_of_ser" BE.add_identifier_of_expression code in
(* Unused for now, require the output type [mn] to be record-sorted:
    let code, _, _sersize_of_type =
      sersize_of_type mn |>
      add_identifier_of_expression ~name:"sersize_of_type" BE.add_identifier_of_expression code in
    let code, _, _serialize =
      serialize mn |>
      add_identifier_of_expression ~name:"serialize" BE.add_identifier_of_expression code in
*)
    p "(* Helpers for deserializing type:\n\n%a\n\n*)\n"
      DT.print_maybe_nullable mn ;
    BE.print_definitions code oc ;
    (* A public entry point to unserialize the tuple with a more meaningful
     * name, and which also convert the tuple representation.
     * Indeed, CodeGen_OCaml uses actual tuples for tuples while Dessser uses
     * more convenient records with mutable fields.
     * Also, CodeGen_OCaml uses a dedicated nullable type whereas Desser
     * uses only an option type (TODO: it should really use a dedicated
     * nullable type as well, which it will as soon as we identify Desser user
     * types with RamenTypes types).
     * Assuming there are no embedded records, then it's enough to convert
     * the outer level which is trivially done here.
     * Of course once Dessser has grown to replace CodeGen_Ocaml then this
     * conversion useless. *)
    p "" ;
    p "open DessserOCamlBackendHelpers" ;
    p "" ;
    p "let read_tuple buffer start stop _has_more =" ;
    p "  assert (stop >= start) ;" ;
    p "  let src = Pointer.of_bytes buffer start stop in" ;
    p "  let heap_value, src' = DessserGen.%s src in" value_of_ser ;
    p "  let read_sz = Pointer.sub src' src" ;
    p "  and tuple =" ;
    emit_ramen_of_dessser_value ~depth:2 mn oc "heap_value" ;
    p "  in" ;
    p "  tuple, read_sz"
end

module CPP =
struct
  module BE = BackEndCPP

  let emit deserializer mn oc =
    let code = BE.make_state () in
    let code, _, _value_of_ser =
      deserializer mn |>
      add_identifier_of_expression ~name:"value_of_ser" BE.add_identifier_of_expression code in
(* Unused for now, require the output type [mn] to be record-sorted:
    let code, _, _sersize_of_type =
      sersize_of_type mn |>
      add_identifier_of_expression ~name:"sersize_of_type" BE.add_identifier_of_expression code in
    let code, _, _serialize =
      serialize mn |>
      add_identifier_of_expression ~name:"serialize" BE.add_identifier_of_expression code in
*)
    Printf.fprintf oc "/* Helpers for function:\n\n%a\n\n*/\n"
      DT.print_maybe_nullable mn ;
    BE.print_definitions code oc ;
    (* Also add some OCaml wrappers: *)
    String.print oc {|

/* OCaml wrappers */

extern "C" {
# define CAML_NAME_SPACE
# include <caml/mlvalues.h>
# include <caml/memory.h>
# include <caml/alloc.h>

  /* Takes an ocaml byte string containing the data and an offset (int), a max
   * offset (int), and a "more to come" flag (bool), and returns a pair
   * with the tuple (as a custom value) and the next offset. */
  CAMLprim value dessser_read_tuple(value data_, value offset_, value max_sz_,
                                    value has_more_)
  {
    CAMLparam4(data_, offset_, max_sz_, has_more_);
    CAMLlocal1(ret);
    ret = caml_alloc_tuple(2);
    Store_field(ret, 0, Val_unit);  // TODO
    Store_field(ret, 1, Val_long(100)); // TODO
    CAMLreturn(ret);
  }

  /* Takes a tuple (as a custom value) and return its size */
  CAMLprim value dessser_sersize_of_tuple(value tuple_)
  {
    CAMLparam1(tuple_);
    CAMLreturn(Val_long(42)); // TODO
  }

  /* Takes a TX, an offset and a tuple (as a custom value) and serialize
   * the tuple into the TX. Returns the actual size. */
  CAMLprim value dessser_serialize_tuple(value tx_, value offset_, value tuple_)
  {
    CAMLparam3(tx_, offset_, tuple_);
    CAMLreturn(Val_long(42)); // TODO
  }
}
|}
end

let dessser_type_of_ramen_tuple tup =
  if tup = [] then
    DT.(make Unit)
  else
    let tup = Array.of_list tup in
    DT.(make (Rec (Array.map (fun ft ->
      (ft.RamenTuple.name :> string), ft.typ
    ) tup)))

let make_env _env =
  [] (* TODO *)

(* Emit the function that will return the next input tuple read from the input
 * ringbuffer, from the passed tx and start offset.
 * The function has to return the deserialized value. *)
let read_in_tuple in_type =
  let cmt =
    Printf.sprintf2 "Deserialize a tuple of type %a"
      DT.print_maybe_nullable in_type in
  let open DE.Ops in
  let tx_t = DT.(Value (make (get_user_type "tx"))) in
  DE.func2 tx_t DT.Size (fun _l tx start_offs ->
    if in_type.DT.vtyp = DT.Unit then (
      if in_type.DT.nullable then
        not_null unit
      else
        unit
    ) else (
      let src = apply (identifier "CodeGenLib_Dessser.pointer_of_tx") [ tx ] in
      let src = data_ptr_add src start_offs in
      let v_src = RingBuf2Value.make in_type src in
      first v_src
    )) |>
  comment cmt

(* Emit the where functions *)
let where_clause ?(with_group=false) ~env in_type out_prev_type global_state_type
                 group_state_type expr =
  ignore env ; (* TODO *)
  let args =
    if with_group then
      DT.[| Value global_state_type ; Value in_type ; Value out_prev_type ;
            Value group_state_type |]
    else
      DT.[| Value global_state_type ; Value in_type ; Value out_prev_type |] in
  let l = make_env env in
  let open DE.Ops in
  DE.func ~l args (fun _l _f_id ->
    (* Add the function parameters to the environment for getting global
     * states, input and previous output tuples, and optional local states *)
    (* TODO once stateful operations are supported in RaQL2DIL *)
    seq
      [ (* Update the environment used by that expression: *)
        (* TODO *)
        (* Compute the boolean result: *)
        RaQL2DIL.expression expr ])

let cmp_for ~env vtyp left_nullable right_nullable =
  let open DE.Ops in
  (* Start from a normal comparison function that returns -1/0/1: *)
  let base_cmp a b =
    if_ ~cond:(gt a b)
        ~then_:(i8 Int8.one)
        ~else_:(
      if_ ~cond:(gt b a)
          ~then_:(i8 (Int8.of_int ~-1))
          ~else_:(i8 Int8.zero)) in
  let l = make_env env in
  DE.func2 ~l DT.(Value (make ~nullable:left_nullable vtyp))
              DT.(Value (make ~nullable:right_nullable vtyp)) (fun _l a b ->
    match left_nullable, right_nullable with
    | false, false ->
        base_cmp a b
    | true, true ->
        if_ ~cond:(and_ (is_null a) (is_null b))
            ~then_:(i8 Int8.zero)
            ~else_:(
          if_ ~cond:(is_null a)
              ~then_:(i8 (Int8.of_int ~-1))
              ~else_:(
            if_ ~cond:(is_null b)
                ~then_:(i8 Int8.one)
                ~else_:(base_cmp a b)))
    | true, false ->
        if_ ~cond:(is_null a)
            ~then_:(i8 (Int8.of_int ~-1))
            ~else_:(base_cmp a b)
    | false, true ->
        if_ ~cond:(is_null b)
            ~then_:(i8 Int8.one)
            ~else_:(base_cmp a b))

let emit_cond0_in ~env in_type global_state_type e =
  ignore env ; (* TODO *)
  let cmt =
    Printf.sprintf2 "The part of the commit condition that depends solely on \
                     the input tuple: %a"
      (E.print false) e in
  let open DE.Ops in
  let l = make_env env in
  (* input tuple -> global state -> something *)
  DE.func2 ~l (DT.Value in_type) (Value global_state_type)
    (fun _l _in_ _global_ ->
      (* add_tuple_environment In in_type env in: TODO *)
      (* Update the states used by this expression: TODO *)
      (* emit_state_update_for_expr ~env ~opc ~what:"commit clause 0, in" e ; *)
      RaQL2DIL.expression e) |>
  comment cmt

let emit_cond0_out ~env minimal_type out_prev_type global_state_type
                   group_state_type e =
  ignore env ; (* TODO *)
  let cmt =
    Printf.sprintf2 "The part of the commit condition that depends on the \
                     output tuple: %a"
      (E.print false) e in
  let open DE.Ops in
  let l = make_env env in
  (* minimal tuple -> previous out -> global state -> local state -> thing *)
  DE.func4 ~l (DT.Value minimal_type) (Value out_prev_type)
           (Value global_state_type) (Value group_state_type)
    (fun _l _min_ _out_previous_ _group_ _global_ ->
      (* Add Out and OutPrevious to the environment: TODO *)
      (* add_tuple_environment Out minimal_type env |>
         add_tuple_environment OutPrevious opc.typ in *)
      (* Update the states used by this expression: TODO *)
      (* emit_state_update_for_expr ~env ~opc ~what:"commit clause 0, out" e *)
    RaQL2DIL.expression e) |>
  comment cmt

let commit_when_clause ~env in_type minimal_type out_prev_type
                       global_state_type group_state_type e =
  ignore env ; (* TODO *)
  let cmt =
    Printf.sprintf2 "The bulk of the commit condition: %a"
      (E.print false) e in
  let open DE.Ops in
  let l = make_env env in
  (* input tuple -> minimal tuple -> previous out -> global state ->
   * local state -> bool *)
  DE.func5 ~l (DT.Value in_type) (DT.Value minimal_type) (Value out_prev_type)
           (Value global_state_type) (Value group_state_type)
    (fun _l _in_ _min_ _out_previous_ _group_ _global_ ->
      (* add_tuple_environment In in_type env TODO *)
      (* add_tuple_environment Out minimal_typ TODO *)
      (* Update the states used by this expression: TODO *)
      (* emit_state_update_for_expr ~env ~opc ~what:"commit clause" e ; *)
      RaQL2DIL.expression e) |>
  comment cmt

(* Build a dummy functio  of the desired type: *)
let dummy_function ins out =
  let ins = Array.map (fun mn -> DT.Value mn) ins in
  DE.func ins (fun _l _fid ->
    DE.default_value ~allow_null:true out)

(* When there is no way to extract a numeric value to order group for
 * optimising the commit condition, pass those placeholder functions to
 * [CodeGenLib_Skeleton.aggregate]: *)
let default_commit_cond commit_cond in_type minimal_type
                        out_prev_type group_state_type global_state_type =
  (* We are free to pick whatever type for group_order_type: *)
  let group_order_type = DT.(make Unit) in
  let dummy_cond0_left_op =
    dummy_function [| in_type |] group_order_type
  and dummy_cond0_right_op =
    dummy_function [| minimal_type ; out_prev_type ; group_state_type ;
                      global_state_type |] group_order_type
  and dummy_cond0_cmp =
    dummy_function [| group_order_type ; group_order_type |] DT.(make (Mac I8)) in
  false,
  dummy_cond0_left_op,
  dummy_cond0_right_op,
  dummy_cond0_cmp,
  false,
  commit_cond

(* Returns the set of functions/flags required for
 * [CodeGenLib_Skeleton.aggregate] to process the commit condition, trying
 * to optimize by splitting the condition in two, with one sortable part: *)
let optimize_commit_cond ~env func_name in_type minimal_type out_prev_type
                         group_state_type global_state_type commit_cond =
  let es = E.as_nary E.And commit_cond in
  (* TODO: take the best possible sub-condition not the first one: *)
  let env = make_env env in
  let rec loop rest = function
    | [] ->
        !logger.warning "Cannot find a way to optimise the commit \
                         condition of function %a"
          N.func_print func_name ;
        default_commit_cond commit_cond in_type minimal_type
                            out_prev_type group_state_type global_state_type
    | e :: es ->
        (match Helpers.defined_order e with
        | exception Not_found ->
            !logger.debug "Expression %a does not define an ordering"
              (E.print false) e ;
            loop (e :: rest) es
        | f, neg, op, g ->
            !logger.debug "Expression %a defines an ordering"
              (E.print false) e ;
            let true_when_eq = op = Ge in
            (* The type of the values used to sort the commit condition: *)
            let group_order_type = T.large_enough_for f.typ.vtyp g.typ.vtyp in
            let may_neg e =
              (* Let's add an unary minus in front of [ e ] it we are supposed
               * to neg the Greater operator, and type it by hand: *)
              if neg then
                E.make ~vtyp:e.E.typ.DT.vtyp ~nullable:e.typ.DT.nullable
                       ?units:e.units (Stateless (SL1 (Minus, e)))
              else e in
            let cond0_cmp =
              cmp_for ~env group_order_type f.typ.DT.nullable g.typ.DT.nullable in
            let cond0_in =
              emit_cond0_in ~env in_type global_state_type (may_neg f) |>
              RaQL2DIL.conv ~from:f.typ.vtyp ~to_:group_order_type in
            let cond0_out =
              emit_cond0_out ~env minimal_type out_prev_type global_state_type
                             group_state_type (may_neg g) |>
              RaQL2DIL.conv ~from:g.typ.vtyp ~to_:group_order_type in
            let rem_cond =
              E.of_nary ~vtyp:commit_cond.typ.vtyp
                        ~nullable:commit_cond.typ.DT.nullable
                        ~units:commit_cond.units
                        E.And (List.rev_append rest es) in
            true, cond0_in, cond0_out, cond0_cmp, true_when_eq, rem_cond) in
  loop [] es

(* Similar to emit_field_selection but with less options, no concept of star and no
 * naming of the fields as the fields from out, since that's not the out tuple
 * we are constructing: *)
let key_of_input ~env in_type key =
  let cmt =
    Printf.sprintf2 "The group-by key: %a"
      (List.print (E.print false)) key in
  let open DE.Ops in
  let l = make_env env in
  DE.func1 ~l (DT.Value in_type) (fun _l _in_ ->
    (* Add in_ in the environment: *)
    (* let env = add_tuple_environment In in_typ env in TODO *)
    make_tup (List.map RaQL2DIL.expression key)) |>
  comment cmt

(* The vectors OutPrevious is nullable: the commit when and
 * select clauses of aggregate operations either have it or not.
 * Each time they need access to a field they call a function "maybe_XXX_"
 * with that nullable tuple, which avoids propagating out_typ down to
 * emit_expr - but hopefully the compiler will inline this. *)
let maybe_field out_type field_name field_type =
  let cmt =
    Printf.sprintf2 "Extract field of %a (type %a) from optional prev-tuple"
      N.field_print field_name
      DT.print_maybe_nullable field_type in
  let nullable_out_type = DT.not_null out_type in
  let open DE.Ops in
  DE.func1 (DT.Value nullable_out_type) (fun _l prev_out ->
    if_ ~cond:(is_null prev_out)
        ~then_:(null field_type.DT.vtyp)
        ~else_:(
          let field_val =
            get_field (field_name :> string) (force prev_out) in
          if field_type.DT.nullable then
            (* Return the field as is: *)
            field_val
          else
            (* Make it nullable: *)
            not_null field_val)) |>
  comment cmt

let fold_fields mn i f =
  match mn.DT.vtyp with
  | DT.Rec mns ->
      Array.fold_left (fun i (field_name, field_type) ->
        f i (N.field field_name) field_type
      ) i mns
  | _ ->
      !logger.warning "Type %a has no fields!"
        DT.print_maybe_nullable mn ;
      i

(* If [build_minimal] is true, the env is updated and only those fields present
 * in minimal tuple are build (only those required by commit_cond and
 * update_states).  If false, the minimal tuple computed above is passed as an
 * extra parameter to the function, which only have to build the final
 * out_tuple (taking advantage of the fields already computed in minimal_type),
 * and need not update states.
 * Notice that there are no notion of deep selection at this point, as input
 * fields have been flattened by now. *)
let select_record ~build_minimal ~env min_fields out_fields in_type
                  minimal_type out_prev_type
                  global_state_type group_state_type =
  let field_in_minimal field_name =
    Array.exists (fun (n, _t) ->
      n = (field_name : N.field :> string)
    ) min_fields in
  let must_output_field field_name =
    not build_minimal || field_in_minimal field_name in
  (* let env = TODO
    add_tuple_environment In in_typ env |>
    add_tuple_environment OutPrevious opc.typ in *)
  (* And optionaly:
    add_tuple_environment Out minimal_typ env *)
  let open DE.Ops in
  let params =
    if build_minimal then
      DT.[| Value in_type ; Value out_prev_type ; Value group_state_type ;
            Value global_state_type |]
    else
      DT.[| Value in_type ; Value out_prev_type ; Value group_state_type ;
            Value global_state_type ; Value minimal_type |]
    in
  let l = make_env env in
  DE.func ~l params (fun l fid ->
    (* Bind each expression to a variable in the order of the select clause
     * (aka. user order) so that previously bound variables can be used in
     * the following expressions.
     * Those identifiers are named after the fields themselves, but must not
     * shadow the passed function parameters ("in_", "global_", etc) so they are
     * transformed by `id_of_field_name]: *)
    let id_of_field_name f =
      "id_"^ (f : N.field :> string) ^"_" in
    let rec loop l rec_args = function
      | [] ->
          (* Once all the values are bound to identifiers, build the record: *)
          if rec_args = [] then unit else make_rec rec_args
      | sf :: out_fields' ->
          if must_output_field sf.O.alias then (
            if build_minimal then (
              (* Update the states as required for this field, just before
               * computing the field actual value. *)
              (* TODO: update state
              let what = (sf.O.alias :> string) in
              emit_state_update_for_expr ~env ~opc ~what sf.O.expr *)
            ) ;
            let id_name = id_of_field_name sf.alias in
            let cmt =
              Printf.sprintf2 "Output field %a of type %a"
                N.field_print sf.alias
                DT.print_maybe_nullable sf.expr.typ in
            let value =
              if not build_minimal && field_in_minimal sf.alias then (
                (* We already have this binding in the parameter: *)
                get_field (sf.alias :> string) (param fid 4 (* minimal *))
              ) else (
                (* So that we have a single out_type both before and after tuples
                 * generation: *)
                if E.is_generator sf.expr then unit
                else RaQL2DIL.expression sf.expr
              ) in
            (* Make that field available in the environment for later users: *)
            let l' = (identifier id_name, DT.Value sf.expr.typ) :: l in
            let rec_args' =
              string (sf.alias :> string) :: identifier id_name :: rec_args in
            let_ id_name value ~in_:(loop l' rec_args' out_fields') |>
            comment cmt
          ) else (
            loop l rec_args out_fields'
          ) in
    loop l [] out_fields)

let select_clause ~build_minimal ~env out_fields in_type minimal_type out_type
                  out_prev_type global_state_type group_state_type =
  let open DE.Ops in
  let cmt =
    Printf.sprintf2 "Build the %s tuple of type %a"
      (if build_minimal then "minimal" else "output")
      DT.print_maybe_nullable
        (if build_minimal then minimal_type else out_type) in
  (* TODO: non record types for I/O: *)
  (match minimal_type.DT.vtyp, out_type.vtyp with
  | DT.Rec min_fields, DT.Rec _ ->
      select_record ~build_minimal ~env min_fields out_fields in_type
                    minimal_type out_prev_type
                    global_state_type group_state_type
  | _ ->
      todo "select_clause for non-record types") |>
  comment cmt

let update_states ~env _in_type _minimal_type _out_fields =
  ignore env ;
  todo "update_states"

(* First serious user type needed: event times *)
module EventTime =
struct
  let field_source_type =
    DT.(Sum [| "OutputField", required Unit ;
               "Parameter", required Unit |])

  let field_type =
    DT.(Tup [| required (Mac String) ;
               required field_source_type ;
               required (Mac Float) |])

  let event_start = field_type

  let event_duration =
    DT.(Sum [| "DurationConst", required (Mac Float) ;
               "DurationField", required field_type ;
               "StopField", required field_type |])

  let t =
    DT.(Tup [| required event_start ; required event_duration |])

  let to_ramen_event_time =
    (* TODO: emit some "asm" code to convert into RamenEventTime.t *)
    ()
end

let id_of_prefix tuple =
  String.nreplace (string_of_variable tuple) "." "_"

let id_of_field_name ?(tuple=In) field_name =
  let id =
    match (field_name : N.field :> string) with
    (* Note: we have a '#count' for the sort tuple. *)
    | "#count" -> "virtual_"^ id_of_prefix tuple ^"_count_"
    | field -> id_of_prefix tuple ^"_"^ field ^"_" in
  DE.Ops.identifier id

let event_time et out_type params =
  let (sta_field, sta_src, sta_scale), dur = et in
  let open RamenEventTime in
  let open DE.Ops in
  let field_value_to_float field_name = function
    | OutputField ->
        (* This must not fail if RamenOperation.check did its job *)
        (match out_type.DT.vtyp with
        | DT.Rec mns ->
            let f = array_assoc (field_name : N.field :> string) mns in
            let e =
              RaQL2DIL.conv_maybe_nullable
                ~from:f ~to_:DT.(make (Mac Float))
                (id_of_field_name ~tuple:Out field_name) in
            if f.nullable then coalesce [ e ; float 0. ]
                          else e
        | _ ->
            assert false) (* Event time output field only usable on records *)
    | Parameter ->
        let param = RamenTuple.params_find field_name params in
        RaQL2DIL.conv
          ~from:param.ptyp.typ.vtyp ~to_:(Mac Float)
          (id_of_field_name ~tuple:Param field_name)
  in
  let_
    "start_"
    (mul (field_value_to_float sta_field !sta_src) (float sta_scale))
    ~in_:(
      let stop =
        match dur with
        | DurationConst d ->
            add (identifier "start_") (float d)
        | DurationField (dur_field, dur_src, dur_scale) ->
            add (identifier "start_")
                (mul (field_value_to_float dur_field !dur_src)
                     (float dur_scale))
        | StopField (sto_field, sto_src, sto_scale) ->
            mul (field_value_to_float sto_field !sto_src)
                (float sto_scale) in
      pair (identifier "start_") stop)

let time_of_tuple et_opt out_type params =
  let open DE.Ops in
  DE.func1 (DT.Value out_type) (fun _l tuple ->
    match et_opt with
    | None -> seq [ ignore tuple ; null EventTime.t ]
    | Some et -> not_null (event_time et out_type params))

(* The sort_expr functions take as parameters the number of entries sorted, the
 * first entry, the last, the smallest and the largest, and compute a value
 * used to sort incoming entries, either a boolean (for sort_until) or any
 * value that can be mapped to numerics (for sort_by). *)
let sort_expr in_type es =
  let open DE.Ops in
  let cmt = "Sort helper" in
  let l = None (* TODO *) in
  let in_t = DT.Value in_type in
  DE.func5 ?l DT.(Value (required (Mac U64))) in_t in_t in_t in_t
           (fun _l _count _first _last _smallest _greatest ->
    (*let env = (* TODO *)
      add_tuple_environment SortFirst in_typ [] |>
      add_tuple_environment In in_typ |>
      add_tuple_environment SortSmallest in_typ |>
      add_tuple_environment SortGreatest in_typ in *)
    match es with
    | [] ->
        (* The default sort_until clause must be false.
         * If there is no sort_by clause, any constant will do: *)
        false_
    | es ->
        (* Output a tuple made of all the expressions: *)
        make_tup (List.map RaQL2DIL.expression es)) |>
  comment cmt

(* Generate a function that, given the out tuples, will return the list of
 * notification names to send, along with all output values as strings: *)
(* TODO: shouldn't CodeGenLib pass this func the global and also maybe
 * the group states? *)
let get_notifications out_type es =
  let open DE.Ops in
  let cmt = "List of notifications" in
  let string_t = DT.(required (Mac String)) in
  let field_value_t = DT.(required (Tup [| string_t ; string_t |])) in
  let l = None (* TODO *) in
  DE.func1 ?l (DT.Value out_type) (fun _l v_out ->
    (*let env = (* TODO *)
      add_tuple_environment In in_typ [] *)
    let names = make_lst string_t (List.map RaQL2DIL.expression es) in
    let values =
      T.map_fields (fun n mn ->
        make_tup [
          string n ;
          RaQL2DIL.conv_maybe_nullable ~from:mn ~to_:string_t
                                       (get_field n v_out) ]
      ) out_type.DT.vtyp |>
      Array.to_list |>
      make_lst field_value_t in
    pair names values) |>
  comment cmt

(* Output the code required for the function operation and returns the new
 * code.
 * Named [emit_full_operation] in reference to [emit_half_operation] for half
 * workers. *)
let emit_aggregate _entry_point code add_expr func_op func_name
                   global_state_env group_state_env
                   env_env param_env globals_env in_type params =
  (* The input type (computed from parent output type and the deep selection
   * of field), aka `tuple_in in CodeGenLib_Skeleton: *)
  let in_type = dessser_type_of_ramen_tuple in_type in
  (* That part of the output value that needs to be computed for every input
   * even when no output is emmitted, aka `minimal_out: *)
  let minimal_type = Helpers.minimal_type func_op |>
                     dessser_type_of_ramen_tuple in
  (* The tuple storing the global state, aka `global_state: *)
  let global_state_type = DT.make Unit in (* TODO *)
  (* The tuple storing the group local state, aka `group_state: *)
  let group_state_type = DT.make Unit in (* TODO *)
  (* The output type of values passed to the final output generator: *)
  let generator_out_type = DT.make Unit in (* TODO *)
  (* Same, nullable: *)
  let out_prev_type = DT.{ generator_out_type with nullable = true } in
  (* The output type: *)
  let out_type = O.out_record_of_operation ~with_private:false func_op in
  (* Extract required info from the operation definition: *)
  let where, commit_cond, key, out_fields, sort, notifications =
    match func_op with
    | O.Aggregate { where ; commit_cond ; key ; fields ; sort ; notifications ;
                    _ } ->
        where, commit_cond, key, fields, sort, notifications
    | _ -> assert false in
  let base_env = param_env @ env_env @ globals_env in
  let global_base_env = global_state_env @ base_env in
  let group_global_env = group_state_env @ global_base_env in
  (* When filtering, the worker has two options:
   * It can check an incoming tuple as soon as it receives it, or it can
   * first compute the group key and retrieve the group state, and then
   * check the tuple. The later, slower option is required when the WHERE
   * expression uses anything from the group state (such as local function
   * states or group tuple).
   * It is best to partition the WHERE expression in two so that as much of
   * it can be checked as early as possible. *)
  let where_fast, where_slow =
    E.and_partition (not % CodeGen_OCaml.expr_needs_group) where in
  let code =
    fail_with_context "coding for tuple reader" (fun () ->
      read_in_tuple in_type |>
      add_expr code "read_in_tuple_") in
  let code =
    fail_with_context "coding for where-fast function" (fun () ->
      where_clause ~env:global_base_env in_type out_prev_type global_state_type
                   group_state_type where_fast |>
      add_expr code "where_fast_") in
  let code =
    fail_with_context "coding for where-slow function" (fun () ->
      where_clause ~with_group:true ~env:group_global_env in_type out_prev_type
                   global_state_type group_state_type where_slow |>
      add_expr code "where_slow_") in
  let has_commit_cond, cond0_left_op, cond0_right_op, cond0_cmp,
      cond0_true_when_eq, commit_cond_rest =
    if Helpers.check_commit_for_all commit_cond then
      fail_with_context "coding for optimized commit condition" (fun () ->
        optimize_commit_cond ~env:group_global_env func_name in_type
                             minimal_type out_prev_type group_state_type
                             global_state_type commit_cond)
    else
      (* No need to optimize: *)
      default_commit_cond commit_cond in_type minimal_type
                          out_prev_type group_state_type global_state_type in
  let code =
    add_expr code "commit_has_commit_cond_" (DE.Ops.bool has_commit_cond) in
  let code =
    add_expr code "commit_cond0_left_op_" cond0_left_op in
  let code =
    add_expr code "commit_cond0_right_op_" cond0_right_op in
  let code =
    add_expr code "commit_cond0_cmp_" cond0_cmp in
  let code =
    add_expr code "commit_cond0_true_when_eq_"
             (DE.Ops.bool cond0_true_when_eq) in
  let code =
    fail_with_context "coding for commit condition function" (fun () ->
      commit_when_clause
        ~env:group_global_env in_type minimal_type out_prev_type
        global_state_type group_state_type commit_cond_rest |>
      add_expr code "commit_cond_") in
  let code =
    fail_with_context "coding for key extraction function" (fun () ->
      key_of_input ~env:global_base_env in_type key |>
      add_expr code "key_of_input_") in
  let code =
    fail_with_context "coding for optional-field getter functions" (fun () ->
      fold_fields out_type code (fun code field_name field_type ->
        let fun_name = "maybe_"^ (field_name : N.field :> string) ^"_" in
        maybe_field out_type field_name field_type |>
        add_expr code fun_name)) in
  let code =
    fail_with_context "coding for select-clause function" (fun () ->
      select_clause ~build_minimal:true ~env:group_global_env out_fields
                    in_type minimal_type out_type out_prev_type
                    global_state_type group_state_type |>
      add_expr code "minimal_tuple_of_group_") in
  let code =
    fail_with_context "coding for output function" (fun () ->
      select_clause ~build_minimal:false ~env:group_global_env out_fields
                    in_type minimal_type out_type out_prev_type
                    global_state_type group_state_type |>
      add_expr code "out_tuple_of_minimal_tuple_") in
  let code =
    fail_with_context "coding for state update function" (fun () ->
      update_states ~env:group_global_env in_type minimal_type out_fields |>
      add_expr code "update_states") in
  let code =
    fail_with_context "coding for sersize-of-tuple function" (fun () ->
      sersize_of_type out_type |>
      add_expr code "sersize_of_tuple_") in
  let code =
    fail_with_context "coding for time-of-tuple function" (fun () ->
      let et = O.event_time_of_operation func_op in
      time_of_tuple et out_type params |>
      add_expr code "time_of_tuple_") in
  let code =
    fail_with_context "coding for tuple serializer" (fun () ->
      serialize out_type |>
      add_expr code "serialize_tuple_") in
  let code =
    fail_with_context "coding for tuple generator" (fun () ->
      generate_tuples in_type out_type out_fields |>
      add_expr code "generate_tuples_") in
  let code =
    fail_with_context "coding for sort-until function" (fun () ->
      sort_expr in_type (match sort with Some (_, Some u, _) -> [u] | _ -> []) |>
      add_expr code "sort_until_") in
  let code =
    fail_with_context "coding for sort-by function" (fun () ->
      sort_expr in_type (match sort with Some (_, _, b) -> b | None -> []) |>
      add_expr code "sort_by_") in
  let code =
    fail_with_context "coding for notification extraction function" (fun () ->
      get_notifications out_type notifications |>
      add_expr code "get_notifications_") in
  let code =
    fail_with_context "coding for default in/out tuples" (fun () ->
      DE.default_value in_type |>
      add_expr code "default_in_" ;
      DE.default_value out_type |>
      add_expr code "default_out_") in
  code

(* Trying to be backend agnostic, generate all the DIL functions required for
 * the given RaQL function.
 * Eventually those will be compiled to OCaml in order to be easily mixed
 * with [CodeGenLib_Skeleton]. *)
let generate_code
      conf func_name func_op in_type
      env_env param_env globals_env global_state_env group_state_env
      obj_name _params_mod_name _dessser_mod_name
      _orc_write_func _orc_read_func params
      _globals_mod_name =
  let backend = (module BackEndOCaml : BACKEND) in (* TODO: a parameter *)
  let module BE = (val backend : BACKEND) in
  let code = BE.make_state () in
  (* We will need a few helper functions: *)
  if not (DT.is_registered "tx") then
    (* No need to be specific about this type but at least name it: *)
    DT.register_user_type "tx" DT.(Mac U8) ;
  let pointer_of_tx_t =
    DT.Function ([| DT.(Value (make (get_user_type "tx"))) |], DataPtr) in
  let code =
    BE.add_external_identifier code "CodeGenLib_Dessser.pointer_of_tx"
                               pointer_of_tx_t in
  (* Make all other functions unaware of the backend with this shorthand: *)
  let add_expr code name d =
    let code, _, _ = BE.add_identifier_of_expression code ~name d in
    code in
  (* Coding for ORC wrappers *)
  (* TODO *)
  (* Coding for factors extractor *)
  (* TODO *)
  (* Coding for all functions required to implement the worker: *)
  let code =
    match func_op with
    | O.Aggregate _ ->
        emit_aggregate EntryPoints.worker code add_expr
                       func_op func_name global_state_env group_state_env
                       env_env param_env globals_env in_type params
    | _ ->
        todo "Non aggregate functions" in
  (* Coding for replay worker: *)
  (* TODO *)
  (* Coding for archive convert functions: *)
  (* TODO *)
  (* Now write all those definitions into a file and compile it: *)
  let src_file =
    RamenOCamlCompiler.with_code_file_for
      obj_name conf.C.reuse_prev_files (fun oc ->
        fail_with_context "emit worker code" (fun () ->
          let p fmt = emit oc 0 fmt in
          p "(* Dessser definitions for worker %a: *)"
            N.func_print func_name ;
          BE.print_definitions code oc) ;
      ) in
  let what = "function "^ N.func_color func_name in
  RamenOCamlCompiler.compile conf ~keep_temp_files:conf.C.keep_temp_files
                             what src_file obj_name
