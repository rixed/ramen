(* Make use of external library Dessser for code generation *)
open Batteries
open Dessser
open Stdint

open RamenConsts
open RamenHelpers
open RamenHelpersNoLog
open RamenLang
open RamenLog

module C = RamenConf
module DE = DessserExpressions
module DP = DessserPrinter
module DT = DessserTypes
module DU = DessserCompilationUnit
module E = RamenExpr
module EntryPoints = RamenConstsEntryPoints
module Files = RamenFiles
module Helpers = CodeGen_Helpers
module N = RamenName
module O = RamenOperation
module Orc = RamenOrc
module T = RamenTypes
module RaQL2DIL = CodeGen_RaQL2DIL

module Value2RingBuf = DessserHeapValue.Serialize (DessserRamenRingBuffer.Ser)
module RingBuf2Value = DessserHeapValue.Materialize (DessserRamenRingBuffer.Des)

module RowBinary2Value = DessserHeapValue.Materialize (DessserRowBinary.Des)
let rowbinary_to_value ?config mn =
  let open DE.Ops in
  comment "Function deserializing the rowbinary into a heap value:"
    (DE.func1 DataPtr (fun l src -> RowBinary2Value.make ?config mn l src))

module Csv2Value = DessserHeapValue.Materialize (DessserCsv.Des)
let csv_to_value ?config mn =
  let open DE.Ops in
  comment "Function deserializing the CSV into a heap value:"
    (DE.func1 DataPtr (fun l src -> Csv2Value.make ?config mn l src))

(* Returns a DIL function that returns the total size of the serialized value
 * filtered by the passed fieldmask: *)
let sersize_of_type mn =
  let cmt =
    Printf.sprintf2 "Compute the serialized size of values of type %a"
      DT.print_maybe_nullable mn in
  let open DE.Ops in
  DE.func2 Mask (Value mn) (fun l ma v ->
    (* Value2RingBuf.sersize returns the fixed and the variable sizes, that
     * have to be added together: *)
    let_ ~l ~name:"size_pair" (Value2RingBuf.sersize mn l ma v) (fun _l pair ->
      add (first pair) (secnd pair))) |>
  comment cmt

(* Takes a fieldmask, a tx, an offset and a heap value, and returns the new
 * offset. *)
let serialize mn =
  let cmt =
    Printf.sprintf2 "Serialize a value of type %a"
      DT.print_maybe_nullable mn in
  let open DE.Ops in
  let tx_t = DT.(Value (required (Ext "tx"))) in
  DE.func4 DT.Mask tx_t DT.Size (DT.Value mn) (fun l ma tx start_offs v ->
    let tx_size = apply (ext_identifier "RingBuf.tx_size") [ tx ] in
    let msg_size = sub tx_size start_offs in
    let dst = data_ptr_of_buffer msg_size in
    let_ ~l ~name:"ser_dst" (Value2RingBuf.serialize mn l ma v dst) (fun _l dst ->
      (* Then copy the buffer back into that TX: *)
      seq
        [ apply (ext_identifier "CodeGenLib_Dessser.blit_into_tx") [ tx ; dst ] ;
          add (data_ptr_offset dst) start_offs ])) |>
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
let add_identifier_of_expression ?name f compunit e =
  try
    f compunit ?name e
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
  module BE = DessserBackEndOCaml

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
      | DT.Unknown | Unit | Mac _ | Ext _ ->
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
      (* Emits an array of maybe_nullable values. [mns] has the field names
       * that must be prefixed with the module name to reach the fields in
       * Dessser generated code. *)
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

  (* Emits the function that reads a CSV line and returns an out tuple.
   * Notice that in_mn and out_mn differs only in ordering. *)
  let emit_reader deserializer in_mn out_mn oc =
    let p fmt = emit oc 0 fmt in
    let compunit = DU.make () in
    let compunit, _, value_of_ser =
      deserializer in_mn |>
      add_identifier_of_expression
        ~name:"value_of_ser" U.add_identifier_of_expression compunit in
(* Unused for now, require the output type [mn] to be record-sorted:
    let compunit, _, _sersize_of_type =
      sersize_of_type mn DE.Ops.copy_field |>
      add_identifier_of_expression ~name:"sersize_of_type" DU.add_identifier_of_expression compunit in
    let compunit, _, _serialize =
      serialize mn |>
      add_identifier_of_expression ~name:"serialize" DU.add_identifier_of_expression compunit in
*)
    p "(* Helpers for deserializing type:\n\n%a\n\n*)\n"
      DT.print_maybe_nullable in_mn ;
    BE.print_definitions oc compunit ;
    (* A public entry point to unserialize the tuple with a more meaningful
     * name, and which also convert the tuple representation.
     * Indeed, CodeGen_OCaml uses actual tuples for tuples while Dessser uses
     * more convenient records with mutable fields.
     * Assuming there are no embedded records, then it's enough to convert
     * the outer level which is trivially done here. *)
    p "" ;
    p "open DessserOCamlBackEndHelpers" ;
    p "" ;
    p "let read_tuple buffer start stop _has_more =" ;
    p "  assert (stop >= start) ;" ;
    p "  let src = Pointer.of_bytes buffer start stop in" ;
    p "  let heap_value, src' = DessserGen.%s src in" value_of_ser ;
    p "  let read_sz = Pointer.sub src' src" ;
    p "  and tuple =" ;
    emit_ramen_of_dessser_value ~depth:2 out_mn oc "heap_value" ;
    p "  in" ;
    p "  tuple, read_sz"
end

module CPP =
struct
  module BE = DessserBackEndCPP

  let emit_reader deserializer in_mn _out_mn oc =
    let compunit = DU.make () in
    let compunit, _, _value_of_ser =
      deserializer in_mn |>
      add_identifier_of_expression ~name:"value_of_ser" DU.add_identifier_of_expression compunit in
(* Unused for now, require the output type [mn] to be record-sorted:
    let compunit, _, _sersize_of_type =
      sersize_of_type mn DE.Ops.copy_field |>
      add_identifier_of_expression ~name:"sersize_of_type" DU.add_identifier_of_expression compunit in
    let compunit, _, _serialize =
      serialize mn |>
      add_identifier_of_expression ~name:"serialize" DU.add_identifier_of_expression compunit in
*)
    Printf.fprintf oc "/* Helpers for function:\n\n%a\n\n*/\n"
      DT.print_maybe_nullable in_mn ;
    BE.print_definitions oc compunit ;
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

let state_init ~r_env state_lifespan global_state_type
               where commit_cond out_fields =
  let name_of_state e =
    "state_"^ string_of_int e.E.uniq_num in
  let fold_unpure_fun i f =
    CodeGen_OCaml.fold_unpure_fun_my_lifespan
      state_lifespan out_fields ~where ~commit_cond i f in
  let cmt =
    Printf.sprintf2 "Initialize the %s state"
      (E.string_of_state_lifespan state_lifespan) in
  let param_t =
    if state_lifespan = E.GlobalState then
      DT.(Value (required Unit))
    else
      DT.Value global_state_type
  in
  let open DE.Ops in
  DE.func1 param_t (fun d_env p ->
    (* TODO: add paramerter p to the env (if it's the global state, depending
     * on state_lifespan *)
    ignore p ;
    make_rec (
      fold_unpure_fun [] (fun l f ->
        let e = RaQL2DIL.init_state ~r_env ~d_env f in
        if e = seq [] then l else
        let n = name_of_state f in
        (n, e) :: l))) |>
  comment cmt

(* Emit the function that will return the next input tuple read from the input
 * ringbuffer, from the passed tx and start offset.
 * The function has to return the deserialized value. *)
let deserialize_tuple ~d_env mn =
  let cmt =
    Printf.sprintf2 "Deserialize a tuple of type %a"
      DT.print_maybe_nullable mn in
  let open DE.Ops in
  let tx_t = DT.(Value (required (Ext "tx"))) in
  DE.func2 ~l:d_env tx_t DT.Size (fun l tx start_offs ->
    if mn.DT.vtyp = DT.Unit then (
      if mn.DT.nullable then
        not_null unit
      else
        unit
    ) else (
      let src = apply (ext_identifier "CodeGenLib_Dessser.pointer_of_tx") [ tx ] in
      let src = data_ptr_add src start_offs in
      let v_src = RingBuf2Value.make mn l src in
      first v_src
    )) |>
  comment cmt

(* Emit the where functions *)
let where_clause ?(with_group=false) ~r_env ~d_env in_type out_prev_type
                 global_state_type group_state_type expr =
  let args =
    if with_group then
      DT.[| Value global_state_type ; Value in_type ; Value out_prev_type ;
            Value group_state_type |]
    else
      DT.[| Value global_state_type ; Value in_type ; Value out_prev_type |] in
  let open DE.Ops in
  DE.func ~l:d_env args (fun d_env _f_id ->
    (* Add the function parameters to the environment for getting global
     * states, input and previous output tuples, and optional local states *)
    (* TODO once stateful operations are supported in RaQL2DIL *)
    seq
      [ (* Update the environment used by that expression: *)
        (* TODO *)
        (* Compute the boolean result: *)
        RaQL2DIL.expression ~r_env ~d_env expr ])

let cmp_for vtyp left_nullable right_nullable =
  let open DE.Ops in
  (* Start from a normal comparison function that returns -1/0/1: *)
  let base_cmp a b =
    if_ ~cond:(gt a b)
        ~then_:(i8 Int8.one)
        ~else_:(
      if_ ~cond:(gt b a)
          ~then_:(i8 (Int8.of_int ~-1))
          ~else_:(i8 Int8.zero)) in
  DE.func2
    DT.(Value (make ~nullable:left_nullable vtyp))
    DT.(Value (make ~nullable:right_nullable vtyp)) (fun _d_env a b ->
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

let emit_cond0_in ~r_env ~d_env in_type global_state_type e =
  let cmt =
    Printf.sprintf2 "The part of the commit condition that depends solely on \
                     the input tuple: %a"
      (E.print false) e in
  let open DE.Ops in
  (* input tuple -> global state -> something *)
  DE.func2 ~l:d_env (DT.Value in_type) (Value global_state_type)
    (fun d_env _in_ _global_ ->
      (* add_tuple_environment In in_type env in: TODO *)
      (* Update the states used by this expression: TODO *)
      let what = "commit clause 0, in" in
      seq
        [ RaQL2DIL.state_update_for_expr ~r_env ~d_env ~what e ;
          RaQL2DIL.expression ~r_env ~d_env e ]) |>
  comment cmt

let emit_cond0_out ~r_env ~d_env minimal_type out_prev_type global_state_type
                   group_state_type e =
  let cmt =
    Printf.sprintf2 "The part of the commit condition that depends on the \
                     output tuple: %a"
      (E.print false) e in
  let open DE.Ops in
  (* minimal tuple -> previous out -> global state -> local state -> thing *)
  DE.func4 ~l:d_env
    (DT.Value minimal_type) (Value out_prev_type)
    (Value global_state_type) (Value group_state_type)
    (fun d_env _min _out_previous _group _global ->
      (* Add Out and OutPrevious to the environment: TODO *)
      (* add_tuple_environment Out minimal_type env |>
         add_tuple_environment OutPrevious opc.typ in *)
      (* Update the states used by this expression: TODO *)
      let what = "commit clause 0, out" in
      seq
        [ RaQL2DIL.state_update_for_expr ~r_env ~d_env ~what e ;
          RaQL2DIL.expression ~r_env ~d_env e ]) |>
  comment cmt

let commit_when_clause ~r_env ~d_env in_type minimal_type out_prev_type
                       global_state_type group_state_type e =
  let cmt =
    Printf.sprintf2 "The bulk of the commit condition: %a"
      (E.print false) e in
  let open DE.Ops in
  (* input tuple -> minimal tuple -> previous out -> global state ->
   * local state -> bool *)
  DE.func5 ~l:d_env (DT.Value in_type) (Value out_prev_type)
           (Value group_state_type) (Value global_state_type)
           (Value minimal_type)
    (fun d_env _in _out_previous _group _global _min ->
      (* add_tuple_environment In in_type env TODO *)
      (* add_tuple_environment Out minimal_type TODO *)
      (* Update the states used by this expression: TODO *)
      let what = "commit clause" in
      seq
        [ RaQL2DIL.state_update_for_expr ~r_env ~d_env ~what e ;
          RaQL2DIL.expression ~r_env ~d_env e ]) |>
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
    dummy_function [| in_type ; global_state_type |] group_order_type
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
let optimize_commit_cond ~r_env ~d_env func_name in_type minimal_type out_prev_type
                         group_state_type global_state_type commit_cond =
  let es = E.as_nary E.And commit_cond in
  (* TODO: take the best possible sub-condition not the first one: *)
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
              cmp_for group_order_type f.typ.DT.nullable g.typ.DT.nullable in
            let cond0_in =
              emit_cond0_in ~r_env ~d_env in_type global_state_type (may_neg f) |>
              RaQL2DIL.conv ~to_:group_order_type d_env in
            let cond0_out =
              emit_cond0_out ~r_env ~d_env minimal_type out_prev_type
                             global_state_type group_state_type (may_neg g) |>
              RaQL2DIL.conv ~to_:group_order_type d_env in
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
let key_of_input ~r_env ~d_env in_type key =
  let cmt =
    Printf.sprintf2 "The group-by key: %a"
      (List.print (E.print false)) key in
  let open DE.Ops in
  DE.func1 ~l:d_env (DT.Value in_type) (fun d_env _in_ ->
    (* Add in_ in the environment: *)
    (* let env = add_tuple_environment In in_typ env in TODO *)
    make_tup (List.map (RaQL2DIL.expression ~r_env ~d_env) key)) |>
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
 * update_states), and the function outputs a minimal_out record. If false, the
 * minimal tuple computed above is passed as an extra parameter to the
 * function, which only have to build the final out_tuple (taking advantage of
 * the fields already computed in minimal_type), and need not update states.
 * Notice that there are no notion of deep selection at this point, as input
 * fields have been flattened by now. *)
let select_record ~r_env ~d_env ~build_minimal min_fields out_fields in_type
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
  (* And optionally:
    add_tuple_environment Out minimal_type env *)
  let open DE.Ops in
  let params =
    if build_minimal then
      DT.[| Value in_type ; Value out_prev_type ; Value group_state_type ;
            Value global_state_type |]
    else
      DT.[| Value in_type ; Value out_prev_type ; Value group_state_type ;
            Value global_state_type ; Value minimal_type |]
    in
  let cmt =
    Printf.sprintf2 "output an out_tuple of type %a"
      (List.print (fun oc sf -> N.field_print oc sf.O.alias)) out_fields in
  DE.func ~l:d_env params (fun d_env fid ->
    let r_env =
      (E.RecordValue In, param fid 0) ::
      (E.RecordValue OutPrevious, param fid 1) ::
      (E.RecordValue Group, param fid 2) ::
      (E.RecordValue Global, param fid 3) :: r_env in
    let r_env =
      if not build_minimal then
        (E.RecordValue Out, param fid 4) :: r_env
      else
        r_env in
    (* Bind each expression to a variable in the order of the select clause
     * (aka. user order) so that previously bound variables can be used in
     * the following expressions.
     * Those identifiers are named after the fields themselves, but must not
     * shadow the passed function parameters ("in_", "global_", etc) so they are
     * transformed by `id_of_field_name]: *)
    let id_of_field_name f =
      "id_"^ (f : N.field :> string) ^"_" in
    let rec loop r_env d_env rec_args = function
      | [] ->
          (* Once all the values are bound to identifiers, build the record: *)
          make_rec rec_args
      | sf :: out_fields ->
          if must_output_field sf.O.alias then (
            let updater =
              if build_minimal then (
                (* Update the states as required for this field, just before
                 * computing the field actual value. *)
                let what = (sf.O.alias :> string) in
                RaQL2DIL.state_update_for_expr ~r_env ~d_env ~what sf.O.expr
              ) else nop in
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
                else RaQL2DIL.expression ~r_env ~d_env sf.expr
              ) in
            seq [ updater ;
                  let_ ~l:d_env ~name:id_name value (fun d_env id ->
                    (* Beware that [let_] might optimise away the actual
                     * binding so better remember that [id]: *)
                    (* Make that field available in the environment for later
                     * users: *)
                    let d_env = (id, DT.Value sf.expr.typ) :: d_env in
                    let rec_args = ((sf.alias :> string), id) :: rec_args in
                    (* Also install an override for this field so that if
                     * out.$this_field is referenced in what follows the we
                     * will read [id] instead. *)
                    let r_env = (E.RecordField (Out, sf.alias), id) :: r_env in
                    loop r_env d_env rec_args out_fields) ] |>
            comment cmt
          ) else (
            (* This field is not part of minimal_out, but we want minimal out
             * to have the same number of fields the out_type with just units
             * as placeholders for missing fields.
             * Note: the exact same type of minimal_out must be output, ie. same
             * field name for the placeholder and unit type. *)
            let cmt =
              Printf.sprintf2 "Placeholder for field %a"
                N.field_print sf.alias in
            let fname = Helpers.not_minimal_field_name sf.alias in
            let rec_args = ((fname :> string), unit) :: rec_args in
            loop r_env d_env rec_args out_fields |>
            comment cmt
          ) in
    loop r_env d_env [] out_fields) |>
  comment cmt

let select_clause ~r_env ~d_env ~build_minimal out_fields in_type minimal_type
                  out_type out_prev_type global_state_type group_state_type =
  let open DE.Ops in
  let cmt =
    Printf.sprintf2 "Build the %s tuple of type %a"
      (if build_minimal then "minimal" else "output")
      DT.print_maybe_nullable
        (if build_minimal then minimal_type else out_type) in
  (* TODO: non record types for I/O: *)
  (match minimal_type.DT.vtyp, out_type.vtyp with
  | DT.Rec min_fields, DT.Rec _ ->
      select_record ~r_env ~d_env ~build_minimal min_fields out_fields in_type
                    minimal_type out_prev_type
                    global_state_type group_state_type
  | _ ->
      todo "select_clause for non-record types") |>
  comment cmt

(* Fields that are part of the minimal tuple have had their states updated
 * while the minimal tuple was computed, but others have not. Let's do this
 * here: *)
let update_states ~r_env ~d_env in_type minimal_type out_prev_type
                  group_state_type global_state_type out_fields =
  let field_in_minimal field_name =
    match minimal_type.DT.vtyp with
    | DT.Unit ->
        false
    | DT.Rec mns ->
        Array.exists (fun (n, _) -> n = (field_name : N.field :> string)) mns
    | _ ->
        assert false
  in
  let open DE.Ops in
  let cmt = "Updating the state of fields not in the minimal tuple" in
  let l = d_env (* TODO *) in
  DE.func5 ~l (DT.Value in_type) (Value out_prev_type) (Value group_state_type)
           (Value global_state_type) (Value minimal_type)
    (fun d_env _in _out_previous _group _global _min ->
      (*let env = (* TODO *) *)
      List.fold_left (fun l sf ->
        if field_in_minimal sf.O.alias then l else (
          (* Update the states as required for this field, just before
           * computing the field actual value. *)
          let what = (sf.O.alias :> string) in
          RaQL2DIL.state_update_for_expr ~r_env ~d_env ~what sf.O.expr :: l)
      ) [] out_fields |>
      seq) |>
  comment cmt

let id_of_prefix tuple =
  String.nreplace (string_of_variable tuple) "." "_"

let id_of_field_name ?(tuple=In) field_name =
  let id =
    match (field_name : N.field :> string) with
    (* Note: we have a '#count' for the sort tuple. *)
    | "#count" -> "virtual_"^ id_of_prefix tuple ^"_count_"
    | field -> id_of_prefix tuple ^"_"^ field ^"_" in
  DE.Ops.identifier id

(* Return a DIL function returning the start and end time (as a pair of floats)
 * of a given output tuple *)
let event_time ~d_env et out_type params =
  let (sta_field, { contents = sta_src }, sta_scale), dur = et in
  let open RamenEventTime in
  let open DE.Ops in
  let default_zero t e =
    if t.DT.nullable then
      if_ ~cond:(is_null e) ~then_:(float 0.) ~else_:e
    else
      e in
  let field_value_to_float d_env field_name = function
    | OutputField ->
        (* This must not fail if RamenOperation.check did its job *)
        (match out_type.DT.vtyp with
        | DT.Rec mns ->
            let f = array_assoc (field_name : N.field :> string) mns in
            let e =
              RaQL2DIL.conv_maybe_nullable
                ~to_:DT.(make (Mac Float)) d_env
                (id_of_field_name ~tuple:Out field_name) in
            default_zero f e
        | _ ->
            assert false) (* Event time output field only usable on records *)
    | Parameter ->
        let param = RamenTuple.params_find field_name params in
        let e =
          RaQL2DIL.conv
            ~to_:(Mac Float) d_env
            (id_of_field_name ~tuple:Param field_name) in
        default_zero param.ptyp.typ e
  in
  let_ ~name:"start_" ~l:d_env
       (mul (field_value_to_float d_env sta_field sta_src)
            (float sta_scale))
    (fun l start ->
      let stop =
        match dur with
        | DurationConst d ->
            add start (float d)
        | DurationField (dur_field, dur_src, dur_scale) ->
            add start
                (mul (field_value_to_float l dur_field !dur_src)
                     (float dur_scale))
        | StopField (sto_field, sto_src, sto_scale) ->
            mul (field_value_to_float l sto_field !sto_src)
                (float sto_scale) in
      apply (ext_identifier "CodeGenLib_Dessser.make_float_pair")
            [ start ; stop ])

(* Return a DIL function returning the optional start and end times of a
 * given output tuple *)
let time_of_tuple ~d_env et_opt out_type params =
  let open DE.Ops in
  DE.func1 (DT.Value out_type) (fun _l tuple ->
    match et_opt with
    | None ->
        seq [ ignore_ tuple ; null (Ext "float_pair") ]
    | Some et ->
        not_null (event_time ~d_env et out_type params))

(* The sort_expr functions take as parameters the number of entries sorted, the
 * first entry, the last, the smallest and the largest, and compute a value
 * used to sort incoming entries, either a boolean (for sort_until) or any
 * value that can be mapped to numerics (for sort_by). *)
let sort_expr ~r_env ~d_env in_type es =
  let open DE.Ops in
  let cmt = "Sort helper" in
  let l = d_env (* TODO *) in
  let in_t = DT.Value in_type in
  DE.func5 ~l DT.(Value (required (Mac U64))) in_t in_t in_t in_t
           (fun d_env _count _first _last _smallest _greatest ->
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
        make_tup (List.map (RaQL2DIL.expression ~r_env ~d_env) es)) |>
  comment cmt

(* Returns an expression that convert an OCaml value into a RamenTypes.value of
 * the given RamenTypes.t. This is useful for instance to get hand off the
 * factors to CodeGenLib. [v] is the DIL expression to get the runtime value. *)
(* TODO: Move this function into RamenValue aka RamenTypes *)
let rec raql_of_dil_value ~d_env mn v =
  let open DE.Ops in
  let_ ~name:"v" ~l:d_env v (fun d_env v ->
    if mn.DT.nullable then
      if_ ~cond:(is_null v)
        ~then_:(ext_identifier "RamenTypes.VNull")
        ~else_:(
          let mn' = DT.{ mn with nullable = false } in
          raql_of_dil_value ~d_env mn' (force v))
    else
      (* As far as Dessser's OCaml backend is concerned, constructor are like
       * functions: *)
      let p f = apply (ext_identifier ("RamenTypes."^ f)) [ v ] in
      match mn.DT.vtyp with
      | DT.Unknown | Ext _ -> assert false
      | Unit -> ext_identifier "RamenTypes.VUnit"
      | Mac Float -> p "VFloat"
      | Mac String -> p "VString"
      | Mac Bool -> p "VBool"
      | Mac Char -> p "VChar"
      | Mac U8 -> p "VU8"
      | Mac U16 -> p "VU16"
      | Mac U24 -> p "VU24"
      | Mac U32 -> p "VU32"
      | Mac U40 -> p "VU40"
      | Mac U48 -> p "VU48"
      | Mac U56 -> p "VU56"
      | Mac U64 -> p "VU64"
      | Mac U128 -> p "VU128"
      | Mac I8 -> p "VI8"
      | Mac I16 -> p "VI16"
      | Mac I24 -> p "VI24"
      | Mac I32 -> p "VI32"
      | Mac I40 -> p "VI40"
      | Mac I48 -> p "VI48"
      | Mac I56 -> p "VI56"
      | Mac I64 -> p "VI64"
      | Mac I128 -> p "VI128"
      | Usr { name = "Eth" ; _ } -> p "VEth"
      | Usr { name = "Ip4" ; _ } -> p "VIpv4"
      | Usr { name = "Ip6" ; _ } -> p "VIpv6"
      | Usr { name = "Ip" ; _ } -> p "VIp"
      | Usr { name = "Cidr4" ; _ } -> p "VCidrv4"
      | Usr { name = "Cidr6" ; _ } -> p "VCidrv6"
      | Usr { name = "Cidr" ; _ } -> p "VCidr"
      | Usr { def ; _ } ->
          raql_of_dil_value ~d_env DT.(make (develop_value_type def)) v
      | Tup mns ->
          apply (ext_identifier "RamenTypes.make_vtup") (
            Array.mapi (fun i mn ->
              raql_of_dil_value ~d_env mn (get_item i v)
            ) mns |> Array.to_list)
      | Rec _
      | Vec _
      | Lst _ ->
          todo "raql_of_dil_value for rec/vec/lst"
      | Map _ -> assert false (* No values of that type *)
      | Sum _ -> invalid_arg "raql_of_dil_value for Sum type"
      | Set _ -> assert false (* No values of that type *))

(* Returns a DIL function that returns a Lst of [factor_value]s *)
let factors_of_tuple func_op out_type =
  let cmt = "Extract factors from the output tuple" in
  let typ = O.out_type_of_operation ~with_priv:false func_op in
  let factors = O.factors_of_operation func_op in
  let open DE.Ops in
  (* Note: we need no environment at the start of [factors_of_tuple] *)
  DE.func1 (DT.Value out_type) (fun d_env v_out ->
    List.map (fun factor ->
      let t = (List.find (fun t -> t.RamenTuple.name = factor) typ).typ in
      apply (ext_identifier "CodeGenLib_Dessser.make_factor_value")
            [ string (factor :> string) ;
              raql_of_dil_value ~d_env t (get_field (factor :> string) v_out) ]
    ) factors |>
    make_lst DT.(required (Ext "factor_value"))) |>
  comment cmt

let print_path oc path =
  List.print (Tuple2.print DT.print_maybe_nullable Int.print) oc path

(* Return the expression reaching path [path] in heap value [v]: *)
let rec extractor path ~d_env v =
  let open DE.Ops in
  match path with
  | (mn, 0) :: [] ->
      raql_of_dil_value ~d_env mn v
  | (DT.{ vtyp = Vec _ ; nullable = false }, i) :: rest ->
      let v = get_vec v (u32_of_int i) in
      extractor rest ~d_env v
  | (DT.{ vtyp = Tup _ ; nullable = false }, i) :: rest ->
      let v = get_item i v in
      extractor rest ~d_env v
  | (DT.{ vtyp = Rec mns ; nullable = false }, i) :: rest ->
      let v = get_field (fst mns.(i)) v in
      extractor rest ~d_env v
  | (DT.{ nullable = true ; vtyp }, i) :: rest ->
      if_
        ~cond:(is_null v)
        ~then_:(null vtyp)
        ~else_:(
          let path = (DT.{ nullable = false ; vtyp }, i) :: rest in
          not_null (extractor path ~d_env (force v)))
  | _ ->
      !logger.error "Cannot build extractor for path %a"
        print_path path ;
      assert false

let extractor_t out_type =
  DT.(Function ([| Value out_type |],
                Value (required (Ext "ramen_value"))))

let scalar_extractors out_type =
  let open DE.Ops in
  let cmt = "Extract scalar values from the output tuple" in
  let extractors = ref (eol (extractor_t out_type)) in
  O.iter_scalars_with_path out_type (fun i path ->
    let cmt =
      Printf.sprintf2 "extractor #%d for path %a" i print_path path in
    let f =
      DE.func1 (DT.Value out_type) (fun d_env v_out ->
        extractor path ~d_env v_out) |>
      comment cmt in
    extractors := cons f !extractors) ;
  apply (ext_identifier "CodeGenLib_Dessser.make_extractors_vector")
        [ !extractors ] |>
  comment cmt

(* Generate a function that, given the out tuples, will return the list of
 * notification names to send, along with all output values as strings: *)
(* TODO: shouldn't CodeGenLib pass this func the global and also maybe
 * the group states? *)
let get_notifications ~d_env out_type es =
  let open DE.Ops in
  let cmt = "List of notifications" in
  let string_t = DT.(required (Mac String)) in
  let string_pair_t = DT.(required (Ext "string_pair")) in
  DE.func1 ~l:d_env (DT.Value out_type) (fun d_env v_out ->
    (*let env = (* TODO *)
      add_tuple_environment In in_typ [] *)
    if es = [] then
      pair (make_lst string_t []) (make_lst string_pair_t [])
    else
      let names =
        make_lst string_t (List.map (RaQL2DIL.expression ~r_env:[] ~d_env) es) in
      let values =
        T.map_fields (fun n _ ->
          apply (ext_identifier "CodeGenLib_Dessser.make_string_pair")
            [ string n ;
              RaQL2DIL.conv_maybe_nullable ~to_:string_t d_env
                                           (get_field n v_out) ]
        ) out_type.DT.vtyp |>
        Array.to_list |>
        make_lst string_pair_t in
      pair names values) |>
  comment cmt

let call_aggregate compunit id_name sort key commit_before flush_how
                   check_commit_for_all =
  let f_name = "CodeGenLib_Skeletons.aggregate" in
  let compunit =
    let l = DU.environment compunit in
    let aggregate_t =
      let open DE.Ops in
      DT.Function ([|
        DE.type_of l (identifier "read_in_tuple_") ;
        DE.type_of l (identifier "sersize_of_tuple_") ;
        DE.type_of l (identifier "time_of_tuple_") ;
        DE.type_of l (identifier "factors_of_tuple_") ;
        DE.type_of l (identifier "scalar_extractors_") ;
        DE.type_of l (identifier "serialize_tuple_") ;
        DE.type_of l (identifier "generate_tuples_") ;
        DE.type_of l (identifier "minimal_tuple_of_group_") ;
        DE.type_of l (identifier "update_states_") ;
        DE.type_of l (identifier "out_tuple_of_minimal_tuple_") ;
        DT.u32 ;
        DE.type_of l (identifier "sort_until_") ;
        DE.type_of l (identifier "sort_by_") ;
        DE.type_of l (identifier "where_fast_") ;
        DE.type_of l (identifier "where_slow_") ;
        DE.type_of l (identifier "key_of_input_") ;
        DT.bool ;
        DE.type_of l (identifier "commit_cond_") ;
        DE.type_of l (identifier "commit_has_commit_cond_") ;
        DE.type_of l (identifier "commit_cond0_left_op_") ;
        DE.type_of l (identifier "commit_cond0_right_op_") ;
        DE.type_of l (identifier "commit_cond0_cmp_") ;
        DE.type_of l (identifier "commit_cond0_true_when_eq_") ;
        DT.bool ;
        DT.bool ;
        DT.bool ;
        DE.type_of l (identifier "global_init_") ;
        DE.type_of l (identifier "group_init_") ;
        DE.type_of l (identifier "get_notifications_") ;
        DE.type_of l (identifier "every_") ;
        DE.type_of l (identifier "default_in_") ;
        DE.type_of l (identifier "default_out_") ;
        DE.type_of l (identifier "orc_make_handler_") ;
        DE.type_of l (ext_identifier "orc_write") ;
        DE.type_of l (ext_identifier "orc_close") |],
      DT.unit) in
    DU.add_external_identifier compunit f_name aggregate_t in
  let cmt = "Entry point for aggregate full worker" in
  let open DE.Ops in
  let e =
    DE.func0 (fun _l ->
      apply (ext_identifier f_name) [
        identifier "read_in_tuple_" ;
        identifier "sersize_of_tuple_" ;
        identifier "time_of_tuple_" ;
        identifier "factors_of_tuple_" ;
        identifier "scalar_extractors_" ;
        identifier "serialize_tuple_" ;
        identifier "generate_tuples_" ;
        identifier "minimal_tuple_of_group_" ;
        identifier "update_states_" ;
        identifier "out_tuple_of_minimal_tuple_" ;
        u32_of_int (match sort with None -> 0 | Some (n, _, _) -> n) ;
        identifier "sort_until_" ;
        identifier "sort_by_" ;
        identifier "where_fast_" ;
        identifier "where_slow_" ;
        identifier "key_of_input_" ;
        bool (key = []) ;
        identifier "commit_cond_" ;
        identifier "commit_has_commit_cond_" ;
        identifier "commit_cond0_left_op_" ;
        identifier "commit_cond0_right_op_" ;
        identifier "commit_cond0_cmp_" ;
        identifier "commit_cond0_true_when_eq_" ;
        bool commit_before ;
        bool (flush_how <> O.Never) ;
        bool check_commit_for_all ;
        identifier "global_init_" ;
        identifier "group_init_" ;
        identifier "get_notifications_" ;
        identifier "every_" ;
        identifier "default_in_" ;
        identifier "default_out_" ;
        identifier "orc_make_handler_" ;
        ext_identifier "orc_write" ;
        ext_identifier "orc_close" ]) |>
    comment cmt in
  let compunit, _, _ =
    DU.add_identifier_of_expression compunit ~name:id_name e in
  compunit

let where_top ~r_env ~d_env where_fast in_type =
  (* The filter used by the top-half must be a partition of the normal
   * where_fast filter selecting only the part that use only pure
   * functions and no previous out tuple.
   * A partition of a filter is the separation of the ANDed clauses of a
   * filter according a any criteria on expressions (first part being the
   * part of the condition which all expressions fulfill the condition).
   * We could then reuse filter partitioning to optimise the filtering in
   * the normal case by moving part of the where into the where_fast,
   * before here we use partitioning again to extract the top-half
   * version of the where_fast.
   * Note that the tuples surviving the top-half filter will again be
   * filtered against the full fast_filter. *)
  let expr_needs_global_tuples =
    Helpers.expr_needs_tuple_from
      [ OutPrevious; SortFirst; SortSmallest; SortGreatest ] in
  let where, _ =
    E.and_partition (fun e ->
      E.is_pure e && not (expr_needs_global_tuples e)
    ) where_fast in
  let open DE.Ops in
  DE.func1 ~l:d_env DT.(Value in_type) (fun d_env _in ->
    (* TODO: env
    let env =
      add_tuple_environment In in_typ base_env in *)
    RaQL2DIL.expression ~r_env ~d_env where) |>
  comment "where clause for top-half"

let call_top_half compunit id_name =
  let f_name = "CodeGenLib_Skeletons.top_half" in
  let compunit =
    let l = DU.environment compunit in
    let top_half_t =
      let open DE.Ops in
      DT.Function ([|
        DE.type_of l (identifier "read_in_tuple_") ;
        DE.type_of l (identifier "top_where_") |],
      DT.unit) in
    DU.add_external_identifier compunit f_name top_half_t in
  let cmt = "Entry point for aggregate top-half worker" in
  let open DE.Ops in
  let e =
    DE.func1 DT.unit (fun _l _unit ->
      apply (ext_identifier f_name) [
        identifier "read_in_tuple_" ;
        identifier "top_where_" ]) |>
      comment cmt in
  let compunit, _, _ =
    DU.add_identifier_of_expression compunit ~name:id_name e in
  compunit

(* Output the code required for the function operation and returns the new
 * code.
 * Named [emit_full_operation] in reference to [emit_half_operation] for half
 * workers. *)
let emit_aggregate ~r_env compunit add_expr func_op func_name
                   in_type out_type params =
  (* Gather the globals that have already been declared (envs, params and
   * globals): *)
  let d_env = DU.environment compunit in
  (* The input type (computed from parent output type and the deep selection
   * of field), aka `tuple_in in CodeGenLib_Skeleton: *)
  let in_type = dessser_type_of_ramen_tuple in_type in
  (* That part of the output value that needs to be computed for every input
   * even when no output is emitted, aka `minimal_out: *)
  let minimal_type = Helpers.minimal_type func_op |>
                     dessser_type_of_ramen_tuple in
  (* The tuple storing the global state, aka `global_state: *)
  let global_state_type = DT.make Unit in (* TODO *)
  (* The tuple storing the group local state, aka `group_state: *)
  let group_state_type = DT.make Unit in (* TODO *)
  (* The output type of values passed to the final output generator: *)
  let generator_out_type = out_type in (* TODO *)
  (* Same, nullable: *)
  let out_prev_type = DT.{ generator_out_type with nullable = true } in
  (* Extract required info from the operation definition: *)
  let where, commit_before, commit_cond, key, out_fields, sort, flush_how,
      notifications, every =
    match func_op with
    | O.Aggregate { where ; commit_before ; commit_cond ; key ; fields ; sort ;
                    flush_how ; notifications ; every ; _ } ->
        where, commit_before, commit_cond, key, fields, sort, flush_how,
        notifications, every
    | _ -> assert false in
  (* The worker will have a global state and a local one per group, stored in
   * its snapshotted state. The first functions needed are those that create
   * the initial state for the global and local states. *)
  let compunit =
    fail_with_context "coding for global state initializer" (fun () ->
      state_init ~r_env E.GlobalState global_state_type
                 where commit_cond out_fields |>
      add_expr compunit "global_init_") in
  let compunit =
    fail_with_context "coding for group state initializer" (fun () ->
      state_init ~r_env E.LocalState  global_state_type
                 where commit_cond out_fields |>
      add_expr compunit "group_init_") in
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
  let compunit =
    fail_with_context "coding for tuple reader" (fun () ->
      deserialize_tuple ~d_env in_type |>
      add_expr compunit "read_in_tuple_") in
  let compunit =
    fail_with_context "coding for where-fast function" (fun () ->
      where_clause ~r_env ~d_env in_type out_prev_type global_state_type
                   group_state_type where_fast |>
      add_expr compunit "where_fast_") in
  let compunit =
    fail_with_context "coding for where-slow function" (fun () ->
      where_clause ~r_env ~d_env ~with_group:true in_type out_prev_type
                   global_state_type group_state_type where_slow |>
      add_expr compunit "where_slow_") in
  let check_commit_for_all = Helpers.check_commit_for_all commit_cond in
  let has_commit_cond, cond0_left_op, cond0_right_op, cond0_cmp,
      cond0_true_when_eq, commit_cond_rest =
    if check_commit_for_all then
      fail_with_context "coding for optimized commit condition" (fun () ->
        optimize_commit_cond ~r_env ~d_env func_name in_type
                             minimal_type out_prev_type group_state_type
                             global_state_type commit_cond)
    else
      (* No need to optimize: *)
      default_commit_cond commit_cond in_type minimal_type
                          out_prev_type group_state_type global_state_type in
  let compunit =
    add_expr compunit "commit_has_commit_cond_" (DE.Ops.bool has_commit_cond) in
  let compunit =
    add_expr compunit "commit_cond0_left_op_" cond0_left_op in
  let compunit =
    add_expr compunit "commit_cond0_right_op_" cond0_right_op in
  let compunit =
    add_expr compunit "commit_cond0_cmp_" cond0_cmp in
  let compunit =
    add_expr compunit "commit_cond0_true_when_eq_"
             (DE.Ops.bool cond0_true_when_eq) in
  let compunit =
    fail_with_context "coding for commit condition function" (fun () ->
      commit_when_clause
        ~r_env ~d_env in_type minimal_type out_prev_type
        global_state_type group_state_type commit_cond_rest |>
      add_expr compunit "commit_cond_") in
  let compunit =
    fail_with_context "coding for key extraction function" (fun () ->
      key_of_input ~r_env ~d_env in_type key |>
      add_expr compunit "key_of_input_") in
  let compunit =
    fail_with_context "coding for optional-field getter functions" (fun () ->
      fold_fields out_type compunit (fun compunit field_name field_type ->
        let fun_name = "maybe_"^ (field_name : N.field :> string) ^"_" in
        maybe_field out_type field_name field_type |>
        add_expr compunit fun_name)) in
  let compunit =
    fail_with_context "coding for select-clause function" (fun () ->
      select_clause ~r_env ~d_env ~build_minimal:true out_fields
                    in_type minimal_type out_type out_prev_type
                    global_state_type group_state_type |>
      add_expr compunit "minimal_tuple_of_group_") in
  let compunit =
    fail_with_context "coding for output function" (fun () ->
      select_clause ~r_env ~d_env ~build_minimal:false out_fields
                    in_type minimal_type out_type out_prev_type
                    global_state_type group_state_type |>
      add_expr compunit "out_tuple_of_minimal_tuple_") in
  let compunit =
    fail_with_context "coding for state update function" (fun () ->
      update_states ~r_env ~d_env in_type minimal_type out_prev_type
                    group_state_type global_state_type out_fields |>
      add_expr compunit "update_states_") in
  let compunit =
    fail_with_context "coding for sersize-of-tuple function" (fun () ->
      sersize_of_type out_type |>
      add_expr compunit "sersize_of_tuple_") in
  let compunit =
    fail_with_context "coding for time-of-tuple function" (fun () ->
      let et = O.event_time_of_operation func_op in
      time_of_tuple ~d_env et out_type params |>
      add_expr compunit "time_of_tuple_") in
  let compunit =
    fail_with_context "coding for tuple serializer" (fun () ->
      serialize out_type |>
      add_expr compunit "serialize_tuple_") in
  let compunit =
    fail_with_context "coding for tuple generator" (fun () ->
      generate_tuples in_type out_type out_fields |>
      add_expr compunit "generate_tuples_") in
  let compunit =
    fail_with_context "coding for sort-until function" (fun () ->
      sort_expr ~r_env ~d_env in_type
                (match sort with Some (_, Some u, _) -> [u] | _ -> []) |>
      add_expr compunit "sort_until_") in
  let compunit =
    fail_with_context "coding for sort-by function" (fun () ->
      sort_expr ~r_env ~d_env in_type
                (match sort with Some (_, _, b) -> b | None -> []) |>
      add_expr compunit "sort_by_") in
  let compunit =
    fail_with_context "coding for notification extraction function" (fun () ->
      get_notifications ~d_env out_type notifications |>
      add_expr compunit "get_notifications_") in
  let compunit =
    fail_with_context "coding for default input tuples" (fun () ->
      DE.default_value in_type |>
      add_expr compunit "default_in_") in
  let compunit =
    fail_with_context "coding for default output tuples" (fun () ->
      DE.default_value out_type |>
      add_expr compunit "default_out_") in
  let compunit =
    fail_with_context "coding for the 'every' clause" (fun () ->
      (match every with
      | Some e ->
          RaQL2DIL.expression ~r_env ~d_env e |>
          RaQL2DIL.conv ~to_:DT.(Mac Float) d_env
      | None ->
          DE.Ops.float 0.) |>
      add_expr compunit "every_") in
  let compunit =
    fail_with_context "coding for aggregate entry point" (fun () ->
      call_aggregate compunit EntryPoints.worker sort key commit_before
                     flush_how check_commit_for_all) in
  let compunit =
    fail_with_context "coding for top-where function" (fun () ->
      where_top ~r_env ~d_env where_fast in_type |>
      add_expr compunit "top_where_") in
  let compunit =
    fail_with_context "coding for top-half aggregate entry point" (fun () ->
      call_top_half compunit EntryPoints.top_half) in
  compunit

let orc_wrapper out_type orc_write_func orc_read_func ps oc =
  let p fmt = emit oc 0 fmt in
  let pub = T.filter_out_private out_type in
  p "(* A handler to be passed to the function generated by \
        emit_write_value: *)" ;
  p "type handler" ;
  p "" ;
  p "external orc_write : handler -> %s -> float -> float -> unit = %S"
    (DessserBackEndOCaml.type_identifier ps DT.(Value out_type))
    orc_write_func ;
  p "external orc_read_pub : \
       RamenName.path -> int -> (%s -> unit) -> (int * int) = %S"
    (DessserBackEndOCaml.type_identifier ps DT.(Value pub))
    orc_read_func ;
  (* Destructor do not seems to be called when the OCaml program exits: *)
  p "external orc_close : handler -> unit = \"orc_handler_close\"" ;
  p "" ;
  p "(* Parameters: schema * path * index * row per batch * batches per file * archive *)" ;
  p "external orc_make_handler : \
       string -> RamenName.path -> bool -> int -> int -> bool -> handler =" ;
  p "  \"orc_handler_create_bytecode_lol\" \"orc_handler_create\"" ;
  p "" ;
  (* A wrapper that inject missing private fields: *)
  p "let orc_read fname_ batch_sz_ k_ =" ;
  p "  orc_read_pub fname_ batch_sz_ (fun t_ -> k_ (out_of_pub_ t_))" ;
  p ""

let make_orc_handler name out_type oc _ps =
  let p fmt = emit oc 0 fmt in
  let schema = Orc.of_value_type out_type.DT.vtyp |>
               IO.to_string Orc.print in
  p "let %s = orc_make_handler %S" name schema

let out_of_pub out_type =
  let cmt = "add fake private fields to public tuple" in
  let open DE.Ops in
  let rec full mn pub =
    (* Important optimisation, as no compiler is going to notice if we end up
     * copying the same structure: *)
    if not (T.has_private_fields mn) then pub else
    let e =
      match (mn |> DT.develop_maybe_nullable).vtyp with
      | DT.Rec mns ->
          make_rec (
            Array.map (fun (n, mn) ->
              let v =
                if N.is_private (N.field n) then
                  DE.default_value ~allow_null:true mn
                else
                  get_field n pub in
              n, v
            ) mns |> Array.to_list)
      | Vec (_, mn) | Lst mn | Set mn ->
          map pub DE.(func1 DT.(Value mn) (fun _l v -> full mn v))
      | Tup mns ->
          make_tup (
            List.init (Array.length mns) (fun i ->
              let pub = get_item i pub in
              full mns.(i) pub))
      | Sum mns ->
          assert (Array.length mns > 0) ; (* because has_private_fields *)
          let rec next_alt i =
            let copy_v () =
              let label, mn = mns.(i) in
              let pub = get_alt label pub in
              if T.has_private_fields mn then
                construct mns i (full mn pub)
              else
                pub in
            if i = Array.length mns - 1 then copy_v () else
            if_ ~cond:(eq (label_of pub) (u16_of_int i))
                ~then_:(copy_v ())
                ~else_:(next_alt (i + 1)) in
          next_alt 0
      | _ ->
          pub in
    if mn.nullable then
      if_ ~cond:(is_null pub) ~then_:pub ~else_:(not_null e)
    else e in
  DE.func1 DT.(Value out_type) (fun _l pub ->
    full out_type pub) |>
  comment cmt

(* A function that reads the history and writes it according to some out_ref
 * under a given channel: *)
let replay compunit id_name func_op =
  let d_env = DU.environment compunit in
  let typ = O.out_record_of_operation ~with_priv:false func_op in
  let compunit, _, _ =
    fail_with_context "coding for tuple reader" (fun () ->
      deserialize_tuple ~d_env typ |>
      DU.add_identifier_of_expression compunit ~name:"read_pub_tuple_") in
  let open DE.Ops in
  let compunit, _, _ =
    fail_with_context "coding for read_out_tuple" (fun () ->
      let tx_t = DT.(Value (required (Ext "tx"))) in
      DE.func2 ~l:d_env tx_t DT.Size (fun _l tx offs ->
        let tup =
          apply (identifier "read_pub_tuple_") [ tx ; offs ] in
        apply (identifier "out_of_pub_") [ tup ]) |>
      DU.add_identifier_of_expression compunit ~name:"read_out_tuple_") in
  let f_name = "CodeGenLib_Skeletons.replay" in
  let compunit =
    let l = DU.environment compunit in
    let aggregate_t =
      let open DE.Ops in
      DT.Function ([|
        DE.type_of l (identifier "read_out_tuple_") ;
        DE.type_of l (identifier "sersize_of_tuple_") ;
        DE.type_of l (identifier "time_of_tuple_") ;
        DE.type_of l (identifier "factors_of_tuple_") ;
        DE.type_of l (identifier "scalar_extractors_") ;
        DE.type_of l (identifier "serialize_tuple_") ;
        DE.type_of l (identifier "orc_make_handler_") ;
        DE.type_of l (ext_identifier "orc_write") ;
        DE.type_of l (ext_identifier "orc_read") ;
        DE.type_of l (ext_identifier "orc_close") |],
      DT.unit) in
    DU.add_external_identifier compunit f_name aggregate_t in
  let cmt = "Entry point for the replay worker" in
  let e =
    DE.func0 (fun _l ->
      apply (ext_identifier f_name) [
        identifier "read_out_tuple_" ;
        identifier "sersize_of_tuple_" ;
        identifier "time_of_tuple_" ;
        identifier "factors_of_tuple_" ;
        identifier "scalar_extractors_" ;
        identifier "serialize_tuple_" ;
        identifier "orc_make_handler_" ;
        ext_identifier "orc_write" ;
        ext_identifier "orc_read" ;
        ext_identifier "orc_close" ]) |>
    comment cmt in
  let compunit, _, _ =
    DU.add_identifier_of_expression compunit ~name:id_name e in
  compunit

(* Trying to be backend agnostic, generate all the DIL functions required for
 * the given RaQL function.
 * Eventually those will be compiled to OCaml in order to be easily mixed
 * with [CodeGenLib_Skeleton]. *)
let generate_function
      conf func_name func_op in_type
      obj_name _params_mod_name _dessser_mod_name
      orc_write_func orc_read_func params
      global_mod_name
      envs_t params_t globals_t =
  (* The output type, in serialization order: *)
  let out_type = O.out_record_of_operation ~with_priv:true func_op in
  let backend = (module DessserBackEndOCaml : BACKEND) in (* TODO: a parameter *)
  let module BE = (val backend : BACKEND) in
  let compunit = DU.make () in
  let add_expr compunit name d =
    let compunit, _, _ = DU.add_identifier_of_expression compunit ~name d in
    compunit in
  (* The initial environment gives access to envs, params and globals: *)
  let r_env, compunit =
    let open DE.Ops in
    [ E.RecordValue Env, "envs_" , envs_t ;
      E.RecordValue Param, "params_", params_t ;
      E.RecordValue Global, "globals_", globals_t ] |>
    List.fold_left (fun (r_env, compunit) (var, var_name, var_t) ->
      let name = global_mod_name ^"."^ var_name
                 (* Prepare the name so that appending the field name will
                  * point at the record defined in the globals module not the
                  * local incarnation with the same name.
                  * TODO: Name the types used for the globals module (with
                  * [DU.name_type]) so we do not redefine the same type that
                  * OCaml compiler would refuse to unify - requires to know
                  * the module name (ie. type_id) though: *)
                 ^"."^ global_mod_name ^".DessserGen" in
      let id = ext_identifier name in
      let r_env = (var, id) :: r_env
      and compunit = DU.add_external_identifier compunit name var_t in
      r_env, compunit) ([], compunit) in
  (* Those three are just passed to the external function [aggregate] and so
   * their exact type is irrelevant.
   * We could have an explicit "Unchecked" type in Dessser for such cases
   * but we could as well pick any value for Unchecked such as Unit. *)
  let unchecked_t = DT.unit in
  let compunit =
    [ "orc_write" ; "orc_read" ; "orc_close" ] |>
    List.fold_left (fun compunit name ->
      DU.add_external_identifier compunit name unchecked_t
    ) compunit in
  (* This will actually not only define orc_make_handler_ but also orc_write
   * etc, but since they are all used together dependency on only one of them
   * is good enough. *)
  let compunit =
    let name = "orc_make_handler_" in
    let dependencies = [ "out_of_pub_" ]
    and backend = DessserBackEndOCaml.id
    and typ = unchecked_t in
    DU.add_verbatim_definition
      compunit ~name ~dependencies ~typ ~backend (fun oc ps ->
        orc_wrapper out_type orc_write_func orc_read_func ps oc ;
        make_orc_handler name out_type oc ps) in
  let compunit =
    fail_with_context "coding for out_of_pub_ function" (fun () ->
      out_of_pub out_type |>
      add_expr compunit "out_of_pub_") in
  (* We will also need a few helper functions: *)
  let compunit =
    DU.register_external_type compunit "tx" (fun _ps -> function
      | DessserBackEndOCaml.OCaml -> "RingBuf.tx"
      | _ -> todo "codegen for other backends than OCaml") in
  let compunit =
    let name = "CodeGenLib_Dessser.pointer_of_tx" in
    let pointer_of_tx_t =
      DT.Function ([| DT.(Value (required (Ext "tx"))) |], DataPtr) in
    DU.add_external_identifier compunit name pointer_of_tx_t in
  let compunit =
    let name = "CodeGenLib_Dessser.blit_into_tx" in
    let blit_into_tx_t =
      DT.Function ([| DT.(Value (required (Ext "tx"))) ; DataPtr |], DT.Void) in
    DU.add_external_identifier compunit name blit_into_tx_t in
  let compunit =
    let name = "RingBuf.tx_size" in
    let tx_size_t =
      DT.Function ([| DT.(Value (required (Ext "tx"))) |], Size) in
    DU.add_external_identifier compunit name tx_size_t in
  let compunit =
    DU.register_external_type compunit "ramen_value" (fun _ps -> function
      | DessserBackEndOCaml.OCaml -> "RamenTypes.value"
      | _ -> todo "codegen for other backends than OCaml") in
  (* Registger all RamenType.value types: *)
  let compunit =
    let name = "RamenTypes.VNull" in
    let t = DT.(Value (required (Ext "ramen_value"))) in
    (* Note on the above "required": a "ramen_value" is a RamenType.value, which
     * is never going to be nullable, since it's not even a maybe_nullable,
     * even when it's VNull. *)
    DU.add_external_identifier compunit name t in
  let compunit =
    let name = "RamenTypes.VUnit" in
    let t = DT.(Value (required (Ext "ramen_value"))) in
    DU.add_external_identifier compunit name t in
  (* Those are function-like: *)
  let compunit =
    DT.[ "VFloat", Mac Float ; "VString", Mac String ; "VBool", Mac Bool ;
         "VChar", Mac Char ;
         "VU8", Mac U8 ; "VU16", Mac U16 ; "VU24", Mac U24 ; "VU32", Mac U32 ;
         "VU40", Mac U40 ; "VU48", Mac U48 ; "VU56", Mac U56 ; "VU64", Mac U64 ;
         "VU128", Mac U128 ;
         "VI8", Mac I8 ; "VI16", Mac I16 ; "VI24", Mac I24 ; "VI32", Mac I32 ;
         "VI40", Mac I40 ; "VI48", Mac I48 ; "VI56", Mac I56 ; "VI64", Mac I64 ;
         "VI128", Mac I128 ;
         "VEth", get_user_type "Eth" ; "VIpv4", get_user_type "Ip4" ;
         "VIpv6", get_user_type "Ip6" ; "VIp", get_user_type "Ip" ;
         "VCidrv4", get_user_type "Cidr4" ; "VCidrv6", get_user_type "Cidr6" ;
         "VCidr", get_user_type "Cidr" ] |>
    List.fold_left (fun compunit (n, in_t) ->
      let name = "RamenTypes."^ n in
      let out_t = DT.(Value (required (Ext "ramen_value"))) in
      let t = DT.(Function ([| Value (required in_t) |], out_t)) in
      DU.add_external_identifier compunit name t
    ) compunit in
  let compunit =
    DU.register_external_type compunit "float_pair" (fun _ps -> function
      | DessserBackEndOCaml.OCaml -> "(float * float)"
      | _ -> todo "codegen for other backends than OCaml") in
  let compunit =
    let name = "CodeGenLib_Dessser.make_float_pair" in
    let t =
      DT.(Function ([| Value (required (Mac Float)) ;
                       Value (required (Mac Float)) |],
                    Value (required (Ext "float_pair")))) in
    DU.add_external_identifier compunit name t in
  let compunit =
    DU.register_external_type compunit "string_pair" (fun _ps -> function
      | DessserBackEndOCaml.OCaml -> "(string * string)"
      | _ -> todo "codegen for other backends than OCaml") in
  let compunit =
    let name = "CodeGenLib_Dessser.make_string_pair" in
    let t =
      DT.(Function ([| Value (required (Mac String)) ;
                       Value (required (Mac String)) |],
                    Value (required (Ext "string_pair")))) in
    DU.add_external_identifier compunit name t in
  let compunit =
    DU.register_external_type compunit "factor_value" (fun _ps -> function
      | DessserBackEndOCaml.OCaml -> "(string * RamenTypes.value)"
      | _ -> todo "codegen for other backends than OCaml") in
  let compunit =
    let name = "CodeGenLib_Dessser.make_factor_value" in
    let t =
      DT.(Function ([| Value (required (Mac String)) ;
                       Value (required (Ext "ramen_value")) |],
                    Value (required (Ext "factor_value")))) in
    DU.add_external_identifier compunit name t in
  let compunit =
    DU.register_external_type compunit "scalar_extractors" (fun ps -> function
      | DessserBackEndOCaml.OCaml ->
          BE.type_identifier ps (extractor_t out_type) ^" array"
      | _ ->
          todo "codegen for other backends than OCaml") in
  let compunit =
    let name = "CodeGenLib_Dessser.make_extractors_vector" in
    let t =
      DT.(Function ([| DT.SList (extractor_t out_type) |],
                    Value (required (Ext "scalar_extractors")))) in
    DU.add_external_identifier compunit name t in
  (* Coding for factors extractor *)
  let compunit =
    fail_with_context "coding for factors extractor" (fun () ->
      factors_of_tuple func_op out_type |>
      add_expr compunit "factors_of_tuple_") in
  (* Coding for the vector of scalar extractors: *)
  let compunit =
    fail_with_context "coding for scalar extractors" (fun () ->
      scalar_extractors out_type |>
      add_expr compunit "scalar_extractors_") in
  (* Coding for all functions required to implement the worker: *)
  let compunit =
    match func_op with
    | O.Aggregate _ ->
        emit_aggregate ~r_env compunit add_expr func_op func_name
                       in_type out_type params ;
    | _ ->
        todo "Non aggregate functions" in
  (* Coding for replay worker: *)
  let compunit =
    fail_with_context "coding for replay function" (fun () ->
      replay compunit EntryPoints.replay func_op) in
  (* Coding for archive convert functions: *)
  (* TODO *)
  (* Now write all those definitions into a file and compile it: *)
  let src_file =
    RamenOCamlCompiler.with_code_file_for
      obj_name conf.C.reuse_prev_files (fun oc ->
        fail_with_context "emitting worker code" (fun () ->
          let p fmt = emit oc 0 fmt in
          p "(* Dessser definitions for worker %a: *)"
            N.func_print func_name ;
          BE.print_definitions oc compunit ;
          Printf.fprintf oc "let %s = DessserGen.%s\n"
            EntryPoints.worker
            EntryPoints.worker ;
          Printf.fprintf oc "let %s = DessserGen.%s\n"
            EntryPoints.top_half
            EntryPoints.top_half ;
          Printf.fprintf oc "let %s = DessserGen.%s\n"
            EntryPoints.replay
            EntryPoints.replay ;
          Printf.fprintf oc "let %s _ _ _ _ = assert false\n"
            EntryPoints.convert)
      ) in
  let what = "function "^ N.func_color func_name in
  RamenOCamlCompiler.compile conf ~keep_temp_files:conf.C.keep_temp_files
                             what src_file obj_name

(* Helper functions to get the internal representation of a value of a
 * given type. Cannot be shared with CodeGen_OCaml since the internal
 * representations are not identical. *)

let rec emit_value_of_string
    indent mn str_var offs_var emit_is_null fins may_quote oc =
  let p fmt = emit oc indent fmt in
  if mn.DT.nullable then (
    p "let is_null_, o_ = %t in" (emit_is_null fins str_var offs_var) ;
    p "if is_null_ then Null, o_ else" ;
    p "let x_, o_ =" ;
    let mn = DT.force_maybe_nullable mn in
    emit_value_of_string (indent+1) mn str_var "o_" emit_is_null fins may_quote oc ;
    p "  in" ;
    p "NotNull x_, o_"
  ) else (
    let emit_parse_list indent mn oc =
      let p fmt = emit oc indent fmt in
      p "let rec read_next_ prevs_ o_ =" ;
      p "  let o_ = string_skip_blanks %s o_ in" str_var ;
      p "  if o_ >= String.length %s then" str_var ;
      p "    failwith \"List interrupted by end of string\" ;" ;
      p "  if %s.[o_] = ']' then prevs_, o_ + 1 else" str_var ;
      p "  let x_, o_ =" ;
      emit_value_of_string
        (indent + 2) mn str_var "o_" emit_is_null (';' :: ']' :: fins) may_quote oc ;
      p "    in" ;
      p "  let prevs_ = x_ :: prevs_ in" ;
      p "  let o_ = string_skip_blanks %s o_ in" str_var ;
      p "  if o_ >= String.length %s then" str_var ;
      p "    failwith \"List interrupted by end of string\" ;" ;
      p "  if %s.[o_] = ';' then read_next_ prevs_ (o_ + 1) else"
        str_var ;
      p "  if %s.[o_] = ']' then prevs_, o_+1 else" str_var ;
      p "  Printf.sprintf \"Unexpected %%C while parsing a list\"" ;
      p "    %s.[o_] |> failwith in" str_var ;
      p "let offs_ = string_skip_blanks_until '[' %s %s + 1 in"
        str_var offs_var ;
      p "let lst_, offs_ = read_next_ [] offs_ in" ;
      p "Array.of_list (List.rev lst_), offs_" in
    let emit_parse_record indent mn kts oc =
      (* Look for '(' *)
      p "let offs_ = string_skip_blanks_until '(' %s %s + 1 in"
        str_var offs_var ;
      p "if offs_ >= String.length %s then" str_var ;
      p "  failwith \"Tuple interrupted by end of string\" ;" ;
      let num_fields = Array.length kts in
      for i = 0 to num_fields - 1 do
        let fn, mn = kts.(i) in
        let fn = N.field fn in
        if N.is_private fn then (
          p "let x%d_ = %s in" i (Helpers.dummy_var_name fn)
        ) else (
          p "(* Read field %a *)" N.field_print fn ;
          p "let x%d_, offs_ =" i ;
          let fins = ';' :: fins in
          let fins = if i = num_fields - 1 then ')' :: fins else fins in
          emit_value_of_string
            (indent + 1) mn str_var "offs_" emit_is_null fins may_quote oc ;
          p "  in"
        ) ;
        p "let offs_ = string_skip_blanks %s offs_ in" str_var ;
        p "let offs_ =" ;
        if i = num_fields - 1 then (
          (* Last separator is optional *)
          p "  if offs_ < String.length %s && %s.[offs_] = ';' then"
            str_var str_var ;
          p "    offs_ + 1 else offs_ in"
        ) else (
          p "  if offs_ >= String.length %s || %s.[offs_] <> ';' then"
            str_var str_var ;
          p "    Printf.sprintf \"Expected separator ';' at offset %%d\" offs_ |>" ;
          p "    failwith" ;
          p "  else offs_ + 1 in"
        )
      done ;
      p "let offs_ = string_skip_blanks_until ')' %s offs_ + 1 in"
        str_var ;
      p "%s.{ %a }, offs_"
        (DessserBackEndOCaml.Config.module_of_type (DT.Value mn))
        (array_print_i ~first:"" ~last:"" ~sep:"; "
          (fun i oc (field_name, _) ->
            Printf.fprintf oc "%s = x%d_" field_name i)) kts
    in
    match mn.DT.vtyp with
    | Vec (d, mn) ->
        p "let lst_, offs_ as res_ =" ;
        emit_parse_list (indent + 1) mn oc ;
        p "in" ;
        p "if Array.length lst_ <> %d then" d ;
        p "  Printf.sprintf \"Was expecting %d values but got %%d\"" d ;
        p "    (Array.length lst_) |> failwith ;" ;
        p "res_"
    | Lst mn ->
        emit_parse_list indent mn oc
    | Tup ts ->
        let kts =
          Array.mapi (fun i mn ->
            DessserBackEndOCaml.Config.tuple_field_name i, mn
          ) ts in
        emit_parse_record indent mn kts oc
    | Rec kts ->
        (* When reading values from a string (command line param values, CSV
         * files...) fields are expected to be given in definition order (as
         * opposed to serialization order).
         * Similarly, private fields are expected to be missing, and are thus
         * replaced by dummy values (so that we return the proper type). *)
        emit_parse_record indent mn kts oc
    | Mac String ->
        (* This one is a bit harder than the others due to optional quoting
         * (from the command line parameters, as CSV strings have been unquoted
         * already), and could benefit from [fins]: *)
        p "RamenTypeConverters.string_of_string ~fins:%a ~may_quote:%b %s %s"
          (List.print char_print_quoted) fins
          may_quote str_var offs_var
    | _ ->
        p "RamenTypeConverters.%s_of_string %s %s"
          (Helpers.id_of_typ mn.DT.vtyp) str_var offs_var
  )

(* Emit a function that either returns the parameter value or exit: *)
let emit_string_parser oc name mn =
  let p fmt = emit oc 0 fmt in
  p "let %s s_ =" name ;
  p "  try" ;
  p "    let parsed_ =" ;
  let emit_is_null fins str_var offs_var oc =
    Printf.fprintf oc
      "if looks_like_null ~offs:%s %s && \
          string_is_term %a %s (%s + 4) then \
       true, %s + 4 else false, %s"
      offs_var str_var
      (List.print char_print_quoted) fins str_var offs_var
      offs_var offs_var in
  emit_value_of_string 3 mn "s_" "0" emit_is_null [] true oc ;
  p "    in" ;
  p "    check_parse_all s_ parsed_" ;
  p "  with e_ ->" ;
  p "    let what_ =" ;
  p "      Printf.sprintf \"Cannot parse value %%S for parameter %s: %%s\"" name ;
  p "                     s_ (Printexc.to_string e_) in" ;
  p "    RamenHelpers.print_exception ~what:what_ e_ ;" ;
  p "    exit RamenConstsExitCodes.cannot_parse_param\n"

(* Output the code but also returns the compunit *)
let generate_global_env oc globals_mod_name params envvars globals =
  Printf.fprintf oc "open RamenHelpersNoLog\n" ;
  let backend = (module DessserBackEndOCaml : BACKEND) in (* TODO: a parameter *)
  let module BE = (val backend : BACKEND) in
  let compunit = DU.make () in
  let open DE.Ops in
  let compunit, _, _ =
    List.map (fun f ->
      let n = (f : N.field :> string) in
      let v = getenv (string n) in
      n, v
    ) envvars |>
    make_rec |>
    comment "Used environment variables" |>
    DU.add_identifier_of_expression compunit ~name:"envs_" in
  (* We need to parse values from friendly text form into internal dessser
   * encoding, for which a value parser for each used type is needed: *)
  let parser_name p =
    "param_"^ (p.RamenTuple.ptyp.name :> string) ^"_value_of_string_" in
  let def_value_name p =
    "params_"^ (p.RamenTuple.ptyp.name :> string) ^"_default_value_" in
  let compunit =
    List.fold_left (fun compunit p ->
      let name = parser_name p
      and backend = DessserBackEndOCaml.id
      and typ = DT.Function ([| DT.string |], DT.Value p.ptyp.typ) in
      DU.add_verbatim_definition
        compunit ~name ~typ ~backend (fun oc _ps ->
          emit_string_parser oc name p.ptyp.typ)
    ) compunit params in
  (* Also adds the default value for each parameters: *)
  let compunit =
    List.fold_left (fun compunit p ->
      let name = def_value_name p in
      let compunit, _, _ =
        RaQL2DIL.constant p.ptyp.typ p.value |>
        comment ("Default value for "^ (p.RamenTuple.ptyp.name :> string)) |>
        DU.add_identifier_of_expression compunit ~name in
      compunit
    ) compunit params in
  (* Finally build the params global vector, which also depends on the
   * environment: *)
  let compunit, fields =
    let d_env = DU.environment compunit in
    List.fold_left (fun (compunit, fields) p ->
      let def_value = identifier (def_value_name p)
      and str_parser = identifier (parser_name p)
      and n = (p.RamenTuple.ptyp.name :> string) in
      let v =
        let_ ~name:"env" ~l:d_env
          (getenv (string (param_envvar_prefix ^ n))) (fun _l env ->
          if_ ~cond:(is_null env)
              ~then_:def_value
              ~else_:(apply str_parser [ force env ])) in
      compunit, (n, v) :: fields
    ) (compunit, []) params in
  let compunit, _, _ =
    make_rec fields |>
    comment "Command line parameters" |>
    DU.add_identifier_of_expression compunit ~name:"params_" in
  let compunit, _, _ =
    List.map (fun g ->
      let n =
        assert (globals_mod_name <> "") ;
        globals_mod_name ^"."^ CodeGen_OCaml.id_of_global g in
      let v = ext_identifier n in
      n, v
    ) globals |>
    make_rec |>
    comment "Used globals" |>
    DU.add_identifier_of_expression compunit ~name:"globals_" in
  BE.print_definitions oc compunit ;
  List.iter (fun n ->
    Printf.fprintf oc "let %s = DessserGen.%s\n" n n
  ) [ "envs_" ; "params_" ; "globals_" ] ;
  compunit
