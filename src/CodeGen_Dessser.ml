(* Make use of external library Dessser for code generation *)
open Batteries
open Stdint
open RamenHelpers
open RamenHelpersNoLog
open RamenLog
open Dessser
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
    (DE.func1 TDataPtr (fun _l src -> RowBinary2Value.make ?config mn src))

module Csv2Value = HeapValue.Materialize (Csv.Des)
let csv_to_value ?config mn =
  let open DE.Ops in
  comment "Function deserializing the CSV into a heap value:"
    (DE.func1 TDataPtr (fun _l src -> Csv2Value.make ?config mn src))

(*
let sersize_of_value mn =
  let open DE.Ops in
  let ma = copy_field in
  comment "Compute the serialized size of the passed heap value:"
    (DE.func1 (TValue mn) (fun _l v -> (Value2RingBuf.sersize mn ma v)))

(* Takes a heap value and a pointer (ideally pointing in a TX) and returns
 * the new pointer location within the TX. *)
let value_to_ringbuf mn =
  let open DE.Ops in
  let ma = copy_field in
  comment "Serialize a heap value into a ringbuffer location:"
    (DE.func2 (TValue mn) TDataPtr (fun _l v dst ->
      let src_dst = Value2RingBuf.serialize mn ma v dst in
      secnd src_dst))
*)
(* Wrap around add_identifier_of_expression to display the full expression in case
 * type_check fails: *)
let add_identifier_of_expression ?name f state e =
  try
    f state ?name e
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
      | TVec (_, mn) | TList mn | TSet mn ->
          need_conversion mn
      | TTup _ | TRec _ ->
          (* Represented as records in Dessser but tuples in Ramen (FIXME): *)
          true
      | TSum mns ->
          Array.exists (need_conversion % snd) mns
      | TMap (k, v) ->
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
        BE.Config.module_of_type (DT.TValue { mn with nullable = false }) in
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
      | TVec (_, mn) | TList mn ->
          emit oc depth' "Array.map (fun x ->" ;
          emit_ramen_of_dessser_value ~depth:depth' mn oc "x" ;
          emit oc depth' ") %s" vname'
      | TTup mns ->
          Array.mapi (fun i t ->
            BE.Config.tuple_field_name i, t
          ) mns |>
          emit_tuple mod_name
      | TRec mns ->
          emit_tuple mod_name mns
      | TSum _ ->
          (* No values of type TSum yet *)
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
    let state = BE.make_state () in
    let state, _, value_of_ser =
      deserializer mn |>
      add_identifier_of_expression ~name:"value_of_ser" BE.add_identifier_of_expression state in
(* Unused for now, require the output type [mn] to be record-sorted:
    let state, _, _sersize_of_value =
      sersize_of_value mn |>
      add_identifier_of_expression ~name:"sersize_of_value" BE.add_identifier_of_expression state in
    let state, _, _value_to_ringbuf =
      value_to_ringbuf mn |>
      add_identifier_of_expression ~name:"value_to_ringbuf" BE.add_identifier_of_expression state in
*)
    p "(* Helpers for deserializing type:\n\n%a\n\n*)\n"
      DT.print_maybe_nullable mn ;
    BE.print_definitions state oc ;
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
    let state = BE.make_state () in
    let state, _, _value_of_ser =
      deserializer mn |>
      add_identifier_of_expression ~name:"value_of_ser" BE.add_identifier_of_expression state in
(* Unused for now, require the output type [mn] to be record-sorted:
    let state, _, _sersize_of_value =
      sersize_of_value mn |>
      add_identifier_of_expression ~name:"sersize_of_value" BE.add_identifier_of_expression state in
    let state, _, _value_to_ringbuf =
      value_to_ringbuf mn |>
      add_identifier_of_expression ~name:"value_to_ringbuf" BE.add_identifier_of_expression state in
*)
    Printf.fprintf oc "/* Helpers for function:\n\n%a\n\n*/\n"
      DT.print_maybe_nullable mn ;
    BE.print_definitions state oc ;
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
    DT.(make (TRec (Array.map (fun ft ->
      (ft.RamenTuple.name :> string), ft.typ
    ) tup)))

(* Emit the function that will return the next input tuple read from the input
 * ringbuffer, from the passed tx and start offset.
 * The function has to return the deserialized value. *)
let read_in_tuple in_type =
  let cmt =
    Printf.sprintf2 "Deserialize a tuple of type %a"
      DT.print_maybe_nullable in_type in
  let open DE.Ops in
  let tx_t = DT.(TValue (make (get_user_type "tx"))) in
  let l = None in (* TODO *)
  DE.func2 ?l tx_t DT.TSize (fun _l tx start_offs ->
    if in_type.DT.vtyp = DT.Unit then (
      if in_type.DT.nullable then
        to_nullable unit
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
let where_clause ?(with_group=false) ~env _in_type expr =
  ignore env ; (* TODO *)
  let global_t = DT.void (* TODO *)
  and in_t = DT.void (* TODO *)
  and out_prev_t = DT.void (* TODO *)
  and group_t = DT.void in (* TODO *)
  let args =
    if with_group then [| global_t ; in_t ; out_prev_t ; group_t |]
                  else [| global_t ; in_t ; out_prev_t |] in
  let l = [] in (* TODO *)
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
  let l = None in (* TODO *)
  DE.func2 ?l DT.(TValue (make ~nullable:left_nullable vtyp))
              DT.(TValue (make ~nullable:right_nullable vtyp)) (fun _l a b ->
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
  let l = None in (* TODO *)
  (* input tuple -> global state -> something *)
  DE.func2 ?l (DT.TValue in_type) (TValue global_state_type)
    (fun _l _in_ _global_ ->
      (* add_tuple_environment In in_type env in: TODO *)
      (* Update the states used by this expression: TODO *)
      (* emit_state_update_for_expr ~env ~opc ~what:"commit clause 0, in" e ; *)
      RaQL2DIL.expression e) |>
  comment cmt

let emit_cond0_out ~env minimal_type out_last_type global_state_type
                   local_state_type e =
  ignore env ; (* TODO *)
  let cmt =
    Printf.sprintf2 "The part of the commit condition that depends on the \
                     output tuple: %a"
      (E.print false) e in
  let open DE.Ops in
  let l = None in (* TODO *)
  (* minimal tuple -> previous out -> global state -> local state -> thing *)
  DE.func4 ?l (DT.TValue minimal_type) (TValue out_last_type)
           (TValue global_state_type) (TValue local_state_type)
    (fun _l _min_ _out_previous_ _group_ _global_ ->
      (* Add Out and OutPrevious to the environment: TODO *)
      (* add_tuple_environment Out minimal_type env |>
         add_tuple_environment OutPrevious opc.typ in *)
      (* Update the states used by this expression: TODO *)
      (* emit_state_update_for_expr ~env ~opc ~what:"commit clause 0, out" e *)
    RaQL2DIL.expression e) |>
  comment cmt

let commit_when_clause ~env in_type minimal_type out_last_type
                       global_state_type local_state_type e =
  ignore env ; (* TODO *)
  let cmt =
    Printf.sprintf2 "The bulk of the commit condition: %a"
      (E.print false) e in
  let open DE.Ops in
  let l = None in (* TODO *)
  (* input tuple -> minimal tuple -> previous out -> global state ->
   * local state -> bool *)
  DE.func5 ?l (DT.TValue in_type) (DT.TValue minimal_type) (TValue out_last_type)
           (TValue global_state_type) (TValue local_state_type)
    (fun _l _in_ _min_ _out_previous_ _group_ _global_ ->
      (* add_tuple_environment In in_type env TODO *)
      (* add_tuple_environment Out minimal_typ TODO *)
      (* Update the states used by this expression: TODO *)
      (* emit_state_update_for_expr ~env ~opc ~what:"commit clause" e ; *)
      RaQL2DIL.expression e) |>
  comment cmt

(* Build a dummy functio  of the desired type: *)
let dummy_function ins out =
  let ins = Array.map (fun mn -> DT.TValue mn) ins in
  DE.func ins (fun _l _fid ->
    DE.default_value ~allow_null:true out)

(* When there is no way to extract a numeric value to order group for
 * optimising the commit condition, pass those placeholder functions to
 * [CodeGenLib_Skeleton.aggregate]: *)
let default_commit_cond commit_cond in_type minimal_type
                        out_last_type local_state_type global_state_type =
  (* We are free to pick whatever type for group_order_type: *)
  let group_order_type = DT.(make Unit) in
  let dummy_cond0_left_op =
    dummy_function [| in_type |] group_order_type
  and dummy_cond0_right_op =
    dummy_function [| minimal_type ; out_last_type ; local_state_type ;
                      global_state_type |] group_order_type
  and dummy_cond0_cmp =
    dummy_function [| group_order_type ; group_order_type |] DT.(make (Mac TI8)) in
  false,
  dummy_cond0_left_op,
  dummy_cond0_right_op,
  dummy_cond0_cmp,
  false,
  commit_cond

(* Returns the set of functions/flags required for
 * [CodeGenLib_Skeleton.aggregate] to process the commit condition, trying
 * to optimize by splitting the condition in two, with one sortable part: *)
let optimize_commit_cond func_name in_type minimal_type out_last_type
                         local_state_type global_state_type commit_cond =
  let es = E.as_nary E.And commit_cond in
  (* TODO: take the best possible sub-condition not the first one: *)
  let rec loop rest = function
    | [] ->
        !logger.warning "Cannot find a way to optimise the commit \
                         condition of function %a"
          N.func_print func_name ;
        default_commit_cond commit_cond in_type minimal_type
                            out_last_type local_state_type global_state_type
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
            let env = [] in (* TODO *)
            let cond0_in =
              emit_cond0_in ~env in_type global_state_type (may_neg f) |>
              RaQL2DIL.cast ~from:f.typ.vtyp ~to_:group_order_type in
            let cond0_out =
              emit_cond0_out ~env minimal_type out_last_type global_state_type
                             local_state_type (may_neg g) |>
              RaQL2DIL.cast ~from:g.typ.vtyp ~to_:group_order_type in
            let rem_cond =
              E.of_nary ~vtyp:commit_cond.typ.vtyp
                        ~nullable:commit_cond.typ.DT.nullable
                        ~units:commit_cond.units
                        E.And (List.rev_append rest es) in
            true, cond0_in, cond0_out, cond0_cmp, true_when_eq, rem_cond) in
  loop [] es

(* Output the code required for the function operation and returns the new
 * code.
 * Named [emit_full_operation] in reference to [emit_half_operation] for half
 * workers. *)
let emit_aggregate _entry_point code add_expr func_op func_name
                   global_state_env group_state_env
                   env_env param_env globals_env in_type =
  (* The input type (computed from parent output type and the deep selection
   * of field), aka `tuple_in in CodeGenLib_Skeleton: *)
  let in_type = dessser_type_of_ramen_tuple in_type in
  (* That part of the output value that needs to be computed for every input
   * even when no output is emmitted, aka `minimal_out: *)
  let minimal_type = Helpers.minimal_type func_op |>
                     dessser_type_of_ramen_tuple in
  (* The tuple storing the global state, aka `global_state: *)
  let global_state_type = DT.make Unit in (* TODO *)
  (* The tuple storing the group local state, aka `local_state: *)
  let local_state_type = DT.make Unit in (* TODO *)
  (* The output type of values passed to the final output generator: *)
  let generator_out_type = DT.make Unit in (* TODO *)
  (* Same, nullable: *)
  let out_last_type = DT.{ generator_out_type with nullable = true } in
  let where, commit_cond =
    match func_op with
    | O.Aggregate { where ; commit_cond ; _ } ->
        where, commit_cond
    | _ -> assert false in
  let base_env = param_env @ env_env @ globals_env in
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
      let env = global_state_env @ base_env in
      where_clause ~env in_type where_fast |>
      add_expr code "where_fast_") in
  let env = group_state_env @ global_state_env @ base_env in
  let code =
    fail_with_context "coding for where-slow function" (fun () ->
      where_clause ~with_group:true ~env in_type where_slow |>
      add_expr code "where_slow_") in
  let has_commit_cond, cond0_left_op, cond0_right_op, cond0_cmp,
      cond0_true_when_eq, commit_cond_rest =
    if Helpers.check_commit_for_all commit_cond then
      fail_with_context "coding for optimized commit condition" (fun () ->
        optimize_commit_cond func_name in_type minimal_type out_last_type
                             local_state_type global_state_type commit_cond)
    else
      (* No need to optimize: *)
      default_commit_cond commit_cond in_type minimal_type
                          out_last_type local_state_type global_state_type in
  let code =
    add_expr code "commit_has_commit_cond_" (DE.Ops.bool has_commit_cond) in
  let code =
    add_expr code "commit_cond0_left_op_" cond0_left_op in
  let code =
    add_expr code "commit_cond0_right_op_" cond0_right_op in
  let code =
    add_expr code "commit_cond0_cmp_" cond0_cmp in
  let code =
    add_expr code "commit_cond0_true_when_eq_" (DE.Ops.bool cond0_true_when_eq) in
  let code =
    fail_with_context "coding for commit condition function" (fun () ->
      commit_when_clause ~env in_type minimal_type out_last_type
                         global_state_type local_state_type commit_cond_rest |>
      add_expr code "commit_cond_") in
  ignore code ;
  todo "emit_aggregate"

(* Trying to be backend agnostic, generate all the DIL functions required for
 * the given RaQL function.
 * Eventually those will be compiled to OCaml in order to be easily mixed
 * with [CodeGenLib_Skeleton]. *)
let generate_code
      _conf func_name func_op in_type
      env_env param_env globals_env global_state_env group_state_env
      _obj_name _params_mod_name _dessser_mod_name
      _orc_write_func _orc_read_func _params
      _globals_mod_name =
  let backend = (module BackEndOCaml : BACKEND) in (* TODO: a parameter *)
  let module BE = (val backend : BACKEND) in
  let code = BE.make_state () in
  (* We will need a few helper functions: *)
  if not (DT.is_registered "tx") then
    (* No need to be specific about this type but at least name it: *)
    DT.register_user_type "tx" DT.(Mac TU8) ;
  let pointer_of_tx_t =
    DT.TFunction ([| DT.(TValue (make (get_user_type "tx"))) |], TDataPtr) in
  let code =
    BE.add_external_identifier code "CodeGenLib_Dessser.pointer_of_tx"
                               pointer_of_tx_t in
  (* Make all other functions unaware of the backend with this shorthand: *)
  let add_expr code name d =
    let code, _, _ = BE.add_identifier_of_expression code ~name d in
    code in
  let _code =
    match func_op with
    | O.Aggregate _ ->
        emit_aggregate EntryPoints.worker code add_expr
                       func_op func_name global_state_env group_state_env
                       env_env param_env globals_env in_type
    | _ ->
      todo "Non aggregate functions"
  in
  todo "Not implemented"
(*
  let src_file =
    RamenOCamlCompiler.with_code_file_for
      obj_name conf.C.reuse_prev_files (fun oc ->
        fail_with_context "operation" (fun () ->
          emit_operation EntryPoints.worker EntryPoints.top_half func
                         global_state_env group_state_env env_env param_env
                         globals_env opc) ;
      ) in
  let what = "function "^ N.func_color func.VSI.name in
  RamenOCamlCompiler.compile conf ~keep_temp_files:conf.C.keep_temp_files
                             what src_file obj_name
*)
