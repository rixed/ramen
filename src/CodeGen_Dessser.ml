(* Make use of external library Dessser for code generation *)
open Batteries
open RamenHelpersNoLog
open RamenLog
open Dessser
module Files = RamenFiles
module DE = DessserExpressions
module DT = DessserTypes
module E = RamenExpr
module EntryPoints = RamenConstsEntryPoints
module N = RamenName
module O = RamenOperation
module T = RamenTypes
module Value2RingBuf = HeapValue.Serialize (RamenRingBuffer.Ser)
module RingBuf2Value = HeapValue.Materialize (RamenRingBuffer.Des)

open DessserExpressions

module RowBinary2Value = HeapValue.Materialize (RowBinary.Des)
let rowbinary_to_value ?config mn =
  let open Ops in
  comment "Function deserializing the rowbinary into a heap value:"
    (func1 TDataPtr (fun _l src -> RowBinary2Value.make ?config mn src))

module Csv2Value = HeapValue.Materialize (Csv.Des)
let csv_to_value ?config mn =
  let open Ops in
  comment "Function deserializing the CSV into a heap value:"
    (func1 TDataPtr (fun _l src -> Csv2Value.make ?config mn src))

(*
let sersize_of_value mn =
  let open Ops in
  let ma = copy_field in
  comment "Compute the serialized size of the passed heap value:"
    (func1 (TValue mn) (fun _l v -> (Value2RingBuf.sersize mn ma v)))

(* Takes a heap value and a pointer (ideally pointing in a TX) and returns
 * the new pointer location within the TX. *)
let value_to_ringbuf mn =
  let open Ops in
  let ma = copy_field in
  comment "Serialize a heap value into a ringbuffer location:"
    (func2 (TValue mn) TDataPtr (fun _l v dst ->
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
        print ?max_depth:None oc e)) () ;
    !logger.error "Invalid expression: %a (see complete expression in %s), %s"
      (print ~max_depth:3) e
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
      | DT.Unknown | Mac _ ->
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

(* Emit the function that will return the next input tuple read from the input
 * ringbuffer, from the passed tx and start offset.
 * The function has to return the deserialized value. *)
let read_in_tuple in_type =
  let _cmt =
    Printf.sprintf2 "Deserialize a tuple of type %a"
      RamenTuple.print_typ in_type in
  let to_value = RingBuf2Value.make in_type in
  todo "read_in_tuple"

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
  func ~l args (fun _l _f_id ->
    (* Add the function parameters to the environment for getting global
     * states, input and previous output tuples, and optional local states *)
    (* TODO once stateful operations are supported in CodeGen_RaQL2DIL *)
    seq
      [ (* Update the environment used by that expression: *)
        (* TODO *)
        (* Compute the boolean result: *)
        CodeGen_RaQL2DIL.expression expr ])

(* Output the code required for the function operation and returns the new
 * code.
 * Named [emit_full_operation] in reference to [emit_half_operation] for half
 * workers. *)
let emit_aggregate _entry_point code add_expr func_op
                   global_state_env group_state_env
                   env_env param_env globals_env in_type =
  let _minimal_typ = CodeGen_MinimalTuple.minimal_type func_op in
  let where =
    match func_op with
    | O.Aggregate { where ; _ } ->
        where
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
    fail_with_context "tuple reader" (fun () ->
      read_in_tuple in_type |>
      add_expr code "read_in_tuple_") in
  let code =
    fail_with_context "where-fast function" (fun () ->
      let env = global_state_env @ base_env in
      where_clause ~env in_type where_fast |>
      add_expr code "where_fast_") in
  let code =
    fail_with_context "where-slow function" (fun () ->
      let env = group_state_env @ global_state_env @ base_env in
      where_clause ~with_group:true ~env in_type where_slow |>
      add_expr code "where_slow_") in
  ignore code ;
  todo "emit_aggregate"

(* Trying to be backend agnostic, generate all the DIL functions required for
 * the given RaQL function.
 * Eventually those will be compiled to OCaml in order to be easily mixed
 * with [CodeGenLib_Skeleton]. *)
let generate_code
      _conf _func_name func_op in_type
      env_env param_env globals_env global_state_env group_state_env
      _obj_name _params_mod_name _dessser_mod_name
      _orc_write_func _orc_read_func _params
      _globals_mod_name =
  let backend = (module BackEndOCaml : BACKEND) in (* TODO: a parameter *)
  let module BE = (val backend : BACKEND) in
  let code = BE.make_state () in
  let add_expr code name d =
    let code, _, _ = BE.add_identifier_of_expression code ~name d in
    code in
  let _code =
    match func_op with
    | O.Aggregate _ ->
        emit_aggregate EntryPoints.worker code add_expr
                       func_op global_state_env group_state_env
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
