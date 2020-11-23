(* Make use of an external library for efficient serialization/deserialization
 * (and also in the future: updating states, finalizing tuples...)
 * The plan:
 *
 * - At first we can use this only for RowBinary, switching in CodeGenLib
 *   Skeleton:
 *   - Generate the required functions:
 *     - parse_data: ('tuple -> unit) -> unit (deserialize from rowbinary into C++)
 *     - compute the sersize of that value
 *     - serialize that C++ value into ringbuffer
 *
 * - Then we could use this for all external reads (ie. also for CSV):
 *   - Dessser should be able to desserialize from CSV as well so we could use
 *     the same technique for all input formats.
 *
 * - Then we could use the Dessser heap representation of data (generated
 *   record type) for minimal tuple and the whole family, and use dessser'
 *   ringbuf serializers.
 *
 * - Finally, we could implement some where filters, state updates etc using Dessser
 *   codegenerator.
 *)
open Batteries
open RamenHelpersNoLog
open RamenLog
open Dessser
module DT = DessserTypes
module T = RamenTypes
module N = RamenName
module Files = RamenFiles
(*module Value2RingBuf = HeapValue.Serialize (RamenRingBuffer.Ser)*)

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
(* Wrap around identifier_of_expression to display the full expression in case
 * type_check fails: *)
let identifier_of_expression ?name f state e =
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
      | TVec (_, mn) | TList mn ->
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
      identifier_of_expression ~name:"value_of_ser" BE.identifier_of_expression state in
(* Unused for now, require the output type [mn] to be record-sorted:
    let state, _, _sersize_of_value =
      sersize_of_value mn |>
      identifier_of_expression ~name:"sersize_of_value" BE.identifier_of_expression state in
    let state, _, _value_to_ringbuf =
      value_to_ringbuf mn |>
      identifier_of_expression ~name:"value_to_ringbuf" BE.identifier_of_expression state in
*)
    p "(* Helpers for deserializing type:\n\n%a\n\n*)\n"
      DT.print_maybe_nullable mn ;
    BE.print_definitions state oc ;
    (* A public entry point to unserialize the tuple with a more meaningful
     * name, and which also convert the tuple representation.
     * Indeed, CodeGen_OCaml uses actual tuples for tuples while Dessser uses
     * more convenient records with mutable fields.
     * Also, CodeGen_Ocaml uses a dedicated nullable type whereas Desser
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
      identifier_of_expression ~name:"value_of_ser" BE.identifier_of_expression state in
(* Unused for now, require the output type [mn] to be record-sorted:
    let state, _, _sersize_of_value =
      sersize_of_value mn |>
      identifier_of_expression ~name:"sersize_of_value" BE.identifier_of_expression state in
    let state, _, _value_to_ringbuf =
      value_to_ringbuf mn |>
      identifier_of_expression ~name:"value_to_ringbuf" BE.identifier_of_expression state in
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
