(* Make use of an external library for efficient serialization/deserialization
 * (and also in the future: updating states, finalizaing tuples...)
 * The plan:
 *
 * - At first we can use this only for RowBinary, switching in CodeGenLib
 *   Skeleton:
 *   - Convert ramen types into Dessser types
 *   - Generate the required functions:
 *     - parse_data: ('tuple -> unit) -> unit (deserialize from rowbinary into C++)
 *     - compute the sersize of that value
 *     - serialize that C++ value into ringbuffer
 *
 * - Then we could use this for all external reads (ie. also for CSV):
 *   - Dessser should be able to desserialize from CSV as well so we could use
 *     the same technique for all input formats.
 *
 * - Then we could also have a Dessser representation of data (generated
 *   record type) for minimal tuple and the whole family, and then implement
 *   where filters, state updates etc using Dessser codegenerator.
 *)
open Batteries
open RamenHelpersNoLog
open RamenLog
open Dessser
module T = RamenTypes
module N = RamenName
module Files = RamenFiles
module D = DessserTypes

let rec to_value_type =
  let eth = D.(Usr (get_user_type "Eth"))
  and ip4 = D.(Usr (get_user_type "Ipv4"))
  and ip6 = D.(Usr (get_user_type "Ipv6"))
  and cidrv4 = D.(Usr (get_user_type "Cidrv4"))
  and cidrv6 = D.(Usr (get_user_type "Cidrv6"))
  in
  function
  | T.TEmpty
  | T.TNum
  | T.TAny ->
      (* Not supposed to be des/ser *)
      assert false
  | T.TFloat -> D.(Mac TFloat)
  | T.TString -> D.(Mac TString)
  | T.TBool -> D.(Mac TBool)
  | T.TChar -> D.(Mac TChar)
  | T.TU8 -> D.(Mac TU8)
  | T.TU16 -> D.(Mac TU16)
  | T.TU32 -> D.(Mac TU32)
  | T.TU64 -> D.(Mac TU64)
  | T.TU128 -> D.(Mac TU128)
  | T.TI8 -> D.(Mac TI8)
  | T.TI16 -> D.(Mac TI16)
  | T.TI32 -> D.(Mac TI32)
  | T.TI64 -> D.(Mac TI64)
  | T.TI128 -> D.(Mac TI128)
  | T.TEth -> eth
  | T.TIpv4 -> ip4
  | T.TIpv6 -> ip6
  | T.TIp -> todo "to_value_type TIp"
  | T.TCidrv4 -> cidrv4
  | T.TCidrv6 -> cidrv6
  | T.TCidr -> todo "to_value_type TCidr"
  | T.TTuple typs -> D.TTup (Array.map to_maybe_nullable typs)
  | T.TVec (dim, typ) -> D.TVec (dim, to_maybe_nullable typ)
  | T.TList _ -> todo "to_value_type TList"
  | T.TRecord typs ->
      D.TRec (
        Array.map (fun (name, typ) ->
          name, to_maybe_nullable typ
        ) typs)
  | T.TMap _ -> assert false (* No values of that type *)

and to_maybe_nullable typ =
  let value_type = to_value_type typ.T.structure in
  if typ.T.nullable then
    D.(Nullable value_type)
  else
    D.(NotNullable value_type)

module RingBufSer = RamenRingBuffer.Ser
module RingBufDes = RamenRingBuffer.Des
module ToValue = HeapValue.Materialize (RingBufDes)
module OfValue = HeapValue.Serialize (RingBufSer)

open DessserExpressions

let rowbinary_to_value typ =
  let open Ops in
  func1 TDataPtr (fun _l src ->
    comment "Function deserializing the rowbinary into a heap value:"
      (ToValue.make typ src))

let sersize_of_value typ =
  let open Ops in
  func1 (TValue typ) (fun _l vptr ->
    comment "Compute the serialized size of the passed heap value:"
      (OfValue.sersize typ vptr))

(* Takes a heap value and a pointer (ideally pointing in a TX) and return
 * the new pointer location within the TX. *)
let value_to_ringbuf typ =
  let open Ops in
  func2 (TValue typ) TDataPtr (fun _l vptr dst ->
    comment "Serialize a heap value into a ringbuffer location:"
      (let src_dst = OfValue.serialize typ vptr dst in
      secnd src_dst))

(* Wrap around identifier_of_expression to display the full expression in case
 * type_check fails: *)
let print_type_errors ?name identifier_of_expression state e =
  try
    identifier_of_expression state ?name e
  with exn ->
    let fname = Filename.get_temp_dir_name () ^"/dessser_type_error.last" in
    ignore_exceptions (fun () ->
      let mode = [ `create ; `trunc ; `text ] in
      File.with_file_out fname ~mode (fun oc ->
        print_expr ?max_depth:None oc e)) () ;
    !logger.error "Invalid expression: %a (see complete expression in %s), %s"
      (print_expr ~max_depth:3) e
      fname
      (Printexc.to_string exn) ;
    raise exn

module OCaml =
struct
  module BE = BackEndOCaml

  let emit typ oc =
    let p fmt = emit oc 0 fmt in
    let dtyp = to_maybe_nullable typ in
    let state = BE.make_state () in
    let state, _, rowbinary_to_value =
      rowbinary_to_value dtyp |>
      print_type_errors ~name:"rowbinary_to_value" BE.identifier_of_expression state in
    let state, _, _sersize_of_value =
      sersize_of_value dtyp |>
      print_type_errors ~name:"sersize_of_value" BE.identifier_of_expression state in
    let state, _, _value_to_ringbuf =
      value_to_ringbuf dtyp |>
      print_type_errors ~name:"value_to_ringbuf" BE.identifier_of_expression state in
    p "(* Helpers for deserializing type:\n\n%a\n\n*)\n" T.print_typ typ ;
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
    p "open RamenNullable" ;
    p "" ;
    p "let read_tuple buffer start stop _has_more =" ;
    p "  let src = Pointer.of_bytes buffer start stop in" ;
    p "  let src', heap_value = %s src in" rowbinary_to_value ;
    p "  let heap_value = !heap_value in" ;
    p "  let read_sz = Pointer.sub src' src" ;
    p "  and tuple =" ;
    let typs =
      match dtyp with
      | D.(Nullable (TTup typs) | NotNullable (TTup typs)) ->
          Array.mapi (fun i t ->
            BackEndOCaml.Config.tuple_field_name i, t
          ) typs
      | D.(Nullable (TRec typs) | NotNullable (TRec typs)) ->
          typs
      | _ ->
          [||] in
    let num_fields = Array.length typs in
    if num_fields = 0 then
      p "    heap_value"
    else
      Array.iteri (fun i (fname, typ) ->
        let fname = BackEndCLike.valid_identifier fname in
        p "    %sheap_value.%s%s"
          (if D.is_nullable typ then "nullable_of_option " else "")
          fname
          (if i < num_fields - 1 then "," else " in")
      ) typs ;
    p "  tuple, read_sz"
end

module CPP =
struct
  module BE = BackEndCPP

  let emit typ oc =
    let dtyp = to_maybe_nullable typ in
    let state = BE.make_state () in
    let state, _, _rowbinary_to_value =
      rowbinary_to_value dtyp |>
      print_type_errors ~name:"rowbinary_to_value" BE.identifier_of_expression state in
    let state, _, _sersize_of_value =
      sersize_of_value dtyp |>
      print_type_errors ~name:"sersize_of_value" BE.identifier_of_expression state in
    let state, _, _value_to_ringbuf =
      value_to_ringbuf dtyp |>
      print_type_errors ~name:"value_to_ringbuf" BE.identifier_of_expression state in
    Printf.fprintf oc "/* Helpers for function:\n\n%a\n\n*/\n"
      T.print_typ typ ;
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
