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
module T = RamenTypes
module N = RamenName
module Files = RamenFiles
open RamenHelpers
open RamenLog
open Dessser

let rec to_dessser_structure = function
  | T.TEmpty
  | T.TNum
  | T.TAny ->
      (* Not supposed to be des/ser *)
      assert false
  | T.TFloat -> Types.TFloat
  | T.TString -> Types.TString
  | T.TBool -> Types.TBool
  | T.TChar -> Types.TChar
  | T.TU8 -> Types.TU8
  | T.TU16 -> Types.TU16
  | T.TU32 -> Types.TU32
  | T.TU64 -> Types.TU64
  | T.TU128 -> Types.TU128
  | T.TI8 -> Types.TI8
  | T.TI16 -> Types.TI16
  | T.TI32 -> Types.TI32
  | T.TI64 -> Types.TI64
  | T.TI128 -> Types.TI128
  | T.TEth -> Types.TU64
  | T.TIpv4 -> Types.TU32
  | T.TIpv6 -> Types.TU128
  | T.TIp -> todo "to_dessser_structure TIp"
  | T.TCidrv4 -> todo "to_dessser_structure TCidrv4"
  | T.TCidrv6 -> todo "to_dessser_structure TCidrv6"
  | T.TCidr -> todo "to_dessser_structure TCidr"
  | T.TTuple typs -> Types.TTup (Array.map to_dessser_type typs)
  | T.TVec (dim, typ) -> Types.TVec (dim, to_dessser_type typ)
  | T.TList _ -> todo "to_dessser_structure TList"
  | T.TRecord typs ->
      Types.TRec (
        Array.map (fun (name, typ) ->
          name, to_dessser_type typ
        ) typs)

and to_dessser_type typ =
  Types.{ nullable = typ.T.nullable ;
          structure = to_dessser_structure typ.T.structure }

module Make (BE : BACKEND) =
struct
  module RingBufSer = RamenRingBuffer.Ser (BE)
  module RowBinary2Value = DesSer (RowBinary.Des (BE)) (HeapValue.Ser (BE))
  module Value2RingBuf = DesSer (HeapValue.Des (BE)) (RingBufSer)
  module RingBufSizer = HeapValue.SerSizer (RingBufSer)

  let tptr = Types.(make TPointer)

  let rowbinary_to_value be_output typ =
    (* Since we are going to extract the heap value we need to be
     * precise about the type: *)
    let out_typ = Types.(make (TPair (tptr, typ))) in
    BE.print_function2 be_output tptr tptr out_typ (fun oc src dst ->
      BE.ignore oc dst ;
      BE.comment oc "Function deserializing the rowbinary into a heap value:" ;
      let src, dst = RowBinary2Value.desser typ oc src dst in
      BE.make_pair oc t_pair_ptrs src dst)

  let sersize_of_value be_output typ =
    let t_size = Types.(make TSize) in
    BE.print_function1 be_output typ t_size (fun oc v ->
      BE.comment oc "Compute the serialized size of the passed heap value:" ;
      let const_sz, dyn_sz = RingBufSizer.sersize typ be_output v in
      BE.size_add oc const_sz dyn_sz)

  let value_to_ringbuf be_output typ =
    (* Takes a heap value and a pointer (ideally pointing in a TX) and return
     * the new pointer location within the TX. *)
    BE.print_function2 be_output typ tptr tptr (fun oc v dst ->
      BE.comment oc "Serialize a heap value into a ringbuffer location:" ;
      let _, dst = Value2RingBuf.desser typ oc v dst in
      dst)
end

module OCaml =
struct
  module BE = BackEndOCaml
  include Make (BE)

  let emit typ oc =
    let dtyp = to_dessser_type typ in
    let be_output = BE.make_output () in
    (* BE.comment does not work outside functions :( *)
    Printf.fprintf oc "(* Helpers for function:\n\n%a\n\n*)\n"
      T.print_typ typ ;
    let rowbinary_to_value = rowbinary_to_value be_output dtyp
    (* Yet to be used: *)
    and _sersize_of_value = sersize_of_value be_output dtyp
    and _value_to_ringbuf = value_to_ringbuf be_output dtyp in
    BE.print_output oc be_output ;
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
    let p fmt = emit oc 0 fmt in
    p "open RamenNullable" ;
    p "" ;
    p "let read_tuple buffer start stop _has_more =" ;
    p "  let src = Pointer.of_bytes buffer start stop in" ;
    p "  (* Will be ignored by HeapValue.Ser: *)" ;
    p "  let dummy = Pointer.make 0 in" ;
    p "  let src', heap_value = %a src dummy in"
      Identifier.print rowbinary_to_value ;
    p "  let read_sz = Pointer.sub src' src" ;
    p "  and tuple =" ;
    let typs =
      match dtyp.Types.structure with
      | Types.TTup typs -> typs
      | Types.TRec typs -> Array.map snd typs
      | _ -> [||] in
    let num_fields = Array.length typs in
    if num_fields = 0 then
      p "    heap_value"
    else
      Array.iteri (fun i typ ->
        let fname = BackEndOCaml.subfield_name dtyp i in
        p "    %sheap_value.%s%s"
          (if typ.Types.nullable then "nullable_of_option " else "")
          fname
          (if i < num_fields - 1 then "," else " in")
      ) typs ;
    p "  tuple, read_sz"
end

module CPP =
struct
  module BE = BackEndCPP
  include Make (BE)

  let emit typ oc =
    let dtyp = to_dessser_type typ in
    let be_output = BE.make_output () in
    Printf.fprintf oc "/* Helpers for function:\n\n%a\n\n*/\n"
      T.print_typ typ ;
    let _rowbinary_to_value = rowbinary_to_value be_output dtyp
    (* Yet to be used: *)
    and _sersize_of_value = sersize_of_value be_output dtyp
    and _value_to_ringbuf = value_to_ringbuf be_output dtyp in
    BE.print_output oc be_output ;
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
