(* ORC types and mapping with Ramen types.
 *
 * No comprehensive doc for ORC but see:
 * http://hadooptutorial.info/hive-data-types-examples/
 *
 * Note: in all this private fields are not taken into account.
 * This is because it is easier to deal with them in the ocaml code than the
 * C++ code (especially reading them back as `any cheap value`). This must
 * therefore be wrapped in an OCaml generated code that remove/add them.
 *)
open Batteries
open RamenHelpersNoLog
module T = RamenTypes
module DT = DessserTypes
module N = RamenName

let debug = false

(*$inject
   module T = RamenTypes
*)

type t =
  | Boolean
  | TinyInt (* 8 bits, signed *)
  | SmallInt (* 16 bits, signed *)
  | Int (* 32 bits, signed *)
  | BigInt (*64 bits, signed *)
  | Float
  | Double
  | TimeStamp
  | Date
  | String
  | Binary
  (* It seems that precision is the number of digits and scale the number of
   * those after the fractional dot: *)
  | Decimal of { precision : int ; scale : int }
  | VarChar of int (* max length *)
  | Char of int (* also max length *)
  (* Compound types: *)
  | Array of t
  | UnionType of t array
  | Struct of (string * t) array
  | Map of (t * t)

(* Orc types string format use colon as a separator, and liborc accepts only
 * alphanums and underscores, then optionally a dot followed by some
 * alphabetic (no nums). Otherwise it will throw:
 * std::logic_error: Unrecognized character.
 * Better replace everything that's not alphanum by an underscore: *)
let print_label oc str =
  String.map (fun c -> if is_alphanum c then c else '_') str |>
  String.print oc

(* Output the schema of the type.
 * Similar to https://github.com/apache/orc/blob/529bfcedc10402c58d9269c2c95919cba6c4b93f/c%2B%2B/src/TypeImpl.cc#L157
 * in a more civilised language. *)
let rec print oc = function
  | Boolean -> String.print oc "boolean"
  | TinyInt -> String.print oc "tinyint"
  | SmallInt -> String.print oc "smallint"
  | Int -> String.print oc "int"
  | BigInt -> String.print oc "bigint"
  | Double -> String.print oc "double"
  | Float -> String.print oc "float"
  | TimeStamp -> String.print oc "timestamp"
  | Date -> String.print oc "date"
  | String -> String.print oc "string"
  | Binary -> String.print oc "binary"
  | Decimal { precision ; scale } ->
      Printf.fprintf oc "decimal(%d,%d)" precision scale
  | VarChar len ->
      Printf.fprintf oc "varchar(%d)" len
  | Char len ->
      Printf.fprintf oc "char(%d)" len
  | Array t ->
      Printf.fprintf oc "array<%a>" print t
  | UnionType ts ->
      Printf.fprintf oc "uniontype<%a>"
        (Array.print ~first:"" ~last:"" ~sep:"," print) ts
  | Struct kts ->
      Printf.fprintf oc "struct<%a>"
        (Array.print ~first:"" ~last:"" ~sep:","
          (fun oc (l, v) -> Printf.fprintf oc "%a:%a" print_label l print v)) kts
  | Map (k, v) ->
      Printf.fprintf oc "map<%a,%a>" print k print v

(*$= print & ~printer:BatPervasives.identity
  "double" (BatIO.to_string print Double)
  "struct<a_map:map<smallint,char(5)>,a_list:array<decimal(3,1)>>" \
    (BatIO.to_string print \
      (Struct [| "a_map", Map (SmallInt, Char 5); \
                 "a_list", Array (Decimal { precision=3; scale=1 }) |]))
 *)

(* Generate the ORC type corresponding to a Ramen type. Note that for ORC
 * every value can be NULL so there is no nullability in the type.
 * In the other way around that is not a conversion but a cast.
 * ORC has no unsigned integer types. We could upgrade all unsigned types
 * to the next bigger signed type, but that would not be very efficient for
 * storing values. Also, we could not do this for uint128 as the largest
 * Decimal supported must fit an int128.
 * Therefore, we encode unsigned as signed. This is no problem when Ramen
 * read them back, as it always know the exact type, but could cause some
 * issues when importing the files in Hive etc. *)
let rec of_value_type vt =
  match (DT.develop_value_type vt) with
  | DT.Unknown | Ext _ | Unit -> assert false
  | Mac Char -> TinyInt
  | Mac Float -> Double
  | Mac String -> String
  | Mac Bool -> Boolean
  | Mac (I8 | U8) -> TinyInt
  | Mac (I16 | U16) -> SmallInt
  | Mac (I24 | U24 | I32 | U32) -> Int
  | Mac (I40 | U40 | I48 | U48 | I56 | U56 | I64 | U64) -> BigInt
  (* 128 bits would be 39 digits, but liborc would fail on 39.
   * It will happily store 128 bits inside its 128 bits value though.
   * Not all other ORC readers might perform that well unfortunately. *)
  | Mac (I128 | U128) -> Decimal { precision = 38 ; scale = 0 }
  | Tup ts ->
      (* There are no tuple in ORC so we use a Struct: *)
      Struct (
        Array.mapi (fun i t ->
          string_of_int i, of_value_type t.DT.vtyp) ts)
  | Vec (_, t) | Lst t | Set t ->
      Array (of_value_type t.DT.vtyp)
  | Rec kts ->
      (* Keep the order of definition but ignore private fields
       * that are going to be skipped over when serializing.
       * (TODO: also ignore shadowed fields): *)
      Struct (
        Array.filter_map (fun (k, t) ->
          if N.(is_private (field k)) then None else
          Some (k, of_value_type t.DT.vtyp)
        ) kts)
  | Sum mns ->
      UnionType (
        Array.map (fun (_, mn) ->
          of_value_type mn.DT.vtyp
        ) mns)
  | Map _ -> assert false (* No values of that type *)
  | Usr _ -> assert false (* Should have been developed *)

(*$= of_value_type & ~printer:BatPervasives.identity
  "struct<ip:int,mask:tinyint>" \
    (BatIO.to_string print (of_value_type T.cidrv4))
  "struct<pas_glop:int>" \
    (BatIO.to_string print (of_value_type \
      (DT.Rec [| "pas:glop", DT.make (Mac I32) |])))
*)

(* Check the outmost type is not nullable: *)
let of_type mn =
  assert (not mn.DT.nullable) ;
  of_value_type mn.vtyp

(* Map ORC types into the C++ class used to batch this type: *)
let batch_type_of = function
  | Boolean | TinyInt | SmallInt | Int | BigInt | Date -> "LongVectorBatch"
  | Float | Double -> "DoubleVectorBatch"
  | TimeStamp -> "TimestampVectorBatch"
  | String | Binary | VarChar _ | Char _ -> "StringVectorBatch"
  | Decimal { precision ; _ } ->
      if precision <= 18 then "Decimal64VectorBatch"
      else "Decimal128VectorBatch"
  | Struct _ -> "StructVectorBatch"
  | Array _ -> "ListVectorBatch"
  | UnionType _ -> "UnionVectorBatch"
  | Map _ -> "MapVectorBatch"

let batch_type_of_value_type = batch_type_of % of_value_type

let make_valid_cpp = DessserBackEndCLike.valid_identifier

let gensym =
  let seq = ref 0 in
  fun pref ->
    incr seq ;
    make_valid_cpp pref ^"_"^ string_of_int !seq

(*
 * Writing ORC files
 *)

(* Convert from OCaml value to a corresponding C++ value suitable for ORC: *)
let emit_conv_of_ocaml vt val_var oc =
  let p fmt = Printf.fprintf oc fmt in
  let scaled_int s =
    (* Signed small integers are shifted all the way to the left: *)
    p "(((intnat)Long_val(%s)) >> \
        (CHAR_BIT * sizeof(intnat) - %d - 1))"
      val_var s in
  match (DT.develop_value_type vt) with
  | DT.Unknown | Ext _ | Unit ->
      assert false
  | Mac Bool ->
      p "Bool_val(%s)" val_var
  | Mac Char ->
      p "Long_val(%s)" val_var
  | Mac (U8 | U16 | U24) ->
      p "Long_val(%s)" val_var  (* FIXME: should be Unsigned_long_val? *)
  | Mac U32 ->
      (* Assuming the custom val is suitably aligned: *)
      p "(*(uint32_t*)Data_custom_val(%s))" val_var
  | Mac U40 ->
      p "((*(uint64_t*)Data_custom_val(%s)) >> 24)" val_var
  | Mac U48 ->
      p "((*(uint64_t*)Data_custom_val(%s)) >> 16)" val_var
  | Mac U56 ->
      p "((*(uint64_t*)Data_custom_val(%s)) >> 8)" val_var
  | Mac U64 ->
      p "(*(uint64_t*)Data_custom_val(%s))" val_var
  | Mac U128 ->
      p "(*(uint128_t*)Data_custom_val(%s))" val_var
  | Mac I8 ->
      scaled_int 8
  | Mac I16 ->
      scaled_int 16
  | Mac I24 ->
      scaled_int 24
  | Mac I32 ->
      p "(*(int32_t*)Data_custom_val(%s))" val_var
  | Mac I40 ->
      p "((*(int64_t*)Data_custom_val(%s)) >> 24)" val_var
  | Mac I48 ->
      p "((*(int64_t*)Data_custom_val(%s)) >> 16)" val_var
  | Mac I56 ->
      p "((*(int64_t*)Data_custom_val(%s)) >> 8)" val_var
  | Mac I64 ->
      p "(*(int64_t*)Data_custom_val(%s))" val_var
  | Mac I128 ->
      p "(*(int128_t*)Data_custom_val(%s))" val_var
  | Mac Float ->
      p "Double_val(%s)" val_var
  | Mac String ->
      (* String_val return a pointer to the string, that the StringVectorBatch
       * will store. Obviously, we want it to store a non-relocatable copy and
       * then free it... FIXME *)
      (* Note: we pass the OCaml string length. the string might be actually
       * nul terminated before that, but that's not supposed to happen and
       * would not cause problems other than the string appear shorter. *)
      p "handler->keep_string(String_val(%s), caml_string_length(%s))"
        val_var val_var
  | Tup _ | Vec _ | Lst _ | Set _ | Rec _ | Map _ ->
      (* Compound types have no values of their own *)
      ()
  | Sum _ ->
      todo "emit_conv_of_ocaml for sum types"
  | Usr _ ->
      (* Should have been developed already *)
      assert false

(* Convert from OCaml value to a corresponding C++ value suitable for ORC
 * and write it in the vector buffer: *)
let rec emit_store_data indent vb_var i_var vt val_var oc =
  let p fmt = emit oc indent fmt in
  match DT.develop_value_type vt with
  | DT.Unknown | Ext _ -> assert false
  | Unit -> ()
  | Usr _ -> assert false (* must have been developed *)
  (* Never called on recursive types (dealt with iter_struct): *)
  | Tup _ | Vec _ | Lst _ | Set _ | Rec _ | Map _ | Sum _ ->
      assert false
  | Mac (Bool | Float | Char | I8 | U8 | I16 | U16 | I24 | U24 |
         I32 | U32 | I40 | U40 | I48 | U48 | I56 | U56 | I64 | U64) ->
      (* Most of the time we just store a single value in an array: *)
      p "%s->data[%s] = %t;" vb_var i_var (emit_conv_of_ocaml vt val_var)
  | Mac I128 ->
      (* ORC Int128 is a custom thing which constructor will accept only a
       * int16_t for the low bits, or two int64_t for high and low bits.
       * Initializing from an int128_t will /silently/ cast it to a single
       * int64_t and initialize the Int128 with garbage. *)
      let tmp_var = gensym "i128" in
      p "int128_t const %s = %t;" tmp_var (emit_conv_of_ocaml vt val_var) ;
      p "%s->values[%s] = Int128((int64_t)(%s >> 64), (int64_t)%s);"
        vb_var i_var tmp_var tmp_var
  | Mac U128 ->
      let tmp_var = gensym "u128" in
      p "uint128_t const %s = %t;" tmp_var (emit_conv_of_ocaml vt val_var) ;
      p "%s->values[%s] = Int128((int64_t)(%s >> 64U), (int64_t)%s);"
        vb_var i_var tmp_var tmp_var
  | Mac String ->
      p "assert(String_tag == Tag_val(%s));" val_var ;
      p "%s->data[%s] = %t;" vb_var i_var (emit_conv_of_ocaml vt val_var) ;
      p "%s->length[%s] = caml_string_length(%s);" vb_var i_var val_var

(* From the writers we need only two functions:
 *
 * The first one to "register interest" in a ORC files writer for a given
 * ORC type. That one only need to return a handle with all appropriate
 * information but must not perform any file opening or anything, as we
 * do not know yet if anything will actually be ever written.
 *
 * And the second function required by the worker is one to write a single
 * message. That one must create the file when required, flush a batch when
 * required, close the file etc, in addition to actually converting the
 * ocaml representation of the output tuple into an ORC value and write it
 * in the many involved vector batches. *)

(* Cast [batch_var] into a vector for the given type, named vb: *)
let emit_get_vb indent vb_var rtyp batch_var oc =
  let p fmt = emit oc indent fmt in
  let btyp = batch_type_of_value_type rtyp.DT.vtyp in
  p "%s *%s = dynamic_cast<%s *>(%s);" btyp vb_var btyp batch_var

(* Write a single OCaml value [val_var] of the given RamenType [rtyp] into the
 * ColumnVectorBatch [batch_var].
 * Note: [field_name] is for debug print only. *)
let rec emit_add_value_to_batch
          indent depth val_var batch_var i_var rtyp field_name oc =
  let p fmt = Printf.fprintf oc ("%s"^^fmt^^"\n") (indent_of indent) in
  let add_to_batch indent rtyp val_var =
    let p fmt = Printf.fprintf oc ("%s"^^fmt^^"\n") (indent_of indent) in
    let iter_struct tuple_with_1_element kts =
      Enum.fold (fun (oi, xi) (k, t) ->
        (* Skip over private fields.
         * TODO: also skip over shadowed fields! *)
        if N.(is_private (field k)) then
          oi, xi + 1
        else (
          p "{ /* Structure/Tuple item %s */" k ;
          let btyp = batch_type_of_value_type t.DT.vtyp in
          let arr_item = gensym "arr_item" in
          p "  %s *%s = dynamic_cast<%s *>(%s->fields[%d]);"
            btyp arr_item btyp batch_var oi ;
          let val_var =
            Option.map (fun v ->
              (* OCaml has no such tuples. Value is then unboxed. *)
              if tuple_with_1_element then v
              else Printf.sprintf "Field(%s, %d)" v xi
            ) val_var
          and field_name =
            if field_name = "" then k else field_name ^"."^ k
          and i_var =
            Printf.sprintf "%s->numElements" arr_item in
          emit_add_value_to_batch
            (indent + 1) (depth + 1) val_var arr_item i_var t field_name oc ;
          p "}" ;
          oi + 1, xi + 1
        )
      ) (0, 0) kts |> ignore
    in
    match DT.develop_value_type rtyp.DT.vtyp with
    | DT.Unknown | Usr _ | Ext _ ->
        assert false
    | Unit ->
        p "/* Skip unit value */"
    | Mac (Bool | Char | Float | String |
           U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 |
           I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128) ->
        Option.may (fun v ->
          p "/* Write the value for %s (of type %a) */"
            (if field_name <> "" then field_name else "root value")
            DT.print_maybe_nullable rtyp ;
          emit_store_data
            indent batch_var i_var rtyp.DT.vtyp v oc
        ) val_var
    | Tup ts ->
        Array.enum ts |>
        Enum.mapi (fun i t -> string_of_int i, t) |>
        iter_struct (Array.length ts = 1)
    | Rec kts ->
        Array.enum kts |>
        iter_struct (Array.length kts = 1)
    | Lst t | Set t | Vec (_, t) ->
        (* Regardless of [t], we treat a list as a "scalar" because
         * that's how it looks like for ORC: each new list value is
         * added to the [offsets] vector, while the list items are on
         * the side pushed to the global [elements] vector-batch. *)
        Option.may (fun v ->
          p "/* Write the values for %s (of type %a) */"
            (if field_name <> "" then field_name else "root value")
            DT.print_maybe_nullable t ;
          let vb = gensym "vb" in
          emit_get_vb
            indent vb t (batch_var ^"->elements.get()") oc ;
          let bi_lst = gensym "bi_lst" in
          p "uint64_t const %s = %s->numElements;" bi_lst vb ;
          (* FIXME: handle arrays of unboxed values *)
          let idx_var = gensym "idx" in
          p "unsigned %s;" idx_var ;
          p "for (%s = 0; %s < Wosize_val(%s); %s++) {"
            idx_var idx_var v idx_var ;
          let v_lst = gensym "v_lst" in
          p "  value %s = Field(%s, %s);" v_lst v idx_var ;
          emit_add_value_to_batch
            (indent + 1) 0 (Some v_lst) vb (bi_lst ^"+"^ idx_var) t
            (field_name ^".elmt") oc ;
          p "}"
        ) val_var ;
        (* Regardless of the value being NULL or not, when we have a
         * list we must initialize the offsets value. *)
        let nb_vals =
          match val_var with
          | None -> "0"
          | Some v -> Printf.sprintf "Wosize_val(%s)" v in
        p "%s->offsets[%s + 1] = %s->offsets[%s] + %s;"
          batch_var i_var batch_var i_var nb_vals ;
        if debug then (
          p "cerr << \"%s.offsets[\" << %s << \"+1]=\"" field_name i_var ;
          p "     << %s->offsets[%s + 1] << \"\\n\";" batch_var i_var)
    | Sum mns ->
        (* Unions: we have many children and we have to fill them independently.
         * Then in the union itself the [tags] array that we must fill with the
         * tag for that row, as well as the [offsets] array where to put the
         * offset in the children of the current row because liborc is a bit
         * lazy. *)
        Option.may (fun v ->
          p "switch (Tag_val(%s)) {" v ;
          Array.iteri (fun i (cstr_name, mn) ->
            p "case %d: { /* %s */" i cstr_name ;
            let vbtyp = batch_type_of_value_type mn.DT.vtyp in
            let vbs = gensym cstr_name in
            p "  %s *%s = dynamic_cast<%s *>(%s->children[%d]);"
              vbtyp vbs vbtyp batch_var i ;
            p "  %s->tags[%s] = %d;" batch_var i_var i ;
            p "  %s->offsets[%s] = %s->numElements;" batch_var i_var vbs ;
            let i_var = Printf.sprintf "%s->numElements" vbs in
            let v_cstr = gensym "v_cstr" in
            p "  value %s = Field(%s, 0);" v_cstr v ;
            emit_add_value_to_batch
              (indent + 1) (depth + 1) (Some v_cstr) vbs i_var mn
              (field_name ^"."^ cstr_name) oc ;
            p "  break;" ;
            p "}"
          ) mns ;
          p "default: assert(false);" ;
          p "}"
        ) val_var
    | Map _ -> assert false (* No values of that type *)
  in
  (match val_var with
  | Some v when rtyp.DT.nullable ->
      (* Only generate that code in the "not null" branches: *)
      p "if (Is_block(%s)) { /* Not null */" v ;
      (* The first non const constructor is "NotNull of ...": *)
      let non_null = gensym "non_null" in
      p "  value %s = Field(%s, 0);" non_null v ;
      let rtyp' = DT.force_maybe_nullable rtyp in
      add_to_batch (indent + 1) rtyp' (Some non_null) ;
      p "} else { /* Null */" ;
      (* liborc initializes hasNulls to false and notNull to all ones: *)
      if debug then
        p "  cerr << \"%s[\" << %s << \"] is null\\n\";"
          field_name i_var ;
      p "  %s->hasNulls = true;" batch_var ;
      p "  %s->notNull[%s] = 0;" batch_var i_var ;
      add_to_batch (indent + 1) rtyp None ;
      p "}"
  | _ ->
      if val_var = None then (
        (* A field above us was null. We have to set all subfields to null. *)
        if debug then
          p "cerr << \"%s[\" << %s << \"] is null\\n\";"
            field_name i_var ;
        p "%s->hasNulls = true;" batch_var ;
        p "%s->notNull[%s] = 0;" batch_var i_var
      ) ;
      add_to_batch indent rtyp val_var
  ) ;
  (* Also increase numElements: *)
  p "%s->numElements++;" batch_var ;
  if debug then
    p "cerr << \"%s->numElements=\" << %s->numElements << \"\\n\";"
      field_name batch_var

(* Generate an OCaml callable function named [func_name] that receives a
 * "handler" and an OCaml value of a given type [rtyp] and batch it.
 * Notice that we want the handler created with the fname and type, but
 * without creating a file nor a batch before values are actually added. *)
let emit_write_value func_name rtyp oc =
  let p fmt = emit oc 0 fmt in
  p "extern \"C\" CAMLprim value %s(" func_name ;
  p "    value hder_, value v_, value start_, value stop_)" ;
  p "{" ;
  p "  CAMLparam4(hder_, v_, start_, stop_);" ;
  p "  OrcHandler *handler = Handler_val(hder_);" ;
  p "  if (! handler->writer) handler->start_write();" ;
  emit_get_vb 1 "root" rtyp "handler->batch.get()" oc ;
  emit_add_value_to_batch
    1 0 (Some "v_") "root" "root->numElements" rtyp "" oc ;
  p "  if (root->numElements >= root->capacity) {" ;
  p "    handler->flush_batch(true);" ; (* might destroy the writer... *)
  p "    root->numElements = 0;" ;  (* ... but not the batch! *)
  p "  }" ;
  p "  // Since we survived, update this file timestamps:" ;
  p "  double start = Double_val(start_);" ;
  p "  double stop = Double_val(stop_);" ;
  p "  if (start < handler->start) handler->start = start;" ;
  p "  if (stop > handler->stop) handler->stop = stop;" ;
  p "  CAMLreturn(Val_unit);" ;
  p "}"

(* ...where flush_batch check for handle->num_batches and close the file and
 * reset handler->ri when the limit is reached.  *)

(*
 * Reading ORC files
 *)

(* Emits the code to read row [row_var] (an uint64_t) from [batch_var] (a
 * pointer to a ColumnVectorBatch), that's of RamenTypes.t [rtyp].
 * The result must be set in the (uninitialized) value [res_var].
 * [depth] is the recursion depth (ie. indent + const) that gives us the
 * name of the OCaml temp value we can use. *)
let rec emit_read_value_from_batch
    indent depth orig_batch_var row_var res_var rtyp oc =
  let p fmt = emit oc indent fmt in
  let tmp_var = "tmp" ^ string_of_int depth in
  (* Start with casting the batch to the proper type corresponding to [rtyp]
   * structure: *)
  let batch_var = gensym "batch" in
  emit_get_vb indent batch_var rtyp orig_batch_var oc ;
  let emit_read_nonnull indent =
    let p fmt = emit oc indent fmt in
    let emit_read_array t len_var =
      p "%s = caml_alloc(%s, 0);" res_var len_var ;
      let idx_var = gensym "idx" in
      p "for (uint64_t %s = 0; %s < %s; %s++) {"
        idx_var idx_var len_var idx_var ;
      let elmts_var = batch_var ^"->elements.get()" in
      let elmt_idx_var = gensym "row" in
      p "  uint64_t %s = %s->offsets[%s] + %s;"
        elmt_idx_var batch_var row_var idx_var ;
      emit_read_value_from_batch
        (indent + 1) (depth + 1) elmts_var elmt_idx_var tmp_var t oc ;
      p "  caml_modify(&Field(%s, %s), %s);" res_var idx_var tmp_var ;
      p "}"
    and emit_read_boxed ops custom_sz =
      (* See READ_BOXED in ringbuf/wrapper.c *)
      p "%s = caml_alloc_custom(&%s, %d, 0, 1);" res_var ops custom_sz ;
      p "memcpy(Data_custom_val(%s), &%s->data[%s], %d);"
        res_var batch_var row_var custom_sz
    and emit_read_boxed64_signed width =
      (* Makes a signed integer between 32 and 64 bits wide from a scaled
       * down int64_t (stored in a LongVectorBatch as an int64_t already): *)
      p "%s = caml_alloc_custom(&caml_int64_ops, 8, 0, 1);" res_var ;
      p "*(int64_t *)Data_custom_val(%s) = %s->data[%s] << %d;"
        res_var batch_var row_var (64 - width)
    and emit_read_boxed64_unsigned width =
      (* Same as above, with unsigned ints: *)
      p "%s = caml_alloc_custom(&uint64_ops, 8, 0, 1);" res_var ;
      p "*(uint64_t *)Data_custom_val(%s) = ((uint64_t)%s->data[%s]) << %d;"
        res_var batch_var row_var (64 - width)
    and emit_read_unboxed_signed shift =
      (* See READ_UNBOXED_INT in ringbuf/wrapper.c, remembering than i8 and
       * i16 are normal ints shifted all the way to the left. *)
      p "%s = Val_long((intnat)%s->data[%s] << \
                       (CHAR_BIT * sizeof(intnat) - %d - 1));"
        res_var batch_var row_var shift
    and emit_read_unboxed_unsigned typ_name =
      (* Same as above, but we have to take care that liborc extended the sign
       * of our unsigned value: *)
      p "%s = Val_long((%s)%s->data[%s]);" res_var typ_name batch_var row_var
    and emit_read_struct tuple_with_1_element kts =
      (* For structs, we build an OCaml tuple in the same order
       * as that of the ORC fields; unless that's a tuple with onle 1
       * element, in which case we return directly the unboxed var. *)
      if not tuple_with_1_element then
        p "%s = caml_alloc_tuple(%s->fields.size());" res_var batch_var ;
      Enum.iteri (fun i (k, t) ->
        p "/* Field %s */" k ;
        if debug then p "cerr << \"Field %s\" << endl;" k ;
        (* Use our tmp var to store the result of reading the i-th field: *)
        let field_var = gensym "field" in
        let field_batch_var =
          Printf.sprintf "%s->fields[%d]" batch_var i in
        emit_get_vb indent field_var t field_batch_var oc ;
        emit_read_value_from_batch
          indent (depth + 1) field_var row_var tmp_var t oc ;
        if tuple_with_1_element then
          p "%s = %s; // Single element tuple is unboxed" res_var tmp_var
        else
          p "Store_field(%s, %d, %s);" res_var i tmp_var
      ) kts
    and emit_case tag cstr_name vt =
      let iptyp = DT.make ~nullable:false vt in
      p "  case %d: /* %s : %a */" tag cstr_name DT.print_value_type vt ;
      p "    {" ;
      let val_var = gensym (make_valid_cpp cstr_name) in
      let chld_var = Printf.sprintf "%s->children[%d]" batch_var tag in
      emit_get_vb (indent + 3) val_var iptyp chld_var oc ;
      let offs_var = Printf.sprintf "%s->offsets[%s]" batch_var row_var in
      emit_read_value_from_batch
        (indent + 3) (depth + 1) val_var offs_var tmp_var iptyp oc ;
      p "      %s = caml_alloc_small(1, %d);" res_var tag ;
      p "      Field(%s, 0) = %s;" res_var tmp_var ;
      p "      break;" ;
      p "    }"
    and emit_default typ_name =
      p "  default: /* Invalid */" ;
      p "    {" ;
      p "      cerr << \"Invalid tag for %s: \" << %s->tags[%s] << \"\\n\";"
        typ_name batch_var row_var ;
      p "      assert(false);" ;  (* TODO: raise an OCaml exception *)
      p "      break;" ;
      p "    }"
    and emit_read_i128 signed =
      p "%s = caml_alloc_custom(&%s, 16, 0, 1);"
        res_var (if signed then "int128_ops" else "uint128_ops") ;
      let i128_var = gensym "i128" and i_var = gensym "i128" in
      p "Int128 *%s = &%s->values[%s];" i128_var batch_var row_var ;
      let std_typ = if signed then "int128_t" else "uint128_t" in
      p "%s const %s =" std_typ i_var ;
      p "  ((%s)%s->getHighBits() << 64%s) | (%s->getLowBits());"
        std_typ i128_var (if signed then "U" else "") i128_var ;
      p "memcpy(Data_custom_val(%s), &%s, 16);" res_var i_var
    in
    match DT.develop_value_type rtyp.DT.vtyp with
    | DT.Unknown | Usr _ | Ext _ -> assert false
    | Unit -> ()
    | Mac I8 -> emit_read_unboxed_signed 8
    | Mac I16 -> emit_read_unboxed_signed 16
    | Mac I24 -> emit_read_unboxed_signed 24
    | Mac I32 -> emit_read_boxed "caml_int32_ops" 4
    | Mac I40 -> emit_read_boxed64_signed 40
    | Mac I48 -> emit_read_boxed64_signed 48
    | Mac I56 -> emit_read_boxed64_signed 56
    | Mac I64 -> emit_read_boxed "caml_int64_ops" 8
    | Mac U8 -> emit_read_unboxed_unsigned "uint8_t"
    | Mac U16 -> emit_read_unboxed_unsigned "uint16_t"
    | Mac U24 -> emit_read_unboxed_unsigned "uint24_t"
    | Mac U32 -> emit_read_boxed "uint32_ops" 4
    | Mac U40 -> emit_read_boxed64_unsigned 40
    | Mac U48 -> emit_read_boxed64_unsigned 48
    | Mac U56 -> emit_read_boxed64_unsigned 56
    | Mac U64 -> emit_read_boxed "uint64_ops" 8
    | Mac I128 -> emit_read_i128 true
    | Mac U128 -> emit_read_i128 false
    | Mac Bool ->
        p "%s = Val_bool(%s->data[%s]);" res_var batch_var row_var
    | Mac Char -> emit_read_unboxed_unsigned "uint8_t"
    | Mac Float ->
        p "%s = caml_copy_double(%s->data[%s]);" res_var batch_var row_var
    | Mac String ->
        p "%s = caml_alloc_initialized_string(%s->length[%s], %s->data[%s]);"
          res_var batch_var row_var batch_var row_var
    | Sum mns as vtyp ->
        (* Cf. emit_store_data for a description of the encoding *)
        p "switch (%s->tags[%s]) {" batch_var row_var ;
        Array.iteri (fun i (cstr_name, mn) ->
          let vt = DT.develop_value_type mn.DT.vtyp in
          emit_case i cstr_name vt
        ) mns ;
        emit_default (DT.string_of_value_type vtyp) ;
        p "}"
    | Lst t ->
        (* The [elements] field will have all list items concatenated and
         * the [offsets] data buffer at row [row_var] will have the row
         * number of the starting element.
         * We can therefore get the size of that list, alloc an array for
         * [res_var] and then read each of the value into it (recursing). *)
        let len_var = gensym "len" in
        (* It seems that the offsets of the last+1 element is set to the
         * end of the elements, so this works also for the last element: *)
        p "int64_t %s =" len_var ;
        p "  %s->offsets[%s + 1] - %s->offsets[%s];"
          batch_var row_var batch_var row_var ;
        p "if (%s < 0) {" len_var ;
        p "  cerr << \"Invalid list of \" << %s << \" entries at row \" << %s"
          len_var row_var ;
        p "       << \"(offsets are \" << %s->offsets[%s]"
          batch_var row_var ;
        p "       << \" and then \" << %s->offsets[%s + 1] << \")\\n\";"
          batch_var row_var ;
        (* TODO: raise an OCaml exception instead *)
        p "  assert(false);" ;
        p "}" ;
        emit_read_array t ("((uint64_t)"^ len_var ^")") ;
    | Vec (d, t) ->
        emit_read_array t (string_of_int d)
    | Tup ts ->
        Array.enum ts |>
        Enum.mapi (fun i t -> string_of_int i, t) |>
        emit_read_struct (Array.length ts = 1)
    | Rec kts ->
        Array.enum kts //
        (fun (k, _) -> not N.(is_private (field k))) |>
        emit_read_struct (Array.length kts = 1)
    | Map _ -> assert false (* No values of that type *)
    | Set _ -> assert false (* No values of that type *)
  in
  (* If the type is nullable, check the null column (we can do this even
   * before getting the proper column vector. Convention: if we have no
   * notNull buffer then that means we have no nulls (it is assumed that
   * NULLs are rare in the wild): *)
  if rtyp.DT.nullable then (
    p "if (%s->hasNulls && !%s->notNull[%s]) {"
      orig_batch_var orig_batch_var row_var ;
    p "  %s = Val_long(0); /* DessserOCamlBackEndHelpers.Null */" res_var ;
    p "} else {" ;
    emit_read_nonnull (indent + 1) ;
    (* We must wrap res into a NotNull block (tag 0). Since we are back
     * from emit_read_nonnull we are free to reuse our tmp value: *)
    p "  %s = caml_alloc_small(1, 0);" tmp_var ;
    p "  Field(%s, 0) = %s;" tmp_var res_var ;
    p "  %s = %s;" res_var tmp_var ;
    p "}"
  ) else (
    (* Value is not nullable *)
    emit_read_nonnull indent
  )

(* Generate an OCaml callable function named [func_name] that receive a
 * file name, a batch size and an OCaml callback and read that file, calling
 * back OCaml code with each row as an OCaml value: *)
let emit_read_values func_name rtyp oc =
  let p fmt = emit oc 0 fmt in
  (* According to dessser, depth 0 is flat scalars: *)
  let max_depth = DT.(depth rtyp.vtyp) + 1 in
  p "extern \"C\" value %s(value path_, value batch_sz_, value cb_)" func_name ;
  p "{" ;
  p "  CAMLparam3(path_, batch_sz_, cb_);" ;
  p "  CAMLlocal1(res);" ;
  let rec localN n =
    if n < max_depth then
      let c = min 5 (max_depth - n) in
      p "  CAMLlocal%d(%a);" c
        (Enum.print ~sep:", " (fun oc n -> Printf.fprintf oc "tmp%d" n))
          (Enum.range n ~until:(n + c - 1)) ;
      localN (n + c) in
  localN 0 ;
  p "  char const *path = String_val(path_);" ;
  p "  unsigned batch_sz = Long_val(batch_sz_);" ;
  p "  unique_ptr<InputStream> in_file = readLocalFile(path);" ;
  p "  ReaderOptions options;" ;
  p "  unique_ptr<Reader> reader = createReader(move(in_file), options);" ;
  p "  RowReaderOptions row_options;" ;
  p "  unique_ptr<RowReader> row_reader =" ;
  p "    reader->createRowReader(row_options);" ;
  p "  unique_ptr<ColumnVectorBatch> batch =" ;
  p "    row_reader->createRowBatch(batch_sz);" ;
  p "  unsigned num_lines = 0;" ;
  p "  unsigned num_errors = 0;" ;
  p "  while (row_reader->next(*batch)) {" ;
  p "    for (uint64_t row = 0; row < batch->numElements; row++) {" ;
  emit_read_value_from_batch 3 0 "batch.get()" "row" "res" rtyp oc ;
  p "      res = caml_callback_exn(cb_, res);" ;
  p "      if (Is_exception_result(res)) {" ;
  p "        res = Extract_exception(res);" ;
  p "        // Print only the first 10 such exceptions:" ;
  p "        if (num_errors++ < 10) {" ;
  p "          cerr << \"Exception while reading ORC file \" << path" ;
  p "               << \": to_be_printed\\n\";" ;
  p "        }" ;
  p "      }" ;
  p "      num_lines++;" ;
  p "    }" ;
  p "  }" ;
  p "  // Return the number of lines and errors:" ;
  p "  res = caml_alloc(2, 0);" ;
  p "  Store_field(res, 0, Val_long(num_lines));" ;
  p "  Store_field(res, 1, Val_long(num_errors));" ;
  p "  CAMLreturn(res);" ;
  p "}"

let emit_intro oc =
  let p fmt = emit oc 0 fmt in
  p "/* This code is automatically generated. Edition is futile. */" ;
  p "#include <cassert>" ;
  p "#include <orc/OrcFile.hh>" ;
  p "extern \"C\" {" ;
  p "#  include <limits.h> /* CHAR_BIT */" ;
  p "#  include <caml/mlvalues.h>" ;
  p "#  include <caml/memory.h>" ;
  p "#  include <caml/alloc.h>" ;
  p "#  include <caml/custom.h>" ;
  p "#  include <caml/callback.h>" ;
  p "extern struct custom_operations uint128_ops;" ;
  p "extern struct custom_operations uint64_ops;" ;
  p "extern struct custom_operations uint32_ops;" ;
  p "extern struct custom_operations int128_ops;" ;
  p "extern struct custom_operations caml_int64_ops;" ;
  p "extern struct custom_operations caml_int32_ops;" ;
  p "}" ;
  p "typedef __int128_t int128_t;" ;
  p "typedef __uint128_t uint128_t;" ;
  p "using namespace std;" ;
  p "using namespace orc;" ;
  p "" ;
  p "class OrcHandler {" ;
  p "    unique_ptr<Type> type;" ;
  p "    string fname;" ;
  p "    bool const with_index;" ;
  p "    unsigned const batch_size;" ;
  p "    unsigned const max_batches;" ;
  p "    unsigned num_batches;" ;
  p "    bool archive;" ;
  p "    std::vector<char> strs;" ;
  p "  public:" ;
  p "    OrcHandler(string schema, string fn, bool with_index, unsigned bsz, unsigned mb, bool arc);" ;
  p "    ~OrcHandler();" ;
  p "    void start_write();" ;
  p "    void flush_batch(bool);" ;
  p "    char *keep_string(char const *, size_t);" ;
  p "    unique_ptr<OutputStream> outStream;" ;
  p "    unique_ptr<Writer> writer;" ;
  p "    unique_ptr<ColumnVectorBatch> batch;" ;
  p "    double start, stop;" ;
  p "};" ;
  p "" ;
  p "#define Handler_val(v) (*((class OrcHandler **)Data_custom_val(v)))" ;
  p ""

let emit_outro oc =
  ignore oc
