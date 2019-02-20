(* ORC types and mapping with Ramen types.
 *
 * No comprehensive doc for ORC but see:
 * http://hadooptutorial.info/hive-data-types-examples/
 *)
open Batteries
open RamenHelpers
module T = RamenTypes

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

(* Orc types string format use colon as a separator, therefore we must
 * encode it somehow (note: there is no escape mecanism that I know of
 * for ORC schema strings, but we can make trade our dots with ORC's
 * comas. *)
let print_label oc str =
  String.print oc (String.nreplace ~sub:":" ~by:"." ~str)

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
          (Tuple2.print ~first:"" ~last:"" ~sep:":" print_label print)) kts
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
 * In the other way around that is not a conversion but a cast. *)
let rec of_structure = function
  | T.TEmpty | T.TAny -> assert false
  (* We use TNum to denotes a normal OCaml integer. We use some to encode
   * Cidrs for instance. *)
  | T.TNum -> Int
  | T.TFloat -> Double
  | T.TString -> String
  | T.TBool -> Boolean
  | T.TU8 -> SmallInt (* Upgrade as ORC integers are all signed *)
  | T.TU16 -> Int
  | T.TU32 -> BigInt
  | T.TU64 -> Decimal { precision = 20 ; scale = 0 }
  | T.TU128 -> Decimal { precision = 39 ; scale = 0 }
  | T.TI8 -> TinyInt
  | T.TI16 -> SmallInt
  | T.TI32 -> Int
  | T.TI64 -> BigInt
  | T.TI128 -> Decimal { precision = 39 ; scale = 0 }
  | T.TEth -> BigInt
  (* We store IPv4/6 in the smallest numeric type that fits.
   * For TIp, we use a union. *)
  | T.TIpv4 -> Int
  | T.TIpv6 -> Decimal { precision = 39 ; scale = 0 }
  | T.TIp ->
      UnionType [| of_structure T.TIpv4 ; of_structure T.TIpv6 |]
  (* For CIDR we use a structure of an IP and a mask: *)
  | T.TCidrv4 ->
      Struct [| "ip", of_structure T.TIpv4 ; "mask", TinyInt |]
  | T.TCidrv6 ->
      Struct [| "ip", of_structure T.TIpv6 ; "mask", TinyInt |]
  | T.TCidr ->
      UnionType [| of_structure T.TCidrv4 ; of_structure T.TCidrv6 |]
  | T.TTuple ts ->
      (* There are no tuple in ORC so we use a Struct: *)
      Struct (
        Array.mapi (fun i t ->
          string_of_int i, of_structure t.T.structure) ts)
  | T.TVec (_, t) | T.TList t ->
      Array (of_structure t.T.structure)
  | T.TRecord kts ->
      (* Keep the order of definition: *)
      Struct (
        Array.map (fun (k, t) ->
          k, of_structure t.T.structure) kts)

(*$= of_structure & ~printer:BatPervasives.identity
  "struct<ip:int,mask:tinyint>" \
    (BatIO.to_string print (of_structure T.TCidrv4))
  "struct<pas.glop:int>" \
    (BatIO.to_string print (of_structure \
      (T.TRecord [| "pas:glop", T.make T.TI32 |])))
*)

(* Until we have a single output value, mimic outputting a record: *)
let of_tuple typ =
  Struct (
    List.enum typ /@
    (fun ft ->
      (ft.RamenTuple.name :> string),
      of_structure ft.typ.T.structure) |>
   Array.of_enum)

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

let batch_type_of_structure = batch_type_of % of_structure

let gensym =
  let seq = ref 0 in
  fun pref ->
    incr seq ;
    pref ^"_"^ string_of_int !seq

(*
 * Writing ORC files
 *)

(* Convert from OCaml value to a corresponding C++ value suitable for ORC: *)
let emit_conv_of_ocaml st val_var oc =
  let scaled_int s =
    (* Signed small integers are shifted all the way to the left: *)
    Printf.fprintf oc
      "(((intnat)Long_val(%s)) >> \
        (numeric_limits<intnat>::digits - %d))"
      val_var s in
  match st with
  | T.TEmpty | T.TAny ->
      assert false
  | T.TBool ->
      Printf.fprintf oc "Bool_val(%s)" val_var
  | T.TNum | T.TU8 | T.TU16 ->
      Printf.fprintf oc "Long_val(%s)" val_var
  | T.TU32 | T.TIpv4 ->
      (* Assuming the custom val is suitably aligned: *)
      Printf.fprintf oc "(*(uint32_t*)Data_custom_val(%s))" val_var
  | T.TU64 | T.TEth ->
      Printf.fprintf oc "(*(uint64_t*)Data_custom_val(%s))" val_var
  | T.TU128 | T.TIpv6 ->
      Printf.fprintf oc "(*(uint128_t*)Data_custom_val(%s))" val_var
  | T.TI8 ->
      scaled_int 8
  | T.TI16 ->
      scaled_int 16
  | T.TI32 ->
      Printf.fprintf oc "(*(int32_t*)Data_custom_val(%s))" val_var
  | T.TI64 ->
      Printf.fprintf oc "(*(int64_t*)Data_custom_val(%s))" val_var
  | T.TI128 ->
      Printf.fprintf oc "(*(int128_t*)Data_custom_val(%s))" val_var
  | T.TFloat ->
      Printf.fprintf oc "Double_val(%s)" val_var
  | T.TString ->
      Printf.fprintf oc "String_val(%s)" val_var
  | T.TIp | T.TCidrv4 | T.TCidrv6 | T.TCidr
  | T.TTuple _ | T.TVec _ | T.TList _ | T.TRecord _ ->
      (* Compound types have no values of their own *)
      ()

(* Helper to emit code at a given level: *)

let emit oc indent fmt =
  Printf.fprintf oc ("%s" ^^ fmt ^^ "\n") (indent_of indent)

(* Convert from OCaml value to a corresponding C++ value suitable for ORC
 * and write it in the vector buffer: *)
let rec emit_store_data indent vb_var i_var st val_var oc =
  let p fmt = emit oc indent fmt in
  (* Most of the time we just store a single value in an array: *)
  let a arr_name =
    p "%s->%s[%s] = %t;"
      vb_var arr_name i_var (emit_conv_of_ocaml st val_var)
  in
  match st with
  | T.TEmpty | T.TAny
  (* Never called on recursive types (dealt with iter_scalars): *)
  | T.TTuple _ | T.TVec _ | T.TList _ | T.TRecord _ ->
      assert false
  | T.TBool -> a "data"
  | T.TNum | T.TU8 | T.TU16 -> a "data"
  | T.TU32 | T.TIpv4 -> a "data"
  | T.TU64 -> a "values"
  | T.TU128 | T.TIpv6 -> a "values"
  | T.TEth -> a "data"
  | T.TI8 -> a "data"
  | T.TI16 -> a "data"
  | T.TI32 -> a "data"
  | T.TI64 -> a "data"
  | T.TI128 -> a "values"
  | T.TFloat -> a "data"
  | T.TString ->
      p "%s->data[%s] = %t;" vb_var i_var (emit_conv_of_ocaml st val_var) ;
      p "%s->length[%s] = caml_string_length(%s);" vb_var i_var val_var
  | T.TIp ->
      (* Unions: we have 2 children (0 for v4 and 1 for v6) and we have
       * to fill them independently. Then in the Union itself the [tags]
       * array that we must fill with the tag for that row, as well as
       * the [offsets] array where to put the offset in the children of
       * the current row because liborc is a bit lazy.
       * First, are we v4 or v6? *)
      let vb4 = batch_type_of_structure T.TIpv4
      and vb6 = batch_type_of_structure T.TIpv6 in
      p "if (Tag_val(%s) == 0) { /* IPv4 */" val_var ;
      let vbs = gensym "ips" in
      p "  %s *%s = dynamic_cast<%s *>(%s->children[0]);" vb4 vbs vb4 vb_var ;
      p "  %s->tags[%s] = 0;" vb_var i_var ;
      p "  %s->offsets[%s] = %s->numElements;" vb_var i_var vbs ;
      emit_store_data (indent+1) vbs (vbs ^"->numElements") T.TIpv4 val_var oc ;
      p "  %s->numElements ++;" vbs ;
      p "} else {" ;
      p "  %s *%s = dynamic_cast<%s *>(%s->children[1]);" vb6 vbs vb6 vb_var ;
      p "  %s->tags[%s] = 1;" vb_var i_var ;
      p "  %s->offsets[%s] = %s->numElements;" vb_var i_var vbs ;
      emit_store_data (indent+1) vbs (vbs ^"->numElements") T.TIpv6 val_var oc ;
      p "  %s->numElements ++;" vbs ;
      p "}"
  | T.TCidrv4 ->
      (* A structure of IPv4 and mask. Write each field recursively. *)
      let ip_vb = batch_type_of_structure T.TIpv4 in
      let ips = gensym "ips" in
      p "%s *%s = dynamic_cast<%s *>(%s->fields[0]);" ip_vb ips ip_vb vb_var ;
      let ip_var = Printf.sprintf "Field(%s, 0)" val_var in
      emit_store_data indent ips i_var T.TIpv4 ip_var oc ;
      let msk_vb = batch_type_of_structure T.TNum in
      let msks = gensym "msks" in
      p "%s *%s = dynamic_cast<%s *>(%s->fields[1]);" msk_vb msks msk_vb vb_var ;
      let msk_var = Printf.sprintf "Field(%s, 1)" val_var in
      emit_store_data indent msks i_var T.TNum msk_var oc
  | T.TCidrv6 ->
      (* A structure of IPv6 and mask. Write each field recursively. *)
      let ip_vb = batch_type_of_structure T.TIpv6 in
      let ips = gensym "ips" in
      p "%s *%s = dynamic_cast<%s *>(%s->fields[0]);" ip_vb ips ip_vb vb_var ;
      let ip_var = Printf.sprintf "Field(%s, 0)" val_var in
      emit_store_data indent ips i_var T.TIpv6 ip_var oc ;
      let msk_vb = batch_type_of_structure T.TNum in
      let msks = gensym "msks" in
      p "%s *%s = dynamic_cast<%s *>(%s->fields[1]);" msk_vb msks msk_vb vb_var ;
      let msk_var = Printf.sprintf "Field(%s, 1)" val_var in
      emit_store_data indent msks i_var T.TNum msk_var oc
  | T.TCidr ->
      (* Another union with tag 0 for v4 and tag 1 for v6: *)
      let vb4 = batch_type_of_structure T.TCidrv4
      and vb6 = batch_type_of_structure T.TCidrv6 in
      let vbs = gensym "vbs" in
      p "if (Tag_val(%s) == 0) { /* CIDRv4 */" val_var ;
      p "  %s *%s = dynamic_cast<%s *>(%s->children[0]);" vb4 vbs vb4 vb_var ;
      p "  %s->tags[%s] = 0;" vb_var i_var ;
      p "  %s->offsets[%s] = %s->numElements;" vb_var i_var vbs ;
      let ip_vb = batch_type_of_structure T.TIpv4 in
      let ips = gensym "ips" in
      p "%s *%s = dynamic_cast<%s *>(%s->fields[0]);" ip_vb ips ip_vb vbs ;
      let msk_vb = batch_type_of_structure T.TNum in
      let msks = gensym "msks" in
      p "%s *%s = dynamic_cast<%s *>(%s->fields[1]);" msk_vb msks msk_vb vbs ;
      let fld n = Printf.sprintf "Field(%s, %d)" val_var n in
      emit_store_data (indent+1) ips (vbs ^"->numElements") T.TIpv4 (fld 0) oc ;
      emit_store_data (indent+1) msks (vbs ^"->numElements") T.TNum (fld 1) oc ;
      p "  %s->numElements ++;" vbs ;
      p "} else { /* CIDRv6 */" ;
      p "  %s *%s = dynamic_cast<%s *>(%s->children[1]);" vb6 vbs vb6 vb_var ;
      p "  %s->tags[%s] = 1;" vb_var i_var ;
      p "  %s->offsets[%s] = %s->numElements;" vb_var i_var vbs ;
      let ip_vb = batch_type_of_structure T.TIpv6 in
      p "%s *%s = dynamic_cast<%s *>(%s->fields[0]);" ip_vb ips ip_vb vbs ;
      let msk_vb = batch_type_of_structure T.TNum in
      p "%s *%s = dynamic_cast<%s *>(%s->fields[1]);" msk_vb msks msk_vb vbs ;
      let fld n = Printf.sprintf "Field(%s, %d)" val_var n in
      emit_store_data (indent+1) ips (vbs ^"->numElements") T.TIpv6 (fld 0) oc ;
      emit_store_data (indent+1) msks (vbs ^"->numElements") T.TNum (fld 1) oc ;
      p "  %s->numElements ++;" vbs ;
      p "}"

(* Helper to call a function [f] on every scalar subtypes of the given Ramen
 * type [rtyp], while providing the corresponding [batch_var] (C++ expression
 * holding the ORC batch) and [val_var] (C++ expression holding the OCaml
 * value), and also [field_name] for cosmetics) *)
let iter_scalars indent ?(skip_root=false) oc rtyp batch_var val_var
                 field_name f =
  let p indent fmt = Printf.fprintf oc ("%s"^^fmt^^"\n") (indent_of indent) in
  let rec loop indent depth rtyp batch_var val_var field_name =
    match val_var with
    | Some v when rtyp.T.nullable ->
        p indent "if (Is_block(%s)) { /* Not null */" v ;
        (* The first non const constructor is "NotNull of ...": *)
        let non_null = gensym "non_null" in
        p (indent+1) "value %s = Field(%s, 0);" non_null v ;
        let rtyp' = { rtyp with nullable = false } in
        loop (indent+1) depth rtyp' batch_var (Some non_null) field_name ;
        p indent "} else { /* Null */" ;
        if depth > 0 || not skip_root then
          f (indent+1) rtyp batch_var ~is_list:false None field_name ;
        p indent "}"
    | _ ->
        let iter_struct =
          Enum.iteri (fun i (k, t) ->
            p indent "{ /* Structure/Tuple item %s */" k ;
            let btyp = batch_type_of_structure t.T.structure in
            let arr_item = gensym "arr_item" in
            p indent "  %s *%s = dynamic_cast<%s *>(%s->fields[%d]);"
              btyp arr_item btyp batch_var i ;
            let val_var =
              Option.map (fun v ->
                Printf.sprintf "Field(%s, %d)" v i) val_var
            and field_name =
              if field_name = "" then k else field_name ^"."^ k in
            loop (indent+1) (depth+1) t arr_item val_var field_name ;
            p indent "}"
          ) in
        (match rtyp.T.structure with
        | T.TEmpty | T.TAny ->
            assert false
        | T.TBool
        | T.TU8 | T.TU16 | T.TU32 | T.TU64
        | T.TI8 | T.TI16 | T.TI32 | T.TI64
        | T.TU128 | T.TI128
        | T.TIpv4 | T.TIpv6 | T.TIp
        | T.TCidrv4 | T.TCidrv6 | T.TCidr
        | T.TNum | T.TEth | T.TFloat | T.TString ->
            if depth > 0 || not skip_root then
              f indent rtyp batch_var ~is_list:false val_var field_name
        | T.TTuple ts ->
            Array.enum ts |>
            Enum.mapi (fun i t -> string_of_int i, t) |>
            iter_struct
        | T.TRecord kts ->
            (* FIXME: we should not store private fields *)
            Array.enum kts |> iter_struct
        | T.TList t | T.TVec (_, t) ->
            (* Regardless of [t], we treat a list as a "scalar". because
             * that's how it looks like for ORC: each new list value is
             * added to the [offsets] vector, while the list items are on
             * the side pushed to the global [elements] vector-batch. *)
            f indent t batch_var ~is_list:true val_var field_name)
  in
  loop indent 0 rtyp batch_var val_var field_name

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
  let btyp = batch_type_of_structure rtyp.T.structure in
  p "%s *%s = dynamic_cast<%s *>(%s);" btyp vb_var btyp batch_var

(* Write a single OCaml value [val_var] of the given RamenType into the
 * ColumnVectorBatch [batch_var]: *)
let rec emit_add_value_in_batch
          indent val_var batch_var i_var rtyp field_name oc =
  let p indent fmt = Printf.fprintf oc ("%s"^^fmt^^"\n") (indent_of indent) in
  iter_scalars indent oc rtyp batch_var val_var field_name
    (fun indent rtyp batch_var ~is_list val_var field_name ->
      p indent "{ /* Write the value%s for %s (of type %a) */"
        (if is_list then "s" else "")
        (if field_name <> "" then field_name else "root value")
        T.print_typ rtyp ;
      let vb = gensym "vb" in
      (match val_var with
      | None -> (* When the value is NULL *)
          (* liborc initializes hasNulls to false and notNull to all ones: *)
          emit_get_vb (indent+1) vb rtyp batch_var oc ;
          p (indent+1) "%s->hasNulls = true;" vb ;
          p (indent+1) "%s->notNull[%s] = 0;" vb i_var
      | Some val_var ->
          if is_list then (
            (* For lists, our value is still the list and [batch_var] is
             * still the [ListVectorBatch]. We have to write the current
             * size of [batch_var->elements] into
             * [batch_var->offsets(`i_var)], and then append the actual list
             * values to [batch_var->elements]. But as those values can have
             * any type including a compound type, we must recurse. *)
            emit_get_vb (indent+1) vb rtyp (batch_var ^"->elements.get()") oc ;
            let bi_lst = gensym "bi_lst" in
            p (indent+1) "auto const %s = %s->numElements;" bi_lst vb ;
            p (indent+1) "%s->offsets[%s] = %s;" batch_var i_var bi_lst ;
            (* FIXME: handle arrays of unboxed values *)
            p (indent+1) "unsigned i;" ;
            p (indent+1) "for (i=0; i < Wosize_val(%s); i++) {" val_var ;
            let v_lst = gensym "v_lst" in
            p (indent+1) "  value %s = Field(%s, i);" v_lst val_var ;
            emit_add_value_in_batch
              (indent+2) (Some v_lst) vb (bi_lst^"+i") rtyp
              (field_name ^".elmt") oc ;
            p (indent+1) "}" ;
            (* Must set numElements of the list itself: *)
            p (indent+1) "%s->numElements += i;" vb ;
            (* Liborc also expects us to set the offsets of the next element
             * in case this one is the last (offsets size is capa+1) *)
            p (indent+1) "%s->offsets[%s + 1] = %s + i;"
              batch_var i_var bi_lst
          ) else (
            emit_get_vb (indent+1) vb rtyp batch_var oc ;
            emit_store_data (indent+1) vb i_var rtyp.T.structure val_var oc
          )) ;
      p indent "}")

(* Now let's turn to set_numElements_recursively, which sets the numElements
 * count in all involved vectors beside the root one.  It seams unfortunate
 * that there are one count per vector instead of a single one as I cannot
 * think of any way those counters would not share the same value. Therefore
 * we have to copy [root->numElements], also in [bi], in any other
 * subvectors. *)
let rec emit_set_numElements
          indent rtyp batch_var i_var field_name oc =
  let p indent fmt = Printf.fprintf oc ("%s"^^fmt) (indent_of indent) in
  iter_scalars indent ~skip_root:true oc rtyp batch_var None field_name
    (fun indent _rtyp batch_var ~is_list _val_var field_name ->
      (* We do not have anything to do for a list beyond setting its
       * own numElemebnts (ie number of offsets). *)
      ignore is_list ;
      p indent "{ /* Set numElements for %s */\n" field_name ;
      emit_get_vb (indent+1) "vb" rtyp batch_var oc ;
      p (indent+1) "vb->numElements = %s;\n" i_var ;
      p indent "}\n")

(* Generate an OCaml callable function named [func_name] that receives a
 * "handler" and an OCaml value of a given type [rtyp] and batch it.
 * Notice that we want the handler created with the fname and type, but
 * without creating a file nor a batch before values are actually added. *)
let emit_write_value func_name rtyp oc =
  let p fmt = emit oc 0 fmt in
  p "extern \"C\" CAMLprim value %s(value hder_, value v_)" func_name ;
  p "{" ;
  p "  CAMLparam2(hder_, v_);" ;
  p "  LazyWriter *handler = Handler_val(hder_);" ;
  p "  if (! handler->writer) handler->start_write();" ;
  emit_get_vb 1 "root" rtyp "handler->batch.get()" oc ;
  p "  uint64_t const bi = root->numElements;" ;
  emit_add_value_in_batch 1 (Some "v_") "root" "bi" rtyp "" oc ;
  p "  if (++root->numElements >= root->capacity) {" ;
  emit_set_numElements 2 rtyp "root" "bi" "" oc ;
  p "    handler->flush_batch(true);" ; (* might destroy the writer... *)
  p "    root->numElements = 0;" ;  (* ... but not the batch! *)
  p "  }" ;
  p "  CAMLreturn(Val_unit);" ;
  p "}"

(* ...where flush_batch check for handle->num_batches and close the file and
 * reset handler->ri when the limit is reached.  *)

let emit_intro oc =
  let p fmt = emit oc 0 fmt in
  p "/* This code is automatically generated. Edition is futile. */" ;
  p "#include <cassert>" ;
  p "#include <orc/OrcFile.hh>" ;
  p "extern \"C\" {" ;
  p "#  include <caml/mlvalues.h>" ;
  p "#  include <caml/memory.h>" ;
  p "#  include <caml/alloc.h>" ;
  p "#  include <caml/custom.h>" ;
  p "}" ;
  p "typedef __int128_t int128_t;" ;
  p "typedef __uint128_t uint128_t;" ;
  p "using namespace std;" ;
  p "using namespace orc;" ;
  p "" ;
  p "class LazyWriter {" ;
  p "    string fname;" ;
  p "    unique_ptr<Type> type;" ;
  p "    unsigned const batch_size;" ;
  p "    unsigned const max_batches;" ;
  p "    unsigned num_batches;" ;
  p "  public:" ;
  p "    LazyWriter(string fn, string schema, unsigned bsz, unsigned mb);" ;
  p "    ~LazyWriter();" ;
  p "    void start_write();" ;
  p "    void flush_batch(bool);" ;
  p "    unique_ptr<OutputStream> outStream;" ;
  p "    unique_ptr<Writer> writer;" ;
  p "    unique_ptr<ColumnVectorBatch> batch;" ;
  p "};" ;
  p "" ;
  p "#define Handler_val(v) (*((class LazyWriter **)Data_custom_val(v)))" ;
  p ""

let emit_outro oc =
  ignore oc
