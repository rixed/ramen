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
  | T.TEmpty | T.TNum | T.TAny ->
      assert false
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
  (* We store all IPs as binary. For TIp, we use a union. *)
  | T.TIpv4 | T.TIpv6 -> Binary
  | T.TIp ->
      UnionType [| Binary ; Binary |]
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
      Struct (
        Array.map (fun (k, t) ->
          k, of_structure t.T.structure) kts)

(*$= of_structure & ~printer:BatPervasives.identity
  "struct<ip:binary,mask:tinyint>" \
    (BatIO.to_string print (of_structure T.TCidrv4))
  "struct<pas.glop:int>" \
    (BatIO.to_string print (of_structure \
      (T.TRecord [| "pas:glop", T.make T.TI32 |])))
*)

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

(* Convert from OCaml value to a corresponding C++ value suitable for ORC: *)
let emit_conv_of_ocaml rtyp val_var oc =
  let scaled_int s =
    (* Signed small integers are shifted all the way to the left: *)
    Printf.fprintf oc
      "(((intnat)(%s)) >> \
        (Long_val(numeric_limits<intnat>::digits - %d)))"
      val_var s in
  match rtyp.T.structure with
  | T.TBool ->
      Printf.fprintf oc "Bool_val(%s)" val_var
  | T.TU8 | T.TU16 ->
      Printf.fprintf oc "Long_val(%s)" val_var
  | T.TU32 ->
      (* Assuming the custom val is suitably aligned: *)
      Printf.fprintf oc "(*(uint32_t*)Data_custom_val(%s))" val_var
  | T.TU64 | T.TEth ->
      Printf.fprintf oc "(*(uint64_t*)Data_custom_val(%s))" val_var
  | T.TI8 ->
      scaled_int 8
  | T.TI16 ->
      scaled_int 16
  | T.TI32 ->
      Printf.fprintf oc "(*(int32_t*)Data_custom_val(%s))" val_var
  | T.TI64 ->
      Printf.fprintf oc "(*(int64_t*)Data_custom_val(%s))" val_var
  | _ -> todo "emit_conv_of_ocaml"

(* Helper to build the indentation in front of printed lines. We just use 2
 * spaces like normal people: *)
let indent_of i = String.make (i*2) ' '

(* Helper to call a function [f] on every scalar subtypes of the given Ramen
 * type [rtyp], while providing the corresponding [batch_val] (C++ expression
 * holding the ORC batch) and [val_var] (C++ expression holding the OCaml
 * value), and also [field_name] for cosmetics) *)
let iter_scalars indent ?(skip_root=false) oc rtyp batch_val val_var field_name f =
  let p indent fmt = Printf.fprintf oc ("%s"^^fmt^^"\n") (indent_of indent) in
  let rec loop indent depth rtyp batch_val val_var field_name =
    match val_var with
    | Some v when rtyp.T.nullable ->
        p indent "if (Is_block(%s)) { /* Not null */" v ;
        (* The first non const constructor is "NotNull of ...": *)
        p (indent+1) "%s = Field(%s, 0);" v v ;
        let rtyp' = { rtyp with nullable = false } in
        loop (indent+1) depth rtyp' batch_val val_var field_name ;
        p indent "} else { /* Null */" ;
        if depth > 0 || not skip_root then
          f (indent+1) rtyp batch_val None field_name ;
        p indent "}"
    | _ ->
        (match rtyp.T.structure with
        | T.TEmpty | T.TNum | T.TAny ->
            assert false
        | T.TBool
        | T.TU8 | T.TU16 | T.TU32 | T.TU64
        | T.TI8 | T.TI16 | T.TI32 | T.TI64
        | T.TEth ->
            if depth > 0 || not skip_root then
              f indent rtyp batch_val val_var field_name
        | T.TU128 | T.TI128
        | T.TIpv4 | T.TIpv6 | T.TIp
        | T.TCidrv4 | T.TCidrv6 | T.TCidr
        | T.TFloat | T.TString | T.TList _ | T.TTuple _ | T.TVec _ ->
            todo "iter_scalars"
        | T.TRecord kts ->
            Array.iteri (fun i (k, t) ->
              let batch_val = Printf.sprintf "%s->fields[%d]" batch_val i
              and val_var =
                Option.map (fun v ->
                  Printf.sprintf "Field(%s, %d)" v i) val_var
              and field_name =
                if field_name = "" then k else field_name ^"."^ k in
              loop indent (depth+1) t batch_val val_var field_name
            ) kts)
  in
  loop indent 0 rtyp batch_val val_var field_name

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

(* Cast [batch_val] into a vector for the given type, named vb: *)
let emit_get_vb indent vb_val rtyp batch_val oc =
  let p fmt = Printf.fprintf oc ("%s"^^fmt) (indent_of indent) in
  let btyp = batch_type_of (of_structure rtyp.T.structure) in
  p "%s *%s = dynamic_cast<%s *>(%s);\n" btyp vb_val btyp batch_val

(* Write a single OCaml value [val_val] of the given RamenType into the
 * ColumnVectorBatch [batch_val]: *)
let rec emit_add_value_in_batch
          indent val_var batch_val rtyp field_name oc =
  let p indent fmt = Printf.fprintf oc ("%s"^^fmt^^"\n") (indent_of indent) in
  iter_scalars indent oc rtyp batch_val val_var field_name
    (fun indent rtyp batch_val val_var field_name ->
      p indent "{ /* Write the value for %s (of type %a) */"
        field_name T.print_typ rtyp ;
      emit_get_vb (indent+1) "vb" rtyp batch_val oc ;
      (match val_var with
      | None -> (* When the value is NULL *)
          (* liborc initializes hasNulls to false and notNull to all ones: *)
          p (indent+1) "vb->hasNulls = true;" ;
          p (indent+1) "vb->notNull[bi] = 0;"
      | Some val_var ->
        let arr_name =
          (* For some reason Decimal*Vector uses values rather than data: *)
          match of_structure rtyp.T.structure with
          | Decimal _ -> "values"
          | _ -> "data" in
        p (indent+1) "vb->%s[bi] = %t;"
          arr_name (emit_conv_of_ocaml rtyp val_var)) ;
      p indent "}")

(* Now let's turn to set_numElements_recursively, which sets the numElements
 * count in all involved vectors beside the root one.  It seams unfortunate
 * that there are one count per vector instead of a single one as I cannot
 * think of any way those counters would not share the same value. Therefore
 * we have to copy [root->numElements], also in [bi], in any other
 * subvectors. *)
let rec emit_set_numElements
          indent rtyp batch_val field_name oc =
  let p indent fmt = Printf.fprintf oc ("%s"^^fmt) (indent_of indent) in
  iter_scalars indent ~skip_root:true oc rtyp batch_val None field_name
    (fun indent _rtyp batch_val _val_var field_name ->
      p indent "{ /* Set numElements for %s */\n" field_name ;
      emit_get_vb indent "vb" rtyp batch_val oc ;
      p (indent+1) "vb->numElements = bi;\n" ;
      p indent "}\n")

(* Write a function named [func_name] that receives a "handler" and an OCaml
 * Value of a given type [rtyp] and batch it.
 * Notice that we want the handler created with the fname and type, but
 * without creating a file nor a batch before values are actually added. *)
let emit_batch_value func_name rtyp oc =
  let p fmt = Printf.fprintf oc (fmt ^^ "\n") in
  p "void %s(Handler *handler, value val)" func_name ;
  p "{" ;
  p "  if (! handler->writer) handler->start_write();" ;
  emit_get_vb 1 "root" rtyp "handler->batch.get()" oc ;
  p "  uint64_t const bi = root->numElements;" ;
  emit_add_value_in_batch 1 (Some "val") "root" rtyp "" oc ;
  p "  if (++root->numElements >= root->capacity) {" ;
  emit_set_numElements 2 rtyp "root" "" oc ;
  p "    handler->flush_batch();" ; (* might destroy the writer... *)
  p "    root->numElements = 0;" ;  (* ... but not the batch! *)
  p "  }" ;
  p "}"

(* ...where flush_batch check for handle->num_batches and close the file and
 * reset handler->ri when the limit is reached.  *)

(*
 * Compiling C++ at runtime against ORC and its dependencies is hard enough,
 * we can spare ourselves the chore of also having to build a helper C++
 * lib, especially since so little extra code is needed beyond what's
 * generated. So let's add it here instead: *)
let emit_heading oc =
  let p fmt = Printf.fprintf oc (fmt ^^ "\n") in
  p "/* This code is automatically generated. Edition is futile. */" ;
  p "#include <cassert>" ;
  p "#include <caml/mlvalues.h>" ;
  p "#include <orc/OrcFile.hh>" ;
  p "using namespace std;" ;
  p "using namespace orc;" ;
  p "" ;
  p "class Handler {" ;
  p "    string fname;" ;
  p "    unsigned const batch_size;" ;
  p "    unsigned const max_batches;" ;
  p "    unsigned num_batches;" ;
  p "    Type *type;" ;
  p "  public:" ;
  p "    Handler(string fn, unsigned bsz, unsigned mb, Type *t) :" ;
  p "      fname(fn), batch_size(bsz), max_batches(mb), type(t)," ;
  p "      num_batches(0) {};" ;
  p "    ~Handler() { flush_batch(); };" ;
  p "    void start_write() {" ;
  p "      unique_ptr<OutputStream> outStream = writeLocalFile(fname);" ;
  p "      WriterOptions options;" ;
  p "      writer = createWriter(*type, outStream.get(), options);" ;
  p "      batch = writer->createRowBatch(batch_size);" ;
  p "      assert(batch);" ;
  p "    }" ;
  p "    void flush_batch() {" ;
  p "      writer->add(*batch);" ;
  p "      if (++num_batches >= max_batches) {" ;
  p "        writer->close();" ;
  p "        writer.reset();" ;
  (* and then we keep using the batch created by the first writer. This is
   * not a problem, as writer->createRowBatch just call the proper
   * createRowBatch for that Type. *)
  p "      }" ;
  p "    }" ;
  p "    unique_ptr<Writer> writer;" ;
  p "    unique_ptr<ColumnVectorBatch> batch;" ;
  p "};" ;
  p ""

let test () =
  let rtyp = T.(
    make (TRecord [| "x", make TI8 ; "y", make ~nullable:false TU64 |])) in
  emit_heading stdout ;
  emit_batch_value "test" rtyp stdout ;
  Printf.printf "\nint main(void) { return 0; }\n"
