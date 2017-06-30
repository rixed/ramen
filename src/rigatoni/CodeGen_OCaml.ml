(* Code generator for OCaml.
 * We do not use a templating system because thanks to libraries the generated
 * code should be minimal and limited to the less constant pieces of code.
 *)

(* Each operation must be implemented for OCaml which is used both as a
 * prototyping language and as a reference implementation. Some operations may
 * also exist for other languages.
 *)

(* Regarding generated code names: all generated OCaml identifier has a name
 * ending with underscore.  In addition, tuple field names are prefixed by
 * the tuple name. *)

open Batteries
open Log
open RamenSharedTypes

(* Tuple deconstruction as a function parameter: *)
let id_of_field_name ?(tuple="in") field =
  tuple ^"_"^ field ^"_"

let id_of_field_typ ?tuple field_typ =
  id_of_field_name ?tuple field_typ.Lang.Tuple.name

let list_print_as_tuple = List.print ~first:"(" ~last:")" ~sep:", "

let print_tuple_deconstruct tuple =
  let print_field fmt field_typ =
      String.print fmt (id_of_field_typ ~tuple field_typ)
  in
  list_print_as_tuple print_field

(* Emit the code that return the sersize of a fixed size type *)
let emit_sersize_of_fixsz_typ oc = function
  | TFloat  -> Int.print oc (RingBufLib.round_up_to_rb_word 8)
  | TBool | TU8 | TI8 -> Int.print oc (RingBufLib.round_up_to_rb_word 1)
  | TU16 | TI16 -> Int.print oc (RingBufLib.round_up_to_rb_word 2)
  | TU32 | TI32 -> Int.print oc (RingBufLib.round_up_to_rb_word 4)
  | TU64 | TI64 -> Int.print oc (RingBufLib.round_up_to_rb_word 8)
  | TU128 | TI128 -> Int.print oc (RingBufLib.round_up_to_rb_word 16)
  | TString -> assert false
  | TNum -> assert false

(* Emit the code computing the sersize of some variable *)
let emit_sersize_of_field_var typ oc var =
  match typ with
  | TString ->
    Printf.fprintf oc "\
      (%d + RingBufLib.round_up_to_rb_word(String.length %s))"
      RingBufLib.rb_word_bytes var
  | _ -> emit_sersize_of_fixsz_typ oc typ

(* Emit the code to retrieve the sersize of some serialized value *)
let rec emit_sersize_of_field_tx tx_var offs_var nulli oc field =
  let open Lang.Tuple in
  if field.nullable then (
    Printf.fprintf oc "if RingBuf.get_bit %s %d then %a else 0"
      tx_var nulli
      (emit_sersize_of_field_tx tx_var offs_var nulli) { field with nullable = false }
  ) else match field.typ with
    | TString ->
      Printf.fprintf oc "\
        %d + RingBufLib.round_up_to_rb_word(RingBuf.read_word %s %s)"
        RingBufLib.rb_word_bytes tx_var offs_var
    | _ -> emit_sersize_of_fixsz_typ oc field.typ

let id_of_typ typ =
  match typ with
  | TFloat  -> "float"
  | TString -> "string"
  | TBool   -> "bool"
  | TU8     -> "u8"
  | TU16    -> "u16"
  | TU32    -> "u32"
  | TU64    -> "u64"
  | TU128   -> "u128"
  | TI8     -> "i8"
  | TI16    -> "i16"
  | TI32    -> "i32"
  | TI64    -> "i64"
  | TI128   -> "i128"
  | TNum    -> assert false

let emit_value_of_string typ oc var =
  Printf.fprintf oc "CodeGenLib.%s_of_string %s" (id_of_typ typ) var

let nullmask_bytes_of_tuple_typ tuple_typ =
  let open Lang.Tuple in
  List.fold_left (fun s field_typ ->
    if field_typ.nullable then s+1 else s) 0 tuple_typ |>
  RingBufLib.bytes_for_bits |>
  RingBufLib.round_up_to_rb_word

let emit_sersize_of_tuple name oc tuple_typ =
  let open Lang.Tuple in
  (* For nullable fields each ringbuf record has a bitmask of as many bits as
   * there are nullable fields, rounded to the greater or equal multiple of rb_word_size.
   * This is a constant given by the tuple type:
   *)
  let size_for_nullmask = nullmask_bytes_of_tuple_typ tuple_typ in
  (* Let's emit the function definition, deconstructing the tuple with identifiers
   * for varsized fields: *)
  Printf.fprintf oc "let %s %a =\n\t\
      %d (* null bitmask *) + %a\n"
    name
    (print_tuple_deconstruct "out") tuple_typ
    size_for_nullmask
    (List.print ~first:"" ~last:"" ~sep:" + " (fun fmt field_typ ->
      let id = id_of_field_typ ~tuple:"out" field_typ in
      if field_typ.nullable then (
        Printf.fprintf fmt "(match %s with None -> 0 | Some x_ -> %a)"
          id
          (emit_sersize_of_field_var field_typ.typ) "x_"
      ) else (
        Printf.fprintf fmt "%a" (emit_sersize_of_field_var field_typ.typ) id
      ))) tuple_typ

let emit_set_value tx_var offs_var field_var oc field_typ =
  Printf.fprintf oc "RingBuf.write_%s %s %s %s"
    (id_of_typ field_typ) tx_var offs_var field_var

(* The function that will serialize the fields of the tuple at the given
 * addresses.  Everything else (allocating on the RB and writing the record
 * size) is independent of the tuple type and is handled in the library.
 * Also, the lib ensure that null bitmask is 0 at the beginning. Returns
 * the final offset for checking with serialized size of this tuple. *)
let emit_serialize_tuple name oc tuple_typ =
  let open Lang.Tuple in
  Printf.fprintf oc "let %s tx_ %a =\n"
    name
    (print_tuple_deconstruct "out") tuple_typ ;
  let nullmask_bytes = nullmask_bytes_of_tuple_typ tuple_typ in
  Printf.fprintf oc "\tlet offs_ = %d in\n" nullmask_bytes ;
  (* Start by zeroing the nullmask *)
  if nullmask_bytes > 0 then
    Printf.fprintf oc "\tRingBuf.zero_bytes tx_ 0 %d ; (* zero the nullmask *)\n"
      nullmask_bytes ;
  let _ = List.fold_left (fun nulli field ->
      let id = id_of_field_typ ~tuple:"out" field in
      if field.nullable then (
        (* Write either nothing (since the nullmask is initialized with 0) or
         * the nullmask bit and the value *)
        Printf.fprintf oc "\tlet offs_ = match %s with\n" id ;
        Printf.fprintf oc "\t| None -> offs_\n" ;
        Printf.fprintf oc "\t| Some x_ ->\n" ;
        Printf.fprintf oc "\t\tRingBuf.set_bit tx_ %d ;\n" nulli ;
        Printf.fprintf oc "\t\t%a ;\n"
          (emit_set_value "tx_" "offs_" "x_") field.typ ;
        Printf.fprintf oc "\t\toffs_ + %a in\n"
          (emit_sersize_of_field_var field.typ) "x_"
      ) else (
        Printf.fprintf oc "\t%a ;\n"
          (emit_set_value "tx_" "offs_" id) field.typ ;
        Printf.fprintf oc "\tlet offs_ = offs_ + %a in\n"
          (emit_sersize_of_field_var field.typ) id
      ) ;
      nulli + (if field.nullable then 1 else 0)
    ) 0 tuple_typ in
  Printf.fprintf oc "\toffs_\n"

(* Emit a function that, given an array of strings (corresponding to a line of
 * CSV) will return the tuple defined by [tuple_typ] or raises
 * some exception *)
let emit_tuple_of_strings name csv_null oc tuple_typ =
  let open Lang.Tuple in
  Printf.fprintf oc "let %s strs_ =\n" name ;
  Printf.fprintf oc "\t(\n" ;
  let nb_fields = List.length tuple_typ in
  List.iteri (fun i field_typ ->
    let sep = if i < nb_fields - 1 then "," else "" in
    if field_typ.nullable then (
      Printf.fprintf oc "\t\t(let s_ = strs_.(%d) in\n" i ;
      Printf.fprintf oc "\t\tif s_ = %S then None else Some (%a))%s\n"
        csv_null
        (emit_value_of_string field_typ.typ) "s_"
        sep
    ) else (
      let s_var = Printf.sprintf "strs_.(%d)" i in
      Printf.fprintf oc "\t\t%a%s\n"
        (emit_value_of_string field_typ.typ) s_var sep
    )) tuple_typ ;
  Printf.fprintf oc "\t)\n"

(* Given a Tuple.typ, generate the ReadCSVFile operation. *)
let emit_read_csv_file oc csv_fname csv_separator csv_null tuple_typ =
  (* The dynamic part comes from the unpredictable field list.
   * For each input line, we want to read all fields and build a tuple.
   * Then we want to write this tuple in some ring buffer.
   * We need to generate these functions:
   * - reading a CSV string into a tuple type (when nullable fields are option type)
   * - given such a tuple, return its serialized size
   * - given a pointer toward the ring buffer, serialize the tuple *)
  Printf.fprintf oc "open Stdint\n\n\
    %a\n%a\n%a\n\
    let () =\n\
      \tLwt_main.run (\n\
      \t\tCodeGenLib.read_csv_file %S %S sersize_of_tuple_ serialize_tuple_ tuple_of_strings_)\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    (emit_tuple_of_strings "tuple_of_strings_" csv_null) tuple_typ
    csv_fname csv_separator

let emit_tuple tuple oc tuple_typ =
  print_tuple_deconstruct tuple oc tuple_typ

let emit_in_tuple ?(tuple="in") mentioned and_all_others oc in_tuple_typ =
  print_tuple_deconstruct tuple oc (List.filter_map (fun field_typ ->
    if and_all_others || Set.mem field_typ.Lang.Tuple.name mentioned then
      Some field_typ else None) in_tuple_typ)

(* We do not want to read the value from the RB each time it's used,
 * so extract a tuple from the ring buffer. As an optimisation, read
 * (and return) only the mentioned fields. *)
let emit_read_tuple name mentioned and_all_others oc in_tuple_typ =
  let open Lang.Tuple in
  Printf.fprintf oc "\
    let %s tx_ =\n\
    \tlet offs_ = %d in\n"
    name
    (nullmask_bytes_of_tuple_typ in_tuple_typ) ;
  let _ = List.fold_left (fun nulli field ->
      let id = id_of_field_typ ~tuple:"in" field in
      if and_all_others || Set.mem field.name mentioned then (
        Printf.fprintf oc "\tlet %s =\n" id ;
        if field.nullable then
          Printf.fprintf oc "\
            \t\tif RingBuf.get_bit tx_ %d then\n\
            \t\t\tSome (RingBuf.read_%s tx_ offs_) else None in\n"
            nulli
            (id_of_typ field.typ)
        else
          Printf.fprintf oc "\
            \t\tRingBuf.read_%s tx_ offs_ in\n"
            (id_of_typ field.typ) ;
        Printf.fprintf oc "\tlet offs_ = " ;
        if field.nullable then
          Printf.fprintf oc "\
            if %s = None then offs_ else offs_ + %a in\n" id
            (emit_sersize_of_field_var field.typ) id
        else
          Printf.fprintf oc "\
            offs_ + %a in\n"
            (emit_sersize_of_field_var field.typ) id ;
      ) else (
        Printf.fprintf oc "\tlet offs_ = offs_ + (%a) in\n"
          (emit_sersize_of_field_tx "tx_" "offs_" nulli) field
      ) ;
      nulli + (if field.nullable then 1 else 0)
    ) 0 in_tuple_typ in
  Printf.fprintf oc "\tignore offs_ ;\n" ; (* avoid a warning *)
  Printf.fprintf oc "\t%a\n"
    (emit_in_tuple mentioned and_all_others) in_tuple_typ

(* Returns the set of all field names from the "in" tuple mentioned
 * anywhere in the given expression: *)
let rec add_mentioned prev =
  let open Lang.Expr in
  function
  | Const _ | Param _
    -> prev
  | Field (_, tuple, field) ->
    if Lang.same_tuple_as_in !tuple then Set.add field prev else prev
  | AggrMin (_, e) | AggrMax (_, e) | AggrSum (_, e) | AggrAnd (_, e)
  | AggrOr (_, e) | AggrFirst (_, e) | AggrLast (_, e) | Age (_, e)
  | Not (_, e) | Defined (_, e) | Cast (_, e)
    -> add_mentioned prev e
  | AggrPercentile (_, e1, e2) | Sequence (_, e1, e2)
  | Add (_, e1, e2) | Sub (_, e1, e2) | Mul (_, e1, e2) | Div (_, e1, e2)
  | IDiv (_, e1, e2) | Exp (_, e1, e2) | And (_, e1, e2) | Or (_, e1, e2)
  | Ge (_, e1, e2) | Gt (_, e1, e2) | Eq (_, e1, e2) | Mod (_, e1, e2)
    -> add_mentioned (add_mentioned prev e1) e2

let add_all_mentioned lst =
  let rec loop prev = function
    | [] -> prev
    | e :: e' -> loop (add_mentioned prev e) e'
  in
  loop Set.empty lst

let emit_scalar oc =
  let open Stdint in
  let open Lang.Scalar in
  function
  | VFloat  f -> Printf.fprintf oc "%f" f
  | VString s -> Printf.fprintf oc "%S" s
  | VBool   b -> Printf.fprintf oc "%b" b
  | VU8     n -> Printf.fprintf oc "(Uint8.of_int %d)" (Uint8.to_int n)
  | VU16    n -> Printf.fprintf oc "(Uint16.of_int %d)" (Uint16.to_int n)
  | VU32    n -> Printf.fprintf oc "(Uint32.of_int32 %sl)" (Uint32.to_string n)
  | VU64    n -> Printf.fprintf oc "(Uint64.of_int64 %sL)" (Uint64.to_string n)
  | VU128   n -> Printf.fprintf oc "(Uint128.of_string %S)" (Uint128.to_string n)
  | VI8     n -> Printf.fprintf oc "(Int8.of_int %d)" (Int8.to_int n)
  | VI16    n -> Printf.fprintf oc "(Int16.of_int %d)" (Int16.to_int n)
  | VI32    n -> Printf.fprintf oc "%sl" (Int32.to_string n)
  | VI64    n -> Printf.fprintf oc "%sL" (Int64.to_string n)
  | VI128   n -> Printf.fprintf oc "(Int128.of_string %S)" (Int128.to_string n)

(* Given a function name and an output type, return the actual function
 * returning that type, and the type input parameters must be converted into,
 * if any *)
let funcname_of_expr =
  let open Lang.Expr in
  function
  | AggrMin _ -> "min"
  | AggrMax _ -> "max"
  | AggrPercentile _ -> "percentile"
  | AggrSum _ | Add _ -> "add"
  | AggrAnd _ | And _ -> "(&&)"
  | AggrOr _ | Or _ -> "(||)"
  | AggrFirst _ -> "fst"
  | AggrLast _ -> "snd"
  | Age _ -> "age"
  | Sequence _ -> "sequence"
  | Cast _ -> "identity"
  | Not _ -> "not"
  | Defined _ -> "defined"
  | Sub _ -> "sub"
  | Mul _ -> "mul"
  | Div _ | IDiv _ -> "div"
  | Mod _ -> "rem"
  | Exp _ -> "exp"
  | Ge _ -> "(>=)"
  | Gt _ -> "(>)"
  | Eq _ -> "(=)"
  | Const _ | Param _ | Field _ ->
    assert false

(* Returns the function name and the type to convert the args to *)
let implementation_of expr =
  let open Lang in
  let open Expr in
  let name = funcname_of_expr expr in
  let out_typ = typ_of expr in
  match expr, out_typ.scalar_typ with
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Div _), Some TFloat -> "BatFloat."^ name, Some TFloat
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TU8 -> "Uint8."^ name, Some TU8
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TU16 -> "Uint16."^ name, Some TU16
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TU32 -> "Uint32."^ name, Some TU32
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TU64 -> "Uint64."^ name, Some TU64
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TU128 -> "Uint128."^ name, Some TU128
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TI8 -> "Int8."^ name, Some TI8
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TI16 -> "Int16."^ name, Some TI16
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TI32 -> "Int32."^ name, Some TI32
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TI64 -> "Int64."^ name, Some TI64
  | (AggrSum _|Add _|Sub _|Mul _|IDiv _|Mod _), Some TI128 -> "Int128."^ name, Some TI128
  | (Not _|And _|Or _), Some TBool -> name, Some TBool
  | (Ge _| Gt _| Eq _), Some TBool -> name, None (* No conversion necessary *)
  | (AggrMax _|AggrMin _|AggrFirst _|AggrLast _), _ -> name, None (* No conversion necessary *)
  | (Age _|AggrPercentile _), Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as to_typ) ->
    "CodeGenLib."^ name ^"_"^ IO.to_string Scalar.print_typ to_typ, Some TFloat
  | Cast _, t -> "CodeGenLib."^ name, t
  (* Sequence build a sequence of as-large-as-convenient integers (signed or
   * not) *)
  | Sequence _, Some TI128 -> "CodeGenLib."^ name, Some TI128
  | _, Some to_typ ->
    failwith ("Cannot find implementation of "^ name ^" for type "^
              IO.to_string Scalar.print_typ to_typ)
  | _, None ->
    assert false

let name_of_aggr =
  let open Lang in
  let open Expr in
  function
  | AggrMin (t, _) | AggrMax (t, _) | AggrPercentile (t, _, _)
  | AggrSum (t, _) | AggrAnd (t, _) | AggrOr (t, _) | AggrFirst (t, _)
  | AggrLast (t, _) ->
    "field_"^ string_of_int t.uniq_num
  | Const _ | Param _ | Field _ | Age _ | Sequence _ | Not _ | Defined _
  | Add _ | Sub _ | Mul _ | Div _ | IDiv _ | Exp _ | And _ | Or _ | Ge _
  | Gt _ | Eq _ | Mod _ | Cast _ ->
    assert false

  let otype_of_type = function
    | TFloat -> "float" | TString -> "string" | TBool -> "bool"
    | TU8 -> "uint8" | TU16 -> "uint16" | TU32 -> "uint32" | TU64 -> "uint64" | TU128 -> "uint128"
    | TI8 -> "int8" | TI16 -> "int16" | TI32 -> "int32" | TI64 -> "int64" | TI128 -> "int128"
    | TNum -> assert false

let otype_of_aggr e =
  Option.get Lang.Expr.((typ_of e).scalar_typ) |>
  otype_of_type

let omod_of_type = function
  | TFloat -> "BatFloat"
  | TString -> "BatString"
  | TBool -> "BatBool"
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128 as t ->
    String.capitalize (otype_of_type t)
  | TNum -> assert false

let conv_from_to from_typ to_typ p fmt e =
  match from_typ, to_typ with
  | a, b when a = b -> p fmt e
  | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128|TString|TFloat),
      (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128)
  | TString, (TFloat|TBool) ->
    Printf.fprintf fmt "(%s.of_%s %a)"
      (omod_of_type to_typ)
      (otype_of_type from_typ)
      p e
  | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128),
      (TFloat|TString)
  | (TFloat|TBool), TString ->
    Printf.fprintf fmt "(%s.to_%s %a)"
      (omod_of_type from_typ)
      (otype_of_type to_typ)
      p e
  | _ ->
    failwith (Printf.sprintf "Cannot find converter from type %s to type %s"
                (IO.to_string Lang.Scalar.print_typ from_typ)
                (IO.to_string Lang.Scalar.print_typ to_typ))

let conv_from_to_opt from_typ to_typ_opt p fmt e =
  match to_typ_opt with
  | Some to_typ -> conv_from_to from_typ to_typ p fmt e
  | None -> p fmt e (* No conversion required *)

(* Implementation_of gives us the type operands must be converted to.
 * This printer wrap an expression into a converter according to its current
 * type. *)
let rec conv_to to_typ fmt e =
  let open Lang in
  let from_typ = Expr.((typ_of e).scalar_typ) in
  match from_typ, to_typ with
  | Some a, Some b -> conv_from_to a b emit_expr fmt e
  | _, None -> emit_expr fmt e (* No conversion required *)
  | None, Some b ->
    failwith (Printf.sprintf "Cannot convert from unknown type into %s"
                (IO.to_string Scalar.print_typ b))

and emit_expr oc =
  let open Lang.Expr in
  function
  | Const (_, c) ->
    emit_scalar oc c
  | Field (_, tuple, field) ->
    Printf.fprintf oc "%s" (id_of_field_name ~tuple:!tuple field)
  | Param _ ->
    failwith "TODO: code gen for params"
  | (AggrMin _ | AggrMax _ | AggrSum _ | AggrAnd _ | AggrOr _ | AggrFirst _
    | AggrLast _ | AggrPercentile _ as expr) ->
     (* This assumes there is a parameter named aggr_ for the aggregates *)
     Printf.fprintf oc "aggr_.%s" (name_of_aggr expr)
  | Age (_, e) | Not (_, e) | Cast (_, e) as expr ->
    emit_function expr oc e
  | Defined (_, e) ->
    Printf.fprintf oc "(%a <> None)" emit_expr e
  | Add (_, e1, e2) | Sub (_, e1, e2) | Mul (_, e1, e2)
  | Div (_, e1, e2) | IDiv (_, e1, e2) | Exp (_, e1, e2) | And (_, e1, e2)
  | Or (_, e1, e2) | Ge (_, e1, e2) | Gt (_, e1, e2) | Eq (_, e1, e2)
  | Sequence (_, e1, e2) | Mod (_, e1, e2) as expr ->
    emit_function2 expr oc e1 e2

(* The output must be of type [t] *)
and emit_function expr oc e =
  let impl, arg_typ = implementation_of expr in
  Printf.fprintf oc "(%s%s %a)"
    (if Lang.Expr.is_nullable e then "BatOption.map " else "")
    impl
    (conv_to arg_typ) e

and emit_function2 expr oc e1 e2 =
  let impl, arg_typ = implementation_of expr in
  (* When we have no conversion to do, e1 and e2 still have to have the same type or
   * the compiler will complain: *)
  let open Lang in
  let open Expr in
  let arg_typ =
    if arg_typ <> None then arg_typ else
      match (typ_of e1).scalar_typ, (typ_of e2).scalar_typ with
      | None, None -> None
      | Some t1, None -> Some t1
      | None, Some t2 -> Some t2
      | Some t1, Some t2 -> Some (Scalar.larger_type (t1, t2))
  in
  if is_nullable e1 then (
    Printf.fprintf oc "\
      (match %a with None -> None | Some v1_ -> "
      emit_expr e1 ;
    if is_nullable e2 then (
      Printf.fprintf oc "\
        (match %a with None -> None | Some v2_ -> %s %a %a)"
        emit_expr e2
        impl
        (conv_from_to_opt TString arg_typ String.print) "v1_"
        (conv_from_to_opt TString arg_typ String.print) "v2_"
    ) else (
      Printf.fprintf oc "%s %a %a"
        impl
        (conv_from_to_opt TString arg_typ String.print) "v1_"
        (conv_to arg_typ) e2
    ) ;
    Printf.fprintf oc ")"
  ) else (
    if is_nullable e2 then (
      Printf.fprintf oc "\
        (match %a with None -> None | Some v2_ -> %s %a %a)"
        emit_expr e2
        impl
        (conv_to arg_typ) e1
        (conv_from_to_opt TString arg_typ String.print) "v2_"
    ) else (
      Printf.fprintf oc "(%s %a %a)"
        impl
        (conv_to arg_typ) e1
        (conv_to arg_typ) e2
    )
  )

let emit_expr_of_input_tuple
      ?(with_aggr=false) ?(with_first_last=false) ?(always_true=false)
      name in_tuple_typ mentioned and_all_others oc expr =
  Printf.fprintf oc "let %s%s %a "
    name
    (if with_aggr then " aggr_" else "")
    (emit_in_tuple mentioned and_all_others) in_tuple_typ ;
  if with_first_last then
    Printf.fprintf oc "%a %a "
      (emit_in_tuple ~tuple:"first" mentioned and_all_others) in_tuple_typ
      (emit_in_tuple ~tuple:"last" mentioned and_all_others) in_tuple_typ ;
  if always_true then
    Printf.fprintf oc "= true\n"
  else
    Printf.fprintf oc "=\n\t%a\n" emit_expr expr

(* If with aggr we have the aggregate record as first parameter
 * and also the first and last incoming tuple of this aggr as additional
 * parameters *)
let emit_expr_select ?(honor_star=true) ?(with_aggr=false)
                     ?(with_first_last=false)
                     name in_tuple_typ mentioned
                     and_all_others out_tuple_typ oc exprs =
  let open Lang in
  Printf.fprintf oc "let %s " name ;
  if with_aggr then Printf.fprintf oc "aggr_ " ;
  Printf.fprintf oc "%a "
    (emit_in_tuple mentioned and_all_others) in_tuple_typ ;
  if with_first_last then
    Printf.fprintf oc "%a %a "
      (emit_in_tuple ~tuple:"first" mentioned and_all_others) in_tuple_typ
      (emit_in_tuple ~tuple:"last" mentioned and_all_others) in_tuple_typ ;
  Printf.fprintf oc "=\n\t(" ;
  (* We will iter through the selected fields, marking those which have been
   * outputted so that we do not output them again in the STAR operator. *)
  let outputted = ref Set.empty in
  List.iteri (fun i expr ->
      Printf.fprintf oc "%s\n\t\t%a"
        (if i > 0 then "," else "")
        emit_expr expr ;
      match expr with
      | Expr.Field (_, tuple, field) when same_tuple_as_in !tuple ->
        outputted := Set.add field !outputted
      | _ -> ()
    ) exprs ;
  (* The only difference between selecting out_tuple out of in_tuple
   * and returning the key of in_tuple is that we don't want to implement
   * the star when building the key: *)
  if and_all_others && honor_star then (
    List.iteri (fun i field ->
        if not (Set.mem field.Tuple.name !outputted) then
          Printf.fprintf oc "%s\n\t\t%s%s"
            (if i > 0 || exprs <> [] then "," else "")
            (if i = 0 then "(* All other fields *)\n\t\t" else "")
            (id_of_field_name field.Tuple.name)
      ) out_tuple_typ (* we want those fields ordered according to out tuple not in tuple! *)
  ) ;
  Printf.fprintf oc "\n\t)\n"

let exprs_of_selected_fields =
  List.map (fun sf -> sf.Lang.Operation.expr)

let emit_yield oc in_tuple_typ out_tuple_typ selected_fields =
  let mentioned =
    let all_exprs = List.map (fun sf -> sf.Lang.Operation.expr) selected_fields in
    add_all_mentioned all_exprs in
  Printf.fprintf oc "open Stdint\n\n\
    %a\n%a\n%a\n\
    let () =\n\
      \tLwt_main.run (\n\
      \t\tCodeGenLib.yield sersize_of_tuple_ serialize_tuple_ select_)\n"
    (emit_expr_select "select_" in_tuple_typ mentioned false out_tuple_typ)
      (exprs_of_selected_fields selected_fields)
    (emit_sersize_of_tuple "sersize_of_tuple_") out_tuple_typ
    (emit_serialize_tuple "serialize_tuple_") out_tuple_typ

let emit_select oc in_tuple_typ out_tuple_typ
                selected_fields and_all_others where =
  (* We need:
   * - a function to extract the fields used from input (and all others, optionally)
   * - a function corresponding to the where filter
   * - a function to write the output tuple and another one to compute the sersize *)
  let mentioned =
    let all_exprs = where :: List.map (fun sf -> sf.Lang.Operation.expr) selected_fields in
    add_all_mentioned all_exprs in
  Printf.fprintf oc "open Stdint\n\n\
    %a\n%a\n%a\n%a\n%a\n\
    let () =\n\
      \tLwt_main.run (\n\
      \t\tCodeGenLib.select read_tuple_ sersize_of_tuple_ serialize_tuple_ where_ select_)\n"
    (emit_read_tuple "read_tuple_" mentioned and_all_others) in_tuple_typ
    (emit_expr_of_input_tuple "where_" in_tuple_typ mentioned and_all_others) where
    (emit_expr_select "select_" in_tuple_typ mentioned and_all_others out_tuple_typ)
      (exprs_of_selected_fields selected_fields)
    (emit_sersize_of_tuple "sersize_of_tuple_") out_tuple_typ
    (emit_serialize_tuple "serialize_tuple_") out_tuple_typ

let for_each_aggr_fun selected_fields commit_when flush_when f =
  List.iter (fun sf ->
      Lang.Expr.aggr_iter f sf.Lang.Operation.expr
    ) selected_fields ;
  Lang.Expr.aggr_iter f commit_when ;
  Option.may (fun flush_when -> Lang.Expr.aggr_iter f flush_when) flush_when

let emit_aggr_init name in_tuple_typ mentioned and_all_others
                   commit_when flush_when oc selected_fields =
  (* We must collect all aggregation functions present in the selected_fields
   * and return a record with the proper types and init value for the aggr. *)
  Printf.fprintf oc "type %s = {\n" name ;
  for_each_aggr_fun selected_fields commit_when flush_when (fun aggr ->
      Printf.fprintf oc "\tmutable %s : %s ;\n"
        (name_of_aggr aggr)
        (otype_of_aggr aggr)
    ) ;
  Printf.fprintf oc "}\n\n" ;
  Printf.fprintf oc "let %s %a =\n\t{\n"
    name
    (emit_in_tuple mentioned and_all_others) in_tuple_typ ;
  for_each_aggr_fun selected_fields commit_when flush_when (fun aggr ->
      Printf.fprintf oc "\t%s = " (name_of_aggr aggr) ;
      (* For most aggr function we start with the first value *)
      (let open Lang.Expr in
      match aggr with
      | AggrMin (_, e) | AggrMax (_, e) | AggrAnd (_, e)
      | AggrOr (_, e) | AggrFirst (_, e) | AggrLast (_, e) ->
        let _impl, arg_typ = implementation_of aggr in
        conv_to arg_typ oc e
      | AggrSum (to_typ, _) ->
        Printf.fprintf oc "%s.zero" (omod_of_type (Option.get to_typ.scalar_typ))
      | AggrPercentile (_, p, e) ->
        let impl, arg_typ = implementation_of aggr in
        Printf.fprintf oc "(%s None %a %a) ;\n"
          impl
          (conv_to arg_typ) p
          (conv_to arg_typ) e ;
      | Const _ | Param _ | Field _ | Age _ | Not _ | Defined _ | Add _ | Sub _
      | Mul _ | Div _ | IDiv _ | Exp _ | And _ | Or _ | Ge _ | Gt _ | Eq _
      | Sequence _ | Mod _ | Cast _ ->
        assert false) ;
      Printf.fprintf oc " ; \n" ;
    ) ;
  Printf.fprintf oc "\t}\n"

let emit_update_aggr name in_tuple_typ mentioned and_all_others
                     commit_when flush_when oc selected_fields =
  Printf.fprintf oc "let %s aggr_ %a (* TODO: values for others and any *) =\n"
    name
    (emit_in_tuple mentioned and_all_others) in_tuple_typ ;
  for_each_aggr_fun selected_fields commit_when flush_when (fun aggr ->
      Printf.fprintf oc "\taggr_.%s <- " (name_of_aggr aggr) ;
      let open Lang.Expr in
      match aggr with
      | AggrMin (_, e) | AggrMax (_, e) | AggrSum (_, e) | AggrAnd (_, e)
      | AggrOr (_, e) | AggrFirst (_, e) | AggrLast (_, e)  ->
        let impl, arg_typ = implementation_of aggr in
        Printf.fprintf oc "%s aggr_.%s %a ;\n"
          impl (name_of_aggr aggr) (conv_to arg_typ) e ;
      | AggrPercentile (_, p, e) ->
        (* This value is optional but the percentile function takes an
         * optional value and return one so we do not have to deal with
         * it here: *)
        let impl, arg_typ = implementation_of aggr in
        Printf.fprintf oc "%s (Some aggr_.%s) %a %a ;\n"
          impl (name_of_aggr aggr)
          (conv_to arg_typ) p
          (conv_to arg_typ) e ;
      | Const _ | Param _ | Field _ | Age _ | Not _ | Defined _ | Add _ | Sub _
      | Mul _ | Div _ | IDiv _ | Exp _ | And _ | Or _ | Ge _ | Gt _ | Eq _
      | Sequence _ | Mod _ | Cast _ ->
        assert false
    ) ;
  Printf.fprintf oc "\t()\n"

(* Note: we need aggr_ in addition to out_tupple because the commit-when clause
 * might have its own aggregates going on *)
let emit_when name in_tuple_typ mentioned and_all_others out_tuple_typ
              oc commit_when =
  Printf.fprintf oc "\
    let %s aggr_ %a %a %a %a %a =\n\t%a\n"
    name
    (emit_in_tuple mentioned and_all_others) in_tuple_typ
    (emit_in_tuple ~tuple:"first" mentioned and_all_others) in_tuple_typ
    (emit_in_tuple ~tuple:"last" mentioned and_all_others) in_tuple_typ
    (emit_tuple "out") out_tuple_typ
    (emit_tuple "previous") out_tuple_typ
    emit_expr commit_when

let emit_aggregate oc in_tuple_typ out_tuple_typ
                   selected_fields and_all_others where key
                   commit_when flush_when =
(* We need:
 * - as above: a where filter, a serializer,
 * - a function computing the key as a tuple computed from input, exactly as in
 *   a select,
 * - a function of in and out and others and any, that returns true when we
 *   must emit the out tuple
 * - contrary to select, the selected fields are not used to build a function
 *   returning tuple out given tuple in. Here, the select fields are used to
 *   build 2 things:
 *   - a function that returns tuple out init (a mutable record!) from a tuple
 *     in
 *   - a function that update tuple out (record) given a tuple in
 *   We cannot generate less code than that if we want to use a record for
 *   tuple out.
 * With all this CodeGenLib will easily implement a basic version of aggregate
 * and could also implement more sophisticated versions. *)
  let mentioned =
    let all_exprs =
      where :: commit_when :: key @
      List.map (fun sf -> sf.Lang.Operation.expr) selected_fields in
    let all_exprs = match flush_when with
      | None -> all_exprs
      | Some flush_when -> flush_when :: all_exprs in
    add_all_mentioned all_exprs
  and where_need_aggr =
    let open Lang.Expr in
    (* Tells whether the where expression needs either the out tuple
     * or uses any aggregation on it's own. *)
    fold (fun need expr ->
      if need then need else match expr with
        | Field (_, tuple, _) ->
          not (Lang.same_tuple_as_in !tuple)
        | AggrMin _| AggrMax _| AggrSum _| AggrAnd _
        | AggrOr _| AggrFirst _| AggrLast _| AggrPercentile _ ->
          true
        | Age _| Sequence _| Not _| Defined _| Add _| Sub _| Mul _| Div _
        | IDiv _| Exp _| And _| Or _| Ge _| Gt _| Eq _| Const _| Param _
        | Mod _| Cast _ ->
          false) false where
  in
  Printf.fprintf oc "open Stdint\n\n\
    %a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n"
    (emit_aggr_init "aggr_init_" in_tuple_typ mentioned and_all_others commit_when flush_when) selected_fields
    (emit_read_tuple "read_tuple_" mentioned and_all_others) in_tuple_typ
    (if where_need_aggr then
      emit_expr_of_input_tuple "where_fast_" ~always_true:true in_tuple_typ mentioned and_all_others
    else
      emit_expr_of_input_tuple "where_fast_" in_tuple_typ mentioned and_all_others) where
    (if not where_need_aggr then
      emit_expr_of_input_tuple "where_slow_" ~with_aggr:true ~with_first_last:true ~always_true:true in_tuple_typ mentioned and_all_others
    else
      emit_expr_of_input_tuple "where_slow_" ~with_aggr:true ~with_first_last:true in_tuple_typ mentioned and_all_others) where
    (emit_expr_select ~honor_star:false "key_of_input_" in_tuple_typ mentioned and_all_others out_tuple_typ) key
    (emit_update_aggr "update_aggr_" in_tuple_typ mentioned and_all_others commit_when flush_when) selected_fields
    (emit_when "commit_when_" in_tuple_typ mentioned and_all_others out_tuple_typ) commit_when
    (emit_expr_select ~honor_star:true ~with_aggr:true ~with_first_last:true "tuple_of_aggr_" in_tuple_typ mentioned and_all_others out_tuple_typ)
      (exprs_of_selected_fields selected_fields)
    (emit_sersize_of_tuple "sersize_of_tuple_") out_tuple_typ
    (emit_serialize_tuple "serialize_aggr_") out_tuple_typ ;
  (match flush_when with
  | Some flush_when ->
    emit_when "flush_when_" in_tuple_typ mentioned and_all_others out_tuple_typ oc flush_when
  | None ->
    Printf.fprintf oc "let flush_when_ = commit_when_\n") ;
  Printf.fprintf oc "let () =\n\
      \tLwt_main.run (\n\
      \tCodeGenLib.aggregate read_tuple_ sersize_of_tuple_ serialize_aggr_ \
           tuple_of_aggr_ where_fast_ where_slow_ key_of_input_ commit_when_ flush_when_ aggr_init_ update_aggr_)\n"

let emit_field_of_tuple name mentioned and_all_others oc in_tuple_typ =
  Printf.fprintf oc "let %s %a = function\n"
    name
    (emit_in_tuple mentioned and_all_others) in_tuple_typ ;
  List.iter (fun field ->
      let open Lang in
      Printf.fprintf oc "\t| %S -> " field.Tuple.name ;
      let id = id_of_field_name field.Tuple.name in
      if field.Tuple.nullable then (
        Printf.fprintf oc "(match %s with None -> \"?null?\" | Some v_ -> %a)\n"
          id
          (conv_from_to field.Tuple.typ TString String.print) "v_"
      ) else (
        Printf.fprintf oc "%a\n"
          (conv_from_to field.Tuple.typ TString String.print) id
      )
    ) in_tuple_typ ;
  Printf.fprintf oc "\t| _ -> raise Not_found\n"

let emit_alert oc in_tuple_typ team subject text =
  (* We just want to read the in-tuple and have a function that return
   * field value (as a string!) given their name, so we can replace quoted
   * names by their actual value in the alert message *)
  let mentioned = Set.empty in
  Printf.fprintf oc "open Stdint\n\n\
    %a\n%a\n\
    let () =\n\
      \tLwt_main.run (\n\
      \tCodeGenLib.alert read_tuple_ field_of_tuple_ %S %S %S)\n"
    (emit_read_tuple "read_tuple_" mentioned true) in_tuple_typ
    (emit_field_of_tuple "field_of_tuple_" mentioned true) in_tuple_typ
    team subject text

let keep_temp_files = ref true

let with_code_file_for name f =
  let mode = [`create; `excl; `text] in
  let mode = if !keep_temp_files then mode else `delete_on_exit::mode in
  let prefix = "gen_"^ name ^"_" in
  File.with_temporary_out ~mode ~prefix ~suffix:".ml" (fun oc fname ->
    !logger.debug "Source code for %s: %s" name fname ;
    f oc fname)

let compile_source fname =
  (* This is not guaranteed to be unique but should be... *)
  let exec_name = String.sub fname 0 (String.length fname - 3) in
  let comp_cmd =
    Printf.sprintf "ocamlfind ocamlopt -o %s -package batteries,stdint,lwt.ppx,cohttp-lwt-unix \
                                       -linkpkg codegen.cmxa %s"
      exec_name fname in
  let exit_code = Sys.command comp_cmd in
  if exit_code = 0 then (
    !logger.debug "Compiled %s with: %s" fname comp_cmd ;
    exec_name
  ) else (
    !logger.error "Compilation of %s failed with status %d.\n\
                   Failed command was: %S"
                  fname exit_code comp_cmd ;
    failwith "Cannot generate code"
  )

let gen_operation name in_tuple_typ out_tuple_typ op =
  let open Lang.Operation in
  with_code_file_for name (fun oc fname ->
    (match op with
    | Yield fields ->
      emit_yield oc in_tuple_typ out_tuple_typ fields
    | Select { fields ; and_all_others ; where } ->
      emit_select oc in_tuple_typ out_tuple_typ fields and_all_others where
    | ReadCSVFile { fname ; separator ; null ; fields } ->
      emit_read_csv_file oc fname separator null fields
    | Aggregate { fields ; and_all_others ; where ; key ; commit_when ; flush_when } ->
      emit_aggregate oc in_tuple_typ out_tuple_typ fields and_all_others where
                     key commit_when flush_when
    | Alert { team ; subject ; text } ->
      emit_alert oc in_tuple_typ team subject text
    | _ ->
      Printf.fprintf oc "let () = print_string \"TODO\\n\"") ;
    fname) |>
    compile_source
