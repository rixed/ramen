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
open RamenConsts
open RamenLog
open RamenLang
open RamenHelpersNoLog
open RamenHelpers
open RamenTuple
open RamenTypes (* FIXME: RamenTypes.Pub ? *)
module C = RamenConf
module DT = DessserTypes
module VSI = RamenSync.Value.SourceInfo
module E = RamenExpr
module Helpers = CodeGen_Helpers
module T = RamenTypes
module O = RamenOperation
module N = RamenName
module Orc = RamenOrc
module EntryPoints = RamenConstsEntryPoints
module Globals = RamenGlobalVariables

(* If true, the generated code will log details about serialization *)
let verbose_serialization = false

(* We pass this around as "opc" *)
type op_context =
  { op : O.t option ;
    event_time : RamenEventTime.t option ;
    (* The type of the output tuple in ser order *)
    (* FIXME: make is a TRec to simplify code generation: *)
    typ : RamenTuple.typ ;
    params : RamenTuple.params ;
    code : string Batteries.IO.output ;
    consts : string Batteries.IO.output ;
    func_name : N.func option ;
    (* The constant expression id for which a constant hash of elements have
     * been output already; So that if the same constant expression is
     * encountered in several places in the code (as can easily happen with
     * parameters or input fields) we do not generate the constant hash
     * several times. *)
    mutable gen_consts : int Set.t ;
    dessser_mod_name : string }

let id_of_prefix tuple =
  String.nreplace (string_of_variable tuple) "." "_"

let id_of_field_name ?(tuple=In) x =
  (match (x : N.field :> string) with
  (* Note: we have a '#count' for the sort tuple. *)
  | "#count" -> "virtual_"^ id_of_prefix tuple ^"_count_"
  | field -> id_of_prefix tuple ^"_"^ field ^"_") |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let id_of_field_typ ?tuple field_typ =
  id_of_field_name ?tuple field_typ.RamenTuple.name

let id_of_global g =
  let open Globals in
  "global_" ^ string_of_scope g.scope ^"_"^ (g.name :> string) |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let var_name_of_record_field (k : N.field) =
  (k :> string) ^ "_" |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let dummy_var_name fn =
  "dummy_for_private" ^ var_name_of_record_field fn

let list_print_as_tuple ?as_ p oc lst =
  let last =
    match as_ with
    | None -> ")"
    | Some alias -> " as "^ alias ^")" in
  if lst = [] && as_ <> None then
    Printf.fprintf oc "(()%s" last
  else
    List.print ~first:"(" ~last ~sep:", " p oc lst

let list_print_as_tuple_i ?as_ p oc lst =
  let i = ref 0 in
  list_print_as_tuple ?as_ (fun oc x ->
    p oc !i x ; incr i) oc lst

let array_print_as_tuple_i p oc a =
  let i = ref 0 in
  Array.print ~first:"(" ~last:")" ~sep:", " (fun oc x ->
    p oc !i x ; incr i) oc a

let array_print_as_tuple p oc a =
  array_print_as_tuple_i (fun oc _ x -> p oc x) oc a

let list_print_as_vector p = List.print ~first:"[|" ~last:"|]" ~sep:"; " p
let list_print_as_product p = List.print ~first:"(" ~last:")" ~sep:" * " p

let tuple_id tuple =
  string_of_variable tuple ^"_" |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let fail_with_context c f =
  fail_with_context ("generating code for "^ c) f

let emit_tuple ?(with_alias=false) tuple =
  let print_field oc field_typ =
      String.print oc (id_of_field_typ ~tuple field_typ)
  and as_ = if with_alias then Some (tuple_id tuple) else None in
  list_print_as_tuple ?as_ print_field

(* Emit the code that return the sersize of a fixed size type *)
let emit_sersize_of_fixsz_typ oc typ =
  let sz = RingBufLib.sersize_of_fixsz_typ typ in
  Int.print oc sz

let emit_sersize_of_not_null_scalar indent tx_var offs_var oc typ =
  let p fmt = emit oc indent fmt in
  match typ with
  | DT.(Mac String) ->
      p "%d + RingBuf.round_up_to_rb_word (RingBuf.read_word %s %s)"
        DessserRamenRingBuffer.word_size tx_var offs_var
  | Usr { name ="Ip" ; _ } ->
      p "DessserRamenRingBuffer.word_size +" ;
      p "  RingBuf.round_up_to_rb_word(" ;
      p "    match RingBuf.read_word %s %s with" tx_var offs_var ;
      p "    | 0 -> %a" emit_sersize_of_fixsz_typ T.ipv4 ;
      p "    | 1 -> %a" emit_sersize_of_fixsz_typ T.ipv6 ;
      p "    | x -> invalid_byte_for \"IP\" x)"
  | Usr { name = "Cidr" ; _ } ->
      p "DessserRamenRingBuffer.word_size +" ;
      p "  RingBuf.round_up_to_rb_word(" ;
      p "    match RingBuf.read_u8 %s %s |> Uint8.to_int with"
        tx_var offs_var ;
      p "    | 4 -> %a" emit_sersize_of_fixsz_typ T.cidrv4 ;
      p "    | 6 -> %a" emit_sersize_of_fixsz_typ T.cidrv6 ;
      p "    | x -> invalid_byte_for \"CIDR\" x)"
  | Sum _ ->
      todo "Use Dessser lib to get sersize of sum types"
  | Tup _ | Rec _ | Vec _ | Lst _ ->
      assert false
  | t ->
      p "%a" emit_sersize_of_fixsz_typ t

let id_of_typ = function
  | DT.Unknown  -> assert false
  | Unit        -> "unit"
  | Mac Float  -> "float"
  | Mac String -> "string"
  | Mac Char   -> "char"
  | Mac Bool   -> "bool"
  | Mac U8     -> "u8"
  | Mac U16    -> "u16"
  | Mac U24    -> "u24"
  | Mac U32    -> "u32"
  | Mac U40    -> "u40"
  | Mac U48    -> "u48"
  | Mac U56    -> "u56"
  | Mac U64    -> "u64"
  | Mac U128   -> "u128"
  | Mac I8     -> "i8"
  | Mac I16    -> "i16"
  | Mac I24    -> "i24"
  | Mac I32    -> "i32"
  | Mac I40    -> "i40"
  | Mac I48    -> "i48"
  | Mac I56    -> "i56"
  | Mac I64    -> "i64"
  | Mac I128   -> "i128"
  | Usr { name = "Eth" ; _ } -> "eth"
  | Usr { name = "Ip4" ; _ } -> "ip4"
  | Usr { name = "Ip6" ; _ } -> "ip6"
  | Usr { name = "Ip" ; _ } -> "ip"
  | Usr { name = "Cidr4" ; _ } -> "cidr4"
  | Usr { name = "Cidr6" ; _ } -> "cidr6"
  | Usr { name = "Cidr" ; _ } -> "cidr"
  | Ext _ -> assert false
  | Tup _ -> "tuple"
  | Rec _ -> "record"
  | Vec _  -> "vector"
  | Lst _ -> "list"
  | Map _ -> assert false (* No values of that type *)
  | Usr ut -> todo ("Generalize user types to "^ ut.DT.name)
  | Sum _ -> todo "id_of_typ for sum types"
  | Set _ -> assert false (* No values of that type here *)

let rec emit_value_of_string
    indent t str_var offs_var emit_is_null fins may_quote oc =
  let p fmt = emit oc indent fmt in
  if t.DT.nullable then (
    p "let is_null_, o_ = %t in" (emit_is_null fins str_var offs_var) ;
    p "if is_null_ then Null, o_ else" ;
    p "let x_, o_ =" ;
    let t = DT.force_maybe_nullable t in
    emit_value_of_string (indent+1) t str_var "o_" emit_is_null fins may_quote oc ;
    p "  in" ;
    p "NotNull x_, o_"
  ) else (
    let emit_parse_list indent t oc =
      let p fmt = emit oc indent fmt in
      p "let rec read_next_ prevs_ o_ =" ;
      p "  let o_ = string_skip_blanks %s o_ in" str_var ;
      p "  if o_ >= String.length %s then" str_var ;
      p "    failwith \"List interrupted by end of string\" ;" ;
      p "  if %s.[o_] = ']' then prevs_, o_ + 1 else" str_var ;
      p "  let x_, o_ =" ;
      emit_value_of_string
        (indent + 2) t str_var "o_" emit_is_null (';' :: ']' :: fins) may_quote oc ;
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
    let emit_parse_record indent kts oc =
      (* Look for '(' *)
      p "let offs_ = string_skip_blanks_until '(' %s %s + 1 in"
        str_var offs_var ;
      p "if offs_ >= String.length %s then" str_var ;
      p "  failwith \"Tuple interrupted by end of string\" ;" ;
      let num_fields = Array.length kts in
      for i = 0 to num_fields - 1 do
        let fn, t = kts.(i) in
        let fn = N.field fn in
        if N.is_private fn then (
          p "let x%d_ = %s in" i (dummy_var_name fn)
        ) else (
          p "(* Read field %a *)" N.field_print fn ;
          p "let x%d_, offs_ =" i ;
          let fins = ';' :: fins in
          let fins = if i = num_fields - 1 then ')' :: fins else fins in
          emit_value_of_string
            (indent + 1) t str_var "offs_" emit_is_null fins may_quote oc ;
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
      Printf.fprintf oc "%s%a, offs_\n"
        (indent_of indent)
        (array_print_as_tuple_i (fun oc i _ ->
          Printf.fprintf oc "x%d_" i)) kts
    in
    match t.DT.vtyp with
    | Vec (d, t) ->
        p "let lst_, offs_ as res_ =" ;
        emit_parse_list (indent + 1) t oc ;
        p "in" ;
        p "if Array.length lst_ <> %d then" d ;
        p "  Printf.sprintf \"Was expecting %d values but got %%d\"" d ;
        p "    (Array.length lst_) |> failwith ;" ;
        p "res_"
    | Lst t ->
        emit_parse_list indent t oc
    | Tup ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_parse_record indent kts oc
    | Rec kts ->
        (* When reading values from a string (command line param values, CSV
         * files...) fields are expected to be given in definition order (as
         * opposed to serialization order).
         * Similarly, private fields are expected to be missing, and are thus
         * replaced by dummy values (so that we return the proper type). *)
        emit_parse_record indent kts oc
    | Mac String ->
        (* This one is a bit harder than the others due to optional quoting
         * (from the command line parameters, as CSV strings have been unquoted
         * already), and could benefit from [fins]: *)
        p "RamenTypeConverters.string_of_string ~fins:%a ~may_quote:%b %s %s"
          (List.print char_print_quoted) fins
          may_quote str_var offs_var
    | _ ->
        p "RamenTypeConverters.%s_of_string %s %s"
          (id_of_typ t.DT.vtyp) str_var offs_var
  )

let emit_float oc f =
  (* printf "%F" would not work for infinity:
   * https://caml.inria.fr/mantis/view.php?id=7685
   * and "%h" not for neg_infinity. *)
  if f = infinity then String.print oc "infinity"
  else if f = neg_infinity then String.print oc "neg_infinity"
  else Legacy.Printf.sprintf (if f >= 0. then "%h" else "(%h)") f |> String.print oc

(* Prints a function that convert an OCaml value into a RamenTypes.value of
 * the given RamenTypes.t. This is useful for instance to get hand off the
 * factors to CodeGenLib or for early filters. *)
let rec emit_value oc mn =
  let open Stdint in
  if mn.DT.nullable then
    String.print oc "(function Null -> RamenTypes.VNull | NotNull x_ -> "
  else
    String.print oc "(fun x_ -> " ;
  let p n = Printf.fprintf oc "RamenTypes.%s x_" n in
  (match mn.DT.vtyp with
  | DT.Unknown -> assert false
  | Unit -> String.print oc "RamenTypes.VUnit"
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
      emit_value oc (DT.make (DT.develop_value_type def))
  | Ext _ ->
      assert false
  | Tup ts ->
      Printf.fprintf oc "(let %a = x_ in RamenTypes.VTup %a)"
        (array_print_as_tuple_i (fun oc i _ ->
          Printf.fprintf oc "x%d_" i)) ts
        (array_print_i (fun i oc mn ->
          Printf.fprintf oc "(%a x%d_)" emit_value mn i)) ts
  | Rec kts ->
      Printf.fprintf oc "(let h_ = Hashtbl.create %d " (Array.length kts) ;
      Printf.fprintf oc "and %a = x_ in "
        (array_print_as_tuple_i (fun oc i _ ->
          Printf.fprintf oc "x%d_" i)) kts ;
      Array.iter (fun (k, t) ->
        Printf.fprintf oc "Hashtbl.add h_ %S %a ;" k emit_value t) kts ;
      Printf.fprintf oc "RamenTypes.VRec h_)"
  | Vec (_d, t) ->
      Printf.fprintf oc "RamenTypes.VVec (Array.map %a x_)" emit_value t
  | Lst t ->
      Printf.fprintf oc "RamenTypes.VLst (Array.map %a x_)" emit_value t
  | Map _ -> assert false (* No values of that type *)
  | Sum _ -> invalid_arg "emit_value for Sum type"
  | Set _ -> assert false (* No values of that type *)) ;
  String.print oc ")"

let rec emit_type oc =
  let open Stdint in
  function
  | VUnit     -> String.print oc "()"
  | VFloat  f -> emit_float oc f
  | VString s -> Printf.fprintf oc "%S" s
  | VBool   b -> Printf.fprintf oc "%b" b
  | VChar   c -> Printf.fprintf oc "%C" c
  | VU8     n -> Printf.fprintf oc "(Uint8.of_int (%d))" (Uint8.to_int n)
  | VU16    n -> Printf.fprintf oc "(Uint16.of_int (%d))" (Uint16.to_int n)
  | VU24    n -> Printf.fprintf oc "(Uint24.of_int (%d))" (Uint24.to_int n)
  | VU32    n -> Printf.fprintf oc "(Uint32.of_int64 (%sL))" (Uint32.to_string n)
  | VU40    n -> Printf.fprintf oc "(Uint40.of_int64 (%sL))" (Uint40.to_string n)
  | VU48    n -> Printf.fprintf oc "(Uint48.of_int64 (%sL))" (Uint48.to_string n)
  | VU56    n -> Printf.fprintf oc "(Uint56.of_int64 (%sL))" (Uint56.to_string n)
  | VU64    n -> Printf.fprintf oc "(Uint64.of_string %S)" (Uint64.to_string n)
  | VU128   n -> Printf.fprintf oc "(Uint128.of_string %S)" (Uint128.to_string n)
  | VI8     n -> Printf.fprintf oc "(Int8.of_int (%d))" (Int8.to_int n)
  | VI16    n -> Printf.fprintf oc "(Int16.of_int (%d))" (Int16.to_int n)
  | VI24    n -> Printf.fprintf oc "(Int24.of_int (%d))" (Int24.to_int n)
  | VI32    n -> Printf.fprintf oc "(%sl)" (Int32.to_string n)
  | VI40    n -> Printf.fprintf oc "(Int40.of_int64 (%sL))" (Int40.to_string n)
  | VI48    n -> Printf.fprintf oc "(Int48.of_int64 (%sL))" (Int48.to_string n)
  | VI56    n -> Printf.fprintf oc "(Int56.of_int64 (%sL))" (Int56.to_string n)
  | VI64    n -> Printf.fprintf oc "(%sL)" (Int64.to_string n)
  | VI128   n -> Printf.fprintf oc "(Int128.of_string %S)" (Int128.to_string n)
  | VEth    n -> Printf.fprintf oc "(Uint48.of_int64 (%LdL))" (Uint48.to_int64 n)
  | VIpv4   n -> Printf.fprintf oc "(Uint32.of_string %S)" (Uint32.to_string n)
  | VIpv6   n -> Printf.fprintf oc "(Uint128.of_string %S)" (Uint128.to_string n)
  | VIp (RamenIp.V4 n) -> Printf.fprintf oc "(RamenIp.V4 %a)" emit_type (VIpv4 n)
  | VIp (RamenIp.V6 n) -> Printf.fprintf oc "(RamenIp.V6 %a)" emit_type (VIpv6 n)
  | VCidrv4 (n,l) ->
                 Printf.fprintf oc "(Uint32.of_string %S, Uint8.of_int %d)"
                   (Uint32.to_string n) (Uint8.to_int l)
  | VCidrv6 (n,l) ->
                 Printf.fprintf oc "(Uint128.of_string %S, Uint8.of_int %d)"
                   (Uint128.to_string n) (Uint8.to_int l)
  | VCidr (RamenIp.Cidr.V4 n) ->
                 Printf.fprintf oc "(RamenIp.Cidr.(V4 %a))" emit_type (VCidrv4 n)
  | VCidr (RamenIp.Cidr.V6 n) ->
                 Printf.fprintf oc "(RamenIp.Cidr.(V6 %a))" emit_type (VCidrv6 n)
  | VTup vs ->
      Array.print ~first:"(" ~last:")" ~sep:", " emit_type oc vs
  | VRec kvs ->
      (* A record internal value is a tuple with fields in serialization order: *)
      let kvs = Array.copy kvs in
      Array.fast_sort O.FieldOrder.rec_field_cmp kvs ;
      Printf.fprintf oc "(* Record type reordered to %a *)"
        (Array.print (fun oc (fn, _) -> String.print oc fn)) kvs ;
      let vs = Array.map snd kvs in
      emit_type oc (VTup vs)
  | VVec vs   -> Array.print emit_type oc vs
  (* For now ramen lists are ocaml arrays. Should they be ocaml lists? *)
  | VLst vs  -> Array.print emit_type oc vs
  (* Internal OCaml representation of maps are hash tables: *)
  | VMap kvs  ->
      Array.print ~first:"(let h_ = Hashtbl.create 10 in "
                  ~sep:" ; " ~last:" ; h_)"
        (fun oc (k, v) ->
          Printf.fprintf oc "Hashtbl.add h_ %a %a"
            emit_type k
            emit_type v
        ) oc kvs
  | VNull     -> Printf.fprintf oc "Null"

(* Context: helps picking the implementation of an operation. Subexpressions
 * will always have context "Finalize", though. *)
type context = InitState | UpdateState | Finalize | Generator

let string_of_context = function
  | InitState -> "InitState"
  | UpdateState -> "UpdateState"
  | Finalize -> "Finalize"
  | Generator -> "Generator"

let rec otype_of_value_type oc = function
  | DT.Unknown | Ext _ -> assert false
  | Unit -> String.print oc "unit"
  | Mac Float -> String.print oc "float"
  | Mac String -> String.print oc "string"
  | Mac Char -> String.print oc "char"
  | Mac Bool -> String.print oc "bool"
  | Mac U8 -> String.print oc "uint8"
  | Mac U16 -> String.print oc "uint16"
  | Mac U24 -> String.print oc "uint24"
  | Mac U32 -> String.print oc "uint32"
  | Mac U40 -> String.print oc "uint40"
  | Mac U48 -> String.print oc "uint48"
  | Mac U56 -> String.print oc "uint56"
  | Mac U64 -> String.print oc "uint64"
  | Mac U128 -> String.print oc "uint128"
  | Mac I8 -> String.print oc "int8"
  | Mac I16 -> String.print oc "int16"
  | Mac I24 -> String.print oc "int24"
  | Mac I32 -> String.print oc "int32"
  | Mac I40 -> String.print oc "int40"
  | Mac I48 -> String.print oc "int48"
  | Mac I56 -> String.print oc "int56"
  | Mac I64 -> String.print oc "int64"
  | Mac I128 -> String.print oc "int128"
  | Usr { name = "Eth" ; _ } -> String.print oc "uint48"
  | Usr { name = "Ip4" ; _ } -> String.print oc "uint32"
  | Usr { name = "Ip6" ; _ } -> String.print oc "uint128"
  | Usr { name = "Ip" ; _ } -> String.print oc "RamenIp.t"
  | Usr { name = "Cidr4" ; _ } -> String.print oc "(uint32 * uint8)"
  | Usr { name = "Cidr6" ; _ } -> String.print oc "(uint128 * uint8)"
  | Usr { name = "Cidr" ; _ } -> String.print oc "RamenIp.Cidr.t"
  | Usr { def ; _ } ->
      otype_of_value_type oc def
  | Tup ts ->
      Array.print ~first:"(" ~last:")" ~sep:" * "
        (fun oc t -> otype_of_type oc t)
        oc ts
  | Rec kts ->
      (* A record internal representation is a tuple with field in the
       * definition order: *)
      let ts = Array.map snd kts in
      otype_of_value_type oc (Tup ts)
  | Vec (_, t) | Lst t ->
      Printf.fprintf oc "%a array" otype_of_type t
  | Map _ -> assert false (* No values of that type *)
  | Sum _ -> assert false (* DEBUG todo "otype_of_value_type for sum type"*)
  | Set _ -> assert false (* No values of that type *)

and otype_of_type oc t =
  Printf.fprintf oc "%a%s"
    otype_of_value_type t.DT.vtyp
    (if t.DT.nullable then " nullable" else "")

let rec omod_of_type = function
  | DT.Unknown | Unit | Ext _ -> assert false
  | Mac Float -> "Float"
  | Mac String -> "String"
  | Mac Bool -> "Bool"
  | Mac Char -> "Char"
  | Mac (U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 |
         I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128) as t ->
      String.capitalize (IO.to_string otype_of_value_type t)
  | Usr { name = "Eth" ; _ } -> "RamenEthAddr"
  | Usr { name = "Ip4" ; _ } -> "RamenIpv4"
  | Usr { name = "Ip6" ; _ } -> "RamenIpv6"
  | Usr { name = "Ip" ; _ } -> "RamenIp"
  | Usr { name = "Cidr4" ; _ } -> "RamenIpv4.Cidr"
  | Usr { name = "Cidr6" ; _ } -> "RamenIpv6.Cidr"
  | Usr { name = "Cidr" ; _ } -> "RamenIp.Cidr"
  | Usr { def ; _ } -> omod_of_type def
  | Tup _ | Rec _ | Vec _ | Lst _ | Set _ | Map _ | Sum _ ->
      assert false

(* Simpler, temp version of the above: *)
let filter_out_private_from_tup tup =
  List.filter (fun ft ->
    not (N.is_private ft.RamenTuple.name)
  ) tup

(* Why don't we have explicit casts in the AST so that we could stop
 * caring about those pesky conversions once and for all? Because the
 * AST changes to types that we want to work, but do not (have to) know
 * about what conversions are required to implement that in OCaml. *)

(* Note: for field_of_tuple, we must be able to convert any value into a
 * string. *)
(* This only returns the function name (or code) but does not emit the
 * call to that function. *)
let rec conv_from_to
    ?(string_not_null=false) ~nullable from_typ to_typ oc =
  (* Emit a prefix function taking 2 arguments [f] and [x] and cater for the
   * nullability of x and that of the desired output: *)
  let conv_nullable from_nullable to_nullable oc =
    match from_nullable, to_nullable with
    | true, true ->
        Printf.fprintf oc "(fun f x -> try f x with _ -> Null)"
    | false, false ->
        Printf.fprintf oc "(fun f x -> f x)"
    | true, false ->
        (* Type checking must ensure that we do not cast away nullability
         * without the possibility to set the whole tuple to NULL: *)
        Printf.fprintf oc
          "(fun f -> function Null -> raise ImNull | NotNull x_ -> f x_)"
    | false, true ->
        Printf.fprintf oc "(fun f x -> try NotNull (f x) with _ -> Null)" in
  (* Emit a function to convert from/to the given type structures.
   * Emitted code must be prefixable by "Nullable.map": *)
  let rec print_non_null oc (from_typ, to_typ as conv) =
    if from_typ = to_typ then Printf.fprintf oc "identity" else
    match conv with
    | DT.Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
              I8|I16|I24|I32|I40|I48|I56|I64|I128|
              String|Float),
      Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
           I8|I16|I24|I32|I40|I48|I56|I64|I128)
    | Mac String, Mac (Float|Bool) ->
        Printf.fprintf oc "%s.of_%a"
          (omod_of_type to_typ)
          otype_of_value_type from_typ
    | Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
           I8|I16|I24|I32|I40|I48|I56|I64|I128),
      Mac (Float|String)
    | Mac (Float|Bool),
      Mac (String|Float) ->
        Printf.fprintf oc "%s.to_%a"
          (omod_of_type from_typ)
          otype_of_value_type to_typ
    | Mac Bool,
      Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
           I8|I16|I24|I32|I40|I48|I56|I64|I128) ->
        Printf.fprintf oc "(%s.of_int %% Bool.to_int)"
          (omod_of_type to_typ)
    | Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
           I8|I16|I24|I32|I40|I48|I56|I64|I128),
      Mac Bool ->
        Printf.fprintf oc "(fun x_ -> %s.(compare zero x_) <> 0)"
          (omod_of_type from_typ)
    | Usr { name = ("Eth"|"Ip4"|"Ip6"|"Ip"|"Cidr4"|"Cidr6"|"Cidr") ; _ },
      Mac String ->
        Printf.fprintf oc "%s.to_string" (omod_of_type from_typ)
    | Mac Char,
      Mac String ->
        Printf.fprintf oc "(String.make 1)"
    | Mac String,
      _ ->
        Printf.fprintf oc
          "(fun s_ ->\n\t\t\
            let x_, o_ = RamenTypeConverters.%s_of_string s_ 0 in\n\t\t\
            if o_ < String.length s_ then raise ImNull else x_)\n\t"
          (id_of_typ to_typ)
    | (Usr { name = "Ip4" ; _ } | Mac U32),
      Usr { name = "Ip" ; _ } ->
        Printf.fprintf oc "(fun x_ -> RamenIp.V4 x_)"
    | (Usr { name = "Ip6" ; _ } | Mac U128),
      Usr { name = "Ip" ; _ } ->
        Printf.fprintf oc "(fun x_ -> RamenIp.V6 x_)"
    | Usr { name = "Ip4" ; _ },
      Usr { name = "Cidr4" ; _ } ->
        Printf.fprintf oc "(fun x_ -> x_, 32)"
    | Usr { name = "Ip6" ; _ },
      Usr { name = "Cidr6" ; _ } ->
        Printf.fprintf oc "(fun x_ -> x_, 128)"
    | Usr { name = "Ip" ; _ },
      Usr { name = "Cidr" ; _ } ->
        Printf.fprintf oc "(function RamenIp.V4 x_ -> RamenIp.Cidr.V4 (x_, 32) \
                                   | RamenIp.V6 x_ -> RamenIp.Cidr.V6 (x_, 128))"
    | Usr { name = "Cidr4" ; _ },
      Usr { name = "Cidr" ; _ } ->
        Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V4 x_)"
    | Usr { name = "Cidr6" ; _ },
      Usr { name = "Cidr" ; _ } ->
        Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V6 x_)"
    | Usr { name = "Ip4" ; _ },
      Mac U32
    | Mac U32,
      Usr { name = "Ip4" ; _ } ->
        Printf.fprintf oc "identity"
    | Usr { name = "Ip6" ; _ },
      Mac U128
    | Mac U128,
      Usr { name = "Ip6" ; _ } ->
        Printf.fprintf oc "identity"
    | Mac U64,
      Usr { name = "Eth" ; _ } ->
        Printf.fprintf oc "Uint48.of_uint64"
    | Lst t_from, Lst t_to
         when t_from.DT.nullable = t_to.DT.nullable ->
        Printf.fprintf oc "(Array.map (%t))"
          (conv_from_to ~string_not_null ~nullable:t_from.DT.nullable
                        t_from.vtyp t_to.vtyp)
    | Lst t_from,
      Lst t_to
         when nullable && t_from.DT.nullable && not t_to.DT.nullable ->
        Printf.fprintf oc
          "(Array.map (function \
              | Null -> raise ImNull \
              | NotNull x_ -> %t x_))"
          (conv_from_to ~string_not_null ~nullable:t_from.DT.nullable
                        t_from.vtyp t_to.vtyp)
    | Lst t_from,
      Lst t_to
         when not t_from.DT.nullable && t_to.DT.nullable ->
        Printf.fprintf oc
          "(Array.map (fun x_ -> NotNull (%t x_)))"
          (conv_from_to ~string_not_null ~nullable:false
                        t_from.vtyp t_to.vtyp)
    | Vec (_, t_from),
      Lst t_to ->
        print_non_null oc (Lst t_from, Lst t_to)
    | Vec (d_from, t_from),
      Vec (d_to, t_to)
        when (d_from = d_to || d_to = 0) ->
        (* d_to = 0 means no constraint (copy the one from the left-hand side) *)
        print_non_null oc (Lst t_from, Lst t_to)
    | Tup t_from,
      Tup t_to
      when Array.length t_from = Array.length t_to ->
        (* TODO: actually we could project away fields from t_from when t_to
         * is narrower, or inject NULLs in some cases. *)
        Printf.fprintf oc "(fun (%a) -> ("
          (array_print_as_tuple_i (fun oc i _ ->
            Printf.fprintf oc "x%d_" i)) t_from ;
        for i = 0 to Array.length t_from - 1 do
          if i > 0 then Printf.fprintf oc ",\n\t" ;
          Printf.fprintf oc "%t %a x%d_"
            (conv_nullable t_from.(i).DT.nullable t_to.(i).DT.nullable)
            print_non_null (t_from.(i).vtyp, t_to.(i).vtyp)
            i
        done ;
        Printf.fprintf oc "))"
    | Tup t_from,
      Vec (d, t_to) when d = Array.length t_from ->
        print_non_null oc (from_typ, Lst t_to)
    | Tup t_from,
      Lst t_to ->
        Printf.fprintf oc "(fun (%a) -> [|"
          (array_print_as_tuple_i (fun oc i _ ->
            Printf.fprintf oc "x%d_" i)) t_from ;
        for i = 0 to Array.length t_from - 1 do
          if i > 0 then Printf.fprintf oc " ;\n\t" ;
          Printf.fprintf oc "%t %a x%d_"
            (conv_nullable t_from.(i).nullable t_to.DT.nullable)
            print_non_null (t_from.(i).vtyp, t_to.vtyp)
            i
        done ;
        Printf.fprintf oc "|])"
    (* In general, a vector or list is converted to a string by pretty
     * printing the type. But for chars the intend is to convert into
     * a string: *)
    | (Vec (_, t) | Lst t),
      Mac String
      when t.DT.vtyp = Mac Char ->
        (* The case when the vector itself is null is already dealt with
         * so here the vector is not null, but still it's elements can be.
         * In that case, the string result is not nullable (nullability
         * propagates from the vector to the string result, not from the
         * vector items to the string result).
         * Indeed, if we converted to string a vector of nul items, we
         * would like to see "[NULL; NULL; ...]". Here it's the same, just
         * with mere characters.
         * So string_of_nullable_chars will just replace nulls with '?'. *)
        if t.DT.nullable then
          Printf.fprintf oc "CodeGenLib.string_of_nullable_chars"
        else
          Printf.fprintf oc "CodeGenLib.string_of_chars"
    | (Vec (_, t) | Lst t),
      Mac String ->
        Printf.fprintf oc
          "(fun v_ -> \
            \"[\"^ (\
              Array.enum v_ /@ (%t) |> \
                Enum.fold (fun res_ s_ -> \
                  (if String.length res_ = 0 then \"\" else res_^\";\") ^ \
                  (%s s_) \
                ) \"\") \
              ^\"]\")"
          (conv_from_to ~string_not_null ~nullable:t.nullable
                        t.vtyp (Mac String))
          (if not string_not_null && t.nullable then
             Printf.sprintf "Nullable.default %S" string_of_null else "")
    | Tup ts,
      Mac String ->
        let i = ref 0 in
        Printf.fprintf oc
          "(fun %a -> \"(\"^ %a ^\")\")"
            (array_print_as_tuple_i (fun oc i _ ->
              Printf.fprintf oc "x%d_" i)) ts
            (Array.print ~first:"" ~last:"" ~sep:" ^\";\"^ " (fun oc t ->
              Printf.fprintf oc "(%t) x%d_"
                (conv_from_to ~string_not_null ~nullable:t.DT.nullable
                              t.vtyp (Mac String)) !i ;
              incr i)) ts
    | Rec kts,
      Mac String ->
        (* TODO: also print the field names?
         * For now the fields are printed in the definition order: *)
        (* Note: when printing records, private fields disappear *)
        let kts' =
          Array.filter (fun (k, _) ->
            not (N.(is_private (field k)))) kts in
        let arg_var k =
          RamenOCamlCompiler.make_valid_ocaml_identifier ("rec_"^ k) in
        Printf.fprintf oc
          "(fun %a -> \"(\"^ %a ^\")\")"
            (array_print_as_tuple (fun oc (k, _) ->
              String.print oc (arg_var k))) kts
            (Array.print ~first:"" ~last:"" ~sep:" ^\";\"^ " (fun oc (k, t) ->
              Printf.fprintf oc "(%t) %s"
                (conv_from_to ~string_not_null ~nullable:t.DT.nullable
                              t.vtyp (Mac String))
                (arg_var k))) kts'
    (* Any type can also be converted into a singleton vector of a compatible
     * type: *)
    | from_structure,
      Vec (1, to_typ) ->
        (* Let the convertion from [from_typ] to [to_typ] fail as there are
         * no more possible alternative anyway: *)
        Printf.fprintf oc "(fun x_ -> [| %t %a x_ |])"
          (conv_nullable nullable to_typ.nullable)
          print_non_null (from_structure, to_typ.vtyp)
    | _ ->
        Printf.sprintf2 "Cannot find converter from type %a to type %a"
          DT.print_value_type from_typ
          DT.print_value_type to_typ |>
        failwith
  in
  (* In general, when we convert a nullable thing into another type, then
   * if the result is also nullable. But sometime we want to have a non
   * nullable string representation of any values, where we want null values
   * to appear as "null": *)
  match nullable, to_typ, string_not_null with
  | false, _, _ ->
      print_non_null oc (from_typ, to_typ)
  | true, Mac String, true ->
      Printf.fprintf oc "(Nullable.default %S %% Nullable.map_no_fail %a)"
        string_of_null print_non_null (from_typ, to_typ)
  | true, _, _ ->
      (* Here any conversion that fails for any reason can be mapped to NULL *)
      Printf.fprintf oc "Nullable.map_no_fail %a"
        print_non_null (from_typ, to_typ)

let wrap_nullable ~nullable oc f =
  (* TODO: maybe catch ImNull? *)
  if nullable then Printf.fprintf oc "NotNull (%t)" f
  else f oc

(* Used by Generator functions: *)
let freevar_name e =
  "fv_"^ string_of_int e.E.uniq_num ^"_" |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let any_constant_of_expr_type ?(avoid_null=false) typ =
  E.make ~vtyp:typ.DT.vtyp ~nullable:typ.DT.nullable
         (Const (any_value_of_maybe_nullable ~avoid_null typ))

(* In some case we want emit_function to pass arguments as an array
 * (variadic functions...) or as a tuple (functions taking a tuple).
 * In both cases the int refers to how many normal args should we pass
 * before starting the array/tuple.*)
type args_as = Arg | Array of int | Tuple of int

exception Error of E.t * context * string
let () =
  Printexc.register_printer (function
    | Error (expr, context, msg) ->
        Some (Printf.sprintf2 "While generating code for %s %a: %s"
          (match context with
          | InitState -> "initialization of"
          | UpdateState -> "updating state of"
          | Finalize -> "finalization of"
          | Generator -> "value generation for")
          (E.print true) expr
          msg)
    | _ -> None)

(* Each argument have a specific behavior towards nullability: *)
type arg_nullability_propagation =
  | PropagateNull
      (* If the arg is null then the result is; function is not actually
       * called. *)
  | PassNull
      (* Pass the argument as is to the function. *)
  | PassAsNull
      (* Same as PassNull, but if the arg is not nullable change it into
       * a nullable. *)

(*
 * Environments
 *
 * Although most of the times an expression only refers to sub-expressions,
 * sometime an expression has also to refer to something extra: the field of an
 * IO tuple (a parameter, an input value...) or its internal state, or to some
 * value that has been precomputed higher up in the AST...
 *
 * Under what name to find those things is not constant, as for instance a
 * function state might be accessible from the global_ or group_ variables from
 * most places but is opened in the environment while these records are being
 * build.
 *
 * Similarly, the field of an immediate record might be in scope before the
 * record itself is accessible while it's being initialized. The same goes for
 * the out tuple during the evaluation of the select clause.
 *
 * To find how under what name to access such values we maintain an environment
 * stack of bound variables.  Every binding that is "opened", or readily
 * available as an OCaml variable, is stored in this environment.  So that for
 * instance when a stateful function is looking for its state this environment
 * is looked up for the actual OCaml variable to use.
 *)

type binding =
  (* string is the name of the variable (in scope) holding that state ir
   * field: *)
  E.binding_key * string

type env = binding list

let print_env oc =
  pretty_list_print (fun oc (k, v) ->
    Printf.fprintf oc "%a=>%s"
      E.print_binding_key k
      v
  ) oc

let emit_binding env oc k =
  let s =
    match k with
    | E.Direct s -> s
    | k ->
        (match List.assoc k env with
        | exception Not_found ->
            Printf.sprintf2
              "Cannot find a binding for %a in the environment (%a)"
              E.print_binding_key k
              print_env env |>
            failwith
        | s -> s)
  in
  String.print oc s

let name_of_state e =
  "state_"^ string_of_int e.E.uniq_num |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let id_of_state = function
  | E.GlobalState -> "global_"
  | E.LocalState -> "group_"

let string_of_endianness = function
  | E.LittleEndian -> "little"
  | E.BigEndian -> "big"

let add_tuple_environment tuple typ env =
  (* Start by adding the name of the IO record itself, that is
   * always present whenever emit_tuple is used (assuming emit_tuple
   * is always called when this one is): *)
  let env = (E.RecordValue tuple, tuple_id tuple) :: env in
  (* Then each field separately: *)
  List.fold_left (fun env ft ->
    let v =
      match tuple with
      | Env ->
          Printf.sprintf2 "(Sys.getenv_opt %S |> Nullable.of_option)"
            (ft.RamenTuple.name :> string)
      | OutPrevious ->
          Printf.sprintf2 "(maybe_%s_ out_previous_)"
            (ft.name :> string)
      | _ ->
          id_of_field_typ ~tuple ft in
    (E.RecordField (tuple, ft.name), v) :: env
  ) env typ

(* Given a function name and an output type, return the actual function
 * returning that type, and the types each input parameters must be converted
 * into, if any. None means we need no conversion whatsoever (useful for
 * function internal state or 'a values) while AnyType means there must be a
 * type but it has to be found out according to the context.
 *
 * Returns a list of typ option, as long as the type of input arguments *)
(* FIXME: this could be extracted from Compiler.check_expr *)
type arg_conversion =
  (* Useful for function internal state or 'a value: *)
  | NoConv
  (* Determine the smallest subset of all args. If one day proper parametric
   * types are needed then it will be necessary to distinguish between several
   * AnyTypes: *)
  | AnyType
  | ConvTo of DT.value_type

(* Return the list of all unique fields in the record expression, in
 * serialization order: *)
let fields_of_record kvs =
  (List.fast_sort O.FieldOrder.rec_field_cmp kvs |>
  List.enum) /@
  (fun (fn, _) -> N.field fn) |>
  remove_dups N.compare

let rec conv_to ~env ~context ~opc to_typ oc e =
  match e.E.typ.vtyp, to_typ with
  | a, Some b ->
    Printf.fprintf oc "(%t) (%a)"
      (conv_from_to ~nullable:e.typ.nullable a b)
      (emit_expr ~context ~opc ~env) e
  | _, None -> (* No conversion required *)
    (emit_expr ~context ~opc ~env) oc e

(* Apply the given function to the given args (and varargs), after
 * converting them, obeying skip_nulls. *)
and update_state ~env ~opc ~nullable skip my_state
                 es ?(vars=[]) ?vars_to_typ
                 func_name ?args_as oc to_typ =
  let emit_func ~env args oc varargs =
    match vars_to_typ with
    | None ->
      emit_functionN ~env ~opc ~nullable ?args_as
                     func_name ((NoConv, PropagateNull) :: to_typ) oc
                     (my_state :: args)
    | Some vars_to_typ ->
      emit_functionNv ~env ~opc ~nullable func_name
                      ((NoConv, PropagateNull) :: to_typ)
                      (my_state :: args)
                      vars_to_typ oc varargs
  in
  if skip then (
    (* Skip just means that if an entry is null we want to skip the
     * update. But maybe no entries are actually nullable. And the
     * state could be nullable or not. If skip, we will have an
     * additional bool named empty, initialized to true, that will
     * possibly stay true only if we skip all entries because an
     * arg was NULL every time.
     * When this is so, most functions will return NULL (in finalize_state).
     * An exception to this rule is the COUNT function that will return 0
     * (unless its counting nullable predicates, in which case it will return
     * NULL!). *)
    Printf.fprintf oc "\t" ;
    (* Force the args to func_name to be non-nullable inside the
     * assignment, since we have already verified they are not null: *)
    (* Returns both the new expression and the new environment: *)
    let denullify e args =
      if e.E.typ.DT.nullable then (
        let var_name =
          Printf.sprintf "nonnull_%d_" e.E.uniq_num in
        Printf.fprintf oc "(match %a with Null -> () | NotNull %s -> "
          (emit_expr ~context:Finalize ~opc ~env) e
          var_name ;
        (E.make ~vtyp:e.typ.vtyp ~nullable:false
                (Binding (Direct var_name))) :: args
      ) else (e :: args) in
    let func_args = List.fold_right denullify es [] in
    let func_varargs = List.fold_right denullify vars [] in
    (* When skip_nulls the state is accompanied
     * by a boolean that's true iff some values have been seen (used when
     * finalizing).
     * Some aggr function will never return NULL but from propagation or
     * skip, but some will. Those who will are always nullable, and will
     * return the Null/NotNull status themselves. Therefore, the code
     * generator has to know about them when finalizing, otherwise it will
     * assume the aggr function never returns Null. *)
    Printf.fprintf oc "%a <- %a ;\n"
      (emit_expr ~env ~context:Finalize ~opc) my_state
      (emit_func ~env func_args) func_varargs ;
    Printf.fprintf oc "\t\t%a_empty_ <- false\n"
      (emit_expr ~env ~context:Finalize ~opc) my_state ;
    let close_denullify e =
      if e.E.typ.nullable then Printf.fprintf oc ")" in
    List.iter close_denullify es ;
    List.iter close_denullify vars ;
    Printf.fprintf oc " ;\n"
  ) else (
    Printf.fprintf oc "\t%a <- %a ;\n"
      (emit_expr ~env ~context:Finalize ~opc) my_state
      (emit_func ~env es) vars
  )

(* Similarly, return the finalized value of the given state.
 * fin_args are the arguments passed to the finalizers and are not subject
 * to be skipped. If nullable then the Null will merely propagate to the
 * return value. *)
and finalize_state ~env ~opc ~nullable skip my_state func_name fin_args
                   ?impl_return_nullable ?args_as oc to_typ =
  if nullable && skip then
    (* In the case where we stayed empty, the typer must have made this
     * state nullable so we can return directly its value: *)
    Printf.fprintf oc
      "(if %a_empty_ then Null else %a)"
      (emit_expr ~env ~context:Finalize ~opc) my_state
      (emit_functionN ~env ~opc ~nullable ?impl_return_nullable ?args_as
                      func_name ((NoConv, PropagateNull)::to_typ))
        (my_state::fin_args)
  else
    emit_functionN ~env ~opc ~nullable ?impl_return_nullable ?args_as
                   func_name ((NoConv, PropagateNull)::to_typ) oc
                   (my_state::fin_args)

(* The vectors OutPrevious is nullable: the commit when and
 * select clauses of aggregate operations either have it or not.
 * Each time they need access to a field they call a function "maybe_XXX_"
 * with that nullable tuple, which avoids propagating out_typ down to
 * emit_expr - but hopefully the compiler will inline this. *)
and emit_maybe_fields oc out_typ =
  List.iter (fun ft ->
    Printf.fprintf oc "let %s = function\n"
      ("maybe_"^ (ft.name :> string) ^"_" |>
       RamenOCamlCompiler.make_valid_ocaml_identifier) ;
    Printf.fprintf oc "  | Null -> Null\n" ;
    Printf.fprintf oc "  | NotNull %a -> %s%s\n\n"
      (emit_tuple Out) out_typ
      (if ft.typ.nullable then "" else "NotNull ")
      (id_of_field_name ~tuple:Out ft.name)
  ) out_typ

and emit_event_time oc opc =
  let (sta_field, sta_src, sta_scale), dur = Option.get opc.event_time in
  let open RamenEventTime in
  let field_value_to_float src oc field_name =
    match src with
    | OutputField ->
        (* This must not fail if RamenOperation.check did its job *)
        let f = List.find (fun t -> t.name = field_name) opc.typ in
        Printf.fprintf oc
          (if f.typ.DT.nullable then "((%t) %s |! 0.)" else "(%t) %s")
          (conv_from_to ~nullable:f.typ.DT.nullable f.typ.vtyp (Mac Float))
          (id_of_field_name ~tuple:Out field_name)
    | Parameter ->
        let param = RamenTuple.params_find field_name opc.params in
        Printf.fprintf oc "(%t %s_%s_)"
          (conv_from_to ~nullable:false param.ptyp.typ.vtyp (Mac Float))
          (id_of_prefix Param)
          (field_name :> string)
  in
  Printf.fprintf oc "let start_ = %a *. %a "
    (field_value_to_float !sta_src) sta_field
    emit_float sta_scale ;
  (match dur with
  | DurationConst d ->
      Printf.fprintf oc
        "and dur_ = %a in start_, start_ +. dur_"
        emit_float d
  | DurationField (dur_field, dur_src, dur_scale) ->
      Printf.fprintf oc
        "and dur_ = %a *. %a in start_, start_ +. dur_"
        (field_value_to_float !dur_src) dur_field
        emit_float dur_scale ;
  | StopField (sto_field, sto_src, sto_scale) ->
      Printf.fprintf oc
        "and stop_ = %a *. %a in start_, stop_"
        (field_value_to_float !sto_src) sto_field
        emit_float sto_scale)

and emit_expr_ ~env ~context ~opc oc expr =
  let nullable = expr.E.typ.DT.nullable in
  (* my_state will represent the variable holding the state of a stateful
   * function. *)
  let my_state =
    (* A state is always as nullable as its expression (see
     * [otype_of_state]): *)
    E.make ~nullable (Binding (State expr.E.uniq_num)) in
  match context, expr.text, expr.typ.vtyp with
  (* Non-functions *)
  | Finalize, E.Binding k, _ ->
      (* Look for that name in the environment: *)
      emit_binding env oc k
  | Finalize, E.Variable prefix, _ ->
      (* Look for the RecordValue in the environment: *)
      emit_binding env oc (E.RecordValue prefix)
  | _, Const VNull, _ ->
      assert nullable ;
      Printf.fprintf oc "Null"
  | _, Const c, _ ->
      Printf.fprintf oc "%s(%t %a)"
        (if nullable then "NotNull " else "")
        (conv_from_to ~nullable:false (type_of_value c) expr.typ.vtyp)
        emit_type c
  | Finalize, Tuple es, _ ->
      list_print_as_tuple (emit_expr ~env ~context ~opc) oc es
  | Finalize, Record kvs, _ ->
      (* Here we must compute the values in user definedd order, as each
       * expression can refer to the previous one. And we must, for each
       * expression, evaluate it in a context where this record is opened. *)
      let _env =
        List.fold_left (fun env ((k : N.field), v) ->
          let var_name = var_name_of_record_field k in
          Printf.fprintf oc "\tlet %s = %a in\n"
            var_name
            (emit_expr ~env ~context ~opc) v ;
          (E.RecordField (Record, k), var_name) :: env
        ) env kvs in
      (* Finally, regroup those fields in a tuple, in serialization order: *)
      let es =
        fields_of_record (kvs :> (string *  E.t) list) /@
        var_name_of_record_field |>
        List.of_enum in
      list_print_as_tuple String.print oc es
  | Finalize, Vector es, Vec (_, t) ->
      list_print_as_vector (conv_to ~env ~context ~opc (Some t.DT.vtyp))
                           oc es
  | Finalize, Stateless (SL0 (Path _)), _ ->
      !logger.error "Still some Path present in emitted code: %a"
        (E.print ~max_depth:2 false) expr ;
      assert false
  | Finalize, Case (alts, else_), t ->
      List.print ~first:"(" ~last:"" ~sep:" else "
        (fun oc alt ->
           (* If the condition is nullable then we must return NULL immediately.
            * If the cons is not nullable but the case is (for another reason),
            * then adds a Some. *)
           Printf.fprintf oc
             (if alt.E.case_cond.typ.nullable then
                "match %a with Null as n_ -> n_ \
                 | NotNull cond_ -> if cond_ then %s(%a)"
              else
                "if %a then %s(%a)")
             (emit_expr ~env ~context ~opc) alt.case_cond
             (if nullable && not alt.case_cons.typ.DT.nullable
              then "NotNull " else "")
             (conv_to ~env ~context ~opc (Some t)) alt.case_cons)
        oc alts ;
      (match else_ with
      | None ->
        (* If there is no ELSE clause then the expr is nullable: *)
        assert nullable ;
        Printf.fprintf oc " else Null)"
      | Some else_ ->
        Printf.fprintf oc " else %s(%a))"
          (if nullable && not else_.typ.DT.nullable
           then "NotNull " else "")
          (conv_to ~env ~context ~opc (Some t)) else_)
  | Finalize, Stateless (SL1s (Coalesce, es)), t ->
      let rec loop_nullable = function
        | [] -> ()
        | [last] ->
          Printf.fprintf oc "(%a)" (conv_to ~env ~context ~opc (Some t)) last
        | e :: rest ->
          Printf.fprintf oc "(match (%a) with NotNull v_ -> NotNull (%t v_) \
                                            | Null -> "
            (emit_expr ~context ~opc ~env) e
            (conv_from_to ~nullable:false e.E.typ.vtyp t) ;
          loop_nullable rest ;
          Printf.fprintf oc ")" in
      let rec loop_not_nullable = function
        | [] -> ()
        | [last] ->
          Printf.fprintf oc "(%a)" (conv_to ~env ~context ~opc (Some t)) last
        | e :: rest ->
          Printf.fprintf oc "(Nullable.default_delayed (fun () -> " ;
          loop_not_nullable rest ;
          Printf.fprintf oc ") (%a))" (conv_to ~env ~context ~opc (Some t)) e
      in
      (if nullable then loop_nullable else loop_not_nullable) es
  (* Stateless arithmetic functions which actual funcname depends on operand types: *)
  | Finalize, Stateless (SL2 (Add, e1, e2)),
    (Mac (Float|
         U8|U16|U24|U32|U40|U48|U56|U64|U128|
         I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".add")
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Sub, e1, e2)),
    (Mac (Float|
          U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".sub")
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Mul, e1, e2)),
    (Mac (Float|
          U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".mul")
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Mul, e1, e2)), (Mac String as t)->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.string_repeat"
        [ ConvTo t, PropagateNull ;
          ConvTo (Mac U32), PropagateNull ] oc
        (if e1.E.typ.DT.vtyp = Mac String then [e1; e2] else [e2; e1])
  | Finalize, Stateless (SL2 (IDiv, e1, e2)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".div")
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (IDiv, e1, e2)), (Mac Float as t) ->
      (* Here we must convert everything to float first, then divide and
       * take the floor: *)
      Printf.fprintf oc "(let x_ = " ;
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".div")
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2] ;
      Printf.fprintf oc " in if x_ >= 0. then floor x_ else ceil x_)"
  | Finalize, Stateless (SL2 (Div, e1, e2)), Mac Float ->
      emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
        "CodeGenLib.div_or_null"
        [ ConvTo (Mac Float), PropagateNull ;
          ConvTo (Mac Float), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Reldiff, e1, e2)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "reldiff"
        [ ConvTo (Mac Float), PropagateNull ;
          ConvTo (Mac Float), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Pow, e1, e2)), Mac Float ->
      emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
      "CodeGenLib.pow_or_null"
        [ ConvTo (Mac Float), PropagateNull ;
          ConvTo (Mac Float), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Pow, e1, e2)), (Mac I32 as t) ->
      emit_functionN ~env ~opc ~nullable "BatInt32.pow"
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Pow, e1, e2)), (Mac I64 as t) ->
      emit_functionN ~env ~opc ~nullable "BatInt64.pow"
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Pow, e1, e2)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128
         |I8|I16|I24|I40|I48|I56|I128) as t) ->
      (* For all others we exponentiate via floats: *)
      Printf.fprintf oc "(%t %a)"
        (conv_from_to ~nullable (Mac Float) t)
        (emit_functionN ~env ~opc ~nullable "( ** )"
          [ ConvTo (Mac Float), PropagateNull ;
            ConvTo (Mac Float), PropagateNull ])  [e1; e2]
  | Finalize, Stateless (SL2 (Trunc, e1, e2)), (Mac Float as t) ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.Truncate.float"
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Trunc, e1, e2)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128) as t) ->
      let m = omod_of_type t in
      let f =
        Printf.sprintf "CodeGenLib.Truncate.uint %s.div %s.mul" m m in
      emit_functionN ~env ~opc ~nullable f
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Trunc, e1, e2)),
    (Mac (I8|I16|I24|I32|I40|I48|I56|I128) as t) ->
      let m = omod_of_type t in
      let f =
        Printf.sprintf
          "CodeGenLib.Truncate.int %s.sub %s.compare %s.zero %s.div %s.mul"
          m m m m m in
      emit_functionN ~env ~opc ~nullable f
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Mod, e1, e2)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".rem")
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Mod, e1, e2)), (Mac Float as t) ->
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".modulo")
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Strftime, e1, e2)), Mac String ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.strftime"
        [ ConvTo (Mac String), PropagateNull ;
          ConvTo (Mac Float), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL1 (Strptime, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
        "(fun t_ -> time_of_abstime t_ |> Nullable.of_option)"
        [ ConvTo (Mac String), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Variant, e)), Mac String ->
      emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
        "CodeGenLib.get_variant"
        [ ConvTo (Mac String), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Abs, e)),
    (Mac (Float|
          U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".abs")
        [ ConvTo t, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Minus, e)),
    (Mac (Float|
          U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      emit_functionN ~env ~opc ~nullable (omod_of_type t ^".neg")
        [ ConvTo t, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Exp, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "exp"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Log, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "log"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Log10, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "log10"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Sqrt, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
        "CodeGenLib.sqrt_or_null"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Sq, e)), t ->
      let f = "(CodeGenLib.square "^ omod_of_type e.typ.vtyp ^".mul)" in
      emit_functionN ~env ~opc ~nullable f
        [ ConvTo t, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Ceil, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "ceil"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Floor, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "floor"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Round, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "Float.round"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Cos, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "cos"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Sin, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "sin"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Tan, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "tan"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (ACos, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "acos"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (ASin, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "asin"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (ATan, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "atan"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (CosH, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "cosh"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (SinH, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "sinh"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (TanH, e)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "tanh"
        [ ConvTo (Mac Float), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Hash, e)), Mac I64 ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.hash"
        [ NoConv, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Sparkline, e)), Mac String ->
      emit_functionN ~env ~opc ~nullable "sparkline"
        [ ConvTo (Vec (0, DT.make (Mac Float))), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (BeginOfRange, e)),
    Usr { name = "Ip4" ; _ } ->
      emit_functionN ~env ~opc ~nullable "RamenIpv4.Cidr.first"
        [ ConvTo T.cidrv4, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (BeginOfRange, e)),
    Usr { name = "Ip6" ; _ } ->
      emit_functionN ~env ~opc ~nullable "RamenIpv6.Cidr.first"
        [ ConvTo T.cidrv6, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (BeginOfRange, e)),
    Usr { name = "Ip" ; _ } ->
      emit_functionN ~env ~opc ~nullable "RamenIp.first"
        [ ConvTo T.cidr, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (EndOfRange, e)),
    Usr { name = "Ip4" ; _ } ->
      emit_functionN ~env ~opc ~nullable "RamenIpv4.Cidr.last"
        [ ConvTo T.cidrv4, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (EndOfRange, e)),
    Usr { name = "Ip6" ; _ } ->
      emit_functionN ~env ~opc ~nullable "RamenIpv6.Cidr.last"
        [ ConvTo T.cidrv6, PropagateNull] oc [ e ]
  | Finalize, Stateless (SL1 (EndOfRange, e)),
    Usr { name = "Ip" ; _ } ->
      emit_functionN ~env ~opc ~nullable "RamenIp.last"
        [ ConvTo T.cidr, PropagateNull ] oc [ e ]
  (* Stateless functions manipulating constructed types: *)
  | Finalize, Stateless (SL2 (Get, n, e)), _ ->
      let emit_select_from_tuple ts n =
        (* Build a tuple selector: *)
        let num_items = Array.length ts in
        let rec loop_t str i =
          if i >= num_items then str else
          let str = str ^ (if i > 0 then "," else "")
                        ^ (if i = n then "x_" else "_") in
          loop_t str (i + 1) in
        let nth_func = loop_t "(fun (" 0 ^") -> x_)" in
        emit_functionN ~env ~opc ~nullable nth_func
          ~impl_return_nullable:ts.(n).DT.nullable
          [ NoConv, PropagateNull ] oc [ e ]
      in
      (* Cf RamenTyping: if x is a vector and n a constant, then nullability
       * is that of items or vector: *)
      (match e.E.typ.vtyp with
      | Vec (_, t) when E.is_const n ->
          let func = "(fun a_ n_ -> Array.get a_ (Int32.to_int n_))" in
          emit_functionN ~env ~opc ~nullable func
            ~impl_return_nullable:t.DT.nullable
            [ NoConv, PropagateNull ;
              ConvTo (Mac I32), PropagateNull ] oc [e; n]
      (* Otherwise the result is nullable: *)
      | Vec (_, t) | Lst t ->
          let func =
            "(fun a_ n_ -> try " ^
            (* Make the item nullable if they are not already: *)
            (if t.DT.nullable then "" else "NotNull ") ^
            "(Array.get a_ (Int32.to_int n_)) with Invalid_argument _ -> Null)" in
          emit_functionN ~env ~opc ~nullable func
            ~impl_return_nullable:true
            [ NoConv, PropagateNull ;
              ConvTo (Mac I32), PropagateNull ] oc [e; n]
      (* Never nullable: *)
      | Tup ts ->
          let n = E.int_of_const n |>
                  option_get "Get from tuple must have const index" __LOC__ in
          emit_select_from_tuple ts n
      | Rec kts ->
          let s = E.string_of_const n |>
                  option_get "Get from record must have const str index" __LOC__ in
          let pos_of_field =
            try array_rfindi (fun (k, _) -> k = s) kts
            with Not_found ->
              Printf.sprintf2 "Invalid field name %S (have %a)"
                s
                (pretty_array_print (fun oc (k, _) -> String.print oc k)) kts |>
              failwith in
          let ts = Array.map snd kts in
          emit_select_from_tuple ts pos_of_field
      | Map (k, _v) ->
          (* All gets from a map are nullable: *)
          emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
            "CodeGenLib.Globals.map_get"
            [ NoConv, PropagateNull ;
              (* Confidently convert the key value into the declared type for
               * keys, although the actual implementation of map_get accepts
               * only strings (and the type-checker will also only accept a
               * map which keys are strings since integers are list/vector
               * accessors.
               * FIXME: either really support other types for keys, and find
               * a new syntax to distinguish Get from lists than maps, _or_
               * forbid declaring a map of another key type than string.
               * Oh, and by the way, did I mentioned that map_get will only
               * return strings as well? *)
              ConvTo k.DT.vtyp, PropagateNull ] oc [ e ; n ]
      | _ -> assert false)
  (* Other stateless functions *)
  | Finalize, Stateless (SL2 (Ge, e1, e2)), Mac Bool ->
      emit_functionN ~env ~opc ~nullable "(>=)"
        [ AnyType, PropagateNull ;
          AnyType, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Gt, e1, e2)), Mac Bool ->
      emit_functionN ~env ~opc ~nullable "(>)"
        [ AnyType, PropagateNull ;
          AnyType, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Eq, e1, e2)), Mac Bool ->
      emit_functionN ~env ~opc ~nullable "(=)"
        [ AnyType, PropagateNull ;
          AnyType, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Concat, e1, e2)), Mac String ->
      emit_functionN ~env ~opc ~nullable "(^)"
        [ ConvTo (Mac String), PropagateNull ;
          ConvTo (Mac String), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (StartsWith, e1, e2)), Mac Bool ->
      emit_functionN ~env ~opc ~nullable "String.starts_with"
        [ ConvTo (Mac String), PropagateNull ;
          ConvTo (Mac String), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (EndsWith, e1, e2)), Mac Bool ->
      emit_functionN ~env ~opc ~nullable "String.ends_with"
        [ ConvTo (Mac String), PropagateNull ;
          ConvTo (Mac String), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL1 (Like p, e)), Mac Bool ->
      let pattern = Globs.compile ~star:'%' ~placeholder:'_' ~escape:'\\' p in
      Printf.fprintf oc "(let pattern_ = Globs.%a in "
        Globs.print_pattern_ocaml pattern ;
      emit_functionN ~env ~opc ~nullable "Globs.matches pattern_ "
        [ ConvTo (Mac String), PropagateNull ] oc [ e ];
      Printf.fprintf oc ")"
  | Finalize, Stateless (SL1 (Length, e)), Mac U32 when E.is_a_string e ->
      emit_functionN ~env ~opc ~nullable "(Uint32.of_int % String.length)"
        [ ConvTo (Mac String), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Length, e)), Mac U32 when E.is_a_list e ->
      emit_functionN ~env ~opc ~nullable "(Uint32.of_int % Array.length)"
        [NoConv, PropagateNull] oc [ e ]
  (* lowercase and uppercase assume latin1 and will gladly destroy UTF-8
   * encoded char, therefore we use the ascii variants: *)
  | Finalize, Stateless (SL1 (Lower, e)), Mac String ->
      emit_functionN ~env ~opc ~nullable "String.lowercase_ascii"
        [ ConvTo (Mac String), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Upper, e)), Mac String ->
      emit_functionN ~env ~opc ~nullable "String.uppercase_ascii"
        [ ConvTo (Mac String), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (UuidOfU128, e)), Mac String ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.uuid_of_u128"
        [ ConvTo (Mac U128), PropagateNull ] oc [ e ]
  (* And and Or does not inherit nullability from their arguments the way
   * other functions does: given only one value we may be able to find out
   * the result without looking at the other one (that can then be NULL). *)
  (* FIXME: anyway, we would like AND and OR to shortcut the evaluation of
   * their argument when the result is known, so we must not use
   * [emit_functionN] but craft our own version here. *)
  | Finalize, Stateless (SL2 (And, e1, e2)), Mac Bool ->
      if nullable then
        emit_functionN ~env ~opc ~nullable "CodeGenLib.and_opt"
          ~impl_return_nullable:true
          [ ConvTo (Mac Bool), PassAsNull ;
            ConvTo (Mac Bool), PassAsNull ] oc [e1; e2]
      else
        emit_functionN ~env ~opc ~nullable "(&&)"
          [ ConvTo (Mac Bool), PropagateNull ;
            ConvTo (Mac Bool), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (Or, e1,e2)), Mac Bool ->
      if nullable then
        emit_functionN ~env ~opc ~nullable "CodeGenLib.or_opt"
          ~impl_return_nullable:true
          [ ConvTo (Mac Bool), PassAsNull ;
            ConvTo (Mac Bool), PassAsNull ] oc [e1; e2]
      else
        emit_functionN ~env ~opc ~nullable "(||)"
          [ ConvTo (Mac Bool), PropagateNull ;
            ConvTo (Mac Bool), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 ((BitAnd|BitOr|BitXor as op), e1, e2)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      let n = match op with BitAnd -> "logand" | BitOr -> "logor"
                          | _ -> "logxor" in
      emit_functionN ~env ~opc ~nullable
        (omod_of_type t ^"."^ n)
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL2 (BitShift, e1, e2)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      emit_functionN ~env ~opc ~nullable
        ("CodeGenLib.Shift."^ omod_of_type t ^".shift")
        [ ConvTo t, PropagateNull ;
          ConvTo (Mac I16), PropagateNull ] oc [e1; e2]
  | Finalize, Stateless (SL1 (Not, e)), Mac Bool ->
      emit_functionN ~env ~opc ~nullable "not"
        [ ConvTo (Mac Bool), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Defined, e)), Mac Bool ->
      (* Do not call emit_functionN to avoid null propagation: *)
      Printf.fprintf oc "(match %a with Null -> false | _ -> true)"
        (emit_expr ~env ~context ~opc) e
  | Finalize, Stateless (SL1 (Age, e)),
    (Mac (Float|
          U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as to_typ)
  | Finalize, Stateless (SL1 (BeginOfRange, e)),
    (Usr { name = "Cidr4" | "Cidr6" ; _ } as to_typ) ->
      let in_type_name =
        String.lowercase (IO.to_string DT.print_value_type to_typ) in
      let name = "CodeGenLib.age_"^ in_type_name in
      emit_functionN ~env ~opc ~nullable name
        [ ConvTo to_typ, PropagateNull ] oc [ e ]
  (* TODO: Now() for Uint62? *)
  | Finalize, Stateless (SL0  Now), Mac Float ->
      String.print oc "!CodeGenLib.now"
  | Finalize, Stateless (SL0 Random), Mac Float ->
      String.print oc "(Random.float 1.)"
  | Finalize, Stateless (SL0 Pi), Mac Float ->
      String.print oc "Float.pi"
  | Finalize, Stateless (SL0 EventStart), Mac Float ->
      Printf.fprintf oc "((%a) |> fst)" emit_event_time opc
  | Finalize, Stateless (SL0 EventStop), Mac Float ->
      Printf.fprintf oc "((%a) |> snd)" emit_event_time opc
  | Finalize, Stateless (SL1 (Cast _, { text = Const VNull ; _ })), _ ->
      (* Special case when casting NULL to anything: that must work whatever the
       * destination type, even if we have no converter from the type of NULL.
       * This is important because literal NULL type is random. *)
      Printf.fprintf oc "Null"
  | Finalize, Stateless (SL1 (Cast _, e)), t ->
      (* A failure to convert should yield a NULL value rather than crash that
       * tuple, unless the user insisted to convert to a non-nullable type: *)
      if nullable then String.print oc "(try " ;
      let from = e.E.typ in
      (* Shall we force a non-nullable argument to become nullable, or
       * propagates nullability from the argument? *)
      let add_nullable = not from.DT.nullable && nullable in
      if add_nullable then Printf.fprintf oc "NotNull (" ;
      Printf.fprintf oc "(%t) (%a)"
        (conv_from_to ~nullable:from.DT.nullable from.vtyp t)
        (emit_expr ~env ~context ~opc) e ;
      if add_nullable then Printf.fprintf oc ")" ;
      if nullable then String.print oc " with _ -> Null)"
  | Finalize, Stateless (SL1 (Peek (t, endianness), x)), _
    when E.is_a_string x ->
      (* x is a string and t is some nullable integer. *)
      String.print oc "(try " ;
      emit_functionN ~env ~opc ~nullable
        (Printf.sprintf
          "(fun s_ -> %s.of_bytes_%s_endian (Bytes.of_string s_) %d)"
          (omod_of_type t.DT.vtyp)
          (string_of_endianness endianness)
          0 (* TODO: add that offset to PEEK? *))
        [ ConvTo (Mac String), PropagateNull ] oc [ x ] ;
      String.print oc " with _ -> Null)"
  (* Similarly to the above, but reading from an array of integers instead
   * of from a string. *)
  | Finalize, Stateless (SL1 ((Peek (t, endianness)), e)), _ ->
      let omod_res = omod_of_type t.DT.vtyp in
      let inp_typ =
        match e.E.typ.vtyp with
        | DT.Vec (_, t) -> t
        | _ -> assert false (* Bug in type checking *) in
      let inp_width = DT.width_of_int inp_typ.vtyp
      and res_width = DT.width_of_int t.vtyp in
      emit_functionN ~env ~opc ~nullable
        (Printf.sprintf
          "(CodeGenLib.IntOfArray.%s \
             %s.logor %s.shift_left %d %d %s.zero %s.of_uint%d)"
          (string_of_endianness endianness)
          omod_res omod_res inp_width res_width omod_res omod_res inp_width)
        [ ConvTo e.E.typ.vtyp, PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1 (Chr, e)), _ ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.chr"
        [ ConvTo (Mac U32), PropagateNull ] oc [ e ]
  | Finalize, Stateless (SL1s (Max, es)), t ->
      emit_functionN ~opc ~args_as:(Array 0) ~env ~nullable
        "Array.max"
        (List.map (fun _ -> ConvTo t, PropagateNull) es) oc es
  | Finalize, Stateless (SL1s (Min, es)), t ->
      emit_functionN ~opc ~args_as:(Array 0) ~env ~nullable
        "Array.min"
        (List.map (fun _ -> ConvTo t, PropagateNull) es) oc es
  | Finalize, Stateless (SL1s (Print, es)), _ ->
      (* We want to print nulls as well, so we make all parameters optional
       * strings: *)
      (match es with
      | [] -> ()
      | es ->
          let e = List.last es in
          Printf.fprintf oc
            "(let x0_ = %a in CodeGenLib.print (%a) ; x0_)"
            (emit_expr ~env ~context ~opc) e
            (List.print (fun oc e ->
               Printf.fprintf oc "%s(%a)"
                 (if e.E.typ.DT.nullable then "" else "NotNull ")
                 (conv_to ~env ~context ~opc (Some (Mac String))) e)) es)
  (* IN can have many meanings: *)
  | Finalize, Stateless (SL2 (In, e1, e2)), Mac Bool ->
      (match e1.E.typ.vtyp, e2.E.typ.vtyp with
      | Usr { name = "Ip4" ; _ }, Usr { name = "Cidr4" ; _ } ->
          emit_functionN ~env ~opc ~nullable "RamenIpv4.Cidr.is_in"
            [ ConvTo T.ipv4, PropagateNull ;
              ConvTo T.cidrv4, PropagateNull ] oc [ e1 ; e2 ]
      | Usr { name = "Ip6" ; _ }, Usr { name = "Cidr6" ; _ } ->
          emit_functionN ~env ~opc ~nullable "RamenIpv6.Cidr.is_in"
            [ ConvTo T.ipv6, PropagateNull ;
              ConvTo T.cidrv6, PropagateNull ] oc [ e1 ; e2 ]
      | Usr { name = "Ip4"|"Ip6"|"Ip" ; _ },
        Usr { name = "Cidr4"|"Cidr6"|"Cidr" ; _ } ->
          emit_functionN ~env ~opc ~nullable "RamenIp.is_in"
            [ ConvTo T.ip, PropagateNull ;
              ConvTo T.cidr, PropagateNull ] oc [ e1 ; e2 ]
      | Usr { name = "Ip4"|"Ip6"|"Ip" ; _ },
        (Vec (_, ({ vtyp = Usr { name = "Cidr4"|"Cidr6"|"Cidr" ; _ } ; _ } as t)) |
         Lst ({ vtyp = Usr { name = "Cidr4"|"Cidr6"|"Cidr" ; _ } ; _ } as t)) ->
          emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
            (if t.DT.nullable then "RamenIp.is_in_list_of_nullable"
                              else "RamenIp.is_in_list")
            (* We want to know that NULL is not in [], so we pass everything
             * as nullable to the function, that will deal with it. *)
            [ ConvTo T.ip, PassAsNull ;
              ConvTo (Lst (DT.make ~nullable:t.DT.nullable T.cidr)), PassAsNull ]
            oc [ e1 ; e2 ]
      | Mac String, Mac String ->
          emit_functionN ~env ~opc ~nullable "String.exists"
            [ ConvTo (Mac String), PropagateNull ;
              ConvTo (Mac String), PropagateNull ] oc [e2; e1]
      | t1, (Vec (_, t) | Lst t) ->
          let emit_in csts_len csts_hash_init non_csts =
            (* We make a constant hash with the constants. Note that when e1 is
             * also a constant the OCaml compiler could optimize the whole
             * "x=a||x=b||x=b..." operation but only if not too many conversions
             * are involved, so we take no risk and build the hash in any case.
             * Typing only enforce that t1 < t or t > t1 (so we can look for an u8
             * in a set of i32, or the other way around which both make sense).
             * Here for simplicity all values will be converted to the largest of
             * t and t1: *)
            let larger_t = large_enough_for t.vtyp t1 in
            (* Note re. nulls: we are going to emit code such as "A=x1||A=x2" in
             * lieu of "A IN [x1; x2]". Notice that nulls do not propagate from
             * the xs in case A is found in the set, but do if it is not; Indeed,
             * "1 IN [1;NULL]" is true but "2 IN [1;NULL]" is NULL. If A is NULL
             * though, then the result is NULL unless the set is empty:
             * "NULL in [1; 2]" is NULL, but "NULL in []" is false. *)
            if e1.typ.DT.nullable then
              (* Even if e1 is null, we can answer the operation if e2 is
               * empty (but not if it's null, in which case csts_len will
               * also be "0"!): *)
              Printf.fprintf oc "(match %a with Null -> \
                                   if %s = 0 && %s && %s \
                                     then NotNull true else Null \
                                 | NotNull in0_ -> "
                (conv_to ~env ~context:Finalize ~opc (Some larger_t)) e1
                (* e2 is an empty set (or is null!): *)
                csts_len
                (* there are no non constant values in the set: *)
                (string_of_bool (non_csts = []))
                (* e2 is not NULL: *)
                (if e2.typ.DT.nullable then
                  Printf.sprintf2 "(%a) <> Null"
                    (emit_expr ~env ~context:Finalize ~opc) e2
                else "true")
            else
              Printf.fprintf oc "(let in0_ = %a in "
                (conv_to ~env ~context:Finalize ~opc (Some larger_t)) e1 ;
            (* Now if we have some null in es then the return value is either
             * Some true or None, while if we had no null the return value is
             * either Some true or Some false. *)
            Printf.fprintf oc "let _ret_ = ref (NotNull false) in\n" ;
            (* First check the csts: *)
            (* Note that none should be nullable ATM, and even if they were all
             * nullable then we would store the option.get of the values (knowing
             * that, if any of the const is NULL then we can shotcut all this and
             * answer NULL directly) *)
            if csts_len <> "0" then (
              let hash_id =
                "const_in_"^ string_of_int expr.E.uniq_num ^"_" in
              if not (Set.mem expr.E.uniq_num opc.gen_consts) then (
                opc.gen_consts <- Set.add expr.E.uniq_num opc.gen_consts ;
                Printf.fprintf opc.consts
                  "let %s =\n\
                   \tlet h_ = Hashtbl.create (%s) in\n\
                   \t%s ;\n\
                   \th_\n"
                  hash_id csts_len (csts_hash_init larger_t)) ;
              Printf.fprintf oc "if Hashtbl.mem %s in0_ then %strue else "
                hash_id (if nullable then "NotNull " else "")) ;
            (* Then check each non-const in turn: *)
            let had_nullable =
              List.fold_left (fun had_nullable e ->
                if e.E.typ.DT.nullable (* not possible ATM *) then (
                  Printf.fprintf oc
                    "if (match %a with Null -> _ret_ := Null ; false \
                     | NotNull in1_ -> in0_ = in1_) then true else "
                    (conv_to ~env ~context:Finalize ~opc (Some larger_t)) e ;
                  true
                ) else (
                  Printf.fprintf oc "if in0_ = %a then %strue else "
                    (conv_to ~env ~context:Finalize ~opc (Some larger_t)) e
                    (if nullable then "NotNull " else "") ;
                  had_nullable)
              ) false non_csts in
            Printf.fprintf oc "%s)"
              (if had_nullable then "!_ret_" else
               if nullable then "NotNull false" else "false")
          in
          (match e2 with
          | E.{ text = Vector es ; _ } ->
              let csts, non_csts =
                (* TODO: leave the IFs when we know the compiler will optimize them
                 * away:
                if is_const e1 then [], es else*)
                List.partition E.is_const es in
              let csts, non_csts =
                if List.length csts >= 6 (* guessed *) then csts, non_csts
                else [], csts @ non_csts in
              let csts_len = List.length csts |> string_of_int
              and csts_hash_init larger_t =
                Printf.sprintf2 "%a"
                  (List.print ~first:"" ~last:"" ~sep:" ;\n\t" (fun cc e ->
                    Printf.fprintf cc "Hashtbl.replace h_ (%a) ()"
                      (conv_to ~env ~context:Finalize ~opc (Some larger_t)) e))
                  csts in
              emit_in csts_len csts_hash_init non_csts
          | E.{ text = (Stateless (SL0 (Path _)) | Binding (RecordField _)) ;
                typ = { vtyp = (Vec (_, telem) | Lst telem) ; _ } ; _ } ->
              (* Unlike the above case of an immediate list of items, here e2 may be
               * nullable so we have to be more cautious. If it's nullable and
               * actually null then the size of the constant hash we need is 0: *)
              let csts_len =
                if e2.typ.DT.nullable then
                  Printf.sprintf2
                    "(match (%a) with Null -> 0 | NotNull x_ -> Array.length x_)"
                    (emit_expr ~env ~context:Finalize ~opc) e2
                else
                  Printf.sprintf2 "Array.length (%a)"
                    (emit_expr ~env ~context:Finalize ~opc) e2
              and csts_hash_init larger_t =
                Printf.sprintf2
                  "Array.iter (fun e_ -> Hashtbl.replace h_ (%t e_) ()) (%t)"
                  (conv_from_to ~nullable:telem.DT.nullable telem.vtyp larger_t)
                  (fun oc ->
                    if e2.typ.DT.nullable then
                      Printf.fprintf oc
                        "(match (%a) with Null -> [||] | NotNull x_ -> x_)"
                        (emit_expr ~env ~context:Finalize ~opc) e2
                    else
                      emit_expr ~env ~context:Finalize ~opc oc e2)
              in
              emit_in csts_len csts_hash_init []
          | _ -> assert false)
      | _ -> assert false)
  | Finalize, Stateless (SL2 (Percentile, lst, percs)), Vec _ ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.Percentile.multi"
        [ ConvTo (Vec (0, DT.make (Mac Float))), PropagateNull ;
          NoConv, PropagateNull ] oc [ percs ; lst ]
  | Finalize, Stateless (SL2 (Percentile, lst, percs)), _ ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.Percentile.single"
        [ ConvTo (Mac Float), PropagateNull ;
          NoConv, PropagateNull ] oc [ percs ; lst ]
  | Finalize, Stateless (SL2 (Index, s, a)), _ ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.index"
        [ ConvTo (Mac String), PropagateNull ;
          ConvTo (Mac Char), PropagateNull ] oc [ s ; a ]
  | Finalize, Stateless (SL3 (SubString, s, a, b)), _ ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.substring"
        [ ConvTo (Mac String), PropagateNull ;
          ConvTo (Mac I32), PropagateNull ;
          ConvTo (Mac I32), PropagateNull ] oc [ s ; a ; b ]
  | Finalize, Stateless (SL3 (MapSet, m, k, v)), _ ->
      (* This is the only function that modifies some existing value hold
       * by some variable. So m has to be a "GlobalVariable", here also required
       * to be a map of some sort (MapSet could also work with vectors/lists).
       * So at this point m should be bound to some variable. The code for that
       * binding will return a pair of functions (setter/getter). Then, the
       * CodeGenLib.map_add function can receive it and the key and value and
       * do the right thing. *)
      (* Fetch the expected key and value type for the map type of m,
       * and convert the actual k and v into those types: *)
      (match m.E.typ.DT.vtyp with
      | Map (ktyp, vtyp) ->
          emit_functionN ~env ~opc ~nullable "CodeGenLib.Globals.map_set"
            [ NoConv, PropagateNull ;
              ConvTo ktyp.vtyp, PropagateNull ;
              ConvTo vtyp.vtyp, PropagateNull ] oc [ m ; k ; v ]
      | _ ->
          assert false (* If type checker did its job *))
  | Finalize, Stateless (SL1 (Fit, e1)), Mac Float ->
      (* [e1] is supposed to be a list/vector of scalars or tuples of scalars.
       * All items of those tuples are supposed to be numeric, so we convert
       * all of them into floats and then proceed with the regression.  *)
      let ts =
        match e1.E.typ.DT.vtyp with
        | Lst { vtyp = Tup ts ; _ }
        | Vec (_, { vtyp = Tup ts ; _ }) ->
            ts
        | Lst numeric
        | Vec (_, numeric)
          when DT.is_numeric numeric.DT.vtyp ->
            [| numeric |]
        | _ ->
            !logger.error
              "Type-checking failed to ensure Fit argument is a sequence" ;
            assert false in
      (* Convert the argument into a nullable list of nullable vectors
       * of non-nullable floats: *)
      let t =
        DT.(Lst (optional (Vec (Array.length ts, make (Mac Float))))) in
      emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
        "CodeGenLib.LinReg.fit"
        [ ConvTo t, PropagateNull ] oc [ e1 ]
  | Finalize, Stateless (SL1 (CountryCode, e1)), Mac String ->
      let t1 = e1.E.typ.DT.vtyp in
      let fn =
        match t1 with
        | Usr { name = "Ip4" ; _ } -> "CountryOfIp.of_ipv4"
        | Usr { name = "Ip6" ; _ } -> "CountryOfIp.of_ipv6"
        | Usr { name = "Ip" ; _ } -> "CountryOfIp.of_ip"
        | _ -> assert false (* because of typechecking *) in
      assert nullable ; (* CountryCode return value is always nullable *)
      emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true fn
        [ ConvTo t1, PropagateNull ] oc [ e1 ]
  | Finalize, Stateless (SL1 (IpFamily, e1)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      let in_typ_name = omod_of_type t in
      let fn = "(" ^ in_typ_name ^ ".of_int % RamenIp.family)" in
      emit_functionN ~env ~opc ~nullable fn
        [ ConvTo T.ip, PropagateNull ] oc [ e1 ]
  | Finalize, Stateless (SL1 (Basename, e1)), Mac String ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.basename"
      [ ConvTo (Mac String), PropagateNull ] oc [ e1 ]
  (*
   * Stateful functions
   *
   * All the aggregation functions below should accept lists as input.
   * In that case, we merely iterate over all the elements of that list at
   * finalization. We must then reset the initial state of these function,
   * in effect making them stateless (they still have a state though,
   * although they should not.
   * FIXME: Probably this case should be recognized earlier
   * and those functions replaced by some other, specific stateless variant.
   * InitState is unchanged and UpdateState is a NOP.
   * We do this for most of them but not all, as use case is arguable in
   * many cases and a better approach needs to be devised.
   * We pattern match those case first:
   *)
  | UpdateState,
    Stateful (_, _, SF1 (_, e)), _ when E.is_a_list e ->
      ()
  | Finalize,
    Stateful (g, n, SF1 (aggr, e)), _ when E.is_a_list e ->
      (* Build the expression that aggregate the list items rather than the
       * list: *)
      let var_name = "item_" in
      let expr' =
        let item_typ =
          match e.E.typ.vtyp with
          | Lst t | Vec (_, t) -> t
          | _ -> assert false in
        let e' =
          E.make ~nullable:item_typ.DT.nullable ~vtyp:item_typ.vtyp
                 (Binding (Direct var_name)) in
        (* By reporting the skip-null flag we make sure that each update will
         * skip the nulls in the list - while the list itself will make the
         * whole expression null if it's null. *)
        E.{ expr with text = Stateful (g, n, SF1 (aggr, e')) }
      in
      (* FIXME: That the element is scalar should be enforced by type checking
       * but is not ; and actually, there are some aggregation function that
       * does not care (such as First). *)
      assert (not (E.is_a_list expr')) ;
      (* Start by resetting the state: *)
      Printf.fprintf oc "(" ;
      Printf.fprintf oc "\t\t%a <- %a ;\n"
        (emit_expr ~env ~context:Finalize ~opc) my_state
        (emit_expr ~env ~context:InitState ~opc) expr ;
      Printf.fprintf oc "\t\t%a_empty_ <- false ;\n"
        (emit_expr ~env ~context:Finalize ~opc) my_state ;
      Printf.fprintf oc "(match %a with "
        (emit_expr ~env ~context:Finalize ~opc) e ;
      if e.E.typ.DT.nullable then
        Printf.fprintf oc "Null as n_ -> n_ | NotNull arr_ ->\n"
      else
        Printf.fprintf oc "arr_ ->\n" ;
      Printf.fprintf oc
        "Array.iter (fun %s -> %a) arr_ ;\n"
        var_name
        (emit_expr ~env ~context:UpdateState ~opc) expr' ;
      (* And finalize that using the fake expression [expr'] to reach
       * the actual finalizer: *)
      emit_expr ~env ~context ~opc oc expr' ;
      Printf.fprintf oc "))"
  | InitState, Stateful (_, _, SF1 (AggrAnd, _)), Mac Bool ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc "true")
  | UpdateState, Stateful (_, n, SF1 (AggrAnd, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "(&&)" oc [ ConvTo (Mac Bool), PropagateNull ]
  | InitState, Stateful (_, _, SF1 (AggrOr, _)), Mac Bool ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc "false")
  | UpdateState, Stateful (_, n, SF1 (AggrOr, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "(||)" oc [ ConvTo (Mac Bool), PropagateNull ]
  | Finalize, Stateful (_, n, SF1 ((AggrAnd|AggrOr), _)), Mac Bool ->
      finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []
  | InitState, Stateful (_, _, SF1 ((AggrBitAnd|AggrBitOr|AggrBitXor), _)), t ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc (omod_of_type t ^ ".zero"))
  | UpdateState, Stateful (_, n, SF1 (AggrBitAnd, e)), t ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        (omod_of_type t ^ ".logand") oc [ ConvTo t, PropagateNull ]
  | UpdateState, Stateful (_, n, SF1 (AggrBitOr, e)), t ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        (omod_of_type t ^ ".logor") oc [ ConvTo t, PropagateNull ]
  | UpdateState, Stateful (_, n, SF1 (AggrBitXor, e)), t ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        (omod_of_type t ^ ".logxor") oc [ ConvTo t, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 ((AggrBitAnd|AggrBitOr|AggrBitXor), _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []
  | InitState, Stateful (_, _, SF1 (AggrSum, _)), Mac Float ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc "Kahan.init")
  | UpdateState, Stateful (_, n, SF1 (AggrSum, e)), (Mac Float as t) ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "Kahan.add" oc [ ConvTo t, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrSum, _)), Mac Float ->
      finalize_state ~env ~opc ~nullable n my_state
        "Kahan.finalize" [] oc []
  | InitState, Stateful (_, _, SF1 (AggrSum, _)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "%t Uint8.zero"
          (conv_from_to ~nullable:false (Mac U8) t))
  | UpdateState, Stateful (_, n, SF1 (AggrSum, e)),
    (Mac (U8|U16|U24|U32|U40|U48|U56|U64|U128|
          I8|I16|I24|I32|I40|I48|I56|I64|I128) as t) ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        (omod_of_type t ^".add") oc [ ConvTo t, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrSum, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []
  | InitState, Stateful (_, _, SF1 (AggrAvg, _)), Mac Float ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc "CodeGenLib.avg_init")
  | UpdateState, Stateful (_, n, SF1 (AggrAvg, e)), (Mac Float as t) ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.avg_add" oc [ ConvTo t, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrAvg, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.avg_finalize" [] oc []
  | InitState,
    Stateful (_, _, SF1 ((AggrFirst|AggrLast|AggrMax|AggrMin), _)), _ ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc "Null")
  | UpdateState, Stateful (_, n, SF1 (AggrMax, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.aggr_max" oc [ NoConv, PropagateNull ]
  | UpdateState, Stateful (_, n, SF1 (AggrMin, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.aggr_min" oc [ NoConv, PropagateNull ]
  | UpdateState, Stateful (_, n, SF1 (AggrFirst, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.aggr_first" oc [ NoConv, PropagateNull ]
  | UpdateState, Stateful (_, n, SF1 (AggrLast, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.aggr_last" oc [ NoConv, PropagateNull ]
  | Finalize,
    Stateful (_, n, SF1 ((AggrFirst|AggrLast|AggrMax|AggrMin), _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state "Nullable.get"
        [] oc []
  (* Histograms: bucket each float into the array of num_buckets + 2 and then
   * count number of entries per buckets. The 2 extra buckets are for "<min"
   * and ">max". *)
  | InitState,
    Stateful (_, _, SF1 (AggrHistogram (min, max, num_buckets), _)), _ ->
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "CodeGenLib.Histogram.init %s %s %d"
          (Legacy.Printf.sprintf "%h" min)
          (Legacy.Printf.sprintf "%h" max)
          num_buckets)
  | UpdateState, Stateful (_, n, SF1 (AggrHistogram _, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.Histogram.add" oc [ ConvTo (Mac Float), PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrHistogram _, _)), Vec _ ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Histogram.finalize" [] oc []
  | InitState, Stateful (_, _, SF2 (Lag, k, e)), _ ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.Seasonal.init"
        [ ConvTo (Mac U32), PropagateNull ;
          ConvTo (Mac U32), PropagateNull ;
          NoConv, PropagateNull ] oc
        [ k ;
          E.one () ;
          any_constant_of_expr_type ~avoid_null:true e.E.typ ]
  | UpdateState, Stateful (_, n, SF2 (Lag, _k, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.Seasonal.add" oc [ NoConv, PropagateNull ]
  | Finalize, Stateful (_, n, SF2 (Lag, _, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Seasonal.lag" [] oc []
  (* We force the inputs to be float since we are going to return a float anyway. *)
  | InitState, Stateful (_, _, SF3 (MovingAvg, p, n, _)), Mac Float ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.Seasonal.init"
        [ ConvTo (Mac U32), PropagateNull ;
          ConvTo (Mac U32), PropagateNull ;
          ConvTo (Mac Float), PropagateNull ] oc
        [ p ; n ; E.zero () ]
  | UpdateState, Stateful (_, n, SF3 (MovingAvg, _, _, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.Seasonal.add" oc
        [ ConvTo (Mac Float), PropagateNull ]
  | Finalize, Stateful (_, n, SF3 (MovingAvg, p, m, _)), Mac Float ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Seasonal.avg" [p; m] oc
        [ ConvTo (Mac U32), PropagateNull ;
          ConvTo (Mac U32), PropagateNull ]
  | Finalize, Stateful (_, n, SF4s (MultiLinReg, p, m,_ ,_)), Mac Float ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Seasonal.multi_linreg" [p; m] oc
        [ ConvTo (Mac U32), PropagateNull ;
          ConvTo (Mac U32), PropagateNull ]
  | InitState, Stateful (_, _, SF4s (MultiLinReg, p, m, _, es)),
    (Mac Float as t) ->
      emit_functionNv ~env ~opc ~nullable "CodeGenLib.Seasonal.init_multi_linreg"
        [ ConvTo (Mac U32), PropagateNull ;
          ConvTo (Mac U32), PropagateNull ;
          ConvTo (Mac Float), PropagateNull ] [ p ; m ; E.zero () ]
        t oc (List.map (fun _ -> E.zero ()) es)
  | UpdateState, Stateful (_, n, SF4s (MultiLinReg, _p , _m, e, es)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        ~vars:es ~vars_to_typ:(Mac Float)
        "CodeGenLib.Seasonal.add_multi_linreg" oc
        [ ConvTo (Mac Float), PropagateNull ]
  | InitState, Stateful (_, _, SF2 (ExpSmooth, _a, _)), Mac Float ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc "Null")
  | UpdateState, Stateful (_, n, SF2 (ExpSmooth, a, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ a ; e ]
        "CodeGenLib.smooth" oc
        [ ConvTo (Mac Float), PropagateNull ;
          ConvTo (Mac Float), PropagateNull ]
  | Finalize, Stateful (_, n, SF2 (ExpSmooth, _, _)), Mac Float ->
      finalize_state ~env ~opc ~nullable n my_state "Nullable.get" [] oc []
  | InitState, Stateful (_, _, SF4 (DampedHolt, _, _, _, _)), _ ->
      emit_functionN ~env ~opc ~nullable
        "CodeGenLib.smooth_damped_holt_init" [] oc []
  | UpdateState, Stateful (_, n, SF4 (DampedHolt, a, l, f, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ a ; l ; f ; e ]
        "CodeGenLib.smooth_damped_holt" oc
          [ ConvTo (Mac Float), PropagateNull;
            ConvTo (Mac Float), PropagateNull;
            ConvTo (Mac Float), PropagateNull;
            ConvTo (Mac Float), PropagateNull]
  | Finalize, Stateful (_, n, SF4 (DampedHolt, _, _, f, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.smooth_damped_holt_finalize" [ f ] oc
        [ ConvTo (Mac Float), PropagateNull ]
  | InitState, Stateful (_, _, SF6 (DampedHoltWinter, _, _, _, m, _, _)), _ ->
      emit_functionN ~env ~opc ~nullable
        "CodeGenLib.smooth_damped_holt_winter_init"
        [ ConvTo (Mac U8), PropagateNull ] oc [m]
  | UpdateState, Stateful (_, n, SF6 (DampedHoltWinter, a, b, g, m, f, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ a ; b ; g ; m ; f ; e ]
        "CodeGenLib.smooth_damped_holt_winter" oc
          [ ConvTo (Mac Float), PropagateNull;
            ConvTo (Mac Float), PropagateNull;
            ConvTo (Mac Float), PropagateNull;
            ConvTo (Mac U8)   , PropagateNull;
            ConvTo (Mac Float), PropagateNull;
            ConvTo (Mac Float), PropagateNull]
  | Finalize, Stateful (_, n, SF6 (DampedHoltWinter, _, _, _, _, f, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.smooth_damped_holt_winter_finalize" [ f ] oc
        [ ConvTo (Mac Float), PropagateNull ]
  | InitState, Stateful (_, _, SF4s (Remember, fpr,_tim, dur,_es)), Mac Bool ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.Remember.init"
        [ ConvTo (Mac Float), PropagateNull ;
          ConvTo (Mac Float), PropagateNull ] oc [fpr; dur]
  | UpdateState, Stateful (_, n, SF4s (Remember, _fpr, tim, _dur, es)), _ ->
      update_state ~env ~opc ~nullable n my_state (tim :: es)
        ~args_as:(Tuple 2) "CodeGenLib.Remember.add" oc
        ((ConvTo (Mac Float), PropagateNull) ::
         List.map (fun _ -> NoConv, PropagateNull) es)
  | Finalize, Stateful (_, n, SF4s (Remember, _, _, _, _)), Mac Bool ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Remember.finalize" [] oc []
  | InitState, Stateful (_, _, SF1s (Distinct, _es)), _ ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc "CodeGenLib.Distinct.init ()")
  | UpdateState, Stateful (_, n, SF1s (Distinct, es)), _ ->
      update_state ~env ~opc ~nullable n my_state es
        ~args_as:(Tuple 1) "CodeGenLib.Distinct.add" oc
        (List.map (fun _ -> NoConv, PropagateNull) es)
  | Finalize, Stateful (_, n, SF1s (Distinct, _)), Mac Bool ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Distinct.finalize" [] oc []
  | InitState, Stateful (_, _, SF3 (Hysteresis, _, _, _)), t ->
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "%t true" (* Initially within bounds *)
          (conv_from_to ~nullable:false (Mac Bool) t))
  | UpdateState, Stateful (_, n, SF3 (Hysteresis, meas, accept, max)), Mac Bool ->
      (* TODO: shouldn't we promote everything to the most accurate of those
       * types? *)
      let t = meas.E.typ.vtyp in
      update_state ~env ~opc ~nullable n my_state [ meas ; accept ; max ]
        "CodeGenLib.Hysteresis.add " oc
        [ ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ;
          ConvTo t, PropagateNull ]
  | Finalize, Stateful (_, n, SF3 (Hysteresis, _, _, _)), Mac Bool ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Hysteresis.finalize" [] oc []
  | InitState, Stateful (_, _, Top { size ; duration ; max_size ; sigmas ; _ }), _ ->
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "CodeGenLib.Top.init (%a) (%a) (%a)"
          (* Default max_size is ten times size *)
          (fun oc -> function
            | None ->
                Printf.fprintf oc "Uint32.mul (Uint32.of_int 10) (%a)"
                  (conv_to ~env ~context:Finalize ~opc (Some (Mac U32))) size
            | Some s ->
                conv_to ~env ~context:Finalize ~opc (Some (Mac U32)) oc s) max_size
          (* duration can also be a parameter compatible to float: *)
          (conv_to ~env ~context:Finalize ~opc (Some (Mac Float))) duration
          (conv_to ~env ~context:Finalize ~opc (Some (Mac Float))) sigmas)
  | UpdateState, Stateful (_, n, Top { what ; by ; time ; _ }), _ ->
      update_state ~env ~opc ~nullable n my_state (time :: by :: what)
        ~args_as:(Tuple 3) "CodeGenLib.Top.add" oc
        ((ConvTo (Mac Float), PropagateNull) ::
         (ConvTo (Mac Float), PropagateNull) ::
         List.map (fun _ -> NoConv, PropagateNull) what)
  | Finalize, Stateful (_, n, Top { output = Rank ; size ; what ; _ }), t ->
      finalize_state ~env ~opc ~nullable n my_state
        ~impl_return_nullable:true ~args_as:(Tuple 1)
        ("(fun s_ n_ x_ -> \
             CodeGenLib.Top.rank s_ n_ x_ |> \
             Nullable.map "^ omod_of_type t ^".of_int)")
        (size :: what) oc
        ((ConvTo (Mac U32), PropagateNull) ::
         List.map (fun _ -> NoConv, PropagateNull) what)
  | Finalize, Stateful (_, n, Top { output = Membership ; size ; what ; _ }), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        ~args_as:(Tuple 2)
        "CodeGenLib.Top.is_in_top"
        (size :: what) oc
        ((ConvTo (Mac U32), PropagateNull) ::
         List.map (fun _ -> NoConv, PropagateNull) what)
  | Finalize, Stateful (_, n, Top { output = List ; size ; _ }), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Top.to_list"
        [ size ] oc [ ConvTo (Mac U32), PropagateNull ]
  | InitState, Stateful (_, _, SF4s (Largest { inv ; up_to }, c, but, _, _)), _ ->
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "CodeGenLib.Largest.init ~inv:%b ~up_to:%b ~but:(%a) (%a)"
          inv up_to
          (conv_to ~env ~context:Finalize ~opc (Some (Mac U32))) but
          (conv_to ~env ~context:Finalize ~opc (Some (Mac U32))) c)
  (* Special updater that use the internal count when no `by` expressions
   * are present: *)
  | UpdateState, Stateful (_, n, SF4s (Largest _, _, _, e, [])), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.Largest.add_on_count" oc [ NoConv, PassNull ]
  | UpdateState, Stateful (_, n, SF4s (Largest _, _, _, e, es)), _ ->
      update_state ~env ~opc ~nullable n my_state (e :: es)
        ~args_as:(Tuple 2) "CodeGenLib.Largest.add" oc
        ((NoConv, PassNull) :: List.map (fun _ -> NoConv, PassNull) es)
  | Finalize, Stateful (_, n, SF4s (Largest _, _, _, _, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        ~impl_return_nullable:true
        "CodeGenLib.Largest.finalize" [] oc []
  | InitState, Stateful (_, n, SF2 (Sample, c, e)), _ ->
      let init_c =
        let c_typ = e.E.typ in
        let c_typ = if n then { c_typ with nullable = false } else c_typ in
        any_constant_of_expr_type c_typ in
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "RamenSampling.init (%a) (%a)"
          (conv_to ~env ~opc ~context:Finalize (Some (Mac U32))) c
          (emit_expr ~env ~context:Finalize ~opc) init_c)
  | UpdateState, Stateful (_, n, SF2 (Sample, _, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "RamenSampling.add" oc [ NoConv, PassNull ]
  | Finalize, Stateful (_, n, SF2 (Sample, _, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        ~impl_return_nullable:true
        "RamenSampling.finalize" [] oc []
  | InitState, Stateful (_, _, SF2 (OneOutOf, i, _)), _ ->
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "CodeGenLib.OneOutOf.init (%a)"
          (conv_to ~env ~opc ~context:Finalize (Some (Mac U32))) i)
  | UpdateState, Stateful (_, n, SF2 (OneOutOf, _, _)), _ ->
      update_state ~env ~opc ~nullable n my_state []
        "CodeGenLib.OneOutOf.add" oc []
  | Finalize, Stateful (_, n, SF2 (OneOutOf, _, e)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        ~impl_return_nullable:true
        "CodeGenLib.OneOutOf.finalize" [ e ] oc [ NoConv, PassAsNull ]
  | InitState, Stateful (_, _, SF3 (OnceEvery { tumbling }, d, _, _)), _ ->
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "CodeGenLib.OnceEvery.init (%a) %b"
          (conv_to ~env ~opc ~context:Finalize (Some (Mac Float))) d
          tumbling)
  | UpdateState, Stateful (_, n, SF3 (OnceEvery _, _, time, _)), _ ->
      update_state ~env ~opc ~nullable n my_state [ time ]
        "CodeGenLib.OnceEvery.add" oc
        [ ConvTo (Mac Float), PropagateNull ]
  | Finalize, Stateful (_, n, SF3 (OnceEvery _, _, _, e)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        ~impl_return_nullable:true
        "CodeGenLib.OnceEvery.finalize" [ e ] oc [ NoConv, PassAsNull ]
  | InitState, Stateful (_, n, Past {
      what ; max_age ; sample_size ; tumbling ; _ }), _ ->
      let init_c =
        let c_typ = what.E.typ in
        let c_typ = if n then { c_typ with nullable = false } else c_typ in
        any_constant_of_expr_type c_typ in
      wrap_nullable ~nullable oc (fun oc ->
        Printf.fprintf oc "CodeGenLib.Past.init (%a) %b (%a) (%a)"
          (conv_to ~env ~context:Finalize ~opc (Some (Mac Float))) max_age
          tumbling
          (Option.print (fun oc sz ->
            (* Would be nicer if conv_to would handle the parenth itself *)
            Printf.fprintf oc "(%a)"
              (conv_to ~env ~context:Finalize ~opc (Some (Mac U32))) sz))
            sample_size
          (emit_expr ~env ~context:Finalize ~opc) init_c)
  | UpdateState, Stateful (_, n, Past { what ; time ; _ }), _ ->
      update_state ~env ~opc ~nullable n my_state [ what ; time ]
        "CodeGenLib.Past.add" oc
        [ NoConv, PassNull ;
          ConvTo (Mac Float), PropagateNull ]
  | Finalize, Stateful (_, n, Past _), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        ~impl_return_nullable:true
        "CodeGenLib.Past.finalize" [] oc []
  (* Grouping operation: accumulate all values in a list, that we initialize
   * empty. At finalization, an empty list means we skipped all values ;
   * and we return Null in that case. Note that since this is an aggregate
   * function, there is no way ever to commit or use (finalize) a function
   * before it's been sent at least one value. *)
  | InitState, Stateful (_, _, SF1 (Group, _)), _ ->
      wrap_nullable ~nullable oc (fun oc ->
        String.print oc "[]")
  | UpdateState, Stateful (_, n, SF1 (Group, e)), _ ->
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.Group.add" oc [ NoConv, PassNull ]
  | Finalize, Stateful (_, n, SF1 (Group, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state
        "CodeGenLib.Group.finalize" [] oc []
  (* Count behavior depends on the type of what we count: if it's a boolean
   * expression it count how many times it is true (handy for filtering or
   * with Distinct); In all other cases it just count 1 for everything,
   * skipping nulls as requested. *)
  | InitState, Stateful (_, _, SF1 (Count, _)), _ ->
      wrap_nullable ~nullable oc (fun oc -> String.print oc "Uint32.zero")
  | UpdateState, Stateful (_, n, SF1 (Count, e)), _ ->
      if e.typ.vtyp = Mac Bool then
        update_state ~env ~opc ~nullable n my_state [ e ]
          "CodeGenLib.Count.count_true" oc
          [ ConvTo (Mac Bool), PropagateNull ]
      else
        update_state ~env ~opc ~nullable n my_state []
          "CodeGenLib.Count.count_anything" oc []
  | Finalize, Stateful (_, n, SF1 (Count, _)), _ ->
      finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []
  (* Generator: the function appears only during tuple generation, where
   * it sends the output to its continuation as (freevar_name expr).
   * In normal expressions we merely refer to that free variable. *)
  | Generator, Generator (Split (e1,e2)), Mac String ->
      emit_functionN ~env ~opc ~nullable "CodeGenLib.split"
        [ ConvTo (Mac String), PropagateNull ;
          ConvTo (Mac String), PropagateNull ] oc [e1; e2]
  | Finalize, Generator (Split (_e1,_e2)), Mac String ->
      (* Output it as a free variable *)
      String.print oc (freevar_name expr)
  | _, _, _ ->
      let m =
        Printf.sprintf2 "Cannot find implementation of %a for context %s"
          (E.print true) expr
          (string_of_context context) in
      failwith m

and emit_expr ~env ~context ~opc oc expr =
  try emit_expr_ ~env ~context ~opc oc expr
  with Error _ as e -> raise e
     | e -> raise (Error (expr, context, Printexc.to_string e))

and add_missing_types arg_typs es =
  (* The list of args is composed of:
   * - at first, individual types tailored for each argument
   * - then a unique type large enough for all remaining arguments,
   *   repeated for all the rest of the arguments.
   * This is useful for variadic functions, where the first args may have
   * different arguments and the rest are combined together and must be made
   * compatible. Here [ht] is the first part of this list and [rt] is the
   * combined type for the rest of arguments, and [n] how many of these we must
   * have to form the complete list of types.
   * Returns only a type option to convert to (None = no conversion required);
   * AnyType is gone, replaced by the actual type. *)
  let merge_types t1_opt t2 =
    match t1_opt with
    | None -> t2
    | Some t1 -> T.large_enough_for t1 t2 in
  let rec loop ht rt rpn any_type n = function
  | [], _ -> (* No more arguments *)
    List.rev_append ht (List.init n (fun _ -> rt, rpn)) |>
    (* Replace all AnyTypes by the common type large enough to accommodate
     * them all: any_type. *)
    List.map (fun (t, null_prop) ->
      (match t with
      | AnyType -> any_type
      | ConvTo t -> Some t
      | NoConv -> None),
      null_prop)
  | e::es, (t, null_prop)::ts ->
    let any_type =
      if t <> AnyType then any_type else
      Some (merge_types any_type e.E.typ.vtyp) in
    loop ((t, null_prop)::ht) t null_prop any_type n (es, ts)
  | e::es, [] -> (* Missing some types: update rt *)
    let te = e.E.typ.vtyp in
    match rt with
    | NoConv ->
      assert (n = 0) ;
      loop ht (ConvTo te) rpn any_type 1 (es, [])
    | AnyType ->
      assert (n > 0) ;
      (* Keep that and improve any_type for final replacement: *)
      loop ht rt rpn (Some (merge_types any_type te)) (n+1) (es, [])
    | ConvTo t ->
      (* Improve rt: *)
      loop ht (ConvTo (merge_types (Some t) te)) rpn any_type (n+1) (es, [])
  in
  loop [] NoConv PropagateNull None 0 (es, arg_typs)

(*$inject
  open Batteries
  open Stdint
  open DessserTypes
  open RamenTypes
  let const vtyp v =
    RamenExpr.make ~vtyp ~nullable:false (Const v)
 *)
(*$= add_missing_types & ~printer:dump
  [ Some (Mac Float), PropagateNull ] \
    (add_missing_types \
      [ ConvTo (Mac Float), PropagateNull ] \
      [ const (Mac Float) (VFloat 1.) ])

  [ Some (Mac Float), PropagateNull ] \
    (add_missing_types \
      [] \
      [ const (Mac Float) (VFloat 1.) ])

  [ Some (Mac Float), PropagateNull ; \
    Some (Mac U8), PropagateNull ] \
    (add_missing_types \
      [ ConvTo (Mac Float), PropagateNull ; \
        ConvTo (Mac U8), PropagateNull ] \
      [ const (Mac Float) (VFloat 1.) ; \
        const (Mac U8) (VU8 (Uint8.of_int 42)) ])

  [ Some (Mac Float), PropagateNull ; \
    Some (Mac U16), PropagateNull ; \
    Some (Mac U16), PropagateNull ] \
    (add_missing_types \
      [ ConvTo (Mac Float), PropagateNull ; \
        ConvTo (Mac U16), PropagateNull ] \
      [ const (Mac Float) (VFloat 1.) ; \
        const (Mac U8) (VU8 (Uint8.of_int 42)) ; \
        const (Mac U8) (VU8 (Uint8.of_int 42)) ])

  [ Some (Mac Float), PropagateNull ; \
    Some (Mac U16), PropagateNull ; \
    Some (Mac U16), PropagateNull ] \
    (add_missing_types \
      [ ConvTo (Mac Float), PropagateNull ; \
        ConvTo (Mac U16), PropagateNull ] \
      [ const (Mac Float) (VFloat 1.) ; \
        const (Mac U8) (VU8 (Uint8.of_int 42)) ; \
        const (Mac U16) (VU16 (Uint16.of_int  42)) ])

  [ Some (Mac Float), PropagateNull ; \
    Some (Mac U16), PropagateNull ; \
    Some (Mac U16), PropagateNull ] \
    (add_missing_types \
      [ ConvTo (Mac Float), PropagateNull ; \
        ConvTo (Mac U16), PropagateNull ] \
      [ const (Mac Float) (VFloat 1.) ; \
        const (Mac U16) (VU16 (Uint16.of_int 42)) ; \
        const (Mac U8) (VU8 (Uint8.of_int 42)) ])

  [ Some (Mac Float), PropagateNull ; \
    Some (Mac U16), PropagateNull ; \
    Some (Mac U16), PropagateNull ] \
    (add_missing_types \
      [ ConvTo (Mac Float), PropagateNull ; \
        AnyType, PropagateNull ; \
        AnyType, PropagateNull ] \
      [ const (Mac Float) (VFloat 1.) ; \
        const (Mac U16) (VU16 (Uint16.of_int 42)) ; \
        const (Mac U8) (VU8 (Uint8.of_int 42)) ])

  [ Some (Mac Float), PropagateNull ; \
    Some (Mac U16), PropagateNull ; \
    Some (Mac U16), PropagateNull ] \
    (add_missing_types \
      [ ConvTo (Mac Float), PropagateNull ; \
        AnyType, PropagateNull ; \
        AnyType, PropagateNull ] \
      [ const (Mac Float) (VFloat 1.) ; \
        const (Mac U8) (VU8 (Uint8.of_int 42)) ; \
        const (Mac U16) (VU16 (Uint16.of_int 42)) ])

  [ None, PropagateNull ; \
    Some (Mac Float), PropagateNull ] \
    (add_missing_types \
      [ NoConv, PropagateNull ; \
        ConvTo (Mac Float), PropagateNull ] \
      [ const (Mac Float) (VFloat 1.) ; \
        const (Mac Float) (VFloat 1.) ])
 *)

(* When we combine nullable arguments we want to shortcut as much as
 * possible and avoid evaluating any of them if one is null. Here we will just
 * evaluate them in order until one is found to be nullable and null, or until
 * we evaluated them all, and then only we call the function.
 * TODO: ideally we'd like to evaluate the nullable arguments first. *)
and emit_function
      (* Set to true if [impl] already returns an optional value: *)
      ?(impl_return_nullable=false)
      (* Nullability of the result: *)
      ~nullable
      ?(args_as=Arg) ~env ~opc impl arg_typs es oc vt_specs_opt =
  let arg_typs = add_missing_types arg_typs es in
  let num_args =
    List.fold_left2 (fun i e (arg_typ, null_prop) ->
      let var_name = "x"^ string_of_int i ^"_" |>
                     RamenOCamlCompiler.make_valid_ocaml_identifier in
      if e.E.typ.DT.nullable then (
        match null_prop with
        | PropagateNull ->
            Printf.fprintf oc
              "(match %a with Null as n_ -> n_ | NotNull %s -> "
              (conv_to ~env ~context:Finalize ~opc arg_typ) e
              var_name
        | PassNull | PassAsNull ->
            Printf.fprintf oc "(let %s = %a in\n\t"
              var_name
              (conv_to ~env ~context:Finalize ~opc arg_typ) e
      ) else ( (* [ e ] not nullable *)
        match null_prop with
        | PropagateNull | PassNull ->
            Printf.fprintf oc "(let %s = %a in\n\t"
              var_name
              (conv_to ~env ~context:Finalize ~opc arg_typ) e
        | PassAsNull ->
            (* Pass as a nullable: *)
            Printf.fprintf oc "(let %s = NotNull (%a) in\n\t"
              var_name
              (conv_to ~env ~context:Finalize ~opc arg_typ) e
      ) ;
      i + 1
    ) 0 es arg_typs
  in
  let conv_nullable, close_parentheses =
    match impl_return_nullable, nullable with
    | false, true -> "NotNull (", ")"
    | false, false -> "", ""
    | true, true -> "", ""
    | true, false ->
        (* If impl_return_nullable but nullable is false, it means we must
         * force that optional result to make it not-nullable. *)
        "Nullable.get (", ")" in
  Printf.fprintf oc "%s%s"
    conv_nullable impl ;
  for i = 0 to num_args - 1 do
    Printf.fprintf oc "%s"
      (match args_as with Array n when i = n -> " [| "
                        | Array n when i > n -> ";"
                        | Tuple n when i = n -> " ("
                        | Tuple n when i > n -> ", "
                        | _ -> " ") ;
    Printf.fprintf oc "x%d_" i
  done ;
  (* If the list of args do not extends up to the specified Array/Tuple start
   * we must call with an empty array/tuple: *)
  (match args_as with
  | Array n when n >= num_args -> Printf.fprintf oc " [| "
  | Tuple n when n >= num_args -> Printf.fprintf oc " ("
  | _ -> ()) ;
  Printf.fprintf oc "%s"
    (match args_as with Arg -> "" | Array _ -> " |] " | Tuple _ -> ") ") ;
  (* variadic arguments [ves] are passed as a last argument to impl, as an array *)
  Option.may (fun (vt, ves) ->
      (* TODO: handle NULLability *)
      List.print ~first:" [| " ~last:" |]" ~sep:"; "
                 (conv_to ~env ~context:Finalize ~opc (Some vt)) oc ves)
    vt_specs_opt ;
  for _i = 1 to num_args do Printf.fprintf oc ")" done ;
  String.print oc close_parentheses

and emit_functionN ?args_as ?impl_return_nullable ~nullable
                   ~env ~opc impl arg_typs oc es =
  emit_function ?args_as ?impl_return_nullable ~nullable
                ~env ~opc impl arg_typs es oc None

and emit_functionNv ?impl_return_nullable ~nullable
                    ~env ~opc impl arg_typs es vt oc ves =
  emit_function ?impl_return_nullable ~nullable
                ~env ~opc impl arg_typs es oc (Some (vt, ves))

let rec emit_sersize_of_var indent typ oc var =
  let p fmt = emit oc indent fmt in
  if typ.DT.nullable then (
    p "(" ;
    p "  match %s with Null -> 0" var ;
    p "  | NotNull %s ->" var ;
    emit_sersize_of_var (indent + 3) { typ with nullable = false } oc var ;
    p ")"
  ) else (
    let nullmask_words =
      match typ.DT.vtyp with
      | Lst t ->
          if t.DT.nullable then -1 (* special *) else 0
      | vtyp ->
          DessserRamenRingBuffer.NullMaskWidth.words_of_type vtyp in
    let nullmask_sz = nullmask_words * DessserRamenRingBuffer.word_size in
    let emit_for_record kts =
      let item_var k = "item_"^ k |>
                       RamenOCamlCompiler.make_valid_ocaml_identifier in
      p "(" ;
      p "  let %a = %s in"
        (array_print_as_tuple (fun oc (k, _) ->
          String.print oc (item_var k))) kts
        var ;
      (* sersize does not depend on the order of fields so we can consider
       * them in definition order: *)
      Array.iter (fun (k, t) ->
        emit_sersize_of_var (indent + 1) t oc (item_var k);
        p "    +"
      ) kts ;
      p "  %d" nullmask_sz ;
      p ")"
    in
    match typ.DT.vtyp with
    | Tup ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_for_record kts
    | Rec kts ->
        emit_for_record kts
    | Vec (d, t) ->
        for i = 0 to d-1 do
          let item_var = var ^"_"^ string_of_int i in
          p "(" ;
          p "  let %s = %s.(%d) in" item_var var i ;
          emit_sersize_of_var (indent + 1) t oc item_var ;
          p ") +"
        done ;
        p "%d" nullmask_sz
    | Mac String ->
        p "(RingBufLib.sersize_of_string %s)" var
    | Usr { name = "Ip" ; _ } ->
        p "(RingBufLib.sersize_of_ip %s)" var
    | Usr { name = "Cidr" ; _ } ->
        p "(RingBufLib.sersize_of_cidr %s)" var
    | Lst t ->
        (* So var is the name of an array of some values of type t, which can
         * be a constructed type which sersize can't be known statically.
         * So at first sight we have to generate code that will iter through
         * the values and for each, know or compute its size, etc. This is
         * what we do here, but in cases where t has a well known sersize we
         * could generate much faster code of course: *)
        p "(" ;
        p "  Array.fold_left (fun s_ v_ -> s_ +" ;
        emit_sersize_of_var (indent + 2) t oc "v_" ;
        p "  ) (%d%s) %s"
          (* start from the size prefix and nullmask: *)
          RingBufLib.sersize_of_u32
          (if nullmask_words = 0 then "" else
            " + DessserRamenRingBuffer.round_up_const_bits (\
                  8 (* nullmask size *) + Array.length "^ var ^")")
          var ;
        p ")"
    | _ ->
        assert (nullmask_words = 0) ;
        p "%a" emit_sersize_of_fixsz_typ typ.DT.vtyp
  )

(* Given the name of a variable with the fieldmask, emit a given code for
 * every values to be sent, in serialization order.
 * We suppose that the output value is in out_var.
 * Each code block returns a value that is finally returned into out_var.
 *
 * Note on private fields:
 * Private fields are any record fields (top level in the select clause
 * or in any real record) which name start with an underscore ('_').
 * They are normal fields for the parser, the typer and most of the generated
 * code. In particular, they are part of the internal record representation.
 * They are skipped over on fieldmasks and also when serializing.
 * When unserializing the missing values are replaced by any cheap value (so
 * that no distinct type for records with or without private fields are
 * needed).
 *
 * The purpose of private field is to be able to have convenient scratch
 * variables to factorize some intermediary computation, for free.
 *
 * Note that this is related to shadowed fields: those are also parsed/typed
 * normally, but disappear on serialization (TODO). *)

let rec emit_for_serialized_fields
          indent typ copy skip fm_var val_var oc out_var =
  let p fmt = emit oc indent fmt in
  if is_scalar typ.DT.vtyp then (
    p "let %s =" out_var ;
    p "  if %s = DessserMasks.Copy then (" fm_var ;
    copy (indent + 2) oc (val_var, typ) ;
    p "  ) else (" ;
    p "    assert (%s = DessserMasks.Skip) ;" fm_var ;
    skip (indent + 2) oc (val_var, typ) ;
    p "  ) in"
  ) else (
    let emit_for_record kts =
      p "let %s =" out_var ;
      p "  match %s with" fm_var ;
      p "  | DessserMasks.SetNull | Replace _ | Insert _ -> assert false" ;
      p "  | DessserMasks.Copy ->" ;
      copy (indent + 3) oc (val_var, typ) ;
      p "  | DessserMasks.Skip ->" ;
      skip (indent + 3) oc (val_var, typ) ;
      p "  | DessserMasks.Recurse _ ->" ;
      (* Destructure the tuple, propagating Nulls: *)
      let item_var k = Printf.sprintf "tup_" ^ k |>
                       RamenOCamlCompiler.make_valid_ocaml_identifier in
      if typ.DT.nullable then (
        p "      (match %s with " val_var ;
        p "      | Null ->" ;
        p "        %s" out_var ;
        p "      | NotNull (%a) ->"
          (Array.print ~first:"" ~last:"" ~sep:", " (fun oc (k, _) ->
            String.print oc (item_var k))) kts ;
      ) else (
        p "      (let %a ="
          (Array.print ~first:"" ~last:"" ~sep:", " (fun oc (k, _) ->
            String.print oc (item_var k))) kts ;
        p "        %s in" val_var ;
      ) ;
      let ser = RingBufLib.ser_order kts in
      Array.iteri (fun i (k, t) ->
        assert (i < num_all_fields) ;
        let fm_var' = fm_var ^"_"^ string_of_int i in
        p "      let %s = DessserOCamlBackEndHelpers.mask_get %s %d in"
          fm_var' fm_var i ;
        emit_for_serialized_fields
          (indent + 4) t copy skip fm_var' (item_var k) oc out_var
      ) ser ;
      p "      %s) in" out_var
    in
    match typ.vtyp with
    | Vec (_, t) | Lst t ->
        p "let %s =" out_var ;
        p "  match %s with" fm_var ;
        p "  | DessserMasks.SetNull | Replace _ | Insert _ -> assert false" ;
        p "  | DessserMasks.Copy ->" ;
        copy (indent + 3) oc (val_var, typ) ;
        p "  | DessserMasks.Skip ->" ;
        skip (indent + 3) oc (val_var, typ) ;
        p "  | DessserMasks.Recurse fm_ ->" ;
        p "      Array.fold_lefti (fun %s i_ fm_ ->" out_var ;
        (* When we want to serialize subfields of a value that is null, we
         * have to serialize each subfield as null: *)
        let indent =
          if typ.DT.nullable then (
            p "        match %s with Null ->" val_var ;
            skip (indent + 5) oc (val_var, typ) ;
            p "        | NotNull %s ->" val_var ;
            indent + 5
          ) else indent + 3 in
        let p fmt = emit oc indent fmt in
        if t.DT.nullable then (
          (* For arrays but especially lists of nullable elements, make it
           * possible to fetch beyond the boundaries of the list: *)
          p "  let x_ =" ;
          p "    try %s.(i_)" val_var ;
          p "    with Invalid_argument _ -> Null in"
        ) else (
          p "  let x_ = %s.(i_) in" val_var
        ) ;
        emit_for_serialized_fields
          (indent + 1) t copy skip "fm_" "x_" oc out_var ;
        p "  %s" out_var ;
        p ") %s fm_ in" out_var
    | Tup ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_for_record kts
    | Rec kts ->
        emit_for_record kts
    | _ -> assert false (* no other non-scalar types *)
  )

let emit_for_serialized_fields_of_output
      indent typ copy skip fm_var oc out_var =
  let p fmt = emit oc indent fmt in
  List.iteri (fun i ft ->
    if not (N.is_private ft.name) then (
      p "(* Field %a *)" N.field_print ft.RamenTuple.name ;
      let val_var = id_of_field_typ ~tuple:Out ft in
      let fm_var' = fm_var ^"_"^ string_of_int i in
      p "let %s = DessserOCamlBackEndHelpers.mask_get %s %d in"
        fm_var' fm_var i ;
      emit_for_serialized_fields indent ft.typ copy skip fm_var' val_var
                                 oc out_var
    )
  ) typ

(* Same as the above [emit_for_serialized_fields] but for when we do not know
 * the actual value, just its type. *)
let rec emit_for_serialized_fields_no_value
        indent typ copy skip fm_var oc out_var =
  let p fmt = emit oc indent fmt in
  if is_scalar typ.DT.vtyp then (
    p "let %s =" out_var ;
    p "  if %s = DessserMasks.Copy then (" fm_var ;
    copy (indent + 2) oc typ ;
    p "  ) else (" ;
    p "    assert (%s = DessserMasks.Skip) ;" fm_var ;
    skip (indent + 2) oc typ ;
    p "  ) in"
  ) else (
    let emit_for_record kts =
      p "let %s =" out_var ;
      p "  match %s with" fm_var ;
      p "  | DessserMasks.SetNull | Replace _ | Insert _ -> assert false" ;
      p "  | DessserMasks.Copy ->" ;
      copy (indent + 3) oc typ ;
      p "  | DessserMasks.Skip ->" ;
      skip (indent + 3) oc typ ;
      p "  | DessserMasks.Recurse fm_ ->" ;
      let ser = RingBufLib.ser_order kts in
      array_print_i ~first:"" ~last:"" ~sep:"\n" (fun i oc (_, t) ->
        assert (i < num_all_fields) ;
        p "    let fm_ = DessserOCamlBackEndHelpers.mask_get fm_ %d in" i ;
        emit_for_serialized_fields_no_value
          (indent + 3) t copy skip "fm_" oc out_var
      ) oc ser ;
      p "      %s in" out_var
    in
    match typ.vtyp with
    | Vec (_, t) | Lst t ->
        p "let %s =" out_var ;
        p "  match %s with" fm_var ;
        p "  | DessserMasks.SetNull | Replace _ | Insert _ -> assert false" ;
        p "  | DessserMasks.Copy ->" ;
        copy (indent + 3) oc typ ;
        p "  | DessserMasks.Skip ->" ;
        skip (indent + 3) oc typ ;
        p "  | DessserMasks.Recurse fm_ ->" ;
        p "      Array.fold_lefti (fun %s i_ fm_ ->" out_var ;
        emit_for_serialized_fields_no_value
          (indent + 4) t copy skip "fm_" oc out_var ;
        p "        %s" out_var ;
        p "      ) %s fm_ in" out_var
    | Tup ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_for_record kts
    | Rec kts ->
        emit_for_record kts
    | vtyp ->
        !logger.error "Unknown non-scalar type: %a"
          DT.print_value_type vtyp ;
        assert false (* no other non-scalar types *)
  )

(* The actual nullmask size will depend on the fieldmask which is known
 * only at runtime: *)
let emit_sersize_of_tuple indent name oc typ =
  let p fmt = emit oc indent fmt in
  (* Like for serialize_tuple, we receive first the fieldmask and then the
   * actual tuple, so we can compute the nullmask in advance: *)
  p "(* Compute the serialized size of a tuple of type:" ;
  p "     %a" RamenTuple.print_typ typ ;
  p "*)" ;
  p "let %s fieldmask_ %a =" name (emit_tuple Out) typ ;
  (* Assume all fields are selected when computing the nullmask width: *)
  let nullmask_words =
    let mn = RamenTuple.to_record typ in
    DessserRamenRingBuffer.NullMaskWidth.words_of_type mn.DT.vtyp in
  let nullmask_bytes = DessserRamenRingBuffer.word_size * nullmask_words in
  p "  let sz_ = %d in" nullmask_bytes ;
  let copy indent oc (out_var, typ) =
    emit oc indent "sz_ +" ;
    emit_sersize_of_var (indent + 1) typ oc out_var
  and skip indent oc _ = emit oc indent "sz_" in
  emit_for_serialized_fields_of_output
    (indent + 2) typ copy skip "fieldmask_" oc "sz_" ;
  p "  sz_\n\n"

(* The function that will serialize the fields of the tuple at the given
 * addresses. The first argument is the recursive fieldmask of the fields
 * that must actually be sent (depth first order dictates the order of the
 * bits in the nullmask). Before receiving the next arguments the nullmask
 * size is computed (that's a bit expensive).
 * Next arguments are the tx, the offset and the actual value. We need
 * an offset because of record headers.
 * Everything else (allocating on the RB and writing the record size) is
 * independent of the tuple type and is handled in the library.
 * The generated function returns the final offset so that the caller
 * can check for overflow.
 *
 * Format:
 * First comes the header (written by the caller, not our concern here)
 * Then comes the nullmask for the toplevel "structure", with one bit per
 * nullable value that will be copied.
 * Then the values.
 * For list values, we start with the number of elements.
 * Then, for lists, vectors and tuples we have a small local nullmask. *)

(* Emits the code to serialize a value.
 * [start_offs_var] is where the nullmask can be found, while [offs_var] is the
 * current offset.
 * Outputs the new current offset and null bit offset: *)
let rec emit_serialize_value
    indent start_offs_var offs_var nulli_var val_var oc typ =
  let p fmt = emit oc indent fmt in
  if typ.DT.nullable then (
    (* Write either nothing (since the nullmask is initialized with 0) or
     * the nullmask bit and the value *)
    p "(match %s with" val_var ;
    p "| Null -> %s, %s + 1" offs_var nulli_var ;
    p "| NotNull %s ->" val_var ;
    if verbose_serialization then
      p "!logger.debug \"Set nullmask bit %%d\" %s ;" nulli_var ;
    p "    RingBuf.set_bit tx_ %s %s ;" start_offs_var nulli_var ;
    p "    let offs_, nulli_ =" ;
    emit_serialize_value (indent + 3) start_offs_var offs_var "nulli_"
                         val_var oc { typ with nullable = false} ;
    p "      in" ;
    p "     offs_, nulli_ + 1)"
  ) else (
    let emit_write_array indent _start_offs_var offs_var dim_var t =
      let p fmt = emit oc indent fmt in
      p "(" ;
      p "  let start_arr_ = %s in" offs_var ;
      if t.DT.nullable then (
        p "  let nullmask_bytes_ = DessserRamenRingBuffer.bytes_of_const_bits %s in"
          dim_var ;
        p "  let nullmask_words_ = \
               nullmask_bytes_ + 1 |> DessserRamenRingBuffer.words_of_const_bytes in" ;
        if verbose_serialization then
          p "  !logger.debug \"Serializing an array of size %%d at offset %%d \
                  with %%d words of nullmask\" %s %s nullmask_words_ ;"
            dim_var offs_var ;
        (* Also zero the length for faster memset: *)
        p "  RingBuf.zero_bytes tx_ start_arr_ (1 + nullmask_bytes_) ;" ;
        p "  RingBuf.write_u8 tx_ start_arr_ (Uint8.of_int nullmask_words_) ;" ;
        p "  let offs_ = start_arr_ + %d * nullmask_words_ in"
          DessserRamenRingBuffer.word_size
      ) else (
        if verbose_serialization then
          p "  !logger.debug \"Serializing an array of size %%d at offset %%d\" \
                  %s %s ;" dim_var offs_var ;
        p "  let offs_ = start_arr_ in"
      ) ;
      p "  let offs_, _ =" ;
      p "    Array.fold_left (fun (offs_, nulli_) v_ ->" ;
      emit_serialize_value (indent + 3) "start_arr_" "offs_" "nulli_" "v_" oc t ;
      p "    ) (offs_, 8) %s in" val_var ;
      p "  offs_, %s" nulli_var ;
      p ")"
    and emit_write_record indent _start_offs_var offs_var kts =
      let p fmt = emit oc indent fmt in
      p "(" ;
      let has_nullmask, nullmask_bits =
        DessserRamenRingBuffer.NullMaskWidth.rec_bits kts in
      let nullmask_bytes = DessserRamenRingBuffer.bytes_of_const_bits nullmask_bits in
      let nullmask_words =
        if not has_nullmask then 0 else
          8 + nullmask_bits |> DessserRamenRingBuffer.words_of_const_bits in
      if verbose_serialization then
        p "  !logger.debug \"Serializing a tuple of %d elements at offset %%d (nullmask words=%d, %a)\" %s ;"
          (Array.length kts)
          nullmask_words
          (Array.print (pair_print String.print DT.print_maybe_nullable)) kts
          offs_var ;
      let item_var k = val_var ^"_"^ k |>
                       RamenOCamlCompiler.make_valid_ocaml_identifier in
      p "  let %a = %s in"
        (array_print_as_tuple (fun oc (k, _) ->
          String.print oc (item_var k))) kts
        val_var ;
      p "  let start_tup_ = %s in" offs_var ;
      if nullmask_words > 0 then (
        (* Also zero the length for faster memset: *)
        if nullmask_bytes > 0 then
          p "RingBuf.zero_bytes tx_ start_tup_ %d ;" (1 + nullmask_bytes) ;
        p "  RingBuf.write_u8 tx_ start_tup_ (Uint8.of_int %d) ;" nullmask_words ;
        p "  let offs_ = start_tup_ + %d (* nullmask *) in"
          (DessserRamenRingBuffer.word_size * nullmask_words) ;
      ) else (
        p "  let offs_ = start_tup_ in"
      ) ;
      (* We must obviously serialize in serialization order: *)
      let ser = RingBufLib.ser_order kts in
      Array.iteri (fun i (k, t) ->
        p "  let offs_, _ =" ;
        let nulli_var =
          if has_nullmask then (
            p "  let nulli_ = %d in" (8 + i) ;
            "nulli_"
          ) else "nulli_unused_no_nullmask" in
        (* else the nulli_ variable should not be used anywhere! *)
        emit_serialize_value (indent + 3) "start_tup_" "offs_" nulli_var
                             (item_var k) oc t ;
        p "  in"
      ) ser ;
      p "offs_, %s" nulli_var ;
      p ")" ;
    in
    match typ.DT.vtyp with
    (* Constructed types: *)
    | Tup ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_write_record indent start_offs_var offs_var kts

    | Rec kts ->
        emit_write_record indent start_offs_var offs_var kts

    | Vec (d, t) ->
        emit_write_array indent start_offs_var offs_var (string_of_int d) t

    | Lst t ->
        p "let d_ = Array.length %s in" val_var ;
        p "RingBuf.write_u32 tx_ %s (Uint32.of_int d_) ;" offs_var ;
        p "let offs_ = %s + RingBufLib.sersize_of_u32 in" offs_var ;
        emit_write_array indent start_offs_var "offs_" "d_" t

    (* Scalar types: *)
    | t ->
        if verbose_serialization then
          p "!logger.debug \"Serializing %s (%%s) at offset %%d\" (dump %s) %s ;" val_var val_var offs_var ;
        p "RingBuf.write_%s tx_ %s %s ;" (id_of_typ t) offs_var val_var ;
        p "%s +" offs_var ;
        emit_sersize_of_var
          (indent + 1) { typ with nullable = false } oc val_var ;
        p "  , %s" nulli_var
  )

(* Emit a function called [name] and taking as parameter a fieldmask, then a tx,
 * a starting offset and a type. Useful when the fieldmask is not known until run
 * time. *)
let emit_serialize_function indent name oc typ =
  let p fmt = emit oc indent fmt in
  p "let %s fieldmask_ tx_ start_offs_ %a =" name (emit_tuple Out) typ ;
  let nullmask_words =
    let mn = RamenTuple.to_record typ in
    DessserRamenRingBuffer.NullMaskWidth.words_of_type mn.DT.vtyp in
  let has_nullmask = nullmask_words > 0 in
  let nullmask_bytes = DessserRamenRingBuffer.word_size * nullmask_words in
  if verbose_serialization then
    p "    !logger.debug \"Serialize a tuple, nullmask_words:%d\" ;"
      nullmask_words ;
  (* Callbacks [copy] and [skip] have to return the offset and null index
   * but we have several offsets and several null index (when copying full
   * compund types) ; we therefore enforce the rule that those variables
   * are always called "offs_" and "nulli_". *)
  p "    let offs_ = start_offs_ + %d" nullmask_bytes ;
  p "    and nulli_ = 8 (* skip nullmask length *) in" ;
  (*
   * Write a full value, updating offs_var and nulli_var:
   * [start_var]: where we write the value (and where the nullmask is, for
   *              values with a nullmask).
   *)
  (* Start by zeroing the nullmask *)
  if has_nullmask then (
    (* Also zero the length for faster memset: *)
    p "    RingBuf.zero_bytes tx_ start_offs_ %d ;"
      nullmask_bytes ;
    p "    RingBuf.write_u8 tx_ start_offs_ (Uint8.of_int %d) ;"
      nullmask_words
  ) ;
  (* Every nullable values found in the fieldmask will have its nullbit in the
   * global nullmask at start_offs: *)
  let copy indent oc (out_var, typ) =
    emit_serialize_value indent "start_offs_" "offs_" "nulli_" out_var oc typ
  and skip indent oc _ =
    (* We must return offs and null_idx as [copy] does (unchanged here,
     * since the field is not serialized). *)
    emit oc indent "offs_, nulli_"
  in
  emit_for_serialized_fields_of_output
    (indent + 2) typ copy skip "fieldmask_" oc "(offs_, nulli_)" ;
  p "  offs_\n\n"

let rec emit_indent oc n =
  if n > 0 then (
    Printf.fprintf oc "\t" ;
    emit_indent oc (n-1)
  )

(* Emit a function that, given an array of strings (corresponding to a line of
 * CSV, with one field for each non-private field of [typ], properly unquoted
 * and unescaped) will return the tuple defined by [typ] or raises some
 * exception *)
let emit_tuple_of_strings indent name csv_null oc typ =
  let p fmt = emit oc indent fmt in
  let emit_is_null fins str_var offs_var oc =
    Printf.fprintf oc
      "if string_sub_eq %s %s %S 0 %d && \
          string_is_term %a %s (%s + %d) then \
        true, %s + %d else false, %s"
      str_var offs_var csv_null (String.length csv_null)
      (List.print char_print_quoted) fins
      str_var offs_var (String.length csv_null)
      offs_var (String.length csv_null) offs_var in
  p "let %s strs_ =" name ;
  List.iteri (fun i ft ->
    if N.is_private ft.RamenTuple.name then (
      p "  let val_%d = %s in" i (dummy_var_name ft.name)
    ) else (
      p "  let val_%d, strs_ =" i ;
      p "    let s_ =" ;
      p "      try List.hd strs_" ;
      p "      with Failure _ ->" ;
      p "        Printf.sprintf \"Expected more values than %d\" |>" i ;
      p "        failwith in" ;
      p "    (try check_parse_all s_ (" ;
      emit_value_of_string 3 ft.typ "s_" "0" emit_is_null [] false oc ;
      p "    ) with exn -> (" ;
      p "      !logger.error \"Cannot parse field #%d (%s): %%S: %%s\""
        (i+1) (ft.name : N.field :> string) ;
      p "        s_ (Printexc.to_string exn) ;" ;
      p "      raise exn)), List.tl strs_ in"
    )
  ) typ ;
  p "  %a\n\n"
    (list_print_as_tuple_i (fun oc i _ ->
      Printf.fprintf oc "val_%d" i)) typ

let emit_time_of_tuple name opc =
  let open RamenEventTime in
  Printf.fprintf opc.code "let %s %a =\n\t"
    name
    (emit_tuple Out) opc.typ ;
  (match opc.event_time with
  | None -> String.print opc.code "Null"
  | Some _ -> Printf.fprintf opc.code "NotNull (%a)" emit_event_time opc) ;
  String.print opc.code "\n\n"

let emit_factors_of_tuple name func_op oc =
  let typ = O.out_type_of_operation func_op in
  let factors = O.factors_of_operation func_op in
  Printf.fprintf oc "let %s %a = [|\n"
    name
    (emit_tuple Out) typ ;
  List.iter (fun factor ->
    let typ =
      (List.find (fun t -> t.RamenTuple.name = factor) typ).typ in
    Printf.fprintf oc "\t%S, %a %s ;\n"
      (factor :> string)
      emit_value typ
      (id_of_field_name ~tuple:Out factor)
  ) factors ;
  (* TODO *)
  String.print oc "|]\n\n"

let print_path oc path =
  List.print (Tuple2.print DT.print_maybe_nullable Int.print) oc path

let rec emit_extractor path var oc =
  let deconstruct rest i n =
    let patmat =
      List.init n (fun j -> if i = j then "v_" else "_") |>
      String.join "," in
    let var = "(let ("^ patmat ^") = "^ var ^" in v_)" in
    emit_extractor rest var oc in
  match path with
  | (mn, 0) :: [] ->
      Printf.fprintf oc "%a %s" emit_value mn var
  | (DT.{ vtyp = TVec _ ; nullable = false }, i) :: rest ->
      let var = var ^".("^ string_of_int i ^")" in
      emit_extractor rest var oc
  | (DT.{ vtyp = TTup mns ; nullable = false }, i) :: rest ->
      deconstruct rest i (Array.length mns)
  | (DT.{ vtyp = TRec mns ; nullable = false }, i) :: rest ->
      deconstruct rest i (Array.length mns)
  | (DT.{ nullable = true ; vtyp }, i) :: rest ->
      Printf.fprintf oc "(match %s with Null -> VNull | NotNull v_ -> %t)"
        var
        (emit_extractor ((DT.{ nullable = false ; vtyp }, i) :: rest) "v_")
  | _ ->
      !logger.error "Cannot emit_extractor for path %a"
        print_path path ;
      assert false

let emit_scalar_extractors name func oc =
  (* We need the private types in [mn] because that's the type we actually
   * receive for the output tuple, yet obviously all private fields must be
   * skipped over: *)
  let mn = O.out_record_of_operation ~with_private:true func.VSI.operation in
  (* Note: we need to provide the signature or we might end up with weak
   * polymorphic types: *)
  Printf.fprintf oc "let %s : (%a -> RamenTypes.value) array = [|\n"
    name
    otype_of_type mn ;
  (* Enumerate all scalar subfields in [mn] with the index [i] and access path
   * [path], and build a field extractor: *)
  O.iter_scalars_with_path mn (fun i path ->
    Printf.fprintf oc "  (* Field extractor #%d: *)\n" i ;
    Printf.fprintf oc "  (fun v_ -> %t) ;\n"
      (emit_extractor path "v_")
  ) ;
  String.print oc "|]\n\n"

(* Generate a data provider that reads blocks of bytes from a file: *)
let emit_read_file opc param_env env_env globals_env name specs =
  let env = param_env @ env_env @ globals_env
  and p fmt = emit opc.code 0 fmt
  in
  p "let %s field_of_params_ =" name ;
  p "  let unlink_ = %a in"
    (emit_expr ~env ~context:Finalize ~opc) specs.O.unlink ;
  p "  let tuples = [ [ \"param\" ], field_of_params_ ;" ;
  p "                 [ \"env\" ], Sys.getenv ] in" ;
  fail_with_context "file name expression" (fun () ->
    p "  let filename_ = subst_tuple_fields tuples %a"
      (emit_expr ~context:Finalize ~opc ~env) specs.fname) ;
  fail_with_context "file preprocessor expression" (fun () ->
    p "  and preprocessor_ = subst_tuple_fields tuples (%a)"
      (fun oc -> function
      | None -> String.print_quoted oc ""
      | Some pre -> emit_expr ~context:Finalize ~opc ~env oc pre)
        specs.preprocessor) ;
  p "  in" ;
  p "  CodeGenLib_IO.read_glob_file filename_ preprocessor_ unlink_\n\n"

(* Generate a data provider that reads blocks of bytes from a kafka topic: *)
let emit_read_kafka opc param_env env_env globals_env name specs =
  let env = param_env @ env_env @ globals_env
  and p fmt = emit opc.code 0 fmt in
  p "let %s field_of_params_ =" name ;
  p "  let tuples = [ [ \"param\" ], field_of_params_ ;" ;
  p "                 [ \"env\" ], Sys.getenv ] in" ;
  fail_with_context "options expression" (fun () ->
    p "  let topic_options_, consumer_options_ =" ;
    p "    List.partition (fun (n, _) -> String.starts_with n %S) "
      kafka_topic_option_prefix ;
    p "      %a in"
      (List.print (fun oc (n, e) ->
        Printf.fprintf oc
          "(subst_tuple_fields tuples %S, subst_tuple_fields tuples (%a))"
          n (emit_expr ~context:Finalize ~opc ~env) e))
        specs.O.options) ;
  p "  let topic_options_ =" ;
  p "    List.map (fun (n, v) -> String.lchop ~n:%d n, v) topic_options_ in"
    (String.length kafka_topic_option_prefix) ;
  p "  let consumer_ = Kafka.new_consumer consumer_options_ in" ;
  fail_with_context "topic expression" (fun () ->
    p "  let topic_ = subst_tuple_fields tuples (%a) in"
      (emit_expr ~context:Finalize ~opc ~env) specs.topic) ;
  p "  let topic_ = Kafka.new_topic consumer_ topic_ topic_options_ in" ;
  fail_with_context "partition expression" (fun () ->
    match specs.partitions with
    | None ->
        p "  let partitions_ = [||] in"
    | Some partitions ->
        let partitions_t = DT.Lst { vtyp = Mac I32 ; nullable = false } in
        if partitions.E.typ.DT.nullable then (
          p "  let partitions_ =" ;
          p "    match %a with Null -> [||] | NotNull p_ -> %t p_ in"
            (emit_expr ~context:Finalize ~opc ~env) partitions
            (conv_from_to ~nullable:false partitions.typ.vtyp partitions_t)
        ) else (
          p "  let partitions_ = %a in"
            (conv_to ~env ~context:Finalize ~opc (Some partitions_t))
              partitions
        ) ;
        p "  let partitions_ = Array.map Int32.to_int partitions_ |> Array.to_list in") ;
  p "  let partitions_ =" ;
  p "    if partitions_ <> [] then partitions_ else" ;
  p "      (Kafka.topic_metadata consumer_ topic_).topic_partitions in" ;
  fail_with_context "restart-offset expression" (fun () ->
    p "  let offset_ = %a in"
      (fun oc -> function
      | O.Beginning -> String.print oc "Kafka.offset_beginning"
      | O.OffsetFromEnd e ->
          let o = option_get "OffsetFromEnd" __LOC__ (E.int_of_const e) in
          if o = 0 then
            String.print oc "Kafka.offset_end"
          else
            Printf.fprintf oc "Kafka.offset_tail %d" o
      | O.SaveInState ->
          todo "SaveInState"
      | O.UseKafkaGroupCoordinator _ -> (* TODO: snapshot period *)
          String.print oc "Kafka.offset_stored")
        specs.restart_from) ;
  p "  CodeGenLib_IO.read_kafka_topic consumer_ topic_ partitions_ offset_\n\n"

(* In the special case of ReadExternal operation, another cmx is going to be
 * linked in, that will unserialize the tuple (with the idea that this other
 * code generation tool, Dessser, will eventually take over this whole file). *)
let emit_parse_external opc name format_name =
  let p fmt = emit opc.code 0 fmt in
  (* Having no textual parameters there is no parameters to be substituted so
   * [field_of_params] is ignored: *)
  p "let %s _field_of_params =" name ;
  (* This function must return the number of bytes parsed from input: *)
  p "  fun per_tuple_cb buffer start stop has_more ->" ;
  p "    match %s.read_tuple buffer start stop has_more with"
    opc.dessser_mod_name ;
  (* Catch only NotEnoughInput so that genuine encoding errors can crash the
   * worker before we have accumulated too many tuples in the read buffer: *)
  p "    | exception (DessserOCamlBackEndHelpers.NotEnoughInput _ as e) ->" ;
  p "        let what =" ;
  p "          Printf.sprintf \"While decoding %s @%%d..%%d%%s\"" format_name ;
  p "            start stop (if has_more then \"(...)\" else \".\") in" ;
  p "        print_exception ~what e ;" ;
  p "        0" ;
  p "    | tuple, read_sz ->" ;
  p "        per_tuple_cb tuple ;" ;
  p "        read_sz\n\n"

let emit_read opc name source_name parser_name =
  let p fmt = emit opc.code 0 fmt in
  (* The dynamic part comes from the unpredictable field list.
   * For each input line, we want to read all fields and build a tuple.
   * Then we want to write this tuple in some ring buffer.
   * We need to generate these functions:
   * - reading a CSV string into a tuple type (when nullable fields are option type)
   * - given such a tuple, return its serialized size
   * - given a pointer toward the ring buffer, serialize the tuple *)
  fail_with_context "tuple serialization size computation" (fun () ->
    emit_sersize_of_tuple 0 "sersize_of_tuple_" opc.code opc.typ) ;
  fail_with_context "event time extraction" (fun () ->
    emit_time_of_tuple "time_of_tuple_" opc) ;
  fail_with_context "tuple serialization" (fun () ->
    emit_serialize_function 0 "serialize_tuple_" opc.code opc.typ) ;
  fail_with_context "external reader function" (fun () ->
    p "let %s () =" name ;
    p "  CodeGenLib_Skeletons.read" ;
    p "    (%s field_of_params_)" source_name ;
    p "    (%s field_of_params_)" parser_name ;
    p "    sersize_of_tuple_ time_of_tuple_" ;
    p "    factors_of_tuple_ scalar_extractors_ serialize_tuple_" ;
    p "    orc_make_handler_ orc_write orc_close\n\n")

let emit_listen_on opc name net_addr port proto =
  let open RamenProtocols in
  let p fmt = emit opc.code 0 fmt in
  let tuple_typ = tuple_typ_of_proto proto in
  let collector = collector_of_proto proto in
  fail_with_context "serialization size computation" (fun () ->
    emit_sersize_of_tuple 0 "sersize_of_tuple_" opc.code tuple_typ) ;
  fail_with_context "event time extraction" (fun () ->
    emit_time_of_tuple "time_of_tuple_" opc) ;
  fail_with_context "tuple serialization" (fun () ->
    emit_serialize_function 0 "serialize_tuple_" opc.code tuple_typ) ;
  fail_with_context "listening function" (fun () ->
    p "let %s () =" name ;
    p "  CodeGenLib_Skeletons.listen_on" ;
    p "    (%s ~inet_addr:(Unix.inet_addr_of_string %S) ~port:%d)"
      collector
      (Unix.string_of_inet_addr net_addr) port ;
    p "    %S sersize_of_tuple_ time_of_tuple_"
      (string_of_proto proto) ;
    p "    factors_of_tuple_ scalar_extractors_" ;
    p "    serialize_tuple_" ;
    p "    orc_make_handler_ orc_write orc_close\n\n")

(* * All the following emit_* functions Return (value, offset).
 * [offs_var] is the name of the variable holding the current offset within the
 * tx, while [start_offs_var] holds the offset of the start of the current
 * compound structure where the current nullmask can be found. *)
let rec emit_deserialize_value
    indent tx_var start_offs_var offs_var nulli_var oc typ =
  let p fmt = emit oc indent fmt in
  let emit_for_array tx_var offs_var dim_var oc t =
    p "let arr_start_ = %s in" offs_var ;
    (* Arrays come with a nullmask only if the item is nullable: *)
    let has_nullmask = t.DT.nullable in
    if has_nullmask then (
      p "let nullmask_words_ = RingBuf.read_u8 %s %s |> Uint8.to_int in"
        tx_var offs_var ;
      p "let offs_arr_ = \
           ref (%s + %d * nullmask_words_) in"
        offs_var DessserRamenRingBuffer.word_size
    ) else (
      p "let offs_arr_ = ref %s in" offs_var
    ) ;
    p "let v_ = Array.init %s (fun bi_ ->" dim_var ;
    let nulli_var =
      if has_nullmask then (
        p "  let nulli_ = bi_ + 8 in" ;
        "nulli_"
      ) else (
        p "  ignore bi_ ;" ;
        "nulli_unused_no_nullmask"
      ) in
    p "  let v_, o_ =" ;
    p "    let offs_arr_ = !offs_arr_ in" ;
    emit_deserialize_value (indent + 1) tx_var "arr_start_" "offs_arr_"
                           nulli_var oc t ;
    p "    in" ;
    p "  offs_arr_ := o_ ; v_" ;
    p ") in v_, !offs_arr_"
  and emit_for_record kts =
    p "let tuple_start_ = %s in" offs_var ;
    (* Records/tuples always have a nullmask, maybe empty *)
    p "let nullmask_words_ = RingBuf.read_u8 %s %s |> Uint8.to_int in"
      tx_var offs_var ;
    p "let offs_tup_ = \
         %s + %d * nullmask_words_ in"
      offs_var DessserRamenRingBuffer.word_size ;
    let item_var k = "field_"^ k ^"_" |>
                     RamenOCamlCompiler.make_valid_ocaml_identifier in
    let ser = RingBufLib.ser_order kts in
    Array.iteri (fun i (k, t) ->
      p "let bi_ = %d in" (i + 8 (* nullmask length prefix *)) ;
      p "let %s, offs_tup_ =" (item_var k) ;
      emit_deserialize_value (indent + 1) tx_var "tuple_start_" "offs_tup_"
                             "bi_" oc t ;
      p "  in"
    ) ser ;
    p "%a, offs_tup_"
      (array_print_as_tuple (fun oc (k, _) ->
        String.print oc (item_var k))) kts
  in
  if typ.DT.nullable then (
    p "if RingBuf.get_bit %s %s %s then ("
      tx_var start_offs_var nulli_var ;
    p "  let v_, %s =" offs_var ;
    emit_deserialize_value (indent + 2) tx_var start_offs_var offs_var
                           nulli_var oc { typ with nullable = false } ;
    p "    in" ;
    p "  NotNull v_, %s" offs_var ;
    p ") else Null, %s" offs_var
  ) else (
    match typ.DT.vtyp with
    (* Constructed types are prefixed with a nullmask and then read item
     * by item: *)
    | Tup ts ->
        Array.mapi (fun i t -> string_of_int i, t) ts |>
        emit_for_record

    | Rec kts ->
        emit_for_record kts

    | Vec (d, t) ->
        emit_for_array tx_var offs_var (string_of_int d) oc t

    | Lst t ->
        (* List are like vectors but prefixed with the actual number of
         * elements: *)
        p "let d_, offs_lst_ =" ;
        p "  Uint32.to_int (RingBuf.read_u32 %s %s), %s + %d in"
          tx_var offs_var offs_var RingBufLib.sersize_of_u32 ;
        emit_for_array tx_var "offs_lst_" "d_" oc t
    (* Non constructed types: *)
    | _ ->
        p "RingBuf.read_%s %s %s, %s +"
          (id_of_typ typ.DT.vtyp) tx_var offs_var offs_var ;
        emit_sersize_of_not_null_scalar (indent + 1) tx_var offs_var oc
                                        typ.DT.vtyp
  )

(* We do not want to read the value from the RB each time it's used,
 * so extract a tuple from the ring buffer. *)
let emit_deserialize_function indent name ~opc typ =
  let p fmt = emit opc.code indent fmt in
  p "(* Deserialize a tuple of type:" ;
  p "     %a" RamenTuple.print_typ typ ;
  p "*)" ;
  p "let %s tx_ start_offs_ =" name ;
  let indent = indent + 1 in
  let p fmt = emit opc.code indent fmt in
  (* Given top level value is a record, it always has a nullmask: *)
  let has_nullmask = true in
  if has_nullmask then (
    p "let nullmask_words_ = RingBuf.read_u8 tx_ start_offs_ |> Uint8.to_int in" ;
    p "let offs_ = start_offs_ + %d * nullmask_words_ in"
      DessserRamenRingBuffer.word_size ;
    if verbose_serialization then
      p "!logger.debug \"Deserializing a tuple with %%d words of nullmask\" \
          nullmask_words_ ;"
  ) else (
    p "let offs_ = start_offs_ in" ;
    if verbose_serialization then
      p "!logger.debug \"Deserializing a tuple with no nullmask\" ;"
  ) ;
  List.fold_left (fun nulli ft ->
    let id = id_of_field_name ~tuple:In ft.RamenTuple.name in
    p "let bi_ = %d in" nulli ;
    p "let %s, offs_ =" id ;
    emit_deserialize_value (indent + 1) "tx_" "start_offs_" "offs_"
                           "bi_" opc.code ft.typ ;
    p "  in" ;
    nulli + (if ft.typ.DT.nullable then 1 else 0)
  ) (if has_nullmask then 8 else 0) typ |> ignore ;
  (* We want to output the tuple with fields ordered according to the
   * select clause specified order, not according to serialization order: *)
  p "%a\n\n" (emit_tuple In) typ

(* We know that somewhere in expr we have one or several generators.
 * First we transform the AST to move the generators to the root,
 * and insert "free variables" (named after the generator uniq_num)
 * where the generator used to stand. Once this is done, the AST
 * start with a chain of generator, and then an expression that is
 * free of generators. We want to emit:
 * (fun k -> gen1 (fun fv1 -> gen2 (fun fv2 -> ... -> genN (fun fvN ->
 *    k (expr ...)))))
 *)
let emit_generator user_fun ~env ~opc oc expr =
  let generators =
    E.fold_up (fun _ prev e ->
      match e.E.text with
      | Generator _ -> e :: prev
      | _ -> prev
    ) [] [] expr |>
    List.rev (* Inner generator first: *)
  in
  (* Now we start with all the generator. Inner generators are first,
   * so we can confidently call emit_expr on the arguments and if this uses a
   * free variable it should be defined already: *)
  let emit_gen_root oc e =
    match e.E.text with
    | Generator (Split _) ->
      Printf.fprintf oc "%a (fun %s -> "
        (emit_expr ~env ~context:Generator ~opc) e
        (freevar_name e)
    (* We have no other generators (yet) *)
    | _ -> assert false
  in
  List.iter (emit_gen_root oc) generators ;
  (* Finally, call user_func on the actual expression, where all generators will
   * be replaced by their free variable: *)
  Printf.fprintf oc "%s (%a)"
    user_fun
    (emit_expr ~env ~context:Finalize ~opc) expr ;
  List.iter (fun _ -> Printf.fprintf oc ")") generators

let emit_generate_tuples name in_typ out_typ ~opc selected_fields =
  let has_generator =
    List.exists (fun sf ->
      E.is_generator sf.O.expr)
      selected_fields in
  if not has_generator then
    Printf.fprintf opc.code "let %s f_ it_ ot_ = f_ ot_\n" name
  else (
    Printf.fprintf opc.code "let %s f_ (%a as it_) %a =\n"
      name
      (emit_tuple ~with_alias:true In) in_typ
      (emit_tuple ~with_alias:true Out) out_typ ;
    let env =
      add_tuple_environment In in_typ [] |>
      add_tuple_environment Out out_typ in
    (* Each generator is a functional receiving the continuation and calling it
     * as many times as there are values. *)
    let num_gens =
      List.fold_left (fun num_gens sf ->
          if not (E.is_generator sf.O.expr) then num_gens
          else (
            let ff_ = "ff_"^ string_of_int num_gens ^"_" in
            Printf.fprintf opc.code "%a(fun %s -> %a) (fun generated_%d_ ->\n"
              emit_indent (1 + num_gens)
              ff_
              (emit_generator ff_ ~env ~opc) sf.O.expr
              num_gens ;
            num_gens + 1)
        ) 0 selected_fields in
    (* Now we have all the generated values, actually call f_ on the tuple.
     * Note that the tuple must be in out_typ order: *)
    Printf.fprintf opc.code "%af_ (\n%a"
      emit_indent (1 + num_gens)
      emit_indent (2 + num_gens) ;
    let expr_of_field name =
      let sf = List.find (fun sf ->
                 sf.O.alias = name) selected_fields in
      sf.O.expr in
    let _ = List.fold_lefti (fun gi i ft ->
        if i > 0 then Printf.fprintf opc.code ",\n%a" emit_indent (2 + num_gens) ;
        match E.is_generator (expr_of_field ft.name) with
        | exception Not_found ->
          (* For star-imported fields: *)
          Printf.fprintf opc.code "%s"
            (id_of_field_name ft.name) ;
          gi
        | true ->
          Printf.fprintf opc.code "generated_%d_" gi ;
          gi + 1
        | false ->
          Printf.fprintf opc.code "%s"
            (id_of_field_name ~tuple:Out ft.name) ;
          gi
        ) 0 out_typ in
    for _ = 1 to num_gens do Printf.fprintf opc.code ")" done ;
    Printf.fprintf opc.code ")\n\n"
  )

let emit_state_update_for_expr ~env ~what ~opc expr =
  let titled = ref false in
  E.unpure_iter (fun _ e ->
    match e.text with
    | Stateful _ ->
        if not !titled then (
          titled := true ;
          Printf.fprintf opc.code "\t(* State Update for %s: *)\n" what) ;
        emit_expr ~env ~context:UpdateState ~opc opc.code e
    | _ -> ()
  ) expr

let emit_where ?(with_group=false) ~env name in_typ ~opc expr =
  Printf.fprintf opc.code "let %s global_ %a out_previous_ "
    name
    (emit_tuple ~with_alias:true In) in_typ ;
  let env =
    add_tuple_environment In in_typ env |>
    add_tuple_environment OutPrevious opc.typ in
  if with_group then Printf.fprintf opc.code "group_ " ;
  Printf.fprintf opc.code "=\n" ;
  (* Update the states used by this expression: *)
  emit_state_update_for_expr ~env ~opc ~what:"where clause" expr ;
  Printf.fprintf opc.code "\t%a\n\n"
    (emit_expr ~env ~context:Finalize ~opc) expr

let emit_field_selection
      (* If true, we update the env and finalize as few fields as
       * possible (only those required by commit_cond and update_states).
       * If false, we have the minimal tuple as an extra parameter, and
       * only have to build the final out_tuple (taking advantage of the
       * fields already computed in minimal_typ). And no need to update
       * states at all. *)
      ~build_minimal
      ~env name in_typ
      minimal_typ ~opc selected_fields =
  let field_in_minimal field_name =
    List.exists (fun ft ->
      ft.RamenTuple.name = field_name
    ) minimal_typ in
  let must_output_field field_name =
    not build_minimal || field_in_minimal field_name in
  Printf.fprintf opc.code "let %s %a out_previous_ group_ global_ "
    name
    (emit_tuple ~with_alias:true In) in_typ ;
  let env =
    add_tuple_environment In in_typ env |>
    add_tuple_environment OutPrevious opc.typ in
  let env =
    if not build_minimal then (
      Printf.fprintf opc.code "%a "
        (emit_tuple ~with_alias:true Out) minimal_typ ;
      add_tuple_environment Out minimal_typ env
    ) else env in
  Printf.fprintf opc.code "=\n" ;
  let p fmt = emit opc.code 0 fmt in
  (* Bind each expression to a variable in the order of the select clause
   * (aka. user order) so that previously bound variables can be used in
   * the following expressions: *)
  List.fold_left (fun env sf ->
    if must_output_field sf.O.alias then (
      if build_minimal then (
        (* Update the states as required for this field, just before
         * computing the field actual value. *)
        let what = (sf.O.alias :> string) in
        emit_state_update_for_expr ~env ~opc ~what sf.O.expr ;
      ) ;
      if not build_minimal && field_in_minimal sf.alias then (
        (* We already have this binding *)
        env
      ) else (
        p "  (* Output field %s of type %a *)"
          (sf.O.alias :> string)
          DT.print_maybe_nullable sf.expr.E.typ ;
        let var_name =
          id_of_field_name ~tuple:Out sf.O.alias in
        if E.is_generator sf.O.expr then (
          (* So that we have a single out_typ both before and after tuples generation *)
          p "  let %s = () in" var_name
        ) else (
          p "  let %s = %a in" var_name
            (emit_expr ~env ~context:Finalize ~opc)
              sf.O.expr) ;
        (* Make that field available in the environment for later users: *)
        (E.RecordField (Out, sf.alias), var_name) :: env
      )
    ) else env
  ) env selected_fields |> ignore ;
  (* Here the output tuple must be generated in the order specified by out_type
   * not in the order of the select clause. Easy enough, since every items
   * of the tuple is in a named variable: *)
  let is_selected name =
    List.exists (fun sf -> sf.O.alias = name) selected_fields in
  p " (" ;
  List.iteri (fun i ft ->
    if must_output_field ft.name then (
      let tuple =
        if is_selected ft.name then Out else In in
      p "  %s%s"
        (if i > 0 then ", " else "  ")
        (id_of_field_name ~tuple ft.name)
    ) else (
      p "  %s()"
        (if i > 0 then ", " else "  ")
    )
  ) opc.typ ;
  p "  )\n\n"

(* Fields that are part of the minimal tuple have had their states updated
 * while the minimal tuple was computed, but others have not. Let's do this
 * here: *)
let emit_update_states
      ~env name in_typ
      minimal_typ ~opc selected_fields =
  let field_in_minimal field_name =
    List.exists (fun ft ->
      ft.RamenTuple.name = field_name
    ) minimal_typ
  in
  Printf.fprintf opc.code "let %s %a out_previous_ group_ global_ %a =\n"
    name
    (emit_tuple ~with_alias:true In) in_typ
    (emit_tuple ~with_alias:true Out) minimal_typ ;
  let env =
    add_tuple_environment In in_typ env |>
    add_tuple_environment Out minimal_typ |>
    add_tuple_environment OutPrevious opc.typ in
  List.iter (fun sf ->
    if not (field_in_minimal sf.O.alias) then (
      (* Update the states as required for this field, just before
       * computing the field actual value. *)
      let what = (sf.O.alias :> string) in
      emit_state_update_for_expr ~env ~opc ~what sf.O.expr)
  ) selected_fields ;
  Printf.fprintf opc.code "\t()\n\n"

(* Similar to emit_field_selection but with less options, no concept of star and no
 * naming of the fields as the fields from out, since that's not the out tuple
 * we are constructing: *)
let emit_key_of_input name in_typ ~env ~opc exprs =
  Printf.fprintf opc.code "let %s %a =\n\t("
    name
    (emit_tuple ~with_alias:true In) in_typ ;
  let env = add_tuple_environment In in_typ env in
  List.iteri (fun i expr ->
      Printf.fprintf opc.code "%s\n\t\t%a"
        (if i > 0 then "," else "")
        (emit_expr ~env ~context:Finalize ~opc) expr ;
    ) exprs ;
  Printf.fprintf opc.code "\n\t)\n\n"

let fold_unpure_fun selected_fields
                    ?where ?commit_cond i f =
  let i =
    List.fold_left (fun i sf ->
      E.unpure_fold i f sf.O.expr
    ) i selected_fields in
  let i =
    Option.map_default (fun where ->
      E.unpure_fold i f where) i where in
  Option.map_default (fun where ->
    E.unpure_fold i f where) i commit_cond

let for_each_unpure_fun_my_lifespan lifespan selected_fields
                                    ?where ?commit_cond f =
  fold_unpure_fun selected_fields
                  ?where ?commit_cond ()
    (fun _ () e ->
      match e.E.text with
      | Stateful (l, _, _) when l = lifespan -> f e
      | _ -> ())

let fold_unpure_fun_my_lifespan lifespan selected_fields
                                ?where ?commit_cond i f =
  fold_unpure_fun selected_fields
                  ?where ?commit_cond i
    (fun _ i e ->
      match e.E.text with
      | Stateful (l, _, _) when l = lifespan -> f i e
      | _ -> i)

let otype_of_state e =
  let t = e.E.typ.vtyp |>
          IO.to_string otype_of_value_type in
  let print_expr_structure oc e =
    e.E.typ.vtyp |> (* nullable taken care of below *)
    IO.to_string otype_of_value_type |>
    String.print oc in
  let nullable = if e.typ.DT.nullable then " nullable" else "" in
  let print_expr_typ ~skip_null oc e =
    Printf.fprintf oc "%a%s"
      otype_of_value_type e.E.typ.vtyp
      (if e.typ.DT.nullable && not skip_null then " nullable" else "")
  in
  match e.text with
  (* previous tuples and count ; Note: we could get rid of this count if we
   * provided some context to those functions, such as the event count in
   * current window, for instance (ie. pass the full aggr record not just
   * the fields) *)
  | Stateful (_, _, SF2 (Lag, _, _))
  | Stateful (_, _, SF3 (MovingAvg, _, _, _)) ->
    t ^" CodeGenLib.Seasonal.t"^ nullable
  | Stateful (_, _, SF4s (MultiLinReg, _, _, _, _)) ->
    "("^ t ^" * float array) CodeGenLib.Seasonal.t"^ nullable
  | Stateful (_, _, SF4s (Remember, _, _, _, _)) ->
    "CodeGenLib.Remember.state"^ nullable
  | Stateful (_, _, SF1s (Distinct, es)) ->
    Printf.sprintf2 "%a CodeGenLib.Distinct.state%s"
      (list_print_as_product print_expr_structure) es
      nullable
  | Stateful (_, _, SF1 (AggrAvg, _)) -> "(int * (float * float))"^ nullable
  | Stateful (_, _, SF1 ((AggrFirst|AggrLast|AggrMin|AggrMax), _)) ->
    t ^" nullable"^ nullable
  | Stateful (_, _, Top { what ; _ }) ->
    Printf.sprintf2 "%a HeavyHitters.t%s"
      (list_print_as_product print_expr_structure) what
      nullable
  | Stateful (_, _, SF2 (ExpSmooth, _, _)) ->
      t ^" nullable"^ nullable
  | Stateful (_, _, SF4 (DampedHolt, _, _, _, _)) ->
      "(float * float)"^ nullable
  | Stateful (_, _, SF6 (DampedHoltWinter, _, _,_, _, _, _)) ->
      "(float * float * float array * int)"^ nullable
  | Stateful (_, n, SF4s (Largest _, _, _, e, es)) ->
    if es = [] then
      (* In that case we use a special internal counter as the order: *)
      Printf.sprintf2 "(%a, int) CodeGenLib.Largest.state%s"
        (print_expr_typ ~skip_null:n) e
        nullable
    else
      Printf.sprintf2 "(%a, %a) CodeGenLib.Largest.state%s"
        (print_expr_typ ~skip_null:n) e
        print_expr_structure (List.hd es)
        nullable
  | Stateful (_, n, SF2 (Sample, _, e)) ->
      Printf.sprintf2 "%a RamenSampling.reservoir%s"
        (print_expr_typ ~skip_null:n) e
        nullable
  | Stateful (_, _, SF2 (OneOutOf, _, _)) ->
      "CodeGenLib.OneOutOf.state" ^ nullable
  | Stateful (_, _, SF3 (OnceEvery _, _, _, _)) ->
      "CodeGenLib.OnceEvery.state" ^ nullable
  | Stateful (_, n, Past { what ; _ }) ->
      Printf.sprintf2 "%a CodeGenLib.Past.state%s"
        (print_expr_typ ~skip_null:n) what
        nullable
  | Stateful (_, n, SF1 (Group, e)) ->
      Printf.sprintf2 "%a list%s"
        (print_expr_typ ~skip_null:n) e
        nullable
  | Stateful (_, _, SF1 (Count, _)) ->
      "Uint32.t"^ nullable
  | Stateful (_, _, SF1 (AggrHistogram _, _)) ->
      "CodeGenLib.Histogram.state"^ nullable
  | Stateful (_, _, SF1 (AggrSum, _)) when e.E.typ.vtyp = Mac Float ->
      "Kahan.t"^ nullable
  | _ ->
      t ^ nullable

let emit_state_init name state_lifespan ~env other_params
      ?where ?commit_cond ~opc selected_fields =
  (* We must collect all unpure functions present in the selected_fields
   * and return a record with the proper types and init values for the required
   * states. *)
  let for_each_my_unpure_fun f =
    for_each_unpure_fun_my_lifespan
      state_lifespan selected_fields ?where ?commit_cond f
  and fold_my_unpure_fun i f =
    fold_unpure_fun_my_lifespan
      state_lifespan selected_fields ?where ?commit_cond i f
  in
  (* In the special case where we do not have any state at all, though, we
   * end up with an empty record, which is illegal in OCaml so we need to
   * specialize for this: *)
  let need_state =
    try
      for_each_my_unpure_fun (fun _ -> raise Exit) ;
      false
    with Exit -> true in
  if not need_state then (
    Printf.fprintf opc.code "type %s = unit\n" name ;
    Printf.fprintf opc.code "let %s%a = ()\n\n"
      name
      (List.print ~first:" " ~last:"" ~sep:" " String.print)
        other_params
  ) else (
    (* First emit the record type definition: *)
    Printf.fprintf opc.code "type %s = {\n" name ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf opc.code "\tmutable %s : %s (* %a *) ;\n"
          (name_of_state f)
          (otype_of_state f)
          DT.print_maybe_nullable f.E.typ ;
        (* Only used when skip_nulls: *)
        Printf.fprintf opc.code "\tmutable %s_empty_ : bool ;\n"
          (name_of_state f)
      ) ;
    Printf.fprintf opc.code "}\n\n" ;
    (* Then the initialization function proper: *)
    Printf.fprintf opc.code "let %s%a =\n"
      name
      (List.print ~first:" " ~last:"" ~sep:" " String.print)
        other_params ;
    let _state =
      fold_my_unpure_fun env (fun env f ->
        let n = name_of_state f in
        Printf.fprintf opc.code "\tlet %s = %a in\n"
          n
          (emit_expr ~context:InitState ~opc ~env) f ;
        (* Make this state available under that name for following exprs: *)
        (E.State f.uniq_num, n) :: env) in
    (* And now build the state record from all those fields: *)
    Printf.fprintf opc.code "\t{ " ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf opc.code "%s ; %s_empty_ = true ; "
          (name_of_state f) (name_of_state f)) ;
    Printf.fprintf opc.code "}\n"
  ) ;
  Printf.fprintf opc.code "\n"

(* Note: we need group_ in addition to out_tuple because the commit-when clause
 * might have its own stateful functions going on *)
let emit_when ~env name in_typ minimal_typ ~opc when_expr =
  Printf.fprintf opc.code "let %s %a out_previous_ group_ global_ %a =\n"
    name
    (emit_tuple ~with_alias:true In) in_typ
    (emit_tuple ~with_alias:true Out) minimal_typ ;
  let env =
    add_tuple_environment In in_typ env |>
    add_tuple_environment Out minimal_typ |>
    add_tuple_environment OutPrevious opc.typ in
  (* Update the states used by this expression: *)
  emit_state_update_for_expr ~env ~opc ~what:"commit clause" when_expr ;
  Printf.fprintf opc.code "\t%a\n\n"
    (emit_expr ~env ~context:Finalize ~opc) when_expr

(* Similarly but with different signatures: *)
let emit_cond0_in ~env name in_typ ?to_typ ~opc e =
  Printf.fprintf opc.code "let %s %a global_ =\n"
    name
    (emit_tuple ~with_alias:true In) in_typ ;
  let env =
    add_tuple_environment In in_typ env in
  (* Update the states used by this expression: *)
  emit_state_update_for_expr ~env ~opc ~what:"commit clause 0, in" e ;
  Printf.fprintf opc.code "\t%a\n\n"
    (conv_to ~env ~context:Finalize ~opc to_typ) e

let emit_cond0_out ~env name minimal_typ ?to_typ ~opc e =
  Printf.fprintf opc.code "let %s %a out_previous_ group_ global_ =\n"
    name
    (emit_tuple ~with_alias:true Out) minimal_typ ;
  let env =
    add_tuple_environment Out minimal_typ env |>
    add_tuple_environment OutPrevious opc.typ in
  (* Update the states used by this expression: *)
  emit_state_update_for_expr ~env ~opc ~what:"commit clause 0, out" e ;
  Printf.fprintf opc.code "\t%a\n\n"
    (conv_to ~env ~context:Finalize ~opc to_typ) e

let emit_sort_expr name in_typ ~opc es_opt =
  Printf.fprintf opc.code "let %s sort_count_ %a %a %a %a =\n"
    name
    (emit_tuple ~with_alias:true SortFirst) in_typ
    (emit_tuple ~with_alias:true In) in_typ
    (emit_tuple ~with_alias:true SortSmallest) in_typ
    (emit_tuple ~with_alias:true SortGreatest) in_typ ;
  let env =
    add_tuple_environment SortFirst in_typ [] |>
    add_tuple_environment In in_typ |>
    add_tuple_environment SortSmallest in_typ |>
    add_tuple_environment SortGreatest in_typ in
  match es_opt with
  | [] ->
      (* The default sort_until clause must be false.
       * If there is no sort_by clause, any constant will do: *)
      Printf.fprintf opc.code "\tfalse\n\n"
  | es ->
      Printf.fprintf opc.code "\t%a\n\n"
        (List.print ~first:"(" ~last:")" ~sep:", "
           (emit_expr ~env ~context:Finalize ~opc)) es

let emit_string_of_value indent typ val_var oc =
  let p fmt = emit oc indent fmt in
  p "%t %s"
    (conv_from_to ~string_not_null:true ~nullable:typ.DT.nullable
                  typ.vtyp (Mac String))
    val_var

(* We want a function that, when given the out tuples, will return the list
 * of notification names to send, along with all output values as strings: *)
(* TODO: shouldn't CodeGenLib pass this func the global and also maybe
 * the group states? *)
let emit_get_notifications name out_typ ~opc notifications =
  let env =
    add_tuple_environment Out out_typ [] in
  Printf.fprintf opc.code "let %s %a ="
    name
    (emit_tuple ~with_alias:true Out) out_typ ;
  if notifications = [] then
    Printf.fprintf opc.code " [||], [||]"
  else
    Printf.fprintf opc.code "\n\t%a,\n\t%a\n\n"
      (* The list of notification names: *)
      (List.print ~first:"[|" ~last:"|]" ~sep:";\n\t\t"
                  (emit_expr ~env ~context:Finalize ~opc))
        notifications
      (* The association list of all string valued parameters: *)
      (List.print ~first:"[|" ~last:"|]" ~sep:";\n\t\t  "
        (fun oc ft ->
          let id = id_of_field_name ~tuple:Out ft.RamenTuple.name in
          Printf.fprintf oc "%S, "
            (ft.RamenTuple.name :> string) ;
          emit_string_of_value 1 ft.typ id oc)) opc.typ

let emit_default_tuple name ~opc typ =
  let v = T.any_value_of_maybe_nullable typ in
  Printf.fprintf opc.code "let %s =\n\t%a\n\n"
    name
    emit_type v

let expr_needs_tuple_from lst e =
  match e.E.text with
  | Variable tuple
  | Binding (RecordField (tuple, _)) ->
      List.mem tuple lst
  | _ ->
      false

(* Tells whether this expression requires the out tuple (or anything else
 * from the group). *)
(* FIXME: Move into a compilation helper module with other helpers
 * independent of the backend. *)
let expr_needs_group e =
  expr_needs_tuple_from [ Group ] e ||
  (match e.E.text with
  | Stateful (LocalState, _, _) -> true
  | Stateless (SL0 (EventStart|EventStop)) ->
      (* This depends on the definition of the event time really.
       * TODO: pass the event time down here and actually check. *)
      true
  | _ ->
      false)

let default_commit_cond0 =
  (* Pass to Skeleton.aggregate some placeholder functions that will
   * never be called: *)
  "false \
   (fun _ _ -> assert false) \
   (fun _ _ _ _ -> assert false) \
   (fun _ _ -> assert false) \
   false"

let optimize_commit_cond ~env ~opc in_typ minimal_typ commit_cond =
  let es = E.as_nary E.And commit_cond in
  (* TODO: take the best possible sub-condition not the first one: *)
  let rec loop rest = function
    | [] ->
        !logger.warning "Cannot find a way to optimise the commit \
                         condition of function %s"
          (N.func_color (option_get "func_name" __LOC__ opc.func_name)) ;
        default_commit_cond0, commit_cond
    | e :: es ->
        (match Helpers.defined_order e with
        | exception Not_found ->
            !logger.debug "Expression %a does not define an ordering"
              (E.print false) e ;
            loop (e :: rest) es
        | f, neg, op, g ->
            !logger.debug "Expression %a defines an ordering"
              (E.print false) e ;
            (* We will convert both [f] and [g] into the bigger numeric type
             * as [emit_function] would do: *)
            let to_typ = large_enough_for f.typ.vtyp g.typ.vtyp in
            let may_neg e =
              (* Let's add an unary minus in front of [ e ] it we are supposed
               * to neg the Greater operator, and type it by hand: *)
              if neg then
                E.make ~vtyp:e.E.typ.DT.vtyp ~nullable:e.typ.DT.nullable
                       ?units:e.units (Stateless (SL1 (Minus, e)))
              else e in
            let cmp = omod_of_type to_typ ^".compare" in
            let cmp =
              match f.typ.DT.nullable, g.typ.DT.nullable with
              | false, false -> cmp
              | true, true -> "(Nullable.compare "^ cmp ^")"
              | true, false -> "(Nullable.compare_left "^ cmp ^")"
              | false, true -> "(Nullable.compare_right "^ cmp ^")" in
            (* Make it fair for other backends by using only machine types: *)
            let cmp = "(fun a_ b_ -> Int8.of_int ("^ cmp ^" a_ b_))" in
            let cond_in = "commit_cond_in_"
            and cond_out = "commit_cond_out_" in
            emit_cond0_in ~env cond_in in_typ ~opc
                          ~to_typ (may_neg f) ;
            emit_cond0_out ~env cond_out minimal_typ ~opc
                           ~to_typ (may_neg g) ;
            let cond =
              E.of_nary ~vtyp:commit_cond.typ.vtyp
                        ~nullable:commit_cond.typ.DT.nullable
                        ~units:commit_cond.units
                        E.And (List.rev_append rest es) in
            Printf.sprintf "true %s %s %s %b" cond_in cond_out cmp (op = Ge),
            cond) in
  loop [] es

let emit_aggregate opc global_state_env group_state_env
                   env_env param_env globals_env
                   name top_half_name in_typ =
  let out_typ = opc.typ in
  match opc.op with
  | Some O.Aggregate
      { fields ; sort ; where ; key ; commit_before ; commit_cond ;
        flush_how ; notifications ; every ; _ } ->
  let minimal_typ =
    Helpers.minimal_type (option_get "op" __LOC__ opc.op) in
  (* When filtering, the worker has two options:
   * It can check an incoming tuple as soon as it receives it, or it can
   * first compute the group key and retrieve the group state, and then
   * check the tuple. The later, slower option is required when the WHERE
   * expression uses anything from the group state (such as local function
   * states or group tuple).
   * It is best to partition the WHERE expression in two so that as much of
   * it can be checked as early as possible. *)
  let where_fast, where_slow =
    E.and_partition (not % expr_needs_group) where
  and check_commit_for_all = Helpers.check_commit_for_all commit_cond
  (* Every functions have at least access to env + params + globals: *)
  and base_env = param_env @ env_env @ globals_env in
  let commit_cond0, commit_cond_rest =
    let env = group_state_env @ global_state_env @ base_env in
    if check_commit_for_all then
      fail_with_context "optimized commit condition" (fun () ->
        optimize_commit_cond in_typ minimal_typ ~env ~opc commit_cond)
    else default_commit_cond0, commit_cond
  in
  fail_with_context "global state initializer" (fun () ->
    emit_state_init "global_init_" E.GlobalState ~env:base_env ["()"] ~where
                    ~commit_cond ~opc fields) ;
  fail_with_context "group state initializer" (fun () ->
    emit_state_init "group_init_" E.LocalState ~env:(global_state_env @ base_env)
                    ["global_"] ~where ~commit_cond ~opc fields) ;
  fail_with_context "tuple reader" (fun () ->
    emit_deserialize_function 0 "read_in_tuple_" ~opc in_typ) ;
  fail_with_context "where-fast function" (fun () ->
    emit_where ~env:(global_state_env @ base_env) "where_fast_" in_typ ~opc
      where_fast) ;
  fail_with_context "where-slow function" (fun () ->
    emit_where ~env:(group_state_env @ global_state_env @ base_env) "where_slow_"
               in_typ ~opc ~with_group:true where_slow) ;
  fail_with_context "key extraction function" (fun () ->
    emit_key_of_input "key_of_input_" in_typ ~env:(global_state_env @ base_env)
                      ~opc key) ;
  fail_with_context "optional-field extraction functions" (fun () ->
    emit_maybe_fields opc.code out_typ) ;
  fail_with_context "commit condition function" (fun () ->
    emit_when ~env:(group_state_env @ global_state_env @ base_env) "commit_cond_"
              in_typ minimal_typ ~opc commit_cond_rest) ;
  fail_with_context "select-clause function" (fun () ->
    emit_field_selection ~build_minimal:true
                         ~env:(group_state_env @ global_state_env @ base_env)
                         "minimal_tuple_of_group_" in_typ minimal_typ ~opc
                         fields) ;
  fail_with_context "output tuple function" (fun () ->
    emit_field_selection ~build_minimal:false
                         ~env:(group_state_env @ global_state_env @ base_env)
                         "out_tuple_of_minimal_tuple_" in_typ minimal_typ
                         ~opc fields) ;
  fail_with_context "state update function" (fun () ->
    emit_update_states ~env:(group_state_env @ global_state_env @ base_env)
                       "update_states_" in_typ minimal_typ ~opc fields) ;
  fail_with_context "sersize-of-tuple function" (fun () ->
    emit_sersize_of_tuple 0 "sersize_of_tuple_" opc.code out_typ) ;
  fail_with_context "time-of-tuple function" (fun () ->
    emit_time_of_tuple "time_of_tuple_" opc) ;
  fail_with_context "tuple serializer" (fun () ->
    emit_serialize_function 0 "serialize_tuple_" opc.code out_typ) ;
  fail_with_context "tuple generator" (fun () ->
    emit_generate_tuples "generate_tuples_" in_typ out_typ ~opc fields) ;
  fail_with_context "sort-until function" (fun () ->
    emit_sort_expr "sort_until_" in_typ ~opc
                   (match sort with Some (_, Some u, _) -> [u] | _ -> [])) ;
  fail_with_context "sort-by function" (fun () ->
    emit_sort_expr "sort_by_" in_typ ~opc
                   (match sort with Some (_, _, b) -> b | None -> [])) ;
  fail_with_context "notification extraction function" (fun () ->
    emit_get_notifications "get_notifications_" out_typ ~opc
                           notifications) ;
  fail_with_context "default in/out tuples" (fun () ->
    let in_rtyp = RamenTuple.to_record in_typ in
    emit_default_tuple "default_in_" ~opc in_rtyp ;
    let out_rtyp = RamenTuple.to_record out_typ in
    emit_default_tuple "default_out_" ~opc out_rtyp) ;
  let p fmt = emit opc.code 0 fmt in
  fail_with_context "aggregate function" (fun () ->
    p "let %s () =" name ;
    p "  CodeGenLib_Skeletons.aggregate" ;
    p "    read_in_tuple_ sersize_of_tuple_ time_of_tuple_" ;
    p "    factors_of_tuple_" ;
    p "    scalar_extractors_" ;
    p "    serialize_tuple_" ;
    p "    generate_tuples_" ;
    p "    minimal_tuple_of_group_" ;
    p "    update_states_" ;
    p "    out_tuple_of_minimal_tuple_" ;
    p "    (Uint32.of_int %d) sort_until_ sort_by_"
      (match sort with None -> 0 | Some (n, _, _) -> n) ;
    p "    where_fast_ where_slow_ key_of_input_ %b" (key = []) ;
    p "    commit_cond_ %s %b %b %b"
      commit_cond0 commit_before (flush_how <> Never) check_commit_for_all ;
    p "    global_init_ group_init_" ;
    p "    get_notifications_ %a"
      (fun oc -> function
      | Some e ->
          Printf.fprintf oc "(%a)"
            (conv_to ~env:base_env ~context:Finalize ~opc (Some (Mac Float))) e
      | None ->
          Float.print oc 0.) every ;
    p "    default_in_ default_out_" ;
    p "    orc_make_handler_ orc_write orc_close\n") ;
  (* The top-half is similar, but need less parameters.
   *
   * The filter used by the top-half must be a partition of the normal
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
  let where_top, _ =
    E.and_partition (fun e ->
      E.is_pure e && not (expr_needs_global_tuples e)
    ) where_fast in
  fail_with_context "top-where function" (fun () ->
    p "let top_where_ %a ="
      (emit_tuple ~with_alias:true In) in_typ ;
    let env =
      add_tuple_environment In in_typ base_env in
    p "  %a\n"
      (emit_expr ~env ~context:Finalize ~opc) where_top) ;
  fail_with_context "top-half function" (fun () ->
    p "let %s () =" top_half_name ;
    p "  CodeGenLib_Skeletons.top_half read_in_tuple_ top_where_\n\n")

    | _ -> assert false

let sanitize_ocaml_fname s =
  let open Str in
  let replace_by_underscore _ = "_"
  and re = regexp "[^A-Za-z0-9_]" in
  (* Must start with a letter: *)
  "m"^ global_substitute re replace_by_underscore s

module GlobalVariables =
struct
  (* Emit the Lmdb.Conv.t value corresponding to the given RamenTypes.t: *)
  let emit_lmdb_conv indent oc typ =
    let p fmt = emit oc indent fmt in
    let emit_serialise indent oc =
      let p fmt = emit oc indent fmt in
      p "(fun alloc_ x_ ->" ;
      p "  let sz_ =" ;
      emit_sersize_of_var (indent + 2) typ oc "x_" ;
      p "  in" ;
      (* For now go through some bytes, and then copy them into the bigstring: *)
      p "  let tx_ = RingBuf.bytes_tx sz_ in" ;
      p "  (* Serialize x_ into the bigstring: *)" ;
      p "  let offs_, _ =" ;
      emit_serialize_value (indent + 2) "0" "0" "0" "x_" oc typ ;
      p "  in" ;
      p "  (* Return the result in a bigstring: *)" ;
      p "  let bytes_ = RingBuf.read_raw_tx tx_ in" ;
      p "  let a_ = alloc_ offs_ in" ;
      p "  Bigstringaf.blit_from_bytes bytes_ ~src_off:0 a_ ~dst_off:0 ~len:offs_ ;" ;
      p "  a_)"
    and emit_deserialise indent oc =
      let p fmt = emit oc indent fmt in
      p "(fun a_ ->" ;
      p "  let bytes_ = Bigstringaf.to_string a_ |> Bytes.of_string in" ;
      p "  let tx_ = RingBuf.tx_of_bytes bytes_ in" ;
      p "  let x_, offs_ =" ;
      emit_deserialize_value (indent + 2) "tx_" "0" "0" "0" oc typ ;
      p "  in" ;
      p "  x_)"
    in
    p "let serialise =" ;
    emit_serialise (indent + 1) oc ;
    p "and deserialise =" ;
    emit_deserialise (indent + 1) oc ;
    p "in" ;
    p "Lmdb.Conv.make ~serialise ~deserialise ()"

  let emit oc globals src_path =
    let code = IO.output_string ()
    and consts = IO.output_string () in
    let opc =
      { op = None ; event_time = None ; func_name = None ;
        params = [] ; code ; consts ; typ = [] ; gen_consts = Set.empty ;
        dessser_mod_name = "" } in
    let indent = 0 in
    let p fmt = emit opc.consts indent fmt in
    let mod_name g = Printf.sprintf "Var_%s" (id_of_global g) in
    fail_with_context "globals accessors" (fun () ->
      (* For each globals of scope wider than function, emit a global variable of
       * the proper type, initialized with the "default" value. *)
      List.iter (fun g ->
        (* The id that's accessible from the stack is actually a getter/setter
         * pair of functions, calling the RamenGlobalVariabes module: *)
        p "(* Global variable %a of scope %s and type %a: *)"
          N.field_print g.Globals.name
          (Globals.string_of_scope g.scope)
          DT.print_maybe_nullable g.Globals.typ ;
        let scope_id = Globals.scope_id g src_path in
        (match g.typ.vtyp with
        | DT.Map (k, v) ->
            p "module %s = CodeGenLib_Globals.MakeMap (struct"
              (mod_name g) ;
            p "  let scope_id = %S" scope_id ;
            p "  type k = %a" otype_of_type k ;
            p "  type v = %a" otype_of_type v ;
            p "  let k_conv =" ;
            emit_lmdb_conv (indent + 2) opc.consts k ;
            p "  let v_conv =" ;
            emit_lmdb_conv (indent + 2) opc.consts v ;
            p "end)\n"
        | _ ->
            todo "emit_globals for other types")
      ) globals ;
      p "(* Globals as a Ramen record: *)" ;
      p "let globals_ = %a\n"
        (list_print_as_tuple (fun oc g ->
            Printf.fprintf oc "%s.init %S"
              (mod_name g) (g.name :> string)))
          globals ;
      Printf.fprintf oc "%s\n%s"
        (IO.close_out opc.consts) (IO.close_out opc.code))
end

let emit_parameters oc params envvars =
  (* Emit params module, that has a static value for each parameter and
   * the record expression for params and envvars. *)
  Printf.fprintf oc "\n(* Parameters: *)\n" ;
  List.iter (fun p ->
    let ctx =
      Printf.sprintf2 "definition of parameter %a"
        N.field_print p.ptyp.name in
    fail_with_context ctx (fun () ->
      (* FIXME: nullable parameters *)
      Printf.fprintf oc
        "let %s =\n\
         \tlet parser_ s_ =\n"
        (id_of_field_name ~tuple:Param p.ptyp.name) ;
      let emit_is_null fins str_var offs_var oc =
        Printf.fprintf oc
          "if looks_like_null ~offs:%s %s && \
              string_is_term %a %s (%s + 4) then \
           true, %s + 4 else false, %s"
          offs_var str_var
          (List.print char_print_quoted) fins str_var offs_var
          offs_var offs_var in
      emit_value_of_string 2 p.ptyp.typ "s_" "0" emit_is_null [] true oc ;
      Printf.fprintf oc
        "\tin\n\
         \tCodeGenLib.parameter_value ~def:(%s(%a)) parser_ %S\n"
        (if p.ptyp.typ.DT.nullable && p.value <> VNull then "NotNull " else "")
        emit_type
        p.value (p.ptyp.name :> string))
  ) params ;
  (* Also a function that takes a parameter name (string) and return its
   * value (as a string) - useful for text replacements within strings *)
  fail_with_context "parameter field extraction function" (fun () ->
    Printf.fprintf oc "let field_of_params_ = function\n%a\
                       \t| _ -> raise Not_found\n\n"
      (List.print ~first:"" ~last:"" ~sep:"" (fun oc p ->
        let glob_name =
          Printf.sprintf "%s_%s_"
            (id_of_prefix Param)
            (p.ptyp.name :> string) in
        Printf.fprintf oc "\t| %S -> %t %s%s\n"
          (p.ptyp.name :> string)
          (conv_from_to ~nullable:(p.ptyp.typ.DT.nullable)
                        p.ptyp.typ.vtyp (Mac String))
          glob_name
          (if p.ptyp.typ.DT.nullable then Printf.sprintf " |! %S" string_of_null
           else ""))) params) ;
  (* params and envs must be accessible as records (encoded as tuples)
   * under names "params_" and "envs_". Note that since we can refer to
   * the whole tuple "env" and "param", and that we type all functions
   * in a program together, then these records must contain all fields
   * used in the program, not only the fields used by any single function. *)
  fail_with_context "definition of the parameter record" (fun () ->
    Printf.fprintf oc
      "\n(* Parameters as a Ramen record: *)\n\
       let params_ = %a\n\n"
      (list_print_as_tuple (fun oc p ->
        Printf.fprintf oc "%s_%s_"
          (id_of_prefix Param)
          (p.ptyp.name :> string)))
        (RamenTuple.params_sort params)) ;
  fail_with_context "definition of the env record" (fun () ->
    Printf.fprintf oc
      "\n(* Environment variables as a Ramen record: *)\n\
       let envs_ = %a\n\n"
      (list_print_as_tuple (fun oc (n : N.field) ->
        Printf.fprintf oc "Sys.getenv_opt %S |> Nullable.of_option"
          (n :> string)))
        envvars)

let emit_running_condition oc params env cond =
  let code = IO.output_string ()
  and consts = IO.output_string () in
  let opc =
    { op = None ; event_time = None ; func_name = None ;
      params ; code ; consts ; typ = [] ; gen_consts = Set.empty ;
      dessser_mod_name = "" } in
  fail_with_context "running condition" (fun () ->
    Printf.fprintf opc.code "let run_condition_ () =\n\t%a\n\n"
      (emit_expr ~env ~context:Finalize ~opc) cond ;
    Printf.fprintf oc "%s\n%s\n\n"
      (IO.close_out opc.consts) (IO.close_out opc.code))

let emit_title func_name func_op oc =
  Printf.fprintf oc "(* Code generated for operation %S:\n%a\n*)\n"
    (func_name : N.func :> string)
    (O.print true) func_op

let emit_header params_mod_name globals_mod_name oc =
  Printf.fprintf oc "\
    open Batteries\n\
    open Stdint\n\
    open DessserOCamlBackEndHelpers\n\
    open RamenHelpersNoLog\n\
    open RamenHelpers\n\
    open RamenLog\n\
    open RamenConsts\n\n\
    open %s\n\
    open %s\n\n"
    params_mod_name
    globals_mod_name

let emit_operation name top_half_name func_op in_type
                   global_state_env group_state_env
                   env_env param_env globals_env opc =
  (* Default top-half (for non-aggregate operations): a NOP *)
  Printf.fprintf opc.code "let %s = ignore\n\n" top_half_name ;
  (* Emit code for all the operations: *)
  match func_op with
  | O.ReadExternal { source ; format ; _ } ->
    let source_name = name ^"_source"
    and parser_name = name ^"_format"
    and format_name =
      match format with CSV _ -> "CSV" | RowBinary _ -> "RowBinary" in
    (match source with
    | File specs ->
        emit_read_file opc param_env env_env globals_env source_name specs
    | Kafka specs ->
        emit_read_kafka opc param_env env_env globals_env source_name specs) ;
    emit_parse_external opc parser_name format_name ;
    emit_read opc name source_name parser_name
  | ListenFor { net_addr ; port ; proto } ->
    emit_listen_on opc name net_addr port proto
  | Aggregate _ ->
    emit_aggregate opc global_state_env group_state_env
                   env_env param_env globals_env
                   name top_half_name in_type

(* A function that reads the history and write it according to some out_ref
 * under a given channel: *)
let emit_replay name func_op opc =
  let p fmt = emit opc.code 0 fmt in
  let ser = O.out_type_of_operation func_op in
  let ser = O.filter_out_private ser in
  emit_deserialize_function 0 "read_pub_tuple_" ~opc ser ;
  p "let read_out_tuple_ tx_ start_offs_ =" ;
  p "  let tup_ = read_pub_tuple_ tx_ start_offs_ in" ;
  p "  out_of_pub_ tup_\n" ;
  p "let %s () =" name ;
  p "  CodeGenLib_Skeletons.replay read_out_tuple_" ;
  p "    sersize_of_tuple_ time_of_tuple_ factors_of_tuple_" ;
  p "    scalar_extractors_ serialize_tuple_" ;
  p "    orc_make_handler_ orc_write orc_read orc_close\n\n"

(* Generator for function [out_of_pub_] that adds missing private fields. *)
let emit_priv_pub opc =
  let op = option_get "must have function" __LOC__ opc.op in
  let rtyp = O.out_record_of_operation op in
  let var_of var k =
    var ^"_"^ k ^"_" |>
    RamenOCamlCompiler.make_valid_ocaml_identifier in
  let rec emit_transform indent trim var typ oc =
    let transform_record indent kts =
      let p fmt = emit oc indent fmt in
      p "let %a = %s in"
        (Enum.print ~first:"" ~last:"" ~sep:", " (fun oc (k, _) ->
          String.print oc (var_of var k)))
          (Array.enum kts // fun (k, _) ->
            trim || not N.(is_private (field k)))
        var ;
      Array.fold_left (fun i (k, t) ->
        let var' = var_of var k in
        if trim then (
          (* remove all private fields, recursively: *)
          if N.(is_private (field k)) then i
          else (
            if i > 0 then p "," ;
            p "(" ;
            emit_transform (indent + 1) trim var' t oc ;
            p ")" ;
            i + 1
          )
        ) else (
          if i > 0 then p "," ;
          if N.(is_private (field k)) then (
            (* add private value that was missing: *)
            let e = any_constant_of_expr_type t in
            Printf.fprintf opc.consts
              "let dummy_for_private_%s_ = %a\n"
              var' (emit_expr ~env:[] ~context:Finalize ~opc) e ;
            p "dummy_for_private_%s_" var'
          ) else (
            p "%s" (var_of var k)
          ) ;
          i + 1
        )
      ) 0 kts |> ignore
    in
    let p fmt = emit opc.code 0 fmt in
    let indent, var =
      if typ.DT.nullable then (
        let var' = "notnull_"^ var in
        p "(match %s with" var ;
        p "| Null -> Null" ;
        p "| NotNull %s -> NotNull (" var' ;
        indent + 1, var'
      ) else indent, var in
    let p fmt = emit oc indent fmt in
    (match typ.DT.vtyp with
    | DT.Rec kts ->
        transform_record indent kts ;
    | Tup ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        transform_record indent kts
    | Vec (_, t) | Lst t ->
        p "Array.map (fun v_ ->" ;
        emit_transform (indent + 1) trim "v_" t oc ;
        p ") %s" var
    | _ ->
        p "%s" var) ;
    if typ.DT.nullable then p "))"
  in
  (* print_record_as_tuple will explode all subrecords, naming subfields
   * with full path from the root so that the next [print_record_as_tuple]
   * will be able to reuse them. *)
  let p fmt = emit opc.code 0 fmt in
(*  p "let pub_of_out_ out_ =" ;
  emit_transform 1 true "out_" rtyp opc.code ;*)
  p "let out_of_pub_ pub_ =" ;
  emit_transform 1 false "pub_" rtyp opc.code ;
  p ""

let emit_dummies_for_private opc =
  let p fmt = emit opc.code 0 fmt in
  List.iter (fun ft ->
    if N.is_private ft.RamenTuple.name then (
      p "(* Dummy value for private field %a *)" N.field_print ft.name ;
      let e = any_constant_of_expr_type ft.typ in
      p "let %s = %a\n"
        (dummy_var_name ft.name)
        (emit_expr ~env:[] ~context:Finalize ~opc) e
    )
  ) opc.typ

let emit_orc_wrapper func_op orc_write_func orc_read_func oc =
  let p fmt = emit oc 0 fmt in
  let rtyp = O.out_record_of_operation func_op in
  let pub = T.filter_out_private rtyp in
  p "(* A handler to be passed to the function generated by" ;
  p "   emit_write_value: *)" ;
  p "type handler" ;
  p "" ;
  p "external orc_write : handler -> %a -> float -> float -> unit = %S"
    otype_of_type rtyp
    orc_write_func ;
  p "external orc_read_pub : \
       RamenName.path -> int -> (%a -> unit) -> (int * int) = %S"
    otype_of_type pub
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

let emit_make_orc_handler name func_op oc =
  let p fmt = emit oc 0 fmt in
  let rtyp = O.out_record_of_operation func_op in
  let schema = Orc.of_value_type rtyp.DT.vtyp |>
               IO.to_string Orc.print in
  p "let %s = orc_make_handler %S" name schema

(* Given the names of ORC reader/writer, build a universal conversion
 * function from/to CSV/RB/ORC named [name] and that takes the in and out
 * formats and file names (see [per_func_info] in CodeGenLib_Casing).
 * We have to deal with full tuples (including private fields) since that's
 * what take and return the ORC writer/readers. *)
let emit_convert name func_op oc =
  let p fmt = emit oc 0 fmt in
  let rtyp =
    O.out_record_of_operation func_op in
  p "let %s in_fmt_ in_fname_ out_fmt_ out_fname_ =" name ;
  (* We need our own tuple_of_strings_ because that for the CSV reader uses
   * a custom CSV separator/null string. *)
  O.out_type_of_operation func_op |>
  emit_tuple_of_strings 1 "my_tuple_of_strings_" string_of_null oc ;
  p "  in" ;
  p "  let csv_write fd v =" ;
  p "    let str =" ;
  emit_string_of_value 3 rtyp "v" oc ;
  p "      in" ;
  p "    RamenFiles.write_whole_string fd (str ^ \"\\n\")" ;
  p "  in" ;
  p "  CodeGenLib_Skeletons.convert" ;
  p "    in_fmt_ in_fname_ out_fmt_ out_fname_" ;
  p "    orc_read csv_write orc_make_handler_ orc_write orc_close" ;
  p "    read_out_tuple_ sersize_of_tuple_ time_of_tuple_" ;
  p "    serialize_tuple_ my_tuple_of_strings_\n\n"

let generate_code
      conf func_name func_op in_type
      env_env param_env globals_env global_state_env group_state_env
      obj_name params_mod_name dessser_mod_name
      orc_write_func orc_read_func params
      globals_mod_name =
  (* The code might need some global constant parameters, thus the two strings
   * that are assembled later: *)
  let code = IO.output_string ()
  and consts = IO.output_string ()
  and typ = O.out_type_of_operation func_op
  in
  let opc =
    { op = Some func_op ; func_name = Some func_name ; params ; code ; consts ;
      typ ; event_time = O.event_time_of_operation func_op ;
      gen_consts = Set.empty ; dessser_mod_name } in
  let src_file =
    RamenOCamlCompiler.with_code_file_for
      obj_name conf.C.reuse_prev_files (fun oc ->
        fail_with_context "header" (fun () ->
          emit_title func_name func_op oc ;
          emit_header params_mod_name globals_mod_name oc) ;
        (* FIXME: in theory those dummy variables may be referenced from the
         * code outputing default values for parameters, and therefore should
         * be emitted earlier. *)
        fail_with_context "dummies for private fields" (fun () ->
          emit_dummies_for_private opc) ;
        fail_with_context "priv_to_pub function" (fun () ->
          emit_priv_pub opc) ;
        fail_with_context "orc wrapper" (fun () ->
          emit_orc_wrapper func_op orc_write_func orc_read_func opc.code) ;
        fail_with_context "orc handler builder" (fun () ->
          emit_make_orc_handler "orc_make_handler_" func_op opc.code) ;
        fail_with_context "factors extractor" (fun () ->
          emit_factors_of_tuple "factors_of_tuple_" func_op opc.code) ;
        fail_with_context "scalar extractors" (fun () ->
          emit_scalar_extractors "scalar_extractors_" func opc.code) ;
        fail_with_context "operation" (fun () ->
          emit_operation EntryPoints.worker EntryPoints.top_half func_op
                         in_type global_state_env group_state_env env_env
                         param_env globals_env opc) ;
        fail_with_context "replay function" (fun () ->
          emit_replay EntryPoints.replay func_op opc) ;
        fail_with_context "tuple conversion function" (fun () ->
          emit_convert EntryPoints.convert func_op opc.code) ;
        Printf.fprintf oc "\n(* Global constants: *)\n\n%s\n\
                           \n(* Operation Implementation: *)\n\n%s\n"
          (IO.close_out consts) (IO.close_out code)
      ) in
  let what = "function "^ N.func_color func_name in
  RamenOCamlCompiler.compile conf ~keep_temp_files:conf.C.keep_temp_files
                             what src_file obj_name
