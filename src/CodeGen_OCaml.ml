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
open RamenLog
open RamenLang
open RamenHelpers
open RamenTuple
open RamenNullable
module C = RamenConf
module VSI = RamenSync.Value.SourceInfo
module E = RamenExpr
module T = RamenTypes
module O = RamenOperation
module N = RamenName
module Orc = RamenOrc
module Globals = RamenGlobalVariables
open RamenConsts
open RamenTypes (* FIXME: RamenTypes.Pub ? *)

(* If true, the generated code will log details about serialization *)
let verbose_serialization = false

(* We pass this around as "opc" *)
type op_context =
  { op : O.t option ;
    event_time : RamenEventTime.t option ;
    (* The type of the output tuple in user order *)
    (* FIXME: make is a TRecord to simplify code generation: *)
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

(* Tuple deconstruction as a function parameter: *)
let id_of_field_name ?(tuple=In) (x : N.field) =
  (match (x :> string) with
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
  | TString ->
      p "%d + RingBuf.round_up_to_rb_word (RingBuf.read_word %s %s)"
        RingBuf.rb_word_bytes tx_var offs_var
  | TIp ->
      p "RingBuf.(rb_word_bytes +" ;
      p "  round_up_to_rb_word(" ;
      p "    match RingBuf.read_word %s %s with" tx_var offs_var ;
      p "    | 0 -> %a" emit_sersize_of_fixsz_typ TIpv4 ;
      p "    | 1 -> %a" emit_sersize_of_fixsz_typ TIpv6 ;
      p "    | x -> invalid_byte_for \"IP\" x))"
  | TCidr ->
      p "RingBuf.(rb_word_bytes +" ;
      p "  round_up_to_rb_word(" ;
      p "    match RingBuf.read_u8 %s %s |> Uint8.to_int with"
        tx_var offs_var ;
      p "    | 4 -> %a" emit_sersize_of_fixsz_typ TCidrv4 ;
      p "    | 6 -> %a" emit_sersize_of_fixsz_typ TCidrv6 ;
      p "    | x -> invalid_byte_for \"CIDR\" x))"
  | TTuple _ | TRecord _ | TVec _ | TList _ ->
      assert false
  | t ->
      p "%a" emit_sersize_of_fixsz_typ t

let id_of_typ = function
  | TFloat  -> "float"
  | TString -> "string"
  | TChar   -> "char"
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
  | TEth    -> "eth"
  | TIpv4   -> "ip4"
  | TIpv6   -> "ip6"
  | TIp     -> "ip"
  | TCidrv4 -> "cidr4"
  | TCidrv6 -> "cidr6"
  | TCidr   -> "cidr"
  | TTuple _ -> "tuple"
  | TRecord _ -> "record"
  | TVec _  -> "vector"
  | TList _ -> "list"
  | TNum | TAny | TEmpty -> assert false
  | TMap _ -> assert false (* No values of that type *)

let rec emit_value_of_string
    indent t str_var offs_var emit_is_null fins may_quote oc =
  let p fmt = emit oc indent fmt in
  if t.T.nullable then (
    p "let is_null_, o_ = %t in" (emit_is_null fins str_var offs_var) ;
    p "if is_null_ then Null, o_ else" ;
    p "let x_, o_ =" ;
    let t = { t with nullable = false } in
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
    let emit_parse_tuple indent ts oc =
      (* Look for '(' *)
      p "let offs_ = string_skip_blanks_until '(' %s %s + 1 in"
        str_var offs_var ;
      p "if offs_ >= String.length %s then" str_var ;
      p "  failwith \"Tuple interrupted by end of string\" ;" ;
      for i = 0 to Array.length ts - 1 do
        p "let x%d_, offs_ =" i ;
        let fins = ';' :: fins in
        let fins = if i = Array.length ts - 1 then ')' :: fins else fins in
        emit_value_of_string
          (indent + 1) ts.(i) str_var "offs_" emit_is_null fins may_quote oc ;
        p "  in" ;
        p "let offs_ = string_skip_blanks %s offs_ in" str_var ;
        p "let offs_ =" ;
        if i = Array.length ts - 1 then (
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
          Printf.fprintf oc "x%d_" i)) ts
    in
    match t.T.structure with
    | TVec (d, t) ->
        p "let lst_, offs_ as res_ =" ;
        emit_parse_list (indent + 1) t oc ;
        p "in" ;
        p "if Array.length lst_ <> %d then" d ;
        p "  Printf.sprintf \"Was expecting %d values but got %%d\"" d ;
        p "    (Array.length lst_) |> failwith ;" ;
        p "res_"
    | TList t ->
        emit_parse_list indent t oc
    | TTuple ts ->
        emit_parse_tuple indent ts oc
    | TRecord kts ->
        (* TODO: read field labels and reorder.
         * For now we will expect fields in user definition order: *)
        let ts = Array.map snd kts in
        emit_parse_tuple indent ts oc
    | TString ->
        (* This one is a bit harder than the others due to optional quoting
         * (from the command line parameters, as CSV strings have been unquoted
         * already), and could benefit from [fins]: *)
        p "RamenTypeConverters.string_of_string ~fins:%a ~may_quote:%b %s %s"
          (List.print char_print_quoted) fins
          may_quote str_var offs_var
    | _ ->
        p "RamenTypeConverters.%s_of_string %s %s"
          (id_of_typ t.T.structure) str_var offs_var
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
 * factors to CodeGenLib. *)
let rec emit_value oc typ =
  let open Stdint in
  if typ.T.nullable then
    String.print oc "(function Null -> RamenTypes.VNull | NotNull x_ -> "
  else
    String.print oc "(fun x_ -> " ;
  let p n = Printf.fprintf oc "RamenTypes.%s x_" n in
  (match typ.structure with
  | TEmpty | TNum | TAny -> assert false
  | TFloat -> p "VFloat" | TString -> p "VString" | TBool -> p "VBool"
  | TChar -> p "VChar" | TU8 -> p "VU8" | TU16 -> p "VU16" | TU32 -> p "VU32"
  | TU64 -> p "VU64" | TU128 -> p "VU128"
  | TI8 -> p "VI8" | TI16 -> p "VI16" | TI32 -> p "VI32"
  | TI64 -> p "VI64" | TI128 -> p "VI128"
  | TEth -> p "VEth" | TIpv4 -> p "VIpv4" | TIpv6 -> p "VIpv6"
  | TIp -> p "VIp" | TCidrv4 -> p "VCidrv4" | TCidrv6 -> p "VCidrv6"
  | TCidr -> p "VCidr"
  | TTuple ts ->
      Printf.fprintf oc "(let %a = x_ in RamenTypes.VTuple %a)"
        (array_print_as_tuple_i (fun oc i _ ->
          Printf.fprintf oc "x%d_" i)) ts
        (array_print_i (fun i oc typ ->
          Printf.fprintf oc "(%a x%d_)" emit_value typ i)) ts
  | TRecord kts ->
      Printf.fprintf oc "(let h_ = Hashtbl.create %d " (Array.length kts) ;
      Printf.fprintf oc "and %a = x_ in "
        (array_print_as_tuple_i (fun oc i _ ->
          Printf.fprintf oc "x%d_" i)) kts ;
      Array.iter (fun (k, t) ->
        Printf.fprintf oc "Hashtbl.add h_ %S %a ;" k emit_value t) kts ;
      Printf.fprintf oc "RamenTypes.VRecord h_)"
  | TVec (_d, t) ->
      Printf.fprintf oc "RamenTypes.VVec (Array.map %a x_)" emit_value t
  | TList t ->
      Printf.fprintf oc "RamenTypes.VList (Array.map %a x_)" emit_value t
  | TMap _ -> assert false (* No values of that type *)) ;
  String.print oc ")"

let rec emit_type oc =
  let open Stdint in
  function
  | VFloat  f -> emit_float oc f
  | VString s -> Printf.fprintf oc "%S" s
  | VBool   b -> Printf.fprintf oc "%b" b
  | VChar   c -> Printf.fprintf oc "%C" c
  | VU8     n -> Printf.fprintf oc "(Uint8.of_int (%d))" (Uint8.to_int n)
  | VU16    n -> Printf.fprintf oc "(Uint16.of_int (%d))" (Uint16.to_int n)
  | VU32    n -> Printf.fprintf oc "(Uint32.of_int64 (%sL))" (Uint32.to_string n)
  | VU64    n -> Printf.fprintf oc "(Uint64.of_string %S)" (Uint64.to_string n)
  | VU128   n -> Printf.fprintf oc "(Uint128.of_string %S)" (Uint128.to_string n)
  | VI8     n -> Printf.fprintf oc "(Int8.of_int (%d))" (Int8.to_int n)
  | VI16    n -> Printf.fprintf oc "(Int16.of_int (%d))" (Int16.to_int n)
  | VI32    n -> Printf.fprintf oc "(%sl)" (Int32.to_string n)
  | VI64    n -> Printf.fprintf oc "(%sL)" (Int64.to_string n)
  | VI128   n -> Printf.fprintf oc "(Int128.of_string %S)" (Int128.to_string n)
  | VEth    n -> Printf.fprintf oc "(Uint48.of_int64 (%LdL))" (Uint48.to_int64 n)
  | VIpv4   n -> Printf.fprintf oc "(Uint32.of_string %S)" (Uint32.to_string n)
  | VIpv6   n -> Printf.fprintf oc "(Uint128.of_string %S)" (Uint128.to_string n)
  | VIp (RamenIp.V4 n) -> emit_type oc (VIpv4 n)
  | VIp (RamenIp.V6 n) -> emit_type oc (VIpv6 n)
  | VCidrv4 (n,l) ->
                 Printf.fprintf oc "(Uint32.of_string %S, %d)" (Uint32.to_string n) l
  | VCidrv6 (n,l) ->
                 Printf.fprintf oc "(Uint128.of_string %S, %d)" (Uint128.to_string n) l
  | VCidr (RamenIp.Cidr.V4 n) -> emit_type oc (VCidrv4 n)
  | VCidr (RamenIp.Cidr.V6 n) -> emit_type oc (VCidrv6 n)
  | VTuple vs ->
      Array.print ~first:"(" ~last:")" ~sep:", " emit_type oc vs
  | VRecord kvs ->
      (* A record internal value is a tuple with fields in definition order: *)
      let vs = Array.map snd kvs in
      emit_type oc (VTuple vs)
  | VVec vs   -> Array.print emit_type oc vs
  (* For now ramen lists are ocaml arrays. Should they be ocaml lists? *)
  | VList vs  -> Array.print emit_type oc vs
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

let rec otype_of_structure oc = function
  | TFloat -> String.print oc "float"
  | TString -> String.print oc "string"
  | TChar -> String.print oc "char"
  | TBool -> String.print oc "bool"
  | TU8 -> String.print oc "uint8"
  | TU16 -> String.print oc "uint16"
  | TU32 -> String.print oc "uint32"
  | TU64 -> String.print oc "uint64"
  | TU128 -> String.print oc "uint128"
  | TI8 -> String.print oc "int8"
  | TI16 -> String.print oc "int16"
  | TI32 -> String.print oc "int32"
  | TI64 -> String.print oc "int64"
  | TI128 -> String.print oc "int128"
  | TEth -> String.print oc "uint48"
  | TIpv4 -> String.print oc "uint32"
  | TIpv6 -> String.print oc "uint128"
  | TIp -> String.print oc "RamenIp.t"
  | TCidrv4 -> String.print oc "(uint32 * int)"
  | TCidrv6 -> String.print oc "(uint128 * int)"
  | TCidr -> String.print oc "RamenIp.Cidr.t"
  | TTuple ts ->
      Array.print ~first:"(" ~last:")" ~sep:" * "
        (fun oc t -> otype_of_type oc t)
        oc ts
  | TRecord kts ->
      (* A record internal representation is a tuple with field in the
       * definition order: *)
      let ts = Array.map snd kts in
      otype_of_structure oc (TTuple ts)
  | TVec (_, t) | TList t ->
      Printf.fprintf oc "%a array" otype_of_type t
  | TNum | TAny | TEmpty -> assert false
  | TMap _ -> assert false (* No values of that type *)

and otype_of_type oc t =
  Printf.fprintf oc "%a%s"
    otype_of_structure t.T.structure
    (if t.T.nullable then " nullable" else "")

let omod_of_type = function
  | TFloat -> "Float"
  | TString -> "String"
  | TBool -> "Bool"
  | TChar -> "Char"
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128 as t ->
    String.capitalize (IO.to_string otype_of_structure t)
  | TEth -> "RamenEthAddr"
  | TIpv4 -> "RamenIpv4"
  | TIpv6 -> "RamenIpv6"
  | TIp -> "RamenIp"
  | TCidrv4 -> "RamenIpv4.Cidr"
  | TCidrv6 -> "RamenIpv6.Cidr"
  | TCidr -> "RamenIp.Cidr"
  | TTuple _ | TRecord _ | TVec _ | TList _ | TMap _
  | TNum | TAny | TEmpty ->
      assert false

let rec filter_out_private t =
  match t.T.structure with
  | T.TRecord kts ->
      let kts =
        Array.filter_map (fun (k, t') ->
          if N.(is_private (field k)) then None
          else (
            filter_out_private t' |> Option.map (fun t' -> k, t')
          )
        ) kts in
      if Array.length kts = 0 then None
      else Some T.{ t with structure = TRecord kts }
  | T.TTuple ts ->
      let ts = Array.filter_map filter_out_private ts in
      if Array.length ts = 0 then None
      else Some T.{ t with structure = TTuple ts }
  | T.TVec (d, t') ->
      filter_out_private t' |>
      Option.map (fun t' -> T.{ t with structure = TVec (d, t') })
  | T.TList t' ->
      filter_out_private t' |>
      Option.map (fun t' -> T.{ t with structure = TList t' })
  | _ -> Some t

(* Simpler, temp version of the above: *)
let filter_out_private_from_tup tup =
  List.filter (fun ft ->
    not (N.is_private ft.RamenTuple.name)
  ) tup

(* Given a function name and an output type, return the actual function
 * returning that type, and the types each input parameters must be converted
 * into, if any. None means we need no conversion whatsoever (useful for
 * function internal state or 'a values) while Some TAny means there must be a
 * type but it has to be found out according to the context.
 *
 * Returns a list of typ option, as long as the type of input arguments *)
(* FIXME: this could be extracted from Compiler.check_expr *)

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
   * Emitted code must be prefixable by "nullable_map": *)
  let rec print_non_null oc (from_typ, to_typ as conv) =
    if from_typ = to_typ then Printf.fprintf oc "identity" else
    match conv with
    | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128|TString|TFloat),
        (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128)
    | TString, (TFloat|TBool) ->
      Printf.fprintf oc "%s.of_%a"
        (omod_of_type to_typ)
        otype_of_structure from_typ
    | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128),
        (TFloat|TString)
    | (TFloat|TBool), (TString|TFloat) ->
      Printf.fprintf oc "%s.to_%a"
        (omod_of_type from_typ)
        otype_of_structure to_typ
    | TBool, (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) ->
      Printf.fprintf oc "(%s.of_int %% Bool.to_int)"
        (omod_of_type to_typ)
    | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128), TBool ->
      Printf.fprintf oc "(fun x_ -> %s.(compare zero x_) <> 0)"
        (omod_of_type from_typ)
    | (TEth|TIpv4|TIpv6|TIp|TCidrv4|TCidrv6|TCidr), TString ->
      Printf.fprintf oc "%s.to_string" (omod_of_type from_typ)
    | TChar , TString ->
      Printf.fprintf oc "String.make 1"
    | TString, _ ->
      Printf.fprintf oc
        "(fun s_ ->\n\t\t\
          let x_, o_ = RamenTypeConverters.%s_of_string s_ 0 in\n\t\t\
          if o_ < String.length s_ then raise ImNull else x_)\n\t"
        (id_of_typ to_typ)
    | (TIpv4 | TU32), TIp -> Printf.fprintf oc "(fun x_ -> RamenIp.V4 x_)"
    | (TIpv6 | TU128), TIp -> Printf.fprintf oc "(fun x_ -> RamenIp.V6 x_)"
    | TIpv4, TCidrv4 -> Printf.fprintf oc "(fun x_ -> x_, 32)"
    | TIpv6, TCidrv6 -> Printf.fprintf oc "(fun x_ -> x_, 128)"
    | TIp, TCidr ->
        Printf.fprintf oc "(function RamenIp.V4 x_ -> RamenIp.Cidr.V4 (x_, 32) \
                                   | RamenIp.V6 x_ -> RamenIp.Cidr.V6 (x_, 128))"
    | TCidrv4, TCidr -> Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V4 x_)"
    | TCidrv6, TCidr -> Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V6 x_)"
    | TIpv4, TU32 | TU32, TIpv4 -> Printf.fprintf oc "identity"
    | TIpv6, TU128 | TU128, TIpv6 -> Printf.fprintf oc "identity"
    | TU64, TEth -> Printf.fprintf oc "Uint48.of_uint64"
    | TList t_from, TList t_to
         when t_from.nullable = t_to.nullable ->
      Printf.fprintf oc "(Array.map (%t))"
        (conv_from_to ~string_not_null ~nullable:t_from.nullable
                      t_from.structure t_to.structure)
    | TList t_from, TList t_to
         when nullable && t_from.nullable && not t_to.nullable ->
      Printf.fprintf oc
        "(Array.map (function \
            | Null -> raise ImNull \
            | NotNull x_ -> %t x_))"
        (conv_from_to ~string_not_null ~nullable:t_from.nullable
                      t_from.structure t_to.structure)
    | TList t_from, TList t_to
         when not t_from.nullable && t_to.nullable ->
      Printf.fprintf oc
        "(Array.map (fun x_ -> NotNull (%t x_)))"
        (conv_from_to ~string_not_null ~nullable:false
                      t_from.structure t_to.structure)
    | TVec (_, t_from), TList t_to ->
      print_non_null oc (TList t_from, TList t_to)
    | TVec (d_from, t_from), TVec (d_to, t_to)
      when (d_from = d_to || d_to = 0) ->
      (* d_to = 0 means no constraint (copy the one from the left-hand side) *)
      print_non_null oc (TList t_from, TList t_to)

    | TTuple t_from, TTuple t_to
      when Array.length t_from = Array.length t_to ->
      (* TODO: actually we could project away fields from t_from when t_to
       * is narrower, or inject NULLs in some cases. *)
      Printf.fprintf oc "(fun (%a) -> ("
        (array_print_as_tuple_i (fun oc i _ ->
          Printf.fprintf oc "x%d_" i)) t_from ;
      for i = 0 to Array.length t_from - 1 do
        if i > 0 then Printf.fprintf oc ",\n\t" ;
        Printf.fprintf oc "%t %a x%d_"
          (conv_nullable t_from.(i).nullable t_to.(i).nullable)
          print_non_null (t_from.(i).structure, t_to.(i).structure)
          i
      done ;
      Printf.fprintf oc "))"

    | TTuple t_from, TVec (d, t_to) when d = Array.length t_from ->
      print_non_null oc (from_typ, TList t_to)

    | TTuple t_from, TList t_to ->
      Printf.fprintf oc "(fun (%a) -> [|"
        (array_print_as_tuple_i (fun oc i _ ->
          Printf.fprintf oc "x%d_" i)) t_from ;
      for i = 0 to Array.length t_from - 1 do
        if i > 0 then Printf.fprintf oc " ;\n\t" ;
        Printf.fprintf oc "%t %a x%d_"
          (conv_nullable t_from.(i).nullable t_to.nullable)
          print_non_null (t_from.(i).structure, t_to.structure)
          i
      done ;
      Printf.fprintf oc "|])"

    (* In general, a vector or list is converting to a string by pretty
     * printing the type. But for chars the intend is to convert into
     * a string: *)
    | (TVec (_, t) | TList t), TString
      when t.T.structure = T.TChar ->
        (* The case when the vector itself is null is already dealt with
         * so here the vector is not null, but still it's elements can be.
         * In that case, the string result is not nullable (nullability
         * propagates from the vector to the string result, not from the
         * vector items to the string result).
         * Indeed, if we converted to string a vector of nul items, we
         * would like to see "[NULL; NULL; ...]". Here it's the same, just
         * with mere characters.
         * So string_of_nullable_chars will just replace nulls with '?'. *)
        if t.nullable then
          Printf.fprintf oc "CodeGenLib.string_of_nullable_chars"
        else
          Printf.fprintf oc "CodeGenLib.string_of_chars"

    | (TVec (_, t) | TList t), TString ->
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
                      t.structure TString)
        (if not string_not_null && t.nullable then
           Printf.sprintf "default %S" string_of_null else "")
    | TTuple ts, TString ->
      let i = ref 0 in
      Printf.fprintf oc
        "(fun %a -> \"(\"^ %a ^\")\")"
          (array_print_as_tuple_i (fun oc i _ ->
            Printf.fprintf oc "x%d_" i)) ts
          (Array.print ~first:"" ~last:"" ~sep:" ^\";\"^ " (fun oc t ->
            Printf.fprintf oc "(%t) x%d_"
              (conv_from_to ~string_not_null ~nullable:t.nullable
                            t.structure TString) !i ;
            incr i)) ts
    | TRecord kts, TString ->
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
              (conv_from_to ~string_not_null ~nullable:t.nullable
                            t.structure TString)
              (arg_var k))) kts'

    (* Any type can also be converted into a singleton vector of a compatible
     * type: *)
    | from_structure, TVec (1, to_typ) ->
      (* Let the convertion from [from_typ] to [to_typ] fail as there are
       * no more possible alternative anyway: *)
      Printf.fprintf oc "(fun x_ -> [| %t %a x_ |])"
        (conv_nullable nullable to_typ.nullable)
        print_non_null (from_structure, to_typ.structure)

    | _ ->
      Printf.sprintf2 "Cannot find converter from type %a to type %a"
        print_structure from_typ
        print_structure to_typ |>
      failwith
  in
  (* In general, when we convert a nullable thing into another type, then
   * if the result is also nullable. But sometime we want to have a non
   * nullable string representation of any values, where we want null values
   * to appear as "null": *)
  match nullable, to_typ, string_not_null with
  | false, _, _ ->
      print_non_null oc (from_typ, to_typ)
  | true, TString, true ->
      Printf.fprintf oc "(default %S %% nullable_map_no_fail %a)"
        string_of_null print_non_null (from_typ, to_typ)
  | true, _, _ ->
      (* Here any conversion that fails for any reason can be mapped to NULL *)
      Printf.fprintf oc "nullable_map_no_fail %a"
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
  E.make ~structure:typ.T.structure ~nullable:typ.T.nullable
         (Const (any_value_of_type ~avoid_null typ))

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

(* Return the environment corresponding to the used envvars: *)
let env_of_envvars envvars =
  List.map (fun (f : N.field) ->
    let v =
      Printf.sprintf2 "(Sys.getenv_opt %S |> nullable_of_option)"
        (f :> string) in
    (* FIXME: RecordField should take a tuple and a _path_ not a field
     * name *)
    E.RecordField (Env, f), v
  ) envvars

let env_of_params params =
  List.map (fun param ->
    let f = param.RamenTuple.ptyp.name in
    let v = id_of_field_name ~tuple:Param f in
    (* FIXME: RecordField should take a tuple and a _path_ not a field
     * name *)
    E.RecordField (Param, f), v
  ) params

let env_of_globals globals_mod_name globals =
  List.map (fun g ->
    let v =
      assert (globals_mod_name <> "") ;
      globals_mod_name ^"."^ id_of_global g in
    E.RecordField (Global, g.Globals.name), v
  ) globals

(* Returns all the bindings for accessing the env and param 'tuples': *)
let static_environments
    globals_mod_name params envvars globals =
  let init_env = E.RecordValue Env, "envs_"
  and init_param = E.RecordValue Param, "params_"
  and init_global = E.RecordValue Global, "globals_" in
  let env_env = init_env :: env_of_envvars envvars
  and param_env = init_param :: env_of_params params
  and global_state_env =
    init_global :: env_of_globals globals_mod_name globals in
  env_env, param_env, global_state_env

(* Returns all the bindings in global and group states: *)
let initial_environments op =
  let glob_env, loc_env =
    O.fold_expr ([], []) (fun _c _s (glo, loc as prev) e ->
      match e.E.text with
      | Stateful (g, _, _) ->
          let n = name_of_state e in
          (match g with
          | E.GlobalState ->
              let v = id_of_state GlobalState ^"."^ n in
              (E.State e.uniq_num, v) :: glo, loc
          | E.LocalState ->
              let v = id_of_state LocalState ^"."^ n in
              glo, (E.State e.uniq_num, v) :: loc)
      | _ -> prev
    ) op in
  glob_env, loc_env

(* Takes an operation and convert all its Path expressions for the
 * given tuple into a Binding to the environment: *)
let subst_fields_for_binding pref =
  O.map_expr (fun _stack e ->
    match e.E.text with
    | Stateless (SL0 (Path path))
      when pref = In ->
        let f = E.id_of_path path in
        { e with text = Binding (RecordField (pref, f)) }
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                           { text = Variable prefix ; }))
      when pref = prefix ->
        let f = N.field n in
        { e with text = Binding (RecordField (pref, f)) }
    | _ -> e)

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
          Printf.sprintf2 "(Sys.getenv_opt %S |> nullable_of_option)"
            (ft.RamenTuple.name :> string)
      | OutPrevious ->
          Printf.sprintf2 "(maybe_%s_ out_previous_)"
            (ft.name :> string)
      | _ ->
          id_of_field_typ ~tuple ft in
    (E.RecordField (tuple, ft.name), v) :: env
  ) env typ

let rec conv_to ~env ~context ~opc to_typ oc e =
  match e.E.typ.structure, to_typ with
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
                     func_name ((None, PropagateNull) :: to_typ) oc
                     (my_state :: args)
    | Some vars_to_typ ->
      emit_functionNv ~env ~opc ~nullable func_name
                      ((None, PropagateNull) :: to_typ)
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
      if e.E.typ.nullable then (
        let var_name =
          Printf.sprintf "nonnull_%d_" e.E.uniq_num in
        Printf.fprintf oc "(match %a with Null -> () | NotNull %s -> "
          (emit_expr ~context:Finalize ~opc ~env) e
          var_name ;
        (E.make ~structure:e.typ.structure ~nullable:false
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
                      func_name ((None, PropagateNull)::to_typ)) (my_state::fin_args)
  else
    emit_functionN ~env ~opc ~nullable ?impl_return_nullable ?args_as
                   func_name ((None, PropagateNull)::to_typ) oc (my_state::fin_args)

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
          (if f.typ.nullable then "((%t) %s |! 0.)" else "(%t) %s")
          (conv_from_to ~nullable:f.typ.nullable f.typ.structure TFloat)
          (id_of_field_name ~tuple:Out field_name)
    | Parameter ->
        let param = RamenTuple.params_find field_name opc.params in
        Printf.fprintf oc "(%t %s_%s_)"
          (conv_from_to ~nullable:false param.ptyp.typ.structure TFloat)
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
  let nullable = expr.E.typ.nullable in
  (* my_state will represent the variable holding the state of a stateful
   * function. *)
  let my_state =
    (* A state is always as nullable as its expression (see
     * [otype_of_state]): *)
    E.make ~nullable (Binding (State expr.E.uniq_num)) in
  match context, expr.text, expr.typ.structure with
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
      (conv_from_to ~nullable:false (structure_of c) expr.typ.structure)
      emit_type c
  | Finalize, Tuple es, _ ->
    list_print_as_tuple (emit_expr ~env ~context ~opc) oc es
  | Finalize, Record kvs, _ ->
    (* Here we must compute the values in order, as each expression can
     * refer to the previous one. And we must, for each expression, evaluate
     * it in a context where this record is opened. *)
    let _env =
      List.fold_left (fun env ((k : N.field), v) ->
        let var_name = var_name_of_record_field k in
        Printf.fprintf oc "\tlet %s = %a in\n"
          var_name
          (emit_expr ~env ~context ~opc) v ;
        (E.RecordField (Record, k), var_name) :: env
      ) env kvs in
    (* finally, regroup those fields in a tuple: *)
    let es =
      Array.enum (E.fields_of_record kvs) /@
      var_name_of_record_field |>
      List.of_enum in
    list_print_as_tuple String.print oc es

  | Finalize, Vector es, TVec (_, t) ->
    list_print_as_vector (conv_to ~env ~context ~opc (Some t.T.structure))
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
           (if nullable && not alt.case_cons.typ.nullable
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
        (if nullable && not else_.typ.nullable
         then "NotNull " else "")
        (conv_to ~env ~context ~opc (Some t)) else_)
  | Finalize, Stateless (SL1s (Coalesce, es)), t ->
    let rec loop = function
      | [] -> ()
      | [last] ->
        Printf.fprintf oc "(%a)" (conv_to ~env ~context ~opc (Some t)) last
      | e :: rest ->
        Printf.fprintf oc "(default_delayed (fun () -> " ;
        loop rest ;
        Printf.fprintf oc ") (%a))" (conv_to ~env ~context ~opc (Some t)) e
    in
    loop es
  (* Stateless arithmetic functions which actual funcname depends on operand types: *)
  | Finalize, Stateless (SL2 (Add, e1, e2)),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".add")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Sub, e1, e2)),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".sub")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Mul, e1, e2)),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".mul")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Mul, e1, e2)), TString ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.string_repeat"
      [Some TString, PropagateNull; Some TU32, PropagateNull] oc
      (if e1.E.typ.T.structure = TString then [e1; e2] else [e2; e1])
  | Finalize, Stateless (SL2 (IDiv, e1, e2)),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".div")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (IDiv, e1, e2)), (TFloat as t) ->
    (* Here we must convert everything to float first, then divide and
     * take the floor: *)
    Printf.fprintf oc "(let x_ = " ;
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".div")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2] ;
    Printf.fprintf oc " in if x_ >= 0. then floor x_ else ceil x_)"
  | Finalize, Stateless (SL2 (Div, e1, e2)), (TFloat as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".div")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Reldiff, e1, e2)), TFloat ->
    emit_functionN ~env ~opc ~nullable "reldiff"
      [Some TFloat, PropagateNull; Some TFloat, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Pow, e1, e2)), (TFloat|TI32|TI64 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".( ** )")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Pow, e1, e2)), (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI128 as t) ->
    (* For all others we exponentiate via floats: *)
    Printf.fprintf oc "(%t %a)"
      (conv_from_to ~nullable TFloat t)
      (emit_functionN ~env ~opc ~nullable "( ** )"
        [Some TFloat, PropagateNull; Some TFloat, PropagateNull])  [e1; e2]

  | Finalize, Stateless (SL2 (Trunc, e1, e2)), (TFloat as t) ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.Truncate.float"
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Trunc, e1, e2)), (TU8|TU16|TU32|TU64|TU128 as t) ->
    let m = omod_of_type t in
    let f =
      Printf.sprintf "CodeGenLib.Truncate.uint %s.div %s.mul" m m in
    emit_functionN ~env ~opc ~nullable f
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Trunc, e1, e2)), (TI8|TI16|TI32|TI128 as t) ->
    let m = omod_of_type t in
    let f =
      Printf.sprintf
        "CodeGenLib.Truncate.int %s.sub %s.compare %s.zero %s.div %s.mul"
        m m m m m in
    emit_functionN ~env ~opc ~nullable f
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]

  | Finalize, Stateless (SL2 (Mod, e1, e2)),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".rem")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Mod, e1, e2)), (TFloat as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".modulo")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Strftime, e1, e2)), TString ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.strftime"
      [Some TString, PropagateNull; Some TFloat, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL1 (Strptime, e)), TFloat ->
    emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
      "(fun t_ -> time_of_abstime t_ |> nullable_of_option)"
        [Some TString, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Variant, e)), TString ->
    emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
      "CodeGenLib.get_variant" [Some TString, PropagateNull] oc [e]

  | Finalize, Stateless (SL1 (Abs, e)),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".abs")
      [Some t, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Minus, e)),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".neg")
      [Some t, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Exp, e)), TFloat ->
    emit_functionN ~env ~opc ~nullable "exp"
      [Some TFloat, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Log, e)), TFloat ->
    emit_functionN ~env ~opc ~nullable "log"
      [Some TFloat, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Log10, e)), TFloat ->
    emit_functionN ~env ~opc ~nullable "log10"
      [Some TFloat, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Sqrt, e)), TFloat ->
    emit_functionN ~env ~opc ~nullable "sqrt"
      [Some TFloat, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Sq, e)), t ->
    let f = "(CodeGenLib.square "^ omod_of_type e.typ.structure ^".mul)" in
    emit_functionN ~env ~opc ~nullable f
      [Some t, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Ceil, e)), TFloat ->
    emit_functionN ~env ~opc ~nullable "ceil"
      [Some TFloat, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Floor, e)), TFloat ->
    emit_functionN ~env ~opc ~nullable "floor"
      [Some TFloat, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Round, e)), TFloat ->
    emit_functionN ~env ~opc ~nullable "Float.round"
      [Some TFloat, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Hash, e)), TI64 ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.hash"
      [None, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Sparkline, e)), TString ->
    emit_functionN ~env ~opc ~nullable "sparkline"
      [Some (TVec (0, T.make ~nullable:false TFloat)), PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (BeginOfRange, e)), TIpv4 ->
    emit_functionN ~env ~opc ~nullable "RamenIpv4.Cidr.first"
      [Some TCidrv4, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (BeginOfRange, e)), TIpv6 ->
    emit_functionN ~env ~opc ~nullable "RamenIpv6.Cidr.first"
      [Some TCidrv6, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (BeginOfRange, e)), TIp ->
    emit_functionN ~env ~opc ~nullable "RamenIp.first"
      [Some TCidr, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (EndOfRange, e)), TIpv4 ->
    emit_functionN ~env ~opc ~nullable "RamenIpv4.Cidr.last"
      [Some TCidrv4, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (EndOfRange, e)), TIpv6 ->
    emit_functionN ~env ~opc ~nullable "RamenIpv6.Cidr.last"
      [Some TCidrv6, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (EndOfRange, e)), TIp ->
    emit_functionN ~env ~opc ~nullable "RamenIp.last"
      [Some TCidr, PropagateNull] oc [e]

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
        ~impl_return_nullable:ts.(n).nullable
        [None, PropagateNull] oc [e]
    in
    (match e.E.typ.structure with
    | TVec (_, t) | TList t ->
        let func = "(fun a_ n_ -> Array.get a_ (Int32.to_int n_))" in
        emit_functionN ~env ~opc ~nullable func
          ~impl_return_nullable:t.nullable
          [None, PropagateNull; Some TI32, PropagateNull] oc [e; n]
    | TTuple ts ->
        let n = E.int_of_const n |>
                option_get "Get from tuple must have const index" __LOC__ in
        emit_select_from_tuple ts n
    | TRecord kts ->
        let s = E.string_of_const n |>
                option_get "Get from structure must have const str index" __LOC__ in
        let pos_of_field =
          try array_rfindi (fun (k, _) -> k = s) kts
          with Not_found ->
            Printf.sprintf2 "Invalid field name %S (have %a)"
              s
              (pretty_array_print (fun oc (k, _) -> String.print oc k)) kts |>
            failwith in
        let ts = Array.map snd kts in
        emit_select_from_tuple ts pos_of_field
    | TMap (k, _v) ->
        (* All gets from a map are nullable: *)
        emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
          "CodeGenLib.Globals.map_get"
          [ None, PropagateNull ;
            Some k.T.structure, PropagateNull ] oc [ e ; n ]
    | _ -> assert false)

  (* Other stateless functions *)
  | Finalize, Stateless (SL2 (Ge, e1, e2)), TBool ->
    emit_functionN ~env ~opc ~nullable "(>=)"
      [Some TAny, PropagateNull; Some TAny, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Gt, e1, e2)), TBool ->
    emit_functionN ~env ~opc ~nullable "(>)"
      [Some TAny, PropagateNull; Some TAny, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Eq, e1, e2)), TBool ->
    emit_functionN ~env ~opc ~nullable "(=)"
      [Some TAny, PropagateNull; Some TAny, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Concat, e1, e2)), TString ->
    emit_functionN ~env ~opc ~nullable "(^)"
      [Some TString, PropagateNull; Some TString, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (StartsWith, e1, e2)), TBool ->
    emit_functionN ~env ~opc ~nullable "String.starts_with"
      [Some TString, PropagateNull; Some TString, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (EndsWith, e1, e2)), TBool ->
    emit_functionN ~env ~opc ~nullable "String.ends_with"
      [Some TString, PropagateNull; Some TString, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL1 (Like p, e)), TBool ->
    let pattern = Globs.compile ~star:'%' ~placeholder:'_' ~escape:'\\' p in
    Printf.fprintf oc "(let pattern_ = Globs.%a in "
      Globs.print_pattern_ocaml pattern ;
    emit_functionN ~env ~opc ~nullable "Globs.matches pattern_ "
      [Some TString, PropagateNull] oc [e];
    Printf.fprintf oc ")"
  | Finalize, Stateless (SL1 (Length, e)), TU32 when E.is_a_string e ->
    emit_functionN ~env ~opc ~nullable "(Uint32.of_int % String.length)"
      [Some TString, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Length, e)), TU32 when E.is_a_list e ->
    emit_functionN ~env ~opc ~nullable "(Uint32.of_int % Array.length)"
      [None, PropagateNull] oc [e]
  (* lowercase and uppercase assume latin1 and will gladly destroy UTF-8
   * encoded char, therefore we use the ascii variants: *)
  | Finalize, Stateless (SL1 (Lower, e)), TString ->
    emit_functionN ~env ~opc ~nullable "String.lowercase_ascii"
      [Some TString, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Upper, e)), TString ->
    emit_functionN ~env ~opc ~nullable "String.uppercase_ascii"
      [Some TString, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (UuidOfU128, e)), TString ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.uuid_of_u128"
      [Some TU128, PropagateNull] oc [e]

  (* And and Or does not inherit nullability from their arguments the way
   * other functions does: given only one value we may be able to find out
   * the result without looking at the other one (that can then be NULL). *)
  (* FIXME: anyway, we would like AND and OR to shortcut the evaluation of
   * their argument when the result is known, so we must not use
   * [emit_functionN] but craft our own version here. *)
  | Finalize, Stateless (SL2 (And, e1, e2)), TBool ->
    if nullable then
      emit_functionN ~env ~opc ~nullable "CodeGenLib.and_opt"
        ~impl_return_nullable:true
        [Some TBool, PassAsNull; Some TBool, PassAsNull] oc [e1; e2]
    else
      emit_functionN ~env ~opc ~nullable "(&&)"
        [Some TBool, PropagateNull; Some TBool, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Or, e1,e2)), TBool ->
    if nullable then
      emit_functionN ~env ~opc ~nullable "CodeGenLib.or_opt"
        ~impl_return_nullable:true
        [Some TBool, PassAsNull; Some TBool, PassAsNull] oc [e1; e2]
    else
      emit_functionN ~env ~opc ~nullable "(||)"
        [Some TBool, PropagateNull; Some TBool, PropagateNull] oc [e1; e2]

  | Finalize, Stateless (SL2 ((BitAnd|BitOr|BitXor as op), e1, e2)),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    let n = match op with BitAnd -> "logand" | BitOr -> "logor"
                        | _ -> "logxor" in
    emit_functionN ~env ~opc ~nullable
      (omod_of_type t ^"."^ n)
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (BitShift, e1, e2)),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ~env ~opc ~nullable
      ("CodeGenLib.Shift."^ omod_of_type t ^".shift")
      [Some t, PropagateNull; Some TI16, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL1 (Not, e)), TBool ->
    emit_functionN ~env ~opc ~nullable "not"
      [Some TBool, PropagateNull] oc [e]
  | Finalize, Stateless (SL1 (Defined, e)), TBool ->
    (* Do not call emit_functionN to avoid null propagation: *)
    Printf.fprintf oc "(match %a with Null -> false | _ -> true)"
      (emit_expr ~env ~context ~opc) e
  | Finalize, Stateless (SL1 (Age, e)),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as to_typ)
  | Finalize, Stateless (SL1 (BeginOfRange, e)),
    (TCidrv4 | TCidrv6 as to_typ) ->
    let in_type_name =
      String.lowercase (IO.to_string print_structure to_typ) in
    let name = "CodeGenLib.age_"^ in_type_name in
    emit_functionN ~env ~opc ~nullable name
      [Some to_typ, PropagateNull] oc [e]
  (* TODO: Now() for Uint62? *)
  | Finalize, Stateless (SL0  Now), TFloat ->
    String.print oc "!CodeGenLib.now"
  | Finalize, Stateless (SL0 Random), TFloat ->
    String.print oc "(Random.float 1.)"
  | Finalize, Stateless (SL0 Pi), TFloat ->
    String.print oc "Float.pi"
  | Finalize, Stateless (SL0 EventStart), TFloat ->
    Printf.fprintf oc "((%a) |> fst)" emit_event_time opc
  | Finalize, Stateless (SL0 EventStop), TFloat ->
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
    let add_nullable = not from.nullable && nullable in
    if add_nullable then Printf.fprintf oc "NotNull (" ;
    Printf.fprintf oc "(%t) (%a)"
      (conv_from_to ~nullable:from.nullable from.structure t)
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
        (omod_of_type t.T.structure)
        (string_of_endianness endianness)
        0 (* TODO: add that offset to PEEK? *))
      [ Some TString, PropagateNull ] oc [ x ] ;
    String.print oc " with _ -> Null)"

  (* Similarly to the above, but reading from an array of integers instead
   * of from a string. *)
  | Finalize, Stateless (SL1 ((Peek (t, endianness)), e)), _ ->
    let omod_res = omod_of_type t.T.structure  in
    let inp_typ =
      match e.E.typ.structure with
      | T.TVec (_, t) -> t
      | _ -> assert false (* Bug in type checking *) in
    let inp_width = T.bits_of_structure inp_typ.structure
    and res_width = T.bits_of_structure t.structure in
    emit_functionN ~env ~opc ~nullable
      (Printf.sprintf
        "(CodeGenLib.IntOfArray.%s \
           %s.logor %s.shift_left %d %d %s.zero %s.of_uint%d)"
        (string_of_endianness endianness)
        omod_res omod_res inp_width res_width omod_res omod_res inp_width)
      [ Some e.E.typ.structure, PropagateNull ] oc [ e ]

  | Finalize, Stateless (SL1 (Chr, e)), _ ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.chr"
      [Some TU32, PropagateNull ] oc [e]

  | Finalize, Stateless (SL1s (Max, es)), t ->
    emit_functionN ~opc ~args_as:(Array 0) ~env ~nullable
      "Array.max" (List.map (fun _ -> Some t, PropagateNull) es) oc es
  | Finalize, Stateless (SL1s (Min, es)), t ->
    emit_functionN ~opc ~args_as:(Array 0) ~env ~nullable
      "Array.min" (List.map (fun _ -> Some t, PropagateNull) es) oc es
  | Finalize, Stateless (SL1s (Print, es)), _ ->
    (* We want to print nulls as well, so we make all parameters optional
     * strings: *)
    (match es with
    | [] -> ()
    | e::es ->
        Printf.fprintf oc
          "(let x0_ = %a in CodeGenLib.print (%s(%t x0_)::%a) ; x0_)"
          (emit_expr ~env ~context ~opc) e
          (if e.E.typ.nullable then "" else "NotNull ")
          (conv_from_to ~nullable:e.typ.nullable e.E.typ.structure TString)
          (List.print (fun oc e ->
             Printf.fprintf oc "%s(%a)"
               (if e.E.typ.nullable then "" else "NotNull ")
               (conv_to ~env ~context ~opc (Some TString)) e)) es)
  (* IN can have many meanings: *)
  | Finalize, Stateless (SL2 (In, e1, e2)), TBool ->
    (match e1.E.typ.structure, e2.E.typ.structure with
    | TIpv4, TCidrv4 ->
      emit_functionN ~env ~opc ~nullable "RamenIpv4.Cidr.is_in"
        [Some TIpv4, PropagateNull; Some TCidrv4, PropagateNull] oc [e1; e2]
    | TIpv6, TCidrv6 ->
      emit_functionN ~env ~opc ~nullable "RamenIpv6.Cidr.is_in"
        [Some TIpv6, PropagateNull; Some TCidrv6, PropagateNull] oc [e1; e2]
    | (TIpv4|TIpv6|TIp), (TCidrv4|TCidrv6|TCidr) ->
      emit_functionN ~env ~opc ~nullable "RamenIp.is_in"
        [Some TIp, PropagateNull; Some TCidr, PropagateNull] oc [e1; e2]
    | (TIpv4|TIpv6|TIp), (
          TVec (_, ({ structure = (TCidrv4|TCidrv6|TCidr) ; _ } as t))
        | TList ({ structure = (TCidrv4|TCidrv6|TCidr) ; _ } as t)
      ) ->
      emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
        (if t.nullable then "RamenIp.is_in_list_of_nullable"
                       else "RamenIp.is_in_list")
        (* We want to know that NULL is not in [], so we pass everything
         * as nullable to the function, that will deal with it. *)
        [ Some TIp, PassAsNull ;
          Some (TList { structure = TCidr ; nullable = t.nullable }), PassAsNull ]
        oc [ e1 ; e2 ]
    | TString, TString ->
      emit_functionN ~env ~opc ~nullable "String.exists"
        [Some TString, PropagateNull; Some TString, PropagateNull] oc [e2; e1]
    | t1, (TVec (_, t) | TList t) ->
      let emit_in csts_len csts_hash_init non_csts =
        (* We make a constant hash with the constants. Note that when e1 is
         * also a constant the OCaml compiler could optimize the whole
         * "x=a||x=b||x=b..." operation but only if not too many conversions
         * are involved, so we take no risk and build the hash in any case.
         * Typing only enforce that t1 < t or t > t1 (so we can look for an u8
         * in a set of i32, or the other way around which both make sense).
         * Here for simplicity all values will be converted to the largest of
         * t and t1: *)
        let larger_t = large_enough_for t.structure t1 in
        (* Note re. nulls: we are going to emit code such as "A=x1||A=x2" in
         * lieu of "A IN [x1; x2]". Notice that nulls do not propagate from
         * the xs in case A is found in the set, but do if it is not; Indeed,
         * "1 IN [1;NULL]" is true but "2 IN [1;NULL]" is NULL. If A is NULL
         * though, then the result is NULL unless the set is empty:
         * "NULL in [1; 2]" is NULL, but "NULL in []" is false. *)
        if e1.typ.nullable then
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
            (if e2.typ.nullable then
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
            if e.E.typ.nullable (* not possible ATM *) then (
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
            typ = { structure = (TVec (_, telem) | TList telem) ; _ } ; _ } ->
        (* Unlike the above case of an immediate list of items, here e2 may be
         * nullable so we have to be more cautious. If it's nullable and
         * actually null then the size of the constant hash we need is 0: *)
        let csts_len =
          if e2.typ.nullable then
            Printf.sprintf2
              "(match (%a) with Null -> 0 | NotNull x_ -> Array.length x_)"
              (emit_expr ~env ~context:Finalize ~opc) e2
          else
            Printf.sprintf2 "Array.length (%a)"
              (emit_expr ~env ~context:Finalize ~opc) e2
        and csts_hash_init larger_t =
          Printf.sprintf2
            "Array.iter (fun e_ -> Hashtbl.replace h_ (%t e_) ()) (%t)"
            (conv_from_to ~nullable:telem.nullable telem.structure larger_t)
            (fun oc ->
              if e2.typ.nullable then
                Printf.fprintf oc
                  "(match (%a) with Null -> [||] | NotNull x_ -> x_)"
                  (emit_expr ~env ~context:Finalize ~opc) e2
              else
                emit_expr ~env ~context:Finalize ~opc oc e2)
        in
        emit_in csts_len csts_hash_init []
      | _ -> assert false)
    | _ -> assert false)

  | Finalize, Stateless (SL2 (Percentile, lst, percs)), TVec _ ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.Percentile.multi"
      [Some (TVec (0, T.make ~nullable:false TFloat)), PropagateNull;
       None, PropagateNull] oc
      [percs; lst]
  | Finalize, Stateless (SL2 (Percentile, lst, percs)), _ ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.Percentile.single"
      [Some TFloat, PropagateNull; None, PropagateNull] oc
      [percs; lst]

  | Finalize, Stateless (SL2 (Index, s, a)), _ ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.index"
      [ Some TString, PropagateNull ;
        Some TChar, PropagateNull ] oc [s; a]

  | Finalize, Stateless (SL3 (SubString, s, a, b)), _ ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.substring"
      [ Some TString, PropagateNull ;
        Some TI32, PropagateNull ;
        Some TI32, PropagateNull ] oc [s; a; b]

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
    (match m.E.typ.T.structure with
    | TMap (ktyp, vtyp) ->
        emit_functionN ~env ~opc ~nullable "CodeGenLib.Globals.map_set"
          [ None, PropagateNull ;
            Some ktyp.structure, PropagateNull ;
            Some vtyp.structure, PropagateNull ] oc [ m ; k ; v ]
    | _ ->
        assert false (* If type checker did its job *))

  | Finalize, Stateless (SL1 (Fit, e1)), TFloat ->
    (* [e1] is supposed to be a list/vector of scalars or tuples of scalars.
     * All items of those tuples are supposed to be numeric, so we convert
     * all of them into floats and then proceed with the regression.  *)
    let ts =
      match e1.E.typ.T.structure with
      | TList { structure = TTuple ts ; _ }
      | TVec (_, { structure = TTuple ts ; _ }) ->
          ts
      | TList numeric
      | TVec (_, numeric)
        when T.is_numeric numeric.T.structure ->
          [| numeric |]
      | _ ->
          !logger.error
            "Type-checking failed to ensure Fit argument is a sequence" ;
          assert false in
    (* Convert the argument into a nullable list of nullable vectors
     * of non-nullable floats: *)
    let t =
      T.TList {
        structure = TVec (Array.length ts,
                          { structure = TFloat ; nullable = false }) ;
        nullable = true } in
    emit_functionN ~env ~opc ~nullable ~impl_return_nullable:true
      "CodeGenLib.LinReg.fit" [ Some t, PropagateNull ] oc [ e1 ]

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
        match e.E.typ.structure with
        | TList t | TVec (_, t) -> t
        | _ -> assert false in
      let e' =
        E.make ~nullable:item_typ.nullable ~structure:item_typ.structure
               (Binding (Direct var_name)) in
      (* By reporting the skip-null flag we make sure that each update will
       * skip the nulls in the list - while the list itself will make the
       * whole expression null if it's null. *)
      E.{ expr with text = Stateful (g, n, SF1 (aggr, e')) }
    in
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
    if e.E.typ.nullable then
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

  | InitState, Stateful (_, _, SF1 (AggrAnd, _)), (TBool as t) ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "%t true"
        (conv_from_to ~nullable:false TBool t))
  | UpdateState, Stateful (_, n, SF1 (AggrAnd, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "(&&)" oc [ Some TBool, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrAnd, _)), TBool ->
    finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []
  | InitState, Stateful (_, _, SF1 (AggrOr, _)), (TBool as t) ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "%t false"
        (conv_from_to ~nullable:false TBool t))
  | UpdateState, Stateful (_, n, SF1 (AggrOr, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "(||)" oc [ Some TBool, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrOr, _)), TBool ->
    finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []

  | InitState, Stateful (_, _, SF1 (AggrSum, _)), TFloat ->
    wrap_nullable ~nullable oc (fun oc ->
      String.print oc "CodeGenLib.kahan_init")
  | UpdateState, Stateful (_, n, SF1 (AggrSum, e)), (TFloat as t) ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.kahan_add" oc [ Some t, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrSum, _)), TFloat ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.kahan_finalize" [] oc []

  | InitState, Stateful (_, _, SF1 (AggrSum, _)),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "%t Uint8.zero"
        (conv_from_to ~nullable:false TU8 t))
  | UpdateState, Stateful (_, n, SF1 (AggrSum, e)),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      (omod_of_type t ^".add") oc [ Some t, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrSum, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []

  | InitState, Stateful (_, _, SF1 (AggrAvg, _)), TFloat ->
    wrap_nullable ~nullable oc (fun oc ->
      String.print oc "CodeGenLib.avg_init")
  | UpdateState, Stateful (_, n, SF1 (AggrAvg, e)), (TFloat as t) ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.avg_add" oc [ Some t, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrAvg, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.avg_finalize" [] oc []

  | InitState,
    Stateful (_, _, SF1 ((AggrFirst|AggrLast|AggrMax|AggrMin), _)), _ ->
    wrap_nullable ~nullable oc (fun oc ->
      String.print oc "Null")
  | UpdateState, Stateful (_, n, SF1 (AggrMax, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.aggr_max" oc [ None, PropagateNull ]
  | UpdateState, Stateful (_, n, SF1 (AggrMin, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.aggr_min" oc [ None, PropagateNull ]
  | UpdateState, Stateful (_, n, SF1 (AggrFirst, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.aggr_first" oc [ None, PropagateNull ]
  | UpdateState, Stateful (_, n, SF1 (AggrLast, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.aggr_last" oc [ None, PropagateNull ]
  | Finalize,
    Stateful (_, n, SF1 ((AggrFirst|AggrLast|AggrMax|AggrMin), _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state "nullable_get"
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
      "CodeGenLib.Histogram.add" oc [ Some TFloat, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrHistogram _, _)), TVec _ ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Histogram.finalize" [] oc []

  | InitState, Stateful (_, _, SF2 (Lag, k, e)), _ ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.Seasonal.init"
      [Some TU32, PropagateNull; Some TU32, PropagateNull;
       None, PropagateNull] oc
      [k; E.one (); any_constant_of_expr_type ~avoid_null:true e.E.typ]
  | UpdateState, Stateful (_, n, SF2 (Lag, _k, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.Seasonal.add" oc [ None, PropagateNull ]
  | Finalize, Stateful (_, n, SF2 (Lag, _, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Seasonal.lag" [] oc []

  (* We force the inputs to be float since we are going to return a float anyway. *)
  | InitState, Stateful (_, _, SF3 (MovingAvg, p, n, _)), TFloat ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.Seasonal.init"
      [Some TU32, PropagateNull; Some TU32, PropagateNull;
       Some TFloat, PropagateNull] oc
      [p; n; E.zero ()]
  | UpdateState, Stateful (_, n, SF3 (MovingAvg, _, _, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.Seasonal.add" oc [ Some TFloat, PropagateNull ]
  | Finalize, Stateful (_, n, SF3 (MovingAvg, p, m, _)), TFloat ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Seasonal.avg" [p; m] oc
      [Some TU32, PropagateNull; Some TU32, PropagateNull]
  | Finalize, Stateful (_, n, SF4s (MultiLinReg, p, m,_ ,_)), TFloat ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Seasonal.multi_linreg" [p; m] oc
      [Some TU32, PropagateNull; Some TU32, PropagateNull]

  | InitState, Stateful (_, _, SF4s (MultiLinReg, p, m, _, es)), TFloat ->
    emit_functionNv ~env ~opc ~nullable "CodeGenLib.Seasonal.init_multi_linreg"
      [Some TU32, PropagateNull; Some TU32, PropagateNull;
       Some TFloat, PropagateNull] [p; m; E.zero ()]
      (Some TFloat) oc (List.map (fun _ -> E.zero ()) es)
  | UpdateState, Stateful (_, n, SF4s (MultiLinReg, _p , _m, e, es)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      ~vars:es ~vars_to_typ:(Some TFloat)
      "CodeGenLib.Seasonal.add_multi_linreg" oc [ Some TFloat, PropagateNull ]

  | InitState, Stateful (_, _, SF2 (ExpSmooth, _a, _)), (TFloat as t) ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "%t Uint8.zero"
        (conv_from_to ~nullable:false TU8 t))
  | UpdateState, Stateful (_, n, SF2 (ExpSmooth, a, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ a ; e ]
      "CodeGenLib.smooth" oc
      [ Some TFloat, PropagateNull; Some TFloat, PropagateNull ]
  | Finalize, Stateful (_, n, SF2 (ExpSmooth, _, _)), TFloat ->
    finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []

  | InitState, Stateful (_, _, SF4 (DampedHolt, _, _, _, _)), _ ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.smooth_damped_holt_init" [] oc []
  | UpdateState, Stateful (_, n, SF4 (DampedHolt, a, l, f, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ a ; l ; f ; e ]
      "CodeGenLib.smooth_damped_holt" oc
        [ Some TFloat, PropagateNull;
          Some TFloat, PropagateNull;
          Some TFloat, PropagateNull;
          Some TFloat, PropagateNull]
  | Finalize, Stateful (_, n, SF4 (DampedHolt, _, _, f, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.smooth_damped_holt_finalize" [f] oc [Some TFloat, PropagateNull]

  | InitState, Stateful (_, _, SF6 (DampedHoltWinter, _, _, _, m, _, _)), _ ->
    emit_functionN ~env ~opc ~nullable
      "CodeGenLib.smooth_damped_holt_winter_init" [Some TU8, PropagateNull] oc [m]
  | UpdateState, Stateful (_, n, SF6 (DampedHoltWinter, a, b, g, m, f, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ a ; b ; g ; m ; f ; e ]
      "CodeGenLib.smooth_damped_holt_winter" oc
        [ Some TFloat, PropagateNull;
          Some TFloat, PropagateNull;
          Some TFloat, PropagateNull;
          Some TU8   , PropagateNull;
          Some TFloat, PropagateNull;
          Some TFloat, PropagateNull]
  | Finalize, Stateful (_, n, SF6 (DampedHoltWinter, _, _, _, _, f, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.smooth_damped_holt_winter_finalize" [f] oc [Some TFloat, PropagateNull]

  | InitState, Stateful (_, _, SF4s (Remember, fpr,_tim, dur,_es)), TBool ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.Remember.init"
      [Some TFloat, PropagateNull; Some TFloat, PropagateNull] oc [fpr; dur]
  | UpdateState, Stateful (_, n, SF4s (Remember, _fpr, tim, _dur, es)), _ ->
    update_state ~env ~opc ~nullable n my_state (tim :: es)
      ~args_as:(Tuple 2) "CodeGenLib.Remember.add" oc
      ((Some TFloat, PropagateNull) :: List.map (fun _ -> None, PropagateNull) es)
  | Finalize, Stateful (_, n, SF4s (Remember, _, _, _, _)), TBool ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Remember.finalize" [] oc []

  | InitState, Stateful (_, _, SF1s (Distinct, _es)), _ ->
    wrap_nullable ~nullable oc (fun oc ->
      String.print oc "CodeGenLib.Distinct.init ()")
  | UpdateState, Stateful (_, n, SF1s (Distinct, es)), _ ->
    update_state ~env ~opc ~nullable n my_state es
      ~args_as:(Tuple 1) "CodeGenLib.Distinct.add" oc
      (List.map (fun _ -> None, PropagateNull) es)
  | Finalize, Stateful (_, n, SF1s (Distinct, _)), TBool ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Distinct.finalize" [] oc []

  | InitState, Stateful (_, _, SF3 (Hysteresis, _, _, _)), t ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "%t true" (* Initially within bounds *)
        (conv_from_to ~nullable:false TBool t))
  | UpdateState, Stateful (_, n, SF3 (Hysteresis, meas, accept, max)), TBool ->
    (* TODO: shouldn't we promote everything to the most accurate of those types? *)
    let t = meas.E.typ.structure in
    update_state ~env ~opc ~nullable n my_state [ meas ; accept ; max ]
      "CodeGenLib.Hysteresis.add " oc
      [Some t, PropagateNull; Some t, PropagateNull; Some t, PropagateNull]
  | Finalize, Stateful (_, n, SF3 (Hysteresis, _, _, _)), TBool ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Hysteresis.finalize" [] oc []

  | InitState, Stateful (_, _, Top { c ; duration ; max_size ; _ }), _ ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "CodeGenLib.Top.init (%a) (%a)"
        (* Default max_size is ten times c: *)
        (fun oc -> function
          | None ->
              Printf.fprintf oc "Uint32.mul (Uint32.of_int 10) (%a)"
                (conv_to ~env ~context:Finalize ~opc (Some TU32)) c
          | Some s -> conv_to ~env ~context:Finalize ~opc (Some TU32) oc s) max_size
        (* duration can also be a parameter compatible to float: *)
        (conv_to ~env ~context:Finalize ~opc (Some TFloat)) duration)
  | UpdateState, Stateful (_, n, Top { what ; by ; time ; _ }), _ ->
    update_state ~env ~opc ~nullable n my_state (time :: by :: what)
      ~args_as:(Tuple 3) "CodeGenLib.Top.add" oc
      ((Some TFloat, PropagateNull) :: (Some TFloat, PropagateNull) :: List.map (fun _ -> None, PropagateNull) what)
  | Finalize, Stateful (_, n, Top { want_rank = true ; c ; what ; _ }), t ->
    finalize_state ~env ~opc ~nullable n my_state
      ~impl_return_nullable:true ~args_as:(Tuple 1)
      ("(fun s_ n_ x_ -> \
           CodeGenLib.Top.rank s_ n_ x_ |> \
           nullable_map "^ omod_of_type t ^".of_int)")
      (c :: what) oc ((Some TU32, PropagateNull) :: List.map (fun _ -> None, PropagateNull) what)
  | Finalize, Stateful (_, n, Top { want_rank = false ; c ; what ; _ }), _ ->
    finalize_state ~env ~opc ~nullable n my_state
      ~args_as:(Tuple 2)
      "CodeGenLib.Top.is_in_top"
      (c :: what) oc ((Some TU32, PropagateNull) :: List.map (fun _ -> None, PropagateNull) what)

  | InitState, Stateful (_, _, SF4s (Largest { inv ; up_to }, c, but, _, _)), _ ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "CodeGenLib.Largest.init ~inv:%b ~up_to:%b ~but:(%a) (%a)"
        inv up_to
        (conv_to ~env ~context:Finalize ~opc (Some TU32)) but
        (conv_to ~env ~context:Finalize ~opc (Some TU32)) c)
  (* Special updater that use the internal count when no `by` expressions
   * are present: *)
  | UpdateState, Stateful (_, n, SF4s (Largest _, _, _, e, [])), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.Largest.add_on_count" oc [ None, PassNull ]
  | UpdateState, Stateful (_, n, SF4s (Largest _, _, _, e, es)), _ ->
    update_state ~env ~opc ~nullable n my_state (e :: es)
      ~args_as:(Tuple 2) "CodeGenLib.Largest.add" oc
      ((None, PassNull) :: List.map (fun _ -> None, PassNull) es)
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
        (conv_to ~env ~opc ~context:Finalize (Some TU32)) c
        (emit_expr ~env ~context:Finalize ~opc) init_c)
  | UpdateState, Stateful (_, n, SF2 (Sample, _, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "RamenSampling.add" oc [ None, PassNull ]
  | Finalize, Stateful (_, n, SF2 (Sample, _, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state
      ~impl_return_nullable:true
      "RamenSampling.finalize" [] oc []

  | InitState, Stateful (_, n, Past { what ; max_age ; sample_size ; _ }), _ ->
    let init_c =
      let c_typ = what.E.typ in
      let c_typ = if n then { c_typ with nullable = false } else c_typ in
      any_constant_of_expr_type c_typ in
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "CodeGenLib.Past.init (%a) (%a) (%a)"
        (conv_to ~env ~context:Finalize ~opc (Some TFloat)) max_age
        (Option.print (fun oc sz ->
          (* Would be nicer if conv_to would handle the parenth itself *)
          Printf.fprintf oc "(%a)"
            (conv_to ~env ~context:Finalize ~opc (Some TU32)) sz))
          sample_size
        (emit_expr ~env ~context:Finalize ~opc) init_c)
  | UpdateState, Stateful (_, n, Past { what ; time ; _ }), _ ->
    update_state ~env ~opc ~nullable n my_state [ what ; time ]
      "CodeGenLib.Past.add" oc [ None, PassNull ; Some TFloat, PropagateNull ]
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
      "CodeGenLib.Group.add" oc [ None, PassNull ]
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
    if e.typ.structure = TBool then
      update_state ~env ~opc ~nullable n my_state [ e ]
        "CodeGenLib.Count.count_true" oc [ Some TBool, PropagateNull ]
    else
      update_state ~env ~opc ~nullable n my_state []
        "CodeGenLib.Count.count_anything" oc []
  | Finalize, Stateful (_, n, SF1 (Count, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []

  (* Generator: the function appears only during tuple generation, where
   * it sends the output to its continuation as (freevar_name expr).
   * In normal expressions we merely refer to that free variable. *)
  | Generator, Generator (Split (e1,e2)), TString ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.split"
      [Some TString, PropagateNull; Some TString, PropagateNull] oc [e1; e2]
  | Finalize, Generator (Split (_e1,_e2)), TString -> (* Output it as a free variable *)
    String.print oc (freevar_name expr)

  | _, _, _ ->
    let m =
      Printf.sprintf "Cannot find implementation of %s for context %s"
        (IO.to_string (E.print true) expr)
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
   * have to form the complete list of types. *)
  let merge_types t1 t2 =
    match t1, t2 with
    | None, t | t, None -> t
    | Some t1, Some t2 -> Some (large_enough_for t1 t2) in
  let rec loop ht rt rpn any_type n = function
  | [], _ -> (* No more arguments *)
    (* Replace all None types by a common type large enough to accommodate
     * them all: any_type. *)
    let ht =
      List.map (fun (t, null_prop) ->
        (if t <> Some TAny then t else any_type), null_prop
      ) ht in
    List.rev_append ht (List.init n (fun _ -> rt, rpn))
  | e::es, (t, null_prop)::ts ->
    let any_type =
      if t <> Some TAny then any_type else
      merge_types any_type (Some e.E.typ.structure) in
    loop ((t, null_prop)::ht) t null_prop any_type n (es, ts)
  | e::es, [] -> (* Missing some types: update rt *)
    let te = Some e.E.typ.structure in
    if rt = Some TAny then
      loop ht rt rpn (merge_types any_type te) (n+1) (es, [])
    else
      loop ht (merge_types rt te) rpn any_type (n+1) (es, [])
  in
  loop [] None PropagateNull None 0 (es, arg_typs)

(*$inject
  open Batteries
  open Stdint
  open RamenTypes
  let const structure v =
    RamenExpr.make ~structure ~nullable:false (Const v)
 *)
(*$= add_missing_types & ~printer:dump
  [Some TFloat, PropagateNull] \
    (add_missing_types [Some TFloat, PropagateNull] [const TFloat (VFloat 1.)])
  [Some TFloat, PropagateNull] \
    (add_missing_types [] [const TFloat (VFloat 1.)])

  [Some TFloat, PropagateNull; Some TU8, PropagateNull] \
    (add_missing_types [Some TFloat, PropagateNull; Some TU8, PropagateNull] [const TFloat (VFloat 1.); const TU8 (VU8 (Uint8.of_int 42))])

  [Some TFloat, PropagateNull; Some TU16, PropagateNull; Some TU16, PropagateNull] \
    (add_missing_types [Some TFloat, PropagateNull; Some TU16, PropagateNull] [const TFloat (VFloat 1.); const TU8 (VU8 (Uint8.of_int 42)); const TU8 (VU8 (Uint8.of_int 42))])

  [Some TFloat, PropagateNull; Some TU16, PropagateNull; Some TU16, PropagateNull] \
    (add_missing_types [Some TFloat, PropagateNull; Some TU16, PropagateNull] [const TFloat (VFloat 1.); const TU8 (VU8 (Uint8.of_int 42)); const TU16 (VU16 (Uint16.of_int  42))])

  [Some TFloat, PropagateNull; Some TU16, PropagateNull; Some TU16, PropagateNull] \
    (add_missing_types [Some TFloat, PropagateNull; Some TU16, PropagateNull] [const TFloat (VFloat 1.); const TU16 (VU16 (Uint16.of_int 42)); const TU8 (VU8 (Uint8.of_int 42))])

  [Some TFloat, PropagateNull; Some TU16, PropagateNull; Some TU16, PropagateNull] \
    (add_missing_types [Some TFloat, PropagateNull; Some TAny, PropagateNull; Some TAny, PropagateNull] [const TFloat (VFloat 1.); const TU16 (VU16 (Uint16.of_int 42)); const TU8 (VU8 (Uint8.of_int 42))])

  [Some TFloat, PropagateNull; Some TU16, PropagateNull; Some TU16, PropagateNull] \
    (add_missing_types [Some TFloat, PropagateNull; Some TAny, PropagateNull; Some TAny, PropagateNull] [const TFloat (VFloat 1.); const TU8 (VU8 (Uint8.of_int 42)); const TU16 (VU16 (Uint16.of_int 42))])

  [None, PropagateNull; Some TFloat, PropagateNull] \
    (add_missing_types [None, PropagateNull; Some TFloat, PropagateNull] [const TFloat (VFloat 1.); const TFloat (VFloat 1.)])
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
      if e.E.typ.nullable then (
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
      ) else ( (* [e] not nullable *)
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
        "nullable_get (", ")" in
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
                 (conv_to ~env ~context:Finalize ~opc vt) oc ves)
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
  if typ.T.nullable then (
    p "(" ;
    p "  match %s with Null -> 0" var ;
    p "  | NotNull %s ->" var ;
    emit_sersize_of_var (indent + 3) { typ with nullable = false } oc var ;
    p ")"
  ) else (
    let emit_for_record kts =
      let nullmask_sz = RingBufLib.nullmask_sz_of_record kts in
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
    match typ.T.structure with
    | TTuple ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_for_record kts
    | TRecord kts ->
        emit_for_record kts
    | TVec (d, t) ->
        let nullmask_sz = RingBufLib.nullmask_sz_of_vector d in
        for i = 0 to d-1 do
          let item_var = var ^"_"^ string_of_int i in
          p "(" ;
          p "  let %s = %s.(%d) in" item_var var i ;
          emit_sersize_of_var (indent + 1) t oc item_var ;
          p ") +"
        done ;
        p "%d" nullmask_sz
    | TString ->
      p "(RingBufLib.sersize_of_string %s)" var
    | TIp ->
      p "(RingBufLib.sersize_of_ip %s)" var
    | TCidr ->
      p "(RingBufLib.sersize_of_cidr %s)" var
    | TList t ->
      (* So var is the name of an array of some values of type t, which can
       * be a constructed type which sersize can't be known statically.
       * So at first sight we have to generate code that will iter through
       * the values and for each, know or compute its size, etc. This is
       * what we do here, but in cases where t has a well known sersize we
       * could generate much faster code of course: *)
      p "(" ;
      p "  Array.fold_left (fun s_ v_ -> s_ +" ;
      emit_sersize_of_var (indent + 2) t oc "v_" ;
      p "  ) (%d + RingBufLib.nullmask_sz_of_vector (Array.length %s)) %s"
        (* start from the size prefix and nullmask: *)
        RingBufLib.sersize_of_u32 var var ;
      p ")"
    | _ ->
      p "%a" emit_sersize_of_fixsz_typ typ.T.structure
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
  if is_scalar typ.structure then (
    p "let %s =" out_var ;
    p "  if %s = RamenFieldMask.Copy then (" fm_var ;
    copy (indent + 2) oc (val_var, typ) ;
    p "  ) else (" ;
    p "    assert (%s = RamenFieldMask.Skip) ;" fm_var ;
    skip (indent + 2) oc (val_var, typ) ;
    p "  ) in"
  ) else (
    let emit_for_record kts =
      p "let %s =" out_var ;
      p "  match %s with" fm_var ;
      p "  | RamenFieldMask.Copy ->" ;
      copy (indent + 3) oc (val_var, typ) ;
      p "  | RamenFieldMask.Skip ->" ;
      skip (indent + 3) oc (val_var, typ) ;
      p "  | RamenFieldMask.Rec fm_ ->" ;
      (* Destructure the tuple, propagating Nulls: *)
      let item_var k = Printf.sprintf "tup_" ^ k |>
                       RamenOCamlCompiler.make_valid_ocaml_identifier in
      if typ.nullable then (
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
        let fm_var = Printf.sprintf "fm_.(%d)" i in
        emit_for_serialized_fields
          (indent + 4) t copy skip fm_var (item_var k) oc out_var
      ) ser ;
      p "      %s) in" out_var
    in
    match typ.structure with
    | TVec (_, t) | TList t ->
        p "let %s =" out_var ;
        p "  match %s with" fm_var ;
        p "  | RamenFieldMask.Copy ->" ;
        copy (indent + 3) oc (val_var, typ) ;
        p "  | RamenFieldMask.Skip ->" ;
        skip (indent + 3) oc (val_var, typ) ;
        p "  | RamenFieldMask.Rec fm_ ->" ;
        p "      Array.fold_lefti (fun %s i_ fm_ ->" out_var ;
        (* When we want to serialize subfields of a value that is null, we
         * have to serialize each subfield as null: *)
        let indent =
          if typ.nullable then (
            p "        match %s with Null ->" val_var ;
            skip (indent + 5) oc (val_var, typ) ;
            p "        | NotNull %s ->" val_var ;
            indent + 5
          ) else indent + 3 in
        let p fmt = emit oc indent fmt in
        if t.nullable then (
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
    | TTuple ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_for_record kts
    | TRecord kts ->
        emit_for_record kts
    | _ -> assert false (* no other non-scalar types *)
  )

let emit_for_serialized_fields_of_output
      indent typ copy skip fm_var oc out_var =
  let p fmt = emit oc indent fmt in
  RingBufLib.ser_tuple_typ_of_tuple_typ ~recursive:false typ |>
  List.iter (fun (ft, i) ->
    if not (N.is_private ft.name) then (
      p "(* Field %a *)" N.field_print ft.RamenTuple.name ;
      let val_var = id_of_field_typ ~tuple:Out ft in
      let fm_var = Printf.sprintf "%s.(%d)" fm_var i in
      emit_for_serialized_fields indent ft.typ copy skip fm_var val_var
                                 oc out_var))

(* Same as the above [emit_for_serialized_fields] but for when we do not know
 * the actual value, just its type. *)
let rec emit_for_serialized_fields_no_value
        indent typ copy skip fm_var oc out_var =
  let p fmt = emit oc indent fmt in
  if is_scalar typ.structure then (
    p "let %s =" out_var ;
    p "  if %s = RamenFieldMask.Copy then (" fm_var ;
    copy (indent + 2) oc typ ;
    p "  ) else (" ;
    p "    assert (%s = RamenFieldMask.Skip) ;" fm_var ;
    skip (indent + 2) oc typ ;
    p "  ) in"
  ) else (
    let emit_for_record kts =
      p "let %s =" out_var ;
      p "  match %s with" fm_var ;
      p "  | RamenFieldMask.Copy ->" ;
      copy (indent + 3) oc typ ;
      p "  | RamenFieldMask.Skip ->" ;
      skip (indent + 3) oc typ ;
      p "  | RamenFieldMask.Rec fm_ ->" ;
      let ser = RingBufLib.ser_order kts in
      array_print_i ~first:"" ~last:"" ~sep:"\n" (fun i oc (_, t) ->
        let fm_var = Printf.sprintf "fm_.(%d)" i in
        emit_for_serialized_fields_no_value
          (indent + 3) t copy skip fm_var oc out_var
      ) oc ser ;
      p "      %s in" out_var
    in
    match typ.structure with
    | TVec (_, t) | TList t ->
        p "let %s =" out_var ;
        p "  match %s with" fm_var ;
        p "  | RamenFieldMask.Copy ->" ;
        copy (indent + 3) oc typ ;
        p "  | RamenFieldMask.Skip ->" ;
        skip (indent + 3) oc typ ;
        p "  | RamenFieldMask.Rec fm_ ->" ;
        p "      Array.fold_lefti (fun %s i_ fm_ ->" out_var ;
        emit_for_serialized_fields_no_value
          (indent + 4) t copy skip "fm_" oc out_var ;
        p "        %s" out_var ;
        p "      ) %s fm_ in" out_var
    | TTuple ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_for_record kts
    | TRecord kts ->
        emit_for_record kts
    | _ -> assert false (* no other non-scalar types *)
  )

let emit_for_serialized_fields_of_output_no_value
      indent typ copy skip fm_var oc out_var =
  let p fmt = emit oc indent fmt in
  (* TODO: a fake record for emit_for_serialized_fields_no_value,
   * with out_var = a whole tuple. *)
  let ser_typ =
    RingBufLib.ser_tuple_typ_of_tuple_typ ~recursive:false typ in
  let num_ser_fields = List.length ser_typ in
  p "if Array.length %s <> %d && Array.length %s <> %d then ("
    fm_var num_ser_fields fm_var num_all_fields ;
  p "  !logger.error \"bad fieldmask of length %%d while serializing %d fields\""
    num_ser_fields ;
  p "    (Array.length %s) ;" fm_var ;
  p "    assert false" ;
  p ") ;" ;
  List.iter (fun (ft, i) ->
    if not (N.is_private ft.name) then (
      p "(* Field %a *)" N.field_print ft.RamenTuple.name ;
      let fm_var = Printf.sprintf "%s.(%d)" fm_var i in
      emit_for_serialized_fields_no_value
        indent ft.typ copy skip fm_var oc out_var)
  ) ser_typ

let emit_compute_nullmask_size indent fm_var oc typ =
  let p fmt = emit oc indent fmt in
  let copy indent oc typ =
    emit oc indent "%s" (if typ.nullable then "b_+1" else "b_")
  and skip indent oc _ =
    emit oc indent "b_" in
  p "let b_ = 0 in" ;
  emit_for_serialized_fields_of_output_no_value
    indent typ copy skip fm_var oc "b_" ;
  p "RingBuf.(round_up_to_rb_word (bytes_for_bits b_))"

(* The actual nullmask size will depend on the fieldmask which is known
 * only at runtime: *)
let emit_sersize_of_tuple indent name oc typ =
  let p fmt = emit oc indent fmt in
  (* Like for serialize_tuple, we receive first the fieldmask and then the
   * actual tuple, so we can compute the nullmask in advance: *)
  p "(* Compute the serialized size of a tuple of type:" ;
  p "     %a" RamenTuple.print_typ typ ;
  p "*)" ;
  p "let %s fieldmask_ =" name ;
  p "  let nullmask_bytes_ =" ;
  emit_compute_nullmask_size (indent + 2) "fieldmask_" oc typ ;
  p "    in" ;
  p "  assert (nullmask_bytes_ <= %d) ;"
    (RingBufLib.nullmask_bytes_of_tuple_type typ) ;
  p "  fun %a ->" (emit_tuple Out) typ ;
  p "    let sz_ = nullmask_bytes_ in" ;
  let copy indent oc (out_var, typ) =
    emit oc indent "sz_ +" ;
    emit_sersize_of_var (indent + 1) typ oc out_var
  and skip indent oc _ = emit oc indent "sz_" in
  emit_for_serialized_fields_of_output
    (indent + 2) typ copy skip "fieldmask_" oc "sz_" ;
  p "    sz_\n"

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
 * Then, for lists, vectors and tuples we have a small local nullmask
 * (for tuples, even for fields that are not nullable, FIXME). *)

(* Emits the code to serialize a value.
 * [start_offs_var] is where the nullmask can be found, while [offs_var] is the
 * current offset.
 * Outputs the new current offset and null bit offset: *)
let rec emit_serialize_value
    indent start_offs_var offs_var nulli_var val_var oc typ =
  let p fmt = emit oc indent fmt in
  if typ.T.nullable then (
    (* Write either nothing (since the nullmask is initialized with 0) or
     * the nullmask bit and the value *)
    p "(match %s with" val_var ;
    p "| Null -> %s, %s + 1" offs_var nulli_var ;
    p "| NotNull %s ->" val_var ;
    if verbose_serialization then
      p "!logger.debug \"Set nullmask bit %%d\" %s ;" nulli_var ;
    p "    RingBuf.set_bit tx_ %s %s ;" start_offs_var nulli_var ;
    p "    let offs_, nulli_ =" ;
    emit_serialize_value (indent + 3) start_offs_var offs_var "nulli_" val_var oc { typ with nullable = false} ;
    p "      in" ;
    p "     offs_, nulli_ + 1)"
  ) else (
    let emit_write_array indent _start_offs_var offs_var dim_var t =
      let p fmt = emit oc indent fmt in
      if verbose_serialization then
        p "!logger.debug \"Serializing an array of size %%d at offset %%d\" %s %s ;" dim_var offs_var ;
      p "(" ;
      p "  let nullmask_bytes_ = RingBufLib.nullmask_sz_of_vector %s in"
        dim_var ;
      p "  let start_arr_ = %s in" offs_var ;
      p "  RingBuf.zero_bytes tx_ start_arr_ nullmask_bytes_ ;" ;
      p "  let offs_ = start_arr_ + nullmask_bytes_ in" ;
      p "  let offs_, _ =" ;
      p "    Array.fold_left (fun (offs_, nulli_) v_ ->" ;
      emit_serialize_value (indent + 3) "start_arr_" "offs_" "nulli_" "v_" oc t ;
      p "    ) (offs_, 0) %s in" val_var ;
      p "  offs_, %s" nulli_var ;
      p ")"
    and emit_write_record indent _start_offs_var offs_var kts =
      let p fmt = emit oc indent fmt in
      let nullmask_sz = RingBufLib.nullmask_sz_of_record kts in
      if verbose_serialization then
        p "!logger.debug \"Serializing a tuple of %d elements at offset %%d (nullmask size=%d, %a)\" %s ;" (Array.length kts) nullmask_sz (Array.print (Tuple2.print String.print T.print_typ)) kts offs_var ;
      let item_var k = val_var ^"_"^ k |>
                       RamenOCamlCompiler.make_valid_ocaml_identifier in
      p "let %a = %s in"
        (array_print_as_tuple (fun oc (k, _) ->
          String.print oc (item_var k))) kts
        val_var ;
      p "let start_tup_ = %s in" offs_var ;
      if nullmask_sz > 0 then
        p "RingBuf.zero_bytes tx_ start_tup_ %d ;" nullmask_sz ;
      p "let offs_ = start_tup_ + %d (* nullmask *) in" nullmask_sz ;
      (* We must obviously serialize in serialization order: *)
      let ser = RingBufLib.ser_order kts in
      Array.iteri (fun i (k, t) ->
        p "let offs_, _ =" ;
        p "  let nulli_ = %d in" i ;
        emit_serialize_value (indent + 1) "start_tup_" "offs_" "nulli_" (item_var k) oc t ;
        p "  in"
      ) ser ;
      p "offs_, %s" nulli_var
    in
    match typ.T.structure with
    (* Constructed types: *)
    | TTuple ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        emit_write_record indent start_offs_var offs_var kts

    | TRecord kts ->
        emit_write_record indent start_offs_var offs_var kts

    | TVec (d, t) ->
        emit_write_array indent start_offs_var offs_var (string_of_int d) t

    | TList t ->
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
  p "let %s fieldmask_ =" name ;
  p "  let nullmask_bytes_ =" ;
  emit_compute_nullmask_size (indent + 2) "fieldmask_" oc typ ;
  p "    in" ;
  p "  fun tx_ start_offs_ %a ->" (emit_tuple Out) typ ;
  if verbose_serialization then
    p "    !logger.debug \"Serialize a tuple, nullmask_bytes=%%d\" nullmask_bytes_ ;" ;
  (* Callbacks [copy] and [skip] have to return the offset and null index
   * but we have several offsets and several null index (when copying full
   * compund types) ; we therefore enforce the rule that those variables
   * are always called "offs_" and "nulli_". *)
  p "    let offs_ = start_offs_ + nullmask_bytes_" ;
  p "    and nulli_ = 0 in" ;
  (*
   * Write a full value, updating offs_var and nulli_var:
   * [start_var]: where we write the value (and where the nullmask is, for
   *              values with a nullmask).
   *)
  (* Start by zeroing the nullmask *)
  p "    if nullmask_bytes_ > 0 then" ;
  p "      RingBuf.zero_bytes tx_ start_offs_ nullmask_bytes_ ;" ;
  (* All nullable values found in the fieldmask will have its nullbit in the
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
  p "  offs_\n"

let rec emit_indent oc n =
  if n > 0 then (
    Printf.fprintf oc "\t" ;
    emit_indent oc (n-1)
  )

(* Emit a function that, given an array of strings (corresponding to a line of
 * CSV, once properly unquoted and unescaped) will return the tuple defined by [typ]
 * or raises some exception *)
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
    p "  let val_%d, strs_ =" i ;
    p "    let s_ =" ;
    p "      try List.hd strs_" ;
    p "      with Failure _ ->" ;
    p "        Printf.sprintf \"Expected more values than %d\" |>" i ;
    p "        failwith in" ;
    p "    (try check_parse_all s_ (" ;
    emit_value_of_string 3 ft.RamenTuple.typ "s_" "0" emit_is_null [] false oc ;
    p "    ) with exn -> (" ;
    p "      !logger.error \"Cannot parse field #%d (%s): %%S: %%s\""
      (i+1) (ft.name :> string) ;
    p "        s_ (Printexc.to_string exn) ;" ;
    p "      raise exn)), List.tl strs_ in" ;
  ) typ ;
  p "  %a\n"
    (list_print_as_tuple_i (fun oc i _ ->
      Printf.fprintf oc "val_%d" i)) typ

let emit_time_of_tuple name opc =
  let open RamenEventTime in
  Printf.fprintf opc.code "let %s %a =\n\t"
    name
    (emit_tuple Out) opc.typ ;
  (match opc.event_time with
  | None -> String.print opc.code "None"
  | Some _ -> Printf.fprintf opc.code "Some (%a)" emit_event_time opc) ;
  String.print opc.code "\n\n"

let emit_factors_of_tuple name func oc =
  let typ = O.out_type_of_operation ~with_private:true func.VSI.operation in
  let factors = O.factors_of_operation func.VSI.operation in
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
  p "  CodeGenLib_IO.read_glob_file filename_ preprocessor_ unlink_\n"

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
    p "  let partitions_ = %a in"
      (List.print
        (conv_to ~env ~context:Finalize ~opc (Some TI32)))
        specs.partitions ;
    p "  let partitions_ = List.map Int32.to_int partitions_ in") ;
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
  p "  CodeGenLib_IO.read_kafka_topic consumer_ topic_ partitions_ offset_\n"

(* Given a tuple type (in op.typ), generate the CSV reader operation.
 * A Reader generates a stream of tuples from a data provider.
 * We use CodeGenLib_IO.read_lines to turn data chunks into lines. *)
let emit_parse_csv opc name specs =
  let p fmt = emit opc.code 0 fmt in
  fail_with_context "csv line parser" (fun () ->
    emit_tuple_of_strings 0 "tuple_of_strings_" specs.O.null opc.code opc.typ) ;
  p "let %s field_of_params_ =" name ;
  p "  let tuples = [ [ \"param\" ], field_of_params_ ;" ;
  p "                 [ \"env\" ], Sys.getenv ] in" ;
  p "  let separator_ = subst_tuple_fields tuples %S" specs.separator ;
  p "  and null_ = subst_tuple_fields tuples %S" specs.null ;
  p "  and escape_seq_ = subst_tuple_fields tuples %S in" specs.escape_seq ;
  p "  let for_each_line =" ;
  p "    CodeGenLib_IO.tuple_of_csv_line separator_ %b escape_seq_ tuple_of_strings_"
      specs.may_quote ;
  p "  in" ;
  p "  fun k ->" ;
  p "    CodeGenLib_IO.lines_of_chunks (for_each_line k)\n"

(* In the special case of RowBinary We are going to add another cmx into the
 * mix, that will unserialize the tuple for us (with the idea that this other
 * code generation tool, Dessser, will eventually take over this whole file). *)
let emit_parse_rowbinary opc name _specs =
  let p fmt = emit opc.code 0 fmt in
  (* Having no textual parameters there is no parameters to be substituted si
   * [field_of_params] is ignored: *)
  p "let %s _field_of_params =" name ;
  (* This function must return the number of bytes parsed from input: *)
  p "  fun per_tuple_cb buffer start stop has_more ->" ;
  p "    match %s.read_tuple buffer start stop has_more with"
    opc.dessser_mod_name ;
  (* FIXME: only catch NotEnoughInput so that genuine encoding errors
   * can crash the worker before we have accumulated too many tuples in
   * the read buffer. *)
  p "    | exception (DessserOCamlBackendHelpers.NotEnoughInput _ as e) ->" ;
  p "        let what =" ;
  p "          Printf.sprintf \"While decoding rowbinary @%%d..%%d%%s\"" ;
  p "            start stop (if has_more then \"(...)\" else \".\") in" ;
  p "        print_exception ~what e ;" ;
  p "        0" ;
  p "    | tuple, read_sz ->" ;
  p "        per_tuple_cb tuple ;" ;
  p "        read_sz\n"

let emit_read opc name source_name format_name =
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
    p "    (%s field_of_params_)" format_name ;
    p "    sersize_of_tuple_ time_of_tuple_" ;
    p "    factors_of_tuple_ serialize_tuple_" ;
    p "    orc_make_handler_ orc_write orc_close\n")

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
    p "    %S sersize_of_tuple_ time_of_tuple_ factors_of_tuple_"
      (string_of_proto proto) ;
    p "    serialize_tuple_" ;
    p "    orc_make_handler_ orc_write orc_close\n")

let emit_well_known opc name from
                    unserializer_name ringbuf_envvar worker_and_time =
  let open RamenProtocols in
  let p fmt = emit opc.code 0 fmt in
  fail_with_context "serialized size computation" (fun () ->
    emit_sersize_of_tuple 0 "sersize_of_tuple_" opc.code opc.typ) ;
  fail_with_context "event time extractor" (fun () ->
    emit_time_of_tuple "time_of_tuple_" opc) ;
  fail_with_context "tuple serializer" (fun () ->
    emit_serialize_function 0 "serialize_tuple_" opc.code opc.typ) ;
  fail_with_context "well-known listening function" (fun () ->
    p "let %s () =" name ;
    p "  CodeGenLib_Skeletons.read_well_known %a"
      (List.print (fun oc ds ->
        Printf.fprintf oc "%S" (
          IO.to_string (O.print_data_source true) ds))) from ;
    p "    sersize_of_tuple_ time_of_tuple_ factors_of_tuple_" ;
    p "    serialize_tuple_ %s %S %s"
     unserializer_name ringbuf_envvar worker_and_time ;
    p "    orc_make_handler_ orc_write orc_close\n")

(* * All the following emit_* functions Return (value, offset).
 * [offs_var] is the name of the variable holding the current offset within the
 * tx, while [start_offs_var] holds the offset of the start of the current
 * compound structure where the current nullmask can be found. *)
let rec emit_deserialize_value
    indent tx_var start_offs_var offs_var nulli_var oc typ =
  let emit_read_array indent tx_var offs_var dim_var oc t =
    let p fmt = emit oc indent fmt in
    p "let arr_start_ = %s" offs_var ;
    p "and offs_arr_ = ref (%s + (RingBufLib.nullmask_sz_of_vector %s)) in"
      offs_var dim_var ;
    p "let v_ = Array.init %s (fun bi_ ->" dim_var ;
    p "  let v_, o_ =" ;
    p "    let offs_arr_ = !offs_arr_ in" ;
    emit_deserialize_value (indent + 1) tx_var "arr_start_" "offs_arr_"
                           "bi_" oc t ;
    p "    in" ;
    p "  offs_arr_ := o_ ; v_" ;
    p ") in v_, !offs_arr_"
  in
  let p fmt = emit oc indent fmt in
  if typ.T.nullable then (
    p "if RingBuf.get_bit %s %s %s then ("
      tx_var start_offs_var nulli_var ;
    p "  let v_, %s =" offs_var ;
    emit_deserialize_value (indent + 2) tx_var start_offs_var offs_var
                           nulli_var oc { typ with nullable = false } ;
    p "    in" ;
    p "  NotNull v_, %s" offs_var ;
    p ") else Null, %s" offs_var
  ) else (
    let emit_for_record kts =
      let nullmask_sz = RingBufLib.nullmask_sz_of_tuple kts in
      p "let tuple_start_ = %s and offs_tup_ = %s + %d in"
        offs_var offs_var nullmask_sz ;
      let item_var k = "field_"^ k ^"_" |>
                       RamenOCamlCompiler.make_valid_ocaml_identifier in
      let ser = RingBufLib.ser_order kts in
      Array.iteri (fun i (k, t) ->
        p "let bi_ = %d in" i ;
        p "let %s, offs_tup_ =" (item_var k) ;
        emit_deserialize_value (indent + 1) tx_var "tuple_start_" "offs_tup_"
                               "bi_" oc t ;
        p "  in"
      ) ser ;
      p "%a, offs_tup_"
        (array_print_as_tuple (fun oc (k, _) ->
          String.print oc (item_var k))) kts
    in
    match typ.T.structure with
    (* Constructed types are prefixed with a nullmask and then read item
     * by item: *)
    | TTuple ts ->
        Array.mapi (fun i t -> string_of_int i, t) ts |>
        emit_for_record

    | TRecord kts ->
        emit_for_record  kts

    | TVec (d, t) ->
        emit_read_array indent tx_var offs_var (string_of_int d) oc t

    | TList t ->
        (* List are like vectors but prefixed with the actual number of
         * elements: *)
        p "let d_, offs_lst_ =" ;
        p "  Uint32.to_int (RingBuf.read_u32 %s %s), %s + %d in"
          tx_var offs_var offs_var RingBufLib.sersize_of_u32 ;
        emit_read_array indent tx_var "offs_lst_" "d_" oc t
    (* Non constructed types: *)
    | _ ->
        p "RingBuf.read_%s %s %s, %s +"
          (id_of_typ typ.T.structure) tx_var offs_var offs_var ;
        emit_sersize_of_not_null_scalar (indent + 1) tx_var offs_var oc
                                        typ.T.structure
  )

(* We do not want to read the value from the RB each time it's used,
 * so extract a tuple from the ring buffer. *)
let emit_deserialize_function indent name ?(is_yield=false) ~opc typ =
  let p fmt = emit opc.code indent fmt in
  p "(* Deserialize a tuple of type:" ;
  p "     %a" RamenTuple.print_typ typ ;
  p "*)" ;
  p "let %s tx_ =" name ;
  let indent =
    if is_yield then (
      (* Yield produce only tuples for the live channel: *)
      p "  let m_ = RingBufLib.(DataTuple RamenChannel.live) in" ;
      p "  let start_offs_ = 0 in" ;
      indent + 1
    ) else (
      p "  match RingBufLib.read_message_header tx_ 0 with" ;
      p "  | RingBufLib.EndOfReplay _ as m_ -> m_, None" ;
      p "  | RingBufLib.DataTuple _ as m_ ->" ;
      p "      let start_offs_ = RingBufLib.message_header_sersize m_ in" ;
      indent + 3
    ) in
  let p fmt = emit opc.code indent fmt in
  p "let offs_ = start_offs_ + %d in"
    (RingBufLib.nullmask_bytes_of_tuple_type typ) ;
  if verbose_serialization then
    p "!logger.debug \"Deserializing a tuple\" ;" ;
  (* It is important we only the first layer of fields is reordered and that
   * deeper records are unaltered: *)
  RingBufLib.ser_tuple_typ_of_tuple_typ ~recursive:false typ |>
  List.fold_left (fun nulli (ft, _) ->
    let id = id_of_field_typ ~tuple:In ft in
    p "let bi_ = %d in" nulli ;
    p "let %s, offs_ =" id ;
    emit_deserialize_value (indent + 1) "tx_" "start_offs_" "offs_"
                           "bi_" opc.code ft.typ ;
    p "  in" ;
    nulli + (if ft.typ.nullable then 1 else 0)
  ) 0 |> ignore ;
  (* We want to output the tuple with fields ordered according to the
   * select clause specified order, not according to serialization order: *)
  p "  m_, Some %a\n" (emit_tuple In) typ

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
    Printf.fprintf opc.code "let %s f_ chan_ it_ ot_ = f_ chan_ it_ ot_\n" name
  else (
    Printf.fprintf opc.code "let %s f_ chan_ (%a as it_) %a =\n"
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
    Printf.fprintf opc.code "%af_ chan_ it_ (\n%a"
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
    Printf.fprintf opc.code ")\n"
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
  Printf.fprintf opc.code "\t%a\n"
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
          T.print_typ sf.expr.E.typ ;
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
  (* Here we must generate the tuple in the order specified by out_type,
   * not selected_fields: *)
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
  p "  )\n"

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
  Printf.fprintf opc.code "\t()\n"

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
  Printf.fprintf opc.code "\n\t)\n"

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
  let t = e.E.typ.structure |>
          IO.to_string otype_of_structure in
  let print_expr_structure oc e =
    e.E.typ.structure |> (* nullable taken care of below *)
    IO.to_string otype_of_structure |>
    String.print oc in
  let nullable = if e.typ.nullable then " nullable" else "" in
  let print_expr_typ ~skip_null oc e =
    Printf.fprintf oc "%a%s"
      otype_of_structure e.E.typ.structure
      (if e.typ.nullable && not skip_null then " nullable" else "")
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
  | Stateful (_, _, SF4 (DampedHolt, _, _, _, _)) -> "(float * float)"^ nullable
  | Stateful (_, _, SF6 (DampedHoltWinter, _, _,_, _, _, _)) -> "(float * float * float array * int)"^ nullable
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
  | Stateful (_, _, SF1 (AggrSum, _)) when e.E.typ.structure = TFloat ->
    "(float * float)"^ nullable
  | _ -> t ^ nullable

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
          T.print_typ f.E.typ ;
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
    Printf.fprintf opc.code "\t{" ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf opc.code " %s ; %s_empty_ = true ; "
          (name_of_state f) (name_of_state f)) ;
    Printf.fprintf opc.code " }\n"
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

(* Depending on what uses a commit/flush condition, we might need to check
 * all groups after every single input tuple (very slow), or after every
 * selected input tuple (still quite slow), or only when this group is
 * modified (fast). Users should limit all/selected tuple to aggregations
 * with few groups only. *)
let check_commit_for_all expr =
  (* Tells whether the commit condition applies to all or only to the
   * selected group: *)
  try
    E.iter (fun _ e ->
      match e.E.text with
      | Stateless (SL0 (Path _))
      | Binding (RecordField (In, _)) ->
          raise Exit
      | _ -> ()
    ) expr ;
    false
  with Exit ->
    true

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
      Printf.fprintf opc.code "\tfalse\n"
  | es ->
      Printf.fprintf opc.code "\t%a\n"
        (List.print ~first:"(" ~last:")" ~sep:", "
           (emit_expr ~env ~context:Finalize ~opc)) es

let emit_string_of_value indent typ val_var oc =
  let p fmt = emit oc indent fmt in
  p "%t %s"
    (conv_from_to ~string_not_null:true ~nullable:typ.T.nullable
                  typ.structure TString)
    val_var

let emit_notification_tuple ~env ~opc oc notif =
  let print_expr = emit_expr ~env ~context:Finalize ~opc in
  Printf.fprintf oc
    "(%a,\n\t\t%a)"
    print_expr notif
    (List.print ~sep:";\n\t\t  "
      (fun oc ft ->
        let id = id_of_field_name ~tuple:Out ft.RamenTuple.name in
        Printf.fprintf oc "%S, "
          (ft.RamenTuple.name :> string) ;
        emit_string_of_value 1 ft.typ id oc)) opc.typ

(* We want a function that, when given the worker name, current time and the
 * output tuple, will return the list of RamenNotification.tuple to send: *)
(* TODO: shouldn't CodeGenLib pass this func the global and also maybe
 * the group states? *)
(* TODO: do not return this value for each notification name, as this is
 * always the same now. Instead, return the list of notification names and
 * a single string for the output value. *)
let emit_get_notifications name in_typ out_typ ~opc notifications =
  let env =
    add_tuple_environment In in_typ [] |>
    add_tuple_environment Out out_typ in
  Printf.fprintf opc.code "let %s %a %a =\n\t%a\n"
    name
    (emit_tuple ~with_alias:true In) in_typ
    (emit_tuple ~with_alias:true Out) out_typ
    (List.print ~sep:";\n\t\t" (emit_notification_tuple ~env ~opc))
      notifications

let expr_needs_tuple_from lst e =
  match e.E.text with
  | Variable tuple
  | Binding (RecordField (tuple, _)) ->
      List.mem tuple lst
  | _ ->
      false

(* Tells whether this expression requires the out tuple (or anything else
 * from the group). *)
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

let optimize_commit_cond ~env ~opc in_typ minimal_typ commit_cond =
  let no_optim = "None", commit_cond in
  (* Takes an expression and if that expression is equivalent to
   * f(in) op g(out) then returns [f], [neg], [op], [g] where [neg] if true
   * if [op] is meant to be negated (remember Lt is Not Gt), or raise
   * Not_found: *)
  let rec defined_order = function
    | E.{ text = Stateless (SL2 ((Gt|Ge as op), l, r)) } ->
        let dep_only_on lst e =
          (* env and params are always ok on both sides of course: *)
          let lst = Env :: Param :: lst in
          let open RamenOperation in
          try check_depends_only_on lst e ; true
          with DependsOnInvalidVariable _ -> false
        and no_local_state e =
          try
            E.unpure_iter (fun _ -> function
              | E.{ text = Stateful (LocalState, _, _) } ->
                  raise Exit
              | _ -> ()) e ;
            true
          with Exit ->false
        in
        if dep_only_on [ In ] l &&
           no_local_state l &&
           dep_only_on [ Out; OutPrevious ] r
        then l, false, op, r
        else if dep_only_on [ Out; OutPrevious ] l &&
                dep_only_on [ In ] r &&
                no_local_state r
        then r, true, op, r
        else raise Not_found
    | E.{ text = Stateless (SL1 (Not, e)) } ->
        let l, neg, op, r = defined_order e in
        l, not neg, op, r
    | _ -> raise Not_found
  in
  let es = E.as_nary E.And commit_cond in
  (* TODO: take the best possible sub-condition not the first one: *)
  let rec loop rest = function
    | [] ->
        !logger.warning "Cannot find a way to optimise the commit \
                         condition of function %s"
          (N.func_color (option_get "func_name" __LOC__ opc.func_name)) ;
        no_optim
    | e :: es ->
        (match defined_order e with
        | exception Not_found ->
            !logger.debug "Expression %a does not define an ordering"
              (E.print false) e ;
            loop (e :: rest) es
        | f, neg, op, g ->
            !logger.debug "Expression %a defines an ordering"
              (E.print false) e ;
            (* We will convert both [f] and [g] into the bigger numeric type
             * as [emit_function] would do: *)
            let to_typ = large_enough_for f.typ.structure g.typ.structure in
            let may_neg e =
              (* Let's add an unary minus in front of [e] it we are supposed
               * to neg the Greater operator, and type it by hand: *)
              if neg then
                E.make ~structure:e.E.typ.structure ~nullable:e.typ.nullable
                       ?units:e.units (Stateless (SL1 (Minus, e)))
              else e in
            let cmp = omod_of_type to_typ ^".compare" in
            let cmp =
              match f.typ.nullable, g.typ.nullable with
              | false, false -> cmp
              | true, true -> "(compare_nullable "^ cmp ^")"
              | true, false -> "(compare_nullable_left "^ cmp ^")"
              | false, true -> "(compare_nullable_right "^ cmp ^")" in
            let cond_in = "commit_cond_in_"
            and cond_out = "commit_cond_out_" in
            emit_cond0_in ~env cond_in in_typ ~opc
                          ~to_typ (may_neg f) ;
            emit_cond0_out ~env cond_out minimal_typ ~opc
                           ~to_typ (may_neg g) ;
            let cond0 =
              Printf.sprintf "(Some (%s, %s, %s, %b))"
              cond_in cond_out cmp (op = Ge) in
            let cond =
              E.of_nary ~structure:commit_cond.typ.structure
                        ~nullable:commit_cond.typ.nullable
                        ~units:commit_cond.units
                        E.And (List.rev_append rest es) in
            cond0, cond) in
  loop [] es

let emit_aggregate opc global_state_env group_state_env
                   env_env param_env globals_env
                   name top_half_name in_typ =
  let out_typ = opc.typ in
  match opc.op with
  | Some O.Aggregate
      { fields ; sort ; where ; key ; commit_before ; commit_cond ;
        flush_how ; notifications ; every ; from ; _ } ->
  let fetch_recursively s =
    let s = ref s in
    if not (reach_fixed_point (fun () ->
      let num_fields = Set.cardinal !s in
      List.iter (fun sf ->
        (* is this out field selected for minimal_out yet? *)
        if Set.mem sf.O.alias !s then (
          (* Add all other fields from out that are needed in this field
           * expression *)
          E.iter (fun _ e ->
            match e.E.text with
            | Binding (RecordField (Out, fn)) ->
                s := Set.add fn !s
            | _ -> ()
          ) sf.O.expr)
      ) fields ;
      Set.cardinal !s > num_fields))
    then failwith "Cannot build minimal_out set?!" ;
    !s in
  (* minimal tuple: the subset of the out tuple that must be finalized at
   * every input even in the absence of commit. We need those fields that
   * are used in the commit condition itself, or used as parameter of a
   * stateful function used by another field (as the state update function
   * will need its finalized value) and also if it's used to compute the
   * event time in any way, as we want to know the front time at every
   * input. Also for convenience any field that involves the print function.
   * Of course, any field required to compute a minimal field must also be
   * minimal. *)
  let add_if_needs_out s e =
    match e.E.text with
    | Binding (RecordField (Out, fn)) (* not supposed to happen *) ->
        Set.add fn s
    | Stateless (SL2 (Get, E.{ text = Const (VString fn) ; _ },
                           E.{ text = Variable Out ; _ })) ->
        Set.add (N.field fn) s
    | _ -> s in
  let minimal_fields =
    let from_commit_cond =
      E.fold (fun _ s e ->
        add_if_needs_out s e
      ) [] Set.empty commit_cond
    and for_updates =
      List.fold_left (fun s sf ->
        E.unpure_fold s (fun _ s e ->
          E.fold (fun _ s e ->
            add_if_needs_out s e
          ) [] s e
        ) sf.O.expr
      ) Set.empty fields
    and for_event_time =
      let req_fields = Option.map_default RamenEventTime.required_fields
                                          Set.empty opc.event_time in
      List.fold_left (fun s sf ->
        if Set.mem sf.O.alias req_fields then
          Set.add sf.alias s
        else s
      ) Set.empty fields
    and for_printing =
      List.fold_left (fun s sf ->
        try
          E.iter (fun _ e ->
            match e.E.text with
            | Stateless (SL1s (Print, _)) -> raise Exit | _ -> ()
          ) sf.O.expr ;
          s
        with Exit -> Set.add sf.O.alias s
      ) Set.empty fields
    in
    (* Now combine these sets: *)
    Set.union from_commit_cond for_updates |>
    Set.union for_event_time |>
    Set.union for_printing |>
    fetch_recursively
  in
  !logger.debug "minimal fields: %a"
    (Set.print N.field_print) minimal_fields ;
  (* Replace removed values with a dull type. Should not be accessed
   * ever. This is because we want out and minimal to have the same
   * ramen type, so that field access works on both. *)
  let minimal_typ =
    List.map (fun ft ->
      if Set.mem ft.RamenTuple.name minimal_fields then
        ft
      else (* Replace it *)
        RamenTuple.{ ft with
          name = N.field ("_not_minimal_"^ (ft.name :> string)) ;
          typ = T.{ ft.typ with structure = TEmpty } }
    ) out_typ in
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
  and check_commit_for_all = check_commit_for_all commit_cond
  and is_yield = from = []
  (* Every functions have at least access to env + params + globals: *)
  and base_env = param_env @ env_env @ globals_env in
  let commit_cond0, commit_cond_rest =
    let env = group_state_env @ global_state_env @ base_env in
    if check_commit_for_all then
      fail_with_context "optimized commit condition" (fun () ->
        optimize_commit_cond in_typ minimal_typ ~env ~opc commit_cond)
    else "None", commit_cond
  in
  fail_with_context "global state initializer" (fun () ->
    emit_state_init "global_init_" E.GlobalState ~env:base_env ["()"] ~where
                    ~commit_cond ~opc fields) ;
  fail_with_context "group state initializer" (fun () ->
    emit_state_init "group_init_" E.LocalState ~env:(global_state_env @ base_env)
                    ["global_"] ~where ~commit_cond ~opc fields) ;
  fail_with_context "tuple reader" (fun () ->
    emit_deserialize_function 0 "read_in_tuple_" ~is_yield ~opc in_typ) ;
  fail_with_context "where-fast function" (fun () ->
    emit_where ~env:(global_state_env @ base_env) "where_fast_" in_typ ~opc
      where_fast) ;
  fail_with_context "where-slow function" (fun () ->
    emit_where ~env:(global_state_env @ base_env) "where_slow_" in_typ ~opc
      ~with_group:true where_slow) ;
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
    emit_get_notifications "get_notifications_" in_typ out_typ ~opc
                           notifications) ;
  let p fmt = emit opc.code 0 fmt in
  fail_with_context "aggregate function" (fun () ->
    p "let %s () =" name ;
    p "  CodeGenLib_Skeletons.aggregate" ;
    p "    read_in_tuple_ sersize_of_tuple_ time_of_tuple_" ;
    p "    factors_of_tuple_ serialize_tuple_" ;
    p "    generate_tuples_" ;
    p "    minimal_tuple_of_group_" ;
    p "    update_states_" ;
    p "    out_tuple_of_minimal_tuple_" ;
    p "    %d sort_until_ sort_by_"
      (match sort with None -> 0 | Some (n, _, _) -> n) ;
    p "    where_fast_ where_slow_ key_of_input_ %b" (key = []) ;
    p "    commit_cond_ %s %b %b %b"
      commit_cond0 commit_before (flush_how <> Never) check_commit_for_all ;
    p "    global_init_ group_init_" ;
    p "    get_notifications_ (%a)"
      (Option.print
        (fun oc e ->
          Printf.fprintf oc "(%a)"
            (conv_to ~env:base_env ~context:Finalize ~opc (Some TFloat)) e))
        every ;
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
    expr_needs_tuple_from
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
    p "  CodeGenLib_Skeletons.top_half read_in_tuple_ top_where_\n")

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
          T.print_typ g.Globals.typ ;
        let scope_id = Globals.scope_id g src_path in
        (match g.typ.structure with
        | T.TMap (k, v) ->
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
        (if p.ptyp.typ.nullable && p.value <> VNull then "NotNull " else "")
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
          (conv_from_to ~nullable:p.ptyp.typ.nullable
                        p.ptyp.typ.structure TString)
          glob_name
          (if p.ptyp.typ.nullable then Printf.sprintf " |! %S" string_of_null
           else ""))) params) ;
  (* params and envs must be accessible as records (encoded as tuples)
   * under names "params_" and "envs_". Note that since we can refer to
   * the whole tuple "env" and "params", and that we type all functions
   * in a program together, then these records must contain all fields
   * used in the program, not only the fields used by any single function. *)
  fail_with_context "definition of the parameter record" (fun () ->
    Printf.fprintf oc
      "\n(* Parameters as a Ramen record: *)\n\
       let params_ = %a\n\n"
      (list_print_as_tuple (fun oc p ->
        (* See emit_parameters *)
        Printf.fprintf oc "%s_%s_"
          (id_of_prefix Param)
          (p.ptyp.name :> string)))
        (RamenTuple.params_sort params)) ;
  fail_with_context "definition of the env record" (fun () ->
    Printf.fprintf oc
      "\n(* Environment variables as a Ramen record: *)\n\
       let envs_ = %a\n\n"
      (list_print_as_tuple (fun oc (n : N.field) ->
        Printf.fprintf oc "Sys.getenv_opt %S |> nullable_of_option"
          (n :> string)))
        envvars)

let emit_running_condition oc params envvars cond =
  let code = IO.output_string ()
  and consts = IO.output_string () in
  let opc =
    { op = None ; event_time = None ; func_name = None ;
      params ; code ; consts ; typ = [] ; gen_consts = Set.empty ;
      dessser_mod_name = "" } in
  fail_with_context "running condition" (fun () ->
    (* Running condition has no input/output tuple but must have a
     * value once and for all depending on params/env only: *)
    let env_env, param_env, _ =
      static_environments "" params envvars [] in
    let env = param_env @ env_env in
    Printf.fprintf opc.code "let run_condition_ () =\n\t%a\n\n"
      (emit_expr ~env ~context:Finalize ~opc) cond ;
    Printf.fprintf oc "%s\n%s\n"
      (IO.close_out opc.consts) (IO.close_out opc.code))

let emit_title func oc =
  Printf.fprintf oc "(* Code generated for operation %S:\n%a\n*)\n"
    (func.VSI.name :> string)
    (O.print true) func.VSI.operation

let emit_header params_mod_name globals_mod_name oc =
  Printf.fprintf oc "\
    open Batteries\n\
    open Stdint\n\
    open RamenHelpers\n\
    open RamenNullable\n\
    open RamenLog\n\
    open RamenConsts\n\n\
    open %s\n\
    open %s\n"
    params_mod_name
    globals_mod_name

let emit_operation name top_half_name func
                   global_state_env group_state_env
                   env_env param_env globals_env opc =
  (* Default top-half (for non-aggregate operations): a NOP *)
  Printf.fprintf opc.code "let %s = ignore\n\n" top_half_name ;
  (* Emit code for all the operations: *)
  match func.VSI.operation with
  | ReadExternal { source ; format ; _ } ->
    let source_name = name ^"_source" and format_name = name ^"_format" in
    (match source with
    | File specs ->
        emit_read_file opc param_env env_env globals_env source_name specs
    | Kafka specs ->
        emit_read_kafka opc param_env env_env globals_env source_name specs) ;
    (match format with
    | CSV specs ->
        emit_parse_csv opc format_name specs
    | RowBinary specs ->
        emit_parse_rowbinary opc format_name specs);
    emit_read opc name source_name format_name
  | ListenFor { net_addr ; port ; proto } ->
    emit_listen_on opc name net_addr port proto
  | Instrumentation { from } ->
    emit_well_known opc name from
      "RamenWorkerStatsSerialization.unserialize" "report_ringbuf"
      "(fun (_, w, _, t, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) -> w, t)"
  | Notifications { from } ->
    emit_well_known opc name from
      "RamenNotificationSerialization.unserialize" "notifs_ringbuf"
      "(fun (_, w, _, t, _, _, _, _, _, _) -> w, t)"
  | Aggregate _ ->
    (* Temporary hack: build a RamenTuple out of this in_type (at this point
     * we do not need the access paths anyway): *)
    let in_type =
      RamenFieldMaskLib.in_type_of_operation func.VSI.operation |>
      List.map (fun f ->
        RamenTuple.{
          name = E.id_of_path f.RamenFieldMaskLib.path ;
          typ = f.typ ; units = f.units ;
          doc = "" ; aggr = None }) in
    emit_aggregate opc global_state_env group_state_env
                   env_env param_env globals_env
                   name top_half_name in_type

(* A function that reads the history and write it according to some out_ref
 * under a given chanel: *)
let emit_replay name func opc =
  let p fmt = emit opc.code 0 fmt in
  let typ =
    O.out_type_of_operation ~with_private:false func.VSI.operation in
  emit_deserialize_function 0 "read_pub_tuple_" ~opc typ ;
  p "let read_out_tuple_ tx =" ;
  p "  let hdr_, tup_ = read_pub_tuple_ tx in" ;
  p "  hdr_, Option.map out_of_pub_ tup_\n" ;
  p "let %s () =" name ;
  p "  CodeGenLib_Skeletons.replay read_out_tuple_" ;
  p "    sersize_of_tuple_ time_of_tuple_ factors_of_tuple_" ;
  p "    serialize_tuple_" ;
  p "    orc_make_handler_ orc_write orc_read orc_close\n"

(* Generator for function [out_of_pub_] that adds missing private fields. *)
let emit_priv_pub opc =
  let op = option_get "must have function" __LOC__ opc.op in
  let rtyp = O.out_record_of_operation ~with_private:true op in
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
      if typ.T.nullable then (
        let var' = "notnull_"^ var in
        p "(match %s with" var ;
        p "| Null -> Null" ;
        p "| NotNull %s -> NotNull (" var' ;
        indent + 1, var'
      ) else indent, var in
    let p fmt = emit oc indent fmt in
    (match typ.T.structure with
    | T.TRecord kts ->
        transform_record indent kts ;
    | T.TTuple ts ->
        let kts = Array.mapi (fun i t -> string_of_int i, t) ts in
        transform_record indent kts
    | T.TVec (_, t) | T.TList t ->
        p "Array.map (fun v_ ->" ;
        emit_transform (indent + 1) trim "v_" t oc ;
        p ") %s" var
    | _ ->
        p "%s" var) ;
    if typ.T.nullable then p "))"
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

let emit_orc_wrapper func orc_write_func orc_read_func oc =
  let p fmt = emit oc 0 fmt in
  let rtyp = O.out_record_of_operation ~with_private:true func.VSI.operation in
  let pub = filter_out_private rtyp |>
            option_get "no support for void types" __LOC__ in
  p "(* A handler to be passed to the function generated by" ;
  p "   emit_write_value: *)" ;
  p "type handler" ;
  p "" ;
  p "external orc_write : handler -> %a -> float -> float -> unit = %S"
    otype_of_type rtyp
    orc_write_func ;
  p "external orc_read_pub : N.path -> int -> (%a -> unit) -> (int * int) = %S"
    otype_of_type pub
    orc_read_func ;
  (* Destructor do not seems to be called when the OCaml program exits: *)
  p "external orc_close : handler -> unit = \"orc_handler_close\"" ;
  p "" ;
  p "(* Parameters: schema * path * index * row per batch * batches per file * archive *)" ;
  p "external orc_make_handler : string -> N.path -> bool -> int -> int -> bool -> handler =" ;
  p "  \"orc_handler_create_bytecode_lol\" \"orc_handler_create\"" ;
  p "" ;
  (* A wrapper that inject missing private fields: *)
  p "let orc_read fname_ batch_sz_ k_ =" ;
  p "  orc_read_pub fname_ batch_sz_ (fun t_ -> k_ (out_of_pub_ t_))" ;
  p ""

let emit_make_orc_handler name func oc =
  let p fmt = emit oc 0 fmt in
  let rtyp = O.out_record_of_operation ~with_private:true func.VSI.operation in
  let schema = Orc.of_structure rtyp.T.structure |>
               IO.to_string Orc.print in
  p "let %s = orc_make_handler %S" name schema

(* Given the names of ORC reader/writer, build a universal conversion
 * function from/to CSV/RB/ORC named [name] and that takes the in and out
 * formats and file names (see [per_func_info] in CodeGenLib_Casing).
 * We have to deal with full tuples (including private fields) since that's
 * what take and return the ORC writer/readers. *)
let emit_convert name func oc =
  let p fmt = emit oc 0 fmt in
  let rtyp =
    O.out_record_of_operation ~with_private:true func.VSI.operation in
  p "let %s in_fmt_ in_fname_ out_fmt_ out_fname_ =" name ;
  (* We need our own tuple_of_strings_ because that for the CSV reader uses
   * a custom CSV separator/null string. *)
  O.out_type_of_operation ~with_private:false func.VSI.operation |>
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
  p "    serialize_tuple_ (out_of_pub_ %% my_tuple_of_strings_)\n"

let compile
      conf func obj_name params_mod_name dessser_mod_name
      orc_write_func orc_read_func params envvars globals
      globals_mod_name =
  !logger.debug "Going to generate code for function %s: %a"
    (N.func_color func.VSI.name)
    (O.print true) func.VSI.operation ;
  (* The code might need some global constant parameters, thus the two strings
   * that are assembled later: *)
  let code = IO.output_string ()
  and consts = IO.output_string ()
  and typ = O.out_type_of_operation ~with_private:true func.VSI.operation
  and env_env, param_env, globals_env =
    static_environments globals_mod_name params envvars globals
  and global_state_env, group_state_env =
    initial_environments func.VSI.operation
  in
  !logger.debug "Global state environment: %a" print_env global_state_env ;
  !logger.debug "Group state environment: %a" print_env group_state_env ;
  !logger.debug "Unix-env environment: %a" print_env env_env ;
  !logger.debug "Parameters environment: %a" print_env param_env ;
  !logger.debug "Global variables environment: %a" print_env globals_env ;
  (* As all exposed IO tuples are present in the environment, any Path can
   * now be replaced with a Binding. The [subst_fields_for_binding] function
   * takes an expression and does this change for any variable. The
   * [Path] expression is therefore not used anywhere in the code
   * generation process. We could similarly replace some Get in addition to
   * some Path. *)
  let op =
    List.fold_left (fun op tuple ->
      subst_fields_for_binding tuple op
    ) func.VSI.operation
      [ Env ; Param ; In ; Group ; OutPrevious ;
        Out ; SortFirst ; SortSmallest ; SortGreatest ;
        Record ]
  in
  !logger.debug "After substitutions for environment bindings: %a"
    (O.print true) op ;
  let opc =
    { op = Some op ; func_name = Some func.VSI.name ; params ; code ; consts ;
      typ ; event_time = O.event_time_of_operation func.VSI.operation ;
      gen_consts = Set.empty ; dessser_mod_name } in
  let src_file =
    RamenOCamlCompiler.with_code_file_for
      obj_name conf.C.reuse_prev_files (fun oc ->
        fail_with_context "header" (fun () ->
          emit_title func oc ;
          emit_header params_mod_name globals_mod_name oc) ;
        fail_with_context "priv_to_pub function" (fun () ->
          emit_priv_pub opc) ;
        fail_with_context "orc wrapper" (fun () ->
          emit_orc_wrapper func orc_write_func orc_read_func opc.code) ;
        fail_with_context "orc handler builder" (fun () ->
          emit_make_orc_handler "orc_make_handler_" func opc.code) ;
        fail_with_context "factors extractor" (fun () ->
          emit_factors_of_tuple "factors_of_tuple_" func opc.code) ;
        fail_with_context "operation" (fun () ->
          emit_operation EntryPoints.worker EntryPoints.top_half func
                         global_state_env group_state_env env_env param_env
                         globals_env opc) ;
        fail_with_context "replay function" (fun () ->
          emit_replay EntryPoints.replay func opc) ;
        fail_with_context "tuple conversion function" (fun () ->
          emit_convert EntryPoints.convert func opc.code) ;
        Printf.fprintf oc "\n(* Global constants: *)\n\n%s\n\
                           \n(* Operation Implementation: *)\n\n%s\n"
          (IO.close_out consts) (IO.close_out code)
      ) in
  let what = "function "^ N.func_color func.VSI.name in
  RamenOCamlCompiler.compile conf ~keep_temp_files:conf.C.keep_temp_files
                             what src_file obj_name
