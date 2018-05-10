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
open RamenScalar
open RamenTuple
module C = RamenConf

(* If true, the generated code will log details about serialization *)
let verbose_serialization = false

let id_of_prefix tuple =
  String.nreplace (string_of_prefix tuple) "." "_"

(* Tuple deconstruction as a function parameter: *)
let id_of_field_name ?(tuple=TupleIn) = function
  | "#count" -> "virtual_"^ id_of_prefix tuple ^"_count_"
  | "#successive" -> "virtual_"^ id_of_prefix tuple ^"_successive_"
  | field -> id_of_prefix tuple ^"_"^ field ^"_"

let id_of_field_typ ?tuple field_typ =
  id_of_field_name ?tuple field_typ.RamenTuple.typ_name

let list_print_as_tuple p = List.print ~first:"(" ~last:")" ~sep:", " p
let list_print_as_product p = List.print ~first:"(" ~last:")" ~sep:" * " p

let print_tuple_deconstruct tuple =
  let print_field fmt field_typ =
      String.print fmt (id_of_field_typ ~tuple field_typ)
  in
  list_print_as_tuple print_field

(* Emit the code that return the sersize of a fixed size type *)
let emit_sersize_of_fixsz_typ oc typ =
  let sz = RingBufLib.sersize_of_fixsz_typ typ in
  Int.print oc sz

(* Emit the code computing the sersize of some variable *)
let emit_sersize_of_field_var typ oc var =
  match typ with
  | TString ->
    Printf.fprintf oc "(RingBufLib.sersize_of_string %s)" var
  | _ -> emit_sersize_of_fixsz_typ oc typ

(* Emit the code to retrieve the sersize of some serialized value *)
let rec emit_sersize_of_field_tx tx_var offs_var nulli oc field =
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
  | TNull   -> "null"
  | TEth    -> "eth"
  | TIpv4   -> "ip4"
  | TIpv6   -> "ip6"
  | TCidrv4 -> "cidr4"
  | TCidrv6 -> "cidr6"
  | TNum | TAny -> assert false

let emit_value_of_string typ oc var =
  Printf.fprintf oc "RamenTypeConverters.%s_of_string %s" (id_of_typ typ) var

(* Returns the set of all field names from the "in" tuple mentioned
 * anywhere in the given expression: *)
let add_mentioned prev =
  let open RamenExpr in
  fold_by_depth (fun prev e ->
    match e with
    | Field (_, tuple, field) when tuple_has_type_input !tuple ->
      Set.add field prev
    | _ -> prev) prev

let add_all_mentioned_in_expr lst =
  let rec loop prev = function
    | [] -> prev
    | e :: e' -> loop (add_mentioned prev e) e'
  in
  loop Set.empty lst

let add_all_mentioned_in_string mentioned _str =
  (* TODO! *)
  mentioned

let emit_float oc f =
  (* printf "%F" would not work for infinity:
   * https://caml.inria.fr/mantis/view.php?id=7685 *)
  if f = infinity then String.print oc "infinity"
  else if f = neg_infinity then String.print oc "neg_infinity"
  else Printf.fprintf oc "(%f)" f

let emit_scalar oc =
  let open Stdint in
  function
  | VFloat  f -> emit_float oc f
  | VString s -> Printf.fprintf oc "%S" s
  | VBool   b -> Printf.fprintf oc "%b" b
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
  | VEth    n -> Printf.fprintf oc "(Uint40.of_int64 (%LdL))" (Uint48.to_int64 n)
  | VIpv4   n -> Printf.fprintf oc "(Uint32.of_string %S)" (Uint32.to_string n)
  | VIpv6   n -> Printf.fprintf oc "(Uint128.of_string %S)" (Uint128.to_string n)
  | VCidrv4 (n,l) ->
                 Printf.fprintf oc "(Uint32.of_string %S, %d)" (Uint32.to_string n) l
  | VCidrv6 (n,l) ->
                 Printf.fprintf oc "(Uint128.of_string %S, %d)" (Uint128.to_string n) l
  | VNull     -> Printf.fprintf oc "None"

(* Given a function name and an output type, return the actual function
 * returning that type, and the types each input parameters must be converted
 * into, if any. None means we need no conversion whatsoever (useful for
 * function internal state or 'a values) while Some TAny means there must be a
 * type but it has to be found out according to the context.
 *
 * Returns a list of typ option, as long as the type of input arguments *)
(* FIXME: this could be extracted from Compiler.check_expr *)

(* Context: helps picking the implementation of an operation. Subexpressions
 * will always have context "Finalize", though. *)
type context = InitState | UpdateState | Finalize | Generator

let string_of_context = function
  | InitState -> "InitState"
  | UpdateState -> "UpdateState"
  | Finalize -> "Finalize"
  | Generator -> "Generator"

let name_of_state =
  let open RamenExpr in
  function
  | StatefulFun (t, g, _) ->
    let prefix = match g with
      | LocalState -> "group_"
      | GlobalState -> "global_" in
    prefix ^ string_of_int t.uniq_num
  | _ -> assert false

let otype_of_type = function
  | TFloat -> "float" | TString -> "string" | TBool -> "bool"
  | TU8 -> "uint8" | TU16 -> "uint16" | TU32 -> "uint32" | TU64 -> "uint64" | TU128 -> "uint128"
  | TI8 -> "int8" | TI16 -> "int16" | TI32 -> "int32" | TI64 -> "int64" | TI128 -> "int128"
  | TNull -> "unit"
  | TEth -> "uint48"
  | TIpv4 -> "uint32"
  | TIpv6 -> "uint128"
  | TCidrv4 -> "(uint32 * int)"
  | TCidrv6 -> "(uint128 * int)"
  | TNum | TAny -> assert false

let omod_of_type = function
  | TFloat -> "Float"
  | TString -> "String"
  | TBool -> "Bool"
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128 as t ->
    String.capitalize (otype_of_type t)
  | TEth -> "RamenEthAddr"
  | TIpv4 -> "RamenIpv4"
  | TIpv6 -> "RamenIpv6"
  | TCidrv4 -> "RamenIpv4.Cidr"
  | TCidrv6 -> "RamenIpv6.Cidr"
  | TNull -> assert false (* Never used on NULLs *)
  | TNum | TAny -> assert false

(* TODO: Why don't we have explicit casts in the AST so that we could stop
 * caring about those pesky conversions once and for all? *)
(* Note: for field_of_tuple, we must be able to convert any value into a
 * string *)
let conv_from_to from_typ ~nullable to_typ p fmt e =
  match from_typ, to_typ with
  | a, b when a = b -> p fmt e
  | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128|TString|TFloat),
      (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128)
  | TString, (TFloat|TBool) ->
    Printf.fprintf fmt "(%s%s.of_%s (%a))"
      (if nullable then "Option.map " else "")
      (omod_of_type to_typ)
      (otype_of_type from_typ)
      p e
  | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128),
      (TFloat|TString)
  | (TFloat|TBool), (TString|TFloat) ->
    Printf.fprintf fmt "(%s%s.to_%s (%a))"
      (if nullable then "Option.map " else "")
      (omod_of_type from_typ)
      (otype_of_type to_typ)
      p e
  | TBool, (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) ->
    Printf.fprintf fmt "(%s(%s.of_int %% Bool.to_int) (%a))"
      (if nullable then "Option.map " else "")
      (omod_of_type to_typ)
      p e
  | _, TNull ->
    (* We could as well just print "()" but this is easier for debugging,
     * and hopefully the compiler will make it the same: *)
    Printf.fprintf fmt "(ignore %a)" p e
  | (TEth|TIpv4|TIpv6|TCidrv4|TCidrv6), TString ->
    Printf.fprintf fmt "(%s.to_string %a)"
      (omod_of_type from_typ) p e
  | _ ->
    failwith (Printf.sprintf "Cannot find converter from type %s to type %s"
                (IO.to_string RamenScalar.print_typ from_typ)
                (IO.to_string RamenScalar.print_typ to_typ))

let freevar_name t = "fv_"^ string_of_int t.RamenExpr.uniq_num ^"_"

let min_of_num_scalar_type =
  let open Stdint in
  function
  | TFloat -> VFloat neg_infinity
  | TBool -> VBool false
  | TU8 -> VU8 Uint8.zero
  | TU16 -> VU16 Uint16.zero
  | TU32 -> VU32 Uint32.zero
  | TU64 -> VU64 Uint64.zero
  | TU128 -> VU128 Uint128.zero
  | TI8 -> VI8 Int8.min_int
  | TI16 -> VI16 Int16.min_int
  | TI32 -> VI32 Int32.min_int
  | TI64 -> VI64 Int64.min_int
  | TI128 -> VI128 Int128.min_int
  | TEth -> VEth Uint48.zero
  | TIpv4 -> VIpv4 Uint32.zero
  | TIpv6 -> VIpv6 Uint128.zero
  | _ -> assert false

let max_of_num_scalar_type =
  let open Stdint in
  function
  | TFloat -> VFloat infinity
  | TBool -> VBool true
  | TU8 -> VU8 Uint8.max_int
  | TU16 -> VU16 Uint16.max_int
  | TU32 -> VU32 Uint32.max_int
  | TU64 -> VU64 Uint64.max_int
  | TU128 -> VU128 Uint128.max_int
  | TI8 -> VI8 Int8.max_int
  | TI16 -> VI16 Int16.max_int
  | TI32 -> VI32 Int32.max_int
  | TI64 -> VI64 Int64.max_int
  | TI128 -> VI128 Int128.max_int
  | TEth -> VEth Uint48.max_int
  | TIpv4 -> VIpv4 Uint32.max_int
  | TIpv6 -> VIpv6 Uint128.max_int
  | _ -> assert false

let min_of_num_type t =
  let open RamenExpr in
  Const (make_typ ?typ:t.scalar_typ ?nullable:t.nullable "min-init",
         min_of_num_scalar_type (Option.get t.scalar_typ))

let max_of_num_type t =
  let open RamenExpr in
  Const (make_typ ?typ:t.scalar_typ ?nullable:t.nullable "max-init",
         max_of_num_scalar_type (Option.get t.scalar_typ))

let any_constant_of_type t =
  let open RamenExpr in
  let open Stdint in
  let c v =
    Const (make_typ ?typ:t.scalar_typ ?nullable:t.nullable "init", v)
  in
  c (match Option.get t.scalar_typ with
  | TNull -> VNull
  | TString -> VString ""
  | TNum -> assert false
  | TAny -> assert false
  | TCidrv4 -> VCidrv4 (Uint32.of_int 0, 0)
  | TCidrv6 -> VCidrv6 (Uint128.of_int 0, 0)
  | s -> min_of_num_scalar_type s)

let emit_tuple tuple oc tuple_typ =
  print_tuple_deconstruct tuple oc tuple_typ

(* In some case we want emit_function to pass arguments as an array
 * (variadic functions...) or as a tuple (functions taking a tuple).
 * In both cases the int refers to how many normal args should we pass
 * before starting the array/tuple.*)
type args_as = Arg | Array of int | Tuple of int

(* Implementation_of gives us the type operands must be converted to.
 * This printer wrap an expression into a converter according to its current
 * type. *)
let rec conv_to ?state ~context to_typ fmt e =
  let open RamenExpr in
  let t = typ_of e in
  if t.nullable = None then (
    !logger.error "Problem: Have to convert expression %a into %a"
      (print true) e
      RamenScalar.print_typ (Option.get to_typ)
  ) ;
  let nullable = Option.get t.nullable in
  match t.scalar_typ, to_typ with
  | Some a, Some b ->
    conv_from_to a ~nullable b (emit_expr ~context ?state) fmt e
  | _, None ->
    (emit_expr ~context ?state) fmt e (* No conversion required *)
  | None, Some b ->
    failwith (Printf.sprintf "Cannot convert from unknown type into %s"
                (IO.to_string RamenScalar.print_typ b))

(* The vectors Tuple{Group,Out}Previous are optional: the commit when and
 * select clauses of aggregate operations either have it or not.
 * Each time they need access to a field they call a function "maybe_XXX_"
 * with that optional tuple, which avoids propagating out_typ down to
 * emit_expr - but hopefully the compiler will inline this.
 * (TODO: have a context in a single place and inline it directly?) *)
and emit_maybe_fields oc out_typ =
  List.iter (fun ft ->
    Printf.fprintf oc "let maybe_%s_ = function\n" ft.typ_name ;
    Printf.fprintf oc "  | None -> None\n" ;
    Printf.fprintf oc "  | Some %a -> %s%s\n\n"
      (emit_tuple TupleOut) out_typ
      (if ft.nullable then "" else "Some ")
      (id_of_field_name ~tuple:TupleOut ft.typ_name)
  ) out_typ

(* state is just the name of the state that's "opened" in the local environment,
 * ie "global_" if we are initializing the global state or "local_" if we are
 * initializing the group state or nothing (empty string) if we are not initializing
 * anything and state fields must be accessed via the actual state record.
 * It is used by stateful functions when they need to access their state. *)
(* FIXME: return a list of type * arg instead of two lists *)
and emit_expr ?state ~context oc expr =
  let open RamenExpr in
  let out_typ = typ_of expr in
  let true_or_nul nullable =
    Const (make_bool_typ ~nullable "true", VBool true)
  and false_or_nul nullable =
    Const (make_bool_typ ~nullable "false", VBool false)
  and zero_or_nul nullable =
    Const (make_typ ~typ:TU8 ~nullable "zero", VU8 (Stdint.Uint8.of_int 0)) in
  let my_state lifespan =
    let state_name =
      match lifespan with LocalState -> "group_"
                        | GlobalState -> "global_" in
    StateField (out_typ,
               (if state = Some lifespan then "" else state_name ^".") ^
               name_of_state expr)
  in
  match context, expr, out_typ.scalar_typ with
  (* Non-functions *)
  | Finalize, StateField (_, s), _ ->
    Printf.fprintf oc "%s" s
  | _, Const (_, c), _ ->
    Printf.fprintf oc "%s%a"
      (if is_nullable expr then "Some " else "")
      emit_scalar c
  | Finalize, Field (_, tuple, field), _ ->
    (match !tuple with
    | TupleGroupPrevious ->
      Printf.fprintf oc "(maybe_%s_ group_previous_opt_)" field
    | TupleOutPrevious ->
      Printf.fprintf oc "(maybe_%s_ out_previous_opt_)" field
    | _ ->
      String.print oc (id_of_field_name ~tuple:!tuple field))
  | Finalize, Case (_, alts, else_), t ->
    List.print ~first:"(" ~last:"" ~sep:" else "
      (fun oc alt ->
         (* If the condition is nullable then we must return NULL immediately.
          * If the cons is not nullable but the case is (for another reason),
          * then adds a Some. *)
         Printf.fprintf oc
           (if is_nullable alt.case_cond then
              "match %a with None -> None | Some cond_ -> if cond_ then %s(%a)"
            else
              "if %a then %s(%a)")
           (emit_expr ?state ~context) alt.case_cond
           (if is_nullable expr && not (is_nullable alt.case_cons) then "Some " else "")
           (conv_to ?state ~context t) alt.case_cons)
      oc alts ;
    (match else_ with
    | None ->
      (* If there is no ELSE clause then the expr is nullable: *)
      assert (is_nullable expr) ;
      Printf.fprintf oc " else None)"
    | Some else_ ->
      Printf.fprintf oc " else %s(%a))"
        (if is_nullable expr && not (is_nullable else_) then "Some " else "")
        (conv_to ?state ~context t) else_)
  | Finalize, Coalesce (_, es), t ->
    let rec loop = function
      | [] -> ()
      | [last] ->
        Printf.fprintf oc "(%a)" (conv_to ?state ~context t) last
      | e :: rest ->
        Printf.fprintf oc "(Option.default_delayed (fun () -> " ;
        loop rest ;
        Printf.fprintf oc ") (%a))" (conv_to ?state ~context t) e
    in
    loop es
  (* Stateless arithmetic functions which actual funcname depends on operand types: *)
  | Finalize, StatelessFun2 (_, Add, e1, e2),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".add") [Some t; Some t] [e1; e2]
  | Finalize, StatelessFun2 (_, Sub, e1, e2),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".sub") [Some t; Some t] [e1; e2]
  | Finalize, StatelessFun2 (_, Mul, e1, e2),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".mul") [Some t; Some t] [e1; e2]
  | Finalize, StatelessFun2 (_, IDiv, e1, e2),
    Some (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".div") [Some t; Some t] [e1; e2]
  | Finalize, StatelessFun2 (_, IDiv, e1, e2), Some (TFloat as t) ->
    (* Here we must convert everything to float first, then divide and
     * take the floor: *)
    Printf.fprintf oc "(let x_ = " ;
    emit_functionN oc ?state (omod_of_type t ^".div") [Some t; Some t] [e1; e2] ;
    Printf.fprintf oc " in if x_ >= 0. then floor x_ else ceil x_)"
  | Finalize, StatelessFun2 (_, Div, e1, e2), Some (TFloat as t) ->
    emit_functionN oc ?state (omod_of_type t ^".div") [Some t; Some t] [e1; e2]
  | Finalize, StatelessFun2 (_, Pow, e1, e2),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".( ** )") [Some t; Some t] [e1; e2]
  | Finalize, StatelessFun2 (_, Mod, e1, e2),
    Some (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".rem") [Some t; Some t] [e1; e2]
  | Finalize, StatelessFun2 (_, Mod, e1, e2), Some (TFloat as t) ->
    emit_functionN oc ?state (omod_of_type t ^".modulo") [Some t; Some t] [e1; e2]

  | Finalize, StatelessFun1 (_, Abs, e),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".abs") [Some t] [e]
  | Finalize, StatelessFun1 (_, Minus, e),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".neg") [Some t] [e]
  | Finalize, StatelessFun1 (_, Exp, e), Some TFloat ->
    emit_functionN oc ?state "exp" [Some TFloat] [e]
  | Finalize, StatelessFun1 (_, Log, e), Some TFloat ->
    emit_functionN oc ?state "log" [Some TFloat] [e]
  | Finalize, StatelessFun1 (_, Sqrt, e), Some TFloat ->
    emit_functionN oc ?state "sqrt" [Some TFloat] [e]
  | Finalize, StatelessFun1 (_, Hash, e), Some TI64 ->
    emit_functionN oc ?state "CodeGenLib.hash" [None] [e]

  (* Other stateless functions *)
  | Finalize, StatelessFun2 (_, Ge, e1, e2), Some TBool ->
    emit_functionN oc ?state "(>=)" [Some TAny; Some TAny] [e1; e2]
  | Finalize, StatelessFun2 (_, Gt, e1, e2), Some TBool ->
    emit_functionN oc ?state "(>)" [Some TAny; Some TAny] [e1; e2]
  | Finalize, StatelessFun2 (_, Eq, e1, e2), Some TBool ->
    emit_functionN oc ?state "(=)" [Some TAny; Some TAny] [e1; e2]
  | Finalize, StatelessFun2 (_, Concat, e1, e2), Some TString ->
    emit_functionN oc ?state "(^)" [Some TString; Some TString] [e1; e2]
  | Finalize, StatelessFunMisc (_, Like (e, p)), Some TBool ->
    let pattern = Globs.compile ~star:'%' ~escape:'\\' p in
    Printf.fprintf oc "(let pattern_ = \
      Globs.{ anchored_start = %b ; anchored_end = %b ; chunks = %a } in "
      pattern.anchored_start pattern.anchored_end
      (List.print (fun oc s -> Printf.fprintf oc "%S" s)) pattern.chunks ;
    emit_functionN oc ?state "Globs.matches pattern_ " [Some TString] [e];
    Printf.fprintf oc ")"
  | Finalize, StatelessFun1 (_, Length, e), Some TU16 (* The only possible output type *) ->
    emit_functionN oc ?state "String.length" [Some TString] [e]
  (* lowercase and uppercase assume latin1 and will gladly destroy UTF-8
   * encoded char, therefore we use the ascii variants: *)
  | Finalize, StatelessFun1 (_, Lower, e), Some TString ->
    emit_functionN oc ?state "String.lowercase_ascii" [Some TString] [e]
  | Finalize, StatelessFun1 (_, Upper, e), Some TString ->
    emit_functionN oc ?state "String.uppercase_ascii" [Some TString] [e]
  | Finalize, StatelessFun2 (_, And, e1, e2), Some TBool ->
    emit_functionN oc ?state "(&&)" [Some TBool; Some TBool] [e1; e2]
  | Finalize, StatelessFun2 (_, Or, e1,e2), Some TBool ->
    emit_functionN oc ?state "(||)" [Some TBool; Some TBool] [e1; e2]
  | Finalize, StatelessFun2 (_, (BitAnd|BitOr|BitXor as op), e1, e2),
    Some (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    let n = match op with BitAnd -> "logand" | BitOr -> "logor"
                        | _ -> "logxor" in
    emit_functionN oc ?state (omod_of_type t ^"."^ n) [Some t; Some t] [e1; e2]
  | Finalize, StatelessFun1 (_, Not, e), Some TBool ->
    emit_functionN oc ?state "not" [Some TBool] [e]
  | Finalize, StatelessFun1 (_, Defined, e), Some TBool ->
    (* Do not call emit_functionN to avoid null propagation: *)
    Printf.fprintf oc "(match %a with None -> false | _ -> true)"
      (emit_expr ?state ~context) e
  | Finalize, StatelessFun1 (_, Age, e),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as to_typ)
  | Finalize, StatelessFun1 (_, BeginOfRange, e),
    Some (TCidrv4 | TCidrv6 as to_typ) ->
    let in_type_name =
      String.lowercase (IO.to_string RamenScalar.print_typ to_typ) in
    let name = "CodeGenLib.age_"^ in_type_name in
    emit_functionN oc ?state name [Some to_typ] [e]
  (* TODO: Now() for Uint62? *)
  | Finalize, StatelessFun0 (_, Now), Some TFloat ->
    String.print oc "!CodeGenLib_IO.now"
  | Finalize, StatelessFun0 (_, Random), Some TFloat ->
    String.print oc "(Random.float 1.)"
  | Finalize, StatelessFun1 (_, Cast, e), t ->
    emit_functionN oc ?state "identity" [t] [e]
  (* Sequence build a sequence of as-large-as-convenient integers (signed or
   * not) *)
  | Finalize, StatelessFun2 (_, Sequence, e1, e2), Some TI128 ->
    emit_functionN oc ?state "CodeGenLib.sequence" [Some TI128; Some TI128] [e1; e2]
  | Finalize, StatelessFunMisc (_, Max es), t ->
    emit_functionN ~args_as:(Array 0) oc ?state "Array.max" (List.map (fun _ -> t) es) es
  | Finalize, StatelessFunMisc (_, Min es), t ->
    emit_functionN ~args_as:(Array 0) oc ?state "Array.min" (List.map (fun _ -> t) es) es

  (* Stateful functions *)
  | InitState, StatefulFun (_, _, AggrAnd _), (Some TBool as t) ->
    conv_to ?state ~context t oc (true_or_nul (is_nullable expr))
  | InitState, StatefulFun (_, _, AggrOr _), (Some TBool as t) ->
    conv_to ?state ~context t oc (false_or_nul (is_nullable expr))
  | Finalize, StatefulFun (_, g, (AggrAnd _|AggrOr _)), Some TBool ->
    emit_functionN oc ?state "identity" [None] [my_state g]
  | UpdateState, StatefulFun (_, g, AggrAnd (e)), _ ->
    emit_functionN oc ?state "(&&)" [None; Some TBool] [my_state g; e]
  | UpdateState, StatefulFun (_, g, AggrOr (e)), _ ->
    emit_functionN oc ?state "(||)" [None; Some TBool] [my_state g; e]

  | InitState, StatefulFun (_, _, AggrSum _),
    (Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) as t) ->
    conv_to ?state ~context t oc (zero_or_nul (is_nullable expr))
  | UpdateState, StatefulFun (_, g, AggrSum e),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN oc ?state (omod_of_type t ^".add") [None; Some t] [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrSum _), _ ->
    emit_functionN oc ?state "identity" [None] [my_state g]

  | InitState, StatefulFun (_, _, AggrAvg e), Some TFloat ->
    Printf.fprintf oc "%s(0, 0.)"
      (if is_nullable e then "Some " else "")
  | UpdateState, StatefulFun (_, g, AggrAvg e), Some (TFloat as t) ->
    emit_functionN oc ?state "CodeGenLib.avg_add" [None; Some t] [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrAvg _), _ ->
    emit_functionN oc ?state "CodeGenLib.avg_finalize" [None] [my_state g]

  | InitState, StatefulFun (_, _, AggrMax e), t ->
    conv_to ?state ~context t oc (min_of_num_type (typ_of e))
  | InitState, StatefulFun (_, _, AggrMin e), t ->
    conv_to ?state ~context t oc (max_of_num_type (typ_of e))
  | InitState, StatefulFun (_, _, (AggrFirst e|AggrLast e)), t ->
    conv_to ?state ~context t oc (any_constant_of_type (typ_of e))

  | Finalize, StatefulFun (_, g, (AggrMax _|AggrMin _|AggrFirst _|AggrLast _)), _ ->
    emit_functionN oc ?state "identity" [None] [my_state g]
  | UpdateState, StatefulFun (_, g, AggrMax (e)), _ ->
    emit_functionN oc ?state "max" [None; None] [my_state g; e]
  | UpdateState, StatefulFun (_, g, AggrMin (e)), _ ->
    emit_functionN oc ?state "min" [None; None] [my_state g; e]
  | UpdateState, StatefulFun (_, g, AggrFirst (e)), _ ->
    (* This hack relies on the fact that UpdateState is always called in
     * a context where we have the group.#count available and that its
     * name is "virtual_group_count_". *)
    emit_functionN oc ?state "(fun x y -> if virtual_group_count_ = Uint64.one then y else x)" [None; None] [my_state g; e]
  | UpdateState, StatefulFun (_, g, AggrLast (e)), _ ->
    emit_functionN oc ?state "(fun _ x -> x)" [None; None] [my_state g; e]

  (* Note: for InitState it is probably useless to check out_type.
   * For Finalize it is useful only to extract the types to be checked by Compiler. *)
  (* TODO: If any value is null the whole percentile is going to be null,
   * which is excessive because we could very easily compute good
   * lower and upper bounds, and that's often all that's needed. So maybe
   * the percentile should return 2 values? Or maybe we should have an
   * additional boolean parameter to tell which of the bounds we are
   * interested in? Or, if more function are like that, have a proper
   * `bound` or `pair` type constructor, with `low/high` or `first/second`
   * accessors? *)
  | InitState, StatefulFun (_, _, AggrPercentile (_p,e)), Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) ->
    Printf.fprintf oc "%s[]"
      (if is_nullable e then "Some " else "")
  | UpdateState, StatefulFun (_, g, AggrPercentile (_p,e)), Some TFloat ->
    emit_functionN oc ?state "CodeGenLib.float_percentile_add" [None; None] [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrPercentile (p,_e)), Some TFloat ->
    emit_functionN oc ?state "CodeGenLib.float_percentile_finalize" [Some TFloat; None] [p; my_state g]
  | UpdateState, StatefulFun (_, g, AggrPercentile (_p,e)), _ ->
    emit_functionN oc ?state "CodeGenLib.percentile_add" [None; None] [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrPercentile (p,_e)), Some (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) ->
    emit_functionN oc ?state "CodeGenLib.percentile_finalize" [Some TFloat; None] [p; my_state g]

  | InitState, StatefulFun (_, _, Lag (k,e)), _ ->
    emit_functionN oc ?state "CodeGenLib.Seasonal.init"
      [Some TU32; Some TU32; None]
      [k; expr_one; any_constant_of_type (typ_of e)]
  | UpdateState, StatefulFun (_, g, Lag (_k,e)), _ ->
    emit_functionN oc ?state "CodeGenLib.Seasonal.add" [None; None] [my_state g; e]
  | Finalize, StatefulFun (_, g, Lag _), _ ->
    emit_functionN oc ?state "CodeGenLib.Seasonal.lag" [None] [my_state g]

  (* We force the inputs to be float since we are going to return a float anyway. *)
  | InitState, StatefulFun (_, _, (MovingAvg(p,n,_)|LinReg(p,n,_))), Some TFloat ->
    emit_functionN oc ?state "CodeGenLib.Seasonal.init" [Some TU32; Some TU32; Some TFloat] [p; n; expr_zero]
  | UpdateState, StatefulFun (_, g, (MovingAvg(_p,_n,e)|LinReg(_p,_n,e))), _ ->
    emit_functionN oc ?state "CodeGenLib.Seasonal.add" [None; Some TFloat] [my_state g; e]
  | Finalize, StatefulFun (_, g, MovingAvg (p,n,_)), Some TFloat ->
    emit_functionN oc ?state "CodeGenLib.Seasonal.avg" [Some TU32; Some TU32; None] [p; n; my_state g]
  | Finalize, StatefulFun (_, g, LinReg (p,n,_)), Some TFloat ->
    emit_functionN oc ?state "CodeGenLib.Seasonal.linreg" [Some TU32; Some TU32; None] [p; n; my_state g]
  | Finalize, StatefulFun (_, g, MultiLinReg (p,n,_,_)), Some TFloat ->
    emit_functionN oc ?state "CodeGenLib.Seasonal.multi_linreg" [Some TU32; Some TU32; None] [p; n; my_state g]

  | InitState, StatefulFun (_, _, MultiLinReg (p,n,_,es)), Some TFloat ->
    emit_functionNv oc ?state "CodeGenLib.Seasonal.init_multi_linreg"
      [Some TU32; Some TU32; Some TFloat]
      [p; n; expr_zero]
      (Some TFloat) (List.map (fun _ -> expr_zero) es)
  | UpdateState, StatefulFun (_, g, MultiLinReg (_p,_n,e,es)), _ ->
    emit_functionNv oc ?state "CodeGenLib.Seasonal.add_multi_linreg" [None; Some TFloat] [my_state g; e] (Some TFloat) es

  | InitState, StatefulFun (_, _, ExpSmooth (_a,_)), (Some TFloat as t) ->
    conv_to ?state ~context t oc (zero_or_nul (is_nullable expr))
  | UpdateState, StatefulFun (_, g, ExpSmooth (a,e)), _ ->
    emit_functionN oc ?state "CodeGenLib.smooth" [None; Some TFloat; Some TFloat] [my_state g; a; e]
  | Finalize, StatefulFun (_, g, ExpSmooth _), Some TFloat ->
    emit_functionN oc ?state "identity" [None] [my_state g]

  | InitState, StatefulFun (_, _, Remember (fpr,_tim,dur,_e)), Some TBool ->
    emit_functionN oc ?state "CodeGenLib.remember_init" [Some TFloat; Some TFloat] [fpr; dur]
  | UpdateState, StatefulFun (_, g, Remember (_fpr,tim,_dur,e)), _ ->
    emit_functionN oc ?state "CodeGenLib.remember_add" [None; Some TFloat; None] [my_state g; tim; e]
  | Finalize, StatefulFun (_, g, Remember _), Some TBool ->
    emit_functionN oc ?state "CodeGenLib.remember_finalize" [None] [my_state g]

  | InitState, StatefulFun (_, _, Distinct _es), _ ->
    Printf.fprintf oc "%s(CodeGenLib.distinct_init ())"
      (if is_nullable expr then "Some " else "")
  | UpdateState, StatefulFun (_, g, Distinct es), _ ->
    emit_functionN oc ?state ~args_as:(Tuple 1) "CodeGenLib.distinct_add" (None :: List.map (fun e -> None) es) (my_state g :: es)
  | Finalize, StatefulFun (_, g, Distinct es), Some TBool ->
    emit_functionN oc ?state "CodeGenLib.distinct_finalize" [None] [my_state g]

  | InitState, StatefulFun (_, _, Hysteresis _), t ->
    conv_to ?state ~context t oc (true_or_nul (is_nullable expr)) (* initially within bounds *)
  | UpdateState, StatefulFun (_, g, Hysteresis (meas, accept, max)), Some TBool ->
    let t = (typ_of meas).scalar_typ in (* TODO: shouldn't we promote everything to the most accurate of those types? *)
    emit_functionN oc ?state "CodeGenLib.hysteresis_update" [None; t; t; t] [my_state g; meas; accept; max]
  | Finalize, StatefulFun (_, g, Hysteresis _), Some TBool ->
    emit_functionN oc ?state "CodeGenLib.hysteresis_finalize" [None] [my_state g]

  (* Generator: the function appears only during tuple generation, where
   * it sends the output to its continuation as (freevar_name t).
   * In normal expressions we merely refer to that free variable. *)
  | Generator, GeneratorFun (_, Split (e1,e2)), Some TString ->
    emit_functionN oc ?state "CodeGenLib.split" [Some TString; Some TString] [e1; e2]
  | Finalize, GeneratorFun (t, Split (_e1,_e2)), Some TString -> (* Output it as a free variable *)
    String.print oc (freevar_name t)

  | _, _, Some _ ->
    let m =
      Printf.sprintf "Cannot find implementation of %s for context %s"
        (IO.to_string (print true) expr)
        (string_of_context context) in
    failwith m
  | _, _, None -> (* untyped?! *)
    assert false

and add_missing_types arg_typs es =
  let open RamenExpr in
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
    | Some t1, Some t2 -> Some (RamenScalar.larger_type (t1, t2)) in
  let rec loop ht rt any_type n = function
  | [], _ -> (* No more arguments *)
    (* Replace all None types by a common type large enough to accommodate
     * them all: any_type. *)
    let ht = List.map (fun t ->
      if t <> Some TAny then t else any_type) ht in
    List.rev_append ht (List.init n (fun _ -> rt))
  | e::es, t::ts ->
    let any_type =
      if t <> Some TAny then any_type else
      merge_types any_type (typ_of e).scalar_typ in
    loop (t::ht) t any_type n (es, ts)
  | e::es, [] -> (* Missing some types: update rt *)
    let te = (typ_of e).scalar_typ in
    if rt = Some TAny then
      loop ht rt (merge_types any_type te) (n+1) (es, [])
    else
      loop ht (merge_types rt te) any_type (n+1) (es, [])
  in
  loop [] None None 0 (es, arg_typs)

(*$inject
  open Batteries
  open Stdint
  let const typ v = RamenLang.(RamenExpr.(Const (make_typ ~typ "test", v)))
 *)
(*$= add_missing_types & ~printer:(IO.to_string (List.print (Option.print (RamenScalar.print_typ))))
  [Some TFloat] \
    (add_missing_types [Some TFloat] [const TFloat (VFloat 1.)])
  [Some TFloat] \
    (add_missing_types [] [const TFloat (VFloat 1.)])

  [Some TFloat; Some TU8] \
    (add_missing_types [Some TFloat; Some TU8] [const TFloat (VFloat 1.); const TU8 (VU8 (Uint8.of_int 42))])

  [Some TFloat; Some TU16; Some TU16] \
    (add_missing_types [Some TFloat; Some TU16] [const TFloat (VFloat 1.); const TU8 (VU8 (Uint8.of_int 42)); const TU8 (VU8 (Uint8.of_int 42))])

  [Some TFloat; Some TU16; Some TU16] \
    (add_missing_types [Some TFloat; Some TU16] [const TFloat (VFloat 1.); const TU8 (VU8 (Uint8.of_int 42)); const TU16 (VU16 (Uint16.of_int  42))])

  [Some TFloat; Some TU16; Some TU16] \
    (add_missing_types [Some TFloat; Some TU16] [const TFloat (VFloat 1.); const TU16 (VU16 (Uint16.of_int 42)); const TU8 (VU8 (Uint8.of_int 42))])

  [Some TFloat; Some TU16; Some TU16] \
    (add_missing_types [Some TFloat; Some TAny; Some TAny] [const TFloat (VFloat 1.); const TU16 (VU16 (Uint16.of_int 42)); const TU8 (VU8 (Uint8.of_int 42))])

  [Some TFloat; Some TU16; Some TU16] \
    (add_missing_types [Some TFloat; Some TAny; Some TAny] [const TFloat (VFloat 1.); const TU8 (VU8 (Uint8.of_int 42)); const TU16 (VU16 (Uint16.of_int 42))])

  [None; Some TFloat] \
    (add_missing_types [None; Some TFloat] [const TFloat (VFloat 1.); const TFloat (VFloat 1.)])
 *)

(* When we combine nullable arguments we want to shortcut as much as
 * possible and avoid evaluating any of them if one is null. Here we will just
 * evaluate them in order until one is found to be nullable and null, or until
 * we evaluated them all, and then only we call the function.
 * TODO: ideally we'd like to evaluate the nullable arguments first. *)
and emit_function ?(args_as=Arg) oc ?state impl arg_typs es vt_specs_opt =
  let open RamenExpr in
  let arg_typs = add_missing_types arg_typs es in
  let len, has_nullable =
    List.fold_left2 (fun (i, had_nullable) e arg_typ ->
        if is_nullable e then (
          Printf.fprintf oc "(match %a with None -> None | Some x%d_ -> "
            (conv_to ?state ~context:Finalize arg_typ) e
            i ;
          i + 1, true
        ) else (
          Printf.fprintf oc "(let x%d_ =\n\t\t%a in "
            i
            (conv_to ?state ~context:Finalize arg_typ) e ;
          i + 1, had_nullable
        )
      ) (0, false) es arg_typs
  in
  Printf.fprintf oc "%s(%s" (if has_nullable then "Some " else "") impl ;
  for i = 0 to len-1 do
    Printf.fprintf oc "%s"
      (match args_as with Array n when i = n -> "[| "
                        | Array n when i > n -> ";"
                        | Tuple n when i = n -> "("
                        | Tuple n when i > n -> ", "
                        | _ -> "") ;
    Printf.fprintf oc " x%d_" i
  done ;
  Printf.fprintf oc "%s"
    (match args_as with Arg -> "" | Array _ -> " |]" | Tuple _ -> ")") ;
  (* variadic arguments [ves] are passed as a last argument to impl, as an array *)
  Option.may (fun (vt, ves) ->
      (* TODO: handle NULLability *)
      List.print ~first:" [| " ~last:" |]" ~sep:"; "
                 (conv_to ?state ~context:Finalize vt) oc ves)
    vt_specs_opt ;
  for _i = 0 to len do Printf.fprintf oc ")" done

and emit_functionN ?args_as oc ?state impl arg_typs es =
  emit_function ?args_as oc ?state impl arg_typs es None

and emit_functionNv oc ?state impl arg_typs es vt ves =
  emit_function oc ?state impl arg_typs es (Some (vt, ves))

let emit_compute_nullmask_size oc ser_typ =
  Printf.fprintf oc "\tlet nullmask_bytes_ =\n" ;
  Printf.fprintf oc "\t\tList.fold_left2 (fun s nullable keep ->\n" ;
  Printf.fprintf oc "\t\t\tif nullable && keep then s+1 else s) 0\n" ;
  Printf.fprintf oc "\t\t\t%a\n"
    (List.print (fun oc field -> Bool.print oc field.nullable))
      ser_typ ;
  Printf.fprintf oc "\t\t\tskiplist_ |>\n" ;
  Printf.fprintf oc "\t\tRingBufLib.bytes_for_bits |>\n" ;
  Printf.fprintf oc "\t\tRingBufLib.round_up_to_rb_word in\n"

let emit_sersize_of_tuple name oc tuple_typ =
  (* We want the sersize of the serialized version of course: *)
  let ser_typ = RingBufLib.ser_tuple_typ_of_tuple_typ tuple_typ in
  (* Like for serialize_tuple, we receive first the skiplist and then the
   * actual tuple, so we can compute the nullmask in advance: *)
  Printf.fprintf oc "let %s skiplist_ =\n" name ;
  emit_compute_nullmask_size oc ser_typ ;
  (* Now for the code run for each tuple: *)
  Printf.fprintf oc "\tfun %a ->\n"
    (print_tuple_deconstruct TupleOut) tuple_typ ;
  Printf.fprintf oc "\t\tlet sz_ = nullmask_bytes_ in\n" ;
  List.iter (fun field ->
      let id = id_of_field_typ ~tuple:TupleOut field in
      Printf.fprintf oc "\t\t(* %s *)\n" id ;
      Printf.fprintf oc "\t\tlet sz_ = sz_ + if List.hd skiplist_ then (\n" ;
      if field.nullable then
        Printf.fprintf oc "\t\t\tmatch %s with None -> 0 | Some x_ -> %a\n"
          id
          (emit_sersize_of_field_var field.typ) "x_"
      else
        Printf.fprintf oc "\t\t\t%a"
          (emit_sersize_of_field_var field.typ) id ;
      Printf.fprintf oc "\t\t) else 0 in\n" ;
      Printf.fprintf oc "\t\tlet skiplist_ = List.tl skiplist_ in\n" ;
    ) ser_typ ;
  Printf.fprintf oc "\t\tignore skiplist_ ;\n" ;
  Printf.fprintf oc "\t\tsz_\n"

let emit_set_value tx_var offs_var field_var oc field_typ =
  Printf.fprintf oc "RingBuf.write_%s %s %s %s"
    (id_of_typ field_typ) tx_var offs_var field_var

(* The function that will serialize the fields of the tuple at the given
 * addresses. The first argument is a bitmask of the fields that we must
 * actually output (true = output, false = skip) in serialization order.
 * CodeGenLib will first call the function with this single parameter, leaving
 * us the opportunity to specialize the actual outputer according to this
 * skiplist (here we merely compute the nullmask size).
 * Everything else (allocating on the RB and writing the record size) is
 * independent of the tuple type and is handled in the library.
 * Returns the final offset for * checking with serialized size of this
 * tuple. *)
let emit_serialize_tuple name oc tuple_typ =
  (* Serialize in tuple_typ.typ_name order: *)
  let ser_typ = RingBufLib.ser_tuple_typ_of_tuple_typ tuple_typ in
  Printf.fprintf oc "let %s skiplist_ =\n" name ;
  emit_compute_nullmask_size oc ser_typ ;
  Printf.fprintf oc "\tfun tx_ %a ->\n"
    (print_tuple_deconstruct TupleOut) tuple_typ ;
  if verbose_serialization then
    Printf.fprintf oc "\t\t!RamenLog.logger.RamenLog.debug \"Serialize a tuple, nullmask_bytes=%%d\" nullmask_bytes_ ;\n" ;
  (* Start by zeroing the nullmask *)
  Printf.fprintf oc "\t\tif nullmask_bytes_ > 0 then\n\
                     \t\t\tRingBuf.zero_bytes tx_ 0 nullmask_bytes_ ; (* zero the nullmask *)\n" ;
  Printf.fprintf oc "\t\tlet offs_, nulli_ = nullmask_bytes_, 0 in\n" ;
  List.iter (fun field ->
      Printf.fprintf oc "\t\tlet offs_, nulli_ =\n\
                         \t\t\tif List.hd skiplist_ then (\n" ;
      let id = id_of_field_typ ~tuple:TupleOut field in
      if field.nullable then (
        (* Write either nothing (since the nullmask is initialized with 0) or
         * the nullmask bit and the value *)
        Printf.fprintf oc "\t\t\t\tmatch %s with\n" id ;
        Printf.fprintf oc "\t\t\t\t| None -> offs_, nulli_ + 1\n" ;
        Printf.fprintf oc "\t\t\t\t| Some x_ ->\n" ;
        Printf.fprintf oc "\t\t\t\t\tRingBuf.set_bit tx_ nulli_ ;\n" ;
        Printf.fprintf oc "\t\t\t\t\t%a ;\n"
          (emit_set_value "tx_" "offs_" "x_") field.typ ;
        if verbose_serialization then
          Printf.fprintf oc "\t\t\t\t!RamenLog.logger.RamenLog.debug \"Serializing %s (Some %%s) at offset %%d\" (dump x_) offs_ ;\n" id ;
        Printf.fprintf oc "\t\t\t\t\toffs_ + %a, nulli_ + 1\n"
          (emit_sersize_of_field_var field.typ) "x_"
      ) else (
        Printf.fprintf oc "\t\t\t\t%a ;\n"
          (emit_set_value "tx_" "offs_" id) field.typ ;
        if verbose_serialization then
          Printf.fprintf oc "\t\t\t\t!RamenLog.logger.RamenLog.debug \"Serializing %s (%%s) at offset %%d\" (dump %s) offs_ ;\n" id id ;
        Printf.fprintf oc "\t\t\t\toffs_ + %a, nulli_\n"
          (emit_sersize_of_field_var field.typ) id
      ) ;
      Printf.fprintf oc "\t\t\t) else offs_, nulli_ in\n" ;
      Printf.fprintf oc "\t\tlet skiplist_ = List.tl skiplist_ in\n"
    ) ser_typ ;
  Printf.fprintf oc "\t\tignore skiplist_ ;\n" ;
  Printf.fprintf oc "\t\toffs_\n"

let rec emit_indent oc n =
  if n > 0 then (
    Printf.fprintf oc "\t" ;
    emit_indent oc (n-1)
  )

(* Emit a function that, given an array of strings (corresponding to a line of
 * CSV) will return the tuple defined by [tuple_typ] or raises
 * some exception *)
let emit_tuple_of_strings name csv_null oc tuple_typ =
  Printf.fprintf oc "let %s strs_ =\n" name ;
  Printf.fprintf oc "\t(\n" ;
  let nb_fields = List.length tuple_typ in
  List.iteri (fun i field_typ ->
    let sep = if i < nb_fields - 1 then "," else "" in
    Printf.fprintf oc "\t\t(try (\n" ;
    if field_typ.nullable then (
      Printf.fprintf oc "\t\t\t(let s_ = strs_.(%d) in\n" i ;
      Printf.fprintf oc "\t\t\tif s_ = %S then None else Some (%a))\n"
        csv_null
        (emit_value_of_string field_typ.typ) "s_"
    ) else (
      let s_var = Printf.sprintf "strs_.(%d)" i in
      Printf.fprintf oc "\t\t\t%a\n"
        (emit_value_of_string field_typ.typ) s_var
    ) ;
    Printf.fprintf oc "\t\t) with exn -> (\n" ;
    Printf.fprintf oc "\t\t\t!RamenLog.logger.RamenLog.error \"Cannot parse field %d: %s\" ;\n" (i+1) field_typ.typ_name ;
    Printf.fprintf oc "\t\t\traise exn))%s\n" sep ;
  ) tuple_typ ;
  Printf.fprintf oc "\t)\n"

let emit_time_of_tuple name event_time oc tuple_typ =
  Printf.fprintf oc "let %s %a =\n"
    name
    (print_tuple_deconstruct TupleOut) tuple_typ ;
  match event_time with
  | None ->
      Printf.fprintf oc "\t0., 0. (* No event time info *)\n"
  | Some ((sta_field, sta_scale), dur) ->
      let field_value_to_float oc field_name =
        let f = List.find (fun t -> t.typ_name = field_name) tuple_typ in
        Printf.fprintf oc
          (if f.nullable then "(%a |? 0.)" else "%a")
          (conv_from_to f.typ ~nullable:f.nullable TFloat String.print)
            (id_of_field_name ~tuple:TupleOut field_name)
      in
      Printf.fprintf oc "\tlet start_ = %a *. %a\n"
        field_value_to_float sta_field
        emit_float sta_scale ;
      (match dur with
      | RamenEventTime.DurationConst d ->
          Printf.fprintf oc "\tand dur_ = %a in\n\
                             \tstart_, start_ +. dur_\n"
            emit_float d
      | RamenEventTime.DurationField (dur_field, dur_scale) ->
          Printf.fprintf oc "\tand dur_ = %a *. %a in\n\
                             \tstart_, start_ +. dur_\n"
            field_value_to_float dur_field
            emit_float dur_scale ;
      | RamenEventTime.StopField (sto_field, sto_scale) ->
          Printf.fprintf oc "\tand stop_ = %a *. %a in\n\
                             \tstart_, stop_\n"
            field_value_to_float sto_field
            emit_float sto_scale)

(* Given a tuple type, generate the ReadCSVFile operation. *)
let emit_read_csv_file oc name csv_fname unlink csv_separator csv_null
                       tuple_typ preprocessor event_time =
  (* The dynamic part comes from the unpredictable field list.
   * For each input line, we want to read all fields and build a tuple.
   * Then we want to write this tuple in some ring buffer.
   * We need to generate these functions:
   * - reading a CSV string into a tuple type (when nullable fields are option type)
   * - given such a tuple, return its serialized size
   * - given a pointer toward the ring buffer, serialize the tuple *)
  Printf.fprintf oc
     "open Batteries\nopen Stdint\n\n\
     %a\n%a\n%a\n%a\n\
     let %s () =\n\
       \tCodeGenLib.read_csv_file %S %b %S sersize_of_tuple_ time_of_tuple_ serialize_tuple_ tuple_of_strings_ %S\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_time_of_tuple "time_of_tuple_" event_time) tuple_typ
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    (emit_tuple_of_strings "tuple_of_strings_" csv_null) tuple_typ
    name
    csv_fname unlink csv_separator preprocessor

let emit_listen_on oc name net_addr port proto =
  let open RamenProtocols in
  let tuple_typ = tuple_typ_of_proto proto in
  let collector = collector_of_proto proto in
  let event_time = event_time_of_proto proto in
  Printf.fprintf oc "open Batteries\nopen Stdint\n\n\
    %a\n%a\n%a\n\
    let %s () =\n\
      \tCodeGenLib.listen_on %s %S %d %S sersize_of_tuple_ time_of_tuple_ serialize_tuple_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_time_of_tuple "time_of_tuple_" event_time) tuple_typ
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    name
    collector
    (Unix.string_of_inet_addr net_addr) port
    (string_of_proto proto)

let emit_instrumentation oc name from =
  let open RamenProtocols in
  let tuple_typ = RamenBinocle.tuple_typ in
  let event_time = RamenBinocle.event_time in
  Printf.fprintf oc "open Batteries\nopen Stdint\n\n\
    %a\n%a\n%a\n\
    let %s () =\n\
      \tCodeGenLib.instrumentation %a sersize_of_tuple_ time_of_tuple_ serialize_tuple_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_time_of_tuple "time_of_tuple_" event_time) tuple_typ
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    name
    (List.print (fun oc s -> Printf.fprintf oc "%S" s)) from

(* tuple must be some kind of _input_ tuple *)
let emit_in_tuple ?(tuple=TupleIn) mentioned and_all_others oc in_typ =
  print_tuple_deconstruct tuple oc (List.filter_map (fun field_typ ->
    if and_all_others || Set.mem field_typ.typ_name mentioned then
      Some field_typ else None) in_typ)

(* We do not want to read the value from the RB each time it's used,
 * so extract a tuple from the ring buffer. As an optimisation, read
 * (and return) only the mentioned fields. *)
let emit_read_tuple name mentioned and_all_others oc in_typ =
  (* Deserialize in in_tuple_typ.typ_name order: *)
  let ser_typ = RingBufLib.ser_tuple_typ_of_tuple_typ in_typ in
  Printf.fprintf oc "\
    let %s tx_ =\n\
    \tlet offs_ = %d in\n"
    name
    (RingBufLib.nullmask_bytes_of_tuple_type ser_typ) ;
  if verbose_serialization then
    Printf.fprintf oc "\t!RamenLog.logger.RamenLog.debug \"Deserializing a tuple\" ;\n" ;
  let _ = List.fold_left (fun nulli field ->
      let id = id_of_field_typ ~tuple:TupleIn field in
      if and_all_others || Set.mem field.typ_name mentioned then (
        (* TODO: let id, offs_ = ... would be faster *)
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
        if verbose_serialization then
          Printf.fprintf oc "\t!RamenLog.logger.RamenLog.debug \"deserialized field %s (%%s) at ofset %%d\" (dump %s) offs_ ;\n" id id ;
        Printf.fprintf oc "\tlet offs_ = " ;
        if field.nullable then
          Printf.fprintf oc
            "(match %s with None -> offs_ | Some %s -> offs_ + %a) in\n"
            id id
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
    ) 0 ser_typ in
  Printf.fprintf oc "\tignore offs_ ;\n" ; (* avoid a warning *)
  (* We want to output the tuple with fields ordered according to the
   * select clause specified order, not according to serialization order: *)
  let in_typ_only_ser =
    List.filter (fun t ->
      not (is_private_field t.RamenTuple.typ_name)
    ) in_typ in
  Printf.fprintf oc "\t%a\n"
    (emit_in_tuple mentioned and_all_others) in_typ_only_ser

(* We know that somewhere in expr we have one or several generators.
 * First we transform the AST to move the generators to the root,
 * and insert "free variables" (named after the generator uniq_num)
 * where the generator used to stand. Once this is done, the AST
 * start with a chain of generator, and then an expression that is
 * free of generators. We want to emit:
 * (fun k -> gen1 (fun fv1 -> gen2 (fun fv2 -> ... -> genN (fun fvN ->
 *    k (expr ...)))))
 *)
let emit_generator user_fun oc expr =
  let open RamenExpr in

  let generators = fold_by_depth (fun prev e ->
    match e with
    | GeneratorFun _ -> e :: prev
    | _ -> prev) [] expr |>
    List.rev (* Inner generator first: *)
  in

  (* Now we start with all the generator. Inner generators are first,
   * so we can confidently call emit_expr on the arguments and if this uses a
   * free variable it should be defined already: *)
  let emit_gen_root oc = function
    | GeneratorFun (t, Split _) as expr ->
      Printf.fprintf oc "%a (fun %s -> "
        (emit_expr ?state:None ~context:Generator) expr
        (freevar_name t)
    (* We have no other generators (yet) *)
    | _ -> assert false
  in
  List.iter (emit_gen_root oc) generators ;

  (* Finally, call user_func on the actual expression, where all generators will
   * be replaced by their free variable: *)
  Printf.fprintf oc "%s (%a)"
    user_fun
    (emit_expr ?state:None ~context:Finalize) expr ;
  List.iter (fun _ -> Printf.fprintf oc ")") generators

let emit_generate_tuples name in_typ mentioned and_all_others out_typ oc selected_fields =
  let has_generator =
    List.exists (fun sf ->
      RamenExpr.is_generator sf.RamenOperation.expr)
      selected_fields in
  if not has_generator then
    Printf.fprintf oc "let %s f_ _it_ ot_ = f_ ot_\n" name
  else (
    Printf.fprintf oc "let %s f_ %a %a =\n"
      name
      (emit_in_tuple mentioned and_all_others) in_typ
      (print_tuple_deconstruct TupleOut) out_typ ;
    (* Each generator is a functional receiving the continuation and calling it
     * as many times as there are values. *)
    let nb_gens =
      List.fold_left (fun nb_gens sf ->
          if not (RamenExpr.is_generator sf.RamenOperation.expr) then nb_gens
          else (
            let ff_ = "ff_"^ string_of_int nb_gens ^"_" in
            Printf.fprintf oc "%a(fun %s -> %a) (fun generated_%d_ ->\n"
              emit_indent (1 + nb_gens)
              ff_
              (emit_generator ff_) sf.RamenOperation.expr
              nb_gens ;
            nb_gens + 1)
        ) 0 selected_fields in
    (* Now we have all the generated values, actually call f_ on the tuple.
     * Note that the tuple must be in out_typ order: *)
    Printf.fprintf oc "%af_ (\n%a"
      emit_indent (1 + nb_gens)
      emit_indent (2 + nb_gens) ;
    let expr_of_field name =
      let sf = List.find (fun sf ->
                 sf.RamenOperation.alias = name) selected_fields in
      sf.RamenOperation.expr in
    let _ = List.fold_lefti (fun gi i ft ->
        if i > 0 then Printf.fprintf oc ",\n%a" emit_indent (2 + nb_gens) ;
        match RamenExpr.is_generator (expr_of_field ft.typ_name) with
        | exception Not_found ->
          (* For star-imported fields: *)
          Printf.fprintf oc "%s"
            (id_of_field_name ft.typ_name) ;
          gi
        | true ->
          Printf.fprintf oc "generated_%d_" gi ;
          gi + 1
        | false ->
          Printf.fprintf oc "%s"
            (id_of_field_name ~tuple:TupleOut ft.typ_name) ;
          gi
        ) 0 out_typ in
    for _ = 1 to nb_gens do Printf.fprintf oc ")" done ;
    Printf.fprintf oc ")\n"
  )

let emit_field_of_tuple name oc tuple_typ =
  Printf.fprintf oc "let %s %a = function\n"
    name
    (print_tuple_deconstruct TupleOut) tuple_typ ;
  List.iter (fun field_typ ->
      Printf.fprintf oc "\t| %S -> " field_typ.typ_name ;
      let id = id_of_field_name ~tuple:TupleOut field_typ.typ_name in
      if field_typ.nullable then (
        Printf.fprintf oc "(match %s with None -> \"?null?\" | Some v_ -> %a)\n"
          id
          (conv_from_to field_typ.typ ~nullable:false TString String.print) "v_"
      ) else (
        Printf.fprintf oc "%a\n"
          (conv_from_to field_typ.typ ~nullable:false TString String.print) id
      )
    ) tuple_typ ;
  Printf.fprintf oc "\t| _ -> raise Not_found\n"

let emit_state_update_for_expr oc expr =
  RamenExpr.unpure_iter (function
      | RamenExpr.StatefulFun (_, lifespan, _) as e ->
        let state_var =
          match lifespan with LocalState -> "group_"
                            | GlobalState -> "global_" in
        Printf.fprintf oc "\t%s.%s <- (%a) ;\n"
          state_var
          (name_of_state e)
          (emit_expr ?state:None ~context:UpdateState) e
      | _ -> ()
    ) expr

let emit_where
      ?(with_group=false) ?(always_true=false)
      name in_typ mentioned and_all_others oc expr =
  Printf.fprintf oc "let %s global_ virtual_in_count_ %a %a \
                       virtual_selected_count_ virtual_selected_successive_ %a \
                       virtual_unselected_count_ virtual_unselected_successive_ %a \
                       virtual_out_count_ out_previous_opt_ "
    name
    (emit_in_tuple mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastIn mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastSelected mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastUnselected mentioned and_all_others) in_typ ;
  if with_group then
    Printf.fprintf oc "virtual_group_count_ virtual_group_successive_ group_ %a %a "
      (emit_in_tuple ~tuple:TupleGroupFirst mentioned and_all_others) in_typ
      (emit_in_tuple ~tuple:TupleGroupLast mentioned and_all_others) in_typ ;
  if always_true then
    Printf.fprintf oc "= true\n"
  else (
    Printf.fprintf oc "=\n" ;
    (* Update the states used by this expression: *)
    emit_state_update_for_expr oc expr ;
    Printf.fprintf oc "\t%a\n"
      (emit_expr ?state:None ~context:Finalize) expr
  )

let emit_field_selection
      ?(with_selected=false) (* and unselected *)
      ?(with_group=false) (* including previous, of type tuple_out option *)
      name in_typ mentioned
      and_all_others out_typ oc selected_fields =
  Printf.fprintf oc "let %s virtual_in_count_ %a %a "
    name
    (emit_in_tuple mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastIn mentioned and_all_others) in_typ ;
  if with_selected then
    Printf.fprintf oc "virtual_selected_count_ virtual_selected_successive_ %a \
                       virtual_unselected_count_ virtual_unselected_successive_ %a "
      (emit_in_tuple ~tuple:TupleLastSelected mentioned and_all_others) in_typ
      (emit_in_tuple ~tuple:TupleLastUnselected mentioned and_all_others) in_typ ;
  if with_group then
    Printf.fprintf oc "virtual_out_count out_previous_opt_ group_previous_opt_ \
                       virtual_group_count_ virtual_group_successive_ group_ global_ %a %a "
      (emit_in_tuple ~tuple:TupleGroupFirst mentioned and_all_others) in_typ
      (emit_in_tuple ~tuple:TupleGroupLast mentioned and_all_others) in_typ ;
  Printf.fprintf oc "=\n" ;
  List.iter (fun sf ->
      (* Update the states as required for this field, just before
       * computing the field actual value. *)
      Printf.fprintf oc "\t(* State Updates: *)\n" ;
      emit_state_update_for_expr oc sf.RamenOperation.expr ;
      Printf.fprintf oc "\t(* Output field: *)\n" ;
      if RamenExpr.is_generator sf.RamenOperation.expr then
        (* So that we have a single out_typ both before and after tuples generation *)
        Printf.fprintf oc "\tlet %s = () in\n"
          (id_of_field_name ~tuple:TupleOut sf.RamenOperation.alias)
      else
        Printf.fprintf oc "\tlet %s = %a in\n"
          (id_of_field_name ~tuple:TupleOut sf.RamenOperation.alias)
          (emit_expr ?state:None ~context:Finalize) sf.RamenOperation.expr
    ) selected_fields ;
  (* Here we must generate the tuple in the order specified by out_type,
   * not selected_fields: *)
  let is_selected name =
    List.exists (fun sf -> sf.RamenOperation.alias = name) selected_fields in
  Printf.fprintf oc "\t(\n\t\t" ;
  List.iteri (fun i ft ->
      let tuple =
        if is_selected ft.typ_name then TupleOut else TupleIn in
      Printf.fprintf oc "%s%s"
        (if i > 0 then ",\n\t\t" else "")
        (id_of_field_name ~tuple ft.typ_name) ;
    ) out_typ ;
  Printf.fprintf oc "\n\t)\n"

(* Similar to emit_field_selection but with less options, no concept of star and no
 * naming of the fields as the fields from out, since that's not the out tuple
 * we are constructing: *)
let emit_key_of_input name in_typ mentioned and_all_others oc exprs =
  Printf.fprintf oc "let %s %a =\n\t("
    name
    (emit_in_tuple mentioned and_all_others) in_typ ;
  List.iteri (fun i expr ->
      Printf.fprintf oc "%s\n\t\t%a"
        (if i > 0 then "," else "")
        (emit_expr ?state:None ~context:Finalize) expr ;
    ) exprs ;
  Printf.fprintf oc "\n\t)\n"

let emit_top name in_typ mentioned and_all_others out_typ oc top =
  Printf.fprintf oc "let %s =" name ;
  match top with
  | None -> Printf.fprintf oc " None\n"
  | Some (n, by) ->
    (* The function that updates top_state from in_tuple: *)
    (* FIXME: rename all those "group_" into "local_" *)
    Printf.fprintf oc
      "\n\tlet top_update_ virtual_in_count_ %a %a \
             virtual_selected_count_ virtual_selected_successive_ %a \
             virtual_unselected_count_ virtual_unselected_successive_ %a \
             virtual_out_count out_previous_opt_ group_previous_opt_ \
             virtual_group_count_ virtual_group_successive_ group_ \
             global_ %a %a %a =\n\
             \t\t(%a) (* parentheses to make it legit if empty *) in\n"
      (emit_in_tuple mentioned and_all_others) in_typ
      (emit_in_tuple ~tuple:TupleLastIn mentioned and_all_others) in_typ
      (emit_in_tuple ~tuple:TupleLastSelected mentioned and_all_others) in_typ
      (emit_in_tuple ~tuple:TupleLastUnselected mentioned and_all_others) in_typ
      (emit_in_tuple ~tuple:TupleGroupFirst mentioned and_all_others) in_typ
      (emit_in_tuple ~tuple:TupleGroupLast mentioned and_all_others) in_typ
      (emit_tuple TupleOut) out_typ
      emit_state_update_for_expr by ;
    Printf.fprintf oc
      "\tSome (\n\
       \t\t(Uint32.to_int (%a)),\n\
       \t\ttop_update_)\n"
      (conv_to ~context:Finalize (Some TU32)) n

let emit_float_of_top name in_typ mentioned and_all_others out_typ oc top_by =
  Printf.fprintf oc
    "let %s virtual_in_count_ %a %a \
         virtual_selected_count_ virtual_selected_successive_ %a \
         virtual_unselected_count_ virtual_unselected_successive_ %a \
         virtual_out_count out_previous_opt_ group_previous_opt_ \
         virtual_group_count_ virtual_group_successive_ group_ \
         global_ %a %a %a =\n"
    name
    (emit_in_tuple mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastIn mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastSelected mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastUnselected mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleGroupFirst mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleGroupLast mentioned and_all_others) in_typ
    (emit_tuple TupleOut) out_typ ;
  match top_by with
  | None -> String.print oc "0."
  | Some top_by ->
    Printf.fprintf oc "\t%a\n"
      (conv_to ~context:Finalize (Some TFloat)) top_by

let for_each_unpure_fun selected_fields
                        ?where ?commit_when ?top_by f =
  List.iter (fun sf ->
      RamenExpr.unpure_iter f sf.RamenOperation.expr
    ) selected_fields ;
  Option.may (RamenExpr.unpure_iter f) where ;
  Option.may (RamenExpr.unpure_iter f) commit_when ;
  Option.may (RamenExpr.unpure_iter f) top_by

let for_each_unpure_fun_my_lifespan lifespan selected_fields
                                    ?where ?commit_when ?top_by f =
  let open RamenExpr in
  for_each_unpure_fun selected_fields
                      ?where ?commit_when ?top_by
    (function
    | StatefulFun (_, l, _) as e when l = lifespan ->
      f e
    | _ -> ())

let otype_of_state e =
  let open RamenExpr in
  let typ = typ_of e in
  let t = Option.get typ.scalar_typ |>
          otype_of_type in
  let t =
    match e with
    | StatefulFun (_, _, AggrPercentile _) -> t ^" list"
    (* previous tuples and count ; Note: we could get rid of this count if we
     * provided some context to those functions, such as the event count in
     * current window, for instance (ie. pass the full aggr record not just
     * the fields) *)
    | StatefulFun (_, _, (Lag _ | MovingAvg _ | LinReg _)) ->
      t ^" CodeGenLib.Seasonal.t"
    | StatefulFun (_, _, MultiLinReg _) ->
      "("^ t ^" * float array) CodeGenLib.Seasonal.t"
    | StatefulFun (_, _, Remember _) ->
      "CodeGenLib.remember_state"
    | StatefulFun (_, _, Distinct es) ->
      let print_expr_typ oc e =
        let typ = typ_of e in
        Option.get typ.scalar_typ |>
        otype_of_type |>
        String.print oc in
      Printf.sprintf2 "%a CodeGenLib.distinct_state"
        (list_print_as_product print_expr_typ) es
    | StatefulFun (_, _, AggrAvg _) -> "(int * float)"
    | _ -> t in
  if Option.get typ.nullable then t ^" option" else t

let emit_state_init name state_lifespan other_state_vars
      ?where ?commit_when ?top_by
      oc selected_fields =
  (* We must collect all unpure functions present in the selected_fields
   * and return a record with the proper types and init values for the required
   * states. *)
  let for_each_my_unpure_fun f =
    for_each_unpure_fun_my_lifespan
      state_lifespan selected_fields ?where ?commit_when ?top_by f
  in
  (* In the special case where we do not have any state at all, though, we
   * end up with an empty record, which is illegal in OCaml so we need to
   * specialize for this: *)
  let need_state =
    try
      for_each_my_unpure_fun (fun _ -> raise Exit);
      false
    with Exit -> true in
  if not need_state then (
    Printf.fprintf oc "type %s = unit\n" name ;
    Printf.fprintf oc "let %s%a = ()\n\n"
      name
      (List.print ~first:" " ~last:"" ~sep:" " String.print)
        other_state_vars
  ) else (
    (* First emit the record type definition: *)
    Printf.fprintf oc "type %s = {\n" name ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf oc "\tmutable %s : %s (* %a *) ;\n"
          (name_of_state f)
          (otype_of_state f)
          RamenExpr.print_typ (RamenExpr.typ_of f)
      ) ;
    Printf.fprintf oc "}\n\n" ;
    (* Then the initialization function proper: *)
    Printf.fprintf oc "let %s%a =\n"
      name
      (List.print ~first:" " ~last:"" ~sep:" " String.print)
        other_state_vars ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf oc "\tlet %s = %a in\n"
          (name_of_state f)
          (emit_expr ~context:InitState ~state:state_lifespan) f) ;
    (* And now build the state record from all those fields: *)
    Printf.fprintf oc "\t{" ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf oc " %s ;" (name_of_state f)) ;
    Printf.fprintf oc " }\n"
  )

(* Note: we need group_ in addition to out_tuple because the commit-when clause
 * might have its own stateful functions going on *)
let emit_when name in_typ mentioned and_all_others out_typ
              oc when_expr =
  Printf.fprintf oc "let %s virtual_in_count_ %a %a \
                       virtual_selected_count_ virtual_selected_successive_ %a \
                       virtual_unselected_count_ virtual_unselected_successive_ %a \
                       virtual_out_count out_previous_opt_ group_previous_opt_ \
                       virtual_group_count_ virtual_group_successive_ group_ global_ \
                       %a %a \
                       %a =\n"
    name
    (emit_in_tuple mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastIn mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastSelected mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleLastUnselected mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleGroupFirst mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleGroupLast mentioned and_all_others) in_typ
    (emit_tuple TupleOut) out_typ ;
  (* Update the states used by this expression: *)
  emit_state_update_for_expr oc when_expr ;
  Printf.fprintf oc "\t%a\n"
    (emit_expr ?state:None ~context:Finalize) when_expr

let emit_should_resubmit name in_typ mentioned and_all_others
                         oc flush_how =
  let open RamenOperation in
  Printf.fprintf oc "let %s group_ %a =\n"
    name
    (emit_in_tuple mentioned and_all_others) in_typ ;
  match flush_how with
  | Reset | Never ->
    Printf.fprintf oc "\tfalse\n"
  | Slide n ->
    Printf.fprintf oc "\tgroup_.CodeGenLib.nb_entries > %d\n" n
  | KeepOnly e ->
    Printf.fprintf oc "\t%a\n" (emit_expr ?state:None ~context:Finalize) e
  | RemoveAll e ->
    Printf.fprintf oc "\tnot (%a)\n" (emit_expr ?state:None ~context:Finalize) e

(* Depending on what uses a commit/flush condition, we might need to check
 * all groups after every single input tuple (very slow), or after every
 * selected input tuple (still quite slow), or only when this group is
 * modified (fast). Users should limit all/selected tuple to aggregations
 * with few groups only. *)
let when_to_check_group_for_expr expr =
  (* Tells whether the commit condition needs the all or the selected tuple *)
  let open RamenExpr in
  (* FIXME: for TOP, since we flush all when a single group matches, we
   * should relax this a bit: if we need_all (because, say, we use in.#count
   * in the condition) but the condition uses no field specific to group
   * then it's safe to check only ForInGroup. *)
  let need_all, need_selected =
    fold_by_depth (fun (need_all, need_selected) -> function
        | Field (_, tuple, _) ->
          (need_all || !tuple = TupleIn || !tuple = TupleLastIn),
          (need_selected || !tuple = TupleLastSelected || !tuple = TupleSelected
                         || !tuple = TupleLastUnselected || !tuple = TupleUnselected)
        | _ ->
          need_all, need_selected
      ) (false, false) expr
  in
  if need_all then "CodeGenLib.ForAll" else
  if need_selected then "CodeGenLib.ForAllSelected" else
  "CodeGenLib.ForInGroup"

let emit_sort_expr name in_typ mentioned and_all_others oc es_opt =
  Printf.fprintf oc "let %s sort_count_ %a %a %a %a =\n"
    name
    (emit_in_tuple ~tuple:TupleSortFirst mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleIn mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleSortSmallest mentioned and_all_others) in_typ
    (emit_in_tuple ~tuple:TupleSortGreatest mentioned and_all_others) in_typ ;
  match es_opt with
  | [] ->
      (* The default sort_until clause must be false.
       * If there is no sort_by clause, any constant will do: *)
      Printf.fprintf oc "\tfalse\n"
  | es ->
      Printf.fprintf oc "\t%a\n"
        (List.print ~first:"(" ~last:")" ~sep:", "
           (emit_expr ?state:None ~context:Finalize)) es

let emit_merge_on name in_typ mentioned and_all_others oc es =
  Printf.fprintf oc "let %s %a =\n\t%a\n"
    name
    (emit_in_tuple mentioned and_all_others) in_typ
    (List.print ~first:"(" ~last:")" ~sep:", "
       (emit_expr ?state:None ~context:Finalize)) es

let expr_needs_group e =
  let open RamenExpr in
  fold_by_depth (fun need expr ->
    need || match expr with
      | Field (_, tuple, _) -> tuple_need_state !tuple
      | StatefulFun (_, LocalState, _) -> true
      | _ -> false
  ) false e

let emit_aggregate oc name in_typ out_typ = function
  | RamenOperation.Aggregate
      { fields ; and_all_others ; merge ; sort ; where ; key ; top ;
        commit_before ; commit_when ; flush_how ; notifications ; event_time ;
        every ; _ } as op ->
  let mentioned =
    let all_exprs = RamenOperation.fold_expr [] (fun l s -> s :: l) op in
    add_all_mentioned_in_expr all_exprs
  and top_by = Option.map snd top
  (* Tells whether we need the group to check the where clause (because it
   * uses the group tuple or build a group-wise aggregation on its own,
   * despite this is forbidden in RamenOperation.check): *)
  and where_need_group = expr_needs_group where
  (* Good to know when performing a TOP: *)
  and commit_when_needs_group = expr_needs_group commit_when
  and when_to_check_for_commit = when_to_check_group_for_expr commit_when in
  Printf.fprintf oc "open Batteries\nopen Stdint\n\n\
    %a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n"
    (emit_state_init "global_init_" RamenExpr.GlobalState [] ~where ~commit_when ?top_by:None) fields
    (emit_state_init "group_init_" RamenExpr.LocalState ["global_"] ~where ~commit_when ?top_by:None) fields
    (emit_read_tuple "read_tuple_" mentioned and_all_others) in_typ
    (if where_need_group then
      emit_where "where_fast_" ~always_true:true in_typ mentioned and_all_others
    else
      emit_where "where_fast_" in_typ mentioned and_all_others) where
    (if not where_need_group then
      emit_where "where_slow_" ~with_group:true ~always_true:true in_typ mentioned and_all_others
    else
      emit_where "where_slow_" ~with_group:true in_typ mentioned and_all_others) where
    (emit_key_of_input "key_of_input_" in_typ mentioned and_all_others) key
    emit_maybe_fields out_typ
    (emit_when "commit_when_" in_typ mentioned and_all_others out_typ) commit_when
    (emit_field_selection ~with_selected:true ~with_group:true "tuple_of_group_" in_typ mentioned and_all_others out_typ) fields
    (emit_sersize_of_tuple "sersize_of_tuple_") out_typ
    (emit_time_of_tuple "time_of_tuple_" event_time) out_typ
    (emit_serialize_tuple "serialize_group_") out_typ
    (emit_generate_tuples "generate_tuples_" in_typ mentioned and_all_others out_typ) fields
    (emit_should_resubmit "should_resubmit_" in_typ mentioned and_all_others) flush_how
    (emit_field_of_tuple "field_of_tuple_") out_typ
    (emit_state_init "top_init_" RamenExpr.LocalState ["global_"] ?where:None ?commit_when:None ?top_by) []
    (emit_top "top_" in_typ mentioned and_all_others out_typ) top
    (emit_float_of_top "float_of_top_state_" in_typ mentioned and_all_others out_typ) top_by
    (emit_merge_on "merge_on_" in_typ mentioned and_all_others) (fst merge)
    (emit_sort_expr "sort_until_" in_typ mentioned and_all_others) (match sort with Some (_, Some u, _) -> [u] | _ -> [])
    (emit_sort_expr "sort_by_" in_typ mentioned and_all_others) (match sort with Some (_, _, b) -> b | None -> []) ;
  Printf.fprintf oc "let %s () =\n\
      \tCodeGenLib.aggregate\n\
      \t\tread_tuple_ sersize_of_tuple_ time_of_tuple_ serialize_group_\n\
      \t\tgenerate_tuples_\n\
      \t\ttuple_of_group_ merge_on_ %F %d sort_until_ sort_by_\n\
      \t\twhere_fast_ where_slow_ key_of_input_ %b\n\
      \t\ttop_ top_init_ float_of_top_state_\n\
      \t\tcommit_when_ %b %b %b %s should_resubmit_\n\
      \t\tglobal_init_ group_init_ field_of_tuple_ %a %f\n"
    name
    (snd merge)
    (match sort with None -> 0 | Some (n, _, _) -> n)
    (key = [])
    commit_when_needs_group
    commit_before
    (flush_how <> Never)
    when_to_check_for_commit
    (List.print (fun oc n ->
      let s = PPP.to_string RamenOperation.notification_ppp_ocaml n in
      Printf.fprintf oc "%S" s)) notifications
    every
  | _ -> assert false

let sanitize_ocaml_fname s =
  let open Str in
  let replace_by_underscore _ = "_"
  and re = regexp "[^A-Za-z0-9_]" in
  (* Must start with a letter: *)
  "m"^ global_substitute re replace_by_underscore s

let compile conf entry_point func_name obj_name in_typ out_typ params op =
  let open RamenOperation in
  let src_file =
    RamenOCamlCompiler.with_code_file_for obj_name conf (fun oc ->
      Printf.fprintf oc "(* Code generated for operation %S:\n%a\n*)\n"
        func_name
        RamenOperation.print op ;
      (* Emit parameters: *)
      Printf.fprintf oc "\n(* Parameters: *)\n" ;
      List.iter (fun (n, v) ->
        Printf.fprintf oc
          "let %s_%s_ =\n\
           \tlet parser_ = RamenTypeConverters.%s_of_string in\n\
           \tCodeGenLib.parameter_value ~def:(%a) parser_ %S\n"
          (id_of_prefix TupleParam) n
          (id_of_typ (RamenScalar.type_of v))
          emit_scalar v
          n
      ) params ;
      (match op with
      | ReadCSVFile { where = { fname ; unlink } ; preprocessor ;
                      what = { separator ; null ; fields } ; event_time } ->
        emit_read_csv_file oc entry_point fname unlink separator null
                           fields preprocessor event_time
      | ListenFor { net_addr ; port ; proto } ->
        emit_listen_on oc entry_point net_addr port proto
      | Instrumentation { from } ->
        emit_instrumentation oc entry_point from
      | Aggregate _ ->
        emit_aggregate oc entry_point in_typ out_typ op)) in
  (* TODO: any failure in compilation -> delete the source code! Or it will be reused *)
  RamenOCamlCompiler.compile conf func_name src_file obj_name
