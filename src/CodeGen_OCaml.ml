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
open RamenTypes
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
let array_print_as_tuple_i p =
  let i = ref 0 in
  Array.print ~first:"(" ~last:")" ~sep:", " (fun oc x ->
    p oc !i x ; incr i)

let list_print_as_vector p = List.print ~first:"[|" ~last:"|]" ~sep:"; " p
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

let rec emit_sersize_of_not_null tx_var offs_var oc = function
  | TString ->
    Printf.fprintf oc "\
      %d + RingBuf.round_up_to_rb_word(RingBuf.read_word %s %s)"
      RingBuf.rb_word_bytes tx_var offs_var
  | TIp ->
    Printf.fprintf oc "RingBuf.round_up_to_rb_word(1 + \
                       match RingBuf.read_word %s %s with \
                       | 0 -> %a | 1 -> %a | _ -> assert false)"
      tx_var offs_var
      emit_sersize_of_fixsz_typ TIpv4
      emit_sersize_of_fixsz_typ TIpv6
  | TCidr ->
    Printf.fprintf oc "RingBuf.round_up_to_rb_word(1 + \
                       match RingBuf.read_u8 %s %s |> Uint8.to_int with \
                       | 4 -> %a | 6 -> %a | _ -> assert false)"
      tx_var offs_var
      emit_sersize_of_fixsz_typ TCidrv4
      emit_sersize_of_fixsz_typ TCidrv6

  | TTuple ts -> (* Should not be used! Will be used when a constructed field is not mentioned and is skipped. Get rid of mentioned fields!*)
    Printf.fprintf oc "\
      let %s_ = %s in\n"
      offs_var offs_var ;
    Array.iteri (fun i t ->
      Printf.fprintf oc "\
      let %s_ = %s_ + (%a) in\n"
        offs_var offs_var
        (emit_sersize_of_not_null tx_var offs_var) t
    ) ts ;
    Printf.fprintf oc "\
      %s - %s_"
      offs_var offs_var
  | TVec (dim, t) ->
    todo "vector sersize"
  | t -> emit_sersize_of_fixsz_typ oc t

(* Emit the code to retrieve the sersize of some serialized value *)
let emit_sersize_of_field_tx tx_var offs_var nulli oc field =
  if field.nullable then (
    Printf.fprintf oc "if RingBuf.get_bit %s %d then %a else 0"
      tx_var nulli
      (emit_sersize_of_not_null tx_var offs_var) field.typ
  ) else
    emit_sersize_of_not_null tx_var offs_var oc field.typ

let id_of_typ = function
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
  | TEth    -> "eth"
  | TIpv4   -> "ip4"
  | TIpv6   -> "ip6"
  | TIp     -> "ip"
  | TCidrv4 -> "cidr4"
  | TCidrv6 -> "cidr6"
  | TCidr   -> "cidr"
  | TTuple _ -> "tuple"
  | TVec _  -> "vector"
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

let rec emit_type oc =
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
  | VIp (RamenIp.V4 n) -> emit_type oc (VIpv4 n)
  | VIp (RamenIp.V6 n) -> emit_type oc (VIpv6 n)
  | VCidrv4 (n,l) ->
                 Printf.fprintf oc "(Uint32.of_string %S, %d)" (Uint32.to_string n) l
  | VCidrv6 (n,l) ->
                 Printf.fprintf oc "(Uint128.of_string %S, %d)" (Uint128.to_string n) l
  | VCidr (RamenIp.Cidr.V4 n) -> emit_type oc (VCidrv4 n)
  | VCidr (RamenIp.Cidr.V6 n) -> emit_type oc (VCidrv6 n)
  | VTuple vs -> Array.print ~first:"(" ~last:")" ~sep:", " emit_type oc vs
  | VVec vs   -> Array.print emit_type oc vs
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

let rec otype_of_type oc = function
  | TFloat -> String.print oc "float"
  | TString -> String.print oc "string"
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
      Array.print ~first:"(" ~last:")" ~sep:" * " otype_of_type oc ts
  | TVec (_d, t) -> Printf.fprintf oc "%a array" otype_of_type t
  | TNum | TAny -> assert false

let omod_of_type = function
  | TFloat -> "Float"
  | TString -> "String"
  | TBool -> "Bool"
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128 as t ->
    String.capitalize (IO.to_string otype_of_type t)
  | TEth -> "RamenEthAddr"
  | TIpv4 -> "RamenIpv4"
  | TIpv6 -> "RamenIpv6"
  | TIp -> "RamenIp"
  | TCidrv4 -> "RamenIpv4.Cidr"
  | TCidrv6 -> "RamenIpv6.Cidr"
  | TCidr -> "RamenIp.Cidr"
  | TTuple _ | TVec _ | TNum | TAny -> assert false

(* Why don't we have explicit casts in the AST so that we could stop
 * caring about those pesky conversions once and for all? Because the
 * AST changes to types that we want to work, but do not (have to) know
 * about what conversions are required to implement that in OCaml. *)
(* Note: for field_of_tuple, we must be able to convert any value into a
 * string *)
(* This only returns the function name (or code) but does not emit the
 * call to that function. *)
let rec conv_from_to ~nullable oc (from_typ, to_typ) =
  (* Emitted code must be prefixable by "Option.map": *)
  let print_non_null oc (from_typ, to_typ as conv) =
    match conv with
    | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128|TString|TFloat),
        (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128)
    | TString, (TFloat|TBool) ->
      Printf.fprintf oc "%s.of_%a"
        (omod_of_type to_typ)
        otype_of_type from_typ
    | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128),
        (TFloat|TString)
    | (TFloat|TBool), (TString|TFloat) ->
      Printf.fprintf oc "%s.to_%a"
        (omod_of_type from_typ)
        otype_of_type to_typ
    | TBool, (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) ->
      Printf.fprintf oc "(%s.of_int %% Bool.to_int)"
        (omod_of_type to_typ)
    | (TEth|TIpv4|TIpv6|TIp|TCidrv4|TCidrv6|TCidr), TString ->
      Printf.fprintf oc "%s.to_string" (omod_of_type from_typ)
    | TIpv4, TIp -> Printf.fprintf oc "(fun x_ -> RamenIp.V4 x_)"
    | TIpv6, TIp -> Printf.fprintf oc "(fun x_ -> RamenIp.V6 x_)"
    | TCidrv4, TCidr -> Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V4 x_)"
    | TCidrv6, TCidr -> Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V6 x_)"
    | TVec (d_from, t_from), TVec (d_to, t_to)
      when d_from = d_to || d_to = 0 ->
      (* d_to = 0 means no constraint (copy the one from the left-hand side) *)
      (* Note: vector items cannot be NULL: *)
      Printf.fprintf oc "(fun v_ -> Array.map (%a) v_)"
        (conv_from_to ~nullable:false) (t_from, t_to)

    | TVec (d, t), TString ->
      Printf.fprintf oc
        "(fun v_ -> \
           \"[\"^ (\
            Array.enum v_ /@ (%a) |> \
            Enum.reduce (fun s1_ s2_ -> s1_^\",\"^s2_)
           ) ^\"]\")"
        (conv_from_to ~nullable:false) (t, TString)

    | TTuple ts, TString ->
      let i = ref 0 in
      Printf.fprintf oc
        "(fun %a -> %a)"
          (array_print_as_tuple_i (fun oc i _ -> Printf.fprintf oc "x%d_" i)) ts
          (Array.print ~first:"" ~last:"" ~sep:"^" (fun oc t ->
            Printf.fprintf oc "(%a) x%d_"
              (conv_from_to ~nullable:false) (t, TString) !i ;
            incr i)) ts

    | _ ->
      failwith (Printf.sprintf "Cannot find converter from type %s to type %s"
                  (IO.to_string RamenTypes.print_typ from_typ)
                  (IO.to_string RamenTypes.print_typ to_typ))
  in
  if from_typ = to_typ then Printf.fprintf oc "identity"
  else
    Printf.fprintf oc "(%s%a)"
      (if nullable then "Option.map " else "")
      print_non_null (from_typ, to_typ)

let freevar_name t = "fv_"^ string_of_int t.RamenExpr.uniq_num ^"_"

let rec any_constant_of_expr_type t =
  let open RamenExpr in
  let open Stdint in
  let c v =
    Const (make_typ ?typ:t.scalar_typ ?nullable:t.nullable "init", v)
  in
  c (any_value_of_type (Option.get t.scalar_typ))

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
let rec conv_to ?state ~context ~consts to_typ fmt e =
  let open RamenExpr in
  let t = typ_of e in
  if t.nullable = None then (
    !logger.error "Problem: Have to convert expression %a into %a"
      (print true) e
      RamenTypes.print_typ (Option.get to_typ)
  ) ;
  let nullable = Option.get t.nullable in
  match t.scalar_typ, to_typ with
  | Some a, Some b ->
    Printf.fprintf fmt "(%a) (%a)"
      (conv_from_to ~nullable) (a, b)
      (emit_expr ~context ~consts ?state) e
  | _, None ->
    (emit_expr ~context ~consts ?state) fmt e (* No conversion required *)
  | None, Some b ->
    failwith (Printf.sprintf "Cannot convert from unknown type into %s"
                (IO.to_string RamenTypes.print_typ b))

(* The vectors Tuple{Group,Out}Previous are optional: the commit when and
 * select clauses of aggregate operations either have it or not.
 * Each time they need access to a field they call a function "maybe_XXX_"
 * with that optional tuple, which avoids propagating out_typ down to
 * emit_expr - but hopefully the compiler will inline this.
 * (TODO: have a context in a single place and inline it directly?) *)
and emit_maybe_fields oc out_typ =
  List.iter (fun ft ->
    Printf.fprintf oc "let maybe_%s_ = function\n" ft.typ_name ;
    Printf.fprintf oc "  | None as n_ -> n_\n" ;
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
and emit_expr ?state ~context ~consts oc expr =
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
  | _, Const (_, VNull), _ ->
    assert (is_nullable expr) ;
    Printf.fprintf oc "None"
  | _, Const (t, c), _ ->
    Printf.fprintf oc "%s(%a %a)"
      (if is_nullable expr then "Some " else "")
      (conv_from_to ~nullable:false) (RamenTypes.type_of c,
                                      Option.get t.scalar_typ)
      emit_type c
  | Finalize, Tuple (_, es), _ ->
    list_print_as_tuple (emit_expr ?state ~context ~consts) oc es
  | Finalize, Vector (_, es), _ ->
    list_print_as_vector (emit_expr ?state ~context ~consts) oc es

  | Finalize, Field (_, tuple, field), _ ->
    (match !tuple with
    | TupleGroupPrevious ->
      Printf.fprintf oc "(maybe_%s_ group_previous_opt_)" field
    | TupleOutPrevious ->
      Printf.fprintf oc "(maybe_%s_ out_previous_opt_)" field
    | TupleEnv ->
      Printf.fprintf oc "(Sys.getenv_opt %S)" field
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
              "match %a with None as n_ -> n_ | Some cond_ -> if cond_ then %s(%a)"
            else
              "if %a then %s(%a)")
           (emit_expr ?state ~context ~consts) alt.case_cond
           (if is_nullable expr && not (is_nullable alt.case_cons) then "Some " else "")
           (conv_to ?state ~context ~consts t) alt.case_cons)
      oc alts ;
    (match else_ with
    | None ->
      (* If there is no ELSE clause then the expr is nullable: *)
      assert (is_nullable expr) ;
      Printf.fprintf oc " else None)"
    | Some else_ ->
      Printf.fprintf oc " else %s(%a))"
        (if is_nullable expr && not (is_nullable else_) then "Some " else "")
        (conv_to ?state ~context ~consts t) else_)
  | Finalize, Coalesce (_, es), t ->
    let rec loop = function
      | [] -> ()
      | [last] ->
        Printf.fprintf oc "(%a)" (conv_to ?state ~context ~consts t) last
      | e :: rest ->
        Printf.fprintf oc "(Option.default_delayed (fun () -> " ;
        loop rest ;
        Printf.fprintf oc ") (%a))" (conv_to ?state ~context ~consts t) e
    in
    loop es
  (* Stateless arithmetic functions which actual funcname depends on operand types: *)
  | Finalize, StatelessFun2 (_, Add, e1, e2),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".add") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Sub, e1, e2),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".sub") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Mul, e1, e2),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".mul") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, IDiv, e1, e2),
    Some (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".div") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, IDiv, e1, e2), Some (TFloat as t) ->
    (* Here we must convert everything to float first, then divide and
     * take the floor: *)
    Printf.fprintf oc "(let x_ = " ;
    emit_functionN ?state ~consts (omod_of_type t ^".div") [Some t; Some t] oc [e1; e2] ;
    Printf.fprintf oc " in if x_ >= 0. then floor x_ else ceil x_)"
  | Finalize, StatelessFun2 (_, Div, e1, e2), Some (TFloat as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".div") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Pow, e1, e2),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".( ** )") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Mod, e1, e2),
    Some (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".rem") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Mod, e1, e2), Some (TFloat as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".modulo") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Strftime, e1, e2), Some TString ->
    emit_functionN ?state ~consts "CodeGenLib.strftime"
      [Some TString; Some TFloat] oc [e1; e2]
  | Finalize, StatelessFun1 (_, Strptime, e), Some TFloat ->
    emit_functionN ?state ~consts ~impl_return_nullable:true
      "RamenHelpers.time_of_abstime" [Some TString] oc [e]

  | Finalize, StatelessFun1 (_, Abs, e),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".abs") [Some t] oc [e]
  | Finalize, StatelessFun1 (_, Minus, e),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".neg") [Some t] oc [e]
  | Finalize, StatelessFun1 (_, Exp, e), Some TFloat ->
    emit_functionN ?state ~consts "exp" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Log, e), Some TFloat ->
    emit_functionN ?state ~consts "log" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Log10, e), Some TFloat ->
    emit_functionN ?state ~consts "log10" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Sqrt, e), Some TFloat ->
    emit_functionN ?state ~consts "sqrt" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Ceil, e), Some TFloat ->
    emit_functionN ?state ~consts "ceil" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Floor, e), Some TFloat ->
    emit_functionN ?state ~consts "floor" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Hash, e), Some TI64 ->
    emit_functionN ?state ~consts "CodeGenLib.hash" [None] oc [e]
  | Finalize, StatelessFun1 (_, Sparkline, e), Some TString ->
    emit_functionN ?state ~consts "RamenHelpers.sparkline" [Some (TVec (0, TFloat))] oc [e]
  | Finalize, StatelessFun1 (_, BeginOfRange, e), Some TIpv4 ->
    emit_functionN ?state ~consts "RamenIpv4.Cidr.first" [Some TCidrv4] oc [e]
  | Finalize, StatelessFun1 (_, BeginOfRange, e), Some TIpv6 ->
    emit_functionN ?state ~consts "RamenIpv6.Cidr.first" [Some TCidrv6] oc [e]
  | Finalize, StatelessFun1 (_, BeginOfRange, e), Some TIp ->
    emit_functionN ?state ~consts "RamenIp.first" [Some TCidr] oc [e]
  | Finalize, StatelessFun1 (_, EndOfRange, e), Some TIpv4 ->
    emit_functionN ?state ~consts "RamenIpv4.Cidr.last" [Some TCidrv4] oc [e]
  | Finalize, StatelessFun1 (_, EndOfRange, e), Some TIpv6 ->
    emit_functionN ?state ~consts "RamenIpv6.Cidr.last" [Some TCidrv6] oc [e]
  | Finalize, StatelessFun1 (_, EndOfRange, e), Some TIp ->
    emit_functionN ?state ~consts "RamenIp.last" [Some TCidr] oc [e]

  (* Stateless functions manipulating constructed types: *)
  | Finalize, StatelessFun1 (_, Nth n, es), _ ->
    (match (typ_of es).scalar_typ with
    | Some (TTuple ts) ->
        let nb_items = Array.length ts in
        let rec loop_t str i =
          if i >= nb_items then str else
          let str = str ^ (if i > 0 then "," else "")
                        ^ (if i = n then "x_" else "_") in
          loop_t str (i + 1) in
        let nth_func = loop_t "(fun (" 0 ^") -> x_)" in
        (* emit_funcN will take care of nullability of es: *)
        emit_functionN ?state ~consts nth_func [None] oc [es]
    | Some (TVec (dim, t)) ->
        assert (n < dim) ;
        let nth_func = "(fun a_ -> Array.get a_ "^ string_of_int n ^")" in
        emit_functionN ?state ~consts nth_func [None] oc [es]
    | _ -> assert false)
  | Finalize, StatelessFun2 (_, VecGet, n, es), _ ->
    let func = "(fun a_ n_ -> Array.get a_ (Int32.to_int n_))" in
    emit_functionN ?state ~consts func [None; Some TI32] oc [es; n]

  (* Other stateless functions *)
  | Finalize, StatelessFun2 (_, Ge, e1, e2), Some TBool ->
    emit_functionN ?state ~consts "(>=)" [Some TAny; Some TAny] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Gt, e1, e2), Some TBool ->
    emit_functionN ?state ~consts "(>)" [Some TAny; Some TAny] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Eq, e1, e2), Some TBool ->
    emit_functionN ?state ~consts "(=)" [Some TAny; Some TAny] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Concat, e1, e2), Some TString ->
    emit_functionN ?state ~consts "(^)" [Some TString; Some TString] oc [e1; e2]
  | Finalize, StatelessFun2 (_, StartsWith, e1, e2), Some TBool ->
    emit_functionN ?state ~consts "String.starts_with" [Some TString; Some TString] oc [e1; e2]
  | Finalize, StatelessFun2 (_, EndsWith, e1, e2), Some TBool ->
    emit_functionN ?state ~consts "String.ends_with" [Some TString; Some TString] oc [e1; e2]
  | Finalize, StatelessFunMisc (_, Like (e, p)), Some TBool ->
    let pattern = Globs.compile ~star:'%' ~escape:'\\' p in
    Printf.fprintf oc "(let pattern_ = \
      Globs.{ anchored_start = %b ; anchored_end = %b ; chunks = %a } in "
      pattern.anchored_start pattern.anchored_end
      (List.print (fun oc s -> Printf.fprintf oc "%S" s)) pattern.chunks ;
    emit_functionN ?state ~consts "Globs.matches pattern_ " [Some TString] oc [e];
    Printf.fprintf oc ")"
  | Finalize, StatelessFun1 (_, Length, e), Some TU16 (* The only possible output type *) ->
    emit_functionN ?state ~consts "String.length" [Some TString] oc [e]
  (* lowercase and uppercase assume latin1 and will gladly destroy UTF-8
   * encoded char, therefore we use the ascii variants: *)
  | Finalize, StatelessFun1 (_, Lower, e), Some TString ->
    emit_functionN ?state ~consts "String.lowercase_ascii" [Some TString] oc [e]
  | Finalize, StatelessFun1 (_, Upper, e), Some TString ->
    emit_functionN ?state ~consts "String.uppercase_ascii" [Some TString] oc [e]
  | Finalize, StatelessFun2 (_, And, e1, e2), Some TBool ->
    emit_functionN ?state ~consts "(&&)" [Some TBool; Some TBool] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Or, e1,e2), Some TBool ->
    emit_functionN ?state ~consts "(||)" [Some TBool; Some TBool] oc [e1; e2]
  | Finalize, StatelessFun2 (_, (BitAnd|BitOr|BitXor as op), e1, e2),
    Some (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    let n = match op with BitAnd -> "logand" | BitOr -> "logor"
                        | _ -> "logxor" in
    emit_functionN ?state ~consts (omod_of_type t ^"."^ n) [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun1 (_, Not, e), Some TBool ->
    emit_functionN ?state ~consts "not" [Some TBool] oc [e]
  | Finalize, StatelessFun1 (_, Defined, e), Some TBool ->
    (* Do not call emit_functionN to avoid null propagation: *)
    Printf.fprintf oc "(match %a with None -> false | _ -> true)"
      (emit_expr ?state ~context ~consts) e
  | Finalize, StatelessFun1 (_, Age, e),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as to_typ)
  | Finalize, StatelessFun1 (_, BeginOfRange, e),
    Some (TCidrv4 | TCidrv6 as to_typ) ->
    let in_type_name =
      String.lowercase (IO.to_string RamenTypes.print_typ to_typ) in
    let name = "CodeGenLib.age_"^ in_type_name in
    emit_functionN ?state ~consts name [Some to_typ] oc [e]
  (* TODO: Now() for Uint62? *)
  | Finalize, StatelessFun0 (_, Now), Some TFloat ->
    String.print oc "!CodeGenLib_IO.now"
  | Finalize, StatelessFun0 (_, Random), Some TFloat ->
    String.print oc "(Random.float 1.)"
  | Finalize, StatelessFun1 (_, Cast, e), Some to_typ ->
    let from = typ_of e in
    let from_typ = Option.get from.scalar_typ
    and nullable = Option.get from.nullable in
    Printf.fprintf oc "(%a) (%a)"
      (conv_from_to ~nullable) (from_typ, to_typ)
      (emit_expr ?state ~context ~consts) e

  | Finalize, StatelessFunMisc (_, Max es), t ->
    emit_functionN ~consts ~args_as:(Array 0) ?state "Array.max" (List.map (fun _ -> t) es) oc es
  | Finalize, StatelessFunMisc (_, Min es), t ->
    emit_functionN ~consts ~args_as:(Array 0) ?state "Array.min" (List.map (fun _ -> t) es) oc es
  | Finalize, StatelessFunMisc (_, Print es), _ ->
    (* We want to print nulls as well, so we make all parameters optional strings: *)
    assert (es <> []) ;
    Printf.fprintf oc "(CodeGenLib.print %a ; %a)"
      (List.print ~first:"[" ~last:"]" ~sep:";" (fun oc e ->
         Printf.fprintf oc "%s(%a)"
           (if is_nullable e then "" else "Some ")
           (conv_to ?state ~context ~consts (Some TString)) e)) es
      (emit_expr ?state ~context ~consts) (List.hd es)
  (* IN can have many meanings: *)
  | Finalize, StatelessFun2 (_, In, e1, e2), Some TBool ->
    (match (typ_of e1).scalar_typ |> Option.get,
           (typ_of e2).scalar_typ |> Option.get with
    | TIpv4, TCidrv4 ->
      emit_functionN ?state ~consts "RamenIpv4.Cidr.is_in"
        [Some TIpv4; Some TCidrv4] oc [e1; e2]
    | TIpv6, TCidrv6 ->
      emit_functionN ?state ~consts "RamenIpv6.Cidr.is_in"
        [Some TIpv6; Some TCidrv6] oc [e1; e2]
    | (TIpv4|TIpv6|TIp), (TCidrv4|TCidrv6|TCidr) ->
      emit_functionN ?state ~consts "RamenIp.is_in"
        [Some TIp; Some TCidr] oc [e1; e2]
    | TString, TString ->
      emit_functionN ?state ~consts "String.exists"
        [Some TString; Some TString] oc [e2; e1]
    | t1, TVec (d, t) ->
      (match e2 with Vector (t', es) ->
        (* We make a constant hash with the constants. Note that when e1 is
         * also a constant the OCaml compiler could optimize the whole
         * "x=a||x=b||x=b..." operation but only if not too many conversions
         * are involved, so we take no risk and build the hash in any case. *)
        let csts, non_csts =
          (* TODO: leave the IFs when we know the compiler will optimize them
           * away:
          if is_const e1 then [], es else*)
          List.partition is_const es in
        let csts, non_csts =
          if List.length csts < 6 (* guessed *) then [], csts @ non_csts
          else csts, non_csts in
        (* Typing only enforce that t1 < t or t > t1 (so we can look for an u8
         * in a set of i32, or the other way around which both make sense).
         * Here for simplicity all values will be converted to the largest of
         * t and t1: *)
        let larger_t = RamenTypes.large_enough_for t t1 in
        (* Note re. nulls: we are going to emit code such as "A=x1||A=x2" in
         * lieu of "A IN [x1; x2]". Notice that nulls do not propagate from
         * the xs in case A is found in the set, but do if it is not. If A is
         * NULL though, then the result is unless the set is empty: *)
        if is_nullable e1 then
          Printf.fprintf oc "(match %a with None -> %s | Some in0_ -> "
            (conv_to ?state ~context ~consts (Some larger_t)) e1
            (* Even if e1 is null, we can answer the operation if e2 is
             * empty: *)
            (if es = [] then "Some true" else "None")
        else
          Printf.fprintf oc "(let in0_ = %a in "
            (conv_to ?state ~context ~consts (Some larger_t)) e1 ;
        (* Now if we had some null the return value is either Some true or
         * None, while if we had no null the return value is either Some
         * true or Some false. *)
        Printf.fprintf oc "let _ret_ = ref (Some false) in\n" ;
        (* First check the csts: *)
        (* Note that none should be nullable ATM, and even if they were all
         * nullable then we would store the option.get of the values (knowing
         * that, if any of the const is NULL then we can shotcut all this and
         * answer NULL directly) *)
        if csts <> [] then (
          let hash_id =
            "const_in_"^ string_of_int (typ_of expr).uniq_num ^"_" in
          Printf.fprintf consts
            "let %s =\n\
             \tlet h_ = Hashtbl.create %d in\n\
             %a\
             h_\n"
            hash_id (List.length csts)
            (List.print ~first:"" ~last:"" ~sep:"" (fun cc e ->
              Printf.fprintf cc "\tHashtbl.replace h_ (%a) () ;\n"
                (conv_to ?state ~context ~consts (Some larger_t)) e)) csts ;
          Printf.fprintf oc "if Hashtbl.mem %s in0_ then %strue else "
            hash_id (if is_nullable expr then "Some " else "")) ;
        (* Then check each non-const in turn: *)
        let had_nullable =
          List.fold_left (fun had_nullable e ->
            if is_nullable e (* not possible ATM *) then (
              Printf.fprintf oc
                "if (match %a with None -> _ret_ := None; false \
                 | Some in1_ -> in0_ = in1_) then true else "
                (conv_to ?state ~context ~consts (Some larger_t)) e ;
              true
            ) else (
              Printf.fprintf oc "if in0_ = %a then %strue else "
                (conv_to ?state ~context ~consts (Some larger_t)) e
                (if is_nullable expr then "Some " else "") ;
              had_nullable)
          ) false non_csts in
        Printf.fprintf oc "%s)"
          (if had_nullable then "!_ret_" else
           if is_nullable expr then "Some false" else "false") ;
      | _ -> assert false)
    | _ -> assert false)

  (* Stateful functions *)
  | InitState, StatefulFun (_, _, AggrAnd _), (Some TBool as t) ->
    conv_to ?state ~context ~consts t oc (true_or_nul (is_nullable expr))
  | InitState, StatefulFun (_, _, AggrOr _), (Some TBool as t) ->
    conv_to ?state ~context ~consts t oc (false_or_nul (is_nullable expr))
  | Finalize, StatefulFun (_, g, (AggrAnd _|AggrOr _)), Some TBool ->
    emit_functionN ?state ~consts "identity" [None] oc [my_state g]
  | UpdateState, StatefulFun (_, g, AggrAnd e), _ ->
    emit_functionN ?state ~consts "(&&)" [None; Some TBool] oc [my_state g; e]
  | UpdateState, StatefulFun (_, g, AggrOr e), _ ->
    emit_functionN ?state ~consts "(||)" [None; Some TBool] oc [my_state g; e]

  | InitState, StatefulFun (_, _, AggrSum _),
    (Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) as t) ->
    conv_to ?state ~context ~consts t oc (zero_or_nul (is_nullable expr))
  | UpdateState, StatefulFun (_, g, AggrSum e),
    Some (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~consts (omod_of_type t ^".add") [None; Some t] oc [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrSum _), _ ->
    emit_functionN ?state ~consts "identity" [None] oc [my_state g]

  | InitState, StatefulFun (_, _, AggrAvg e), Some TFloat ->
    Printf.fprintf oc "%s(0, 0.)"
      (if is_nullable e then "Some " else "")
  | UpdateState, StatefulFun (_, g, AggrAvg e), Some (TFloat as t) ->
    emit_functionN ?state ~consts "CodeGenLib.avg_add" [None; Some t] oc [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrAvg _), _ ->
    emit_functionN ?state ~consts "CodeGenLib.avg_finalize" [None] oc [my_state g]

  | InitState, StatefulFun (_, _, (AggrFirst e|AggrLast e)), t ->
    conv_to ?state ~context ~consts t oc (any_constant_of_expr_type (typ_of e))

  | InitState, StatefulFun (_, _, (AggrMax e|AggrMin e)), _ ->
    Printf.fprintf oc "%sNone"
      (if is_nullable e then "Some " else "")
  | UpdateState, StatefulFun (_, g, AggrMax e), _ ->
    emit_functionN ?state ~consts "CodeGenLib.aggr_max" [None; None] oc [e; my_state g]
  | UpdateState, StatefulFun (_, g, AggrMin e), _ ->
    emit_functionN ?state ~consts "CodeGenLib.aggr_min" [None; None] oc [e; my_state g]
  | Finalize, StatefulFun (_, g, (AggrMax _|AggrMin _)), _ ->
    emit_functionN ?state ~consts "Option.get" [None] oc [my_state g]

  | Finalize, StatefulFun (_, g, (AggrFirst _|AggrLast _)), _ ->
    emit_functionN ?state ~consts "identity" [None] oc [my_state g]
  | UpdateState, StatefulFun (_, g, AggrFirst e), _ ->
    (* This hack relies on the fact that UpdateState is always called in
     * a context where we have the group.#count available and that its
     * name is "virtual_group_count_". *)
    emit_functionN ?state ~consts "(fun x y -> if virtual_group_count_ = Uint64.one then y else x)" [None; None] oc [my_state g; e]
  | UpdateState, StatefulFun (_, g, AggrLast e), _ ->
    emit_functionN ?state ~consts "(fun _ x -> x)" [None; None] oc [my_state g; e]

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
    emit_functionN ?state ~consts "CodeGenLib.float_percentile_add" [None; None] oc [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrPercentile (p,_e)), Some TFloat ->
    emit_functionN ?state ~consts "CodeGenLib.float_percentile_finalize" [Some TFloat; None] oc [p; my_state g]
  | UpdateState, StatefulFun (_, g, AggrPercentile (_p,e)), _ ->
    emit_functionN ?state ~consts "CodeGenLib.percentile_add" [None; None] oc [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrPercentile (p,_e)), Some (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128) ->
    emit_functionN ?state ~consts "CodeGenLib.percentile_finalize" [Some TFloat; None] oc [p; my_state g]

  (* Histograms: bucket each float into the array of nb_buckets + 2 and then
   * count number of entries per buckets. The 2 extra buckets are for "<min"
   * and ">max". *)
  | InitState, StatefulFun (_, _, AggrHistogram (e, min, max, nb_buckets)), _ ->
    Printf.fprintf oc "%s(CodeGenLib.histogram_init %s %s %d)"
      (if is_nullable e then "Some " else "")
      (Legacy.Printf.sprintf "%h" min)
      (Legacy.Printf.sprintf "%h" max)
      nb_buckets
  | UpdateState, StatefulFun (_, g, AggrHistogram (e, min, max, nb_buckets)), _ ->
    emit_functionN ?state ~consts "CodeGenLib.histogram_add"
      [None; Some TFloat] oc [my_state g; e]
  | Finalize, StatefulFun (_, g, AggrHistogram _), Some TVec (_, TU32) ->
    emit_functionN ?state ~consts "CodeGenLib.histogram_finalize" [None] oc [my_state g]

  | InitState, StatefulFun (_, _, Lag (k,e)), _ ->
    emit_functionN ?state ~consts "CodeGenLib.Seasonal.init"
      [Some TU32; Some TU32; None] oc
      [k; expr_one; any_constant_of_expr_type (typ_of e)]
  | UpdateState, StatefulFun (_, g, Lag (_k,e)), _ ->
    emit_functionN ?state ~consts "CodeGenLib.Seasonal.add" [None; None] oc [my_state g; e]
  | Finalize, StatefulFun (_, g, Lag _), _ ->
    emit_functionN ?state ~consts "CodeGenLib.Seasonal.lag" [None] oc [my_state g]

  (* We force the inputs to be float since we are going to return a float anyway. *)
  | InitState, StatefulFun (_, _, (MovingAvg(p,n,_)|LinReg(p,n,_))), Some TFloat ->
    emit_functionN ?state ~consts "CodeGenLib.Seasonal.init" [Some TU32; Some TU32; Some TFloat] oc [p; n; expr_zero]
  | UpdateState, StatefulFun (_, g, (MovingAvg(_p,_n,e)|LinReg(_p,_n,e))), _ ->
    emit_functionN ?state ~consts "CodeGenLib.Seasonal.add" [None; Some TFloat] oc [my_state g; e]
  | Finalize, StatefulFun (_, g, MovingAvg (p,n,_)), Some TFloat ->
    emit_functionN ?state ~consts "CodeGenLib.Seasonal.avg" [Some TU32; Some TU32; None] oc [p; n; my_state g]
  | Finalize, StatefulFun (_, g, LinReg (p,n,_)), Some TFloat ->
    emit_functionN ?state ~consts "CodeGenLib.Seasonal.linreg" [Some TU32; Some TU32; None] oc [p; n; my_state g]
  | Finalize, StatefulFun (_, g, MultiLinReg (p,n,_,_)), Some TFloat ->
    emit_functionN ?state ~consts "CodeGenLib.Seasonal.multi_linreg" [Some TU32; Some TU32; None] oc [p; n; my_state g]

  | InitState, StatefulFun (_, _, MultiLinReg (p,n,_,es)), Some TFloat ->
    emit_functionNv ?state ~consts "CodeGenLib.Seasonal.init_multi_linreg"
      [Some TU32; Some TU32; Some TFloat]
      [p; n; expr_zero]
      (Some TFloat) oc (List.map (fun _ -> expr_zero) es)
  | UpdateState, StatefulFun (_, g, MultiLinReg (_p,_n,e,es)), _ ->
    emit_functionNv ?state ~consts "CodeGenLib.Seasonal.add_multi_linreg" [None; Some TFloat] [my_state g; e] (Some TFloat) oc es

  | InitState, StatefulFun (_, _, ExpSmooth (_a,_)), (Some TFloat as t) ->
    conv_to ?state ~context ~consts t oc (zero_or_nul (is_nullable expr))
  | UpdateState, StatefulFun (_, g, ExpSmooth (a,e)), _ ->
    emit_functionN ?state ~consts "CodeGenLib.smooth" [None; Some TFloat; Some TFloat] oc [my_state g; a; e]
  | Finalize, StatefulFun (_, g, ExpSmooth _), Some TFloat ->
    emit_functionN ?state ~consts "identity" [None] oc [my_state g]

  | InitState, StatefulFun (_, _, Remember (fpr,_tim,dur,_es)), Some TBool ->
    emit_functionN ?state ~consts "CodeGenLib.remember_init" [Some TFloat; Some TFloat] oc [fpr; dur]
  | UpdateState, StatefulFun (_, g, Remember (_fpr,tim,_dur,es)), _ ->
    emit_functionN ?state ~consts ~args_as:(Tuple 2) "CodeGenLib.remember_add"
      (None :: Some TFloat :: List.map (fun _ -> None) es)
      oc (my_state g :: tim :: es)
  | Finalize, StatefulFun (_, g, Remember _), Some TBool ->
    emit_functionN ?state ~consts "CodeGenLib.remember_finalize" [None] oc [my_state g]

  | InitState, StatefulFun (_, _, Distinct _es), _ ->
    Printf.fprintf oc "%s(CodeGenLib.distinct_init ())"
      (if is_nullable expr then "Some " else "")
  | UpdateState, StatefulFun (_, g, Distinct es), _ ->
    emit_functionN ?state ~consts ~args_as:(Tuple 1) "CodeGenLib.distinct_add" (None :: List.map (fun e -> None) es) oc (my_state g :: es)
  | Finalize, StatefulFun (_, g, Distinct es), Some TBool ->
    emit_functionN ?state ~consts "CodeGenLib.distinct_finalize" [None] oc [my_state g]

  | InitState, StatefulFun (_, _, Hysteresis _), t ->
    conv_to ?state ~context ~consts t oc (true_or_nul (is_nullable expr)) (* initially within bounds *)
  | UpdateState, StatefulFun (_, g, Hysteresis (meas, accept, max)), Some TBool ->
    let t = (typ_of meas).scalar_typ in (* TODO: shouldn't we promote everything to the most accurate of those types? *)
    emit_functionN ?state ~consts "CodeGenLib.hysteresis_update" [None; t; t; t] oc [my_state g; meas; accept; max]
  | Finalize, StatefulFun (_, g, Hysteresis _), Some TBool ->
    emit_functionN ?state ~consts "CodeGenLib.hysteresis_finalize" [None] oc [my_state g]

  | InitState, StatefulFun (_, _, Top { n ; duration ; _ }), _ ->
    Printf.fprintf oc "%s(CodeGenLib.heavy_hitters_init %d %s)"
      (if is_nullable expr then "Some " else "")
      n
      (Legacy.Printf.sprintf "%h" duration)
  | UpdateState, StatefulFun (_, g, Top { what ; by ; time ; _ }), _ ->
    emit_functionN ?state ~consts ~args_as:(Tuple 3)
      "CodeGenLib.heavy_hitters_add"
      (None :: Some TFloat :: Some TFloat :: List.map (fun _ -> None) what)
      oc (my_state g :: time :: by :: what)
  | Finalize, StatefulFun (_, g, Top { want_rank = true ; n ; what ; _ }), Some t ->
    (* heavy_hitters_rank returns an optional int; we then have to convert
     * it to whatever integer size we are supposed to have: *)
    assert (is_nullable expr) ;
    Printf.fprintf oc "(Option.map %s.of_int %a)"
      (omod_of_type t)
      (emit_functionN ~impl_return_nullable:true ?state ~consts ~args_as:(Tuple 1)
         ("CodeGenLib.heavy_hitters_rank ~n:"^ string_of_int n)
         (None :: List.map (fun _ -> None) what)) (my_state g :: what)
  | Finalize, StatefulFun (_, g, Top { want_rank = false ; n ; what ; _ }), _ ->
    emit_functionN ?state ~consts ~args_as:(Tuple 1)
      ("CodeGenLib.heavy_hitters_is_in_top ~n:"^ string_of_int n)
      (None :: List.map (fun _ -> None) what) oc (my_state g :: what)

  (* Generator: the function appears only during tuple generation, where
   * it sends the output to its continuation as (freevar_name t).
   * In normal expressions we merely refer to that free variable. *)
  | Generator, GeneratorFun (_, Split (e1,e2)), Some TString ->
    emit_functionN ?state ~consts "CodeGenLib.split" [Some TString; Some TString] oc [e1; e2]
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
    | Some t1, Some t2 -> Some (RamenTypes.large_enough_for t1 t2) in
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
(*$= add_missing_types & ~printer:(IO.to_string (List.print (Option.print (RamenTypes.print_typ))))
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
and emit_function
      (* Set to true if [impl] already returns an optional type for a
       * nullable value: *)
      ?(impl_return_nullable=false)
      ?(args_as=Arg) ?state ~consts impl arg_typs es oc vt_specs_opt =
  let open RamenExpr in
  let arg_typs = add_missing_types arg_typs es in
  let len, has_nullable =
    List.fold_left2 (fun (i, had_nullable) e arg_typ ->
        if is_nullable e then (
          Printf.fprintf oc "(match %a with None as n_ -> n_ | Some x%d_ -> "
            (conv_to ?state ~context:Finalize ~consts arg_typ) e
            i ;
          i + 1, true
        ) else (
          Printf.fprintf oc "(let x%d_ =\n\t\t%a in "
            i
            (conv_to ?state ~context:Finalize ~consts arg_typ) e ;
          i + 1, had_nullable
        )
      ) (0, false) es arg_typs
  in
  Printf.fprintf oc "%s(%s"
    (if has_nullable && not impl_return_nullable then "Some " else "")
    impl ;
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
                 (conv_to ?state ~context:Finalize ~consts vt) oc ves)
    vt_specs_opt ;
  for _i = 0 to len do Printf.fprintf oc ")" done

and emit_functionN ?args_as ?impl_return_nullable ?state ~consts
                   impl arg_typs oc es =
  emit_function ?args_as ?impl_return_nullable ?state ~consts
                impl arg_typs es oc None

and emit_functionNv ?impl_return_nullable ?state ~consts
                    impl arg_typs es vt oc ves =
  emit_function ?impl_return_nullable ?state ~consts
                impl arg_typs es oc (Some (vt, ves))

let emit_compute_nullmask_size oc ser_typ =
  Printf.fprintf oc "\tlet nullmask_bytes_ =\n" ;
  Printf.fprintf oc "\t\tList.fold_left2 (fun s nullable keep ->\n" ;
  Printf.fprintf oc "\t\t\tif nullable && keep then s+1 else s) 0\n" ;
  Printf.fprintf oc "\t\t\t%a\n"
    (List.print (fun oc field -> Bool.print oc field.nullable))
      ser_typ ;
  Printf.fprintf oc "\t\t\tskiplist_ |>\n" ;
  Printf.fprintf oc "\t\tRingBuf.bytes_for_bits |>\n" ;
  Printf.fprintf oc "\t\tRingBuf.round_up_to_rb_word in\n"

(* Emit the code computing the sersize of some variable *)
let emit_sersize_of_field_var typ oc var =
  match typ with
  | TString ->
    Printf.fprintf oc "(RingBufLib.sersize_of_string %s)" var
  | TIp ->
    Printf.fprintf oc "(RingBufLib.sersize_of_ip %s)" var
  | TCidr ->
    Printf.fprintf oc "(RingBufLib.sersize_of_cidr %s)" var
  | TTuple _ | TVec _ -> assert false
  | _ -> emit_sersize_of_fixsz_typ oc typ

let emit_sersize_of_tuple name oc tuple_typ =
  (* We want the sersize of the serialized version of course: *)
  let ser_typ = RingBufLib.ser_tuple_typ_of_tuple_typ tuple_typ in
  (* Like for serialize_tuple, we receive first the skiplist and then the
   * actual tuple, so we can compute the nullmask in advance: *)
  Printf.fprintf oc "let %s skiplist_ =\n" name ;
  emit_compute_nullmask_size oc ser_typ ;
  (* Returns the size: *)
  let rec emit_sersize_of_scalar val_var nullable oc typ =
    if nullable then (
      Printf.fprintf oc
        "\t\t\t(match %s with None -> 0 | Some %s ->\n\
         \t%a)"
        val_var val_var
        (emit_sersize_of_scalar val_var false) typ
    ) else (match typ with
    | TTuple ts ->
        Printf.fprintf oc "\t\t\t(let %a = %s in\n"
          (array_print_as_tuple_i (fun oc i _ ->
            let item_var = val_var ^"_"^ string_of_int i in
            String.print oc item_var)) ts
          val_var ;
        Array.iteri (fun i t ->
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "%a + "
            (emit_sersize_of_scalar item_var false) t
        ) ts ;
        Printf.fprintf oc "0)"
    | TVec (d, t) ->
        for i = 0 to d-1 do
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\t\t(let %s = %s.(%d) in %a) + "
            item_var val_var i
            (emit_sersize_of_scalar item_var false) t
        done ;
        Printf.fprintf oc "0"
    | t ->
        Printf.fprintf oc "\t\t\t%a"
          (emit_sersize_of_field_var typ) val_var
    )
  in
  (* Now for the code run for each tuple: *)
  Printf.fprintf oc "\tfun %a ->\n"
    (print_tuple_deconstruct TupleOut) tuple_typ ;
  Printf.fprintf oc "\t\tlet sz_ = nullmask_bytes_ in\n" ;
  List.iter (fun field ->
      let id = id_of_field_typ ~tuple:TupleOut field in
      Printf.fprintf oc "\t\t(* %s *)\n" id ;
      Printf.fprintf oc "\t\tlet sz_ = sz_ + if List.hd skiplist_ then (\n" ;
      emit_sersize_of_scalar id field.nullable oc field.typ ;
      (* Note: disable warning 26 because emit_sersize_of_scalar might
       * have generated tons of unused bindings: *)
      Printf.fprintf oc "\n\t\t) else 0 [@@ocaml.warning \"-26\"] in\n" ;
      Printf.fprintf oc "\t\tlet skiplist_ = List.tl skiplist_ in\n" ;
    ) ser_typ ;
  Printf.fprintf oc "\t\tignore skiplist_ ;\n" ;
  Printf.fprintf oc "\t\tsz_\n"

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
  (* Write the value and return new offset and nulli: *)
  let rec emit_write_scalar tx_var offs_var nulli_var val_var nullable oc typ =
    if nullable then (
      (* Write either nothing (since the nullmask is initialized with 0) or
       * the nullmask bit and the value *)
      Printf.fprintf oc "\t\t\t\tmatch %s with\n" val_var ;
      Printf.fprintf oc "\t\t\t\t| None -> %s, %s + 1\n" offs_var nulli_var ;
      Printf.fprintf oc "\t\t\t\t| Some %s ->\n" val_var ;
      Printf.fprintf oc "\t\t\t\t\tRingBuf.set_bit %s %s ;\n" tx_var nulli_var ;
      Printf.fprintf oc "\t\t\t\t\tlet %s, %s =\n\
                         \t%a in
                         \t\t\t\t\t%s, %s + 1"
        offs_var nulli_var
        (emit_write_scalar tx_var offs_var nulli_var val_var false) typ
        offs_var nulli_var
    ) else (match typ with
    (* Constructed types (items not nullable): *)
    | TTuple ts ->
        Printf.fprintf oc "\t\t\tlet %a = %s in\n"
          (array_print_as_tuple_i (fun oc i _ ->
            let item_var = val_var ^"_"^ string_of_int i in
            String.print oc item_var)) ts
          val_var ;
        Array.iteri (fun i t ->
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\t\t\tlet %s, %s = %a in\n"
            offs_var nulli_var
            (emit_write_scalar tx_var offs_var nulli_var item_var false) t
        ) ts ;
        Printf.fprintf oc "\t\t\t\t%s, %s" offs_var nulli_var
    | TVec (d, t) ->
        for i = 0 to d-1 do
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\t\t\tlet %s, %s =\n\
                             \t\t\t\t\tlet %s = %s.(%d) in %a in\n"
            offs_var nulli_var
            item_var val_var i
            (emit_write_scalar tx_var offs_var nulli_var item_var false) t
        done ;
        Printf.fprintf oc "\t\t\t\t%s, %s" offs_var nulli_var
    (* Scalar types (maybe nullable): *)
    | t ->
        Printf.fprintf oc "\t\t\t\tRingBuf.write_%s %s %s %s ;\n"
          (id_of_typ t) tx_var offs_var val_var ;
        Printf.fprintf oc "\t\t\t\t%s + %a, %s"
          offs_var (emit_sersize_of_field_var t) val_var nulli_var ;
        if verbose_serialization then
          Printf.fprintf oc "\t\t\t\t!RamenLog.logger.RamenLog.debug \"Serializing %s (%%s) at offset %%d\" (dump %s) %s ;\n" val_var val_var offs_var
    )
  in
  List.iter (fun field ->
      Printf.fprintf oc "\t\tlet offs_, nulli_ =\n\
                         \t\t\tif List.hd skiplist_ then (\n" ;
      let id = id_of_field_typ ~tuple:TupleOut field in
      emit_write_scalar "tx_" "offs_" "nulli_" id field.nullable oc field.typ ;
      Printf.fprintf oc "\n\t\t\t) else offs_, nulli_ in\n" ;
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

let emit_time_of_tuple name params event_time oc tuple_typ =
  let open RamenEventTime in
  Printf.fprintf oc "let %s %a =\n"
    name
    (print_tuple_deconstruct TupleOut) tuple_typ ;
  match event_time with
  | None ->
      Printf.fprintf oc "\t0., 0. (* No event time info *)\n"
  | Some ((sta_field, sta_src, sta_scale), dur) ->
      let field_value_to_float src oc field_name =
        match src with
        | OutputField ->
            let f = List.find (fun t -> t.typ_name = field_name) tuple_typ in
            Printf.fprintf oc
              (if f.nullable then "((%a) %s |? 0.)" else "(%a) %s")
              (conv_from_to ~nullable:f.nullable) (f.typ, TFloat)
              (id_of_field_name ~tuple:TupleOut field_name)
        | Parameter ->
            let p_typ = List.assoc field_name params |>
                        RamenTypes.type_of in
            Printf.fprintf oc "(%a %s_%s_)"
              (conv_from_to ~nullable:false) (p_typ, TFloat)
              (id_of_prefix TupleParam) field_name
      in
      Printf.fprintf oc "\tlet start_ = %a *. %a\n"
        (field_value_to_float !sta_src) sta_field
        emit_float sta_scale ;
      (match dur with
      | DurationConst d ->
          Printf.fprintf oc "\tand dur_ = %a in\n\
                             \tstart_, start_ +. dur_\n"
            emit_float d
      | DurationField (dur_field, dur_src, dur_scale) ->
          Printf.fprintf oc "\tand dur_ = %a *. %a in\n\
                             \tstart_, start_ +. dur_\n"
            (field_value_to_float !dur_src) dur_field
            emit_float dur_scale ;
      | StopField (sto_field, sto_src, sto_scale) ->
          Printf.fprintf oc "\tand stop_ = %a *. %a in\n\
                             \tstart_, stop_\n"
            (field_value_to_float !sto_src) sto_field
            emit_float sto_scale)

(* Given a tuple type, generate the ReadCSVFile operation. *)
let emit_read_csv_file consts params oc name csv_fname unlink
                       csv_separator csv_null
                       tuple_typ preprocessor event_time =
  (* The dynamic part comes from the unpredictable field list.
   * For each input line, we want to read all fields and build a tuple.
   * Then we want to write this tuple in some ring buffer.
   * We need to generate these functions:
   * - reading a CSV string into a tuple type (when nullable fields are option type)
   * - given such a tuple, return its serialized size
   * - given a pointer toward the ring buffer, serialize the tuple *)
  Printf.fprintf oc
     "%a\n%a\n%a\n%a\n\
     let %s () =\n\
       \tCodeGenLib.read_csv_file %S %b %S sersize_of_tuple_\n\
       \t\ttime_of_tuple_ serialize_tuple_ tuple_of_strings_ %S\n\
       \t\tfield_of_params_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_time_of_tuple "time_of_tuple_" params event_time) tuple_typ
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    (emit_tuple_of_strings "tuple_of_strings_" csv_null) tuple_typ
    name
    csv_fname unlink csv_separator preprocessor

let emit_listen_on consts params oc name net_addr port proto =
  let open RamenProtocols in
  let tuple_typ = tuple_typ_of_proto proto in
  let collector = collector_of_proto proto in
  let event_time = event_time_of_proto proto in
  Printf.fprintf oc "%a\n%a\n%a\n\
    let %s () =\n\
      \tCodeGenLib.listen_on %s %S %d %S sersize_of_tuple_ time_of_tuple_ serialize_tuple_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_time_of_tuple "time_of_tuple_" params event_time) tuple_typ
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    name
    collector
    (Unix.string_of_inet_addr net_addr) port
    (string_of_proto proto)

let emit_instrumentation consts params oc name from =
  let open RamenProtocols in
  let tuple_typ = RamenBinocle.tuple_typ in
  let event_time = RamenBinocle.event_time in
  Printf.fprintf oc "%a\n%a\n%a\n\
    let %s () =\n\
      \tCodeGenLib.instrumentation %a sersize_of_tuple_ time_of_tuple_ serialize_tuple_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_time_of_tuple "time_of_tuple_" params event_time) tuple_typ
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    name
    (List.print (fun oc from ->
      Printf.fprintf oc "%S" RamenOperation.(match from with
        | GlobPattern s -> s
        | NamedOperation id -> string_of_func_id id
        | SubQuery _ -> assert false))) from

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
  (* Returns value, offset: *)
  let rec emit_read_scalar tx_var offs_var val_var nullable nulli oc typ =
    if nullable then (
      Printf.fprintf oc "\
        \t\tif RingBuf.get_bit %s %d then (\n\
        \t\tlet %s, %s =\n\
        \t%a in\n
        \t\tSome %s, %s\n
        \t\t) else None, %s"
        tx_var nulli
        val_var offs_var
        (emit_read_scalar tx_var offs_var val_var false nulli) typ
        val_var offs_var
        offs_var
    ) else (match typ with
    (* Constructed types are read item by item (items are non nullables): *)
    | TTuple ts ->
        Array.iteri (fun i t ->
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\tlet %s, %s = %a in\n"
            item_var offs_var
            (emit_read_scalar tx_var offs_var item_var false nulli) t
        ) ts ;
        Printf.fprintf oc "\t%a, %s"
          (array_print_as_tuple_i (fun oc i _ ->
            let item_var = val_var ^"_"^ string_of_int i in
            String.print oc item_var)) ts
          offs_var ;
    | TVec (d, t) ->
        for i = 0 to d-1 do
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\tlet %s, %s = %a in\n"
            item_var offs_var
            (emit_read_scalar tx_var offs_var item_var false nulli) t
        done ;
        Printf.fprintf oc "\t[| " ;
        for i = 0 to d-1 do
          Printf.fprintf oc "%s_%d;" val_var i
        done ;
        Printf.fprintf oc " |], %s" offs_var
    (* Non constructed types (therefore nullable): *)
    | _ ->
        Printf.fprintf oc "\
          \t\tRingBuf.read_%s %s %s, %s + %a"
          (id_of_typ typ) tx_var offs_var
          offs_var (emit_sersize_of_not_null tx_var offs_var) typ
    ) ;
    if verbose_serialization then
      Printf.fprintf oc "\t!RamenLog.logger.RamenLog.debug \"deserialized field %s (%%s) at offset %%d\" (dump %s) offs_ ;\n" val_var val_var
  in
  let _ = List.fold_left (fun nulli field ->
      let id = id_of_field_typ ~tuple:TupleIn field in
      if and_all_others || Set.mem field.typ_name mentioned then (
        Printf.fprintf oc "\tlet %s, offs_ =\n%a in\n"
          id (emit_read_scalar "tx_" "offs_" id field.nullable nulli) field.typ
      ) else (
        Printf.printf "Non mentionned field: %s\n%!" field.typ_name ;
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
let emit_generator user_fun ~consts oc expr =
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
        (emit_expr ?state:None ~context:Generator ~consts) expr
        (freevar_name t)
    (* We have no other generators (yet) *)
    | _ -> assert false
  in
  List.iter (emit_gen_root oc) generators ;

  (* Finally, call user_func on the actual expression, where all generators will
   * be replaced by their free variable: *)
  Printf.fprintf oc "%s (%a)"
    user_fun
    (emit_expr ?state:None ~context:Finalize ~consts) expr ;
  List.iter (fun _ -> Printf.fprintf oc ")") generators

let emit_generate_tuples name in_typ mentioned and_all_others out_typ ~consts oc selected_fields =
  let has_generator =
    List.exists (fun sf ->
      RamenExpr.is_generator sf.RamenOperation.expr)
      selected_fields in
  if not has_generator then
    Printf.fprintf oc "let %s f_ it_ ot_ = f_ it_ ot_\n" name
  else (
    Printf.fprintf oc "let %s f_ (%a as it_) %a =\n"
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
              (emit_generator ff_ ~consts) sf.RamenOperation.expr
              nb_gens ;
            nb_gens + 1)
        ) 0 selected_fields in
    (* Now we have all the generated values, actually call f_ on the tuple.
     * Note that the tuple must be in out_typ order: *)
    Printf.fprintf oc "%af_ it_ (\n%a"
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
        Printf.fprintf oc "(match %s with None -> \"?null?\" | Some v_ -> (%a) v_)\n"
          id
          (conv_from_to ~nullable:false) (field_typ.typ, TString)
      ) else (
        Printf.fprintf oc "(%a) %s\n"
          (conv_from_to ~nullable:false) (field_typ.typ, TString)
          id
      )
    ) tuple_typ ;
  Printf.fprintf oc "\t| _ -> raise Not_found\n"

let emit_state_update_for_expr ~consts oc expr =
  RamenExpr.unpure_iter (function
      | RamenExpr.StatefulFun (_, lifespan, _) as e ->
        let state_var =
          match lifespan with LocalState -> "group_"
                            | GlobalState -> "global_" in
        Printf.fprintf oc "\t%s.%s <- (%a) ;\n"
          state_var
          (name_of_state e)
          (emit_expr ?state:None ~context:UpdateState ~consts) e
      | _ -> ()
    ) expr

let emit_where
      ?(with_group=false) ?(always_true=false)
      name in_typ mentioned and_all_others ~consts oc expr =
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
    emit_state_update_for_expr ~consts oc expr ;
    Printf.fprintf oc "\t%a\n"
      (emit_expr ?state:None ~context:Finalize ~consts) expr
  )

let emit_field_selection
      ?(with_selected=false) (* and unselected *)
      ?(with_group=false) (* including previous, of type tuple_out option *)
      name in_typ mentioned
      and_all_others out_typ ~consts oc selected_fields =
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
      emit_state_update_for_expr ~consts oc sf.RamenOperation.expr ;
      Printf.fprintf oc "\t(* Output field: *)\n" ;
      if RamenExpr.is_generator sf.RamenOperation.expr then
        (* So that we have a single out_typ both before and after tuples generation *)
        Printf.fprintf oc "\tlet %s = () in\n"
          (id_of_field_name ~tuple:TupleOut sf.RamenOperation.alias)
      else
        Printf.fprintf oc "\tlet %s = %a in\n"
          (id_of_field_name ~tuple:TupleOut sf.RamenOperation.alias)
          (emit_expr ?state:None ~context:Finalize ~consts) sf.RamenOperation.expr
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
let emit_key_of_input name in_typ mentioned and_all_others ~consts oc exprs =
  Printf.fprintf oc "let %s %a =\n\t("
    name
    (emit_in_tuple mentioned and_all_others) in_typ ;
  List.iteri (fun i expr ->
      Printf.fprintf oc "%s\n\t\t%a"
        (if i > 0 then "," else "")
        (emit_expr ?state:None ~context:Finalize ~consts) expr ;
    ) exprs ;
  Printf.fprintf oc "\n\t)\n"

let for_each_unpure_fun selected_fields
                        ?where ?commit_when f =
  List.iter (fun sf ->
      RamenExpr.unpure_iter f sf.RamenOperation.expr
    ) selected_fields ;
  Option.may (RamenExpr.unpure_iter f) where ;
  Option.may (RamenExpr.unpure_iter f) commit_when

let for_each_unpure_fun_my_lifespan lifespan selected_fields
                                    ?where ?commit_when f =
  let open RamenExpr in
  for_each_unpure_fun selected_fields
                      ?where ?commit_when
    (function
    | StatefulFun (_, l, _) as e when l = lifespan ->
      f e
    | _ -> ())

let otype_of_state e =
  let open RamenExpr in
  let typ = typ_of e in
  let t = Option.get typ.scalar_typ |>
          IO.to_string otype_of_type in
  let t =
    let print_expr_typ oc e =
      let typ = typ_of e in
      Option.get typ.scalar_typ |> (* nullable taken care of below *)
      IO.to_string otype_of_type |>
      String.print oc in
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
      Printf.sprintf2 "%a CodeGenLib.distinct_state"
        (list_print_as_product print_expr_typ) es
    | StatefulFun (_, _, AggrAvg _) -> "(int * float)"
    | StatefulFun (_, _, (AggrMin _|AggrMax _)) -> t ^" option"
    | StatefulFun (_, _, Top { what ; _ }) ->
      Printf.sprintf2 "%a HeavyHitters.t"
        (list_print_as_product print_expr_typ) what
    | StatefulFun (_, _, AggrHistogram _) -> "CodeGenLib.histogram"
    | _ -> t in
  if Option.get typ.nullable then t ^" option" else t

let emit_state_init name state_lifespan other_state_vars
      ?where ?commit_when ~consts
      oc selected_fields =
  (* We must collect all unpure functions present in the selected_fields
   * and return a record with the proper types and init values for the required
   * states. *)
  let for_each_my_unpure_fun f =
    for_each_unpure_fun_my_lifespan
      state_lifespan selected_fields ?where ?commit_when f
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
          (emit_expr ~context:InitState ~consts ~state:state_lifespan) f) ;
    (* And now build the state record from all those fields: *)
    Printf.fprintf oc "\t{" ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf oc " %s ;" (name_of_state f)) ;
    Printf.fprintf oc " }\n"
  )

(* Note: we need group_ in addition to out_tuple because the commit-when clause
 * might have its own stateful functions going on *)
let emit_when name in_typ mentioned and_all_others out_typ ~consts
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
  emit_state_update_for_expr ~consts oc when_expr ;
  Printf.fprintf oc "\t%a\n"
    (emit_expr ?state:None ~context:Finalize ~consts) when_expr

let emit_should_resubmit name in_typ mentioned and_all_others ~consts
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
    Printf.fprintf oc "\t%a\n" (emit_expr ?state:None ~context:Finalize ~consts) e
  | RemoveAll e ->
    Printf.fprintf oc "\tnot (%a)\n" (emit_expr ?state:None ~context:Finalize ~consts) e

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

let emit_sort_expr name in_typ mentioned and_all_others ~consts oc es_opt =
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
           (emit_expr ?state:None ~context:Finalize ~consts)) es

let emit_merge_on name in_typ mentioned and_all_others ~consts oc es =
  Printf.fprintf oc "let %s %a =\n\t%a\n"
    name
    (emit_in_tuple mentioned and_all_others) in_typ
    (List.print ~first:"(" ~last:")" ~sep:", "
       (emit_expr ?state:None ~context:Finalize ~consts)) es

let expr_needs_group e =
  let open RamenExpr in
  fold_by_depth (fun need expr ->
    need || match expr with
      | Field (_, tuple, _) -> tuple_need_state !tuple
      | StatefulFun (_, LocalState, _) -> true
      | _ -> false
  ) false e

let emit_aggregate consts params oc name in_typ out_typ = function
  | RamenOperation.Aggregate
      { fields ; and_all_others ; merge ; sort ; where ; key ;
        commit_before ; commit_when ; flush_how ; notifications ; event_time ;
        every ; _ } as op ->
  (* FIXME: now that we serialize only used fields, when do we have fields
   * that are not mentioned?? *)
  let mentioned =
    let all_exprs = RamenOperation.fold_expr [] (fun l s -> s :: l) op in
    add_all_mentioned_in_expr all_exprs
  (* Tells whether we need the group to check the where clause (because it
   * uses the group tuple or build a group-wise aggregation on its own,
   * despite this is forbidden in RamenOperation.check): *)
  and where_need_group = expr_needs_group where
  (* Good to know when performing a TOP: *)
  and when_to_check_for_commit = when_to_check_group_for_expr commit_when in
  Printf.fprintf oc
    "%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n"
    (emit_state_init "global_init_" RamenExpr.GlobalState [] ~where ~commit_when ~consts) fields
    (emit_state_init "group_init_" RamenExpr.LocalState ["global_"] ~where ~commit_when ~consts) fields
    (emit_read_tuple "read_tuple_" mentioned and_all_others) in_typ
    (if where_need_group then
      emit_where "where_fast_" ~always_true:true in_typ mentioned and_all_others ~consts
    else
      emit_where "where_fast_" in_typ mentioned and_all_others ~consts) where
    (if not where_need_group then
      emit_where "where_slow_" ~with_group:true ~always_true:true in_typ mentioned and_all_others ~consts
    else
      emit_where "where_slow_" ~with_group:true in_typ mentioned and_all_others ~consts) where
    (emit_key_of_input "key_of_input_" in_typ mentioned and_all_others ~consts) key
    emit_maybe_fields out_typ
    (emit_when "commit_when_" in_typ mentioned and_all_others out_typ ~consts) commit_when
    (emit_field_selection ~with_selected:true ~with_group:true "tuple_of_group_" in_typ mentioned and_all_others out_typ ~consts) fields
    (emit_sersize_of_tuple "sersize_of_tuple_") out_typ
    (emit_time_of_tuple "time_of_tuple_" params event_time) out_typ
    (emit_serialize_tuple "serialize_group_") out_typ
    (emit_generate_tuples "generate_tuples_" in_typ mentioned and_all_others out_typ ~consts) fields
    (emit_should_resubmit "should_resubmit_" in_typ mentioned and_all_others ~consts) flush_how
    (emit_field_of_tuple "field_of_tuple_in_") in_typ
    (emit_field_of_tuple "field_of_tuple_out_") out_typ
    (emit_merge_on "merge_on_" in_typ mentioned and_all_others ~consts) (fst merge)
    (emit_sort_expr "sort_until_" in_typ mentioned and_all_others ~consts) (match sort with Some (_, Some u, _) -> [u] | _ -> [])
    (emit_sort_expr "sort_by_" in_typ mentioned and_all_others ~consts) (match sort with Some (_, _, b) -> b | None -> []) ;
  Printf.fprintf oc "let %s () =\n\
      \tCodeGenLib.aggregate\n\
      \t\tread_tuple_ sersize_of_tuple_ time_of_tuple_ serialize_group_\n\
      \t\tgenerate_tuples_\n\
      \t\ttuple_of_group_ merge_on_ %F %d sort_until_ sort_by_\n\
      \t\twhere_fast_ where_slow_ key_of_input_ %b\n\
      \t\tcommit_when_ %b %b %s should_resubmit_\n\
      \t\tglobal_init_ group_init_\n\
      \t\tfield_of_tuple_in_ field_of_tuple_out_ field_of_params_ %a %f\n"
    name
    (snd merge)
    (match sort with None -> 0 | Some (n, _, _) -> n)
    (key = [])
    commit_before
    (flush_how <> Never)
    when_to_check_for_commit
    (* TODO: instead of passing a list of strings, pass a list of [RamenOperation.notification]s. *)
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

let emit_operation name func_name in_typ out_typ params op oc =
  Printf.fprintf oc "(* Code generated for operation %S:\n%a\n*)\n\
    open Batteries\n\
    open Stdint\n"
    (RamenName.string_of_func func_name)
    RamenOperation.print op ;
  (* Emit parameters: *)
  Printf.fprintf oc "\n(* Parameters: *)\n" ;
  List.iter (fun (n, v) ->
    Printf.fprintf oc
      "let %s_%s_ =\n\
       \tlet parser_ = RamenTypeConverters.%s_of_string in\n\
       \tCodeGenLib.parameter_value ~def:(%a) parser_ %S\n"
      (id_of_prefix TupleParam) n
      (id_of_typ (RamenTypes.type_of v))
      emit_type v
      n
  ) params ;
  (* Also a function that takes a parameter name (string) and return its
   * value (as a string) - useful for text replacements within strings *)
  Printf.fprintf oc "let field_of_params_ = function\n%a\
                     \t| _ -> raise Not_found\n\n"
    (List.print ~first:"" ~last:"" ~sep:"" (fun oc (n, v) ->
      let glob_name =
        Printf.sprintf "%s_%s_" (id_of_prefix TupleParam) n in
      Printf.fprintf oc "\t| %S -> (%a) %s\n"
        n
        (conv_from_to ~nullable:false) (RamenTypes.type_of v, TString)
        glob_name)) params ;
  (* Now the code, which might need some global constant parameters,
   * thus the two strings that are assembled later: *)
  let consts = IO.output_string () in
  let code = IO.output_string () in
  (match op with
  | ReadCSVFile { where = { fname ; unlink } ; preprocessor ;
                  what = { separator ; null ; fields } ; event_time } ->
    emit_read_csv_file consts params code name fname unlink separator null
                       fields preprocessor event_time
  | ListenFor { net_addr ; port ; proto } ->
    emit_listen_on consts params code name net_addr port proto
  | Instrumentation { from } ->
    emit_instrumentation consts params code name from
  | Aggregate _ ->
    emit_aggregate consts params code name in_typ out_typ op) ;
  Printf.fprintf oc "\n(* Global constants: *)\n\n%s\n\
                     \n(* Operation Implementation: *)\n\n%s\n"
    (IO.close_out consts) (IO.close_out code)

let compile conf entry_point func_name obj_name in_typ out_typ params op =
  let open RamenOperation in
  let%lwt src_file =
    Lwt.wrap (fun () ->
      RamenOCamlCompiler.with_code_file_for obj_name conf
        (emit_operation entry_point func_name in_typ out_typ params op)) in
  (* TODO: any failure in compilation -> delete the source code! Or it will be reused *)
  RamenOCamlCompiler.compile conf func_name src_file obj_name
