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
open RamenNullable

(* If true, the generated code will log details about serialization *)
let verbose_serialization = false

(* We pass this around as "opc" *)
type op_context =
  { op : RamenOperation.t ;
    (* The type of the output tuple in user order *)
    tuple_typ : RamenTuple.field_typ list ;
    params : RamenTuple.params ;
    consts : string Batteries.IO.output }

let id_of_prefix tuple =
  String.nreplace (string_of_prefix tuple) "." "_"

(* Tuple deconstruction as a function parameter: *)
let id_of_field_name ?(tuple=TupleIn) = function
  | "#count" -> "virtual_"^ id_of_prefix tuple ^"_count_"
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
  let print_field oc field_typ =
      String.print oc (id_of_field_typ ~tuple field_typ)
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
    Printf.fprintf oc "RingBuf.(rb_word_bytes + \
                         round_up_to_rb_word(\
                           match RingBuf.read_word %s %s with \
                            0 -> %a | 1 -> %a | _ -> assert false))"
      tx_var offs_var
      emit_sersize_of_fixsz_typ TIpv4
      emit_sersize_of_fixsz_typ TIpv6
  | TCidr ->
    Printf.fprintf oc "RingBuf.(rb_word_bytes + \
                         round_up_to_rb_word(\
                           match RingBuf.read_u8 %s %s |> Uint8.to_int with \
                            4 -> %a | 6 -> %a | _ -> assert false))"
      tx_var offs_var
      emit_sersize_of_fixsz_typ TCidrv4
      emit_sersize_of_fixsz_typ TCidrv6

  | TTuple ts -> (* Should not be used! Will be used when a constructed field is not mentioned and is skipped. Get rid of mentioned fields!*)
    let nullmask_sz = RingBufLib.nullmask_sz_of_tuple ts in
    Printf.fprintf oc "\
      let bi_start_ = %s * 8 in\n" offs_var ;
    Printf.fprintf oc "\
      let %s_ = %s + %d in\n"
      offs_var offs_var nullmask_sz ;
    Array.iteri (fun i t ->
      Printf.fprintf oc "\
        let bi_ = bi_start_ + %d in\n\
        let %s_ = %s_ + (%a) in\n"
        i
        offs_var offs_var
        (emit_sersize_of_field_tx tx_var (offs_var^"_") "bi_"
          t.nullable)
          t.structure
    ) ts ;
    Printf.fprintf oc "%s_ - %s"
      offs_var offs_var
  | TVec _ | TList _ ->
    todo "vector sersize"
  | t -> emit_sersize_of_fixsz_typ oc t

(* Emit the code to retrieve the sersize of some serialized value *)
and emit_sersize_of_field_tx tx_var offs_var nulli_var
                             nullable oc structure =
  if nullable then (
    Printf.fprintf oc "if RingBuf.get_bit %s %s then %a else 0"
      tx_var nulli_var
      (emit_sersize_of_not_null tx_var offs_var) structure
  ) else
    emit_sersize_of_not_null tx_var offs_var oc structure

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
  | TList _ -> "list"
  | TNum | TAny | TEmpty -> assert false

let rec emit_value_of_string typ oc var =
  match typ with
  | TVec (_, t) | TList t ->
      Printf.fprintf oc
        "RamenHelpers.split_string ~sep:';' ~opn:'[' ~cls:']' %s |>\n\
         Array.map (fun x_ -> %a)"
        var
        (emit_value_of_string t.structure) "x_"
  | TTuple ts ->
      (* FIXME: same as above re. [split_on_char]: *)
      Printf.fprintf oc
        "let s_ =\n\
           RamenHelpers.split_string ~sep:';' ~opn:'(' ~cls:')' %s in\n\
         if Array.length s_ <> %d then failwith (\
           Printf.sprintf \"Bad arity for tuple %%s, expected %d items\" \
             %s) ;\n\
         %a"
         var (Array.length ts) (Array.length ts) var
         (array_print_as_tuple_i (fun oc i t ->
           emit_value_of_string t.structure oc
             ("s_.("^ string_of_int i ^")"))) ts
  | typ ->
      Printf.fprintf oc "RamenTypeConverters.%s_of_string %s"
        (id_of_typ typ) var

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
   * https://caml.inria.fr/mantis/view.php?id=7685
   * and "%h" not for neg_infinity. *)
  if f = infinity then String.print oc "infinity"
  else if f = neg_infinity then String.print oc "neg_infinity"
  else Legacy.Printf.sprintf "%h" f |> String.print oc

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
  | VTuple vs -> Array.print ~first:"(" ~last:")" ~sep:", " emit_type oc vs
  | VVec vs   -> Array.print emit_type oc vs
  (* For now ramen lists are ocaml arrays. Should they be ocaml lists? *)
  | VList vs  -> Array.print emit_type oc vs
  | VNull     -> Printf.fprintf oc "Null"

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
  | StatefulFun (t, g, _, _) ->
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
      (* TODO: take into account t.nullable *)
      Array.print ~first:"(" ~last:")" ~sep:" * "
        (fun oc t -> otype_of_type oc t.structure)
        oc ts
  | TVec (_, t) | TList t ->
      (* TODO: take into account t.nullable *)
      Printf.fprintf oc "%a array" otype_of_type t.structure
  | TNum | TAny | TEmpty -> assert false

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
  | TTuple _ | TVec _ | TList _ | TNum | TAny | TEmpty -> assert false

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
 * string *)
(* This only returns the function name (or code) but does not emit the
 * call to that function. *)
let rec conv_from_to ~nullable oc (from_typ, to_typ) =
  (* Emitted code must be prefixable by "nullable_map": *)
  let rec print_non_null oc (from_typ, to_typ as conv) =
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
    | (TIpv4 | TU32), TIp -> Printf.fprintf oc "(fun x_ -> RamenIp.V4 x_)"
    | (TIpv6 | TU128), TIp -> Printf.fprintf oc "(fun x_ -> RamenIp.V6 x_)"
    | TCidrv4, TCidr -> Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V4 x_)"
    | TCidrv6, TCidr -> Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V6 x_)"
    | TIpv4, TU32 | TU32, TIpv4 -> Printf.fprintf oc "identity"
    | TIpv6, TU128 | TU128, TIpv6 -> Printf.fprintf oc "identity"
    | TU64, TEth -> Printf.fprintf oc "Uint48.of_uint64"
    | TVec (d_from, t_from), TVec (d_to, t_to)
      when (d_from = d_to || d_to = 0) &&
           (* TODO: We could implement other combinations as well *)
           t_from.nullable = t_to.nullable ->
      (* d_to = 0 means no constraint (copy the one from the left-hand side) *)
      (* Note: vector items cannot be NULL: *)
      Printf.fprintf oc "(fun v_ -> Array.map (%a) v_)"
        (conv_from_to ~nullable:t_from.nullable) (t_from.structure, t_to.structure)
    | TVec (d, t_from), TList t_to ->
      print_non_null oc (from_typ, TVec (d, t_to))
    | (TVec (_, t) | TList t), TString ->
      Printf.fprintf oc
        "(fun v_ -> \
           \"[\"^ (\
            Array.enum v_ /@ (%a) |> \
            Enum.reduce (fun s1_ s2_ -> s1_^\";\"^s2_)
           ) ^\"]\")"
        (conv_from_to ~nullable:t.nullable) (t.structure, TString)
    | TTuple ts, TString ->
      let i = ref 0 in
      Printf.fprintf oc
        "(fun %a -> \"(\"^ %a ^\")\")"
          (array_print_as_tuple_i (fun oc i _ ->
            Printf.fprintf oc "x%d_" i)) ts
          (Array.print ~first:"" ~last:"" ~sep:" ^\";\"^ " (fun oc t ->
            Printf.fprintf oc "(%a) x%d_"
              (conv_from_to ~nullable:t.nullable) (t.structure, TString) !i ;
            incr i)) ts
    | _ ->
      Printf.sprintf2 "Cannot find converter from type %a to type %a"
        print_structure from_typ
        print_structure to_typ |>
      failwith
  in
  if from_typ = to_typ then Printf.fprintf oc "identity"
  else
    Printf.fprintf oc "(%s%a)"
      (if nullable then "nullable_map " else "")
      print_non_null (from_typ, to_typ)

let wrap_nullable ~nullable oc s =
  if nullable then Printf.fprintf oc "NotNull (%s)" s
  else Printf.fprintf oc "%s" s

let freevar_name t = "fv_"^ string_of_int t.RamenExpr.uniq_num ^"_"

(* FIXME: change lag so that this is no longer needed: *)
let any_constant_of_expr_type typ =
  let open RamenExpr in
  let open Stdint in
  let c v =
    let t = make_typ ~typ "init" in
    Const (t, v)
  in
  c (any_value_of_type typ.structure)

let emit_tuple tuple oc tuple_typ =
  print_tuple_deconstruct tuple oc tuple_typ

(* In some case we want emit_function to pass arguments as an array
 * (variadic functions...) or as a tuple (functions taking a tuple).
 * In both cases the int refers to how many normal args should we pass
 * before starting the array/tuple.*)
type args_as = Arg | Array of int | Tuple of int

exception Error of RamenExpr.t * context * string
let () =
  Printexc.register_printer (function
    | Error (expr, context, msg) ->
        Some (Printf.sprintf2 "While generating code for %s %a: %s"
          (match context with
          | InitState -> "initialization of"
          | UpdateState -> "updating state of"
          | Finalize -> "finalization of"
          | Generator -> "value generation for")
          (RamenExpr.print true) expr
          msg)
    | _ -> None)

(* This printer wrap expression [e] into a converter according to its current
 * type. to_typ is an option type: if None, no conversion is required
 * (useful for states). If e is nullable then so will be the result. *)
let rec conv_to ?state ~context ~opc to_typ oc e =
  let open RamenExpr in
  let t = Option.get (typ_of e).typ in
  match t.structure, to_typ with
  | a, Some b ->
    Printf.fprintf oc "(%a) (%a)"
      (conv_from_to ~nullable:t.nullable) (a, b)
      (emit_expr ~context ~opc ?state) e
  | _, None -> (* No conversion required *)
    (emit_expr ~context ~opc ?state) oc e

(* Apply the given function to the given args (and varargs), after
 * converting them, obeying skip_nulls. It is assumed that nullable
 * is set reliably. *)
and update_state ?state ~opc ~nullable skip my_state
                 es ?(vars=[]) ?vars_to_typ
                 func_name ?args_as oc to_typ =
  let open RamenExpr in
  let emit_func args oc varargs =
    match vars_to_typ with
    | None ->
      emit_functionN ?state ~opc ~nullable ?args_as
                     func_name (None::to_typ) oc (my_state::args)
    | Some vars_to_typ ->
      emit_functionNv ?state ~opc ~nullable func_name
                      (None::to_typ) (my_state::args) vars_to_typ oc
                      varargs
  in
  if nullable && skip then (
    (* Skip just means that if an entry is null we want to skip the
     * update. But maybe no entries are actually nullable. And the
     * state could be nullable or not. If skip, we will have an
     * additional bool named empty, initialized to true, that will
     * possibly stay true only if we skip all entries because an
     * arg was NULL every time. In that case in theory the typer
     * have made this state nullable, and we will return Null (in
     * finalize_state) *)
    (* Force the args to func_name to be non-nullable inside the
     * assignment, since we have already verified they are not null: *)
    Printf.fprintf oc "\t" ;
    let denullify e =
      if is_nullable e then (
        let t = typ_of e in
        let state_var_name =
          Printf.sprintf "nonnull_%d_" t.uniq_num in
        Printf.fprintf oc "(match %a with Null -> () | NotNull %s -> "
          (emit_expr ~context:Finalize ~opc ?state) e
          state_var_name ;
        StateField ({ t with typ = Some {
                        structure = (Option.get t.typ).structure ;
                        nullable = false } },
                    state_var_name)
      ) else e in
    let func_args = List.map denullify es
    and func_varargs = List.map denullify vars in
    (* When skip_nulls the state is accompanied
     * by a boolean that's true iff some values have been seen (used when
     * finalizing).
     * Some aggr function will never return NULL but from propagation or
     * skip, but some will. Those who will are always nullable, and will
     * return the Null/NotNull status themselves. Therefore, the code
     * generator has to know about them when finalizing, otherwise it will
     * assume the aggr function never returns Null. *)
    Printf.fprintf oc "%a <- %a ;\n"
      (emit_expr ?state ~context:Finalize ~opc) my_state
      (emit_func func_args) func_varargs ;
    Printf.fprintf oc "\t\t%a_empty_ <- false\n"
      (emit_expr ?state ~context:Finalize ~opc) my_state ;
    let close_denullify e =
      if is_nullable e then Printf.fprintf oc ")" in
    List.iter close_denullify es ;
    List.iter close_denullify vars ;
    Printf.fprintf oc " ;\n"
  ) else (
    Printf.fprintf oc "\t%a <- %a ;\n"
      (emit_expr ?state ~context:Finalize ~opc) my_state
      (emit_func es) vars
  )

(* Similarly, return the finalized value of the given state.
 * fin_args are the arguments passed to the finalizers and are not subject
 * to be skipped. If nullable then the Null will merely propagate to the
 * return value. *)
and finalize_state ?state ~opc ~nullable skip my_state func_name fin_args
                   ?impl_return_nullable ?args_as oc to_typ =
  let open RamenExpr in
  if nullable && skip then
    (* In the case where we stayed empty, the typer must have made this
     * state nullable so we can return directly its value: *)
    Printf.fprintf oc
      "(if %a_empty_ then Null else %a)"
      (emit_expr ?state ~context:Finalize ~opc) my_state
      (emit_functionN ?state ~opc ~nullable ?impl_return_nullable ?args_as
                      func_name (None::to_typ)) (my_state::fin_args)
  else
    emit_functionN ?state ~opc ~nullable ?impl_return_nullable ?args_as
                   func_name (None::to_typ) oc (my_state::fin_args)

(* The vectors TupleOutPrevious is optional: the commit when and
 * select clauses of aggregate operations either have it or not.
 * Each time they need access to a field they call a function "maybe_XXX_"
 * with that optional tuple, which avoids propagating out_typ down to
 * emit_expr - but hopefully the compiler will inline this.
 * (TODO: have a context in a single place and inline it directly?) *)
and emit_maybe_fields oc out_typ =
  List.iter (fun ft ->
    Printf.fprintf oc "let maybe_%s_ = function\n" ft.typ_name ;
    Printf.fprintf oc "  | None -> Null\n" ;
    Printf.fprintf oc "  | Some %a -> %s%s\n\n"
      (emit_tuple TupleOut) out_typ
      (if ft.typ.nullable then "" else "NotNull ")
      (id_of_field_name ~tuple:TupleOut ft.typ_name)
  ) out_typ

and emit_event_time oc opc =
  let (sta_field, sta_src, sta_scale), dur =
    RamenOperation.event_time_of_operation opc.op |> Option.get in
  let open RamenEventTime in
  let field_value_to_float src oc field_name =
    match src with
    | OutputField ->
        (* This must not fail if RamenOperation.check did its job *)
        let f = List.find (fun t -> t.typ_name = field_name) opc.tuple_typ in
        Printf.fprintf oc
          (if f.typ.nullable then "((%a) %s |! 0.)" else "(%a) %s")
          (conv_from_to ~nullable:f.typ.nullable)
          (f.typ.structure, TFloat)
          (id_of_field_name ~tuple:TupleOut field_name)
    | Parameter ->
        let param = RamenTuple.params_find field_name opc.params in
        Printf.fprintf oc "(%a %s_%s_)"
          (conv_from_to ~nullable:false) (param.ptyp.typ.structure, TFloat)
          (id_of_prefix TupleParam) field_name
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

(* state is just the name of the state that's "opened" in the local environment,
 * ie "global_" if we are initializing the global state or "local_" if we are
 * initializing the group state or nothing (empty string) if we are not initializing
 * anything and state fields must be accessed via the actual state record.
 * It is used by stateful functions when they need to access their state. *)
and emit_expr_ ?state ~context ~opc oc expr =
  let open RamenExpr in
  let out_typ = typ_of expr in
  let nullable = (Option.get out_typ.typ).nullable in
  (* my_state will represent the state of a stateful function with a special
   * field (StateField), which type will match that of the finalized value.
   * With this additional awful hack that we sometime want a different
   * nullability, for when the stateful function handles nulls itself: *)
  let my_state ?nullable lifespan =
    let state_name =
      match lifespan with LocalState -> "group_"
                        | GlobalState -> "global_" in
    StateField ((match nullable with None -> out_typ
                | Some n ->
                    { out_typ with typ = Some {
                        structure = (Option.get out_typ.typ).structure ;
                        nullable = n } }),
                (if state = Some lifespan then "" else state_name ^".") ^
                name_of_state expr)
  in
  match context, expr, (Option.get out_typ.typ).structure with
  (* Non-functions *)
  | Finalize, StateField (_, s), _ ->
    Printf.fprintf oc "%s" s
  | _, Const (_, VNull), _ ->
    assert nullable ;
    Printf.fprintf oc "Null"
  | _, Const (t, c), _ ->
    Printf.fprintf oc "%s(%a %a)"
      (if nullable then "NotNull " else "")
      (conv_from_to ~nullable:false) (structure_of c,
                                      (Option.get t.typ).structure)
      emit_type c
  | Finalize, Tuple (_, es), _ ->
    list_print_as_tuple (emit_expr ?state ~context ~opc) oc es
  | Finalize, Vector (_, es), _ ->
    list_print_as_vector (emit_expr ?state ~context ~opc) oc es

  | Finalize, Field (_, tuple, field), _ ->
    (match !tuple with
    | TupleOutPrevious ->
      Printf.fprintf oc "(maybe_%s_ out_previous_opt_)" field
    | TupleEnv ->
      Printf.fprintf oc "(Sys.getenv_opt %S |> nullable_of_option)" field
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
              "match %a with Null as n_ -> n_ \
               | NotNull cond_ -> if cond_ then %s(%a)"
            else
              "if %a then %s(%a)")
           (emit_expr ?state ~context ~opc) alt.case_cond
           (if nullable && not (is_nullable alt.case_cons)
            then "NotNull " else "")
           (conv_to ?state ~context ~opc (Some t)) alt.case_cons)
      oc alts ;
    (match else_ with
    | None ->
      (* If there is no ELSE clause then the expr is nullable: *)
      assert nullable ;
      Printf.fprintf oc " else Null)"
    | Some else_ ->
      Printf.fprintf oc " else %s(%a))"
        (if nullable && not (is_nullable else_)
         then "NotNull " else "")
        (conv_to ?state ~context ~opc (Some t)) else_)
  | Finalize, Coalesce (_, es), t ->
    let rec loop = function
      | [] -> ()
      | [last] ->
        Printf.fprintf oc "(%a)" (conv_to ?state ~context ~opc (Some t)) last
      | e :: rest ->
        Printf.fprintf oc "(default_delayed (fun () -> " ;
        loop rest ;
        Printf.fprintf oc ") (%a))" (conv_to ?state ~context ~opc (Some t)) e
    in
    loop es
  (* Stateless arithmetic functions which actual funcname depends on operand types: *)
  | Finalize, StatelessFun2 (_, Add, e1, e2),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".add") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Sub, e1, e2),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".sub") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Mul, e1, e2),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".mul") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, IDiv, e1, e2),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".div") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, IDiv, e1, e2), (TFloat as t) ->
    (* Here we must convert everything to float first, then divide and
     * take the floor: *)
    Printf.fprintf oc "(let x_ = " ;
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".div") [Some t; Some t] oc [e1; e2] ;
    Printf.fprintf oc " in if x_ >= 0. then floor x_ else ceil x_)"
  | Finalize, StatelessFun2 (_, Div, e1, e2), (TFloat as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".div") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Reldiff, e1, e2), TFloat ->
    emit_functionN ?state ~opc ~nullable "CodeGenLib.reldiff" [Some TFloat; Some TFloat] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Pow, e1, e2), (TFloat|TI32|TI64 as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".( ** )") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Pow, e1, e2), (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI128 as t) ->
    (* For all others we exponentiate via floats: *)
    Printf.fprintf oc "(%a %a)"
      (conv_from_to ~nullable) (TFloat, t)
      (emit_functionN ?state ~opc ~nullable "( ** )"
        [Some TFloat; Some TFloat])  [e1; e2]

  | Finalize, StatelessFun2 (_, Mod, e1, e2),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".rem") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Mod, e1, e2), (TFloat as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".modulo") [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Strftime, e1, e2), TString ->
    emit_functionN ?state ~opc ~nullable "CodeGenLib.strftime"
      [Some TString; Some TFloat] oc [e1; e2]
  | Finalize, StatelessFun1 (_, Strptime, e), TFloat ->
    emit_functionN ?state ~opc ~nullable ~impl_return_nullable:true
      "(fun t_ -> RamenHelpers.time_of_abstime t_)"
        [Some TString] oc [e]

  | Finalize, StatelessFun1 (_, Abs, e),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".abs") [Some t] oc [e]
  | Finalize, StatelessFun1 (_, Minus, e),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^".neg") [Some t] oc [e]
  | Finalize, StatelessFun1 (_, Exp, e), TFloat ->
    emit_functionN ?state ~opc ~nullable "exp" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Log, e), TFloat ->
    emit_functionN ?state ~opc ~nullable "log" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Log10, e), TFloat ->
    emit_functionN ?state ~opc ~nullable "log10" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Sqrt, e), TFloat ->
    emit_functionN ?state ~opc ~nullable "sqrt" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Ceil, e), TFloat ->
    emit_functionN ?state ~opc ~nullable "ceil" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Floor, e), TFloat ->
    emit_functionN ?state ~opc ~nullable "floor" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Round, e), TFloat ->
    emit_functionN ?state ~opc ~nullable "Float.round" [Some TFloat] oc [e]
  | Finalize, StatelessFun1 (_, Hash, e), TI64 ->
    emit_functionN ?state ~opc ~nullable "CodeGenLib.hash" [None] oc [e]
  | Finalize, StatelessFun1 (_, Sparkline, e), TString ->
    emit_functionN ?state ~opc ~nullable "RamenHelpers.sparkline"
      [Some (TVec (0, { structure = TFloat ; nullable = false }))] oc [e]
  | Finalize, StatelessFun1 (_, BeginOfRange, e), TIpv4 ->
    emit_functionN ?state ~opc ~nullable "RamenIpv4.Cidr.first" [Some TCidrv4] oc [e]
  | Finalize, StatelessFun1 (_, BeginOfRange, e), TIpv6 ->
    emit_functionN ?state ~opc ~nullable "RamenIpv6.Cidr.first" [Some TCidrv6] oc [e]
  | Finalize, StatelessFun1 (_, BeginOfRange, e), TIp ->
    emit_functionN ?state ~opc ~nullable "RamenIp.first" [Some TCidr] oc [e]
  | Finalize, StatelessFun1 (_, EndOfRange, e), TIpv4 ->
    emit_functionN ?state ~opc ~nullable "RamenIpv4.Cidr.last" [Some TCidrv4] oc [e]
  | Finalize, StatelessFun1 (_, EndOfRange, e), TIpv6 ->
    emit_functionN ?state ~opc ~nullable "RamenIpv6.Cidr.last" [Some TCidrv6] oc [e]
  | Finalize, StatelessFun1 (_, EndOfRange, e), TIp ->
    emit_functionN ?state ~opc ~nullable "RamenIp.last" [Some TCidr] oc [e]

  (* Stateless functions manipulating constructed types: *)
  | Finalize, StatelessFun1 (_, Nth n, es), _ ->
    (match (Option.get (typ_of es).typ).structure with
    | TTuple ts ->
        let num_items = Array.length ts in
        let rec loop_t str i =
          if i >= num_items then str else
          let str = str ^ (if i > 0 then "," else "")
                        ^ (if i = n then "x_" else "_") in
          loop_t str (i + 1) in
        let nth_func = loop_t "(fun (" 0 ^") -> x_)" in
        (* emit_funcN will take care of nullability of es: *)
        emit_functionN ?state ~opc ~nullable nth_func [None] oc [es]
    | TVec (dim, t) ->
        assert (n < dim) ;
        let nth_func = "(fun a_ -> Array.get a_ "^ string_of_int n ^")" in
        emit_functionN ?state ~opc ~nullable nth_func [None] oc [es]
    | _ -> assert false)
  | Finalize, StatelessFun2 (_, VecGet, n, es), _ ->
    let func = "(fun a_ n_ -> Array.get a_ (Int32.to_int n_))" in
    emit_functionN ?state ~opc ~nullable func [None; Some TI32] oc [es; n]

  (* Other stateless functions *)
  | Finalize, StatelessFun2 (_, Ge, e1, e2), TBool ->
    emit_functionN ?state ~opc ~nullable "(>=)" [Some TAny; Some TAny] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Gt, e1, e2), TBool ->
    emit_functionN ?state ~opc ~nullable "(>)" [Some TAny; Some TAny] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Eq, e1, e2), TBool ->
    emit_functionN ?state ~opc ~nullable "(=)" [Some TAny; Some TAny] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Concat, e1, e2), TString ->
    emit_functionN ?state ~opc ~nullable "(^)" [Some TString; Some TString] oc [e1; e2]
  | Finalize, StatelessFun2 (_, StartsWith, e1, e2), TBool ->
    emit_functionN ?state ~opc ~nullable "String.starts_with" [Some TString; Some TString] oc [e1; e2]
  | Finalize, StatelessFun2 (_, EndsWith, e1, e2), TBool ->
    emit_functionN ?state ~opc ~nullable "String.ends_with" [Some TString; Some TString] oc [e1; e2]
  | Finalize, StatelessFunMisc (_, Like (e, p)), TBool ->
    let pattern = Globs.compile ~star:'%' ~escape:'\\' p in
    Printf.fprintf oc "(let pattern_ = \
      Globs.{ anchored_start = %b ; anchored_end = %b ; chunks = %a } in "
      pattern.anchored_start pattern.anchored_end
      (List.print (fun oc s -> Printf.fprintf oc "%S" s)) pattern.chunks ;
    emit_functionN ?state ~opc ~nullable "Globs.matches pattern_ " [Some TString] oc [e];
    Printf.fprintf oc ")"
  | Finalize, StatelessFun1 (_, Length, e), TU32 when is_a_string e ->
    emit_functionN ?state ~opc ~nullable "(Uint32.of_int % String.length)" [Some TString] oc [e]
  | Finalize, StatelessFun1 (_, Length, e), TU32 when is_a_list e ->
    emit_functionN ?state ~opc ~nullable "(Uint32.of_int % Array.length)" [None] oc [e]
  (* lowercase and uppercase assume latin1 and will gladly destroy UTF-8
   * encoded char, therefore we use the ascii variants: *)
  | Finalize, StatelessFun1 (_, Lower, e), TString ->
    emit_functionN ?state ~opc ~nullable "String.lowercase_ascii" [Some TString] oc [e]
  | Finalize, StatelessFun1 (_, Upper, e), TString ->
    emit_functionN ?state ~opc ~nullable "String.uppercase_ascii" [Some TString] oc [e]
  | Finalize, StatelessFun2 (_, And, e1, e2), TBool ->
    emit_functionN ?state ~opc ~nullable "(&&)" [Some TBool; Some TBool] oc [e1; e2]
  | Finalize, StatelessFun2 (_, Or, e1,e2), TBool ->
    emit_functionN ?state ~opc ~nullable "(||)" [Some TBool; Some TBool] oc [e1; e2]
  | Finalize, StatelessFun2 (_, (BitAnd|BitOr|BitXor as op), e1, e2),
    (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    let n = match op with BitAnd -> "logand" | BitOr -> "logor"
                        | _ -> "logxor" in
    emit_functionN ?state ~opc ~nullable (omod_of_type t ^"."^ n) [Some t; Some t] oc [e1; e2]
  | Finalize, StatelessFun1 (_, Not, e), TBool ->
    emit_functionN ?state ~opc ~nullable "not" [Some TBool] oc [e]
  | Finalize, StatelessFun1 (_, Defined, e), TBool ->
    (* Do not call emit_functionN to avoid null propagation: *)
    Printf.fprintf oc "(match %a with Null -> false | _ -> true)"
      (emit_expr ?state ~context ~opc) e
  | Finalize, StatelessFun1 (_, Age, e),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as to_typ)
  | Finalize, StatelessFun1 (_, BeginOfRange, e),
    (TCidrv4 | TCidrv6 as to_typ) ->
    let in_type_name =
      String.lowercase (IO.to_string print_structure to_typ) in
    let name = "CodeGenLib.age_"^ in_type_name in
    emit_functionN ?state ~opc ~nullable name [Some to_typ] oc [e]
  (* TODO: Now() for Uint62? *)
  | Finalize, StatelessFun0 (_, Now), TFloat ->
    String.print oc "!CodeGenLib_IO.now"
  | Finalize, StatelessFun0 (_, Random), TFloat ->
    String.print oc "(Random.float 1.)"
  | Finalize, StatelessFun0 (_, EventStart), TFloat ->
    Printf.fprintf oc "((%a) |> fst)" emit_event_time opc
  | Finalize, StatelessFun0 (_, EventStop), TFloat ->
    Printf.fprintf oc "((%a) |> snd)" emit_event_time opc
  | Finalize, StatelessFun1 (_, Cast _, Const (_, VNull)), _ ->
    (* Special case when casting NULL to anything: that must work whatever the
     * destination type, even if we have no converter from the type of NULL.
     * This is important because literal NULL type is random. *)
    Printf.fprintf oc "Null"
  | Finalize, StatelessFun1 (_, Cast to_typ, e), _ ->
    let from = Option.get (typ_of e).typ in
    (* Shall we force a non-nullable argument to become nullable, or
     * propagates nullability from the argument? *)
    let add_nullable = not from.nullable && to_typ.nullable in
    if add_nullable then Printf.fprintf oc "NotNull (" ;
    Printf.fprintf oc "(%a) (%a)"
      (conv_from_to ~nullable:from.nullable)
        (from.structure, to_typ.structure)
      (emit_expr ?state ~context ~opc) e ;
    if add_nullable then Printf.fprintf oc ")"

  | Finalize, StatelessFunMisc (_, Max es), t ->
    emit_functionN ~opc ~args_as:(Array 0) ?state ~nullable
      "Array.max" (List.map (fun _ -> Some t) es) oc es
  | Finalize, StatelessFunMisc (_, Min es), t ->
    emit_functionN ~opc ~args_as:(Array 0) ?state ~nullable
      "Array.min" (List.map (fun _ -> Some t) es) oc es
  | Finalize, StatelessFunMisc (_, Print es), _ ->
    (* We want to print nulls as well, so we make all parameters optional
     * strings: *)
    (match es with
    | [] -> ()
    | e::es ->
        let e_structure = (Option.get (typ_of e).typ).structure in
        Printf.fprintf oc
          "(let x0_ = %a in CodeGenLib.print (%s(%a x0_)::%a) ; x0_)"
          (emit_expr ?state ~context ~opc) e
          (if is_nullable e then "" else "NotNull ")
          (conv_from_to ~nullable:(is_nullable e)) (e_structure, TString)
          (List.print (fun oc e ->
             Printf.fprintf oc "%s(%a)"
               (if is_nullable e then "" else "NotNull ")
               (conv_to ?state ~context ~opc (Some TString)) e)) es)
  (* IN can have many meanings: *)
  | Finalize, StatelessFun2 (_, In, e1, e2), TBool ->
    (match (Option.get (typ_of e1).typ).structure,
           (Option.get (typ_of e2).typ).structure with
    | TIpv4, TCidrv4 ->
      emit_functionN ?state ~opc ~nullable "RamenIpv4.Cidr.is_in"
        [Some TIpv4; Some TCidrv4] oc [e1; e2]
    | TIpv6, TCidrv6 ->
      emit_functionN ?state ~opc ~nullable "RamenIpv6.Cidr.is_in"
        [Some TIpv6; Some TCidrv6] oc [e1; e2]
    | (TIpv4|TIpv6|TIp), (TCidrv4|TCidrv6|TCidr) ->
      emit_functionN ?state ~opc ~nullable "RamenIp.is_in"
        [Some TIp; Some TCidr] oc [e1; e2]
    | TString, TString ->
      emit_functionN ?state ~opc ~nullable "String.exists"
        [Some TString; Some TString] oc [e2; e1]
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
         * the xs in case A is found in the set, but do if it is not. If A is
         * NULL though, then the result is unless the set is empty: *)
        if is_nullable e1 then
          (* Even if e1 is null, we can answer the operation if e2 is
           * empty: *)
          Printf.fprintf oc "(match %a with Null -> \
                               if %s = 0 && %s then NotNull true else Null \
                             | NotNull in0_ -> "
            (conv_to ?state ~context ~opc (Some larger_t)) e1
            csts_len (string_of_bool (non_csts = []))
        else
          Printf.fprintf oc "(let in0_ = %a in "
            (conv_to ?state ~context ~opc (Some larger_t)) e1 ;
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
            "const_in_"^ string_of_int (typ_of expr).uniq_num ^"_" in
          Printf.fprintf opc.consts
            "let %s =\n\
             \tlet h_ = Hashtbl.create (%s) in\n\
             \t%s ;\n\
             \th_\n"
            hash_id csts_len (csts_hash_init larger_t) ;
          Printf.fprintf oc "if Hashtbl.mem %s in0_ then %strue else "
            hash_id (if nullable then "NotNull " else "")) ;
        (* Then check each non-const in turn: *)
        let had_nullable =
          List.fold_left (fun had_nullable e ->
            if is_nullable e (* not possible ATM *) then (
              Printf.fprintf oc
                "if (match %a with Null -> _ret_ := Null ; false \
                 | NotNull in1_ -> in0_ = in1_) then true else "
                (conv_to ?state ~context ~opc (Some larger_t)) e ;
              true
            ) else (
              Printf.fprintf oc "if in0_ = %a then %strue else "
                (conv_to ?state ~context ~opc (Some larger_t)) e
                (if nullable then "NotNull " else "") ;
              had_nullable)
          ) false non_csts in
        Printf.fprintf oc "%s)"
          (if had_nullable then "!_ret_" else
           if nullable then "NotNull false" else "false")
      in
      (match e2 with
      | Vector (_, es) ->
        let csts, non_csts =
          (* TODO: leave the IFs when we know the compiler will optimize them
           * away:
          if is_const e1 then [], es else*)
          List.partition is_const es in
        let csts, non_csts =
          if List.length csts >= 6 (* guessed *) then csts, non_csts
          else [], csts @ non_csts in
        let csts_len = List.length csts |> string_of_int
        and csts_hash_init larger_t =
          IO.to_string (List.print ~first:"" ~last:"" ~sep:" ;\n\t" (fun cc e ->
            Printf.fprintf cc "Hashtbl.replace h_ (%a) ()"
              (conv_to ?state ~context ~opc (Some larger_t)) e)) csts in
        emit_in csts_len csts_hash_init non_csts
      | Field ({ typ = Some { structure = (TVec (_, telem) | TList telem) ; _ } ; _ }, _, _) ->
        let csts_len =
          Printf.sprintf2 "Array.length (%a)"
            (emit_expr ?state ~context ~opc) e2
        and csts_hash_init larger_t =
          Printf.sprintf2
            "Array.iter (fun e_ -> Hashtbl.replace h_ ((%a) e_) ()) (%a)"
            (conv_from_to ~nullable:telem.nullable)
              (telem.structure, larger_t)
            (emit_expr ?state ~context ~opc) e2 in
        emit_in csts_len csts_hash_init []
      | _ -> assert false)
    | _ -> assert false)

  | Finalize, StatelessFun2 (_, Percentile, p, lst), _ ->
    emit_functionN ?state ~opc ~nullable "CodeGenLib.percentile"
      [Some TFloat; None] oc [p; lst]

  (* Stateful functions *)
  | InitState, StatefulFun (_, g, _, AggrAnd _), (TBool as t) ->
    wrap_nullable ~nullable oc
      (Printf.sprintf2 "%a true"
        (conv_from_to ~nullable:false) (TBool, t))
  | UpdateState, StatefulFun (_, g, n, AggrAnd e), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "(&&)" oc [ Some TBool ]
  | Finalize, StatefulFun (_, g, n, AggrAnd e), TBool ->
    finalize_state ?state ~opc ~nullable n (my_state g) "identity" [] oc []
  | InitState, StatefulFun (_, g, _, AggrOr _), (TBool as t) ->
    wrap_nullable ~nullable oc
      (Printf.sprintf2 "%a false"
        (conv_from_to ~nullable:false) (TBool, t))
  | UpdateState, StatefulFun (_, g, n, AggrOr e), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "(||)" oc [ Some TBool ]
  | Finalize, StatefulFun (_, g, n, AggrOr e), TBool ->
    finalize_state ?state ~opc ~nullable n (my_state g) "identity" [] oc []

  | InitState, StatefulFun (_, g, _, AggrSum _),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    wrap_nullable ~nullable oc
      (Printf.sprintf2 "%a Uint8.zero"
        (conv_from_to ~nullable:false) (TU8, t))
  | UpdateState, StatefulFun (_, g, n, AggrSum e),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      (omod_of_type t ^".add") oc [ Some t ]
  | Finalize, StatefulFun (_, g, n, AggrSum _), _ ->
    finalize_state ?state ~opc ~nullable n (my_state g) "identity" [] oc []

  | InitState, StatefulFun (_, g, _, AggrAvg e), TFloat ->
    wrap_nullable ~nullable oc "CodeGenLib.avg_init"
  | UpdateState, StatefulFun (_, g, n, AggrAvg e), (TFloat as t) ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.avg_add" oc [ Some t ]
  | Finalize, StatefulFun (_, g, n, AggrAvg e), _ ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.avg_finalize" [] oc []

  | InitState, StatefulFun (_, g, n, (AggrFirst e|AggrLast e|AggrMax e|AggrMin e)), _ ->
    wrap_nullable ~nullable oc "None"
  | UpdateState, StatefulFun (_, g, n, AggrMax e), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.aggr_max" oc [ None ]
  | UpdateState, StatefulFun (_, g, n, AggrMin e), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.aggr_min" oc [ None ]
  | UpdateState, StatefulFun (_, g, n, AggrFirst e), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.aggr_first" oc [ None ]
  | UpdateState, StatefulFun (_, g, n, AggrLast e), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.aggr_last" oc [ None ]
  | Finalize, StatefulFun (_, g, n, (AggrFirst _|AggrLast _|AggrMax _|AggrMin _)), _ ->
    finalize_state ?state ~opc ~nullable n (my_state g) "Option.get" [] oc []

  (* Histograms: bucket each float into the array of num_buckets + 2 and then
   * count number of entries per buckets. The 2 extra buckets are for "<min"
   * and ">max". *)
  | InitState, StatefulFun (_, g, _, AggrHistogram (e, min, max, num_buckets)), _ ->
    wrap_nullable ~nullable oc
      (Printf.sprintf "CodeGenLib.Histogram.init %s %s %d"
        (Legacy.Printf.sprintf "%h" min)
        (Legacy.Printf.sprintf "%h" max)
        num_buckets)
  | UpdateState, StatefulFun (_, g, n, AggrHistogram (e, min, max, num_buckets)), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.Histogram.add" oc [ Some TFloat ]
  | Finalize, StatefulFun (_, g, n, AggrHistogram _), TVec _ ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Histogram.finalize" [] oc []

  | InitState, StatefulFun (_, g, _, Lag (k, e)), _ ->
    emit_functionN ?state ~opc ~nullable "CodeGenLib.Seasonal.init"
      [Some TU32; Some TU32; None] oc
      [k; expr_one (); any_constant_of_expr_type (Option.get (typ_of e).typ)]
  | UpdateState, StatefulFun (_, g, n, Lag (_k,e)), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.Seasonal.add" oc [ None ]
  | Finalize, StatefulFun (_, g, n, Lag _), _ ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Seasonal.lag" [] oc []

  (* We force the inputs to be float since we are going to return a float anyway. *)
  | InitState, StatefulFun (_, g, _, (MovingAvg(p,n,_)|LinReg(p,n,_))), TFloat ->
    emit_functionN ?state ~opc ~nullable "CodeGenLib.Seasonal.init"
      [Some TU32; Some TU32; Some TFloat] oc [p; n; expr_zero ()]
  | UpdateState, StatefulFun (_, g, n, (MovingAvg(_p,_n,e)|LinReg(_p,_n,e))), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.Seasonal.add" oc [ Some TFloat ]
  | Finalize, StatefulFun (_, g, n, MovingAvg (p, m,_)), TFloat ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Seasonal.avg" [p; m] oc [Some TU32; Some TU32]
  | Finalize, StatefulFun (_, g, n, LinReg (p, m, _)), TFloat ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Seasonal.linreg" [p; m] oc [Some TU32; Some TU32]
  | Finalize, StatefulFun (_, g, n, MultiLinReg (p, m,_ ,_)), TFloat ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Seasonal.multi_linreg" [p; m] oc [Some TU32; Some TU32]

  | InitState, StatefulFun (_, g, _, MultiLinReg (p, m, _, es)), TFloat ->
    emit_functionNv ?state ~opc ~nullable "CodeGenLib.Seasonal.init_multi_linreg"
      [Some TU32; Some TU32; Some TFloat]
      [p; m; expr_zero ()]
      (Some TFloat) oc (List.map (fun _ -> expr_zero ()) es)
  | UpdateState, StatefulFun (_, g, n, MultiLinReg (_p , _m, e, es)), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      ~vars:es ~vars_to_typ:(Some TFloat)
      "CodeGenLib.Seasonal.add_multi_linreg" oc [ Some TFloat ]

  | InitState, StatefulFun (_, g, _, ExpSmooth (_a,_)), (TFloat as t) ->
    wrap_nullable ~nullable oc
      (Printf.sprintf2 "%a Uint8.zero"
        (conv_from_to ~nullable:false) (TU8, t))
  | UpdateState, StatefulFun (_, g, n, ExpSmooth (a,e)), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ a ; e ]
      "CodeGenLib.smooth" oc [ Some TFloat; Some TFloat ]
  | Finalize, StatefulFun (_, g, n, ExpSmooth _), TFloat ->
    finalize_state ?state ~opc ~nullable n (my_state g) "identity" [] oc []

  | InitState, StatefulFun (_, g, _, Remember (fpr,_tim,dur,_es)), TBool ->
    emit_functionN ?state ~opc ~nullable "CodeGenLib.Remember.init"
      [Some TFloat; Some TFloat] oc [fpr; dur]
  | UpdateState, StatefulFun (_, g, n, Remember (_fpr,tim,_dur,es)), _ ->
    update_state ?state ~opc ~nullable n (my_state g) (tim :: es)
      ~args_as:(Tuple 2) "CodeGenLib.Remember.add" oc
      (Some TFloat :: List.map (fun _ -> None) es)
  | Finalize, StatefulFun (_, g, n, Remember _), TBool ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Remember.finalize" [] oc []

  | InitState, StatefulFun (_, g, _, Distinct _es), _ ->
    wrap_nullable ~nullable oc "CodeGenLib.Distinct.init ()"
  | UpdateState, StatefulFun (_, g, n, Distinct es), _ ->
    update_state ?state ~opc ~nullable n (my_state g) es
      ~args_as:(Tuple 1) "CodeGenLib.Distinct.add" oc
      (List.map (fun e -> None) es)
  | Finalize, StatefulFun (_, g, n, Distinct es), TBool ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Distinct.finalize" [] oc []

  | InitState, StatefulFun (_, g, _, Hysteresis _), t ->
    wrap_nullable ~nullable oc
      (Printf.sprintf2 "%a true" (* Initially within bounds *)
        (conv_from_to ~nullable:false) (TBool, t))
  | UpdateState, StatefulFun (_, g, n, Hysteresis (meas, accept, max)), TBool ->
    (* TODO: shouldn't we promote everything to the most accurate of those types? *)
    let t = (Option.get (typ_of meas).typ).structure in
    update_state ?state ~opc ~nullable n (my_state g) [ meas ; accept ; max ]
      "CodeGenLib.Hysteresis.add " oc [Some t; Some t; Some t]
  | Finalize, StatefulFun (_, g, n, Hysteresis _), TBool ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Hysteresis.finalize" [] oc []

  | InitState, StatefulFun (_, g, _, Top { c ; duration ; _ }), _ ->
    wrap_nullable ~nullable oc
      (Printf.sprintf2 "CodeGenLib.Top.init (%a) (%a)"
        (* c can be any numeric type but heavy_hitters_init expects a u32: *)
        (conv_to ?state ~context:Finalize ~opc (Some TU32)) c
        (* duration can also be a parameter compatible to float: *)
        (conv_to ?state ~context:Finalize ~opc (Some TFloat)) duration)
  | UpdateState, StatefulFun (_, g, n, Top { what ; by ; time ; _ }), _ ->
    update_state ?state ~opc ~nullable n (my_state g) (time :: by :: what)
      ~args_as:(Tuple 3) "CodeGenLib.Top.add" oc
      (Some TFloat :: Some TFloat :: List.map (fun _ -> None) what)
  | Finalize, StatefulFun (_, g, n, Top { want_rank = true ; c ; what ; _ }), t ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      ~impl_return_nullable:true ~args_as:(Tuple 1)
      ("(fun s_ n_ x_ -> \
           CodeGenLib.Top.rank s_ n_ x_ |> \
           Option.map "^ omod_of_type t ^".of_int)")
      (c :: what) oc (Some TU32 :: List.map (fun _ -> None) what)
  | Finalize, StatefulFun (_, g, n, Top { want_rank = false ; c ; what ; _ }), _ ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      ~args_as:(Tuple 2)
      "CodeGenLib.Top.is_in_top"
      (c :: what) oc (Some TU32 :: List.map (fun _ -> None) what)

  | InitState, StatefulFun (_, g, _, Last (c, e, _)), _ ->
    let t = (Option.get (typ_of c).typ).structure in
    wrap_nullable ~nullable oc
      (Printf.sprintf2 "CodeGenLib.Last.init (%a %a)"
        (conv_from_to ~nullable:false) (t, TU32)
        (emit_expr ?state ~context:Finalize ~opc) c)
  (* Special updater that use the internal count when no `by` expressions
   * are present: *)
  | UpdateState, StatefulFun (_, g, n, Last (_, e, [])), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.Last.add_on_count" oc [ None ]
  | UpdateState, StatefulFun (_, g, n, Last (_, e, es)), _ ->
    update_state ?state ~opc ~nullable n (my_state g) (e :: es)
      ~args_as:(Tuple 2) "CodeGenLib.Last.add" oc
      (None :: List.map (fun _ -> None) es)
  | Finalize, StatefulFun (_, g, n, Last (_, _, _)), _ ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      ~impl_return_nullable:true
      "CodeGenLib.Last.finalize" [] oc []

  | InitState, StatefulFun (_, g, n, Sample (c, e)), _ ->
    let t = (Option.get (typ_of c).typ).structure in
    let init_c =
      let c_typ = Option.get (typ_of e).typ in
      let c_typ = if n then { c_typ with nullable = false } else c_typ in
      any_constant_of_expr_type c_typ in
    wrap_nullable ~nullable oc
      (Printf.sprintf2 "RamenSampling.init (%a %a) (%a)"
        (conv_from_to ~nullable:false) (t, TU32)
        (emit_expr ?state ~context:Finalize ~opc) c
        (emit_expr ?state ~context:Finalize ~opc) init_c)
  | UpdateState, StatefulFun (_, g, n, Sample (_, e)), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "RamenSampling.add" oc [ None ]
  | Finalize, StatefulFun (_, g, n, Sample (_, _)), _ ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "RamenSampling.finalize" [] oc []

  (* Grouping operation: accumulate all values in a list, that we initialize
   * empty. At finalization, an empty list means we skipped all values ;
   * and we return Null in that case. Note that since this is an aggregate
   * function, there is no way ever to commit or use (finalize) a function
   * before it's been sent at least one value. *)
  | InitState, StatefulFun (_, g, _, Group e), _ ->
    wrap_nullable ~nullable oc "[]"
  | UpdateState, StatefulFun (_, g, n, Group e), _ ->
    update_state ?state ~opc ~nullable n (my_state g) [ e ]
      "CodeGenLib.Group.add" oc [ None ]
  | Finalize, StatefulFun (_, g, n, Group _), _ ->
    finalize_state ?state ~opc ~nullable n (my_state g)
      "CodeGenLib.Group.finalize" [] oc []

  (* Generator: the function appears only during tuple generation, where
   * it sends the output to its continuation as (freevar_name t).
   * In normal expressions we merely refer to that free variable. *)
  | Generator, GeneratorFun (_, Split (e1,e2)), TString ->
    emit_functionN ?state ~opc ~nullable "CodeGenLib.split" [Some TString; Some TString] oc [e1; e2]
  | Finalize, GeneratorFun (t, Split (_e1,_e2)), TString -> (* Output it as a free variable *)
    String.print oc (freevar_name t)

  | _, _, _ ->
    let m =
      Printf.sprintf "Cannot find implementation of %s for context %s"
        (IO.to_string (print true) expr)
        (string_of_context context) in
    failwith m

and emit_expr ?state ~context ~opc oc expr =
  try emit_expr_ ?state ~context ~opc oc expr
  with Error _ as e -> raise e
     | e -> raise (Error (expr, context, Printexc.to_string e))

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
    | Some t1, Some t2 -> Some (large_enough_for t1 t2) in
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
      merge_types any_type (Some (Option.get (typ_of e).typ).structure) in
    loop (t::ht) t any_type n (es, ts)
  | e::es, [] -> (* Missing some types: update rt *)
    let te = Some (Option.get (typ_of e).typ).structure in
    if rt = Some TAny then
      loop ht rt (merge_types any_type te) (n+1) (es, [])
    else
      loop ht (merge_types rt te) any_type (n+1) (es, [])
  in
  loop [] None None 0 (es, arg_typs)

(*$inject
  open Batteries
  open Stdint
  open RamenTypes
  let const structure v =
    let typ = RamenTypes.{ nullable = false ; structure } in
    let t = RamenExpr.make_typ ~typ "test" in
    RamenLang.(RamenExpr.(Const (t, v)))
 *)
(*$= add_missing_types & ~printer:(IO.to_string (List.print (Option.print print_structure)))
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
      (* Set to true if [impl] already returns an optional value: *)
      ?(impl_return_nullable=false)
      (* Nullability of the result: *)
      ~nullable
      ?(args_as=Arg) ?state ~opc impl arg_typs es oc vt_specs_opt =
  let open RamenExpr in
  let arg_typs = add_missing_types arg_typs es in
  let len, has_nullable =
    List.fold_left2 (fun (i, had_nullable) e arg_typ ->
        if is_nullable e then (
          Printf.fprintf oc
            "(match %a with Null as n_ -> n_ | NotNull x%d_ -> "
            (conv_to ?state ~context:Finalize ~opc arg_typ) e
            i ;
          i + 1, true
        ) else (
          Printf.fprintf oc "(let x%d_ = %a in\n\t"
            i
            (conv_to ?state ~context:Finalize ~opc arg_typ) e ;
          i + 1, had_nullable
        )
      ) (0, false) es arg_typs
  in
  Printf.fprintf oc "%s%s"
    (if impl_return_nullable then "nullable_of_option (" else
     if nullable then "NotNull (" else "")
    impl ;
  for i = 0 to len-1 do
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
  | Array n when n >= len -> Printf.fprintf oc " [| "
  | Tuple n when n >= len -> Printf.fprintf oc " ("
  | _ -> ()) ;
  Printf.fprintf oc "%s"
    (match args_as with Arg -> "" | Array _ -> " |] " | Tuple _ -> ") ") ;
  (* variadic arguments [ves] are passed as a last argument to impl, as an array *)
  Option.may (fun (vt, ves) ->
      (* TODO: handle NULLability *)
      List.print ~first:" [| " ~last:" |]" ~sep:"; "
                 (conv_to ?state ~context:Finalize ~opc vt) oc ves)
    vt_specs_opt ;
  for _i = 1 to len do Printf.fprintf oc ")" done ;
  if nullable || impl_return_nullable then Printf.fprintf oc ")"

and emit_functionN ?args_as ?impl_return_nullable ~nullable ?state ~opc
                   impl arg_typs oc es =
  emit_function ?args_as ?impl_return_nullable ~nullable ?state ~opc
                impl arg_typs es oc None

and emit_functionNv ?impl_return_nullable ~nullable ?state ~opc
                    impl arg_typs es vt oc ves =
  emit_function ?impl_return_nullable ~nullable ?state ~opc
                impl arg_typs es oc (Some (vt, ves))

let emit_compute_nullmask_size oc ser_typ =
  Printf.fprintf oc "\tlet nullmask_bytes_ =\n" ;
  Printf.fprintf oc "\t\tList.fold_left2 (fun s nullable keep ->\n" ;
  Printf.fprintf oc "\t\t\tif nullable && keep then s+1 else s) 0\n" ;
  Printf.fprintf oc "\t\t\t%a\n"
    (List.print (fun oc field -> Bool.print oc field.typ.nullable))
      ser_typ ;
  Printf.fprintf oc "\t\t\tskiplist_ |>\n" ;
  Printf.fprintf oc "\t\tRingBuf.bytes_for_bits |>\n" ;
  Printf.fprintf oc "\t\tRingBuf.round_up_to_rb_word in\n"

let rec emit_sersize_of_var typ nullable oc var =
  if nullable then (
    Printf.fprintf oc
      "\t\t\t(match %s with Null -> 0 | NotNull %s ->\n\
       \t%a)"
      var var
      (emit_sersize_of_var typ false) var
  ) else match typ with
  | TTuple ts ->
      let nullmask_sz = RingBufLib.nullmask_sz_of_tuple ts in
      Printf.fprintf oc "\t\t\t(let %a = %s in\n"
        (array_print_as_tuple_i (fun oc i _ ->
          let item_var = var ^"_"^ string_of_int i in
          String.print oc item_var)) ts
        var ;
      Array.iteri (fun i t ->
        let item_var = var ^"_"^ string_of_int i in
        Printf.fprintf oc "%a + "
          (emit_sersize_of_var t.structure t.nullable) item_var
      ) ts ;
      Printf.fprintf oc "%d)" nullmask_sz
  | TVec (d, t) ->
      let nullmask_sz = RingBufLib.nullmask_sz_of_vector d in
      for i = 0 to d-1 do
        let item_var = var ^"_"^ string_of_int i in
        Printf.fprintf oc "\t\t\t(let %s = %s.(%d) in %a) + "
          item_var var i
          (emit_sersize_of_var t.structure t.nullable) item_var
      done ;
      Int.print oc nullmask_sz
  | TString ->
    Printf.fprintf oc "\t\t\t(RingBufLib.sersize_of_string %s)" var
  | TIp ->
    Printf.fprintf oc "\t\t\t(RingBufLib.sersize_of_ip %s)" var
  | TCidr ->
    Printf.fprintf oc "\t\t\t(RingBufLib.sersize_of_cidr %s)" var
  | TList t ->
    (* So var is the name of an array of some values of type t, which can
     * be a constructed type which sersize can't be known statically.
     * So at first sight we have to generate code that will iter through
     * the values and for each, know or compute its size, etc. This is
     * what we do here, but in cases where t has a well known sersize we
     * could generate much faster code of course: *)
    Printf.fprintf oc "(Array.fold_left (fun s_ v_ -> \
      s_ + %a) 0 %s)"
      (emit_sersize_of_var t.structure t.nullable) "v_"
      var
  | _ -> emit_sersize_of_fixsz_typ oc typ

let emit_sersize_of_tuple name oc tuple_typ =
  (* We want the sersize of the serialized version of course: *)
  let ser_typ = RingBufLib.ser_tuple_typ_of_tuple_typ tuple_typ in
  (* Like for serialize_tuple, we receive first the skiplist and then the
   * actual tuple, so we can compute the nullmask in advance: *)
  Printf.fprintf oc "let %s skiplist_ =\n" name ;
  emit_compute_nullmask_size oc ser_typ ;
  Printf.fprintf oc "\tfun %a ->\n"
    (print_tuple_deconstruct TupleOut) tuple_typ ;
  Printf.fprintf oc "\t\tlet sz_ = nullmask_bytes_ in\n" ;
  List.iter (fun field ->
      let id = id_of_field_typ ~tuple:TupleOut field in
      Printf.fprintf oc "\t\t(* %s *)\n" id ;
      Printf.fprintf oc "\t\tlet sz_ = sz_ + if List.hd skiplist_ then (\n" ;
      emit_sersize_of_var field.typ.structure field.typ.nullable oc id ;
      Printf.fprintf oc "\n\t\t) else 0 in\n" ;
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
      Printf.fprintf oc "\t\t\t\t(match %s with\n" val_var ;
      Printf.fprintf oc "\t\t\t\t| Null -> %s, %s + 1\n" offs_var nulli_var ;
      Printf.fprintf oc "\t\t\t\t| NotNull %s ->\n" val_var ;
      Printf.fprintf oc "\t\t\t\t\tRingBuf.set_bit %s %s ;\n" tx_var nulli_var ;
      Printf.fprintf oc "\t\t\t\t\tlet %s, %s = %a in\n\
                         \t\t\t\t\t%s, %s + 1)\n"
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
        let nullmask_sz = RingBufLib.nullmask_sz_of_tuple ts in
        Printf.fprintf oc "\t\t\tlet %s = %s + %d (* nullmask *) in\n"
          offs_var offs_var nullmask_sz ;
        Array.iteri (fun i t ->
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\t\t\tlet %s, %s = %a in\n"
            offs_var nulli_var
            (emit_write_scalar tx_var offs_var nulli_var item_var t.nullable) t.structure
        ) ts ;
        Printf.fprintf oc "\t\t\t\t%s, %s" offs_var nulli_var
    | TVec (d, t) ->
        let nullmask_sz = RingBufLib.nullmask_sz_of_vector d in
        Printf.fprintf oc "\t\t\tlet %s = %s + %d (* nullmask *) in\n"
          offs_var offs_var nullmask_sz ;
        for i = 0 to d-1 do
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\t\t\tlet %s, %s =\n\
                             \t\t\t\t\tlet %s = %s.(%d) in %a in\n"
            offs_var nulli_var
            item_var val_var i
            (emit_write_scalar tx_var offs_var nulli_var item_var t.nullable) t.structure
        done ;
        Printf.fprintf oc "\t\t\t\t%s, %s" offs_var nulli_var
    (* Scalar types (maybe nullable): *)
    | t ->
        Printf.fprintf oc "\t\t\t\tRingBuf.write_%s %s %s %s ;\n"
          (id_of_typ t) tx_var offs_var val_var ;
        Printf.fprintf oc "\t\t\t\t%s + %a, %s"
          offs_var (emit_sersize_of_var t false) val_var nulli_var ;
        if verbose_serialization then
          Printf.fprintf oc "\t\t\t\t!RamenLog.logger.RamenLog.debug \"Serializing %s (%%s) at offset %%d\" (dump %s) %s ;\n" val_var val_var offs_var
    )
  in
  List.iter (fun field ->
      Printf.fprintf oc "\t\tlet offs_, nulli_ =\n\
                         \t\t\tif List.hd skiplist_ then (\n" ;
      let id = id_of_field_typ ~tuple:TupleOut field in
      emit_write_scalar "tx_" "offs_" "nulli_" id field.typ.nullable oc field.typ.structure ;
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
  let num_fields = List.length tuple_typ in
  List.iteri (fun i field_typ ->
    let sep = if i < num_fields - 1 then "," else "" in
    Printf.fprintf oc "\t\t(try (\n" ;
    if field_typ.typ.nullable then (
      Printf.fprintf oc "\t\t\t(let s_ = strs_.(%d) in\n" i ;
      Printf.fprintf oc "\t\t\tif s_ = %S then Null else NotNull (%a))\n"
        csv_null
        (emit_value_of_string field_typ.typ.structure) "s_"
    ) else (
      let s_var = Printf.sprintf "strs_.(%d)" i in
      Printf.fprintf oc "\t\t\t%a\n"
        (emit_value_of_string field_typ.typ.structure) s_var
    ) ;
    Printf.fprintf oc "\t\t) with exn -> (\n" ;
    Printf.fprintf oc "\t\t\t!RamenLog.logger.RamenLog.error \"Cannot parse field %d: %s\" ;\n" (i+1) field_typ.typ_name ;
    Printf.fprintf oc "\t\t\traise exn))%s\n" sep ;
  ) tuple_typ ;
  Printf.fprintf oc "\t)\n"

let emit_time_of_tuple name oc opc =
  let open RamenEventTime in
  Printf.fprintf oc "let %s %a =\n\t"
    name
    (print_tuple_deconstruct TupleOut) opc.tuple_typ ;
  (match RamenOperation.event_time_of_operation opc.op with
  | None -> Printf.fprintf oc "None"
  | Some _ -> Printf.fprintf oc "Some (%a)" emit_event_time opc) ;
  Printf.fprintf oc "\n\n"

(* Given a tuple type, generate the ReadCSVFile operation. *)
let emit_read_csv_file opc oc name csv_fname unlink
                       csv_separator csv_null preprocessor =
  let const_string_of e =
    Printf.sprintf2 "(%a)"
      (emit_expr ~context:Finalize ~opc ?state:None) e
  in
  let preprocessor =
    let open RamenOperation in
    match preprocessor with
    | None -> "\"\""
    | Some p -> const_string_of p
  and csv_fname = const_string_of csv_fname
  in
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
       \tCodeGenLib_Skeletons.read_csv_file %s %b %S sersize_of_tuple_\n\
       \t\ttime_of_tuple_ serialize_tuple_ tuple_of_strings_ %s\n\
       \t\tfield_of_params_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") opc.tuple_typ
    (emit_time_of_tuple "time_of_tuple_") opc
    (emit_serialize_tuple "serialize_tuple_") opc.tuple_typ
    (emit_tuple_of_strings "tuple_of_strings_" csv_null) opc.tuple_typ
    name
    csv_fname unlink csv_separator preprocessor

let emit_listen_on opc oc name net_addr port proto =
  let open RamenProtocols in
  let tuple_typ = tuple_typ_of_proto proto in
  let collector = collector_of_proto proto in
  Printf.fprintf oc "%a\n%a\n%a\n\
    let %s () =\n\
      \tCodeGenLib_Skeletons.listen_on %s %S %d %S sersize_of_tuple_ time_of_tuple_ serialize_tuple_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_time_of_tuple "time_of_tuple_") opc
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    name
    collector
    (Unix.string_of_inet_addr net_addr) port
    (string_of_proto proto)

let emit_well_known opc oc name from
                    unserializer_name ringbuf_envvar worker_and_time =
  let open RamenProtocols in
  Printf.fprintf oc "%a\n%a\n%a\n\
    let %s () =\n\
      \tCodeGenLib_Skeletons.read_well_known %a sersize_of_tuple_ time_of_tuple_ serialize_tuple_ %s %S %s\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") opc.tuple_typ
    (emit_time_of_tuple "time_of_tuple_") opc
    (emit_serialize_tuple "serialize_tuple_") opc.tuple_typ
    name
    (List.print (fun oc ds ->
      Printf.fprintf oc "%S" (
        IO.to_string RamenOperation.print_data_source ds))) from
   unserializer_name ringbuf_envvar worker_and_time

(* tuple must be some kind of _input_ tuple *)
let emit_in_tuple ?(tuple=TupleIn) mentioned oc in_typ =
  print_tuple_deconstruct tuple oc (List.filter_map (fun field_typ ->
    if Set.mem field_typ.typ_name mentioned then
      Some field_typ else None) in_typ)

(* We do not want to read the value from the RB each time it's used,
 * so extract a tuple from the ring buffer. As an optimisation, read
 * (and return) only the mentioned fields. *)
let emit_read_tuple name mentioned oc in_typ =
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
  let rec emit_read_scalar tx_var offs_var val_var nullable nulli_var oc typ =
    if nullable then (
      Printf.fprintf oc "\
        \t\tif RingBuf.get_bit %s %s then (\n\
        \t\tlet %s, %s =\n\
        \t%a in\n
        \t\tNotNull %s, %s\n
        \t\t) else Null, %s"
        tx_var nulli_var
        val_var offs_var
        (emit_read_scalar tx_var offs_var val_var false nulli_var) typ
        val_var offs_var
        offs_var
    ) else (
    match typ with
    (* Constructed types are prefixed with a nullmask and then read item by
     * item: *)
    | TTuple ts ->
        let nullmask_sz = RingBufLib.nullmask_sz_of_tuple ts in
        Printf.fprintf oc "\t\tlet bi_base_ = %s * 8 and %s = %s + %d in\n"
          offs_var offs_var offs_var nullmask_sz ;
        Array.iteri (fun i t ->
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\tlet bi_ = bi_base_ + %d in\n\
                             \t\tlet %s, %s = %a in\n"
            i
            item_var offs_var
            (emit_read_scalar tx_var offs_var item_var t.nullable "bi_") t.structure
        ) ts ;
        Printf.fprintf oc "\t%a, %s"
          (array_print_as_tuple_i (fun oc i _ ->
            let item_var = val_var ^"_"^ string_of_int i in
            String.print oc item_var)) ts
          offs_var ;
    | TVec (d, t) ->
        let nullmask_sz = RingBufLib.nullmask_sz_of_vector d in
        Printf.fprintf oc "\t\tlet bi_base_ = %s * 8 and %s = %s + %d in\n"
          offs_var offs_var offs_var nullmask_sz ;
        for i = 0 to d-1 do
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\tlet bi_ = bi_base_ + %d in\n\
                             \t\tlet %s, %s = %a in\n"
            i
            item_var offs_var
            (emit_read_scalar tx_var offs_var item_var t.nullable "bi_") t.structure
        done ;
        Printf.fprintf oc "\t[| " ;
        for i = 0 to d-1 do
          Printf.fprintf oc "%s_%d;" val_var i
        done ;
        Printf.fprintf oc " |], %s" offs_var
    (* Non constructed types: *)
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
      if Set.mem field.typ_name mentioned then (
        Printf.fprintf oc "\tlet bi_ = %d in\n\
                           \tlet %s, offs_ =\n%a in\n"
          nulli
          id (emit_read_scalar "tx_" "offs_" id field.typ.nullable "bi_") field.typ.structure
      ) else (
        Printf.printf "Non mentionned field: %s\n%!" field.typ_name ;
        Printf.fprintf oc "\tlet bi_ = %d in\n\
                           \nlet offs_ = offs_ + (%a)\n"
          nulli
          (emit_sersize_of_field_tx "tx_" "offs_" "bi_" field.typ.nullable)
            field.typ.structure
      ) ;
      nulli + (if field.typ.nullable then 1 else 0)
    ) 0 ser_typ in
  (* We want to output the tuple with fields ordered according to the
   * select clause specified order, not according to serialization order: *)
  let in_typ_only_ser =
    List.filter (fun t ->
      not (is_private_field t.RamenTuple.typ_name)
    ) in_typ in
  Printf.fprintf oc "\t%a\n"
    (emit_in_tuple mentioned) in_typ_only_ser

(* We know that somewhere in expr we have one or several generators.
 * First we transform the AST to move the generators to the root,
 * and insert "free variables" (named after the generator uniq_num)
 * where the generator used to stand. Once this is done, the AST
 * start with a chain of generator, and then an expression that is
 * free of generators. We want to emit:
 * (fun k -> gen1 (fun fv1 -> gen2 (fun fv2 -> ... -> genN (fun fvN ->
 *    k (expr ...)))))
 *)
let emit_generator user_fun ~opc oc expr =
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
        (emit_expr ?state:None ~context:Generator ~opc) expr
        (freevar_name t)
    (* We have no other generators (yet) *)
    | _ -> assert false
  in
  List.iter (emit_gen_root oc) generators ;

  (* Finally, call user_func on the actual expression, where all generators will
   * be replaced by their free variable: *)
  Printf.fprintf oc "%s (%a)"
    user_fun
    (emit_expr ?state:None ~context:Finalize ~opc) expr ;
  List.iter (fun _ -> Printf.fprintf oc ")") generators

let emit_generate_tuples name in_typ mentioned out_typ ~opc oc selected_fields =
  let has_generator =
    List.exists (fun sf ->
      RamenExpr.is_generator sf.RamenOperation.expr)
      selected_fields in
  if not has_generator then
    Printf.fprintf oc "let %s f_ it_ ot_ = f_ it_ ot_\n" name
  else (
    Printf.fprintf oc "let %s f_ (%a as it_) %a =\n"
      name
      (emit_in_tuple mentioned) in_typ
      (print_tuple_deconstruct TupleOut) out_typ ;
    (* Each generator is a functional receiving the continuation and calling it
     * as many times as there are values. *)
    let num_gens =
      List.fold_left (fun num_gens sf ->
          if not (RamenExpr.is_generator sf.RamenOperation.expr) then num_gens
          else (
            let ff_ = "ff_"^ string_of_int num_gens ^"_" in
            Printf.fprintf oc "%a(fun %s -> %a) (fun generated_%d_ ->\n"
              emit_indent (1 + num_gens)
              ff_
              (emit_generator ff_ ~opc) sf.RamenOperation.expr
              num_gens ;
            num_gens + 1)
        ) 0 selected_fields in
    (* Now we have all the generated values, actually call f_ on the tuple.
     * Note that the tuple must be in out_typ order: *)
    Printf.fprintf oc "%af_ it_ (\n%a"
      emit_indent (1 + num_gens)
      emit_indent (2 + num_gens) ;
    let expr_of_field name =
      let sf = List.find (fun sf ->
                 sf.RamenOperation.alias = name) selected_fields in
      sf.RamenOperation.expr in
    let _ = List.fold_lefti (fun gi i ft ->
        if i > 0 then Printf.fprintf oc ",\n%a" emit_indent (2 + num_gens) ;
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
    for _ = 1 to num_gens do Printf.fprintf oc ")" done ;
    Printf.fprintf oc ")\n"
  )

let emit_field_of_tuple name oc tuple_typ =
  Printf.fprintf oc "let %s %a = function\n"
    name
    (print_tuple_deconstruct TupleOut) tuple_typ ;
  List.iter (fun field_typ ->
      Printf.fprintf oc "\t| %S -> " field_typ.typ_name ;
      let id = id_of_field_name ~tuple:TupleOut field_typ.typ_name in
      if field_typ.typ.nullable then (
        Printf.fprintf oc "(match %s with Null -> \"?null?\" \
                            | NotNull v_ -> (%a) v_)\n"
          id
          (conv_from_to ~nullable:false) (field_typ.typ.structure, TString)
      ) else (
        Printf.fprintf oc "(%a) %s\n"
          (conv_from_to ~nullable:false) (field_typ.typ.structure, TString)
          id
      )
    ) tuple_typ ;
  Printf.fprintf oc "\t| _ -> raise Not_found\n"

let emit_state_update_for_expr ~opc oc expr =
  RamenExpr.unpure_iter (function
      | RamenExpr.StatefulFun (_, lifespan, skip_nulls, _) as e ->
          emit_expr ?state:None ~context:UpdateState ~opc oc e
      | _ -> ()
    ) expr

let emit_where
      ?(with_group=false) ?(always_true=false)
      name in_typ mentioned ~opc oc expr =
  Printf.fprintf oc "let %s global_ %a out_previous_opt_ "
    name
    (emit_in_tuple mentioned) in_typ ;
  if with_group then Printf.fprintf oc "group_ " ;
  if always_true then
    Printf.fprintf oc "= true\n"
  else (
    Printf.fprintf oc "=\n" ;
    (* Update the states used by this expression: *)
    emit_state_update_for_expr ~opc oc expr ;
    Printf.fprintf oc "\t%a\n"
      (emit_expr ?state:None ~context:Finalize ~opc) expr
  )

let emit_field_selection
      (* If true, we update the state and finalize as few fields as
       * possible (only those required by commit_cond and update_states).
       * If false, we have the minimal tuple as an extra parameter, and
       * only have to build the final out_tuple (taking advantage of the
       * fields already computed in minimal_typ). And no need to update
       * states at all. *)
      ~build_minimal
      name in_typ mentioned
      out_typ minimal_typ ~opc oc selected_fields =
  let field_in_minimal field_name =
    List.exists (fun ft ->
      ft.RamenTuple.typ_name = field_name
    ) minimal_typ in
  let must_output_field field_name =
    not build_minimal || field_in_minimal field_name in
  Printf.fprintf oc "let %s %a out_previous_opt_ group_ global_ "
    name
    (emit_in_tuple mentioned) in_typ ;
  if not build_minimal then
    Printf.fprintf oc "%a " (emit_tuple TupleOut) minimal_typ ;
  Printf.fprintf oc "=\n" ;
  List.iter (fun sf ->
      if must_output_field sf.RamenOperation.alias then (
        if build_minimal then (
          (* Update the states as required for this field, just before
           * computing the field actual value. *)
          Printf.fprintf oc "\t(* State Update for %s: *)\n"
            sf.RamenOperation.alias ;
          emit_state_update_for_expr ~opc oc sf.RamenOperation.expr ;
        ) ;
        if not build_minimal && field_in_minimal sf.alias then (
          (* We already have this binding *)
        ) else (
          Printf.fprintf oc "\t(* Output field %s of type %a *)\n"
            sf.RamenOperation.alias
            RamenExpr.print_typ (RamenExpr.typ_of sf.expr) ;
          if RamenExpr.is_generator sf.RamenOperation.expr then
            (* So that we have a single out_typ both before and after tuples generation *)
            Printf.fprintf oc "\tlet %s = () in\n"
              (id_of_field_name ~tuple:TupleOut sf.RamenOperation.alias)
          else
            Printf.fprintf oc "\tlet %s = %a in\n"
              (id_of_field_name ~tuple:TupleOut sf.RamenOperation.alias)
              (emit_expr ?state:None ~context:Finalize ~opc)
                sf.RamenOperation.expr)
        )
    ) selected_fields ;
  (* Here we must generate the tuple in the order specified by out_type,
   * not selected_fields: *)
  let is_selected name =
    List.exists (fun sf -> sf.RamenOperation.alias = name) selected_fields in
  Printf.fprintf oc "\t(\n\t\t" ;
  List.fold_left (fun i ft ->
      if must_output_field ft.typ_name then (
        let tuple =
          if is_selected ft.typ_name then TupleOut else TupleIn in
        Printf.fprintf oc "%s%s"
          (if i > 0 then ",\n\t\t" else "")
          (id_of_field_name ~tuple ft.typ_name) ;
        i + 1
      ) else i
    ) 0 out_typ |> ignore ;
  Printf.fprintf oc "\n\t)\n"

let emit_update_states
      name in_typ mentioned
      out_typ minimal_typ ~opc oc selected_fields =
  let field_in_minimal field_name =
    List.exists (fun ft ->
      ft.RamenTuple.typ_name = field_name
    ) minimal_typ in
  let expression_needs field_name expr =
    try RamenExpr.iter (function
          | RamenExpr.Field (_, { contents = TupleOut }, fn)
              when fn = field_name -> raise Exit
          | _ -> ()
        ) expr ;
        false
    with Exit -> true in
  let need_binding field_name i =
    list_existsi (fun j sf ->
      j > i &&
      expression_needs field_name sf.RamenOperation.expr
    ) selected_fields
  in
  Printf.fprintf oc "let %s %a out_previous_opt_ group_ global_ %a "
    name
    (emit_in_tuple mentioned) in_typ
    (emit_tuple TupleOut) minimal_typ ;
  Printf.fprintf oc "=\n" ;
  List.iteri (fun i sf ->
    if not (field_in_minimal sf.RamenOperation.alias) then (
      (* Update the states as required for this field, just before
       * computing the field actual value. *)
      Printf.fprintf oc "\t(* State Update for %s: *)\n"
        sf.RamenOperation.alias ;
      emit_state_update_for_expr ~opc oc sf.RamenOperation.expr ;
      if not (RamenExpr.is_generator sf.RamenOperation.expr) &&
         (* Avoid finalizing the value yet unless it's needed to update
          * a later state: *)
         need_binding sf.RamenOperation.alias i
      then (
        Printf.fprintf oc "\tlet %s = %a in\n"
          (id_of_field_name ~tuple:TupleOut sf.RamenOperation.alias)
          (emit_expr ?state:None ~context:Finalize ~opc)
            sf.RamenOperation.expr))
  ) selected_fields ;
  Printf.fprintf oc "\t()\n"

(* Similar to emit_field_selection but with less options, no concept of star and no
 * naming of the fields as the fields from out, since that's not the out tuple
 * we are constructing: *)
let emit_key_of_input name in_typ mentioned ~opc oc exprs =
  Printf.fprintf oc "let %s %a =\n\t("
    name
    (emit_in_tuple mentioned) in_typ ;
  List.iteri (fun i expr ->
      Printf.fprintf oc "%s\n\t\t%a"
        (if i > 0 then "," else "")
        (emit_expr ?state:None ~context:Finalize ~opc) expr ;
    ) exprs ;
  Printf.fprintf oc "\n\t)\n"

let for_each_unpure_fun selected_fields
                        ?where ?commit_cond f =
  List.iter (fun sf ->
      RamenExpr.unpure_iter f sf.RamenOperation.expr
    ) selected_fields ;
  Option.may (RamenExpr.unpure_iter f) where ;
  Option.may (RamenExpr.unpure_iter f) commit_cond

let for_each_unpure_fun_my_lifespan lifespan selected_fields
                                    ?where ?commit_cond f =
  let open RamenExpr in
  for_each_unpure_fun selected_fields
                      ?where ?commit_cond
    (function
    | StatefulFun (_, l, _, _) as e when l = lifespan ->
      f e
    | _ -> ())

let otype_of_state e =
  let open RamenExpr in
  let typ = Option.get (typ_of e).typ in
  let t = typ.structure |>
          IO.to_string otype_of_type in
  let print_expr_typ oc e =
    let typ = Option.get (typ_of e).typ in
    typ.structure |> (* nullable taken care of below *)
    IO.to_string otype_of_type |>
    String.print oc in
  let nullable = if typ.nullable then " nullable" else "" in
  match e with
  (* previous tuples and count ; Note: we could get rid of this count if we
   * provided some context to those functions, such as the event count in
   * current window, for instance (ie. pass the full aggr record not just
   * the fields) *)
  | StatefulFun (_, _, _, (Lag _ | MovingAvg _ | LinReg _)) ->
    t ^" CodeGenLib.Seasonal.t"^ nullable
  | StatefulFun (_, _, _, MultiLinReg _) ->
    "("^ t ^" * float array) CodeGenLib.Seasonal.t"^ nullable
  | StatefulFun (_, _, _, Remember _) ->
    "CodeGenLib.Remember.state"^ nullable
  | StatefulFun (_, _, _, Distinct es) ->
    Printf.sprintf2 "%a CodeGenLib.Distinct.state%s"
      (list_print_as_product print_expr_typ) es
      nullable
  | StatefulFun (_, _, _, AggrAvg _) -> "(int * float)"^ nullable
  | StatefulFun (_, _, _, (AggrFirst _|AggrLast _|AggrMin _|AggrMax _)) -> t ^" option"^ nullable
  | StatefulFun (_, _, _, Top { what ; _ }) ->
    Printf.sprintf2 "%a HeavyHitters.t%s"
      (list_print_as_product print_expr_typ) what
      nullable
  | StatefulFun (_, _, _, Last (_, e, es)) ->
    if es = [] then
      (* In that case we use a special internal counter as the order: *)
      Printf.sprintf2 "(%a, int) CodeGenLib.Last.state%s"
        print_expr_typ e
        nullable
    else
      Printf.sprintf2 "(%a, %a) CodeGenLib.Last.state%s"
        print_expr_typ e
        print_expr_typ (List.hd es)
        nullable
  | StatefulFun (_, _, _, Sample (_, e)) ->
      Printf.sprintf2 "%a RamenSampling.reservoir%s"
        print_expr_typ e
        nullable
  | StatefulFun (_, _, _, Group e) ->
    Printf.sprintf2 "%a list%s" print_expr_typ e nullable
  | StatefulFun (_, _, _, AggrHistogram _) -> "CodeGenLib.Histogram.state"^ nullable
  | _ -> t ^ nullable

let emit_state_init name state_lifespan other_state_vars
      ?where ?commit_cond ~opc
      oc selected_fields =
  (* We must collect all unpure functions present in the selected_fields
   * and return a record with the proper types and init values for the required
   * states. *)
  let for_each_my_unpure_fun f =
    for_each_unpure_fun_my_lifespan
      state_lifespan selected_fields ?where ?commit_cond f
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
          RamenExpr.print_typ (RamenExpr.typ_of f) ;
        (* Only used when skip_nulls: *)
        Printf.fprintf oc "\tmutable %s_empty_ : bool ;\n"
          (name_of_state f)
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
          (emit_expr ~context:InitState ~opc ~state:state_lifespan) f) ;
    (* And now build the state record from all those fields: *)
    Printf.fprintf oc "\t{" ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf oc " %s ; %s_empty_ = true ; "
          (name_of_state f) (name_of_state f)) ;
    Printf.fprintf oc " }\n"
  )

(* Note: we need group_ in addition to out_tuple because the commit-when clause
 * might have its own stateful functions going on *)
let emit_when name in_typ mentioned minimal_typ ~opc
              oc when_expr =
  Printf.fprintf oc "let %s %a out_previous_opt_ group_ global_ %a =\n"
    name
    (emit_in_tuple mentioned) in_typ
    (emit_tuple TupleOut) minimal_typ ;
  (* Update the states used by this expression: *)
  emit_state_update_for_expr ~opc oc when_expr ;
  Printf.fprintf oc "\t%a\n"
    (emit_expr ?state:None ~context:Finalize ~opc) when_expr

(* Depending on what uses a commit/flush condition, we might need to check
 * all groups after every single input tuple (very slow), or after every
 * selected input tuple (still quite slow), or only when this group is
 * modified (fast). Users should limit all/selected tuple to aggregations
 * with few groups only. *)
let when_to_check_group_for_expr expr =
  (* Tells whether the commit condition needs the all or the selected tuple *)
  let open RamenExpr in
  let need_all =
    fold_by_depth (fun need_all -> function
        | Field (_, tuple, _) -> need_all || !tuple = TupleIn
        | _ -> need_all
      ) false expr
  in
  if need_all then "CodeGenLib_Skeletons.ForAll"
  else "CodeGenLib_Skeletons.ForInGroup"

let emit_sort_expr name in_typ mentioned ~opc oc es_opt =
  Printf.fprintf oc "let %s sort_count_ %a %a %a %a =\n"
    name
    (emit_in_tuple ~tuple:TupleSortFirst mentioned) in_typ
    (emit_in_tuple ~tuple:TupleIn mentioned) in_typ
    (emit_in_tuple ~tuple:TupleSortSmallest mentioned) in_typ
    (emit_in_tuple ~tuple:TupleSortGreatest mentioned) in_typ ;
  match es_opt with
  | [] ->
      (* The default sort_until clause must be false.
       * If there is no sort_by clause, any constant will do: *)
      Printf.fprintf oc "\tfalse\n"
  | es ->
      Printf.fprintf oc "\t%a\n"
        (List.print ~first:"(" ~last:")" ~sep:", "
           (emit_expr ?state:None ~context:Finalize ~opc)) es

let emit_merge_on name in_typ mentioned ~opc oc es =
  Printf.fprintf oc "let %s %a =\n\t%a\n"
    name
    (emit_in_tuple mentioned) in_typ
    (List.print ~first:"(" ~last:")" ~sep:", "
       (emit_expr ?state:None ~context:Finalize ~opc)) es

let emit_notification_tuple out_typ ~opc oc notif =
  let open RamenOperation in
  let print_expr = emit_expr ?state:None ~context:Finalize ~opc in
  Printf.fprintf oc
    "(%a,\n\t\t%a)"
    print_expr notif.notif_name
    (List.print ~sep:";\n\t\t  "
      (fun oc (n, v) -> Printf.fprintf oc "%S, %a" n print_expr v))
        notif.parameters

(* We want a function that, when given the worker name, current time and the
 * output tuple, will return the list of RamenNotification.tuple to send: *)
let emit_get_notifications name in_typ mentioned out_typ ~opc oc notifications =
  Printf.fprintf oc "let %s %a %a =\n\t%a\n"
    name
    (emit_in_tuple mentioned) in_typ
    (emit_tuple TupleOut) out_typ
    (List.print ~sep:";\n\t\t" (emit_notification_tuple out_typ ~opc))
      notifications

let expr_needs_group e =
  let open RamenExpr in
  fold_by_depth (fun need expr ->
    need || match expr with
      | Field (_, tuple, _) -> tuple_need_state !tuple
      | StatefulFun (_, LocalState, _, _) -> true
      | _ -> false
  ) false e

let emit_aggregate opc oc name in_typ out_typ =
  match opc.op with
  | RamenOperation.Aggregate
      { fields ; merge ; sort ; where ; key ;
        commit_before ; commit_cond ; flush_how ; notifications ; event_time ;
        every ; _ } as op ->
  let fetch_recursively s =
    let s = ref s in
    if not (reach_fixed_point (fun () ->
      let nb_fields = Set.String.cardinal !s in
      List.iter (fun sf ->
        (* is this out field selected for minimal_out yet? *)
        if Set.String.mem sf.RamenOperation.alias !s then (
          (* Add all other fields from out that are needed in this field
           * expression *)
          RamenExpr.iter (function
            | Field (_, { contents = TupleOut }, fn) ->
                s := Set.String.add fn !s
            | _ -> ()) sf.RamenOperation.expr)
      ) fields ;
      Set.String.cardinal !s > nb_fields))
    then failwith "Cannot build minimal_out set?!" ;
    !s in
  let minimal_fields =
    let from_commit_cond =
      RamenExpr.fold_by_depth (fun s -> function
        | Field (_, { contents = TupleOut }, fn) ->
            Set.String.add fn s
        | _ -> s
      ) Set.String.empty commit_cond |>
      (* We also need all the fields from TupleOut that are used in the
       * select expression of those fields that are mentioned in commit_cond,
       * recursively: *)
      fetch_recursively in
    (* We also need all the fields from TupleOut that are used in any
     * stateful function for any other fields (for updating its state),
     * recursively: *)
    let for_updates =
      List.fold_left (fun s sf ->
        RamenExpr.unpure_fold s (fun s -> function
          | e -> RamenExpr.fold_by_depth (fun s -> function
                   | Field (_, { contents = TupleOut }, fn) ->
                       Set.String.add fn s
                   | _ -> s) s e) sf.RamenOperation.expr
      ) Set.String.empty fields |>
      fetch_recursively in
    (* Now combine these sets: *)
    Set.String.union from_commit_cond for_updates
  in
  !logger.debug "minimal fields: %a"
    (Set.String.print String.print) minimal_fields ;
  let minimal_typ =
    List.filter (fun ft ->
      Set.String.mem ft.RamenTuple.typ_name minimal_fields
    ) out_typ in
  (* FIXME: now that we serialize only used fields, when do we have fields
   * that are not mentioned?? *)
  let mentioned =
    let all_exprs = RamenOperation.fold_expr [] (fun l s -> s :: l) op in
    add_all_mentioned_in_expr all_exprs
  (* Tells whether we need the group to check the where clause (because it
   * uses the group tuple or build a group-wise aggregation on its own,
   * despite this is forbidden in RamenOperation.check): *)
  and where_need_group = expr_needs_group where
  and when_to_check_for_commit = when_to_check_group_for_expr commit_cond in
  Printf.fprintf oc
    "%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n"
    (emit_state_init "global_init_" RamenExpr.GlobalState [] ~where ~commit_cond ~opc) fields
    (emit_state_init "group_init_" RamenExpr.LocalState ["global_"] ~where ~commit_cond ~opc) fields
    (emit_read_tuple "read_tuple_" mentioned) in_typ
    (if where_need_group then
      emit_where "where_fast_" ~always_true:true in_typ mentioned ~opc
    else
      emit_where "where_fast_" in_typ mentioned ~opc) where
    (if not where_need_group then
      emit_where "where_slow_" ~with_group:true ~always_true:true in_typ mentioned ~opc
    else
      emit_where "where_slow_" ~with_group:true in_typ mentioned ~opc) where
    (emit_key_of_input "key_of_input_" in_typ mentioned ~opc) key
    emit_maybe_fields out_typ
    (emit_when "commit_cond_" in_typ mentioned minimal_typ ~opc) commit_cond
    (emit_field_selection ~build_minimal:true "minimal_tuple_of_group_" in_typ mentioned out_typ minimal_typ ~opc) fields
    (emit_field_selection ~build_minimal:false "out_tuple_of_minimal_tuple_" in_typ mentioned out_typ minimal_typ ~opc) fields
    (emit_update_states "update_states_" in_typ mentioned out_typ minimal_typ ~opc) fields
    (emit_sersize_of_tuple "sersize_of_tuple_") out_typ
    (emit_time_of_tuple "time_of_tuple_") opc
    (emit_serialize_tuple "serialize_group_") out_typ
    (emit_generate_tuples "generate_tuples_" in_typ mentioned out_typ ~opc) fields
    (emit_field_of_tuple "field_of_tuple_in_") in_typ
    (emit_field_of_tuple "field_of_tuple_out_") out_typ
    (emit_merge_on "merge_on_" in_typ mentioned ~opc) merge.on
    (emit_sort_expr "sort_until_" in_typ mentioned ~opc) (match sort with Some (_, Some u, _) -> [u] | _ -> [])
    (emit_sort_expr "sort_by_" in_typ mentioned ~opc) (match sort with Some (_, _, b) -> b | None -> [])
    (emit_get_notifications "get_notifications_" in_typ mentioned out_typ ~opc) notifications ;
  Printf.fprintf oc "let %s () =\n\
      \tCodeGenLib_Skeletons.aggregate\n\
      \t\tread_tuple_ sersize_of_tuple_ time_of_tuple_ serialize_group_\n\
      \t\tgenerate_tuples_\n\
      \t\tminimal_tuple_of_group_\n\
      \t\tupdate_states_\n\
      \t\tout_tuple_of_minimal_tuple_\n\
      \t\tmerge_on_ %d %F %d sort_until_ sort_by_\n\
      \t\twhere_fast_ where_slow_ key_of_input_ %b\n\
      \t\tcommit_cond_ %b %b %s\n\
      \t\tglobal_init_ group_init_\n\
      \t\tfield_of_tuple_in_ field_of_tuple_out_ field_of_params_\n\
      \t\tget_notifications_ %f\n"
    name
    merge.last merge.timeout
    (match sort with None -> 0 | Some (n, _, _) -> n)
    (key = [])
    commit_before
    (flush_how <> Never)
    when_to_check_for_commit
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
    open Stdint\n\
    open RamenNullable\n"
    (RamenName.string_of_func func_name)
    RamenOperation.print op ;
  (* Emit parameters: *)
  Printf.fprintf oc "\n(* Parameters: *)\n" ;
  List.iter (fun p ->
    (* FIXME: nullable parameters *)
    Printf.fprintf oc
      "let %s_%s_ =\n\
       \tlet parser_ x_ = %s(%a) in\n\
       \tCodeGenLib.parameter_value ~def:(%s(%a)) parser_ %S\n"
      (id_of_prefix TupleParam) p.ptyp.typ_name
      (if p.ptyp.typ.nullable then
        "if RamenHelpers.looks_like_null x_ then Null else NotNull "
       else "") (emit_value_of_string p.ptyp.typ.structure) "x_"
      (if p.ptyp.typ.nullable && p.value <> VNull
       then "NotNull " else "")
      emit_type p.value p.ptyp.typ_name
  ) params ;
  (* Also a function that takes a parameter name (string) and return its
   * value (as a string) - useful for text replacements within strings *)
  Printf.fprintf oc "let field_of_params_ = function\n%a\
                     \t| _ -> raise Not_found\n\n"
    (List.print ~first:"" ~last:"" ~sep:"" (fun oc p ->
      let glob_name =
        Printf.sprintf "%s_%s_"
          (id_of_prefix TupleParam)
          p.ptyp.typ_name in
      Printf.fprintf oc "\t| %S -> (%a) %s%s\n"
        p.ptyp.typ_name
        (conv_from_to ~nullable:p.ptyp.typ.nullable) (p.ptyp.typ.structure, TString)
        glob_name
        (if p.ptyp.typ.nullable then " |! \"?null?\""
         else ""))) params ;
  (* Now the code, which might need some global constant parameters,
   * thus the two strings that are assembled later: *)
  let code = IO.output_string ()
  and consts = IO.output_string () in
  (match op with
  | ReadCSVFile { where = { fname ; unlink } ; preprocessor ;
                  what = { separator ; null ; fields } ; event_time } ->
    let opc = { op ; params ; consts ; tuple_typ = fields } in
    emit_read_csv_file opc code name fname unlink separator null
                       preprocessor
  | ListenFor { net_addr ; port ; proto } ->
    let tuple_typ = RamenProtocols.tuple_typ_of_proto proto in
    let opc = { op ; params ; consts ; tuple_typ } in
    emit_listen_on opc code name net_addr port proto
  | Instrumentation { from } ->
    let tuple_typ = RamenBinocle.tuple_typ in
    let opc = { op ; params ; consts ; tuple_typ } in
    emit_well_known opc code name from
      "RamenBinocle.unserialize" "report_ringbuf"
      "(fun (w, t, _, _, _, _, _, _, _, _, _, _, _, _) -> w, t)"
  | Notifications { from } ->
    let tuple_typ = RamenNotification.tuple_typ in
    let opc = { op ; params ; consts ; tuple_typ } in
    emit_well_known opc code name from
      "RamenNotification.unserialize" "notifs_ringbuf"
      "(fun (w, t, _, _, _, _, _, _) -> w, t)"
  | Aggregate _ ->
    let opc = { op ; params ; consts ; tuple_typ = out_typ } in
    emit_aggregate opc code name in_typ out_typ) ;
  Printf.fprintf oc "\n(* Global constants: *)\n\n%s\n\
                     \n(* Operation Implementation: *)\n\n%s\n"
    (IO.close_out consts) (IO.close_out code)

let compile conf entry_point func_name obj_name in_typ out_typ params op =
  let open RamenOperation in
  let%lwt src_file =
    Lwt.wrap (fun () ->
      RamenOCamlCompiler.with_code_file_for obj_name conf
        (emit_operation entry_point func_name in_typ out_typ params
         op)) in
  (* TODO: any failure in compilation -> delete the source code! Or it will be reused *)
  RamenOCamlCompiler.compile conf func_name src_file obj_name
