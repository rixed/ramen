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
module F = C.Func
module E = RamenExpr
module T = RamenTypes
module O = RamenOperation
open RamenTypes (* FIXME: RamenTypes.Pub ? *)

(* If true, the generated code will log details about serialization *)
let verbose_serialization = false

(* We pass this around as "opc" *)
type op_context =
  { op : O.t option ;
    event_time : RamenEventTime.t option ;
    (* The type of the output tuple in user order *)
    tuple_typ : RamenTuple.field_typ list ;
    params : RamenTuple.params ;
    consts : string Batteries.IO.output }

let id_of_prefix tuple =
  String.nreplace (string_of_prefix tuple) "." "_"

(* Tuple deconstruction as a function parameter: *)
let id_of_field_name ?(tuple=TupleIn) (x : RamenName.field) =
  (match (x :> string) with
  (* Note: we have a '#count' for the sort tuple. *)
  | "#count" -> "virtual_"^ id_of_prefix tuple ^"_count_"
  | field -> id_of_prefix tuple ^"_"^ field ^"_") |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let id_of_field_typ ?tuple field_typ =
  id_of_field_name ?tuple field_typ.RamenTuple.name

let var_name_of_record_field (k : RamenName.field) =
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

let array_print_as_tuple_i p oc a =
  let i = ref 0 in
  Array.print ~first:"(" ~last:")" ~sep:", " (fun oc x ->
    p oc !i x ; incr i) oc a

let list_print_as_vector p = List.print ~first:"[|" ~last:"|]" ~sep:"; " p
let list_print_as_product p = List.print ~first:"(" ~last:")" ~sep:" * " p

let tuple_id tuple =
  string_of_prefix tuple ^"_" |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let emit_tuple ?(with_alias=false) tuple =
  let print_field oc field_typ =
      String.print oc (id_of_field_typ ~tuple field_typ)
  and as_ = if with_alias then Some (tuple_id tuple) else None in
  list_print_as_tuple ?as_ print_field

(* Emit the code that return the sersize of a fixed size type *)
let emit_sersize_of_fixsz_typ oc typ =
  let sz = RingBufLib.sersize_of_fixsz_typ typ in
  Int.print oc sz

let rec emit_sersize_of_not_null_scalar tx_var offs_var oc = function
  | TString ->
    Printf.fprintf oc "\
      %d + RingBuf.round_up_to_rb_word(RingBuf.read_word %s %s)"
      RingBuf.rb_word_bytes tx_var offs_var
  | TIp ->
    Printf.fprintf oc "RingBuf.(rb_word_bytes + \
                         round_up_to_rb_word(\
                           match RingBuf.read_word %s %s with \
                             0 -> %a | 1 -> %a \
                           | x -> invalid_byte_for \"IP\" x))"
      tx_var offs_var
      emit_sersize_of_fixsz_typ TIpv4
      emit_sersize_of_fixsz_typ TIpv6
  | TCidr ->
    Printf.fprintf oc "RingBuf.(rb_word_bytes + \
                         round_up_to_rb_word(\
                           match RingBuf.read_u8 %s %s |> Uint8.to_int with \
                             4 -> %a | 6 -> %a \
                           | x -> invalid_byte_for \"CIDR\" x))"
      tx_var offs_var
      emit_sersize_of_fixsz_typ TCidrv4
      emit_sersize_of_fixsz_typ TCidrv6

  | TTuple _ | TRecord _ | TVec _ | TList _ -> assert false

  | t -> emit_sersize_of_fixsz_typ oc t

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
  | TRecord _ -> "record"
  | TVec _  -> "vector"
  | TList _ -> "list"
  | TNum | TAny | TEmpty -> assert false

let rec emit_value_of_string typ oc var =
  match typ with
  | TVec (_, t) | TList t ->
      Printf.fprintf oc
        "split_string ~sep:';' ~opn:'[' ~cls:']' %s |>\n\
         Array.map (fun x_ -> %a)"
        var
        (emit_value_of_string t.structure) "x_"
  | TTuple ts ->
      (* FIXME: same as above re. [split_on_char]: *)
      Printf.fprintf oc
        "let s_ =\n\
           split_string ~sep:';' ~opn:'(' ~cls:')' %s in\n\
         if Array.length s_ <> %d then failwith (\
           Printf.sprintf \"Bad arity for tuple %%s, expected %d items\" \
             %s) ;\n\
         %a"
         var (Array.length ts) (Array.length ts) var
         (array_print_as_tuple_i (fun oc i t ->
           emit_value_of_string t.structure oc
             ("s_.("^ string_of_int i ^")"))) ts
  | TRecord kts ->
      (* FIXME: same as above re. [split_on_char]: *)
      (* TODO: we should instead also expect to find field names and then
       * reorder. *)
      Printf.fprintf oc
        "let s_ =\n\
           split_string ~sep:';' ~opn:'(' ~cls:')' %s in\n\
         if Array.length s_ <> %d then failwith (\
           Printf.sprintf \"Bad arity for record %%s, expected %d items\" \
             %s) ;\n\
         %a"
         var (Array.length kts) (Array.length kts) var
         (array_print_as_tuple_i (fun oc i (_k, t) ->
           emit_value_of_string t.structure oc
             ("s_.("^ string_of_int i ^")"))) kts
  | typ ->
      Printf.fprintf oc "RamenTypeConverters.%s_of_string %s"
        (id_of_typ typ) var

let emit_float oc f =
  (* printf "%F" would not work for infinity:
   * https://caml.inria.fr/mantis/view.php?id=7685
   * and "%h" not for neg_infinity. *)
  if f = infinity then String.print oc "infinity"
  else if f = neg_infinity then String.print oc "neg_infinity"
  else Legacy.Printf.sprintf "%h" f |> String.print oc

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
  | TU8 -> p "VU8" | TU16 -> p "VU16" | TU32 -> p "VU32"
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
      (* We represent records as OCaml tuples, in serialization order for
       * simplicity and efficiency. When reading the code we would like to
       * have those in declaration order, but it's not easy to keep that
       * declaration order around. Also it would make the code more
       * complicated as two record types differing only by definition order
       * are really the same record. *)
      let a = RingBufLib.ser_array_of_record ~with_private:true kts in
      Printf.fprintf oc "(let h_ = Hashtbl.create %d " (Array.length a) ;
      Printf.fprintf oc "and %a = x_ in "
        (array_print_as_tuple_i (fun oc i _ ->
          Printf.fprintf oc "x%d_" i)) a ;
      Array.iter (fun (k, t) ->
        Printf.fprintf oc "Hashtbl.add h_ %S %a ;" k emit_value t) a ;
      Printf.fprintf oc "RamenTypes.VRecord h_)"
  | TVec (_d, t) ->
      Printf.fprintf oc "RamenTypes.VVec (Array.map %a x_)" emit_value t
  | TList t ->
      Printf.fprintf oc "RamenTypes.VList (Array.map %a x_)" emit_value t) ;
  String.print oc ")"

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
  | VRecord kvs ->
      let vs = RingBufLib.ser_array_of_record ~with_private:true kvs |>
               Array.map snd in
      emit_type oc (VTuple vs)
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
  | TRecord kts ->
      let ts = RingBufLib.ser_array_of_record ~with_private:true kts |>
               Array.map snd in
      otype_of_type oc (TTuple ts)
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
  | TTuple _ | TRecord _ | TVec _ | TList _ | TNum | TAny | TEmpty ->
      assert false

(* When we do have to convert a null value into a string: *)
let string_of_null = "?null?"

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
    | (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128), TBool ->
      Printf.fprintf oc "(fun x_ -> %s.(compare zero x_) <> 0)"
        (omod_of_type from_typ)
    | (TEth|TIpv4|TIpv6|TIp|TCidrv4|TCidrv6|TCidr), TString ->
      Printf.fprintf oc "%s.to_string" (omod_of_type from_typ)
    | (TIpv4 | TU32), TIp -> Printf.fprintf oc "(fun x_ -> RamenIp.V4 x_)"
    | (TIpv6 | TU128), TIp -> Printf.fprintf oc "(fun x_ -> RamenIp.V6 x_)"
    | TCidrv4, TCidr -> Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V4 x_)"
    | TCidrv6, TCidr -> Printf.fprintf oc "(fun x_ -> RamenIp.Cidr.V6 x_)"
    | TIpv4, TU32 | TU32, TIpv4 -> Printf.fprintf oc "identity"
    | TIpv6, TU128 | TU128, TIpv6 -> Printf.fprintf oc "identity"
    | TU64, TEth -> Printf.fprintf oc "Uint48.of_uint64"
    | TList t_from, TList t_to
         when t_from.nullable = t_to.nullable ->
      Printf.fprintf oc "(fun v_ -> Array.map (%a) v_)"
        (conv_from_to ~nullable:t_from.nullable)
          (t_from.structure, t_to.structure)
     | TList t_from, TList t_to
          when nullable && t_from.nullable && not t_to.nullable ->
      Printf.fprintf oc
        "(fun v_ -> Array.map (function \
            | Null -> raise ImNull \
            | NotNull x_ -> %a x_) v_)"
        (conv_from_to ~nullable:t_from.nullable)
          (t_from.structure, t_to.structure)
    | TVec (_, t_from), TList t_to ->
      print_non_null oc (TList t_from, TList t_to)
    | TVec (d_from, t_from), TVec (d_to, t_to)
      when (d_from = d_to || d_to = 0) ->
      (* d_to = 0 means no constraint (copy the one from the left-hand side) *)
      print_non_null oc (TList t_from, TList t_to)
    | (TVec (_, t) | TList t), TString ->
      Printf.fprintf oc
        "(fun v_ -> \
           (\
            Array.enum v_ /@ (%a) |> \
            Enum.fold (fun res_ s_ -> res_^\";\"^(%s s_)) \"[\"
           ) ^\"]\")"
        (conv_from_to ~nullable:t.nullable) (t.structure, TString)
        (if t.nullable then
           Printf.sprintf "RamenNullable.default %S" string_of_null else "")
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
    | TRecord ts, TString ->
      (* TODO: also print the field names? *)
      let i = ref 0 in
      Printf.fprintf oc
        "(fun %a -> \"(\"^ %a ^\")\")"
          (array_print_as_tuple_i (fun oc i _ ->
            Printf.fprintf oc "x%d_" i)) ts
          (Array.print ~first:"" ~last:"" ~sep:" ^\";\"^ " (fun oc (_k, t) ->
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

let wrap_nullable ~nullable oc f =
  (* TODO: maybe catch ImNull? *)
  if nullable then Printf.fprintf oc "NotNull (%t)" f
  else f oc

(* Used by Generator functions: *)
let freevar_name e =
  "fv_"^ string_of_int e.E.uniq_num ^"_" |>
  RamenOCamlCompiler.make_valid_ocaml_identifier

let any_constant_of_expr_type typ =
  E.make ~structure:typ.T.structure ~nullable:typ.T.nullable
         (Const (any_value_of_type typ.structure))

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
       * a nullable (actually, an option) *)

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

(* Return the environment corresponding to the used envvars: *)
let env_of_envvars envvars =
  List.map (fun (f : RamenName.field) ->
    let v =
      Printf.sprintf2 "(Sys.getenv_opt %S |> nullable_of_option)"
        (f :> string) in
    (* FIXME: RecordField should take a tuple and a _path_ not a field
     * name *)
    E.RecordField (TupleEnv, f), v
  ) envvars

let env_of_params params =
  List.map (fun param ->
    let f = param.RamenTuple.ptyp.name in
    let v = id_of_field_name ~tuple:TupleParam f in
    (* FIXME: RecordField should take a tuple and a _path_ not a field
     * name *)
    E.RecordField (TupleParam, f), v
  ) params

(* Returns all the bindings in global and group states as well as the
 * environment for the env and param 'tuples': *)
let initial_environments op params envvars =
  let init_env = E.RecordValue TupleEnv, "envs_"
  and init_param = E.RecordValue TupleParam, "params_" in
  let env_env = init_env :: env_of_envvars envvars
  and param_env = init_param :: env_of_params params in
  let glob_env, loc_env =
    O.fold_expr ([], []) (fun _s (glo, loc as prev) e ->
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
  glob_env, loc_env, env_env, param_env

(* Takes an operation and convert all its Path expressions for the
 * given tuple into a Binding to the environment: *)
let subst_fields_for_binding pref =
  O.map_expr (fun _stack e ->
    match e.E.text with
    | Stateless (SL1 (Path path, { text = Variable prefix ; }))
      when prefix = pref ->
        let f = E.id_of_path path in
        { e with text = Binding (RecordField (pref, f)) }
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                           { text = Variable prefix ; }))
      when prefix = pref ->
        let f = RamenName.field_of_string n in
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
      | TupleEnv ->
          Printf.sprintf2 "(Sys.getenv_opt %S |> nullable_of_option)"
            (ft.RamenTuple.name :> string)
      | TupleOutPrevious ->
          Printf.sprintf2 "(maybe_%s_ out_previous_)"
            (ft.name :> string)
      | _ -> id_of_field_typ ~tuple ft in
    (E.RecordField (tuple, ft.name), v) :: env
  ) env typ

let rec conv_to ~env ~context ~opc to_typ oc e =
  match e.E.typ.structure, to_typ with
  | a, Some b ->
    Printf.fprintf oc "(%a) (%a)"
      (conv_from_to ~nullable:e.typ.nullable) (a, b)
      (emit_expr ~context ~opc ~env) e
  | _, None -> (* No conversion required *)
    (emit_expr ~context ~opc ~env) oc e

(* Apply the given function to the given args (and varargs), after
 * converting them, obeying skip_nulls. It is assumed that nullable
 * is set reliably. *)
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

(* The vectors TupleOutPrevious is nullable: the commit when and
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
      (emit_tuple TupleOut) out_typ
      (if ft.typ.nullable then "" else "NotNull ")
      (id_of_field_name ~tuple:TupleOut ft.name)
  ) out_typ

and emit_event_time oc opc =
  let (sta_field, sta_src, sta_scale), dur = Option.get opc.event_time in
  let open RamenEventTime in
  let field_value_to_float src oc field_name =
    match src with
    | OutputField ->
        (* This must not fail if RamenOperation.check did its job *)
        let f = List.find (fun t -> t.name = field_name) opc.tuple_typ in
        Printf.fprintf oc
          (if f.typ.nullable then "((%a) %s |! 0.)" else "(%a) %s")
          (conv_from_to ~nullable:f.typ.nullable)
          (f.typ.structure, TFloat)
          (id_of_field_name ~tuple:TupleOut field_name)
    | Parameter ->
        let param = RamenTuple.params_find field_name opc.params in
        Printf.fprintf oc "(%a %s_%s_)"
          (conv_from_to ~nullable:false) (param.ptyp.typ.structure, TFloat)
          (id_of_prefix TupleParam)
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
    Printf.fprintf oc "%s(%a %a)"
      (if nullable then "NotNull " else "")
      (conv_from_to ~nullable:false) (structure_of c, expr.typ.structure)
      emit_type c
  | Finalize, Tuple es, _ ->
    list_print_as_tuple (emit_expr ~env ~context ~opc) oc es
  | Finalize, Record kvs, _ ->
    (* Here we must compute the values in order, as each expression can
     * refer to the previous one. And we must, for each expression, evaluate
     * it in a context where this record is opened. *)
    let _env =
      List.fold_left (fun env ((k : RamenName.field), v) ->
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

  | Finalize, Vector es, _ ->
    list_print_as_vector (emit_expr ~env ~context ~opc) oc es

  | Finalize, Stateless (SL1 (Path _, _)), _ ->
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
    emit_functionN ~env ~opc ~nullable "CodeGenLib.reldiff"
      [Some TFloat, PropagateNull; Some TFloat, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Pow, e1, e2)), (TFloat|TI32|TI64 as t) ->
    emit_functionN ~env ~opc ~nullable (omod_of_type t ^".( ** )")
      [Some t, PropagateNull; Some t, PropagateNull] oc [e1; e2]
  | Finalize, Stateless (SL2 (Pow, e1, e2)), (TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI128 as t) ->
    (* For all others we exponentiate via floats: *)
    Printf.fprintf oc "(%a %a)"
      (conv_from_to ~nullable) (TFloat, t)
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
      [Some (TVec (0, T.make ~nullable:false TFloat)),
       PropagateNull] oc [e]
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
                option_get "Get from tuple must have const index" in
        emit_select_from_tuple ts n
    | TRecord kts ->
        let s = E.string_of_const n |>
                option_get "Get from structure must have const str index" in
        let pos_of_field =
          try array_rfindi (fun (k, _t) -> k = s) kts
          with Not_found ->
            Printf.sprintf2 "Invalid field name %S (have %a)"
              s
              (pretty_array_print (fun oc (k, _) -> String.print oc k)) kts |>
            failwith in
        let ts = Array.map snd kts in
        emit_select_from_tuple ts pos_of_field
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

  (* And and Or does not inherit nullability from their arguments the way
   * other functions does: given only one value we may be able to find out
   * the result without looking at the other one (that can then be NULL). *)
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
    String.print oc "!CodeGenLib_IO.now"
  | Finalize, Stateless (SL0 Random), TFloat ->
    String.print oc "(Random.float 1.)"
  | Finalize, Stateless (SL0 EventStart), TFloat ->
    Printf.fprintf oc "((%a) |> fst)" emit_event_time opc
  | Finalize, Stateless (SL0 EventStop), TFloat ->
    Printf.fprintf oc "((%a) |> snd)" emit_event_time opc
  | Finalize, Stateless (SL1 (Cast _, { text = Const VNull ; _ })), _ ->
    (* Special case when casting NULL to anything: that must work whatever the
     * destination type, even if we have no converter from the type of NULL.
     * This is important because literal NULL type is random. *)
    Printf.fprintf oc "Null"
  | Finalize, Stateless (SL1 (Cast to_typ, e)), _ ->
    (* A failure to convert should yield a NULL value rather than crash that
     * tuple, unless the user insisted to convert to a non-nullable type: *)
    if to_typ.nullable then String.print oc "(try " ;
    let from = e.E.typ in
    (* Shall we force a non-nullable argument to become nullable, or
     * propagates nullability from the argument? *)
    let add_nullable = not from.nullable && to_typ.nullable in
    if add_nullable then Printf.fprintf oc "NotNull (" ;
    Printf.fprintf oc "(%a) (%a)"
      (conv_from_to ~nullable:from.nullable)
        (from.structure, to_typ.structure)
      (emit_expr ~env ~context ~opc) e ;
    if add_nullable then Printf.fprintf oc ")" ;
    if to_typ.nullable then String.print oc " with _ -> Null)"

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
          "(let x0_ = %a in CodeGenLib.print (%s(%a x0_)::%a) ; x0_)"
          (emit_expr ~env ~context ~opc) e
          (if e.E.typ.nullable then "" else "NotNull ")
          (conv_from_to ~nullable:e.typ.nullable) (e.E.typ.structure, TString)
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
         * the xs in case A is found in the set, but do if it is not. If A is
         * NULL though, then the result is unless the set is empty: *)
        if e1.typ.nullable then
          (* Even if e1 is null, we can answer the operation if e2 is
           * empty: *)
          Printf.fprintf oc "(match %a with Null -> \
                               if %s = 0 && %s then NotNull true else Null \
                             | NotNull in0_ -> "
            (conv_to ~env ~context ~opc (Some larger_t)) e1
            csts_len (string_of_bool (non_csts = []))
        else
          Printf.fprintf oc "(let in0_ = %a in "
            (conv_to ~env ~context ~opc (Some larger_t)) e1 ;
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
            if e.E.typ.nullable (* not possible ATM *) then (
              Printf.fprintf oc
                "if (match %a with Null -> _ret_ := Null ; false \
                 | NotNull in1_ -> in0_ = in1_) then true else "
                (conv_to ~env ~context ~opc (Some larger_t)) e ;
              true
            ) else (
              Printf.fprintf oc "if in0_ = %a then %strue else "
                (conv_to ~env ~context ~opc (Some larger_t)) e
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
                (conv_to ~env ~context ~opc (Some larger_t)) e)) csts in
        emit_in csts_len csts_hash_init non_csts
      | E.{ text = (Stateless (SL1 (Path _, _)) | Binding (RecordField _)) ;
            typ = { structure = (TVec (_, telem) | TList telem) ; _ } ; _ } ->
        let csts_len =
          Printf.sprintf2 "Array.length (%a)"
            (emit_expr ~env ~context ~opc) e2
        and csts_hash_init larger_t =
          Printf.sprintf2
            "Array.iter (fun e_ -> Hashtbl.replace h_ ((%a) e_) ()) (%a)"
            (conv_from_to ~nullable:telem.nullable)
              (telem.structure, larger_t)
            (emit_expr ~env ~context ~opc) e2 in
        emit_in csts_len csts_hash_init []
      | _ -> assert false)
    | _ -> assert false)

  | Finalize, Stateless (SL2 (Percentile, p, lst)), _ ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.percentile"
      [Some TFloat, PropagateNull; None, PropagateNull] oc [p; lst]

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
      Printf.fprintf oc "%a true"
        (conv_from_to ~nullable:false) (TBool, t))
  | UpdateState, Stateful (_, n, SF1 (AggrAnd, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "(&&)" oc [ Some TBool, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrAnd, _)), TBool ->
    finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []
  | InitState, Stateful (_, _, SF1 (AggrOr, _)), (TBool as t) ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "%a false"
        (conv_from_to ~nullable:false) (TBool, t))
  | UpdateState, Stateful (_, n, SF1 (AggrOr, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "(||)" oc [ Some TBool, PropagateNull ]
  | Finalize, Stateful (_, n, SF1 (AggrOr, _)), TBool ->
    finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []

  | InitState, Stateful (_, _, SF1 (AggrSum, _)),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "%a Uint8.zero"
        (conv_from_to ~nullable:false) (TU8, t))
  | UpdateState, Stateful (_, n, SF1 (AggrSum, e)),
    (TFloat|TU8|TU16|TU32|TU64|TU128|TI8|TI16|TI32|TI64|TI128 as t) ->
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
      [k; E.one (); any_constant_of_expr_type e.E.typ]
  | UpdateState, Stateful (_, n, SF2 (Lag, _k, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.Seasonal.add" oc [ None, PropagateNull ]
  | Finalize, Stateful (_, n, SF2 (Lag, _, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Seasonal.lag" [] oc []

  (* We force the inputs to be float since we are going to return a float anyway. *)
  | InitState, Stateful (_, _, SF3 ((MovingAvg|LinReg), p, n, _)), TFloat ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.Seasonal.init"
      [Some TU32, PropagateNull; Some TU32, PropagateNull;
       Some TFloat, PropagateNull] oc
      [p; n; E.zero ()]
  | UpdateState, Stateful (_, n, SF3 ((MovingAvg|LinReg), _, _, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.Seasonal.add" oc [ Some TFloat, PropagateNull ]
  | Finalize, Stateful (_, n, SF3 (MovingAvg, p, m, _)), TFloat ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Seasonal.avg" [p; m] oc
      [Some TU32, PropagateNull; Some TU32, PropagateNull]
  | Finalize, Stateful (_, n, SF3 (LinReg, p, m, _)), TFloat ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Seasonal.linreg" [p; m] oc
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
      Printf.fprintf oc "%a Uint8.zero"
        (conv_from_to ~nullable:false) (TU8, t))
  | UpdateState, Stateful (_, n, SF2 (ExpSmooth, a, e)), _ ->
    update_state ~env ~opc ~nullable n my_state [ a ; e ]
      "CodeGenLib.smooth" oc
      [ Some TFloat, PropagateNull; Some TFloat, PropagateNull ]
  | Finalize, Stateful (_, n, SF2 (ExpSmooth, _, _)), TFloat ->
    finalize_state ~env ~opc ~nullable n my_state "identity" [] oc []

  | InitState, Stateful (_, _, SF4s (Remember, fpr,_tim,dur,_es)), TBool ->
    emit_functionN ~env ~opc ~nullable "CodeGenLib.Remember.init"
      [Some TFloat, PropagateNull; Some TFloat, PropagateNull] oc [fpr; dur]
  | UpdateState, Stateful (_, n, SF4s (Remember, _fpr, tim, _dur, es)), _ ->
    update_state ~env ~opc ~nullable n my_state (tim :: es)
      ~args_as:(Tuple 2) "CodeGenLib.Remember.add" oc
      ((Some TFloat, PropagateNull) :: List.map (fun _ -> None, PropagateNull) es)
  | Finalize, Stateful (_, n, SF4s (Remember, _, _, _, _)), TBool ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Remember.finalize" [] oc []

  | InitState, Stateful (_, _, Distinct _es), _ ->
    wrap_nullable ~nullable oc (fun oc ->
      String.print oc "CodeGenLib.Distinct.init ()")
  | UpdateState, Stateful (_, n, Distinct es), _ ->
    update_state ~env ~opc ~nullable n my_state es
      ~args_as:(Tuple 1) "CodeGenLib.Distinct.add" oc
      (List.map (fun _ -> None, PropagateNull) es)
  | Finalize, Stateful (_, n, Distinct _), TBool ->
    finalize_state ~env ~opc ~nullable n my_state
      "CodeGenLib.Distinct.finalize" [] oc []

  | InitState, Stateful (_, _, SF3 (Hysteresis, _, _, _)), t ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "%a true" (* Initially within bounds *)
        (conv_from_to ~nullable:false) (TBool, t))
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

  | InitState, Stateful (_, _, Last (c, _, _)), _ ->
    wrap_nullable ~nullable oc (fun oc ->
      Printf.fprintf oc "CodeGenLib.Last.init (%a)"
        (conv_to ~env ~context:Finalize ~opc (Some TU32)) c)
  (* Special updater that use the internal count when no `by` expressions
   * are present: *)
  | UpdateState, Stateful (_, n, Last (_, e, [])), _ ->
    update_state ~env ~opc ~nullable n my_state [ e ]
      "CodeGenLib.Last.add_on_count" oc [ None, PassNull ]
  | UpdateState, Stateful (_, n, Last (_, e, es)), _ ->
    update_state ~env ~opc ~nullable n my_state (e :: es)
      ~args_as:(Tuple 2) "CodeGenLib.Last.add" oc
      ((None, PassNull) :: List.map (fun _ -> None, PassNull) es)
  | Finalize, Stateful (_, n, Last (_, _, _)), _ ->
    finalize_state ~env ~opc ~nullable n my_state
      ~impl_return_nullable:true
      "CodeGenLib.Last.finalize" [] oc []

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

let rec emit_sersize_of_var typ nullable oc var =
  if nullable then (
    Printf.fprintf oc
      "\n\t\t\t(match %s with Null -> 0 | NotNull %s -> %a)"
      var var
      (emit_sersize_of_var typ false) var
  ) else (
    let emit_for_tuple ts =
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
    in
    match typ with
    | TTuple ts ->
        emit_for_tuple ts
    | TRecord kts ->
        let ts = RingBufLib.ser_array_of_record kts |> Array.map snd in
        emit_for_tuple ts
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
      Printf.fprintf oc "(Array.fold_left (fun s_ v_ ->\n\
        \t\t\t\ts_ + %a)\n\
        \t\t\t\t(%d + RingBufLib.nullmask_sz_of_vector (Array.length %s)) %s)"
        (emit_sersize_of_var t.structure t.nullable) "v_"
        (* start from the size prefix and nullmask: *)
        RingBufLib.sersize_of_u32 var
        var
    | _ -> emit_sersize_of_fixsz_typ oc typ
  )

(* Given the name of a variable with the fieldmask, emit a given code for
 * every values to be sent.
 * We suppose that the output value is in out_var.
 * Each code block return a value that is finally returned into out_var. *)
let rec emit_for_serialized_fields typ copy skip fm_var val_var oc out_var =
  if is_scalar typ.structure then
    Printf.fprintf oc
      "let %s =\n\
         if %s = RamenFieldMask.Copy then (%a) else (\
         assert (%s = RamenFieldMask.Skip) ; %a) in\n"
      out_var
      fm_var
      copy (val_var, typ)
      fm_var
      skip (val_var, typ)
  else
    let emit_for_tuple ts =
      Printf.fprintf oc
        "let %s = match %s with\n\
        | RamenFieldMask.Copy -> %a\n\
        | RamenFieldMask.Skip -> %a\n\
        | RamenFieldMask.Rec fm_ ->\n"
        out_var fm_var
        copy (val_var, typ)
        skip (val_var, typ) ;
      (* Destructure the tuple, propagating Nulls: *)
      Printf.fprintf oc
        "let %a = "
        (array_print_i ~first:"" ~last:"" ~sep:"," (fun j oc _ ->
          Printf.fprintf oc "tup_%d_" j)) ts ;
      if typ.nullable then
        Array.print ~first:"" ~last:"" ~sep:","
                    (fun oc _ -> String.print oc "Null") oc ts
      else
        String.print oc val_var ;
      Printf.fprintf oc
        " in\n
         %a
         %s in"
        (array_print_i ~first:"" ~last:"" ~sep:"\n" (fun j oc t ->
          let val_var = Printf.sprintf "tup_%d_" j
          and fm_var = Printf.sprintf "fm_.(%d)" j in
          emit_for_serialized_fields t copy skip fm_var val_var oc
                                     out_var)) ts
        out_var
    in
    match typ.structure with
    | TVec (_, t) | TList t ->
        Printf.fprintf oc
          "let %s = match %s with\n\
          | RamenFieldMask.Copy -> %a\n\
          | RamenFieldMask.Skip -> %a\n\
          | RamenFieldMask.Rec fm_ ->\n"
          out_var fm_var
          copy (val_var, typ)
          skip (val_var, typ) ;
        Printf.fprintf oc
          "Array.fold_lefti (fun %s i_ fm_ ->\n"
          out_var ;
        (* When we want to serialize subfields of a value that is null, we
         * have to serialize each subfield as null: *)
        if typ.nullable then
          Printf.fprintf oc
            "match %s with Null -> %a | NotNull %s ->\n"
            val_var skip (val_var, typ) val_var ;
        if t.nullable then
          (* For arrays but especially lists of nullable elements, make it
           * possible to fetch beyond the boundaries of the list: *)
          Printf.fprintf oc
            "let x_ = try %s.(i_) with Invalid_argument -> Null in\n"
            val_var
        else
          Printf.fprintf oc "let x_ = %s.(i_) in\n" val_var ;
        Printf.fprintf oc
          "%a\n\
           %s
          ) %s fm_ in\n"
          (emit_for_serialized_fields t copy skip "fm_" "x_") out_var
          out_var
          out_var
    | TTuple ts ->
        emit_for_tuple ts
    | TRecord kts ->
        let ts = RingBufLib.ser_array_of_record kts |> Array.map snd in
        emit_for_tuple ts
    | _ -> assert false (* no other non-scalar types *)

let emit_for_serialized_fields_of_output ser_typ copy skip fm_var oc out_var =
  List.iteri (fun i field ->
    Printf.fprintf oc "\n\t\t(* Field %a *)\n"
      RamenName.field_print field.RamenTuple.name ;
    let val_var = id_of_field_typ ~tuple:TupleOut field in
    let fm_var = Printf.sprintf "%s.(%d)" fm_var i in
    emit_for_serialized_fields field.typ copy skip fm_var val_var oc out_var
  ) ser_typ

(* Same as the above [emit_for_serialized_fields] but for when we do not know
 * the actual value, just its type. *)
let rec emit_for_serialized_fields_no_value typ copy skip fm_var oc out_var =
  if is_scalar typ.structure then
    Printf.fprintf oc
      "let %s =\n\
         if %s = RamenFieldMask.Copy then (%a) else (\
         assert (%s = RamenFieldMask.Skip) ; %a) in\n"
      out_var
      fm_var
      copy typ
      fm_var
      skip typ
  else
    let emit_for_tuple ts =
      Printf.fprintf oc
        "let %s = match %s with\n\
        | RamenFieldMask.Copy -> %a\n\
        | RamenFieldMask.Skip -> %a\n\
        | RamenFieldMask.Rec fm_ ->\n\
            %a
            %s in"
        out_var
        fm_var
        copy typ
        skip typ
        (array_print_i ~first:"" ~last:"" ~sep:"\n" (fun j oc t ->
          let fm_var = Printf.sprintf "fm_.(%d)" j in
          emit_for_serialized_fields_no_value t copy skip fm_var oc out_var)) ts
        out_var
    in
    match typ.structure with
    | TVec (_, t) | TList t ->
        Printf.fprintf oc
          "let %s = match %s with\n\
          | RamenFieldMask.Copy -> %a\n\
          | RamenFieldMask.Skip -> %a\n\
          | RamenFieldMask.Rec fm_ ->\n\
              Array.fold_lefti (fun %s i_ fm_ ->\n\
                %a\n\
                %s
              ) %s fm_ in\n"
          out_var fm_var
          copy typ
          skip typ
          out_var
          (emit_for_serialized_fields_no_value t copy skip "fm_") out_var
          out_var
          out_var
    | TTuple ts ->
        emit_for_tuple ts
    | TRecord ts ->
        Array.map snd ts |> emit_for_tuple
    | _ -> assert false (* no other non-scalar types *)

let emit_for_serialized_fields_of_output_no_value ser_typ copy skip fm_var oc out_var =
  List.iteri (fun i field ->
    Printf.fprintf oc "\n\t\t(* Field %a *)\n"
      RamenName.field_print field.RamenTuple.name ;
    let fm_var = Printf.sprintf "%s.(%d)" fm_var i in
    emit_for_serialized_fields_no_value field.typ copy skip fm_var oc out_var
  ) ser_typ

let emit_compute_nullmask_size fm_var oc ser_typ =
  let copy oc typ =
    String.print oc (if typ.nullable then "b_+1" else "b_") in
  let skip oc _ = String.print oc "b_" in
  Printf.fprintf oc "\t\tlet b_ = 0 in\n" ;
  emit_for_serialized_fields_of_output_no_value
    ser_typ copy skip fm_var oc "b_" ;
  Printf.fprintf oc "\t\tRingBuf.(round_up_to_rb_word (bytes_for_bits b_))"

(* The actual nullmask size will depend on the fieldmask which is known
 * only at runtime: *)
let emit_sersize_of_tuple name oc tuple_typ =
  (* We want the sersize of the serialized version of course: *)
  let ser_typ = RingBufLib.ser_tuple_typ_of_tuple_typ tuple_typ in
  (* Like for serialize_tuple, we receive first the fieldmask and then the
   * actual tuple, so we can compute the nullmask in advance: *)
  Printf.fprintf oc "let %s fieldmask_ =\n" name ;
  Printf.fprintf oc "\tlet nullmask_bytes_ = %a in\n"
    (emit_compute_nullmask_size "fieldmask_") ser_typ ;
  Printf.fprintf oc "\tassert (nullmask_bytes_ <= %d) ;\n"
    (RingBufLib.nullmask_bytes_of_tuple_type ser_typ) ;
  Printf.fprintf oc "\tfun %a ->\n"
    (emit_tuple TupleOut) tuple_typ ;
  Printf.fprintf oc "\t\tlet sz_ = nullmask_bytes_ in\n" ;
  let copy oc (out_var, typ) =
    Printf.fprintf oc "sz_ + %a"
      (emit_sersize_of_var typ.structure typ.nullable) out_var
  and skip oc _ = String.print oc "sz_" in
  emit_for_serialized_fields_of_output ser_typ copy skip "fieldmask_" oc "sz_" ;
  String.print oc "\tsz_\n"

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
 * First comes the header (writen by the caller, not our concern here)
 * Then comes the nullmask for the toplevel "structure", with one bit per
 * nullable value that will be copied.
 * Then the values.
 * For list values, we start with the number of elements.
 * Then, for lists, vectors and tuples we have a small local nullmask
 * (for tuples, even for fields that are not nullable, FIXME). *)
let emit_serialize_tuple name oc tuple_typ =
  (* Serialize in tuple_typ.name order: *)
  let ser_typ = RingBufLib.ser_tuple_typ_of_tuple_typ tuple_typ in
  Printf.fprintf oc "let %s fieldmask_ =\n" name ;
  Printf.fprintf oc "\tlet nullmask_bytes_ = %a in\n"
    (emit_compute_nullmask_size "fieldmask_") ser_typ ;
  Printf.fprintf oc "\tfun tx_ start_offs_ %a ->\n"
    (emit_tuple TupleOut) tuple_typ ;
  if verbose_serialization then
    Printf.fprintf oc
      "\t\t!RamenLog.logger.RamenLog.debug \"Serialize a tuple, nullmask_bytes=%%d\" nullmask_bytes_ ;\n" ;

  (* callbacks [copy] and [skip] have to return the offset and null index
   * but we have several offsets and several null index (when copying full
   * compund types) ; we therefore enforce the rule that those variables
   * are always called "offs_" and "nulli_". *)
  Printf.fprintf oc "\t\tlet offs_ = start_offs_ + nullmask_bytes_\n\
                     \t\tand nulli_ = 0 in\n" ;
  (*
   * Write a full value, updating offs_var and nulli_var:
   * [start_var]: where we write the value (and where the nullmask is, for
   *              values with a nullmask).
   *)
  let rec emit_write start_var val_var nullable oc typ =
    if nullable then (
      (* Write either nothing (since the nullmask is initialized with 0) or
       * the nullmask bit and the value *)
      Printf.fprintf oc "\t\t\t\t(match %s with\n" val_var ;
      Printf.fprintf oc "\t\t\t\t| Null -> offs_, nulli_ + 1\n" ;
      Printf.fprintf oc "\t\t\t\t| NotNull %s ->\n" val_var ;
      Printf.fprintf oc "\t\t\t\t\tRingBuf.set_bit tx_ %s nulli_ ;\n" start_var ;
      Printf.fprintf oc "\t\t\t\t\tlet offs_, nulli_ = %a in\n\
                         \t\t\t\t\toffs_, nulli_ + 1)\n"
        (emit_write start_var val_var false) typ
    ) else (
      let emit_write_array dim_var t =
        if verbose_serialization then
          Printf.fprintf oc "\t\t\t!RamenLog.logger.RamenLog.debug \"Serializing an array of size %%d at offset %%d\" %s offs_ ;\n" dim_var ;
        Printf.fprintf oc
          "\t\t\t(let nullmask_bytes_ = RingBufLib.nullmask_sz_of_vector %s in\n"
          dim_var ;
        Printf.fprintf oc
          "\t\t\tRingBuf.zero_bytes tx_ offs_ nullmask_bytes_ ;\n" ;
        Printf.fprintf oc
          "\t\t\tlet start_arr_ = offs_ in\n\
           \t\t\tlet offs_ = offs_ + nullmask_bytes_ in\n" ;
        Printf.fprintf oc
          "\t\t\tlet offs_, _ =\n\
           \t\t\t\tArray.fold_left (fun (offs_, nulli_) v_ ->\n\
           \t\t\t\t\t%a\n\
           \t\t\t\t) (offs_, 0) %s in\n\
           \t\t\toffs_, nulli_)\n"
          (emit_write "start_arr_" "v_" t.nullable) t.structure
          val_var
      and emit_write_tuple ts =
        if verbose_serialization then
          Printf.fprintf oc "\t\t\t!RamenLog.logger.RamenLog.debug \"Serializing a tuple of %d elements at offset %%d\" offs_ ;\n" (Array.length ts) ;
        Printf.fprintf oc "\t\t\tlet %a = %s in\n"
          (array_print_as_tuple_i (fun oc i _ ->
            let item_var = val_var ^"_"^ string_of_int i in
            String.print oc item_var)) ts
          val_var ;
        let nullmask_sz = RingBufLib.nullmask_sz_of_tuple ts in
        if nullmask_sz > 0 then
          Printf.fprintf oc "\t\t\tRingBuf.zero_bytes tx_ offs_ %d ;\n"
            nullmask_sz ;
        Printf.fprintf oc
          "\t\t\tlet start_tup_ = offs_\n\
           \t\t\tand offs_ = offs_ + %d (* nullmask *) in\n"
          nullmask_sz ;
        Array.iteri (fun i t ->
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\t\t\tlet offs_, _ = (let nulli_ = 0 in %a) in\n"
            (emit_write "start_tup_" item_var t.nullable) t.structure
        ) ts ;
        Printf.fprintf oc "\t\t\t\toffs_, nulli_"
      in
      match typ with
      (* Constructed types: *)
      | TTuple ts ->
          emit_write_tuple ts

      | TRecord kts ->
          let ts = RingBufLib.ser_array_of_record kts |> Array.map snd in
          emit_write_tuple ts

      | TVec (d, t) ->
          emit_write_array (string_of_int d) t

      | TList t ->
          Printf.fprintf oc "\t\t\tlet d_ = Array.length %s in\n" val_var ;
          Printf.fprintf oc "\t\t\tRingBuf.write_u32 tx_ offs_ (Uint32.of_int d_) ;\n" ;
          Printf.fprintf oc "\t\t\tlet offs_ = offs_ + RingBufLib.sersize_of_u32 in\n" ;
          emit_write_array "d_" t

      (* Scalar types: *)
      | t ->
          if verbose_serialization then
            Printf.fprintf oc "\t\t\t\t!RamenLog.logger.RamenLog.debug \"Serializing %s (%%s) at offset %%d\" (dump %s) offs_ ;\n" val_var val_var ;
          Printf.fprintf oc "\t\t\t\tRingBuf.write_%s tx_ offs_ %s ;\n"
            (id_of_typ t) val_var ;
          Printf.fprintf oc "\t\t\t\toffs_ + %a, nulli_\n"
            (emit_sersize_of_var t false) val_var
    ) in
  (* Start by zeroing the nullmask *)
  Printf.fprintf oc
    "\t\tif nullmask_bytes_ > 0 then\n\
     \t\t\tRingBuf.zero_bytes tx_ start_offs_ nullmask_bytes_ ;\n" ;
  (* All nullable values found in the fieldmask will have its nullbit in the
   * global nullmask at start_offs: *)
  let copy oc (out_var, typ) =
    emit_write "start_offs_" out_var typ.nullable oc typ.structure
  and skip oc _ =
    (* We must return offs and null_idx as [copy] does (unchanged here,
     * since the field is not serialized). *)
    Printf.fprintf oc "offs_, nulli_"
  in
  emit_for_serialized_fields_of_output ser_typ copy skip "fieldmask_" oc "(offs_, nulli_)" ;
  String.print oc "\toffs_\n"

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
    Printf.fprintf oc
      "\t\t\t!RamenLog.logger.RamenLog.error \"Cannot parse field %d: %s\" ;\n"
      (i+1)
      (field_typ.name :> string) ;
    Printf.fprintf oc "\t\t\traise exn))%s\n" sep ;
  ) tuple_typ ;
  Printf.fprintf oc "\t)\n"

let emit_time_of_tuple name oc opc =
  let open RamenEventTime in
  Printf.fprintf oc "let %s %a =\n\t"
    name
    (emit_tuple TupleOut) opc.tuple_typ ;
  (match opc.event_time with
  | None -> String.print oc "None"
  | Some _ -> Printf.fprintf oc "Some (%a)" emit_event_time opc) ;
  String.print oc "\n\n"

let emit_factors_of_tuple name oc opc =
  let factors =
    match opc.op with
    | Some op -> O.factors_of_operation op
    | None -> [] in
  Printf.fprintf oc "let %s %a = [|\n"
    name
    (emit_tuple TupleOut) opc.tuple_typ ;
  List.iter (fun factor ->
    let typ =
      (List.find (fun t -> t.RamenTuple.name = factor) opc.tuple_typ).typ in
    Printf.fprintf oc "\t%S, %a %s ;\n"
      (factor :> string)
      emit_value typ
      (id_of_field_name ~tuple:TupleOut factor)
  ) factors ;
  (* TODO *)
  String.print oc "|]\n\n"

(* Given a tuple type, generate the ReadCSVFile operation. *)
let emit_read_csv_file opc oc name csv_fname unlink
                       csv_separator csv_null preprocessor =
  let const_string_of e =
    Printf.sprintf2 "(%a)"
      (emit_expr ~context:Finalize ~opc ~env:[]) e
  in
  let preprocessor =
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
     "%a\n%a\n%a\n%a\n%a\n\
     let %s () =\n\
       \tlet unlink_ = %a in
       \tCodeGenLib_Skeletons.read_csv_file %s\n\
       \t\tunlink_ %S sersize_of_tuple_ time_of_tuple_\n\
       \t\tfactors_of_tuple_ serialize_tuple_\n\
       \t\ttuple_of_strings_ %s field_of_params_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") opc.tuple_typ
    (emit_time_of_tuple "time_of_tuple_") opc
    (emit_factors_of_tuple "factors_of_tuple_") opc
    (emit_serialize_tuple "serialize_tuple_") opc.tuple_typ
    (emit_tuple_of_strings "tuple_of_strings_" csv_null) opc.tuple_typ
    name
    (emit_expr ~env:[] ~context:Finalize ~opc) unlink
    csv_fname csv_separator preprocessor

let emit_listen_on opc oc name net_addr port proto =
  let open RamenProtocols in
  let tuple_typ = tuple_typ_of_proto proto in
  let collector = collector_of_proto proto in
  Printf.fprintf oc "%a\n%a\n%a\n%a\n\
    let %s () =\n\
      \tCodeGenLib_Skeletons.listen_on\n\
      \t\t(%s ~inet_addr:(Unix.inet_addr_of_string %S) ~port:%d)\n\
      \t\t%S sersize_of_tuple_ time_of_tuple_ factors_of_tuple_\n\
      \t\tserialize_tuple_\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") tuple_typ
    (emit_time_of_tuple "time_of_tuple_") opc
    (emit_factors_of_tuple "factors_of_tuple_") opc
    (emit_serialize_tuple "serialize_tuple_") tuple_typ
    name
    collector
    (Unix.string_of_inet_addr net_addr) port
    (string_of_proto proto)

let emit_well_known opc oc name from
                    unserializer_name ringbuf_envvar worker_and_time =
  let open RamenProtocols in
  Printf.fprintf oc "%a\n%a\n\n%a%a\n\
    let %s () =\n\
      \tCodeGenLib_Skeletons.read_well_known %a\n\
      \t\tsersize_of_tuple_ time_of_tuple_ factors_of_tuple_\n\
      \t\tserialize_tuple_ %s %S %s\n"
    (emit_sersize_of_tuple "sersize_of_tuple_") opc.tuple_typ
    (emit_time_of_tuple "time_of_tuple_") opc
    (emit_factors_of_tuple "factors_of_tuple_") opc
    (emit_serialize_tuple "serialize_tuple_") opc.tuple_typ
    name
    (List.print (fun oc ds ->
      Printf.fprintf oc "%S" (
        IO.to_string (O.print_data_source true) ds))) from
   unserializer_name ringbuf_envvar worker_and_time

(* We do not want to read the value from the RB each time it's used,
 * so extract a tuple from the ring buffer. *)
let emit_read_tuple name ?(is_yield=false) oc typ =
  (* Deserialize in in_tuple_typ.name order: *)
  let ser_typ = RingBufLib.ser_tuple_typ_of_tuple_typ typ in
  Printf.fprintf oc "let %s tx_ =\n" name ;
  if is_yield then (
    (* Yield produce only tuples for the live channel: *)
    Printf.fprintf oc "\
      \tlet m_ = RingBufLib.(DataTuple RamenChannel.live) in\n\
      \tlet start_offs_ = 0 in\n"
  ) else (
    Printf.fprintf oc "
      \tmatch RingBufLib.read_message_header tx_ 0 with\n\
      \t|RingBufLib.EndOfReplay _ as m_ -> m_, None\n\
      \t|RingBufLib.DataTuple _ as m_ ->\n\
      \t\tlet start_offs_ = RingBufLib.message_header_sersize m_ in\n"
  ) ;
  Printf.fprintf oc
    "\tlet offs_ = start_offs_ + %d in\n"
    (RingBufLib.nullmask_bytes_of_tuple_type ser_typ) ;
  if verbose_serialization then
    Printf.fprintf oc "\t!RamenLog.logger.RamenLog.debug \"Deserializing a tuple\" ;\n" ;
  (*
   * All the following emit_* functions Return value, offset:
   *)
  let rec emit_read_array tx_var offs_var dim_var oc t =
    Printf.fprintf oc
      "\t\tlet arr_start_ = %s\n\
       \t\tand offs_arr_ = ref (%s + (RingBufLib.nullmask_sz_of_vector %s)) in\n"
      offs_var
      offs_var dim_var ;
    Printf.fprintf oc
      "\t\tArray.init %s (fun bi_ ->\n\
       \t\t\tlet v_, o_ = %a in\n\
       \t\t\toffs_arr_ := o_ ; v_), !offs_arr_\n"
      dim_var
      (emit_read_value tx_var "arr_start_" "!offs_arr_" "v_" t.nullable "bi_") t.structure
  and emit_read_value tx_var start_offs_var offs_var val_var
                      nullable nulli_var oc structure =
    if nullable then (
      Printf.fprintf oc "\
        \t\tif RingBuf.get_bit %s %s %s then (\n\
        \t\tlet %s, %s =\n\
        \t%a in\n
        \t\tNotNull %s, %s\n
        \t\t) else Null, %s"
        tx_var start_offs_var nulli_var
        val_var offs_var
        (emit_read_value tx_var start_offs_var offs_var val_var false nulli_var) structure
        val_var offs_var
        offs_var
    ) else (
      let emit_for_tuple ts =
        let nullmask_sz = RingBufLib.nullmask_sz_of_tuple ts in
        Printf.fprintf oc "\t\tlet tuple_start_ = %s and offs_tup_ = %s + %d in\n"
          offs_var offs_var nullmask_sz ;
        Array.iteri (fun i t ->
          let item_var = val_var ^"_"^ string_of_int i in
          Printf.fprintf oc "\t\tlet bi_ = %d in\n\
                             \t\tlet %s, offs_tup_ = %a in\n"
            i
            item_var
            (emit_read_value tx_var "tuple_start_" "offs_tup_" item_var t.nullable "bi_") t.structure
        ) ts ;
        Printf.fprintf oc "\t%a, offs_tup_"
          (array_print_as_tuple_i (fun oc i _ ->
            let item_var = val_var ^"_"^ string_of_int i in
            String.print oc item_var)) ts
      in
      match structure with
      (* Constructed types are prefixed with a nullmask and then read item by
       * item: *)
      | TTuple ts ->
          emit_for_tuple ts

      | TRecord kts ->
          let ts = RingBufLib.ser_array_of_record kts |> Array.map snd in
          if Array.length ts <> Array.length kts then
            Printf.sprintf2 "Cannot deserialize a record of type %a \
                             which has private/shadowed fields"
              T.print_structure structure |>
            failwith ;
          emit_for_tuple ts

      | TVec (d, t) ->
          emit_read_array tx_var offs_var (string_of_int d) oc t

      | TList t ->
          (* List are like vectors but prefixed with the actual number of
           * elements: *)
          Printf.fprintf oc
            "\t\tlet d_, offs_lst_ = Uint32.to_int (RingBuf.read_u32 %s %s), %s + %d in\n"
            tx_var offs_var offs_var RingBufLib.sersize_of_u32 ;
          emit_read_array tx_var "offs_lst_" "d_" oc t

      (* Non constructed types: *)
      | _ ->
          Printf.fprintf oc "\
            \t\tRingBuf.read_%s %s %s, %s + %a"
            (id_of_typ structure) tx_var offs_var
            offs_var (emit_sersize_of_not_null_scalar tx_var offs_var) structure
    )
  in
  let _ = List.fold_left (fun nulli field ->
      let id = id_of_field_typ ~tuple:TupleIn field in
      Printf.fprintf oc "\tlet bi_ = %d in\n\
                         \tlet %s, offs_ =\n%a in\n"
        nulli
        id
        (emit_read_value "tx_" "start_offs_" "offs_" id field.typ.nullable "bi_")
          field.typ.structure ;
      nulli + (if field.typ.nullable then 1 else 0)
    ) 0 ser_typ in
  (* We want to output the tuple with fields ordered according to the
   * select clause specified order, not according to serialization order: *)
  Printf.fprintf oc "\tm_, Some %a\n"
    (emit_tuple TupleIn) typ

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

let emit_generate_tuples name in_typ out_typ ~opc oc selected_fields =
  let has_generator =
    List.exists (fun sf ->
      E.is_generator sf.O.expr)
      selected_fields in
  if not has_generator then
    Printf.fprintf oc "let %s f_ chan_ it_ ot_ = f_ chan_ it_ ot_\n" name
  else (
    Printf.fprintf oc "let %s f_ chan_ (%a as it_) %a =\n"
      name
      (emit_tuple ~with_alias:true TupleIn) in_typ
      (emit_tuple ~with_alias:true TupleOut) out_typ ;
    let env =
      add_tuple_environment TupleIn in_typ [] |>
      add_tuple_environment TupleOut out_typ in
    (* Each generator is a functional receiving the continuation and calling it
     * as many times as there are values. *)
    let num_gens =
      List.fold_left (fun num_gens sf ->
          if not (E.is_generator sf.O.expr) then num_gens
          else (
            let ff_ = "ff_"^ string_of_int num_gens ^"_" in
            Printf.fprintf oc "%a(fun %s -> %a) (fun generated_%d_ ->\n"
              emit_indent (1 + num_gens)
              ff_
              (emit_generator ff_ ~env ~opc) sf.O.expr
              num_gens ;
            num_gens + 1)
        ) 0 selected_fields in
    (* Now we have all the generated values, actually call f_ on the tuple.
     * Note that the tuple must be in out_typ order: *)
    Printf.fprintf oc "%af_ chan_ it_ (\n%a"
      emit_indent (1 + num_gens)
      emit_indent (2 + num_gens) ;
    let expr_of_field name =
      let sf = List.find (fun sf ->
                 sf.O.alias = name) selected_fields in
      sf.O.expr in
    let _ = List.fold_lefti (fun gi i ft ->
        if i > 0 then Printf.fprintf oc ",\n%a" emit_indent (2 + num_gens) ;
        match E.is_generator (expr_of_field ft.name) with
        | exception Not_found ->
          (* For star-imported fields: *)
          Printf.fprintf oc "%s"
            (id_of_field_name ft.name) ;
          gi
        | true ->
          Printf.fprintf oc "generated_%d_" gi ;
          gi + 1
        | false ->
          Printf.fprintf oc "%s"
            (id_of_field_name ~tuple:TupleOut ft.name) ;
          gi
        ) 0 out_typ in
    for _ = 1 to num_gens do Printf.fprintf oc ")" done ;
    Printf.fprintf oc ")\n"
  )

let emit_state_update_for_expr ~env ~what ~opc oc expr =
  let titled = ref false in
  E.unpure_iter (fun _ e ->
    match e.text with
    | Stateful _ ->
        if not !titled then (
          titled := true ;
          Printf.fprintf oc "\t(* State Update for %s: *)\n" what) ;
        emit_expr ~env ~context:UpdateState ~opc oc e
    | _ -> ()
  ) expr

let emit_where
      ?(with_group=false) ?(always_true=false)
      ~env name in_typ ~opc oc expr =
  Printf.fprintf oc "let %s global_ %a %a out_previous_ "
    name
    (emit_tuple ~with_alias:true TupleIn) in_typ
    (emit_tuple ~with_alias:true TupleMergeGreatest) in_typ ;
  let env =
    add_tuple_environment TupleIn in_typ env |>
    add_tuple_environment TupleMergeGreatest in_typ |>
    add_tuple_environment TupleOutPrevious opc.tuple_typ in
  if with_group then Printf.fprintf oc "group_ " ;
  if always_true then
    Printf.fprintf oc "= true\n"
  else (
    Printf.fprintf oc "=\n" ;
    (* Update the states used by this expression: *)
    emit_state_update_for_expr ~env~opc ~what:"where clause"
                               oc expr ;
    Printf.fprintf oc "\t%a\n"
      (emit_expr ~env~context:Finalize ~opc) expr
  )

let emit_field_selection
      (* If true, we update the env and finalize as few fields as
       * possible (only those required by commit_cond and update_states).
       * If false, we have the minimal tuple as an extra parameter, and
       * only have to build the final out_tuple (taking advantage of the
       * fields already computed in minimal_typ). And no need to update
       * states at all. *)
      ~build_minimal
      ~env name in_typ
      minimal_typ ~opc oc selected_fields =
  let field_in_minimal field_name =
    List.exists (fun ft ->
      ft.RamenTuple.name = field_name
    ) minimal_typ in
  let must_output_field field_name =
    not build_minimal || field_in_minimal field_name in
  Printf.fprintf oc "let %s %a out_previous_ group_ global_ "
    name
    (emit_tuple ~with_alias:true TupleIn) in_typ ;
  let env =
    add_tuple_environment TupleIn in_typ env |>
    add_tuple_environment TupleOutPrevious opc.tuple_typ in
  let env =
    if not build_minimal then (
      Printf.fprintf oc "%a "
        (emit_tuple ~with_alias:true TupleOut) minimal_typ ;
      add_tuple_environment TupleOut minimal_typ env
    ) else env in
  Printf.fprintf oc "=\n" ;
  List.fold_left (fun env sf ->
    if must_output_field sf.O.alias then (
      if build_minimal then (
        (* Update the states as required for this field, just before
         * computing the field actual value. *)
        let what = (sf.O.alias :> string) in
        emit_state_update_for_expr ~env ~opc ~what oc sf.O.expr ;
      ) ;
      if not build_minimal && field_in_minimal sf.alias then (
        (* We already have this binding *)
        env
      ) else (
        Printf.fprintf oc "\t(* Output field %s of type %a *)\n"
          (sf.O.alias :> string)
          T.print_typ sf.expr.E.typ ;
        let var_name =
          id_of_field_name ~tuple:TupleOut sf.O.alias in
        if E.is_generator sf.O.expr then (
          (* So that we have a single out_typ both before and after tuples generation *)
          Printf.fprintf oc "\tlet %s = () in\n" var_name
        ) else (
          Printf.fprintf oc "\tlet %s = %a in\n"
            var_name
            (emit_expr ~env ~context:Finalize ~opc)
              sf.O.expr) ;
        (* Make that field available in the environment for later users: *)
        (E.RecordField (TupleOut, sf.alias), var_name) :: env
      )
    ) else env
  ) env selected_fields |> ignore ;
  (* Here we must generate the tuple in the order specified by out_type,
   * not selected_fields: *)
  let is_selected name =
    List.exists (fun sf -> sf.O.alias = name) selected_fields in
  Printf.fprintf oc "\t(\n\t\t" ;
  List.iteri (fun i ft ->
    if must_output_field ft.name then (
      let tuple =
        if is_selected ft.name then TupleOut else TupleIn in
      Printf.fprintf oc "%s%s"
        (if i > 0 then ",\n\t\t" else "")
        (id_of_field_name ~tuple ft.name)
    ) else (
      Printf.fprintf oc "%s()"
        (if i > 0 then ", " else "")
    )
  ) opc.tuple_typ ;
  Printf.fprintf oc "\n\t)\n"

(* Fields that are part of the minimal tuple have had their states updated
 * while the minimal tuple was computed, but others have not. Let's do this
 * here: *)
let emit_update_states
      ~env name in_typ
      minimal_typ ~opc oc selected_fields =
  let field_in_minimal field_name =
    List.exists (fun ft ->
      ft.RamenTuple.name = field_name
    ) minimal_typ
  in
  Printf.fprintf oc "let %s %a out_previous_ group_ global_ %a =\n"
    name
    (emit_tuple ~with_alias:true TupleIn) in_typ
    (emit_tuple ~with_alias:true TupleOut) minimal_typ ;
  let env =
    add_tuple_environment TupleIn in_typ env |>
    add_tuple_environment TupleOut minimal_typ |>
    add_tuple_environment TupleOutPrevious opc.tuple_typ in
  List.iter (fun sf ->
    if not (field_in_minimal sf.O.alias) then (
      (* Update the states as required for this field, just before
       * computing the field actual value. *)
      let what = (sf.O.alias :> string) in
      emit_state_update_for_expr ~env ~opc ~what oc sf.O.expr)
  ) selected_fields ;
  Printf.fprintf oc "\t()\n"

(* Similar to emit_field_selection but with less options, no concept of star and no
 * naming of the fields as the fields from out, since that's not the out tuple
 * we are constructing: *)
let emit_key_of_input name in_typ ~opc oc exprs =
  Printf.fprintf oc "let %s %a =\n\t("
    name
    (emit_tuple ~with_alias:true TupleIn) in_typ ;
  let env = add_tuple_environment TupleIn in_typ [] in
  List.iteri (fun i expr ->
      Printf.fprintf oc "%s\n\t\t%a"
        (if i > 0 then "," else "")
        (emit_expr ~env ~context:Finalize ~opc) expr ;
    ) exprs ;
  Printf.fprintf oc "\n\t)\n"

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
          IO.to_string otype_of_type in
  let print_expr_structure oc e =
    e.E.typ.structure |> (* nullable taken care of below *)
    IO.to_string otype_of_type |>
    String.print oc in
  let nullable = if e.typ.nullable then " nullable" else "" in
  let print_expr_typ ~skip_null oc e =
    Printf.fprintf oc "%a%s"
      otype_of_type e.E.typ.structure
      (if e.typ.nullable && not skip_null then " nullable" else "")
  in
  match e.text with
  (* previous tuples and count ; Note: we could get rid of this count if we
   * provided some context to those functions, such as the event count in
   * current window, for instance (ie. pass the full aggr record not just
   * the fields) *)
  | Stateful (_, _, SF2 (Lag, _, _))
  | Stateful (_, _, SF3 ((MovingAvg|LinReg), _, _, _)) ->
    t ^" CodeGenLib.Seasonal.t"^ nullable
  | Stateful (_, _, SF4s (MultiLinReg, _, _, _, _)) ->
    "("^ t ^" * float array) CodeGenLib.Seasonal.t"^ nullable
  | Stateful (_, _, SF4s (Remember, _, _, _, _)) ->
    "CodeGenLib.Remember.state"^ nullable
  | Stateful (_, _, Distinct es) ->
    Printf.sprintf2 "%a CodeGenLib.Distinct.state%s"
      (list_print_as_product print_expr_structure) es
      nullable
  | Stateful (_, _, SF1 (AggrAvg, _)) -> "(int * float)"^ nullable
  | Stateful (_, _, SF1 ((AggrFirst|AggrLast|AggrMin|AggrMax), _)) ->
    t ^" nullable"^ nullable
  | Stateful (_, _, Top { what ; _ }) ->
    Printf.sprintf2 "%a HeavyHitters.t%s"
      (list_print_as_product print_expr_structure) what
      nullable
  | Stateful (_, n, Last (_, e, es)) ->
    if es = [] then
      (* In that case we use a special internal counter as the order: *)
      Printf.sprintf2 "(%a, int) CodeGenLib.Last.state%s"
        (print_expr_typ ~skip_null:n) e
        nullable
    else
      Printf.sprintf2 "(%a, %a) CodeGenLib.Last.state%s"
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
  | Stateful (_, _, SF1 (AggrHistogram _, _)) ->
    "CodeGenLib.Histogram.state"^ nullable
  | _ -> t ^ nullable

let emit_state_init name state_lifespan ~env other_params
      ?where ?commit_cond ~opc
      oc selected_fields =
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
    Printf.fprintf oc "type %s = unit\n" name ;
    Printf.fprintf oc "let %s%a = ()\n\n"
      name
      (List.print ~first:" " ~last:"" ~sep:" " String.print)
        other_params
  ) else (
    (* First emit the record type definition: *)
    Printf.fprintf oc "type %s = {\n" name ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf oc "\tmutable %s : %s (* %a *) ;\n"
          (name_of_state f)
          (otype_of_state f)
          T.print_typ f.E.typ ;
        (* Only used when skip_nulls: *)
        Printf.fprintf oc "\tmutable %s_empty_ : bool ;\n"
          (name_of_state f)
      ) ;
    Printf.fprintf oc "}\n\n" ;
    (* Then the initialization function proper: *)
    Printf.fprintf oc "let %s%a =\n"
      name
      (List.print ~first:" " ~last:"" ~sep:" " String.print)
        other_params ;
    let _state =
      fold_my_unpure_fun env (fun env f ->
        let n = name_of_state f in
        Printf.fprintf oc "\tlet %s = %a in\n"
          n
          (emit_expr ~context:InitState ~opc ~env) f ;
        (* Make this state available under that name for following exprs: *)
        (E.State f.uniq_num, n) :: env) in
    (* And now build the state record from all those fields: *)
    Printf.fprintf oc "\t{" ;
    for_each_my_unpure_fun (fun f ->
        Printf.fprintf oc " %s ; %s_empty_ = true ; "
          (name_of_state f) (name_of_state f)) ;
    Printf.fprintf oc " }\n"
  )

(* Note: we need group_ in addition to out_tuple because the commit-when clause
 * might have its own stateful functions going on *)
let emit_when ~env name in_typ minimal_typ ~opc oc
              when_expr =
  Printf.fprintf oc "let %s %a out_previous_ group_ global_ %a =\n"
    name
    (emit_tuple ~with_alias:true TupleIn) in_typ
    (emit_tuple ~with_alias:true TupleOut) minimal_typ ;
  let env =
    add_tuple_environment TupleIn in_typ env |>
    add_tuple_environment TupleOut minimal_typ |>
    add_tuple_environment TupleOutPrevious opc.tuple_typ in
  (* Update the states used by this expression: *)
  emit_state_update_for_expr ~env ~opc ~what:"commit clause" oc when_expr ;
  Printf.fprintf oc "\t%a\n"
    (emit_expr ~env ~context:Finalize ~opc) when_expr

(* Depending on what uses a commit/flush condition, we might need to check
 * all groups after every single input tuple (very slow), or after every
 * selected input tuple (still quite slow), or only when this group is
 * modified (fast). Users should limit all/selected tuple to aggregations
 * with few groups only. *)
let when_to_check_group_for_expr expr =
  (* Tells whether the commit condition needs the all or the selected tuple *)
  let need_all =
    try
      E.iter (fun _ e ->
        match e.E.text with
        | Stateless (SL1 (Path _, { text = Variable TupleIn ; _ }))
        | Binding (RecordField (TupleIn, _)) ->
            raise Exit
        | _ -> ()
      ) expr ;
      false
    with Exit ->
      true
  in
  if need_all then "CodeGenLib_Skeletons.ForAll"
  else "CodeGenLib_Skeletons.ForInGroup"

let emit_sort_expr name in_typ ~opc oc es_opt =
  Printf.fprintf oc "let %s sort_count_ %a %a %a %a =\n"
    name
    (emit_tuple ~with_alias:true TupleSortFirst) in_typ
    (emit_tuple ~with_alias:true TupleIn) in_typ
    (emit_tuple ~with_alias:true TupleSortSmallest) in_typ
    (emit_tuple ~with_alias:true TupleSortGreatest) in_typ ;
  let env =
    add_tuple_environment TupleSortFirst in_typ [] |>
    add_tuple_environment TupleIn in_typ |>
    add_tuple_environment TupleSortSmallest in_typ |>
    add_tuple_environment TupleSortGreatest in_typ in
  match es_opt with
  | [] ->
      (* The default sort_until clause must be false.
       * If there is no sort_by clause, any constant will do: *)
      Printf.fprintf oc "\tfalse\n"
  | es ->
      Printf.fprintf oc "\t%a\n"
        (List.print ~first:"(" ~last:")" ~sep:", "
           (emit_expr ~env ~context:Finalize ~opc)) es

let emit_merge_on name in_typ ~opc oc es =
  let env = add_tuple_environment TupleIn in_typ [] in
  Printf.fprintf oc "let %s %a =\n\t%a\n"
    name
    (emit_tuple ~with_alias:true TupleIn) in_typ
    (List.print ~first:"(" ~last:")" ~sep:", "
       (emit_expr ~env ~context:Finalize ~opc)) es

let emit_notification_tuple ~env ~opc oc notif =
  let print_expr = emit_expr ~env ~context:Finalize ~opc in
  Printf.fprintf oc
    "(%a,\n\t\t%a)"
    print_expr notif
    (List.print ~sep:";\n\t\t  "
      (fun oc ft ->
        let id = id_of_field_name ~tuple:TupleOut ft.RamenTuple.name in
        Printf.fprintf oc "%S, "
          (ft.RamenTuple.name :> string) ;
        if ft.typ.nullable then
          Printf.fprintf oc
            "(match %s with Null -> %S \
             | NotNull v_ -> %a v_)\n"
            id string_of_null
            (conv_from_to ~nullable:false) (ft.typ.structure, TString)
        else
          Printf.fprintf oc "%a %s"
            (conv_from_to ~nullable:false) (ft.typ.structure, TString)
            id)) opc.tuple_typ

(* We want a function that, when given the worker name, current time and the
 * output tuple, will return the list of RamenNotification.tuple to send: *)
(* TODO: shouldn't CodeGenLib pass this func the global and also maybe
 * the group states? *)
(* TODO: do not return this value for each notification name, as this is
 * always the same now. Instead, return the list of notification names and
 * a single string for the output value. *)
let emit_get_notifications name in_typ out_typ ~opc oc notifications =
  let env =
    add_tuple_environment TupleIn in_typ [] |>
    add_tuple_environment TupleOut out_typ in
  Printf.fprintf oc "let %s %a %a =\n\t%a\n"
    name
    (emit_tuple ~with_alias:true TupleIn) in_typ
    (emit_tuple ~with_alias:true TupleOut) out_typ
    (List.print ~sep:";\n\t\t" (emit_notification_tuple ~env ~opc))
      notifications

(* Tells whether this expression requires the out tuple (or anything else
 * from the group). *)
let expr_needs_group e =
  try
    E.iter (fun _ e ->
      if (
        match e.E.text with
        | Variable tuple
        | Binding (RecordField (tuple, _)) ->
            tuple_need_state tuple
        | Stateful (LocalState, _, _) -> true
        | Stateless (SL0 (EventStart|EventStop)) ->
            (* This depends on the definition of the event time really.
             * TODO: pass the event time down here and actually check. *)
            true
        | _ -> false
      ) then raise Exit
    ) e ;
    false
  with Exit ->
    true

let emit_aggregate opc oc global_env group_env env_env param_env
                   name in_typ =
  let out_typ = opc.tuple_typ in
  match opc.op with
  | Some O.Aggregate
      { fields ; merge ; sort ; where ; key ; commit_before ; commit_cond ;
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
            | Binding (RecordField (TupleOut, fn)) ->
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
    | Binding (RecordField (TupleOut, fn)) (* not supposed to happen *) ->
        Set.add fn s
    | Stateless (SL2 (Get, E.{ text = Const (VString fn) ; _ },
                           E.{ text = Variable TupleOut ; _ })) ->
        Set.add (RamenName.field_of_string fn) s
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
    (Set.print RamenName.field_print) minimal_fields ;
  (* Replace removed values with a dull type. Should not be accessed
   * ever. This is because we want out and minimal to have the same
   * ramen type, so that field access works on both. *)
  let minimal_typ =
    List.map (fun ft ->
      if Set.mem ft.RamenTuple.name minimal_fields then
        ft
      else (* Replace it *)
        RamenTuple.{ ft with
          name = RamenName.field_of_string ("_not_minimal_"^ (ft.name :> string)) ;
          typ = T.{ ft.typ with structure = TEmpty } }
    ) out_typ in
  (* Tells whether we need the group to check the where clause (because it
   * uses the group tuple or build a group-wise aggregation on its own,
   * despite this is forbidden in RamenOperation.check): *)
  let where_need_group = expr_needs_group where
  and when_to_check_for_commit = when_to_check_group_for_expr commit_cond
  and is_yield = from = []
  (* Every functions have at least access to env + params: *)
  and base_env = List.rev_append param_env env_env
  in
  Printf.fprintf oc
    "%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n%a\n"
    (emit_state_init "global_init_" E.GlobalState ~env:base_env ["()"] ~where ~commit_cond ~opc) fields
    (emit_state_init "group_init_" E.LocalState ~env:(global_env @ base_env) ["global_"] ~where ~commit_cond ~opc) fields
    (emit_read_tuple "read_in_tuple_" ~is_yield) in_typ
    (if where_need_group then
      emit_where ~env:(global_env @ base_env) "where_fast_" ~always_true:true in_typ ~opc
    else
      emit_where ~env:(global_env @ base_env) "where_fast_" in_typ ~opc) where
    (if not where_need_group then
      emit_where ~env:(global_env @ base_env) "where_slow_" ~with_group:true ~always_true:true in_typ ~opc
    else
      emit_where ~env:(global_env @ base_env) "where_slow_" ~with_group:true in_typ ~opc) where
    (emit_key_of_input "key_of_input_" in_typ ~opc) key
    emit_maybe_fields out_typ
    (emit_when ~env:(group_env @ global_env @ base_env) "commit_cond_" in_typ minimal_typ ~opc) commit_cond
    (emit_field_selection ~build_minimal:true ~env:(group_env @ global_env @ base_env) "minimal_tuple_of_group_" in_typ minimal_typ ~opc) fields
    (emit_field_selection ~build_minimal:false ~env:(group_env @ global_env @ base_env) "out_tuple_of_minimal_tuple_" in_typ minimal_typ ~opc) fields
    (emit_update_states ~env:(group_env @ global_env @ base_env) "update_states_" in_typ minimal_typ ~opc) fields
    (emit_sersize_of_tuple "sersize_of_tuple_") out_typ
    (emit_time_of_tuple "time_of_tuple_") opc
    (emit_factors_of_tuple "factors_of_tuple_") opc
    (emit_serialize_tuple "serialize_tuple_") out_typ
    (emit_generate_tuples "generate_tuples_" in_typ out_typ ~opc) fields
    (emit_merge_on "merge_on_" in_typ ~opc) merge.on
    (emit_sort_expr "sort_until_" in_typ ~opc) (match sort with Some (_, Some u, _) -> [u] | _ -> [])
    (emit_sort_expr "sort_by_" in_typ ~opc) (match sort with Some (_, _, b) -> b | None -> [])
    (emit_get_notifications "get_notifications_" in_typ out_typ ~opc) notifications ;
  Printf.fprintf oc "let %s () =\n\
      \tCodeGenLib_Skeletons.aggregate\n\
      \t\tread_in_tuple_ sersize_of_tuple_ time_of_tuple_\n\
      \t\tfactors_of_tuple_ serialize_tuple_\n\
      \t\tgenerate_tuples_\n\
      \t\tminimal_tuple_of_group_\n\
      \t\tupdate_states_\n\
      \t\tout_tuple_of_minimal_tuple_\n\
      \t\tmerge_on_ %d %F %d sort_until_ sort_by_\n\
      \t\twhere_fast_ where_slow_ key_of_input_ %b\n\
      \t\tcommit_cond_ %b %b %s\n\
      \t\tglobal_init_ group_init_\n\
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

let emit_parameters oc params =
  (* Emit parameters: *)
  Printf.fprintf oc "\n(* Parameters: *)\n" ;
  List.iter (fun p ->
    (* FIXME: nullable parameters *)
    Printf.fprintf oc
      "let %s_%s_ =\n\
       \tlet parser_ x_ = %s(%a) in\n\
       \tCodeGenLib.parameter_value ~def:(%s(%a)) parser_ %S\n"
      (id_of_prefix TupleParam) (p.ptyp.name :> string)
      (if p.ptyp.typ.nullable then
        "if looks_like_null x_ then Null else NotNull "
       else "") (emit_value_of_string p.ptyp.typ.structure) "x_"
      (if p.ptyp.typ.nullable && p.value <> VNull
       then "NotNull " else "")
      emit_type p.value (p.ptyp.name :> string)
  ) params ;
  (* Also a function that takes a parameter name (string) and return its
   * value (as a string) - useful for text replacements within strings *)
  Printf.fprintf oc "let field_of_params_ = function\n%a\
                     \t| _ -> raise Not_found\n\n"
    (List.print ~first:"" ~last:"" ~sep:"" (fun oc p ->
      let glob_name =
        Printf.sprintf "%s_%s_"
          (id_of_prefix TupleParam)
          (p.ptyp.name :> string) in
      Printf.fprintf oc "\t| %S -> (%a) %s%s\n"
        (p.ptyp.name :> string)
        (conv_from_to ~nullable:p.ptyp.typ.nullable) (p.ptyp.typ.structure, TString)
        glob_name
        (if p.ptyp.typ.nullable then Printf.sprintf " |! %S" string_of_null
         else ""))) params

let emit_running_condition oc params envvars cond =
  let code = IO.output_string ()
  and consts = IO.output_string () in
  let opc =
    { op = None ; event_time = None ; params ; consts ; tuple_typ = [] } in
  (match cond with
  | Some cond ->
      let env = List.rev_append (env_of_envvars envvars)
                                (env_of_params params) in
      Printf.fprintf code "let run_condition_ () =\n\t%a\n\n"
        (emit_expr ~env ~context:Finalize ~opc) cond
  | None ->
      Printf.fprintf code "let run_condition_ () = true") ;
  Printf.fprintf oc "%s\n%s\n"
    (IO.close_out consts) (IO.close_out code)

(* params and envs must be accessible as records (encoded as tuples)
 * under names "params_" and "envs_". Note that since we can refer to
 * the whole tuple "env" and "params", and that we type all functions
 * in a program together, then these records must contain all fields
 * used in the program, not only the fields used by the function being
 * compiled. Therefore we must be given params and envvars by the
 * compiler. *)
let emit_params_env params_mod params envvars oc =
  (* Collect all used envvars/params: *)
  Printf.fprintf oc
    "\n(* Parameters as a Ramen record: *)\n\
     let params_ = %a\n\n"
    (list_print_as_tuple (fun oc p ->
      (* See emit_parameters *)
      Printf.fprintf oc "%s.%s_%s_"
        params_mod
        (id_of_prefix TupleParam)
        (p.ptyp.name :> string)))
      (RamenTuple.params_sort params) ;
  Printf.fprintf oc
    "\n(* Environment variables as a Ramen record: *)\n\
     let envs_ = %a\n\n"
    (list_print_as_tuple (fun oc (n : RamenName.field) ->
      Printf.fprintf oc "Sys.getenv_opt %S |> nullable_of_option"
        (n :> string)))
      envvars

let emit_header func params_mod oc =
  Printf.fprintf oc "(* Code generated for operation %S:\n%a\n*)\n\
    open Batteries\n\
    open Stdint\n\
    open RamenHelpers\n\
    open RamenNullable\n\
    open %s\n"
    (func.F.name :> string)
    (O.print true) func.F.operation
    params_mod

let emit_operation name func params envvars oc =
  (* Now the code, which might need some global constant parameters,
   * thus the two strings that are assembled later: *)
  let code = IO.output_string ()
  and consts = IO.output_string ()
  and tuple_typ =
    O.out_type_of_operation ~with_private:true func.F.operation
  and global_env, group_env, env_env, param_env =
    initial_environments func.F.operation params envvars
  in
  !logger.debug "Global environment will be: %a" print_env global_env ;
  !logger.debug "Group environment will be: %a" print_env group_env ;
  !logger.debug "Unix-env environment will be: %a" print_env env_env ;
  !logger.debug "Parameters environment will be: %a" print_env param_env ;
  (* As all exposed IO tuples are present in the environment, any Path can
   * now be replaced with a Binding. The [subst_fields_for_binding] function
   * takes an expression and does this change for any tuple_prefix. The
   * [Path] expression is therefore not used anywhere in the code
   * generation process. We could similarly replace some Get in addition to
   * some Path.
   *)
  let op =
    List.fold_left (fun op tuple ->
      subst_fields_for_binding tuple op
    ) func.F.operation
      [ TupleEnv ; TupleParam ; TupleIn ; TupleGroup ; TupleOutPrevious ;
        TupleOut ; TupleSortFirst ; TupleSortSmallest ; TupleSortGreatest ;
        TupleMergeGreatest ; Record ]
  in
  !logger.debug "After substitutions for environment bindings: %a"
    (O.print true) op ;
  (match func.F.operation with
  | ReadCSVFile { where = { fname ; unlink } ; preprocessor ;
                  what = { separator ; null ; _ } ; _ } ->
    let opc =
      { op = Some func.F.operation ;
        event_time = O.event_time_of_operation func.F.operation ;
        params ; consts ; tuple_typ } in
    emit_read_csv_file opc code name fname unlink separator null
                       preprocessor
  | ListenFor { net_addr ; port ; proto } ->
    let opc =
      { op = Some func.F.operation ;
        event_time = O.event_time_of_operation func.F.operation ;
        params ; consts ; tuple_typ } in
    emit_listen_on opc code name net_addr port proto
  | Instrumentation { from } ->
    let opc =
      { op = Some func.F.operation ;
        event_time = O.event_time_of_operation func.F.operation ;
        params ; consts ; tuple_typ } in
    emit_well_known opc code name from
      "RamenBinocle.unserialize" "report_ringbuf"
      "(fun (w, t, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) -> w, t)"
  | Notifications { from } ->
    let opc =
      { op = Some func.F.operation ;
        event_time = O.event_time_of_operation func.F.operation ;
        params ; consts ; tuple_typ } in
    emit_well_known opc code name from
      "RamenNotification.unserialize" "notifs_ringbuf"
      "(fun (w, t, _, _, _, _, _, _) -> w, t)"
  | Aggregate _ ->
    (* Temporary hack: build a RamenTuple out of this in_type (at this point
     * we do not need the access paths anyway): *)
    let in_type =
      List.map (fun f ->
        RamenTuple.{
          name = E.id_of_path f.RamenFieldMaskLib.path ;
          typ = f.typ ; units = f.units ;
          doc = "" ; aggr = None }
      ) func.F.in_type in
    let opc =
      { op = Some op ;
        event_time = O.event_time_of_operation func.F.operation ;
        params ; consts ; tuple_typ } in
    emit_aggregate opc code global_env group_env env_env param_env
                   name in_type) ;
  Printf.fprintf oc "\n(* Global constants: *)\n\n%s\n\
                     \n(* Operation Implementation: *)\n\n%s\n"
    (IO.close_out consts) (IO.close_out code)

(* A function that reads the history and write it according to some out_ref
 * under a given chanel: *)
let emit_replay name func params oc =
  (* We cannot reuse the sersize_of_tuple_, time_of_tuple_ or
   * serialize_tuple_ that has been emitted for aggregate as emit_read_tuple
   * returns a version of out_typ with no private fields, and that's what
   * we need to pass to those functions as well. So here we merely pretend
   * the out_typ is the serialized version of out_type (no private fields
   * _and_ serialization order) *)
  let ser_out_typ =
    O.out_type_of_operation func.F.operation |>
    RingBufLib.ser_tuple_typ_of_tuple_typ in
  let consts = IO.output_string () in
  let opc =
    { op = Some func.F.operation ;
      event_time = O.event_time_of_operation func.F.operation ;
      params ; consts ; tuple_typ = ser_out_typ } in
  emit_read_tuple "read_out_tuple_" oc ser_out_typ ;
  emit_sersize_of_tuple "sersize_of_ser_tuple_" oc ser_out_typ ;
  emit_time_of_tuple "time_of_ser_tuple_" oc opc ;
  emit_factors_of_tuple "factors_of_tuple_" oc opc ;
  emit_serialize_tuple "serialize_ser_tuple_" oc ser_out_typ ;
  Printf.fprintf oc
    "let %s () =\n\
       \tCodeGenLib_Skeletons.replay read_out_tuple_\n\
       \t\tsersize_of_ser_tuple_ time_of_ser_tuple_ factors_of_tuple_\n\
       \t\tserialize_ser_tuple_\n"
    name


let compile conf worker_entry_point replay_entry_point func
            obj_name params_mod params envvars =
  !logger.debug "Going to compile function %a: %a"
    RamenName.func_print func.F.name
    (O.print true) func.F.operation ;
  let src_file =
    RamenOCamlCompiler.with_code_file_for obj_name conf (fun oc ->
      emit_header func params_mod oc ;
      emit_params_env params_mod params envvars oc ;
      emit_operation worker_entry_point func params envvars oc ;
      emit_replay replay_entry_point func params oc) in
  let what = "function "^ RamenName.func_color func.F.name in
  RamenOCamlCompiler.compile conf what src_file obj_name
