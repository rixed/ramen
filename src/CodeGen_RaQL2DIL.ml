(* Compile (typed!) RaQL expressions into DIL expressions *)
open Batteries
open RamenHelpersNoLog
open RamenHelpers
open RamenLog
open Dessser
module DE = DessserExpressions
module DT = DessserTypes
module DS = DessserStdLib
module E = RamenExpr
module Lang = RamenLang
module N = RamenName
module T = RamenTypes
open DE.Ops

(*$inject
  open Batteries *)

(*
 * Helpers
 *)

let mn_of_t = function
  | DT.Value mn -> mn
  | t -> invalid_arg ("mn_of_t for type "^ DT.to_string t)

let print_r_env oc =
  pretty_list_print (fun oc (k, v) ->
    Printf.fprintf oc "%a=>%a"
      E.print_binding_key k
      (DE.print ~max_depth:2) v
  ) oc

(*
 * Helpers regarding state.
 * (See below for some explanations about states and how they are implemented)
 *)

let pick_state r_env e state_lifespan =
  let state_var =
    match state_lifespan with
    | E.LocalState -> Lang.Group
    | E.GlobalState -> Lang.Global in
  try List.assoc (E.RecordValue state_var) r_env
  with Not_found ->
    Printf.sprintf2
      "Expression %a uses variable %s that is not available in the environment \
       (only %a)"
      (E.print false) e
      (Lang.string_of_variable state_var)
      print_r_env r_env |>
    failwith

(* Returns the field name in the state record for that expression: *)
let field_name_of_state e =
  "state_"^ string_of_int e.E.uniq_num

(* Returns the state of the expression: *)
let get_state state_rec e =
  let fname = field_name_of_state e in
  let open DE.Ops in
  get_vec (u8_of_int 0) (get_field fname state_rec)

let set_state state_rec e d =
  let fname = field_name_of_state e in
  let open DE.Ops in
  set_vec (u8_of_int 0) (get_field fname state_rec) d

let with_state ~d_env state_rec e f =
  let open DE.Ops in
  let_ ~name:"state" ~l:d_env (get_state state_rec e) f

(* Convert a non-nullable value to the given value-type.
 * Beware that the returned expression might be nullable (for instance when
 * converting a string to a number). *)
(* TODO: move in dessser.StdLib as a "cast" function *)
let rec conv ?(depth=0) ~to_ l d =
  let from = (mn_of_t (DE.type_of l d)).DT.vtyp in
  if DT.value_type_eq from to_ then d else
  (* A null can be cast to whatever. Actually, type-checking will type nulls
   * arbitrarily. *)
  if match d with DE.E0 (Null _) -> true | _ -> false then null to_ else
  match from, to_ with
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac String -> string_of_int_ d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac Float -> to_float d
  | Mac String, Mac Float -> float_of_string_ d
  | Mac String, Mac Char -> char_of_string d
  | Mac String, Mac I8 -> i8_of_string d
  | Mac String, Mac I16 -> i16_of_string d
  | Mac String, Mac I24 -> i24_of_string d
  | Mac String, Mac I32 -> i32_of_string d
  | Mac String, Mac I40 -> i40_of_string d
  | Mac String, Mac I48 -> i48_of_string d
  | Mac String, Mac I56 -> i56_of_string d
  | Mac String, Mac I64 -> i64_of_string d
  | Mac String, Mac I128 -> i128_of_string d
  | Mac String, Mac U8 -> u8_of_string d
  | Mac String, Mac U16 -> u16_of_string d
  | Mac String, Mac U24 -> u24_of_string d
  | Mac String, Mac U32 -> u32_of_string d
  | Mac String, Mac U40 -> u40_of_string d
  | Mac String, Mac U48 -> u48_of_string d
  | Mac String, Mac U56 -> u56_of_string d
  | Mac String, Mac U64 -> u64_of_string d
  | Mac String, Mac U128 -> u128_of_string d
  | Mac Float, Mac String -> string_of_float_ d
  | Mac Char, Mac U8 -> u8_of_char d
  | Mac U8, Mac Char -> char_of_u8 d
  | Mac Bool, Mac U8 -> u8_of_bool d
  | Mac Char, Mac String -> string_of_char d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I8 -> to_i8 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I16 -> to_i16 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I24 -> to_i24 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I32 -> to_i32 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I40 -> to_i40 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I48 -> to_i48 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I56 -> to_i56 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I64 -> to_i64 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac I128 -> to_i128 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U8 -> to_u8 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U16 -> to_u16 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U24 -> to_u24 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U32 -> to_u32 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U40 -> to_u40 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U48 -> to_u48 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U56 -> to_u56 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U64 -> to_u64 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128 | Float),
    Mac U128 -> to_u128 d
  (* Specialized version for lst/vec of chars that return the
   * string composed of those chars rather than an enumeration: *)
  | Vec (_, { vtyp = Mac Char ; _ }), Mac String
  | Lst { vtyp = Mac Char ; _ }, Mac String ->
      conv_charseq ~depth:(depth+1) (cardinality d) l d
  | Vec _, Mac String
  | Lst _, Mac String ->
      conv_list ~depth:(depth+1) (cardinality d) l d
  | Mac Bool, Mac String ->
      if_ ~cond:d ~then_:(string "true") ~else_:(string "false")
  | Usr { name = ("Ip4" | "Ip6" | "Ip") ; _ }, Mac String ->
      string_of_ip d
  | Vec (d1, mn1), Vec (d2, mn2) when d1 = d2 ->
      map d (DE.func1 ~l (DT.Value mn1) (conv_maybe_nullable ~depth:(depth+1) ~to_:mn2))
  | Lst mn1, Lst mn2 ->
      map d (DE.func1 ~l (DT.Value mn1) (conv_maybe_nullable ~depth:(depth+1) ~to_:mn2))
  (* TODO: Also when d2 < d1, and d2 > d1 extending with null as long as mn2 is
   * nullable *)
  | Vec (_, mn1), Lst mn2 ->
      let d = list_of_vec d in
      map d (DE.func1 ~l (DT.Value mn1) (conv_maybe_nullable ~depth:(depth+1) ~to_:mn2))
  (* TODO: other types to string *)
  | _ ->
      Printf.sprintf2 "Not implemented: Cast from %a to %a of expression %a"
        DT.print_value_type from
        DT.print_value_type to_
        (DE.print ~max_depth:3) d |>
      failwith

and conv_list ?(depth=0) length_e l src =
  (* We use a one entry vector as a ref cell: *)
  let_ ~name:"dst_" ~l (make_vec [ string "[" ]) (fun _l dst ->
    let set v = set_vec (u32_of_int 0) dst v
    and get () = get_vec (u32_of_int 0) dst in
    let idx_t = DT.(Value (required (Mac U32))) in
    let cond =
      DE.func1 ~l idx_t (fun _l i -> lt i length_e)
    and body =
      DE.func1 ~l idx_t (fun _l i ->
        let s =
          conv_maybe_nullable ~depth:(depth+1) ~to_:DT.(required (Mac String))
                              l (get_vec i src) in
        seq [ set (append_string (get ()) s) ;
              add i (u32_of_int 1) ]) in
    seq [ ignore_ (loop_while ~init:(u32_of_int 0) ~cond ~body) ;
          set (append_string (get ()) (string "]")) ;
          get () ])

and conv_charseq ?(depth=0) length_e l src =
  (* We use a one entry vector as a ref cell: *)
  let_ ~name:"dst_" ~l (make_vec [ string "" ]) (fun _l dst ->
    let set v = set_vec (u32_of_int 0) dst v
    and get () = get_vec (u32_of_int 0) dst in
    let idx_t = DT.(Value (required (Mac U32))) in
    let cond =
      DE.func1 ~l idx_t (fun _l i -> lt i length_e)
    and body =
      DE.func1 ~l idx_t (fun _l i ->
        let s =
          conv_maybe_nullable ~depth:(depth+1) ~to_:DT.(required (Mac Char))
                              l (get_vec i src) in
        seq [ set (append_string (get ()) (string_of_char s)) ;
              add i (u32_of_int 1) ]) in
    seq [ ignore_ (loop_while ~init:(u32_of_int 0) ~cond ~body) ;
          get () ])

and conv_maybe_nullable ?(depth=0) ~to_ l d =
  !logger.debug "%sConverting into %a: %a"
    (indent_of depth)
    DT.print_maybe_nullable to_
    (DE.print ?max_depth:None) d ;
  let conv = conv ~depth:(depth+1) ~to_:to_.DT.vtyp in
  let from = mn_of_t (DE.type_of l d) in
  let is_const_null =
    match d with DE.E0 (Null _) -> true | _ -> false in
  let if_null def =
    if is_const_null then def else
    let_ ~name:"nullable_to_not_nullable_" ~l d (fun l d ->
      if_
        ~cond:(is_null d)
        ~then_:def
        ~else_:(conv l (force d))) in
  (* Beware that [conv] can return a nullable expression: *)
  match from.DT.nullable, to_.DT.nullable with
  | false, false ->
      !logger.debug "%s...from not nullable to not nullable" (indent_of depth) ;
      let d' = conv l d in
      if (mn_of_t (DE.type_of l d')).DT.nullable then force d'
                                                 else d'
  | true, false ->
      !logger.debug "%s...from nullable to not nullable" (indent_of depth) ;
      (match to_.DT.vtyp with
      | DT.(Mac String) ->
          if_null (string "NULL")
      | DT.(Mac Char) ->
          if_null (char '?')
      | _ ->
          let d' = conv l (force d) in
          if (mn_of_t (DE.type_of l d')).DT.nullable then force d'
                                                     else d')
  | false, true ->
      !logger.debug "%s...from not nullable to nullable" (indent_of depth) ;
      let d' = conv l d in
      if (mn_of_t (DE.type_of l d')).DT.nullable then d'
                                                 else not_null d'
  | true, true ->
      !logger.debug "%s...from nullable to nullable" (indent_of depth) ;
      if is_const_null then null to_.DT.vtyp else
      let_ ~name:"conv_mn_x_" ~l d (fun l x ->
        if_ ~cond:(is_null x)
            ~then_:(null to_.DT.vtyp)
            ~else_:(conv_maybe_nullable ~depth:(depth+1) ~to_ l (force x)))

let rec constant mn v =
  let bad_type () =
    Printf.sprintf2 "Invalid type %a for literal %a"
      DT.print_maybe_nullable mn
      T.print v |>
    failwith
  in
  match v with
  | T.VNull -> null mn.DT.vtyp
  | VUnit -> unit
  | VFloat f -> float f
  | VString s -> string s
  | VBool b -> bool b
  | VChar c -> char c
  | VU8 n -> u8 n
  | VU16 n -> u16 n
  | VU24 n -> u24 n
  | VU32 n -> u32 n
  | VU40 n -> u40 n
  | VU48 n -> u48 n
  | VU56 n -> u56 n
  | VU64 n -> u64 n
  | VU128 n -> u128 n
  | VI8 n -> i8 n
  | VI16 n -> i16 n
  | VI24 n -> i24 n
  | VI32 n -> i32 n
  | VI40 n -> i40 n
  | VI48 n -> i48 n
  | VI56 n -> i56 n
  | VI64 n -> i64 n
  | VI128 n -> i128 n
  | VEth n -> u48 n
  | VIpv4 i -> u32 i
  | VIpv6 i -> u128 i
  | VIp (RamenIp.V4 i) ->
      (match T.ip with
      | DT.Usr { def = Sum alts ; _ } ->
          construct alts 0 (constant (snd alts.(0)) (VIpv4 i))
      | _ -> assert false)
  | VIp (RamenIp.V6 i) ->
      (match T.ip with
      | DT.Usr { def = Sum alts ; _ } ->
          construct alts 1 (constant (snd alts.(1)) (VIpv6 i))
      | _ -> assert false)
  | VCidrv4 (i, m) ->
      make_rec [ "ip", u32 i ; "mask", u8 m ]
  | VCidrv6 (i, m) ->
      make_rec [ "ip", u128 i ; "mask", u8 m ]
  | VCidr (RamenIp.Cidr.V4 i_m) ->
      (match T.cidr with
      | DT.Usr { def = Sum alts ; _ } ->
          construct alts 0 (constant (snd alts.(0)) (VCidrv4 i_m))
      | _ -> assert false)
  | VCidr (RamenIp.Cidr.V6 i_m) ->
      (match T.cidr with
      | DT.Usr { def = Sum alts ; _ } ->
          construct alts 1 (constant (snd alts.(1)) (VCidrv6 i_m))
      | _ -> assert false)
  (* Although there are not much constant literal compound values in RaQL
   * (instead there are literal *expressions*, which individual items are
   * typed), it's actually possible to translate them thanks to the passed
   * type [mn]: *)
  | VTup vs ->
      (match mn.vtyp with
      | DT.Tup mns ->
          if Array.length mns <> Array.length vs then bad_type () ;
          make_tup (List.init (Array.length mns) (fun i ->
            constant mns.(i) vs.(i)))
      | _ ->
          bad_type ())
  | VVec vs ->
      (match mn.vtyp with
      | DT.Vec (d, mn) ->
          if d <> Array.length vs then bad_type () ;
          make_vec (List.init d (fun i -> constant mn vs.(i)))
      | _ ->
          bad_type ())
  | VLst vs ->
      (match mn.vtyp with
      | DT.Lst mn ->
          make_lst mn (List.init (Array.length vs) (fun i ->
            constant mn vs.(i)))
      | _ ->
          bad_type ())
  | VRec vs ->
      (match mn.vtyp with
      | DT.Rec mns ->
          if Array.length mns <> Array.length vs then bad_type () ;
          make_rec (Array.map (fun (n, mn) ->
            match Array.find (fun (n', _) -> n = n') vs with
            | exception Not_found ->
                bad_type ()
            | _, v ->
                n, constant mn v
          ) mns |> Array.to_list)
      | _ ->
          bad_type ())
  | VMap _ ->
      invalid_arg "constant: not for VMaps"

(* If [d] is nullable, then return it. If it's a not nullable value type,
 * then make it nullable: *)
let ensure_nullable ~d_env d =
  match DE.type_of d_env d with
  | DT.Value { nullable = false ; _ } -> not_null d
  | DT.Value { nullable = true ; _ } -> d
  | t -> invalid_arg ("ensure_nullable on "^ DT.to_string t)

(* Environments:
 * - [d_env] is the environment used by dessser, ie. a stack of
 *   expression x type (expression being for instance [(identifier n)] or
 *   [(param n m)]. This is used by dessser to do type-checking.
 * - [r_env] is the stack of currently reachable "raql thing", such
 *   as expression state, a record (in other words an E.binding_key), bound
 *   to a dessser expression (typically an identifier or a param). *)
let rec expression ?(depth=0) ~r_env ~d_env e =
  !logger.debug "%sCompiling into DIL: %a"
    (indent_of depth)
    (E.print true) e ;
  assert (E.is_typed e) ;
  let expr d_env =
    expression ~depth:(depth+1) ~r_env ~d_env in
  let bad_type () =
    Printf.sprintf2 "Invalid type %a for expression %a"
      DT.print_maybe_nullable e.E.typ
      (E.print false) e |>
    failwith in
  let convert_in = e.E.typ.DT.vtyp in
  let conv = conv ~depth:(depth+1)
  and conv_maybe_nullable = conv_maybe_nullable ~depth:(depth+1) in
  let conv_from d_env d =
    conv ~to_:e.E.typ.DT.vtyp d_env d in
  let conv_maybe_nullable_from d_env d =
    conv_maybe_nullable ~to_:e.E.typ d_env d in
  let apply_1 = apply_1 ~depth
  and apply_2 = apply_2 ~depth in
  (* In any case we want the output to be converted to the expected type: *)
  conv_maybe_nullable_from d_env (
    match e.E.text with
    | Const v ->
        constant e.E.typ v
    | Tuple es ->
        (match e.E.typ.DT.vtyp with
        | DT.Tup mns ->
            if Array.length mns <> List.length es then bad_type () ;
            (* Better convert items before constructing the tuple: *)
            List.mapi (fun i e ->
              conv_maybe_nullable ~to_:mns.(i) d_env (expr d_env e)
            ) es |>
            make_tup
        | _ ->
            bad_type ())
    | Record nes ->
        (match e.E.typ.DT.vtyp with
        | DT.Rec mns ->
            if Array.length mns <> List.length nes then bad_type () ;
            List.mapi (fun i (n, e) ->
              (n : N.field :> string),
              conv_maybe_nullable ~to_:(snd mns.(i)) d_env (expr d_env e)
            ) nes |>
            make_rec
        | _ ->
            bad_type ())
    | Vector es ->
        (match e.E.typ.DT.vtyp with
        | DT.Vec (dim, mn) ->
            if dim <> List.length es then bad_type () ;
            List.map (fun e ->
              conv_maybe_nullable ~to_:mn d_env (expr d_env e)
            ) es |>
            make_vec
        | _ ->
            bad_type ())
    | Variable v ->
        (* We probably want to replace this with a DIL identifier with a
         * well known name: *)
        identifier (Lang.string_of_variable v)
    | Binding (E.RecordField (var, field) as k) ->
        (* Try first to see if there is this specific binding in the
         * environment. *)
        (try List.assoc k r_env with
        | Not_found ->
            (* If not, that mean this field have not been overridden but we may
             * still find the record it's from and pretend we have a Get from
             * the Variable instead: *)
            (match List.assoc (E.RecordValue var) r_env with
            | exception Not_found ->
                Printf.sprintf2
                  "Cannot find a binding for %a in the environment (%a)"
                  E.print_binding_key k
                  print_r_env r_env |>
                failwith
            | binding ->
                apply_1 d_env binding (fun _d_env binding ->
                  get_field (field :> string) binding)))
    | Binding k ->
        (* A reference to the raql environment. Look for the dessser expression it
         * translates to. *)
        (try List.assoc k r_env with
        | Not_found ->
            Printf.sprintf2
              "Cannot find a binding for %a in the environment (%a)"
              E.print_binding_key k
              print_r_env r_env |>
            failwith)
    | Case (alts, else_) ->
        let rec alt_loop = function
          | [] ->
              (match else_ with
              | Some e -> conv_maybe_nullable_from d_env (expr d_env e)
              | None -> null e.E.typ.DT.vtyp)
          | E.{ case_cond = cond ; case_cons = cons } :: alts' ->
              let do_cond d_env cond =
                if_ ~cond
                    ~then_:(conv_maybe_nullable ~to_:e.E.typ d_env (expr d_env cons))
                    ~else_:(alt_loop alts') in
              if cond.E.typ.DT.nullable then
                let_ ~name:"nullable_cond_" ~l:d_env (expr d_env cond)
                  (fun d_env cond ->
                    if_ ~cond:(is_null cond)
                        ~then_:(null e.E.typ.DT.vtyp)
                        ~else_:(do_cond d_env (force cond)))
              else
                do_cond d_env (expr d_env cond) in
        alt_loop alts
    | Stateless (SL0 Now) ->
        conv_from d_env now
    | Stateless (SL0 Random) ->
        random_float
    | Stateless (SL0 Pi) ->
        float Float.pi
    | Stateless (SL1 (Age, e1)) ->
        apply_1 ~convert_in d_env (expr d_env e1) (fun _l d -> sub now d)
    | Stateless (SL1 (Cast _, e1)) ->
        (* Type checking already set the output type of that Raql expression to the
         * target type, and the result will be converted into this type in any
         * case. *)
        expr d_env e1
    | Stateless (SL1 (Force, e1)) ->
        force (expr d_env e1)
    | Stateless (SL1 (Length, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _d_env d1 ->
          match e1.E.typ.DT.vtyp with
          | DT.Mac String -> string_length d1
          | DT.Lst _ -> cardinality d1
          | _ -> bad_type ()
        )
    | Stateless (SL1 (Lower, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> lower d)
    | Stateless (SL1 (Upper, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> upper d)
    | Stateless (SL1 (Not, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> not_ d)
    | Stateless (SL1 (Abs, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> abs d)
    | Stateless (SL1 (Minus, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> neg d)
    | Stateless (SL1 (Defined, e1)) ->
        not_ (is_null (expr d_env e1))
    | Stateless (SL1 (Exp, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> exp d)
    | Stateless (SL1 (Log, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> log d)
    | Stateless (SL1 (Log10, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> log10 d)
    | Stateless (SL1 (Sqrt, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> sqrt d)
    | Stateless (SL1 (Ceil, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> ceil d)
    | Stateless (SL1 (Floor, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> floor d)
    | Stateless (SL1 (Round, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> round d)
    | Stateless (SL1 (Cos, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> cos d)
    | Stateless (SL1 (Sin, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> sin d)
    | Stateless (SL1 (Tan, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> tan d)
    | Stateless (SL1 (ACos, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> acos d)
    | Stateless (SL1 (ASin, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> asin d)
    | Stateless (SL1 (ATan, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> atan d)
    | Stateless (SL1 (CosH, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> cosh d)
    | Stateless (SL1 (SinH, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> sinh d)
    | Stateless (SL1 (TanH, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> tanh d)
    | Stateless (SL1 (Hash, e1)) ->
        apply_1 d_env (expr d_env e1) (fun _l d -> hash d)
    | Stateless (SL1 (Chr, e1)) ->
        apply_1 d_env (expr d_env e1) (fun d_env d1 ->
          char_of_u8 (conv ~to_:DT.(Mac U8) d_env d1)
        )
    | Stateless (SL1 (Basename, e1)) ->
        apply_1 d_env (expr d_env e1) (fun d_env d1 ->
          let_ ~name:"str_" ~l:d_env d1 (fun d_env str ->
            let pos = find_substring (bool false) (string "/") str in
            let_ ~name:"pos_" ~l:d_env pos (fun _d_env pos ->
              if_
                ~cond:(is_null pos)
                ~then_:str
                ~else_:(split_at (add (u24_of_int 1) (force pos)) str |>
                        get_item 1)))
        )
    | Stateless (SL1s ((Max | Min as op), es)) ->
        let d_op = match op with Max -> max | _ -> min in
        (match es with
        | [] ->
            assert false
        | [ e1 ] ->
            apply_1 d_env (expr d_env e1) (fun d_env d -> conv_from d_env d)
        | e1 :: es' ->
            apply_1 d_env (expr d_env e1) (fun d_env d1 ->
              let rest = { e with text = Stateless (SL1s (op, es')) } in
              apply_1 d_env (expr d_env rest) (fun d_env d2 ->
                d_op (conv_from d_env d1) d2)))
    | Stateless (SL1s (Print, es)) ->
        (match List.rev es with
        | last :: _ as es ->
            seq (
              List.fold_left (fun lst e -> dump (expr d_env e) :: lst
              ) [ expr d_env last ] es)
        | [] ->
            invalid_arg "RaQL2DIL.expression: empty PRINT")
    | Stateless (SL1s (Coalesce, es)) ->
        let es =
          List.map (fun e ->
            let to_ = { e.E.typ with nullable = e.E.typ.nullable } in
            conv_maybe_nullable ~to_ d_env (expr d_env e)
          ) es in
        DessserStdLib.coalesce d_env es
    | Stateless (SL2 (Add, e1, e2)) ->
        apply_2 ~convert_in d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> add)
    | Stateless (SL2 (Sub, e1, e2)) ->
        apply_2 ~convert_in d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> sub)
    | Stateless (SL2 (Mul, e1, e2)) ->
        apply_2 ~convert_in d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> mul)
    | Stateless (SL2 (Div, e1, e2)) ->
        apply_2 ~convert_in d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> div)
    | Stateless (SL2 (IDiv, e1, e2)) ->
        (* When the result is a float we need to floor it *)
        (match e.E.typ with
        | DT.{ vtyp = Mac Float ; _ } ->
            apply_2 ~convert_in d_env (expr d_env e1) (expr d_env e2) (fun _d_env d1 d2 ->
              floor (div d1 d2))
        | _ ->
            apply_2 ~convert_in d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> div))
    | Stateless (SL2 (Mod, e1, e2)) ->
        apply_2 ~convert_in d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> rem)
    | Stateless (SL2 (Pow, e1, e2)) ->
        apply_2 ~convert_in d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> pow)
    | Stateless (SL2 (And, e1, e2)) ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> and_)
    | Stateless (SL2 (Or, e1, e2)) ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> or_)
    | Stateless (SL2 (Ge, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> ge)
    | Stateless (SL2 (Gt, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> gt)
    | Stateless (SL2 (Eq, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> eq)
    | Stateless (SL2 (StartsWith, e1, e2)) ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> starts_with)
    | Stateless (SL2 (EndsWith, e1, e2)) ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> ends_with)
    | Stateless (SL2 (BitAnd, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> log_and)
    | Stateless (SL2 (BitOr, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> log_or)
    | Stateless (SL2 (BitXor, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr d_env e1) (expr d_env e2) (fun _d_env -> log_xor)
    | Stateless (SL2 (BitShift, e1, { text = Stateless (SL1 (Minus, e2)) ; _ })) ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun _d_env d1 d2 -> right_shift d1 (to_u8 d2))
    | Stateless (SL2 (BitShift, e1, e2)) ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun _d_env d1 d2 -> left_shift d1 (to_u8 d2))
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ }, e2)) ->
        apply_1 d_env (expr d_env e2) (fun _l d -> get_field n d)
    (* Constant get from a vector: the nullability merely propagates, and the
     * program will crash if the constant index is outside the constant vector
     * counds: *)
    | Stateless (SL2 (Get, ({ text = Const n ; _ } as e1),
                           ({ typ = DT.{ vtyp = Vec _ ; _ } ; _ } as e2)))
      when E.is_integer n ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun _l -> get_vec)
    (* In all other cases the result is always nullable, in case the index goes
     * beyond the bounds: *)
    | Stateless (SL2 (Get, e1, e2)) ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun d_env d1 d2 ->
          let_ ~name:"getted" ~l:d_env d2 (fun d_env d2 ->
            let zero = conv ~to_:e1.E.typ.DT.vtyp d_env (i8_of_int 0) in
            if_
              ~cond:(and_ (ge d1 zero) (lt d1 (cardinality d2)))
              ~then_:(conv_maybe_nullable_from d_env (get_vec d1 d2))
              ~else_:(null e.E.typ.DT.vtyp)))
    | Stateless (SL2 (Index, e1, e2)) ->
        apply_2 d_env (expr d_env e1) (expr d_env e2) (fun _d_env d1 (* string *) d2 (* char *) ->
          match find_substring true_ (string_of_char d2) d1 with
          | E0 (Null _) ->
              i32_of_int ~-1
          | res ->
              let_ ~name:"index_" ~l:d_env res (fun d_env res ->
                if_
                  ~cond:(is_null res)
                  ~then_:(i32_of_int ~-1)
                  ~else_:(conv ~to_:DT.(Mac I32) d_env (force res))))
    | Stateful (state_lifespan, _, SF1 (
        (AggrMax | AggrMin | AggrFirst | AggrLast), _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        get_item 1 (get_state state_rec e)
    | Stateful (state_lifespan, _, SF1 (AggrSum, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        get_state state_rec e
        (* TODO: finalization for floats with Kahan sum *)
    | Stateful (state_lifespan, _, SF1 (AggrAvg, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        let count = get_item 0 state
        and ksum = get_item 1 state in
        div (DS.Kahan.finalize ~l:d_env ksum)
            (conv ~to_:(Mac Float) d_env count)
    | Stateful (state_lifespan, _, SF1 (
        (AggrAnd | AggrOr | AggrBitAnd | AggrBitOr | AggrBitXor), _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        get_state state_rec e
    | _ ->
        Printf.sprintf2 "RaQL2DIL.expression for %a"
          (E.print false) e |>
        todo
  )

(*$= expression & ~printer:identity
  "(u8 1)" (expression ~r_env:[] ~d_env:[] (E.of_u8 1) |> IO.to_string DE.print)
*)

(* [d] must be nullable.  Returns either [f (force d)] if d is not null,
 * or NULL (of the same type than that returned by [f]). *)
(* TODO: move all these functions into stdLib: *)
and propagate_null ?(depth=0) d_env d f =
  !logger.debug "%s...propagating null from %a"
    (indent_of depth)
    (DE.print ?max_depth:None) d ;
  let_ ~name:"nullable_" ~l:d_env d (fun d_env d ->
    let res = ensure_nullable ~d_env (f d_env (force d)) in
    let mn = mn_of_t (DE.type_of d_env res) in
    if_
      ~cond:(is_null d)
      ~then_:(null mn.DT.vtyp)
      (* Since [f] can return a nullable value already, rely on
       * [conv_maybe_nullable_from] to do the right thing instead of
       * [not_null]: *)
      ~else_:res)

(* [apply_1] takes a DIL expression and propagate null or apply [f] on it.
 * Unlike [propagate_null], also works on non-nullable values.
 * Also optionally convert the input before passing it to [f] *)
and apply_1 ?depth ?convert_in d_env d1 f =
  let no_prop d_env d1 =
    let d1 =
      match convert_in with
      | None -> d1
      | Some to_ -> conv ~to_ d_env d1 in
    f d_env d1 in
  let t1 = mn_of_t (DE.type_of d_env d1) in
  if t1.DT.nullable then
    propagate_null ?depth d_env d1 no_prop
  else
    no_prop d_env d1

(* Same as [apply_1] for two arguments: *)
and apply_2 ?(depth=0) ?convert_in ?(enlarge_in=false)
            d_env d1 d2 f =
  assert (convert_in = None || not enlarge_in) ;
  (* When neither d1 nor d2 are nullable: *)
  let conv d_env d =
    match convert_in with
    | None -> d
    | Some to_ -> conv ~to_ d_env d in
  (* neither d1 nor d2 are nullable at that point: *)
  let no_prop d_env d1 d2 =
    let d1, d2 =
      if convert_in <> None then
        conv d_env d1, conv d_env d2
      else if enlarge_in then
        let t1 = mn_of_t (DE.type_of d_env d1)
        and t2 = mn_of_t (DE.type_of d_env d2) in
        let vtyp = T.largest_type [ t1.vtyp ; t2.vtyp ] in
        conv_maybe_nullable ~to_:DT.{ t1 with vtyp } d_env d1,
        conv_maybe_nullable ~to_:DT.{ t2 with vtyp } d_env d2
      else d1, d2 in
    f d_env d1 d2 in
  (* d1 is not nullable at this stage: *)
  let no_prop_d1 d_env d1 =
    let t2 = mn_of_t (DE.type_of d_env d2) in
    if t2.DT.nullable then
      propagate_null ~depth d_env d2 (fun d_env d2 -> no_prop d_env d1 d2)
    else
      (* neither d1 nor d2 is nullable so no need to propagate nulls: *)
      no_prop d_env d1 d2 in
  let t1 = mn_of_t (DE.type_of d_env d1) in
  if t1.DT.nullable then (
    propagate_null ~depth d_env d1 no_prop_d1
  ) else (
    no_prop_d1 d_env d1
  )

(*
 * States
 *
 * Stateful operators (aka aggregation functions aka unpure functions) need a
 * state that, although it is not materialized in RaQL, is remembered from one
 * input to the next and passed along to the operator so it can either update
 * it, or compute the final value when an output is due (finalize).
 * Technically, there is also a third required function: the init function that
 * returns the initial value of the state.
 *
 * The only way state appear in RaQL is via the "locally" vs "globally"
 * keywords, which actually specify whether the state of a stateful function is
 * stored with the group or globally.
 *
 * Dessser has no stateful operators, so this mechanism has to be implemented
 * from scratch. To help with that, dessser has:
 *
 * - mutable values (thanks to set-vec), that can be used to update a state;
 *
 * - various flavors of sets, with an API that let users (ie. ramen) knows the
 * last removed values (which will come handy to optimise some stateful
 * operator over sliding windows);
 *
 * So, like with the legacy code generator, states are kept in a record (field
 * names given by [field_name_of_state]);
 * actually, one record for the global states and one for each local (aka
 * group-wide) states. The exact type of this record is given by the actual
 * stateful functions used.
 * Each field is actually composed of a one dimensional vector so that values
 * can be changed with set-vec.
 *
 * All the functions below deal with states for stateful RaQL functions.
 *)

(* This function returns the initial value of the state required to implement
 * the passed RaQL operator (which also provides its type): *)
let init_state ~r_env ~d_env e =
  ignore r_env ;
  match e.E.text with
  | Stateful (_, _, SF1 ((AggrMin | AggrMax | AggrFirst | AggrLast), _)) ->
      (* A bool to tell if there ever was a value, and the selected value *)
      make_tup [ bool false ; null e.typ.DT.vtyp ]
  | Stateful (_, _, SF1 (AggrSum, _)) ->
      u8_of_int 0 |>
      conv_maybe_nullable ~to_:e.E.typ d_env
      (* TODO: nitialization for floats with Kahan sum *)
  | Stateful (_, _, SF1 (AggrAvg, _)) ->
      (* The state of the avg is composed of the count and the (Kahan) sum: *)
      make_tup [ u32_of_int 0 ; DS.Kahan.init ]
  | Stateful (_, _, SF1 (AggrAnd, _)) ->
      bool false
  | Stateful (_, _, SF1 (AggrOr, _)) ->
      bool true
  | Stateful (_, _, SF1 ((AggrBitAnd | AggrBitOr | AggrBitXor), _)) ->
      u8_of_int 0 |>
      conv_maybe_nullable ~to_:e.E.typ d_env
  | _ ->
      (* TODO *)
      todo ("init_state of "^ E.to_string ~max_depth:1 e)

(* Returns the type of the state record needed to store the states of all the
 * given stateful expressions: *)
let state_rec_type_of_expressions ~r_env ~d_env es =
  let mns =
    List.map (fun e ->
      let d = init_state ~r_env ~d_env e in
      !logger.debug "init state of %a: %a"
        (E.print false) e
        (DE.print ?max_depth:None) d ;
      let mn = mn_of_t (DE.type_of d_env d) in
      field_name_of_state e,
      (* The value is a 1 dimensional (mutable) vector *)
      DT.(required (Vec (1, mn)))
    ) es |>
    Array.of_list in
  if mns = [||] then DT.(required Unit)
                else DT.(required (Rec mns))

(* Update the state(s) used by the [e] expression, given [state_rec] is
 * the variable holding all the states. *)
let state_update_for_expr ~r_env ~d_env ~what e =
  ignore r_env ; (* TODO *)
  (* Either call [f] with a DIL variable holding the (forced, if [skip_nulls])
   * value of [e], or do nothing if [skip_nulls] and [e] is null: *)
  let with_expr ~skip_nulls d_env e f =
    let d = expression ~r_env ~d_env e in
    let_ ~name:"state_update_expr" ~l:d_env d (fun d_env d ->
      match DE.type_of d_env d, skip_nulls with
      | DT.Value { nullable = true ; _ }, true ->
          if_
            ~cond:(is_null d)
            ~then_:nop
            ~else_:(let_ ~name:"forced_op" ~l:d_env (force d) f)
      | _ ->
          f d_env d) in
  (* if [d] is nullable and null, then returns it, else apply [f] to (forced,
   * if nullable) value of [d] and return not_null (if nullable) of that
   * instead. This propagates [d]'s nullability to the result of the
   * aggregation. *)
  let null_map ~d_env d f =
    let_ ~name:"null_map" ~l:d_env d (fun d_env d ->
      match DE.type_of d_env d with
      | DT.Value { nullable = true ; _ } ->
          if_
            ~cond:(is_null d)
            ~then_:d
            ~else_:(ensure_nullable ~d_env (f d_env (force d)))
      | _ ->
        ensure_nullable ~d_env (f d_env d)) in
  let cmt = "update state for "^ what in
  let open DE.Ops in
  E.unpure_fold [] (fun _s lst e ->
    let convert_in = e.E.typ.DT.vtyp in
    match e.E.text with
    | Stateful (state_lifespan, skip_nulls, SF1 (
        (AggrMax | AggrMin | AggrFirst | AggrLast as op), e1)) ->
        let d_op =
          match op, e1.E.typ with
          (* As a special case, RaQL allows boolean arguments to min/max: *)
          | AggrMin, DT.{ vtyp = Mac Bool ; _ } ->
              and_
          | AggrMax, DT.{ vtyp = Mac Bool ; _ } ->
              or_
          | AggrMin, _ ->
              min_
          | AggrMax, _ ->
              max_
          | AggrFirst, _ ->
              (fun s _d -> s)
          | _ ->
              assert (op = AggrLast) ;
              (fun _s d -> d) in
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_state ~d_env state_rec e (fun d_env state ->
            let new_state_val =
              (* In any case, if we never got a value then we select this one and
               * call it a day: *)
              if_
                ~cond:(not_ (get_item 0 state))
                ~then_:(ensure_nullable ~d_env d1)
                ~else_:(
                  apply_2 ~convert_in d_env (get_item 1 state) d1
                          (fun _d_env -> d_op)) in
            set_state state_rec e (make_tup [ bool true ; new_state_val ]))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF1 (AggrSum, e1)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_state ~d_env state_rec e (fun d_env state ->
            (* Typing can decide the state and/or d1 are nullable.
             * In any case, nulls must propagate: *)
            let new_state =
              apply_2 ~convert_in d_env state d1 (fun _d_env -> add) in
            set_state state_rec e new_state)
        ) :: lst
        (* TODO: update for float with Kahan sum *)
    | Stateful (state_lifespan, skip_nulls, SF1 (AggrAvg, e1)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_state ~d_env state_rec e (fun d_env state ->
            let count = get_item 0 state
            and ksum = get_item 1 state in
            let new_state =
              null_map ~d_env d1 (fun d_env d ->
                make_tup [ add count (u32_of_int 1) ;
                           DS.Kahan.add ~l:d_env ksum d]) in
            set_state state_rec e new_state)
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF1 ((AggrAnd | AggrOr as op), e1)) ->
        let d_op = match op with AggrAnd -> and_ | _ -> or_ in
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_state ~d_env state_rec e (fun d_env state ->
            let new_state =
              apply_2 d_env state d1 (fun _d_env -> d_op) in
            set_state state_rec e new_state)
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF1 ((AggrBitAnd | AggrBitOr | AggrBitXor as op), e1)) ->
        let d_op =
          match op with AggrBitAnd -> log_and
                      | AggrBitOr -> log_or
                      | _ -> log_xor in
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_state ~d_env state_rec e (fun d_env state ->
            (* Typing can decide the state and/or d1 are nullable.
             * In any case, nulls must propagate: *)
            let new_state =
              apply_2 ~convert_in d_env state d1 (fun _d_env -> d_op) in
            set_state state_rec e new_state)
        ) :: lst
    | Stateful _ ->
        (* emit_expr ~env ~context:UpdateState ~opc opc.code e *)
        todo "state_update"
    | _ ->
        lst
  ) e |>
  List.rev |>
  seq |>
  comment cmt
