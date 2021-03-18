(* Compile (typed!) RaQL expressions into DIL expressions *)
open Batteries
open RamenHelpersNoLog
open RamenLog
open Dessser
module DE = DessserExpressions
module DT = DessserTypes
module E = RamenExpr
module Lang = RamenLang
module N = RamenName
module T = RamenTypes
open DE.Ops

(*$inject
  open Batteries *)

let mn_of_t = function
  | DT.Value mn -> mn
  | t -> invalid_arg ("conversion for type "^ DT.to_string t)

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
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I8 -> to_i8 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I16 -> to_i16 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I24 -> to_i24 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I32 -> to_i32 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I40 -> to_i40 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I48 -> to_i48 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I56 -> to_i56 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I64 -> to_i64 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac I128 -> to_i128 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac U8 -> to_u8 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac U16 -> to_u16 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac U24 -> to_u24 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac U32 -> to_u32 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac U40 -> to_u40 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac U48 -> to_u48 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac U56 -> to_u56 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    Mac U64 -> to_u64 d
  | Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
         U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
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
    let set v = set_vec dst (u32_of_int 0) v
    and get () = get_vec dst (u32_of_int 0) in
    let idx_t = DT.(Value (required (Mac U32))) in
    let cond =
      DE.func1 ~l idx_t (fun _l i -> lt i length_e)
    and body =
      DE.func1 ~l idx_t (fun _l i ->
        let s =
          conv_maybe_nullable ~depth:(depth+1) ~to_:DT.(required (Mac String))
                              l (get_vec src i) in
        seq [ set (append_string (get ()) s) ;
              add i (u32_of_int 1) ]) in
    seq [ ignore_ (loop_while ~init:(u32_of_int 0) ~cond ~body) ;
          set (append_string (get ()) (string "]")) ;
          get () ])

and conv_charseq ?(depth=0) length_e l src =
  (* We use a one entry vector as a ref cell: *)
  let_ ~name:"dst_" ~l (make_vec [ string "" ]) (fun _l dst ->
    let set v = set_vec dst (u32_of_int 0) v
    and get () = get_vec dst (u32_of_int 0) in
    let idx_t = DT.(Value (required (Mac U32))) in
    let cond =
      DE.func1 ~l idx_t (fun _l i -> lt i length_e)
    and body =
      DE.func1 ~l idx_t (fun _l i ->
        let s =
          conv_maybe_nullable ~depth:(depth+1) ~to_:DT.(required (Mac Char))
                              l (get_vec src i) in
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

let print_r_env oc =
  pretty_list_print (fun oc (k, v) ->
    Printf.fprintf oc "%a=>%a"
      E.print_binding_key k
      (DE.print ~max_depth:2) v
  ) oc

(* Environments:
 * - [d_env] is the environment used by dessser, ie. a stack of
 *   expression x type (expression being for instance [(identifier n)] or
 *   [(param n m)]. This is used by dessser to do type-checking.
 * - [r_env] is the stack of currently reachable "raql thing", such
 *   as expression state, a record (in other words an E.binding_key), bound
 *   to a dessser expression (typically an identifier or a param). *)
let rec expression ?(depth=0) ~r_env ~d_env raql =
  !logger.debug "%sCompiling into DIL: %a"
    (indent_of depth)
    (E.print true) raql ;
  assert (E.is_typed raql) ;
  let expr = expression ~depth:(depth+1) ~r_env ~d_env in
  let bad_type () =
    Printf.sprintf2 "Invalid type %a for expression %a"
      DT.print_maybe_nullable raql.E.typ
      (E.print false) raql |>
    failwith in
  let conv = conv ~depth:(depth+1)
  and conv_maybe_nullable = conv_maybe_nullable ~depth:(depth+1) in
  let conv_from d =
    conv ~to_:raql.E.typ.DT.vtyp d_env d in
  let conv_maybe_nullable_from d_env d =
    conv_maybe_nullable ~to_:raql.E.typ d_env d in
  (* For when [d] is nullable: *)
  let propagate_null d_env d f =
    !logger.debug "%s...propagating null from %a"
      (indent_of depth)
      (DE.print ?max_depth:None) d ;
    (* Never ever force a NULL: *)
    match d with
    | DE.E0 (Null _) ->
        null raql.E.typ.DT.vtyp
    | d ->
        let_ ~name:"nullable_" ~l:d_env d (fun d_env d ->
          if_
            ~cond:(is_null d)
            ~then_:(null raql.E.typ.DT.vtyp)
            (* Since [f] can return a nullable value already, rely on
             * [conv_maybe_nullable_from] to do the right thing instead of
             * [not_null]: *)
            ~else_:(conv_maybe_nullable_from d_env (f d_env (force d)))) in
  (* [apply_1] takes a DIL expression rather than a RaQL expression because
   * of Binding: *)
  let apply_1 ?(propagate_nulls=true) ?(convert_in=false) d_env d1 f =
    let no_prop d_env d1 =
      let d1 = if convert_in then conv_maybe_nullable_from d_env d1 else d1 in
      f d_env d1 in
    if propagate_nulls then (
      let t1 = mn_of_t (DE.type_of d_env d1) in
      if t1.DT.nullable then
        propagate_null d_env d1 no_prop
      else
        no_prop d_env d1
    ) else (
      (* It means the [f] takes the operand as is ; so the operator must accept
       * all possible types set by the type checker. *)
       no_prop d_env d1
    ) in
  let apply_2 ?(propagate_nulls=true) ?(convert_in=false) ?(enlarge_in=false)
              d_env e1 e2 f =
    assert (not convert_in || not enlarge_in) ;
    (* When neither d1 nor d2 are nullable: *)
    let no_prop d_env d1 d2 =
      let d1 = if convert_in then conv_maybe_nullable_from d_env d1 else d1 in
      let d2 = if convert_in then conv_maybe_nullable_from d_env d2 else d2 in
      let d1, d2 =
        if convert_in then
          conv_maybe_nullable_from d_env d1, conv_maybe_nullable_from d_env d2
        else if enlarge_in then
          let vtyp = T.largest_type [ e1.E.typ.vtyp ; e2.E.typ.vtyp ]
          and t1 = mn_of_t (DE.type_of d_env d1)
          and t2 = mn_of_t (DE.type_of d_env d2) in
          conv_maybe_nullable ~to_:DT.{ t1 with vtyp } d_env d1,
          conv_maybe_nullable ~to_:DT.{ t2 with vtyp } d_env d2
        else d1, d2 in
      f d_env d1 d2 in
    (* If d1 is not nullable: *)
    let no_prop_d1 d_env d1 =
      let d2 = expr e2 in
      let t2 = mn_of_t (DE.type_of d_env d2) in
      if t2.DT.nullable then (
        propagate_null d_env d2 (fun d_env d2 ->
          let d1 = if convert_in then conv_maybe_nullable_from d_env d1 else d1 in
          let d2 = if convert_in then conv_maybe_nullable_from d_env d2 else d2 in
          f d_env d1 d2)
      ) else (
        (* neither e1 nor e2 is nullable so no need to propagate nulls: *)
        no_prop d_env d1 d2
      ) in
    let d1 = expr e1 in
    if propagate_nulls then (
      let t1 = mn_of_t (DE.type_of d_env d1) in
      if t1.DT.nullable then (
        propagate_null d_env d1 no_prop_d1
      ) else (
        no_prop_d1 d_env d1
      )
    ) else (
      let d2 = expr e2 in
      no_prop d_env d1 d2
    ) in
  (* In any case we want the output to be converted to the expected type: *)
  conv_maybe_nullable_from d_env (
    match raql.E.text with
    | Const v ->
        constant raql.E.typ v
    | Tuple es ->
        (match raql.E.typ.DT.vtyp with
        | DT.Tup mns ->
            if Array.length mns <> List.length es then bad_type () ;
            (* Better convert items before constructing the tuple: *)
            List.mapi (fun i e ->
              conv_maybe_nullable ~to_:mns.(i) d_env (expr e)
            ) es |>
            make_tup
        | _ ->
            bad_type ())
    | Record nes ->
        (match raql.E.typ.DT.vtyp with
        | DT.Rec mns ->
            if Array.length mns <> List.length nes then bad_type () ;
            List.mapi (fun i (n, e) ->
              (n : N.field :> string),
              conv_maybe_nullable ~to_:(snd mns.(i)) d_env (expr e)
            ) nes |>
            make_rec
        | _ ->
            bad_type ())
    | Vector es ->
        (match raql.E.typ.DT.vtyp with
        | DT.Vec (dim, mn) ->
            if dim <> List.length es then bad_type () ;
            List.map (fun e ->
              conv_maybe_nullable ~to_:mn d_env (expr e)
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
              | Some e -> conv_maybe_nullable_from d_env (expr e)
              | None -> null raql.E.typ.DT.vtyp)
          | E.{ case_cond = cond ; case_cons = cons } :: alts' ->
              let do_cond d_env cond =
                if_ ~cond
                    ~then_:(conv_maybe_nullable ~to_:raql.E.typ d_env (expr cons))
                    ~else_:(alt_loop alts') in
              if cond.E.typ.DT.nullable then
                let_ ~name:"nullable_cond_" ~l:d_env (expr cond) (fun d_env cond ->
                  if_ ~cond:(is_null cond)
                      ~then_:(null raql.E.typ.DT.vtyp)
                      ~else_:(do_cond d_env (force cond)))
              else
                do_cond d_env (expr cond) in
        alt_loop alts
    | Stateless (SL0 Now) ->
        conv_from now
    | Stateless (SL0 Random) ->
        random_float
    | Stateless (SL0 Pi) ->
        float Float.pi
    | Stateless (SL1 (Age, e1)) ->
        apply_1 ~convert_in:true d_env (expr e1) (fun _l d -> sub now d)
    | Stateless (SL1 (Cast _, e1)) ->
        (* Type checking already set the output type of that Raql expression to the
         * target type, and apply_1 is going to convert e1 into that: *)
        apply_1 ~convert_in:true d_env (expr e1) (fun _l d -> d)
    | Stateless (SL1 (Force, e1)) ->
        force (expr e1)
    | Stateless (SL1 (Length, e1)) ->
        apply_1 d_env (expr e1) (fun _l d1 ->
          match e1.E.typ.DT.vtyp with
          | DT.Mac String -> string_length d1
          | DT.Lst _ -> cardinality d1
          | _ -> bad_type ()
        )
    | Stateless (SL1 (Lower, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> lower d)
    | Stateless (SL1 (Upper, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> upper d)
    | Stateless (SL1 (Not, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> not_ d)
    | Stateless (SL1 (Abs, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> abs d)
    | Stateless (SL1 (Minus, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> neg d)
    | Stateless (SL1 (Defined, e1)) ->
        not_ (is_null (expr e1))
    | Stateless (SL1 (Exp, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> exp d)
    | Stateless (SL1 (Log, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> log d)
    | Stateless (SL1 (Log10, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> log10 d)
    | Stateless (SL1 (Sqrt, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> sqrt d)
    | Stateless (SL1 (Ceil, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> ceil d)
    | Stateless (SL1 (Floor, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> floor d)
    | Stateless (SL1 (Round, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> round d)
    | Stateless (SL1 (Cos, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> cos d)
    | Stateless (SL1 (Sin, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> sin d)
    | Stateless (SL1 (Tan, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> tan d)
    | Stateless (SL1 (ACos, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> acos d)
    | Stateless (SL1 (ASin, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> asin d)
    | Stateless (SL1 (ATan, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> atan d)
    | Stateless (SL1 (CosH, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> cosh d)
    | Stateless (SL1 (SinH, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> sinh d)
    | Stateless (SL1 (TanH, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> tanh d)
    | Stateless (SL1 (Hash, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> hash d)
    | Stateless (SL1 (Chr, e1)) ->
        apply_1 d_env (expr e1) (fun d_env d1 ->
          char_of_u8 (conv ~to_:DT.(Mac U8) d_env d1)
        )
    | Stateless (SL1 (Basename, e1)) ->
        apply_1 d_env (expr e1) (fun d_env d1 ->
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
        let rec loop = function
          | [] ->
              assert false
          | [ e ] ->
              apply_1 d_env (expr e) (fun _d_env d -> conv_from d)
          | e :: es' ->
              apply_1 d_env (expr e) (fun d_env d1 ->
                let rest = { raql with text = Stateless (SL1s (op, es')) } in
                apply_1 d_env (expr rest) (fun _d_env d2 ->
                  d_op (conv_from d1) d2
                )
              ) in
        loop es
    | Stateless (SL1s (Print, es)) ->
        (match List.rev es with
        | last :: _ as es ->
            seq (
              List.fold_left (fun lst e -> dump (expr e) :: lst
              ) [ expr last ] es)
        | [] ->
            invalid_arg "RaQL2DIL.expression: empty PRINT")
    | Stateless (SL1s (Coalesce, es)) ->
        let es =
          List.map (fun e ->
            let to_ = { raql.E.typ with nullable = e.E.typ.nullable } in
            conv_maybe_nullable ~to_ d_env (expr e)
          ) es in
        DessserStdLib.coalesce d_env es
    | Stateless (SL2 (Add, e1, e2)) ->
        apply_2 ~convert_in:true d_env e1 e2 (fun _d_env d1 d2 -> add d1 d2)
    | Stateless (SL2 (Sub, e1, e2)) ->
        apply_2 ~convert_in:true d_env e1 e2 (fun _d_env d1 d2 -> sub d1 d2)
    | Stateless (SL2 (Mul, e1, e2)) ->
        apply_2 ~convert_in:true d_env e1 e2 (fun _d_env d1 d2 -> mul d1 d2)
    | Stateless (SL2 (Div, e1, e2)) ->
        apply_2 ~convert_in:true d_env e1 e2 (fun _d_env d1 d2 -> div d1 d2)
    | Stateless (SL2 (IDiv, e1, e2)) ->
        (* When the result is a float we need to floor it *)
        (match raql.E.typ with
        | DT.{ vtyp = Mac Float ; _ } ->
            apply_2 ~convert_in:true d_env e1 e2 (fun _d_env d1 d2 ->
              floor (div d1 d2))
        | _ ->
            apply_2 ~convert_in:true d_env e1 e2 (fun _d_env d1 d2 -> div d1 d2))
    | Stateless (SL2 (Mod, e1, e2)) ->
        apply_2 ~convert_in:true d_env e1 e2 (fun _d_env d1 d2 -> rem d1 d2)
    | Stateless (SL2 (Pow, e1, e2)) ->
        apply_2 ~convert_in:true d_env e1 e2 (fun _d_env d1 d2 -> pow d1 d2)
    | Stateless (SL2 (And, e1, e2)) ->
        apply_2 d_env e1 e2 (fun _d_env d1 d2 -> and_ d1 d2)
    | Stateless (SL2 (Or, e1, e2)) ->
        apply_2 d_env e1 e2 (fun _d_env d1 d2 -> or_ d1 d2)
    | Stateless (SL2 (Ge, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env e1 e2 (fun _d_env d1 d2 -> ge d1 d2)
    | Stateless (SL2 (Gt, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env e1 e2 (fun _d_env d1 d2 -> gt d1 d2)
    | Stateless (SL2 (Eq, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env e1 e2 (fun _d_env d1 d2 -> eq d1 d2)
    | Stateless (SL2 (StartsWith, e1, e2)) ->
        apply_2 d_env e1 e2 (fun _d_env d1 d2 -> starts_with d1 d2)
    | Stateless (SL2 (EndsWith, e1, e2)) ->
        apply_2 d_env e1 e2 (fun _d_env d1 d2 -> ends_with d1 d2)
    | Stateless (SL2 (BitAnd, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env e1 e2 (fun _d_env d1 d2 -> log_and d1 d2)
    | Stateless (SL2 (BitOr, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env e1 e2 (fun _d_env d1 d2 -> log_or d1 d2)
    | Stateless (SL2 (BitXor, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env e1 e2 (fun _d_env d1 d2 -> log_xor d1 d2)
    | Stateless (SL2 (BitShift, e1, { text = Stateless (SL1 (Minus, e2)) ; _ })) ->
        apply_2 d_env e1 e2 (fun _d_env d1 d2 -> right_shift d1 (to_u8 d2))
    | Stateless (SL2 (BitShift, e1, e2)) ->
        apply_2 d_env e1 e2 (fun _d_env d1 d2 -> left_shift d1 (to_u8 d2))
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ }, e1)) ->
        apply_1 d_env (expr e1) (fun _l d -> (get_field n) d)
    | Stateless (SL2 (Index, e1, e2)) ->
        apply_2 d_env e1 e2 (fun _d_env d1 (* string *) d2 (* char *) ->
          match find_substring true_ (string_of_char d2) d1 with
          | E0 (Null _) ->
              i32_of_int ~-1
          | res ->
              let_ ~name:"index_" ~l:d_env res (fun d_env res ->
                if_
                  ~cond:(is_null res)
                  ~then_:(i32_of_int ~-1)
                  ~else_:(conv ~to_:DT.(Mac I32) d_env (force res))))
    | _ ->
        Printf.sprintf2 "RaQL2DIL.expression for %a"
          (E.print false) raql |>
        todo
  )

(*$= expression & ~printer:identity
  "(u8 1)" (expression ~r_env:[] ~d_env:[] (E.of_u8 1) |> IO.to_string DE.print)
*)

let init_state ~r_env ~d_env raql =
  (* TODO *)
  ignore d_env ; ignore r_env ; ignore raql ;
  seq []

let state_update_for_expr ~r_env ~d_env ~what e =
  ignore d_env ; ignore r_env ; (* TODO *)
  let cmt = "update state for "^ what in
  let open DE.Ops in
  E.unpure_fold [] (fun _s l e ->
    match e.E.text with
    | Stateful _ ->
        (* emit_expr ~env ~context:UpdateState ~opc opc.code e *)
        todo "state_update"
    | _ ->
        l
  ) e |>
  seq |>
  comment cmt
