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

let rec conv ~from ~to_ d =
  if from = to_ then d else
  match from, to_ with
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac String -> string_of_int d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac Float -> to_float d
  | Mac String, Mac Float -> float_of_string d
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
  | Mac Float, Mac String -> string_of_float d
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
  | Vec (dim, mn), Mac String ->
      (* TODO: specialized version for lst/vec of chars that return the
       * string composed of those chars rather than an enumeration. *)
      conv_list mn (u32_of_int dim) d
  | Lst mn, Mac String ->
      conv_list mn (cardinality d) d
  | Mac Bool, Mac String ->
      if_ ~cond:d ~then_:(string "true") ~else_:(string "false")
  | Usr { name = ("Ip4" | "Ip6" | "Ip") ; _ }, Mac String ->
      string_of_ip d
  (* TODO: other types to string *)
  | _ ->
      Printf.sprintf2 "Not implemented: Cast from %a to %a of expression %a"
        DT.print_value_type from
        DT.print_value_type to_
        (DE.print ~max_depth:3) d |>
      failwith

and conv_list mn length_e src =
  (* We use a one entry vector as a ref cell: *)
  let dst = make_vec [ string "[" ] in
  let set r v = set_vec r (u32_of_int 0) v
  and get r = get_vec r (u32_of_int 0) in
  let idx_t = DT.(Value (required (Mac U32))) in
  let cond =
    DE.func1 idx_t (fun _l i -> lt i length_e)
  and body =
    DE.func1 idx_t (fun _l i ->
      let s1 = get dst
      and s2 =
        conv_maybe_nullable ~from:mn ~to_:DT.(required (Mac String))
                            (get_vec src i) in
      seq [ set dst (append_string s1 s2) ;
            add i (u32_of_int 1) ]) in
  seq [ ignore_ (loop_while ~init:(u32_of_int 0) ~cond ~body) ;
        get dst ]

and conv_maybe_nullable ~from ~to_ d =
  let conv = conv ~from:from.DT.vtyp ~to_:to_.DT.vtyp in
  match from.DT.nullable, to_.DT.nullable with
  | false, false ->
      conv d
  | true, false ->
      conv (force d)
  | false, true ->
      not_null (conv d)
  | true, true ->
      let_ ~name:"x_" d (fun _l x ->
        if_ ~cond:(is_null x)
            ~then_:(null to_.DT.vtyp)
            ~else_:(not_null (conv (force x))))

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
let rec expression ~r_env ~d_env raql =
  assert (E.is_typed raql) ;
  let expr = expression ~r_env ~d_env in
  let bad_type () =
    Printf.sprintf2 "Invalid type %a for expression %a"
      DT.print_maybe_nullable raql.E.typ
      (E.print false) raql |>
    failwith in
  let conv_from e d =
    conv ~from:e.E.typ.DT.vtyp ~to_:raql.E.typ.DT.vtyp d in
  (* For when [d] is nullable: *)
  let propagate_null ?(return_nullable=false) op d =
    let_ ~name:"nullable_" d (fun _l d ->
      if_
        ~cond:(is_null d)
        ~then_:(null raql.E.typ.DT.vtyp)
        ~else_:(
          let res = op (force d) in
          if return_nullable then res
          else not_null res)) in
  let propagate_nulls_1 ?return_nullable op e1 =
    let d1 = expression ~r_env ~d_env e1 in
    if e1.E.typ.DT.nullable then
      propagate_null ?return_nullable op d1
    else
      op d1 in
  let propagate_nulls_2 ?(return_nullable=false) op e1 e2 =
    let d1 = expression ~r_env ~d_env e1 in
    let d2 = expression ~r_env ~d_env e2 in
    match e1.E.typ.DT.nullable, e2.typ.nullable with
    | false, false ->
        op d1 d2
    | true, false ->
        propagate_null ~return_nullable (fun d1 -> op d1 d2) d1
    | false, true ->
        (* This will nicely shortcut the evaluation of d2 is d1 is null: *)
        propagate_null ~return_nullable (fun d2 -> op d1 d2) d2
    | true, true ->
        if_
          ~cond:(is_null d1)
          ~then_:(null raql.E.typ.DT.vtyp)
          ~else_:(
            if_
              ~cond:(is_null d2)
              ~then_:(null raql.E.typ.DT.vtyp)
              ~else_:(
                let res = op (force d1) (force d2) in
                if return_nullable then res
                else not_null res)) in
  match raql.E.text with
  | Const v ->
      constant raql.E.typ v
  | Tuple es ->
      (match raql.E.typ.DT.vtyp with
      | DT.Tup mns ->
          if Array.length mns <> List.length es then bad_type () ;
          make_tup (List.map expr es)
      | _ ->
          bad_type ())
  | Record nes ->
      make_rec (List.map (fun (n, e) ->
        (n : N.field :> string), expr e
      ) nes)
  | Vector es ->
      make_vec (List.map expr es)
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
              match DE.type_of d_env binding with
              | DT.(Value { nullable = true ; _ }) ->
                  propagate_null (get_field (field :> string)) binding
              | _ ->
                  get_field (field :> string) binding))
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
            | Some e -> expr e
            | None -> null raql.E.typ.DT.vtyp)
        | E.{ case_cond = cond ; case_cons = cons } :: alts' ->
            if_ ~cond:(expr cond)
                ~then_:(expr cons)
                ~else_:(alt_loop alts') in
      alt_loop alts
  | Stateless (SL0 Now) ->
      now
  | Stateless (SL0 Random) ->
      rand
  | Stateless (SL0 Pi) ->
      float Float.pi
  | Stateless (SL1 (Age, e1)) ->
      propagate_nulls_1 (fun d1 ->
        conv ~from:DT.(Mac Float) ~to_:raql.E.typ.DT.vtyp (
          sub now
              (conv ~from:e1.E.typ.DT.vtyp ~to_:DT.(Mac Float) d1))
      ) e1
  | Stateless (SL1 (Cast mn, e1)) ->
      propagate_nulls_1 (fun d1 ->
        conv ~from:e1.E.typ.DT.vtyp ~to_:mn.DT.vtyp d1
      ) e1
  | Stateless (SL1 (Length, e1)) ->
      propagate_nulls_1 (fun d1 ->
        match e1.E.typ.DT.vtyp with
        | DT.Mac String -> string_length d1
        | DT.Lst _ -> cardinality d1
        | _ -> bad_type ()
      ) e1
  | Stateless (SL1 (Lower, e1)) ->
      propagate_nulls_1 lower e1
  | Stateless (SL1 (Upper, e1)) ->
      propagate_nulls_1 upper e1
  | Stateless (SL1 (Not, e1)) ->
      propagate_nulls_1 not_ e1
  | Stateless (SL1 (Abs, e1)) ->
      propagate_nulls_1 abs e1
  | Stateless (SL1 (Minus, e1)) ->
      propagate_nulls_1 neg e1
  | Stateless (SL1 (Defined, e1)) ->
      not_ (is_null (expr e1))
  | Stateless (SL1 (Exp, e1)) ->
      propagate_nulls_1 exp e1
  | Stateless (SL1 (Log, e1)) ->
      propagate_nulls_1 log e1
  | Stateless (SL1 (Log10, e1)) ->
      propagate_nulls_1 log10 e1
  | Stateless (SL1 (Sqrt, e1)) ->
      propagate_nulls_1 sqrt e1
  | Stateless (SL1 (Ceil, e1)) ->
      propagate_nulls_1 ceil e1
  | Stateless (SL1 (Floor, e1)) ->
      propagate_nulls_1 floor e1
  | Stateless (SL1 (Round, e1)) ->
      propagate_nulls_1 round e1
  | Stateless (SL1 (Cos, e1)) ->
      propagate_nulls_1 cos e1
  | Stateless (SL1 (Sin, e1)) ->
      propagate_nulls_1 sin e1
  | Stateless (SL1 (Tan, e1)) ->
      propagate_nulls_1 tan e1
  | Stateless (SL1 (ACos, e1)) ->
      propagate_nulls_1 acos e1
  | Stateless (SL1 (ASin, e1)) ->
      propagate_nulls_1 asin e1
  | Stateless (SL1 (ATan, e1)) ->
      propagate_nulls_1 atan e1
  | Stateless (SL1 (CosH, e1)) ->
      propagate_nulls_1 cosh e1
  | Stateless (SL1 (SinH, e1)) ->
      propagate_nulls_1 sinh e1
  | Stateless (SL1 (TanH, e1)) ->
      propagate_nulls_1 tanh e1
  | Stateless (SL1 (Hash, e1)) ->
      propagate_nulls_1 hash e1
  | Stateless (SL1 (Chr, e1)) ->
      propagate_nulls_1 (fun d1 ->
        char_of_u8 (conv ~from:e1.E.typ.DT.vtyp ~to_:DT.(Mac U8) d1)
      ) e1
  | Stateless (SL1s ((Max | Min as op), es)) ->
      let d_op = match op with Max -> max | _ -> min in
      let rec loop = function
        | [] ->
            assert false
        | [ e ] ->
            propagate_nulls_1 (fun d ->
              conv_from e d
            ) e
        | e :: es' ->
            propagate_nulls_1 ~return_nullable:true (fun d1 ->
              let rest = { raql with text = Stateless (SL1s (op, es')) } in
              propagate_nulls_1 (fun d2 ->
                d_op (conv_from e d1) d2
              ) rest
            ) e in
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
      let rec loop i = function
        | [] ->
            assert false (* because of type checking *)
        | [ e ] ->
            conv_maybe_nullable ~from:e.E.typ ~to_:raql.E.typ (expr e)
        | e :: es ->
            assert e.E.typ.DT.nullable ; (* Because of type-checking *)
            let name = "coalesced_"^ Pervasives.string_of_int i in
            let_ ~name (expr e) (fun _l d ->
              if_
                ~cond:(is_null d)
                ~then_:(loop (i + 1) es)
                ~else_:(
                  if raql.E.typ.DT.nullable then
                    not_null (conv ~from:e.E.typ.DT.vtyp ~to_:raql.typ.vtyp
                                   (force d))
                  else
                    force d)) in
      loop 0 es
  | Stateless (SL2 (Add, e1, e2)) ->
      propagate_nulls_2 add e1 e2
  | Stateless (SL2 (Sub, e1, e2)) ->
      propagate_nulls_2 sub e1 e2
  | Stateless (SL2 (Mul, e1, e2)) ->
      propagate_nulls_2 mul e1 e2
  | Stateless (SL2 (Div, e1, e2)) ->
      propagate_nulls_2 div e1 e2
  | Stateless (SL2 (IDiv, e1, e2)) ->
      (* When the result is a float we need to floor it *)
      (match raql.E.typ with
      | DT.{ vtyp = Mac Float ; _ } ->
          propagate_nulls_2 (fun d1 d2 -> floor (div d1 d2)) e1 e2
      | _ ->
          propagate_nulls_2 div e1 e2)
  | Stateless (SL2 (Mod, e1, e2)) ->
      propagate_nulls_2 rem e1 e2
  | Stateless (SL2 (Pow, e1, e2)) ->
      propagate_nulls_2 pow e1 e2
  | Stateless (SL2 (And, e1, e2)) ->
      propagate_nulls_2 and_ e1 e2
  | Stateless (SL2 (Or, e1, e2)) ->
      propagate_nulls_2 or_ e1 e2
  | Stateless (SL2 (Ge, e1, e2)) ->
      propagate_nulls_2 ge e1 e2
  | Stateless (SL2 (Gt, e1, e2)) ->
      propagate_nulls_2 gt e1 e2
  | Stateless (SL2 (Eq, e1, e2)) ->
      propagate_nulls_2 eq e1 e2
  | Stateless (SL2 (StartsWith, e1, e2)) ->
      propagate_nulls_2 starts_with e1 e2
  | Stateless (SL2 (EndsWith, e1, e2)) ->
      propagate_nulls_2 ends_with e1 e2
  | Stateless (SL2 (BitAnd, e1, e2)) ->
      propagate_nulls_2 log_and e1 e2
  | Stateless (SL2 (BitOr, e1, e2)) ->
      propagate_nulls_2 log_or e1 e2
  | Stateless (SL2 (BitXor, e1, e2)) ->
      propagate_nulls_2 log_xor e1 e2
  | Stateless (SL2 (BitShift, e1, { text = Stateless (SL1 (Minus, e2)) ; _ })) ->
      propagate_nulls_2 (fun d1 d2 ->
        right_shift d1 (to_u8 d2)
      ) e1 e2
  | Stateless (SL2 (BitShift, e1, e2)) ->
      propagate_nulls_2 (fun d1 d2 ->
        left_shift d1 (to_u8 d2)
      ) e1 e2
  | Stateless (SL2 (Get, { text = Const (VString n) ; _ }, e1)) ->
      propagate_nulls_1 (get_field n) e1
  | _ ->
      Printf.sprintf2 "RaQL2DIL.expression for %a"
        (E.print false) raql |>
      todo

(*$= expression & ~printer:identity
  "(u8 1)" (expression (E.of_u8 1) |> IO.to_string DE.print)
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
