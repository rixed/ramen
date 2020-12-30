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

let cast ~from ~to_ d =
  if from = to_ then d else
  match from, to_ with
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac String -> string_of_int d
  | DT.Mac String, DT.Mac Float -> float_of_string d
  | DT.Mac String, DT.Mac Char -> char_of_string d
  | DT.Mac String, DT.Mac I8 -> i8_of_string d
  | DT.Mac String, DT.Mac I16 -> i16_of_string d
  | DT.Mac String, DT.Mac I24 -> i24_of_string d
  | DT.Mac String, DT.Mac I32 -> i32_of_string d
  | DT.Mac String, DT.Mac I40 -> i40_of_string d
  | DT.Mac String, DT.Mac I48 -> i48_of_string d
  | DT.Mac String, DT.Mac I56 -> i56_of_string d
  | DT.Mac String, DT.Mac I64 -> i64_of_string d
  | DT.Mac String, DT.Mac I128 -> i128_of_string d
  | DT.Mac String, DT.Mac U8 -> u8_of_string d
  | DT.Mac String, DT.Mac U16 -> u16_of_string d
  | DT.Mac String, DT.Mac U24 -> u24_of_string d
  | DT.Mac String, DT.Mac U32 -> u32_of_string d
  | DT.Mac String, DT.Mac U40 -> u40_of_string d
  | DT.Mac String, DT.Mac U48 -> u48_of_string d
  | DT.Mac String, DT.Mac U56 -> u56_of_string d
  | DT.Mac String, DT.Mac U64 -> u64_of_string d
  | DT.Mac String, DT.Mac U128 -> u128_of_string d
  | DT.Mac Float, DT.Mac String -> string_of_float d
  | DT.Mac Char, DT.Mac U8 -> u8_of_char d
  | DT.Mac U8, DT.Mac Char -> char_of_u8 d
  | DT.Mac Bool, DT.Mac U8 -> u8_of_bool d
  | DT.Mac Char, DT.Mac String -> string_of_char d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I8 -> to_i8 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I16 -> to_i16 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I24 -> to_i24 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I32 -> to_i32 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I40 -> to_i40 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I48 -> to_i48 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I56 -> to_i56 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I64 -> to_i64 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac I128 -> to_i128 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U8 -> to_u8 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U16 -> to_u16 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U24 -> to_u24 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U32 -> to_u32 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U40 -> to_u40 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U48 -> to_u48 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U56 -> to_u56 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U64 -> to_u64 d
  | DT.Mac (I8 | I16 | I24 | I32 | I40 | I48 | I56 | I64 | I128 |
            U8 | U16 | U24 | U32 | U40 | U48 | U56 | U64 | U128),
    DT.Mac U128 -> to_u128 d
  | _ ->
      Printf.sprintf2 "Not implemented: Cast from %a to %a"
        DT.print_value_type from
        DT.print_value_type to_ |>
      failwith

let cast_maybe_nullable ~from ~to_ d =
  let cast = cast ~from:from.DT.vtyp ~to_:to_.DT.vtyp in
  match from.DT.nullable, to_.DT.nullable with
  | false, false ->
      cast d
  | true, false ->
      cast (force d)
  | false, true ->
      not_null (cast d)
  | true, true ->
      let_ "x_" d ~in_:(
        if_ ~cond:(is_null (identifier "x_"))
            ~then_:(null to_.DT.vtyp)
            ~else_:(not_null (cast (force (identifier "x_")))))

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
      make_rec [ string "ip" ; u32 i ; string "mask" ; u8 m ]
  | VCidrv6 (i, m) ->
      make_rec [ string "ip" ; u128 i ; string "mask" ; u8 m ]
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
          make_list mn (List.init (Array.length vs) (fun i ->
            constant mn vs.(i)))
      | _ ->
          bad_type ())
  | VRec vs ->
      (match mn.vtyp with
      | DT.Rec mns ->
          if Array.length mns <> Array.length vs then bad_type () ;
          make_rec (Array.fold_left (fun lst (n, mn) ->
            match Array.find (fun (n', _) -> n = n') vs with
            | exception Not_found ->
                bad_type ()
            | _, v ->
                string n :: constant mn v :: lst
          ) [] mns)
      | _ ->
          bad_type ())
  | VMap _ ->
      invalid_arg "constant: not for VMaps"

let rec expression ?(dil_env=[]) ?(raql_env=[]) raql =
  assert (E.is_typed raql) ;
  let expr = expression ~dil_env ~raql_env in
  let bad_type () =
    Printf.sprintf2 "Invalid type %a for expression %a"
      DT.print_maybe_nullable raql.E.typ
      (E.print false) raql |>
    failwith in
  let cast_from e d =
    cast ~from:e.E.typ.DT.vtyp ~to_:raql.E.typ.DT.vtyp d in
  (* If [d] is nullable, propagate nulls through [f] otherwise just call [f]: *)
  let propagate_null ?(f_returns_nullable=false) ?(cast_to_raql=false) d f =
    let t = DE.type_of dil_env d |> DT.develop_user_types in
    match t with
    | DT.Value { nullable = true ; vtyp } ->
        let not_null' =
          if f_returns_nullable then BatPervasives.identity else not_null in
        let_ "propagate_null_" d ~in_:(
          let d = identifier "propagate_null_" in
          if_ ~cond:(is_null d)
              ~then_:(null vtyp)
              ~else_:(
                let d = if cast_to_raql
                          then cast ~from:vtyp ~to_:raql.E.typ.DT.vtyp d
                          else d in
                not_null' (f d)))
    | DT.Value { nullable = false ; vtyp } ->
        let d = if cast_to_raql
                  then cast ~from:vtyp ~to_:raql.E.typ.DT.vtyp d
                  else d in
        f d
    | _ ->
        invalid_arg "propagate_null" in
  let propagate_null2 ?f_returns_nullable ?cast_to_raql d1 d2 f =
    propagate_null ~f_returns_nullable:true ?cast_to_raql d1 (fun d1 ->
      propagate_null ?f_returns_nullable ?cast_to_raql d2 (fun d2 ->
        f d1 d2)) in
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
      make_rec (List.fold_left (fun lst (n, e) ->
        string (n : N.field :> string) :: expr e :: lst
      ) [] nes)
  | Vector es ->
      make_vec (List.map expr es)
  | Variable v ->
      (* We probably want to replace this with a DIL identifier with a
       * well known name: *)
      identifier (Lang.string_of_variable v)
  | Binding _ ->
      (* Should not be met at this stage *)
      assert false
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
  | Stateless (SL1 (Age, e)) ->
      propagate_null (expr e) (fun d ->
        cast ~from:DT.(Mac Float) ~to_:raql.E.typ.DT.vtyp (
          sub now
              (cast ~from:e.E.typ.DT.vtyp ~to_:DT.(Mac Float) d)))
  | Stateless (SL1 (Cast mn, e)) ->
      propagate_null (expr e) (fun d ->
        cast ~from:e.E.typ.DT.vtyp ~to_:mn.DT.vtyp d)
  | Stateless (SL1 (Length, e)) ->
      propagate_null (expr e) (
        match e.E.typ.DT.vtyp with
        | DT.Mac String -> string_length
        | DT.Lst _ -> cardinality
        | _ -> bad_type ())
  | Stateless (SL1 (Lower, e)) ->
      propagate_null (expr e) lower
  | Stateless (SL1 (Upper, e)) ->
      propagate_null (expr e) upper
  | Stateless (SL1 (Not, e)) ->
      propagate_null (expr e) not_
  | Stateless (SL1 (Abs, e)) ->
      propagate_null (expr e) abs
  | Stateless (SL1 (Minus, e)) ->
      propagate_null (expr e) neg
  | Stateless (SL1 (Defined, e)) ->
      not_ (is_null (expr e))
  | Stateless (SL1 (Exp, e)) ->
      propagate_null (expr e) exp
  | Stateless (SL1 (Log, e)) ->
      propagate_null (expr e) log
  | Stateless (SL1 (Log10, e)) ->
      propagate_null (expr e) log10
  | Stateless (SL1 (Sqrt, e)) ->
      propagate_null (expr e) sqrt
  | Stateless (SL1 (Ceil, e)) ->
      propagate_null (expr e) ceil
  | Stateless (SL1 (Floor, e)) ->
      propagate_null (expr e) floor
  | Stateless (SL1 (Round, e)) ->
      propagate_null (expr e) round
  | Stateless (SL1 (Cos, e)) ->
      propagate_null (expr e) cos
  | Stateless (SL1 (Sin, e)) ->
      propagate_null (expr e) sin
  | Stateless (SL1 (Tan, e)) ->
      propagate_null (expr e) tan
  | Stateless (SL1 (ACos, e)) ->
      propagate_null (expr e) acos
  | Stateless (SL1 (ASin, e)) ->
      propagate_null (expr e) asin
  | Stateless (SL1 (ATan, e)) ->
      propagate_null (expr e) atan
  | Stateless (SL1 (CosH, e)) ->
      propagate_null (expr e) cosh
  | Stateless (SL1 (SinH, e)) ->
      propagate_null (expr e) sinh
  | Stateless (SL1 (TanH, e)) ->
      propagate_null (expr e) tanh
  | Stateless (SL1 (Hash, e)) ->
      propagate_null (expr e) hash
  | Stateless (SL1 (Chr, e)) ->
      char_of_u8 (cast ~from:e.E.typ.DT.vtyp ~to_:DT.(Mac U8) (expr e))
  | Stateless (SL1s ((Max | Min as op), es)) ->
      let d_op = match op with Max -> max | _ -> min in
      let rec loop = function
        | [] ->
            assert false
        | [ e ] ->
            propagate_null (expr e) (fun d ->
              cast_from e d)
        | e :: es' ->
            propagate_null (expr e) (fun d1 ->
              let rest = { raql with text = Stateless (SL1s (op, es')) } in
              propagate_null (expr rest) (fun d2 ->
                d_op (cast_from e d1) d2)) in
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
      coalesce (
        List.map (fun e ->
          propagate_null (expr e) (cast_from e)
        ) es)
  | Stateless (SL2 (Add, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) add
  | Stateless (SL2 (Sub, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) sub
  | Stateless (SL2 (Mul, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) mul
  | Stateless (SL2 (Div, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) div
  | Stateless (SL2 (IDiv, e1, e2)) ->
      (* When the result is a float we need to floor it *)
      (match raql.E.typ with
      | DT.{ vtyp = Mac Float ; _ } ->
          propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) (fun d1 d2 ->
            propagate_null (div d1 d2) floor)
      | _ ->
          propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) div)
  | Stateless (SL2 (Mod, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) rem
  | Stateless (SL2 (Pow, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) pow
  | Stateless (SL2 (And, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) and_
  | Stateless (SL2 (Or, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) or_
  | Stateless (SL2 (Ge, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) ge
  | Stateless (SL2 (Gt, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) gt
  | Stateless (SL2 (Eq, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) eq
  | Stateless (SL2 (StartsWith, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) starts_with
  | Stateless (SL2 (EndsWith, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) ends_with
  | Stateless (SL2 (BitAnd, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) log_and
  | Stateless (SL2 (BitOr, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) log_or
  | Stateless (SL2 (BitXor, e1, e2)) ->
      propagate_null2 ~cast_to_raql:true (expr e1) (expr e2) log_xor
  | _ ->
      Printf.sprintf2 "RaQL2DIL.expression for %a"
        (E.print false) raql |>
      todo

(*$= expression & ~printer:identity
  "(u8 1)" (expression (E.of_u8 1) |> IO.to_string DE.print)
*)
