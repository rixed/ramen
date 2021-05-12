(* Compile (typed!) RaQL expressions into DIL expressions *)
open Batteries

open RamenHelpersNoLog
open RamenHelpers
open RamenLog
module DC = DessserConversions
module DE = DessserExpressions
module DT = DessserTypes
module DS = DessserStdLib
module DU = DessserCompilationUnit
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

let print_r_env oc =
  pretty_list_print (fun oc (k, v) ->
    Printf.fprintf oc "%a=>%a"
      E.print_binding_key k
      (DE.print ~max_depth:2) v
  ) oc

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
  | VIpv4 i -> make_usr "Ip4" [ u32 i ]
  | VIpv6 i -> make_usr "Ip6" [ u128 i ]
  | VIp (RamenIp.V4 i) -> make_usr "Ip" [ make_usr "Ip4" [ u32 i ] ]
  | VIp (RamenIp.V6 i) -> make_usr "Ip" [ make_usr "Ip6" [ u128 i ] ]
  | VCidrv4 (i, m) -> make_usr "Cidr4" [ u32 i ; u8 m ]
  | VCidrv6 (i, m) -> make_usr "Cidr6" [ u128 i ; u8 m ]
  | VCidr (RamenIp.Cidr.V4 (i, m)) ->
      make_usr "Cidr" [ make_usr "Cidr4" [ u32 i ; u8 m ] ]
  | VCidr (RamenIp.Cidr.V6 (i, m)) ->
      make_usr "Cidr" [ make_usr "Cidr6" [ u128 i ; u8 m ] ]
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
 * last removed values (which will come handy to optimize some stateful
 * operator over sliding windows);
 *
 * So, like with the legacy code generator, states are kept in a record (field
 * names given by [field_name_of_state]);
 * actually, one record for the global states and one for each local (aka
 * group-wide) states. The exact type of this record is given by the actual
 * stateful functions used.
 * Each field is actually composed of a one dimensional vector so that values
 * can be changed with set-vec.
 *)

let pick_state r_env e state_lifespan =
  let state_var =
    match state_lifespan with
    | E.LocalState -> Lang.GroupState
    | E.GlobalState -> Lang.GlobalState in
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

let set_state d_env state_rec e d =
  let fname = field_name_of_state e in
  let state = get_field fname state_rec in
  let open DE.Ops in
  let d =
    if DT.is_nullable (DE.type_of d_env (get_vec (u8_of_int 0) state)) then
      DC.ensure_nullable ~l:d_env d
    else d in
  set_vec (u8_of_int 0) state d

let finalize_sf1 ~d_env aggr state =
  match aggr with
  | E.AggrMax | AggrMin | AggrFirst | AggrLast ->
      get_item 1 state
  | AggrSum ->
      state
      (* TODO: finalization for floats with Kahan sum *)
  | AggrAvg ->
      let count = get_item 0 state
      and ksum = get_item 1 state in
      div (DS.Kahan.finalize ~l:d_env ksum)
          (DC.conv ~to_:(Base Float) d_env count)
  | AggrAnd | AggrOr | AggrBitAnd | AggrBitOr | AggrBitXor | Group |
    Count ->
      (* The state is the final value: *)
      state
  | Distinct ->
      let b = get_item 1 state in
      get_vec (u8_of_int 0) b
  | _ ->
      todo "finalize_sf1"

(* Comparison function for heaps of pairs ordered by the second item: *)
let cmp ?(inv=false) item_t =
  DE.func2 item_t item_t (fun _l i1 i2 ->
    (* Should dessser have a compare function? *)
    if_ ((if inv then gt else lt) (get_item 1 i1) (get_item 1 i2))
      ~then_:(i8_of_int ~-1)
      ~else_:(
        if_ ((if inv then lt else gt) (get_item 1 i1) (get_item 1 i2))
          ~then_:(i8_of_int 1)
          ~else_:(i8_of_int 0)))

let lst_item_type e =
  match e.E.typ.DT.vtyp with
  | DT.Lst mn -> mn
  | _ ->
      !logger.error "Not a list?: %a" DT.print_value e.E.typ.DT.vtyp ;
      assert false (* Because of RamenTyping.ml *)

let past_item_t v_t =
  DT.(required (
    Tup [| v_t ; DT.{ vtyp = Base Float ; nullable = false } |]))

let get_binding ~r_env k =
  try List.assoc k r_env
  with Not_found ->
      Printf.sprintf2
        "Cannot find binding for %a in the environment (%a)"
        E.print_binding_key k
        print_r_env r_env |>
      failwith

let print_env oc d_env =
  pretty_list_print (fun oc (e, t) ->
    Printf.fprintf oc "%a:%a"
      (DE.print ~max_depth:3) e
      DT.print t
  ) oc d_env

(* This function returns the initial value of the state required to implement
 * the passed RaQL operator (which also provides its type).
 * This expression must not refer to the incoming data, as state can be
 * initialized before any data arrived (thus the value will not be found in
 * the environment) or just to figure out the required state type. *)
let rec init_state ?depth ~r_env ~d_env e =
  let open DE.Ops in
  let depth = Option.map succ depth in
  let expr ~d_env =
    expression ?depth ~r_env ~d_env in
  match e.E.text with
  | Stateful (_, _, SF1 ((AggrMin | AggrMax | AggrFirst | AggrLast), _)) ->
      (* A bool to tell if there ever was a value, and the selected value *)
      make_tup [ bool false ; null e.typ.DT.vtyp ]
  | Stateful (_, _, SF1 (AggrSum, _)) ->
      u8_of_int 0 |>
      DC.conv_maybe_nullable ~to_:e.E.typ d_env
      (* TODO: initialization for floats with Kahan sum *)
  | Stateful (_, _, SF1 (AggrAvg, _)) ->
      (* The state of the avg is composed of the count and the (Kahan) sum: *)
      make_tup [ u32_of_int 0 ; DS.Kahan.init ]
  | Stateful (_, _, SF1 (AggrAnd, _)) ->
      bool true
  | Stateful (_, _, SF1 (AggrOr, _)) ->
      bool false
  | Stateful (_, _, SF1 ((AggrBitAnd | AggrBitOr | AggrBitXor), _)) ->
      u8_of_int 0 |>
      DC.conv_maybe_nullable ~to_:e.E.typ d_env
  | Stateful (_, _, SF1 (Group, _)) ->
      (* Groups are typed as lists not sets: *)
      let item_t =
        match e.E.typ.DT.vtyp with
        | DT.Lst mn -> mn
        | _ -> invalid_arg ("init_state: "^ E.to_string e) in
      empty_set item_t
  | Stateful (_, _, SF1 (Count, _)) ->
      u8_of_int 0 |>
      DC.conv_maybe_nullable ~to_:e.E.typ d_env
  | Stateful (_, _, SF1 (Distinct, e)) ->
      (* Distinct result is a boolean telling if the last met value was already
       * present in the hash, so we also need to store that bool in the state
       * unfortunately. Since the hash_table is already mutable, let's also make
       * that boolean mutable: *)
      make_tup
        [ hash_table e.E.typ (u8_of_int 100) ;
          make_vec [ bool false ] ]
  | Stateful (_, _, SF2 (Lag, steps, e)) ->
      (* The state is just going to be a list of past values initialized with
       * NULLs (the value when we have so far received less than that number of
       * steps) and the index of the oldest value. *)
      let item_vtyp = e.E.typ.DT.vtyp in
      let steps = expr ~d_env steps in
      make_rec
        [ "past_values",
            (* We need one more item to remember the oldest value before it's
             * updated: *)
            (let len = add (u32_of_int 1) (to_u32 steps)
            and init = null item_vtyp in
            alloc_lst ~len ~init) ;
          "oldest_index", ref_ (u32_of_int 0) ]
  | Stateful (_, _, SF2 (ExpSmooth, _, _)) ->
      null e.E.typ.DT.vtyp
  | Stateful (_, skip_nulls, SF2 (Sample, n, e)) ->
      let n = expr ~d_env n in
      let item_t =
        if skip_nulls then
          T.{ e.E.typ with nullable = false }
        else
          e.E.typ in
      sampling item_t n
  | Stateful (_, _, SF3 (MovingAvg, p, k, x)) when E.is_one p ->
      let one = DC.conv ~to_:k.E.typ.DT.vtyp d_env (u8_of_int 1) in
      let k = expr ~d_env k in
      let len = add k one in
      let init = DE.default_value ~allow_null:true x.E.typ in
      make_rec
        [ "values", alloc_lst ~len ~init ;
          "count", ref_ (u32_of_int 0) ]
  | Stateful (_, _, SF3 (Hysteresis, _, _, _)) ->
      (* The value is supposed to be originally within bounds: *)
      let init = bool true in
      if e.E.typ.DT.nullable then not_null init else init
  (* Remember is implemented completely as external functions for init, update
   * and finalize (using CodeGenLib.Remember).
   * TOP should probably be external too: *)
  | Stateful (_, _, SF4 (Remember, fpr, _, dur, _)) ->
      let frp = to_float (expr ~d_env fpr)
      and dur = to_float (expr ~d_env dur) in
      apply (ext_identifier "CodeGenLib.Remember.init") [ frp ; dur ]
  | Stateful (_, _, SF4s (Largest { inv ; _ }, _, _, _, by)) ->
      let v_t = lst_item_type e in
      let by_t =
        DT.required (
          if by = [] then
            (* [update_state] will then use the count field: *)
            Base U32
          else
            (Tup (List.enum by /@ (fun e -> e.E.typ) |> Array.of_enum))) in
      let item_t = DT.tuple [| v_t ; by_t |] in
      let cmp = cmp ~inv (DT.Data item_t) in
      make_rec
        [ (* Store each values and its weight in a heap: *)
          "values", heap cmp ;
          (* Count insertions, to serve as a default order: *)
          "count", ref_ (u32_of_int 0) ]
  | Stateful (_, _, Past { max_age ; sample_size ; _ }) ->
      if sample_size <> None then
        todo "PAST operator with integrated sampling" ;
      let v_t = lst_item_type e in
      let item_t = past_item_t v_t in
      let cmp = cmp (DT.Data item_t) in
      make_rec
        [ "values", heap cmp ;
          "max_age", to_float (expr ~d_env max_age) ;
          (* If tumbled is true, finalizer should then empty the values: *)
          "tumbled", ref_ (DE.Ops.null DT.(Set (Heap, item_t))) ;
          (* TODO: sampling *) ]
  | Stateful (_, skip_nulls, Top { size ; max_size ; sigmas ; what ; _ }) ->
      (* Dessser TOP set uses a special [insert_weighted] operator to insert
       * values with a weight. It has no notion of time and decay so decay will
       * be implemented when updating the state by inflating the weights with
       * time. It is therefore necessary to store the starting time in the
       * state in addition to the top itself. *)
      let size_t = size.E.typ.DT.vtyp in
      let item_t =
        if skip_nulls then DT.(required what.E.typ.DT.vtyp)
        else what.E.typ in
      let size = expr ~d_env size in
      let max_size =
        match max_size with
        | Some max_size ->
            expr ~d_env max_size
        | None ->
            let ten = DC.conv ~to_:size_t d_env (u8_of_int 10) in
            mul ten size in
      let sigmas = expr ~d_env sigmas in
      make_rec
        [ "starting_time", ref_ (null T.(Base Float)) ;
          "top", top item_t (to_u32 size) (to_u32 max_size) sigmas ]
  | _ ->
      (* TODO *)
      todo ("init_state of "^ E.to_string ~max_depth:1 e)

and get_field_binding ~r_env ~d_env var field =
  (* Try first to see if there is this specific binding in the environment. *)
  let k = E.RecordField (var, field) in
  try List.assoc k r_env with
  | Not_found ->
      (* If not, that means this field has not been overridden but we may
       * still find the record it's from and pretend we have a Get from
       * the Variable instead: *)
      let binding = get_binding ~r_env (E.RecordValue var) in
      apply_1 d_env binding (fun _d_env binding ->
        get_field (field :> string) binding)

(* Returns the type of the state record needed to store the states of all the
 * given stateful expressions: *)
and state_rec_type_of_expressions ~r_env ~d_env es =
  let mns =
    List.map (fun e ->
      let d = init_state ~r_env ~d_env e in
      !logger.debug "init state of %a: %a"
        (E.print false) e
        (DE.print ?max_depth:None) d ;
      let mn = DT.mn_of_t (DE.type_of d_env d) in
      field_name_of_state e,
      (* The value is a 1 dimensional (mutable) vector *)
      DT.(required (Vec (1, mn)))
    ) es |>
    Array.of_list in
  if mns = [||] then DT.(required (Base Unit))
                else DT.(required (Rec mns))

(* Implement an SF1 aggregate function, assuming skip_nulls is handled by the
 * caller (necessary since the item and state are already evaluated).
 * NULL item will propagate to the state.
 * Used for normal state updates as well as aggregation over lists: *)
and update_state_sf1 ~d_env ~convert_in aggr item state =
  let open DE.Ops in
  (* if [d] is nullable and null, then returns it, else apply [f] to (forced,
   * if nullable) value of [d] and return not_null (if nullable) of that
   * instead. This propagates [d]'s nullability to the result of the
   * aggregation. *)
  let null_map ~d_env d f =
    let_ ~name:"null_map" ~l:d_env d (fun d_env d ->
      match DE.type_of d_env d with
      | DT.Data { nullable = true ; _ } ->
          if_null d
            ~then_:d
            ~else_:(
              DC.ensure_nullable ~l:d_env (f d_env (force ~what:"null_map" d)))
      | _ ->
          f d_env d) in
  match aggr with
  | E.AggrMax | AggrMin | AggrFirst | AggrLast ->
      let d_op =
        match aggr, DT.mn_of_t (DE.type_of d_env item) with
        (* As a special case, RaQL allows boolean arguments to min/max: *)
        | AggrMin, DT.{ vtyp = Base Bool ; _ } ->
            and_
        | AggrMax, DT.{ vtyp = Base Bool ; _ } ->
            or_
        | AggrMin, _ ->
            min_
        | AggrMax, _ ->
            max_
        | AggrFirst, _ ->
            (fun s _d -> s)
        | _ ->
            assert (aggr = AggrLast) ;
            (fun _s d -> d) in
      let new_state_val =
        (* In any case, if we never got a value then we select this one and
         * call it a day: *)
        if_ (not_ (get_item 0 state))
          ~then_:(DC.ensure_nullable ~l:d_env item)
          ~else_:(
            apply_2 ~convert_in d_env (get_item 1 state) item
                    (fun _d_env -> d_op)) in
      make_tup [ bool true ; new_state_val ]
  | AggrSum ->
      (* Typing can decide the state and/or item are nullable.
       * In any case, nulls must propagate: *)
      apply_2 ~convert_in d_env state item (fun _d_env -> add)
      (* TODO: update for float with Kahan sum *)
  | AggrAvg ->
      let count = get_item 0 state
      and ksum = get_item 1 state in
      null_map ~d_env item (fun d_env d ->
        make_tup [ add count (u32_of_int 1) ;
                   DS.Kahan.add ~l:d_env ksum d])
  | AggrAnd ->
      apply_2 d_env state item (fun _d_env -> and_)
  | AggrOr ->
      apply_2 d_env state item (fun _d_env -> or_)
  | AggrBitAnd ->
      apply_2 ~convert_in d_env state item (fun _d_env -> bit_and)
  | AggrBitOr ->
      apply_2 ~convert_in d_env state item (fun _d_env -> bit_or)
  | AggrBitXor ->
      apply_2 ~convert_in d_env state item (fun _d_env -> bit_xor)
  | Group ->
      insert state item
  | Count ->
      let one d_env =
        DC.conv ~to_:convert_in d_env (u8_of_int 1) in
      (match DE.type_of d_env item with
      | DT.(Data { vtyp = Base Bool ; _ }) ->
          (* Count how many are true *)
          apply_2 d_env state item (fun d_env state item ->
            if_ item
              ~then_:(add state (one d_env))
              ~else_:state)
      | _ ->
          (* Just count.
           * In previous versions it used to be that count would never be
           * nullable in this case, even when counting nullable items and not
           * skipping nulls, since we can still count unknown values.
           * But having special cases as this in NULL propagation makes this
           * code harder and is also harder for the user who would try to
           * memorise the rules. Better keep it simple and let
           *   COUNT KEEP NULLS NULL
           * be NULL. It is simple enough to write instead:
           *   COUNT (X IS NOT NULL)
           * FIXME: Update typing accordingly. *)
          apply_1 d_env state (fun d_env state -> add state (one d_env)))
  | Distinct ->
      let h = get_item 0 state
      and b = get_item 1 state in
      if_ (member item h)
        ~then_:(set_vec (u8_of_int 0) b (bool false))
        ~else_:(
          seq [
            insert h item ;
            set_vec (u8_of_int 0) b (bool true) ])
  | _ ->
      todo "update_state_sf1"

(* Implement an SF1 aggregate function, assuming skip_nulls is handled by the
 * caller (necessary since the item and state are already evaluated).
 * NULL item will propagate to the state.
 * Used for normal state updates as well as aggregation over lists: *)
and update_state_sf2 ~d_env ~convert_in aggr item1 item2 state =
  ignore convert_in ;
  let open DE.Ops in
  match aggr with
  | E.Lag ->
      let past_vals = get_field "past_values" state
      and oldest_index = get_field "oldest_index" state in
      let item2 =
        if DT.is_nullable (DE.type_of d_env item2) then item2
                                                   else not_null item2 in
      seq [
        set_vec (get_ref oldest_index) past_vals item2 ;
        let next_oldest = add (get_ref oldest_index) (u32_of_int 1) in
        let_ ~name:"next_oldest" ~l:d_env next_oldest (fun _d_env next_oldest ->
          let next_oldest =
            (* [gt] because we had item1+1 items in past_vals: *)
            if_ (gt next_oldest (to_u32 item1))
              ~then_:(u32_of_int 0)
              ~else_:next_oldest in
          set_ref oldest_index next_oldest) ]
  | ExpSmooth ->
      if_null state
        ~then_:item2
        ~else_:(
          add (mul item2 item1)
              (mul (force ~what:"ExpSmooth" state)
                   (sub (float 1.) (to_float item1)))) |>
      not_null
  | Sample ->
      insert state item2
  | _ ->
      todo "update_state_sf2"

and update_state_sf3 ~d_env ~convert_in aggr item1 item2 item3 state =
  ignore convert_in ;
  ignore item1 ;
  ignore item2 ;
  let open DE.Ops in
  match aggr with
  | E.MovingAvg ->
      let_ ~name:"values" ~l:d_env (get_field "values" state) (fun d_env values ->
        let_ ~name:"count" ~l:d_env (get_field "count" state) (fun _d_env count ->
          let x = to_float item3 in
          let idx = force (rem (get_ref count) (cardinality values)) in
          seq [
            set_vec idx values x ;
            set_ref count (add (get_ref count) (u32_of_int 1)) ]))
  | Hysteresis ->
      let to_ =
        [ item1 ; item2 ; item3 ] |>
        List.map (fun d -> (DT.mn_of_t (DE.type_of d_env d)).DT.vtyp) |>
        T.largest_type in
      let_ ~name:"x" ~l:d_env (DC.conv ~to_ d_env item1) (fun d_env x ->
        let_ ~name:"acceptable" ~l:d_env (DC.conv ~to_ d_env item2)
            (fun d_env acceptable ->
          let_ ~name:"maximum" ~l:d_env (DC.conv ~to_ d_env item3)
              (fun d_env maximum ->
            (* Above the maximum the state must become false,
             * below the acceptable the stage must become true,
             * and it must stays the same, including NULL, in between: *)
            let same_nullable b =
              if DT.is_nullable (DE.type_of d_env state) then
                DC.ensure_nullable ~l:d_env (bool b)
              else
                bool b in
            if_ (ge maximum acceptable)
              ~then_:(
                if_ (ge x maximum)
                  ~then_:(same_nullable false)
                  ~else_:(
                    if_ (le x acceptable)
                      ~then_:(same_nullable true)
                      ~else_:state))
              ~else_:(
                if_ (le x maximum)
                  ~then_:(same_nullable false)
                  ~else_:(
                    if_ (ge x acceptable)
                      ~then_:(same_nullable true)
                      ~else_:state)))))
  | _ ->
      todo "update_state_sf3"

and update_state_sf4 ~d_env ~convert_in aggr item1 item2 item3 item4 state =
  ignore d_env ;
  ignore item1 ;
  ignore item3 ;
  ignore convert_in ;
  match aggr with
  | E.Remember ->
      let time = to_float item2
      and e = item4 in
      (* apply (ext_identifier "CodeGenLib.Remember.add")
            [ state ; time ; to_void (make_tup es) ] *)
      verbatim
        [ DT.OCaml, "CodeGenLib.Remember.add %1 %2 %3" ]
        DT.(Data (required (ext "remember_state")))
        [ state ; time ; e ]
  | _ ->
      todo "update_state_sf4"

and update_state_sf4s ~d_env ~convert_in aggr item1 item2 item3 item4s state =
  ignore item2 ;
  ignore convert_in ;
  match aggr with
  | E.Largest _ ->
      let max_len = to_u32 item1
      and e = item3
      and by = item4s in
      let_ ~name:"values" ~l:d_env (get_field "values" state) (fun d_env values ->
        let_ ~name:"count" ~l:d_env (get_field "count" state) (fun d_env count ->
          let by =
            (* Special updater that use the internal count when no `by` expressions
             * are present: *)
            if by = [] then [ get_ref count ] else by in
          let by = make_tup by in
          let heap_item = make_tup [ e ; by ] in
          seq [
            set_ref count (add (get_ref count) (u32_of_int 1)) ;
            insert values heap_item ;
            let_ ~name:"heap_len" ~l:d_env (cardinality values)
              (fun _d_env heap_len ->
                if_ (gt heap_len max_len)
                  ~then_:(del_min values (u32_of_int 1))
                  ~else_:nop) ]))
  | _ ->
      todo "update_state_sf4s"

and update_state_past ~d_env ~convert_in tumbling what time state v_t =
  ignore convert_in ;
  let open DE.Ops in
  let values = get_field "values" state
  and max_age = get_field "max_age" state
  and tumbled = get_field "tumbled" state in
  let item_t =
    DT.required (Tup [| v_t ; DT.{ vtyp = Base Float ; nullable = false } |]) in
  let expell_the_olds =
    if_ (gt (cardinality values) (u32_of_int 0))
      ~then_:(
        let min_time = get_item 1 (get_min values) in
        if tumbling then (
          let_ ~name:"min_time" ~l:d_env min_time (fun _d_env min_time ->
            (* Tumbling window: empty (and save) the whole data set when time
             * is due *)
            set_ref tumbled (
              if_ (eq (to_i32 (div time max_age))
                      (to_i32 (div min_time max_age)))
                ~then_:(null DT.(Set (Heap, item_t)))
                ~else_:(not_null values)))
        ) else (
          (* Sliding window: remove any value older than max_age *)
          loop_while
            ~cond:(
              DE.func1 DT.Void (fun _d_env _unit ->
                lt (sub time min_time) max_age))
            ~body:(
              DE.func1 DT.Void (fun _d_env _unit ->
                del_min values (u32_of_int 1)))
            ~init:nop
        ))
      ~else_:nop |>
    comment "Expelling old values" in
  seq [
    expell_the_olds ;
    insert values (make_tup [ what ; time ]) ]

and update_state_top ~d_env ~convert_in what by decay time state =
  ignore convert_in ;
  (* Those two can be any numeric: *)
  let time = to_float time
  and by = to_float by in
  let open DE.Ops in
  let starting_time = get_field "starting_time" state
  and top = get_field "top" state in
  let inflation =
    let_ ~name:"starting_time" ~l:d_env (get_ref starting_time) (fun d_env t0_opt ->
      if_null t0_opt
        ~then_:(
          seq [
            set_ref starting_time (not_null time) ;
            float 1. ])
        ~else_:(
          let infl = exp_ (mul decay (sub time (force ~what:"inflation" t0_opt))) in
          let_ ~name:"top_infl" ~l:d_env infl (fun _d_env infl ->
            let max_infl = float 1e6 in
            if_ (lt infl max_infl)
              ~then_:infl
              ~else_:(
                seq [
                  scale_weights top (force ~what:"inflation(2)"
                                      (div (float 1.) infl)) ;
                  set_ref starting_time (not_null time) ;
                  float 1. ])))) in
  let_ ~name:"top_inflation" ~l:d_env inflation (fun d_env inflation ->
    let weight = mul by inflation in
    let_ ~name:"weight" ~l:d_env weight (fun _d_env weight ->
      insert_weighted top weight what))

(* Environments:
 * - [d_env] is the environment used by dessser, ie. a stack of
 *   expression x type (expression being for instance [(identifier n)] or
 *   [(param n m)]. This is used by dessser to do type-checking.
 *   The environment must contain the required external identifiers set by
 *   the [init] function.
 * - [r_env] is the stack of currently reachable "raql thing", such
 *   as expression state, a record (in other words an E.binding_key), bound
 *   to a dessser expression (typically an identifier or a param). *)
and expression ?(depth=0) ~r_env ~d_env e =
  !logger.debug "%sCompiling into DIL: %a"
    (indent_of depth)
    (E.print true) e ;
  assert (E.is_typed e) ;
  let apply_1 = apply_1 ~depth
  and apply_2 = apply_2 ~depth in
  let depth = depth + 1 in
  let expr ~d_env =
    expression ~depth ~r_env ~d_env in
  let bad_type () =
    Printf.sprintf2 "Invalid type %a for expression %a"
      DT.print_maybe_nullable e.E.typ
      (E.print false) e |>
    failwith in
  let convert_in = e.E.typ.DT.vtyp in
  let conv = DC.conv ~depth
  and conv_maybe_nullable = DC.conv_maybe_nullable ~depth in
  let conv_from d_env d =
    conv ~to_:e.E.typ.DT.vtyp d_env d in
  let conv_maybe_nullable_from d_env d =
    conv_maybe_nullable ~to_:e.E.typ d_env d in
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
              conv_maybe_nullable ~to_:mns.(i) d_env (expr ~d_env e)
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
              conv_maybe_nullable ~to_:(snd mns.(i)) d_env (expr ~d_env e)
            ) nes |>
            make_rec
        | _ ->
            bad_type ())
    | Vector es ->
        (match e.E.typ.DT.vtyp with
        | DT.Vec (dim, mn) ->
            if dim <> List.length es then bad_type () ;
            List.map (fun e ->
              conv_maybe_nullable ~to_:mn d_env (expr ~d_env e)
            ) es |>
            make_vec
        | _ ->
            bad_type ())
    | Variable var ->
        get_binding ~r_env (E.RecordValue var)
    | Binding (E.RecordField (var, field)) ->
        get_field_binding ~r_env ~d_env var field
    | Binding k ->
        (* A reference to the raql environment. Look for the dessser expression it
         * translates to. *)
        get_binding ~r_env k
    | Case (alts, else_) ->
        let rec alt_loop = function
          | [] ->
              (match else_ with
              | Some e -> conv_maybe_nullable_from d_env (expr ~d_env e)
              | None -> null e.E.typ.DT.vtyp)
          | E.{ case_cond = cond ; case_cons = cons } :: alts' ->
              let do_cond d_env cond =
                if_ cond
                    ~then_:(conv_maybe_nullable ~to_:e.E.typ d_env (expr ~d_env cons))
                    ~else_:(alt_loop alts') in
              if cond.E.typ.DT.nullable then
                let_ ~name:"nullable_cond_" ~l:d_env (expr ~d_env cond)
                  (fun d_env cond ->
                    if_null cond
                        ~then_:(null e.E.typ.DT.vtyp)
                        ~else_:(do_cond d_env (force ~what:"Case" cond)))
              else
                do_cond d_env (expr ~d_env cond) in
        alt_loop alts
    | Stateless (SL0 Now) ->
        conv_from d_env now
    | Stateless (SL0 Random) ->
        random_float
    | Stateless (SL0 Pi) ->
        float Float.pi
    | Stateless (SL0 EventStart) ->
        (* No support for event-time expressions, just convert into the start/stop
         * fields: *)
        get_field_binding ~r_env ~d_env Out (N.field "start")
    | Stateless (SL0 EventStop) ->
        get_field_binding ~r_env ~d_env Out (N.field "stop")
    | Stateless (SL1 (Age, e1)) ->
        apply_1 ~convert_in d_env (expr ~d_env e1) (fun _l d -> sub now d)
    | Stateless (SL1 (Cast _, e1)) ->
        (* Type checking already set the output type of that Raql expression to the
         * target type, and the result will be converted into this type in any
         * case. *)
        expr ~d_env e1
    | Stateless (SL1 (Force, e1)) ->
        force ~what:"explicit Force" (expr ~d_env e1)
    | Stateless (SL1 (Peek (vtyp, endianness), e1)) when E.is_a_string e1 ->
        (* vtyp is some integer. *)
        apply_1 d_env (expr ~d_env e1) (fun _d_env d1 ->
          let ptr = data_ptr_of_string d1 in
          let offs = size 0 in
          match vtyp with
          | DT.Base U128 -> u128_of_oword (peek_oword endianness ptr offs)
          | DT.Base U64 -> u64_of_qword (peek_qword endianness ptr offs)
          | DT.Base U32 -> u32_of_dword (peek_dword endianness ptr offs)
          | DT.Base U16 -> u16_of_word (peek_word endianness ptr offs)
          | DT.Base U8 -> u8_of_byte (peek_byte ptr offs)
          | DT.Base I128 -> to_i128 (u128_of_oword (peek_oword endianness ptr offs))
          | DT.Base I64 -> to_i64 (u64_of_qword (peek_qword endianness ptr offs))
          | DT.Base I32 -> to_i32 (u32_of_dword (peek_dword endianness ptr offs))
          | DT.Base I16 -> to_i16 (u16_of_word (peek_word endianness ptr offs))
          | DT.Base I8 -> to_i8 (peek_byte ptr offs)
          (* Other widths TODO. We might not have enough bytes to read as
           * many bytes than the larger integer type. *)
          | _ ->
              Printf.sprintf2 "Peek %a" DT.print_value vtyp |>
              todo)
    | Stateless (SL1 (Length, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _d_env d1 ->
          match e1.E.typ.DT.vtyp with
          | DT.Base String -> string_length d1
          | DT.Lst _ -> cardinality d1
          | _ -> bad_type ()
        )
    | Stateless (SL1 (Lower, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> lower d)
    | Stateless (SL1 (Upper, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> upper d)
    | Stateless (SL1 (UuidOfU128, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d ->
          apply (ext_identifier "CodeGenLib.uuid_of_u128") [ d ])
    | Stateless (SL1 (Not, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> not_ d)
    | Stateless (SL1 (Abs, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> abs d)
    | Stateless (SL1 (Minus, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> neg d)
    | Stateless (SL1 (Defined, e1)) ->
        not_ (is_null (expr ~d_env e1))
    | Stateless (SL1 (Exp, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> exp_ (to_float d))
    | Stateless (SL1 (Log, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> log_ (to_float d))
    | Stateless (SL1 (Log10, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> log10_ (to_float d))
    | Stateless (SL1 (Sqrt, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> sqrt_ (to_float d))
    | Stateless (SL1 (Sq, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> mul d d)
    | Stateless (SL1 (Ceil, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> ceil_ (to_float d))
    | Stateless (SL1 (Floor, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> floor_ (to_float d))
    | Stateless (SL1 (Round, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> round (to_float d))
    | Stateless (SL1 (Cos, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> cos_ (to_float d))
    | Stateless (SL1 (Sin, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> sin_ (to_float d))
    | Stateless (SL1 (Tan, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> tan_ (to_float d))
    | Stateless (SL1 (ACos, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> acos_ (to_float d))
    | Stateless (SL1 (ASin, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> asin_ (to_float d))
    | Stateless (SL1 (ATan, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> atan_ (to_float d))
    | Stateless (SL1 (CosH, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> cosh_ (to_float d))
    | Stateless (SL1 (SinH, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> sinh_ (to_float d))
    | Stateless (SL1 (TanH, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> tanh_ (to_float d))
    | Stateless (SL1 (Hash, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun _l d -> hash d)
    | Stateless (SL1 (Chr, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun d_env d1 ->
          char_of_u8 (conv ~to_:DT.(Base U8) d_env d1)
        )
    | Stateless (SL1 (Fit, e1)) ->
      (* [e1] is supposed to be a list/vector of tuples of scalars, or a
       * list/vector of scalars. In the first case, the first value of the
       * tuple is the value to be predicted and the others are predictors;
       * whereas in the second case the predictor will defaults to a
       * sequence.
       * All items of those tuples are supposed to be numeric, so we convert
       * all of them into floats and then proceed with the regression.  *)
      apply_1 d_env (expr ~d_env e1) (fun d_env d1 ->
        let has_predictor =
          match e1.E.typ.DT.vtyp with
          | Lst { vtyp = Tup _ ; _ }
          | Vec (_, { vtyp = Tup _ ; _ }) -> true
          | _ -> false in
        if has_predictor then
          (* Convert the argument into a list of nullable lists of
           * non-nullable floats (do not use vector since it would not be
           * possible to type [CodeGenLib.LinReq.fit] for all possible
           * dimensions): *)
          let to_ = DT.(Lst (optional (Lst (required (Base Float))))) in
          let d1 = conv ~to_ d_env d1 in
          apply (ext_identifier "CodeGenLib.LinReg.fit") [ d1 ]
        else
          (* In case we have only the predicted value in a single dimensional
           * array, call a dedicated function: *)
          let to_ = DT.(Lst (optional (Base Float))) in
          let d1 = conv ~to_ d_env d1 in
          apply (ext_identifier "CodeGenLib.LinReg.fit_simple") [ d1 ])
    | Stateless (SL1 (Basename, e1)) ->
        apply_1 d_env (expr ~d_env e1) (fun d_env d1 ->
          let_ ~name:"str_" ~l:d_env d1 (fun d_env str ->
            let pos = find_substring (bool false) (string "/") str in
            let_ ~name:"pos_" ~l:d_env pos (fun _d_env pos ->
              if_null pos
                ~then_:str
                ~else_:(split_at (add (u24_of_int 1)
                                      (force ~what:"Basename" pos)) str |>
                        get_item 1)))
        )
    | Stateless (SL1s ((Max | Min as op), es)) ->
        let d_op = match op with Max -> max | _ -> min in
        (match es with
        | [] ->
            assert false
        | [ e1 ] ->
            apply_1 d_env (expr ~d_env e1) (fun d_env d -> conv_from d_env d)
        | e1 :: es' ->
            apply_1 d_env (expr ~d_env e1) (fun d_env d1 ->
              let rest = { e with text = Stateless (SL1s (op, es')) } in
              apply_1 d_env (expr ~d_env rest) (fun d_env d2 ->
                d_op (conv_from d_env d1) d2)))
    | Stateless (SL1s (Print, es)) ->
        let to_string d_env d =
          let nullable =
            match DE.type_of d_env d with
            | DT.Data { nullable ; _ } -> nullable
            | _ -> false in
          if nullable then
            if_null d
              ~then_:(string "<NULL>")
              ~else_:(
                conv ~to_:DT.(Base String) d_env (force ~what:"Print" d))
          else
            conv ~to_:DT.(Base String) d_env d in
        (match List.rev es with
        | e1 :: es ->
            let_ ~name:"sep" ~l:d_env (string "; ") (fun d_env sep ->
              let_ ~name:"last_printed" ~l:d_env (expr ~d_env e1) (fun d_env d1 ->
                let dumps =
                  List.fold_left (fun lst e ->
                    let lst =
                      if lst = [] then lst else dump sep :: lst in
                    dump (to_string d_env (expr ~d_env e)) :: lst
                  ) [] es in
                seq (
                  [ dump (string "PRINT: [ ") ] @
                  dumps @
                  (if dumps = [] then [] else [ dump sep ]) @
                  [ dump (to_string d_env d1) ;
                    dump (string " ]\n") ;
                    d1 ])))
        | [] ->
            invalid_arg "RaQL2DIL.expression: empty PRINT")
    | Stateless (SL1s (Coalesce, es)) ->
        let es =
          List.map (fun e1 ->
            (* Convert to the result's vtyp: *)
            let to_ = DT.{ e.E.typ with nullable = e1.E.typ.DT.nullable } in
            conv_maybe_nullable ~to_ d_env (expr ~d_env e1)
          ) es in
        DessserStdLib.coalesce d_env es
    | Stateless (SL2 (Add, e1, e2)) ->
        apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> add)
    | Stateless (SL2 (Sub, e1, e2)) ->
        apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> sub)
    (* Multiplication of a string by an integer repeats the string: *)
    | Stateless (SL2 (Mul, (E.{ typ = DT.{ vtyp = Base String ; _ } ; _ } as s), n))
    | Stateless (SL2 (Mul, n, (E.{ typ = DT.{ vtyp = Base String ; _ } ; _ } as s))) ->
        apply_2 d_env (expr ~d_env s) (expr ~d_env n) (fun d_env s n ->
          repeat
            ~from:(i32_of_int 0)
            ~to_:(to_i32 n)
            ~init:(string "")
            ~body:(
              DE.func2 ~l:d_env DT.i32 DT.string (fun _d_env _n ss ->
                append_string ss s)))
    | Stateless (SL2 (Mul, e1, e2)) ->
        apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> mul)
    | Stateless (SL2 (Div, e1, e2)) ->
        apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2) (fun _d_env -> div)
    | Stateless (SL2 (IDiv, e1, e2)) ->
        (* When the result is a float we need to floor it *)
        (match e.E.typ with
        | DT.{ vtyp = Base Float ; _ } ->
            apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2)
                    (fun d_env d1 d2 ->
              apply_1 d_env (div d1 d2) (fun _d_env d -> floor_ d))
        | _ ->
            apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2)
                    (fun _d_env -> div))
    | Stateless (SL2 (Mod, e1, e2)) ->
        apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> rem)
    | Stateless (SL2 (Pow, e1, e2)) ->
        apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> pow)
    | Stateless (SL2 (Trunc, e1, e2)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun d_env d1 d2 ->
          (* Before dividing, convert d1 and d2 into the largest type: *)
          let to_ = T.largest_type [ e1.E.typ.DT.vtyp ; e2.typ.vtyp ] in
          let d1 = conv ~to_ d_env d1
          and d2 = conv ~to_ d_env d2
          and zero = conv ~to_ d_env (u8_of_int 0) in
          match e.E.typ.DT.vtyp with
          | Base Float ->
              apply_1 d_env (div d1 d2) (fun _d_env d -> mul d2 (floor_ d))
          | Base (U8|U16|U24|U32|U40|U48|U56|U64|U128) ->
              apply_1 d_env (div d1 d2) (fun _d_env d -> mul d2 d)
          | Base (I8|I16|I24|I32|I40|I48|I56|I64|I128) ->
              apply_1 d_env (div d1 d2) (fun d_env d ->
                let_ ~name:"truncated" ~l:d_env (mul d2 d) (fun _d_env r ->
                  if_ (ge d1 zero) ~then_:r ~else_:(sub r d2)))
          | t ->
              Printf.sprintf2 "expression: Invalid type for Trunc: %a"
                DT.print_value t |>
              invalid_arg)
    | Stateless (SL2 (Reldiff, e1, e2)) ->
        apply_2 ~convert_in d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun d_env d1 d2 ->
          let scale = max_ (abs d1) (abs d2) in
          let_ ~name:"scale" ~l:d_env scale (fun _d_env scale ->
            let diff = abs (sub d1 d2) in
            if_ (eq scale (float 0.))
              ~then_:(float 0.)
              ~else_:(force (div diff scale))))
    | Stateless (SL2 (And, e1, e2)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2) (fun _d_env -> and_)
    | Stateless (SL2 (Or, e1, e2)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2) (fun _d_env -> or_)
    | Stateless (SL2 (Ge, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> ge)
    | Stateless (SL2 (Gt, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> gt)
    | Stateless (SL2 (Eq, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> eq)
    | Stateless (SL2 (Concat, e1, e2)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env d1 d2 -> join (string "") (make_vec [ d1 ; d2 ]))
    | Stateless (SL2 (StartsWith, e1, e2)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> starts_with)
    | Stateless (SL2 (EndsWith, e1, e2)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> ends_with)
    | Stateless (SL2 (BitAnd, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> bit_and)
    | Stateless (SL2 (BitOr, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> bit_or)
    | Stateless (SL2 (BitXor, e1, e2)) ->
        apply_2 ~enlarge_in:true d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env -> bit_xor)
    | Stateless (SL2 (BitShift, e1,
                      { text = Stateless (SL1 (Minus, e2)) ; _ })) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env d1 d2 -> right_shift d1 (to_u8 d2))
    | Stateless (SL2 (BitShift, e1, e2)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2)
                (fun _d_env d1 d2 -> left_shift d1 (to_u8 d2))
    (* Get a known field from a record: *)
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                           ({ typ = DT.{ vtyp = Rec _ ; _ } ; _ } as e2))) ->
        apply_1 d_env (expr ~d_env e2) (fun _l d -> get_field n d)
    (* Constant get from a vector: the nullability merely propagates, and the
     * program will crash if the constant index is outside the constant vector
     * bounds: *)
    | Stateless (SL2 (Get, ({ text = Const n ; _ } as e1),
                           ({ typ = DT.{ vtyp = Vec _ ; _ } ; _ } as e2)))
      when E.is_integer n ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2) (fun _l -> get_vec)
    (* Similarly, from a tuple: *)
    | Stateless (SL2 (Get, e1,
                           ({ typ = DT.{ vtyp = Tup _ ; _ } ; _ } as e2))) ->
      (match E.int_of_const e1 with
      | Some n ->
          apply_1 d_env (expr ~d_env e2) (fun _l -> get_item n)
      | None ->
          bad_type ())
    (* Get a value from a map: result is always nullable as the key might be
     * unbound at that time. *)
    | Stateless (SL2 (Get, key, ({ typ = DT.{ vtyp = Map (key_t, _) ; _ } ;
                                   _ } as map))) ->
        apply_2 d_env (expr ~d_env key) (expr ~d_env map) (fun d_env key map ->
          (* Confidently convert the key value into the declared type for keys,
           * although the actual implementation of map_get accepts only strings
           * (and the type-checker will also only accept a map which keys are
           * strings since integers are list/vector accessors.
           * FIXME: either really support other types for keys, and find a new
           * syntax to distinguish Get from lists than maps, _or_ forbid
           * declaring a map of another key type than string. Oh, and by the
           * way, did I mentioned that map_get will only return strings as
           * well? *)
          let key = conv ~to_:key_t.DT.vtyp d_env key in
          (* Note: the returned string is going to be converted into the actual
           * expected result type using normal conversion from string. Ideally
           * we'd like to use a more efficient serialization format for LMDB
           * values. TODO. *)
          apply (ext_identifier "CodeGenLib.Globals.map_get") [ map ; key ])
    (* In all other cases the result is always nullable, in case the index goes
     * beyond the bounds of the vector/list or is an unknown field: *)
    | Stateless (SL2 (Get, e1, e2)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env e2) (fun d_env d1 d2 ->
          let_ ~name:"get_from" ~l:d_env d2 (fun d_env d2 ->
            let conv1 = conv ~to_:e1.E.typ.DT.vtyp d_env in
            let zero = conv1 (i8_of_int 0) in
            if_ (and_ (ge d1 zero) (lt d1 (conv1 (cardinality d2))))
              ~then_:(conv_maybe_nullable_from d_env (get_vec d1 d2))
              ~else_:(null e.E.typ.DT.vtyp)))
    | Stateless (SL2 (In, e1, e2)) ->
        DS.is_in ~l:d_env (expr ~d_env e1) (expr ~d_env e2)
    | Stateless (SL2 (Strftime, fmt, time)) ->
        apply_2 d_env (expr ~d_env fmt) (expr ~d_env time)
                (fun _d_env fmt time ->
          strftime fmt (to_float time))
    | Stateless (SL2 (Index, str, chr)) ->
        apply_2 d_env (expr ~d_env str) (expr ~d_env chr) (fun d_env str chr ->
          match find_substring true_ (string_of_char chr) str with
          | E0 (Null _) ->
              i32_of_int ~-1
          | res ->
              let_ ~name:"index_" ~l:d_env res (fun d_env res ->
                if_null res
                  ~then_:(i32_of_int ~-1)
                  ~else_:(conv ~to_:DT.(Base I32) d_env
                               (force ~what:"Index" res))))
    | Stateless (SL3 (SubString, str, start, stop)) ->
        apply_3 d_env (expr ~d_env str) (expr ~d_env start) (expr ~d_env stop)
                (fun _d_env str start stop ->
          substring str start stop)
    | Stateless (SL3 (MapSet, map, k, v)) ->
        (match map.E.typ.DT.vtyp with
        (* Fetch the expected key and value type for the map type of m,
         * and convert the actual k and v into those types: *)
        | Map (key_t, val_t) ->
            apply_3 d_env (expr ~d_env map) (expr ~d_env k) (expr ~d_env v)
                    (fun _d_env map k v ->
              (* map_set takes only non-nullable keys and values, despite the
               * overall MapSet operator accepting nullable keys/values.
               * In theory, if we declare the map as accepting nullable keys
               * or values then we should really insert NULLs (as keys or
               * values) in the map. But we have to implement actual encoding
               * of any type into strings for that to work. Then, we would
               * serialize those values into strings and use map_set on those
               * strings. *)
              let k = conv ~to_:key_t.DT.vtyp d_env k
              and v = conv ~to_:val_t.DT.vtyp d_env v in
              apply (ext_identifier "CodeGenLib.Globals.map_set")
                    [ map ; k ; v ])
        | _ ->
            assert false (* Because of type checking *))
    | Stateless (SL2 (Percentile, e1, percs)) ->
        apply_2 d_env (expr ~d_env e1) (expr ~d_env percs) (fun d_env d1 percs ->
          match e.E.typ.DT.vtyp with
          | Vec _ ->
              DS.percentiles ~l:d_env d1 percs
          | _ ->
              DS.percentiles ~l:d_env d1 (make_vec [ percs ]) |>
              get_vec (u8_of_int 0))
    (*
     * Stateful functions:
     * When the argument is a list then those functions are actually stateless:
     *)
    (* FIXME: do not store a state for those in any state vector *)
    | Stateful (_, skip_nulls, SF1 (aggr, list))
      when E.is_a_list list ->
        let state = init_state ~r_env ~d_env e in
        let state_t = DE.type_of d_env state in
        let list_nullable, list_item_t =
          match list.E.typ with
          | DT.{ nullable ; vtyp = (Vec (_, t) | Lst t | Set (_, t)) } ->
              nullable, t
          | _ ->
              assert false (* Because 0f `E.is_a_list list` *) in
        let convert_in = e.E.typ.DT.vtyp in
        let do_fold list =
          fold
            ~init:state
            ~body:(
              DE.func2 ~l:d_env state_t (Data list_item_t) (fun d_env state item ->
                let update_state ~d_env item =
                  let new_state =
                    update_state_sf1 ~d_env ~convert_in aggr item state in
                  (* If update_state_sf1 returns void, pass the given state that's
                   * been mutated: *)
                  if DT.eq DT.Void (DE.type_of d_env new_state) then state
                  else new_state in
                if skip_nulls && DT.is_nullable (DE.type_of d_env item) then
                  if_null item
                    ~then_:state
                    ~else_:(
                      update_state ~d_env (force ~what:"do_fold" item))
                else
                  update_state ~d_env item))
            ~list in
        let list = expr ~d_env list in
        let state =
          if list_nullable then
            if_null list
              ~then_:(null (DT.mn_of_t state_t).DT.vtyp)
              ~else_:(do_fold (force ~what:"state" list))
          else
            do_fold list in
        (* Finalize the state: *)
        finalize_sf1 ~d_env aggr state
    | Stateful (state_lifespan, _, SF1 (aggr, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        finalize_sf1 ~d_env aggr state
    | Stateful (state_lifespan, _, SF2 (Lag, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        let past_vals = get_field "past_values" state
        and oldest_index = get_field "oldest_index" state in
        get_vec (get_ref oldest_index) past_vals
    | Stateful (state_lifespan, _, SF2 (ExpSmooth, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        force ~what:"finalize ExpSmooth" state
    | Stateful (state_lifespan, _, SF2 (Sample, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        (* If the result is nullable then empty-set is Null. Otherwise
         * an empty set is not possible according to type-checking. *)
        let_ ~name:"sample_set" ~l:d_env state (fun d_env set ->
          if e.E.typ.DT.nullable then
            if_ (eq (cardinality set) (u32_of_int 0))
              ~then_:(null (DT.mn_of_t (DE.type_of d_env set)).DT.vtyp)
              ~else_:(not_null set)
          else
            set)
    | Stateful (state_lifespan, _, SF3 (MovingAvg, _, _, item)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        let values = get_field "values" state in
        let_ ~name:"values" ~l:d_env values (fun d_env values ->
          let count = get_ref (get_field "count" state) in
          if_ (lt count (cardinality values))
            ~then_:(null (Base Float))
            ~else_:(
              let sum =
                (* FIXME: must not take the last added value in that
                 * average! *)
                fold
                  ~init:(float 0.)
                  ~body:(
                    DE.func2 ~l:d_env DT.float (Data item.E.typ) (fun _ s x ->
                      add s (to_float x)))
                  ~list:values in
              (* [div] already returns a nullable *)
              div sum (to_float (cardinality values))))
    | Stateful (state_lifespan, _, SF3 (Hysteresis, _, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        state (* The state is the final result *)
    | Stateful (state_lifespan, _,
                SF4s (Largest { up_to ; _ }, max_len, but, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        let values = get_field "values" state in
        let_ ~name:"values" ~l:d_env values (fun d_env values ->
          let but = to_u32 (expr ~d_env but) in
          let_ ~name:"but" ~l:d_env but (fun d_env but ->
            let heap_len = cardinality values in
            let_ ~name:"heap_len" ~l:d_env heap_len (fun d_env heap_len ->
              let max_len = to_u32 (expr ~d_env max_len) in
              let_ ~name:"max_len" ~l:d_env max_len (fun d_env max_len ->
                let item_t =
                  (* TODO: get_min for sets *)
                  match DE.type_of d_env values with
                  | DT.Data { vtyp = Set (_, mn) ; _ } -> mn
                  | _ -> assert false (* Because of [type_check]  *) in
                let proj =
                  DE.func1 ~l:d_env (DT.Data item_t) (fun _d_env item ->
                    get_item 0 item) in
                let res =
                  not_null (chop_end (map_ (list_of_set values) proj) but) in
                let cond = lt heap_len max_len in
                let cond =
                  if up_to then and_ cond (le heap_len but)
                           else cond in
                if_ cond
                  ~then_:(null e.E.typ.DT.vtyp)
                  ~else_:res))))
    | Stateful (state_lifespan, _,
                SF4 (Remember, _, _, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        apply (ext_identifier "CodeGenLib.Remember.finalize") [ state ]
    | Stateful (state_lifespan, _, Past { tumbling ; _ }) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        let values = get_field "values" state in
        let v_t = lst_item_type e in
        let item_t = past_item_t v_t in
        let_ ~name:"values" ~l:d_env values (fun d_env values ->
          let proj =
            DE.func1 ~l:d_env (DT.Data item_t) (fun d_env heap_item ->
              (conv_maybe_nullable ~to_:v_t d_env (get_item 0 heap_item))) in
          (if tumbling then
            let tumbled = get_field "tumbled" state in
            let_ ~name:"tumbled" ~l:d_env tumbled (fun _d_env tumbled ->
              if_null tumbled
                ~then_:(null e.E.typ.DT.vtyp)
                ~else_:(not_null (map_ (list_of_set tumbled) proj)))
          else
            not_null (map_ (list_of_set values) proj)))
    | Stateful (state_lifespan, _, Top { what ; output ; _ }) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        let top = get_field "top" state in
        (match output with
        | Rank ->
            todo "Top RANK"
        | Membership ->
            let what = expr ~d_env what in
            member what top
        | List ->
            list_of_set top)
    | _ ->
        Printf.sprintf2 "RaQL2DIL.expression for %a"
          (E.print false) e |>
        todo
  )

(*$= expression & ~printer:identity
  "(u8 1)" (expression ~r_env:[] ~d_env:[] (E.of_u8 1) |> IO.to_string DE.print)
*)

(* [d] must be nullable.  Returns either [f (force d)] (making sure it is
 * nullable) if d is not null, or NULL (of the same type than that returned
 * by [f]). *)
(* TODO: move all these functions into stdLib: *)
and propagate_null ?(depth=0) d_env d f =
  !logger.debug "%s...propagating null from %a"
    (indent_of depth)
    (DE.print ?max_depth:None) d ;
  let_ ~name:"nullable_" ~l:d_env d (fun d_env d ->
    let res = DC.ensure_nullable ~l:d_env (f d_env (force d)) in
    let mn = DT.mn_of_t (DE.type_of d_env res) in
    if_null d
      ~then_:(null mn.DT.vtyp)
      (* Since [f] can return a nullable value already, rely on
       * [conv_maybe_nullable_from] to do the right thing instead of
       * [not_null]: *)
      ~else_:res)

and apply_lst ?(depth=0) ?convert_in ?(enlarge_in=false) d_env ds f =
  assert (convert_in = None || not enlarge_in) ;
  let conv d_env d =
    match convert_in with
    | None -> d
    | Some to_ -> DC.conv ~to_ d_env d in
  (* neither of the [ds] are nullable at that point: *)
  let no_prop d_env ds =
    let largest =
      if enlarge_in then
        List.fold_left (fun prev_largest d ->
          let mn = DT.mn_of_t (DE.type_of d_env d) in
          match prev_largest with
          | None -> Some mn.vtyp
          | Some prev -> Some (T.largest_type [ mn.vtyp ; prev ])
        ) None ds
      else None in
    let ds =
      List.fold_left (fun ds d ->
        let d =
          if convert_in <> None then
            conv d_env d
          else match largest with
            | None -> d
            | Some vtyp ->
                let mn = DT.mn_of_t (DE.type_of d_env d) in
                DC.conv_maybe_nullable ~to_:DT.{ mn with vtyp } d_env d in
        d :: ds
      ) [] ds in
    let ds = List.rev ds in
    f d_env ds in
  (* d1 is not nullable at this stage: *)
  let rec prop_loop d_env ds = function
    | [] ->
        no_prop d_env (List.rev ds)
    | d1 :: rest ->
        let mn = DT.mn_of_t (DE.type_of d_env d1) in
        if mn.DT.nullable then
          propagate_null ~depth d_env d1 (fun d_env d1 ->
            prop_loop d_env (d1 :: ds) rest)
        else
          (* no need to propagate nulls: *)
          prop_loop d_env (d1 :: ds) rest in
  prop_loop d_env [] ds

(* [apply_1] takes a DIL expression and propagate null or apply [f] on it.
 * Unlike [propagate_null], also works on non-nullable values.
 * Also optionally convert the input before passing it to [f] *)
and apply_1 ?depth ?convert_in d_env d1 f =
  apply_lst ?depth ?convert_in d_env [ d1 ]
            (fun d_env -> function
    | [ d1 ] -> f d_env d1
    | _ -> assert false)

(* Same as [apply_1] for two arguments: *)
and apply_2 ?depth ?convert_in ?enlarge_in d_env d1 d2 f =
  apply_lst ?depth ?convert_in ?enlarge_in d_env [ d1 ; d2 ]
            (fun d_env -> function
    | [ d1 ; d2 ] -> f d_env d1 d2
    | _ -> assert false)

(* Same as [apply_1] for three arguments: *)
and apply_3 ?depth ?convert_in ?enlarge_in d_env d1 d2 d3 f =
  apply_lst ?depth ?convert_in ?enlarge_in d_env [ d1 ; d2 ; d3 ]
            (fun d_env -> function
    | [ d1 ; d2 ; d3 ] -> f d_env d1 d2 d3
    | _ -> assert false)

(* Update the state(s) used by the expression [e]. *)
let update_state_for_expr ~r_env ~d_env ~what e =
  let with_state ~d_env state_rec e f =
    let open DE.Ops in
    let state = get_state state_rec e in
    let_ ~name:"state" ~l:d_env state f in
  (* Either call [f] with a DIL variable holding the (forced, if [skip_nulls])
   * value of [e], or do nothing if [skip_nulls] and [e] is null: *)
  let with_expr ~skip_nulls d_env e f =
    let d = expression ~r_env ~d_env e in
    let_ ~name:"state_update_expr" ~l:d_env d (fun d_env d ->
      match DE.type_of d_env d, skip_nulls with
      | DT.Data { nullable = true ; _ }, true ->
          if_null d
            ~then_:nop
            ~else_:(let_ ~name:"forced_op" ~l:d_env
                         (force ~what:"with_expr" d) f)
      | _ ->
          f d_env d) in
  let with_exprs ~skip_nulls d_env es f =
    let rec loop d_env ds = function
      | [] ->
          f d_env []
      | [ e ] ->
          with_expr ~skip_nulls d_env e (fun d_env d ->
            f d_env (List.rev (d :: ds)))
      | e :: es ->
          with_expr ~skip_nulls d_env e (fun d_env d ->
            loop d_env (d :: ds) es) in
    loop d_env [] es in
  let cmt = "update state for "^ what in
  E.unpure_fold [] (fun _s lst e ->
    let convert_in = e.E.typ.DT.vtyp in
    let may_set ~d_env state_rec new_state =
      if DT.eq DT.Void (DE.type_of d_env new_state) then
        new_state
      else
        set_state d_env state_rec e new_state in
    match e.E.text with
    | Stateful (_, _, SF1 (_, e1)) when E.is_a_list e1 ->
        (* Those are not actually stateful, see [expression] where those are
         * handled as stateless operators. *)
        lst
    | Stateful (state_lifespan, skip_nulls, SF1 (aggr, e1)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_state ~d_env state_rec e (fun d_env state ->
            let new_state = update_state_sf1 ~d_env ~convert_in aggr
                                             d1 state in
            may_set ~d_env state_rec new_state)
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF2 (aggr, e1, e2)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_expr ~skip_nulls d_env e2 (fun d_env d2 ->
            with_state ~d_env state_rec e (fun d_env state ->
              let new_state = update_state_sf2 ~d_env ~convert_in aggr
                                               d1 d2 state in
              may_set ~d_env state_rec new_state))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF3 (aggr, e1, e2, e3)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_expr ~skip_nulls d_env e2 (fun d_env d2 ->
            with_expr ~skip_nulls d_env e3 (fun d_env d3 ->
              with_state ~d_env state_rec e (fun d_env state ->
                let new_state = update_state_sf3 ~d_env ~convert_in aggr
                                                 d1 d2 d3 state in
                may_set ~d_env state_rec new_state)))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF4 (aggr, e1, e2, e3, e4)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_expr ~skip_nulls d_env e2 (fun d_env d2 ->
            with_expr ~skip_nulls d_env e3 (fun d_env d3 ->
              with_expr ~skip_nulls d_env e4 (fun d_env d4 ->
                with_state ~d_env state_rec e (fun d_env state ->
                  let new_state = update_state_sf4 ~d_env ~convert_in aggr
                                                   d1 d2 d3 d4 state in
                  may_set ~d_env state_rec new_state))))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF4s (aggr, e1, e2, e3, e4s)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env e1 (fun d_env d1 ->
          with_expr ~skip_nulls d_env e2 (fun d_env d2 ->
            with_expr ~skip_nulls d_env e3 (fun d_env d3 ->
              with_exprs ~skip_nulls d_env e4s (fun d_env d4s ->
                with_state ~d_env state_rec e (fun d_env state ->
                  let new_state = update_state_sf4s ~d_env ~convert_in aggr
                                                    d1 d2 d3 d4s state in
                  may_set ~d_env state_rec new_state))))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, Past { what ; time ; tumbling ; _ }) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env what (fun d_env what ->
          with_expr ~skip_nulls d_env time (fun d_env time ->
            with_state ~d_env state_rec e (fun d_env state ->
              let v_t = lst_item_type e in
              let new_state = update_state_past ~d_env ~convert_in
                                                tumbling what time state v_t in
              may_set ~d_env state_rec new_state))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls,
                Top { what ; by ; time ; duration ; _ }) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls d_env what (fun d_env what ->
          with_expr ~skip_nulls d_env by (fun d_env by ->
            with_expr ~skip_nulls d_env time (fun d_env time ->
              with_expr ~skip_nulls d_env duration (fun d_env duration ->
                with_state ~d_env state_rec e (fun d_env state ->
                  let decay =
                    neg (force ~what:"top1"
                          (div (force ~what:"top2" (log_ (float 0.5)))
                               (mul (float 0.5) (to_float duration)))) in
                  let_ ~name:"decay" ~l:d_env decay (fun d_env decay ->
                    let new_state = update_state_top ~d_env ~convert_in
                                                     what by decay time state in
                    may_set ~d_env state_rec new_state)))))
        ) :: lst
    | Stateful _ ->
        todo "update_state"
    | _ ->
        invalid_arg "update_state"
  ) e |>
  List.rev |>
  seq |>
  comment cmt

(* Augment the given compilation unit with some external identifiers required to
 * implement some of the RaQL expressions: *)
let init compunit =
  (* Some external types used in the following helper functions: *)
  let compunit =
    [ "globals_map", "CodeGenLib.Globals.map" ;
      "remember_state", "CodeGenLib.Remember.state" ] |>
    List.fold_left (fun compunit (name, def) ->
      DU.register_external_type compunit name (fun _ps -> function
        | DT.OCaml -> def
        | _ -> todo "codegen for other backends than OCaml")
    ) compunit in
  (* Some helper functions *)
  [ "CodeGenLib.uuid_of_u128",
      DT.(func1 u128 string) ;
    (* Currently map_get returns only strings, and given we cannot have type
     * parameters it will certainly stay that way. When maps of different types
     * are allowed, convert that string into something else after map_get has
     * returned. *)
    "CodeGenLib.Globals.map_get",
      DT.(func2 (Data (required (ext "globals_map"))) string
                (Data (optional (Base String)))) ;
    "CodeGenLib.Globals.map_set",
      DT.(func3 (Data (required (ext "globals_map"))) string string string) ;
    "CodeGenLib.Remember.init",
      DT.(func2 float float (Data (required ((ext "remember_state"))))) ;
    (* There is no way to call a function accepting any type so we will have
     * to encode that code manually with add_verbatim_definition:
    "CodeGenLib.Remember.add",
      DT.(func3 (Data (required (ext "remember_state"))) float XXX (ext "remember_state")) ; *)
    "CodeGenLib.Remember.finalize",
      DT.(func1 (Data (required (ext "remember_state"))) bool) ;
    "CodeGenLib.LinReg.fit_simple",
      DT.(func1 (Data (required (lst (optional (Base Float)))))
                (Data (optional (Base Float)))) ;
    "CodeGenLib.LinReg.fit",
      DT.(func1 (Data (required (lst (optional (Lst (required (Base Float)))))))
                (Data (optional (Base Float)))) ] |>
  List.fold_left (fun compunit (name, typ) ->
    DU.add_external_identifier compunit name typ
  ) compunit
