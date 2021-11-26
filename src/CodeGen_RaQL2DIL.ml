(* Compile (typed!) RaQL expressions into DIL expressions *)
open Batteries
open Stdint

open RamenHelpersNoLog
open RamenHelpers
open RamenLog
module DE = DessserExpressions
module DT = DessserTypes
module DS = DessserStdLib
module DU = DessserCompilationUnit
module E = RamenExpr
module N = RamenName
module T = RamenTypes
module Variable = RamenVariable
open DE.Ops
open Raql_binding_key.DessserGen

(*$inject
  open Batteries *)

(*
 * Helpers
 *)

(*let print_r_env oc =
  pretty_list_print (fun oc (k, (v, mn)) ->
    Printf.fprintf oc "%a=>%a:%a"
      E.print_binding_key k
      (DE.print ~max_depth:2) v
      DT.print_mn mn
  ) oc*)

let print_r_env oc =
  pretty_list_print (fun oc (k, v) ->
    Printf.fprintf oc "%a=>%a"
      E.print_binding_key k
      (DE.print ~max_depth:2) v
  ) oc

let rec constant mn v =
  let bad_type () =
    Printf.sprintf2 "Invalid type %a for literal %a"
      DT.print_mn mn
      T.print v |>
    failwith
  in
  match v with
  | Raql_value.VNull -> null mn.DT.typ
  | VUnit -> void
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
      (match mn.typ with
      | DT.TTup mns ->
          if Array.length mns <> Array.length vs then bad_type () ;
          make_tup (List.init (Array.length mns) (fun i ->
            constant mns.(i) vs.(i)))
      | _ ->
          bad_type ())
  | VVec vs ->
      (match mn.typ with
      | DT.TVec (d, mn) ->
          if d <> Array.length vs then bad_type () ;
          make_vec (List.init d (fun i -> constant mn vs.(i)))
      | _ ->
          bad_type ())
  | VArr vs ->
      (match mn.typ with
      | DT.TArr mn ->
          make_arr mn (List.init (Array.length vs) (fun i ->
            constant mn vs.(i)))
      | _ ->
          bad_type ())
  | VRec vs ->
      (match mn.typ with
      | DT.TRec mns ->
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
    | E.LocalState -> Variable.GroupState
    | E.GlobalState -> Variable.GlobalState in
  try List.assoc (RecordValue state_var) r_env
  with Not_found ->
    Printf.sprintf2
      "Expression %a uses variable %s that is not available in the environment \
       (only %a)"
      (E.print false) e
      (Variable.to_string state_var)
      print_r_env r_env |>
    failwith

(* Returns the field name in the state record for that expression: *)
let field_name_of_state e =
  "state_"^ Uint32.to_string e.E.uniq_num

(* Returns the state of the expression: *)
let get_state state_rec e =
  let fname = field_name_of_state e in
  let open DE.Ops in
  get_ref (get_field fname state_rec)

let set_state state_rec state_t e d =
  let fname = field_name_of_state e in
  let state = get_field fname state_rec in
  let open DE.Ops in
  let d = if state_t.DT.nullable then not_null d else d in
  set_ref state d

(* Comparison function for heaps of pairs ordered by the second item: *)
let cmp ?(inv=false) item_t =
  func2 item_t item_t (fun i1 i2 ->
    (* Should dessser have a compare function? *)
    if_ ((if inv then gt else lt) (get_item 1 i1) (get_item 1 i2))
      ~then_:(i8_of_int ~-1)
      ~else_:(
        if_ ((if inv then lt else gt) (get_item 1 i1) (get_item 1 i2))
          ~then_:(i8_of_int 1)
          ~else_:(i8_of_int 0)))

let lst_item_type e =
  match e.E.typ.DT.typ with
  | DT.TArr mn -> mn
  | _ ->
      !logger.error "Not an array: %a" DT.print e.E.typ.DT.typ ;
      assert false (* Because of RamenTyping.ml *)

let past_item_t v_t =
  DT.(required (TTup [| v_t ; DT.float |]))

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

let item_type_for_sample skip_nulls item_e =
  if skip_nulls then DT.required item_e.E.typ.DT.typ else item_e.typ

let item_type_for_largest res by =
  let v_t = lst_item_type res in
  let by_t =
    DT.required (
      if by = [] then
        (* [update_state] will then use the count field: *)
        TU32
      else
        (TTup (List.enum by /@ (fun e -> e.E.typ) |> Array.of_enum))) in
  DT.tuple [| v_t ; by_t |]

(* This function returns the initial value and type of the state required to
 * implement the passed RaQL operator.
 * This expression must not refer to the incoming data, as state can be
 * initialized before any data arrived (thus the value will not be found in
 * the environment) or just to figure out the required state type. *)
let rec init_state ?depth ~r_env e =
  let open DE.Ops in
  let depth = Option.map succ depth in
  let expr = expression ?depth ~r_env in
  let as_nullable_as_e v t =
    if e.E.typ.DT.nullable then not_null v, DT.optional t
    else v, DT.required t in
  match e.E.text with
  | Stateful (_, _, SF1 ((AggrMin | AggrMax | AggrFirst | AggrLast), _)) ->
      (* A bool to tell if there ever was a value, and the selected value *)
      make_tup [ bool false ; null e.typ.DT.typ ],
      DT.(required (tup [| bool ; optional e.typ.DT.typ |]))
  | Stateful (_, _, SF1 (AggrSum, _)) when e.typ.DT.typ = DT.TFloat ->
      let ksum = DS.Kahan.init in
      as_nullable_as_e ksum DS.Kahan.state_t.DT.typ
  | Stateful (_, _, SF1 (AggrSum, _)) ->
      convert e.E.typ (u8_of_int 0),
      e.E.typ
  | Stateful (_, _, SF1 (AggrAvg, _)) ->
      (* The state of the avg is composed of the count and the (Kahan) sum: *)
      make_tup [ u32_of_int 0 ; DS.Kahan.init ],
      DT.(required (tup [| u32 ; DS.Kahan.state_t |]))
  | Stateful (_, _, SF1 (AggrAnd, _)) ->
      bool true,
      DT.bool
  | Stateful (_, _, SF1 (AggrOr, _)) ->
      bool false,
      DT.bool
  | Stateful (_, _, SF1 ((AggrBitAnd | AggrBitOr | AggrBitXor), _)) ->
      convert e.E.typ (u8_of_int 0),
      e.E.typ
  | Stateful (_, _, SF1 (Group, _)) ->
      (* Groups are typed as lists not sets: *)
      let item_t =
        match e.E.typ.DT.typ with
        | DT.TArr mn -> mn
        | _ -> invalid_arg ("init_state: "^ E.to_string e) in
      empty_set item_t,
      DT.(required (set Simple item_t))
  | Stateful (_, _, SF1 (Count, _)) ->
      convert e.E.typ (u8_of_int 0),
      e.E.typ
  | Stateful (_, _, SF1 (Distinct, e)) ->
      (* Distinct result is a boolean telling if the last met value was already
       * present in the hash, so we also need to store that bool in the state
       * unfortunately. Since the hash_table is already mutable, let's also make
       * that boolean mutable: *)
      make_tup
        [ hash_table e.E.typ (u8_of_int 100) ;
          make_ref (bool false) ],
      DT.(required (tup [| required (set HashTable e.E.typ) ; ref_ bool |]))
  | Stateful (_, _, SF2 (Lag, steps, e)) ->
      (* The state is just going to be a list of past values initialized with
       * NULLs (the value when we have so far received less than that number of
       * steps) and the index of the oldest value. *)
      let item_vtyp = e.E.typ.DT.typ in
      let steps = expr steps in
      make_rec
        [ "past_values",
            (* We need one more item to remember the oldest value before it's
             * updated: *)
            (let len = add (u32_of_int 1) (to_u32 steps)
            and init = null item_vtyp in
            alloc_arr len init) ;
          "oldest_index", make_ref (u32_of_int 0) ],
      DT.(required (record [| "past_values", required (arr (optional item_vtyp)) ;
                              "oldest_index", ref_ u32 |]))
  | Stateful (_, _, SF2 (ExpSmooth, _, _)) ->
      null e.E.typ.DT.typ,
      DT.optional e.E.typ.DT.typ
  | Stateful (_, skip_nulls, SF2 (Sample, n, e)) ->
      let n = expr n in
      let item_t = item_type_for_sample skip_nulls e in
      sampling item_t n,
      DT.(required (set Sampling item_t))
  | Stateful (_, _, SF3 (MovingAvg, p, k, x)) when E.is_one p ->
      let one = convert DT.(required k.E.typ.DT.typ) (u8_of_int 1) in
      let k = expr k in
      let len = add k one in
      let init = DE.default_mn ~allow_null:true x.E.typ in
      make_rec
        [ "values", alloc_arr len init ;
          "count", make_ref (u32_of_int 0) ],
      DT.(required (record [| "values", required (arr x.E.typ) ;
                              "count", ref_ u32 |]))
  | Stateful (_, _, SF3 (Hysteresis, _, _, _)) ->
      (* The value is supposed to be originally within bounds: *)
      let init = bool true in
      as_nullable_as_e init DT.TBool
  (* Remember is implemented completely as external functions for init, update
   * and finalize (using CodeGenLib.Remember).
   * TOP should probably be external too: *)
  | Stateful (_, _, SF4 (Remember _, fpr, _, dur, _)) ->
      let frp = to_float (expr fpr)
      and dur = to_float (expr dur) in
      apply (ext_identifier "CodeGenLib.Remember.init") [ frp ; dur ],
      DT.(required (ext "remember_state"))
  | Stateful (_, _, SF4s (Largest { inv ; _ }, _, _, _, by)) ->
      let item_t = item_type_for_largest e by in
      let cmp = cmp ~inv item_t in
      make_rec
        [ (* Store each values and its weight in a heap: *)
          "values", heap cmp ;
          (* Count insertions, to serve as a default order: *)
          "count", make_ref (u32_of_int 0) ],
      DT.(required (record [| "values", required (set Heap item_t) ;
                              "count", ref_ u32 |]))
  | Stateful (_, _, Past { max_age ; sample_size ; _ }) ->
      if sample_size <> None then
        todo "PAST operator with integrated sampling" ;
      let v_t = lst_item_type e in
      let item_t = past_item_t v_t in
      let cmp = cmp item_t in
      make_rec
        [ "values", heap cmp ;
          "max_age", to_float (expr max_age) ;
          (* If tumbled is true, finalizer should then empty the values: *)
          "tumbled", make_ref (null DT.(set Heap item_t)) ;
          (* TODO: sampling *) ],
      DT.(required (record [| "values", required (set Heap item_t) ;
                              "max_age", float ;
                              "tumbled", ref_ (optional (set Heap item_t)) |]))
  | Stateful (_, skip_nulls, Top { size ; max_size ; sigmas ; top_what ; _ }) ->
      (* Dessser TOP set uses a special [insert_weighted] operator to insert
       * values with a weight. It has no notion of time and decay so decay will
       * be implemented when updating the state by inflating the weights with
       * time. It is therefore necessary to store the starting time in the
       * state in addition to the top itself. *)
      let size_t = size.E.typ.DT.typ in
      let item_t =
        if skip_nulls then DT.(required top_what.E.typ.DT.typ)
        else top_what.E.typ in
      let size = expr size in
      let max_size =
        match max_size with
        | Some max_size ->
            expr max_size
        | None ->
            let ten = convert DT.(required size_t) (u8_of_int 10) in
            mul ten size in
      let sigmas = expr sigmas in
      make_rec
        [ "starting_time", make_ref (null DT.TFloat) ;
          "top", top item_t (to_u32 size) (to_u32 max_size) sigmas ],
      DT.(required (record [| "starting_time", ref_ nfloat ;
                              "top", required (set Top item_t) |]))
  | _ ->
      (* TODO *)
      todo ("init_state of "^ E.to_string ~max_depth:1 e)

and get_field_binding ~r_env var field =
  (* Try first to see if there is this specific binding in the environment. *)
  let k = RecordField (var, field) in
  try List.assoc k r_env with
  | Not_found ->
      (* If not, that means this field has not been overridden but we may
       * still find the record it's from and pretend we have a Get from
       * the Variable instead: *)
      let binding = get_binding ~r_env (RecordValue var) in
      null_map binding (fun binding ->
        get_field (field :> string) binding)

(* Returns the type of the state record needed to store the states of all the
 * given stateful expressions: *)
and state_rec_type_of_expressions ~r_env es =
  let mns =
    List.map (fun e ->
      let d, mn = init_state ~r_env e in
      !logger.debug "init state of %a: %a"
        (E.print false) e
        (DE.print ?max_depth:None) d ;
      field_name_of_state e,
      (* The value is a 1 dimensional (mutable) vector *)
      DT.(ref_ mn)
    ) es |>
    Array.of_list in
  if mns = [||] then DT.void else DT.(required (record mns))

(* Implement an SF1 aggregate function, assuming skip_nulls is handled by the
 * caller (necessary since the item and state are already evaluated).
 * Also return a boolean telling if the state is mutated in place by the
 * returned expression.
 * NULL item will propagate to the state.
 * If [convert_in], item will be converted to [res_mm] first.
 * Used for normal state updates as well as aggregation over lists: *)
and update_state_sf1 aggr item item_t state res_t =
  let open DE.Ops in
  let convert_in = [ res_t ] in
  match aggr with
  | E.AggrMax | AggrMin | AggrFirst | AggrLast ->
      let d_op =
        match aggr, item_t.DT.typ with
        (* As a special case, RaQL allows boolean arguments to min/max: *)
        | AggrMin, TBool ->
            and_
        | AggrMax, TBool ->
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
          ~then_:(not_null item)
          ~else_:(
            apply_2 ~convert_in (get_item 1 state) item d_op) in
      make_tup [ bool true ; new_state_val ],
      false
  | AggrSum when res_t = DT.TFloat ->
      apply_2 state item (fun ksum d -> DS.Kahan.add ksum (to_float d)),
      false
  | AggrSum ->
      (* Typing can decide the state and/or item are nullable.
       * In any case, nulls must propagate: *)
      apply_2 ~convert_in state item add,
      false
  | AggrAvg ->
      let count = get_item 0 state
      and ksum = get_item 1 state in
      null_map item (fun d ->
        make_tup [ add count (u32_of_int 1) ;
                   DS.Kahan.add ksum d]),
      false
  | AggrAnd ->
      apply_2 state item and_,
      false
  | AggrOr ->
      apply_2 state item or_,
      false
  | AggrBitAnd ->
      apply_2 ~convert_in state item bit_and,
      false
  | AggrBitOr ->
      apply_2 ~convert_in state item bit_or,
      false
  | AggrBitXor ->
      apply_2 ~convert_in state item bit_xor,
      false
  | Group ->
      insert state item,
      true
  | Count ->
      let one = convert DT.(required res_t) (u8_of_int 1) in
      (match item_t.DT.typ with
      | TBool ->
          (* Count how many are true *)
          apply_2 state item (fun state item ->
            if_ item
              ~then_:(add state one)
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
          null_map state (add one)),
      false
  | Distinct ->
      let h = get_item 0 state
      and b = get_item 1 state in
      if_ (member item h)
        ~then_:(set_ref b (bool false))
        ~else_:(
          seq [
            insert h item ;
            set_ref b (bool true) ]),
      true
  | _ ->
      todo "update_state_sf1"

(* Implement an SF1 aggregate function, assuming skip_nulls is handled by the
 * caller (necessary since the item and state are already evaluated).
 * NULL item will propagate to the state.
 * Used for normal state updates as well as aggregation over lists: *)
and update_state_sf2 ~convert_in aggr item1 item1_t item2 item2_t state res_t =
  ignore convert_in ; ignore item1_t ; ignore res_t ;
  let open DE.Ops in
  match aggr with
  | E.Lag ->
      let past_vals = get_field "past_values" state
      and oldest_index = get_field "oldest_index" state in
      let item2 =
        if item2_t.DT.nullable then item2
                               else not_null item2 in
      seq [
        set_vec (get_ref oldest_index) past_vals item2 ;
        let next_oldest = add (get_ref oldest_index) (u32_of_int 1) in
        let_ ~name:"next_oldest" next_oldest (fun next_oldest ->
          let next_oldest =
            (* [gt] because we had item1+1 items in past_vals: *)
            if_ (gt next_oldest (to_u32 item1))
              ~then_:(u32_of_int 0)
              ~else_:next_oldest in
          set_ref oldest_index next_oldest) ],
      true
  | ExpSmooth ->
      if_null state
        ~then_:item2
        ~else_:(
          add (mul item2 item1)
              (mul (force ~what:"ExpSmooth" state)
                   (sub (float 1.) (to_float item1)))) |>
      not_null,
      false
  | Sample ->
      insert state item2,
      true
  | _ ->
      todo "update_state_sf2"

and update_state_sf3 aggr
                     item1 item1_mn item2 item2_mn item3 item3_mn
                     state state_t =
  let open DE.Ops in
  match aggr with
  | E.MovingAvg ->
      let_ ~name:"values" (get_field "values" state) (fun values ->
        let_ ~name:"count" (get_field "count" state) (fun count ->
          let x = to_float item3 in
          let idx =
            force ~what:"MovingAvg" (rem (get_ref count) (cardinality values)) in
          seq [
            set_vec idx values x ;
            set_ref count (add (get_ref count) (u32_of_int 1)) ])),
      true
  | Hysteresis ->
      let to_ =
        T.largest_type [ item1_mn.DT.typ ; item2_mn.DT.typ ; item3_mn.DT.typ ] in
      let to_ = DT.required to_ in
      let_ ~name:"x" (convert to_ item1) (fun x ->
        let_ ~name:"acceptable" (convert to_ item2)
             (fun acceptable ->
          let_ ~name:"maximum" (convert to_ item3)
               (fun maximum ->
            (* Above the maximum the state must become false,
             * below the acceptable the stage must become true,
             * and it must stays the same, including NULL, in between: *)
            let same_nullable b =
              if state_t.DT.nullable then
                not_null (bool b)
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
                      ~else_:state))))),
      false
  | _ ->
      todo "update_state_sf3"

and update_state_sf4 ~convert_in aggr
                     item1 item1_t item2 item2_t item3 item3_t
                     item4 item4_t state state_t =
  ignore item1 ; ignore item1_t ;
  ignore item3 ; ignore item3_t ;
  ignore item2_t ; ignore item4_t ; ignore state_t ;
  ignore convert_in ;
  match aggr with
  | E.Remember refresh ->
      let time = to_float item2
      and e = item4 in
      (* apply (ext_identifier "CodeGenLib.Remember.add")
            [ state ; time ; to_void (make_tup es) ] *)
      verbatim
        [ DessserMiscTypes.OCaml,
          "CodeGenLib.Remember.add "^ string_of_bool refresh ^" %1 %2 %3" ]
        DT.(required (ext "remember_state"))
        [ state ; time ; e ],
      false
  | _ ->
      todo "update_state_sf4"

and update_state_sf4s aggr item1 item2 item3 item4s state =
  ignore item2 ;
  match aggr with
  | E.Largest _ ->
      let max_len = to_u32 item1
      and e = item3
      and by = item4s in
      let_ ~name:"values" (get_field "values" state) (fun values ->
        let_ ~name:"count" (get_field "count" state) (fun count ->
          let by =
            (* Special updater that use the internal count when no `by` expressions
             * are present: *)
            if by = [] then [ get_ref count ] else by in
          let by = make_tup by in
          let heap_item = make_tup [ e ; by ] in
          seq [
            set_ref count (add (get_ref count) (u32_of_int 1)) ;
            insert values heap_item ;
            let_ ~name:"heap_len" (cardinality values)
              (fun heap_len ->
                if_ (gt heap_len max_len)
                  ~then_:(del_min values (u32_of_int 1))
                  ~else_:nop) ])),
      true
  | _ ->
      todo "update_state_sf4s"

and update_state_past ~convert_in tumbling what time state v_t =
  ignore convert_in ;
  let open DE.Ops in
  let values = get_field "values" state
  and max_age = get_field "max_age" state
  and tumbled = get_field "tumbled" state in
  let item_t =
    DT.required (TTup [| v_t ; DT.float |]) in
  let expell_the_olds =
    if_ (gt (cardinality values) (u32_of_int 0))
      ~then_:(
        let min_time = get_item 1 (get_min values) in
        if tumbling then (
          let_ ~name:"min_time" min_time (fun min_time ->
            (* Tumbling window: empty (and save) the whole data set when time
             * is due *)
            set_ref tumbled (
              if_ (eq (to_i32 (div time max_age))
                      (to_i32 (div min_time max_age)))
                ~then_:(null DT.(TSet (Heap, item_t)))
                ~else_:(not_null values)))
        ) else (
          (* Sliding window: remove any value older than max_age *)
          while_
            (let min_time = get_item 1 (get_min values) in
            lt (sub time min_time) max_age)
            (del_min values (u32_of_int 1))
        ))
      ~else_:nop |>
    comment "Expelling old values" in
  seq [
    expell_the_olds ;
    insert values (make_tup [ what ; time ]) ],
  true

and update_state_top ~convert_in what by decay time state =
  ignore convert_in ;
  (* Those two can be any numeric: *)
  let time = to_float time
  and by = to_float by in
  let open DE.Ops in
  let starting_time = get_field "starting_time" state
  and top = get_field "top" state in
  let inflation =
    let_ ~name:"starting_time" (get_ref starting_time) (fun t0_opt ->
      if_null t0_opt
        ~then_:(
          seq [
            set_ref starting_time (not_null time) ;
            float 1. ])
        ~else_:(
          let infl = exp_ (mul decay (sub time (force ~what:"inflation" t0_opt))) in
          let_ ~name:"top_infl" infl (fun infl ->
            let max_infl = float 1e6 in
            if_ (lt infl max_infl)
              ~then_:infl
              ~else_:(
                seq [
                  scale_weights top (force ~what:"inflation(2)"
                                      (div (float 1.) infl)) ;
                  set_ref starting_time (not_null time) ;
                  float 1. ])))) in
  let_ ~name:"top_inflation" inflation (fun inflation ->
    let weight = mul by inflation in
    let_ ~name:"weight" weight (fun weight ->
      insert_weighted top weight what)),
  true

(* Environments:
 * - [r_env] is the stack of currently reachable "raql thing", such
 *   as expression state, a record (in other words an E.binding_key), bound
 *   to a dessser expression (typically an identifier or a param). *)
and expression ?(depth=0) ~r_env e =
  !logger.debug "%sCompiling into DIL: %a"
    (indent_of depth)
    (E.print true) e ;
  assert (E.is_typed e) ;
  let convert_in = [ e.E.typ.DT.typ ] in
  let depth = depth + 1 in
  let expr = expression ~depth ~r_env in
  let bad_type () =
    Printf.sprintf2 "Invalid type %a for expression %a"
      DT.print_mn e.E.typ
      (E.print false) e |>
    failwith in
  let conv_from = convert DT.(required e.E.typ.DT.typ) in
  let conv_mn_from = convert e.E.typ in
  (* In any case we want the output to be converted to the expected type: *)
  conv_mn_from (
    match e.E.text with
    | Stateless (SL0 (Const v)) ->
        constant e.E.typ v
    | Tuple es ->
        (match e.E.typ.DT.typ with
        | DT.TTup mns ->
            if Array.length mns <> List.length es then bad_type () ;
            (* Better convert items before constructing the tuple: *)
            List.mapi (fun i e ->
              convert mns.(i) (expr e)
            ) es |>
            make_tup
        | _ ->
            bad_type ())
    | Record nes ->
        (match e.E.typ.DT.typ with
        | DT.TRec mns ->
            if Array.length mns <> List.length nes then bad_type () ;
            List.mapi (fun i (n, e) ->
              (n : N.field :> string),
              convert (snd mns.(i)) (expr e)
            ) nes |>
            make_rec
        | _ ->
            bad_type ())
    | Vector es ->
        (match e.E.typ.DT.typ with
        | DT.TVec (dim, mn) ->
            if dim <> List.length es then bad_type () ;
            List.map (fun e ->
              convert mn (expr e)
            ) es |>
            make_vec
        | _ ->
            bad_type ())
    | Stateless (SL0 (Variable var)) ->
        get_binding ~r_env (RecordValue var)
    | Stateless (SL0 (Binding (RecordField (var, field)))) ->
        get_field_binding ~r_env var field
    | Stateless (SL0 (Binding k)) ->
        (* A reference to the raql environment. Look for the dessser expression it
         * translates to. *)
        get_binding ~r_env k
    | Case (alts, else_) ->
        let rec alt_loop = function
          | [] ->
              (match else_ with
              | Some e -> conv_mn_from (expr e)
              | None -> null e.E.typ.DT.typ)
          | E.{ case_cond = cond ; case_cons = cons } :: alts' ->
              null_map (expr cond) (fun cond ->
                if_ cond
                    ~then_:(convert e.E.typ (expr cons))
                    ~else_:(alt_loop alts')) in
        alt_loop alts
    | Stateless (SL0 Now) ->
        conv_from now
    | Stateless (SL0 Random) ->
        random_float
    | Stateless (SL0 Pi) ->
        float Float.pi
    | Stateless (SL0 EventStart) ->
        (* No support for event-time expressions, just convert into the start/stop
         * fields: *)
        get_field_binding ~r_env Out (N.field "start")
    | Stateless (SL0 EventStop) ->
        get_field_binding ~r_env Out (N.field "stop")
    | Stateless (SL1 (Age, e1)) ->
        null_map (expr e1) (fun d ->
          let d = convert DT.(required e.E.typ.DT.typ) d in
          sub now d)
    | Stateless (SL1 (Cast _, e1)) ->
        (* Type checking already set the output type of that Raql expression to the
         * target type, and the result will be converted into this type in any
         * case. *)
        expr e1
    | Stateless (SL1 (Force, e1)) ->
        force ~what:"explicit Force" (expr e1)
    | Stateless (SL1 (Peek (mn, endianness), e1)) when E.is_a_string e1 ->
        assert (not mn.DT.nullable) ;
        let endianness = E.endianness_of_wire endianness in
        (* peeked type is some integer. *)
        null_map (expr e1) (fun d1 ->
          let ptr = ptr_of_string d1 in
          let offs = size 0 in
          match mn.DT.typ with
          | DT.TU128 -> peek_u128 endianness ptr offs
          | DT.TU64 -> peek_u64 endianness ptr offs
          | DT.TU32 -> peek_u32 endianness ptr offs
          | DT.TU16 -> peek_u16 endianness ptr offs
          | DT.TU8 -> peek_u8 ptr offs
          | DT.TI128 -> to_i128 (peek_u128 endianness ptr offs)
          | DT.TI64 -> to_i64 (peek_u64 endianness ptr offs)
          | DT.TI32 -> to_i32 (peek_u32 endianness ptr offs)
          | DT.TI16 -> to_i16 (peek_u16 endianness ptr offs)
          | DT.TI8 -> to_i8 (peek_u8 ptr offs)
          (* Other widths TODO. We might not have enough bytes to read as
           * many bytes than the larger integer type. *)
          | typ ->
              Printf.sprintf2 "Peek %a" DT.print typ |>
              todo)
    | Stateless (SL1 (Length, e1)) ->
        null_map (expr e1) (fun d1 ->
          match e1.E.typ.DT.typ with
          | DT.TString -> string_length d1
          | DT.TArr _ -> cardinality d1
          | _ -> bad_type ()
        )
    | Stateless (SL1 (Lower, e1)) ->
        null_map (expr e1) lower
    | Stateless (SL1 (Upper, e1)) ->
        null_map (expr e1) upper
    | Stateless (SL1 (UuidOfU128, e1)) ->
        null_map (expr e1) (fun d ->
          apply (ext_identifier "CodeGenLib.uuid_of_u128") [ d ])
    | Stateless (SL1 (Not, e1)) ->
        null_map (expr e1) not_
    | Stateless (SL1 (Abs, e1)) ->
        null_map (expr e1) abs
    | Stateless (SL1 (Minus, e1)) ->
        null_map (expr e1) neg
    | Stateless (SL1 (Defined, e1)) ->
        not_ (is_null (expr e1))
    | Stateless (SL1 (Exp, e1)) ->
        null_map (expr e1) (fun d -> exp_ (to_float d))
    | Stateless (SL1 (Log, e1)) ->
        null_map (expr e1) (fun d -> log_ (to_float d))
    | Stateless (SL1 (Log10, e1)) ->
        null_map (expr e1) (fun d -> log10_ (to_float d))
    | Stateless (SL1 (Sqrt, e1)) ->
        null_map (expr e1) (fun d -> sqrt_ (to_float d))
    | Stateless (SL1 (Sq, e1)) ->
        null_map (expr e1) (fun d -> mul d d)
    | Stateless (SL1 (Ceil, e1)) ->
        null_map (expr e1) (fun d -> ceil_ (to_float d))
    | Stateless (SL1 (Floor, e1)) ->
        null_map (expr e1) (fun d -> floor_ (to_float d))
    | Stateless (SL1 (Round, e1)) ->
        null_map (expr e1) (fun d -> round (to_float d))
    | Stateless (SL1 (Cos, e1)) ->
        null_map (expr e1) (fun d -> cos_ (to_float d))
    | Stateless (SL1 (Sin, e1)) ->
        null_map (expr e1) (fun d -> sin_ (to_float d))
    | Stateless (SL1 (Tan, e1)) ->
        null_map (expr e1) (fun d -> tan_ (to_float d))
    | Stateless (SL1 (ACos, e1)) ->
        null_map (expr e1) (fun d -> acos_ (to_float d))
    | Stateless (SL1 (ASin, e1)) ->
        null_map (expr e1) (fun d -> asin_ (to_float d))
    | Stateless (SL1 (ATan, e1)) ->
        null_map (expr e1) (fun d -> atan_ (to_float d))
    | Stateless (SL1 (CosH, e1)) ->
        null_map (expr e1) (fun d -> cosh_ (to_float d))
    | Stateless (SL1 (SinH, e1)) ->
        null_map (expr e1) (fun d -> sinh_ (to_float d))
    | Stateless (SL1 (TanH, e1)) ->
        null_map (expr e1) (fun d -> tanh_ (to_float d))
    | Stateless (SL1 (Hash, e1)) ->
        null_map (expr e1) hash
    | Stateless (SL1 (Chr, e1)) ->
        null_map (expr e1) (fun d1 ->
          char_of_u8 (convert DT.u8 d1)
        )
    | Stateless (SL1 (Fit, e1)) ->
      (* [e1] is supposed to be a list/vector of tuples of scalars, or a
       * list/vector of scalars. In the first case, the first value of the
       * tuple is the value to be predicted and the others are predictors;
       * whereas in the second case the predictor will defaults to a
       * sequence.
       * All items of those tuples are supposed to be numeric, so we convert
       * all of them into floats and then proceed with the regression.  *)
      null_map (expr e1) (fun d1 ->
        let has_predictor =
          match e1.E.typ.DT.typ with
          | TArr { typ = TTup _ ; _ }
          | TVec (_, { typ = TTup _ ; _ }) -> true
          | _ -> false in
        if has_predictor then
          (* Convert the argument into a list of nullable lists of
           * non-nullable floats (do not use vector since it would not be
           * possible to type [CodeGenLib.LinReq.fit] for all possible
           * dimensions): *)
          let to_ = DT.(required (arr (optional (arr (required TFloat))))) in
          let d1 = convert to_ d1 in
          apply (ext_identifier "CodeGenLib.LinReg.fit") [ d1 ]
        else
          (* In case we have only the predicted value in a single dimensional
           * array, call a dedicated function: *)
          let to_ = DT.(required (arr nfloat)) in
          let d1 = convert to_ d1 in
          apply (ext_identifier "CodeGenLib.LinReg.fit_simple") [ d1 ])
    | Stateless (SL1 (Basename, e1)) ->
        null_map (expr e1) (fun d1 ->
          let_ ~name:"str_" d1 (fun str ->
            let pos = find_substring (bool false) (string "/") str in
            let_ ~name:"pos_" pos (fun pos ->
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
            null_map (expr e1) (fun d -> conv_from d)
        | e1 :: es' ->
            null_map (expr e1) (fun d1 ->
              let rest = { e with text = Stateless (SL1s (op, es')) } in
              null_map (expr rest) (fun d2 ->
                d_op (conv_from d1) d2)))
    | Stateless (SL1s (Print, es)) ->
        let to_string e =
          let d = expr e in
          convert DT.string d in
        (match List.rev es with
        | e1 :: es ->
            let_ ~name:"sep" (string "; ") (fun sep ->
              let_ ~name:"last" (expr e1) (fun d1 ->
                let dumps =
                  List.fold_left (fun lst e ->
                    let lst =
                      if lst = [] then lst else dump sep :: lst in
                    dump (to_string e) :: lst
                  ) [] es in
                seq (
                  [ dump (string "PRINT: [ ") ] @
                  dumps @
                  (if dumps = [] then [] else [ dump sep ]) @
                  [ dump (convert DT.string d1) ;
                    dump (string " ]\n") ;
                    d1 ])))
        | [] ->
            invalid_arg "RaQL2DIL.expression: empty PRINT")
    | Stateless (SL1s (Coalesce, es)) ->
        let e_ns =
          List.map (fun e1 ->
            (* Convert to the result's typ: *)
            let to_ = DT.{ e.E.typ with nullable = e1.E.typ.DT.nullable } in
            convert to_ (expr e1),
            e1.typ.nullable
          ) es in
        DS.coalesce e_ns
    | Stateless (SL2 (Add, e1, e2)) ->
        apply_2 ~convert_in (expr e1) (expr e2) add
    | Stateless (SL2 (Sub, e1, e2)) ->
        apply_2 ~convert_in (expr e1) (expr e2) sub
    (* Multiplication of a string by an integer repeats the string: *)
    | Stateless (SL2 (Mul, (E.{ typ = DT.{ typ = TString ; _ } ; _ } as s), n))
    | Stateless (SL2 (Mul, n, (E.{ typ = DT.{ typ = TString ; _ } ; _ } as s))) ->
        apply_2 (expr s) (expr n) (fun s n ->
          let_ ~name:"ss_ref" (make_ref (string "")) (fun ss_ref ->
            seq [
              DS.repeat ~from:(i32_of_int 0) ~to_:(to_i32 n) (fun _n ->
                set_ref ss_ref (append_string (get_ref ss_ref) s)) ;
              get_ref ss_ref ]))
    | Stateless (SL2 (Mul, e1, e2)) ->
        apply_2 ~convert_in (expr e1) (expr e2) mul
    | Stateless (SL2 (Div, e1, e2)) ->
        apply_2 ~convert_in (expr e1) (expr e2) div
    | Stateless (SL2 (IDiv, e1, e2)) ->
        (* When the result is a float we need to floor it *)
        (match e.E.typ with
        | DT.{ typ = TFloat ; _ } ->
            apply_2 ~convert_in (expr e1) (expr e2) (fun d1 d2 ->
              null_map (div d1 d2) floor_)
        | _ ->
            apply_2 ~convert_in (expr e1) (expr e2) div)
    | Stateless (SL2 (Mod, e1, e2)) ->
        apply_2 ~convert_in (expr e1) (expr e2) rem
    | Stateless (SL2 (Pow, e1, e2)) ->
        apply_2 ~convert_in (expr e1) (expr e2) pow
    | Stateless (SL2 (Trunc, e1, e2)) ->
        apply_2 (expr e1) (expr e2) (fun d1 d2 ->
          (* Before dividing, convert d1 and d2 into the largest type: *)
          let to_ = DT.required (T.largest_type [ e1.E.typ.DT.typ ; e2.typ.typ ]) in
          let d1 = convert to_ d1
          and d2 = convert to_ d2
          and zero = convert to_ (u8_of_int 0) in
          match e.E.typ.DT.typ with
          | TFloat ->
              null_map (div d1 d2) (fun d -> mul d2 (floor_ d))
          | TU8|TU16|TU24|TU32|TU40|TU48|TU56|TU64|TU128 ->
              null_map (div d1 d2) (fun d -> mul d2 d)
          | TI8|TI16|TI24|TI32|TI40|TI48|TI56|TI64|TI128 ->
              null_map (div d1 d2) (fun d ->
                let_ ~name:"truncated" (mul d2 d) (fun r ->
                  if_ (ge d1 zero) ~then_:r ~else_:(sub r d2)))
          | t ->
              Printf.sprintf2 "expression: Invalid type for Trunc: %a"
                DT.print t |>
              invalid_arg)
    | Stateless (SL2 (Reldiff, e1, e2)) ->
        apply_2 ~convert_in (expr e1) (expr e2) (fun d1 d2 ->
          let scale = max_ (abs d1) (abs d2) in
          let_ ~name:"scale" scale (fun scale ->
            let diff = abs (sub d1 d2) in
            if_ (eq scale (float 0.))
              ~then_:(float 0.)
              ~else_:(force ~what:"reldiff" (div diff scale))))
    (* AND and OR are shortcutting therefore NULLs can't be propagated like
     * for other operators: *)
    | Stateless (SL2 (And, e1, e2)) ->
        let mn1 = e1.E.typ
        and mn2 = e2.E.typ in
        let d1 = expr e1
        and d2 = expr e2 in
        (* If one of the operands is known to be false then the result is
         * false. *)
        (match mn1.DT.nullable, mn2.DT.nullable with
        | false, false ->
            and_ d1 d2
        | true, false ->
            let_ ~name:"and_op1" d1 (fun d1 ->
              let_ ~name:"and_op2" d2 (fun d2 ->
                if_ (is_null d1)
                  ~then_:(
                    if_ d2 ~then_:(null DT.TBool)
                           ~else_:(not_null (bool false)))
                  ~else_:(
                    not_null (and_ (force d1) d2))))
        | false, true ->
            let_ ~name:"and_op1" d1 (fun d1 ->
              if_ d1 ~then_:d2 ~else_:(not_null (bool false)))
        | true, true ->
            let_ ~name:"and_op1" d1 (fun d1 ->
              let_ ~name:"and_op2" d2 (fun d2 ->
                if_ (is_null d1)
                  ~then_:(
                    if_ (is_null d2)
                      ~then_:(null DT.TBool)
                      ~else_:(
                        if_ (force d2)
                          ~then_:(null DT.TBool)
                          ~else_:(not_null (bool false))))
                  ~else_:(
                    not_null (and_ (force d1) (force d2))))))
    | Stateless (SL2 (Or, e1, e2)) ->
        let mn1 = e1.E.typ
        and mn2 = e2.E.typ in
        let d1 = expr e1
        and d2 = expr e2 in
        (* If one of the operands is known to be true then the result is
         * true. *)
        (match mn1.DT.nullable, mn2.DT.nullable with
        | false, false ->
            or_ d1 d2
        | true, false ->
            let_ ~name:"or_op1" d1 (fun d1 ->
              let_ ~name:"or_op2" d2 (fun d2 ->
                if_ (is_null d1)
                  ~then_:(
                    if_ d2 ~then_:(not_null (bool true))
                           ~else_:(null DT.TBool))
                  ~else_:(
                    not_null (or_ (force d1) d2))))
        | false, true ->
            let_ ~name:"or_op1" d1 (fun d1 ->
              if_ d1 ~then_:(not_null (bool true)) ~else_:d2)
        | true, true ->
            let_ ~name:"or_op1" d1 (fun d1 ->
              let_ ~name:"or_op2" d2 (fun d2 ->
                if_ (is_null d1)
                  ~then_:(
                    if_ (is_null d2)
                      ~then_:(null DT.TBool)
                      ~else_:(
                        if_ (force d2)
                          ~then_:(not_null (bool true))
                          ~else_:(null DT.TBool)))
                  ~else_:(
                    not_null (or_ (force d1) (force d2))))))
    | Stateless (SL2 (Ge, e1, e2)) ->
        apply_2 ~convert_in:[ e1.E.typ.DT.typ ; e2.typ.typ ] (expr e1) (expr e2) ge
    | Stateless (SL2 (Gt, e1, e2)) ->
        apply_2 ~convert_in:[ e1.E.typ.DT.typ ; e2.typ.typ ] (expr e1) (expr e2) gt
    | Stateless (SL2 (Eq, e1, e2)) ->
        apply_2 ~convert_in:[ e1.E.typ.DT.typ ; e2.typ.typ ] (expr e1) (expr e2) eq
    | Stateless (SL2 (Concat, e1, e2)) ->
        apply_2 (expr e1) (expr e2) (fun d1 d2 ->
          join (string "") (make_vec [ d1 ; d2 ]))
    | Stateless (SL2 (StartsWith, e1, e2)) ->
        apply_2 (expr e1) (expr e2) starts_with
    | Stateless (SL2 (EndsWith, e1, e2)) ->
        apply_2 (expr e1) (expr e2) ends_with
    | Stateless (SL2 (BitAnd, e1, e2)) ->
        apply_2 ~convert_in:[ e1.E.typ.DT.typ ; e2.typ.typ ] (expr e1) (expr e2) bit_and
    | Stateless (SL2 (BitOr, e1, e2)) ->
        apply_2 ~convert_in:[ e1.E.typ.DT.typ ; e2.typ.typ ] (expr e1) (expr e2) bit_or
    | Stateless (SL2 (BitXor, e1, e2)) ->
        apply_2 ~convert_in:[ e1.E.typ.DT.typ ; e2.typ.typ ] (expr e1) (expr e2) bit_xor
    | Stateless (SL2 (BitShift, e1,
                      { text = Stateless (SL1 (Minus, e2)) ; _ })) ->
        apply_2 (expr e1) (expr e2) (fun d1 d2 ->
          right_shift d1 (to_u8 d2))
    | Stateless (SL2 (BitShift, e1, e2)) ->
        apply_2 (expr e1) (expr e2) (fun d1 d2 -> left_shift d1 (to_u8 d2))
    (* Get a known field from a record: *)
    | Stateless (SL2 (
          Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
              ({ typ = DT.{ typ = TRec _ ; _ } ; _ } as e2))) ->
        null_map (expr e2) (fun d -> get_field n d)
    (* Constant get from a vector: the nullability merely propagates, and the
     * program will crash if the constant index is outside the constant vector
     * bounds: *)
    | Stateless (SL2 (
          Get, ({ text = Stateless (SL0 (Const n)) ; _ } as e1),
               ({ typ = DT.{ typ = TVec _ ; _ } ; _ } as e2)))
      when E.is_integer n ->
        apply_2 (expr e1) (expr e2) nth
    (* Similarly, from a tuple: *)
    | Stateless (SL2 (Get, e1,
                           ({ typ = DT.{ typ = TTup _ ; _ } ; _ } as e2))) ->
      (match E.int_of_const e1 with
      | Some n ->
          null_map (expr e2) (get_item n)
      | None ->
          bad_type ())
    (* Get a value from a map: result is always nullable as the key might be
     * unbound at that time. *)
    | Stateless (SL2 (Get, key, ({ typ = DT.{ typ = TMap (key_t, _) ; _ } ;
                                   _ } as map))) ->
        apply_2 (expr key) (expr map) (fun key map ->
          (* Confidently convert the key value into the declared type for keys,
           * although the actual implementation of map_get accepts only strings
           * (and the type-checker will also only accept a map which keys are
           * strings since integers are list/vector accessors).
           * FIXME: either really support other types for keys, and find a new
           * syntax to distinguish Get from lists than maps, _or_ forbid
           * declaring a map of another key type than string. Oh, and by the
           * way, did I mentioned that map_get will only return strings as
           * well? *)
          let key = convert DT.(required key_t.DT.typ) key in
          (* Note: the returned string is going to be converted into the actual
           * expected result type using normal conversion from string. Ideally
           * we'd like to use a more efficient serialization format for LMDB
           * values. TODO. *)
          apply (ext_identifier "CodeGenLib.Globals.map_get") [ map ; key ])
    (* In all other cases the result is always nullable, in case the index goes
     * beyond the bounds of the vector/list or is an unknown field: *)
    | Stateless (SL2 (Get, e1, e2)) ->
        apply_2 (expr e1) (expr e2) nth
    | Stateless (SL2 (In, e1, e2)) ->
        DS.is_in (expr e1) e1.E.typ (expr e2) e2.E.typ
    | Stateless (SL2 (Strftime, fmt, time)) ->
        apply_2 (expr fmt) (expr time) (fun fmt time ->
          strftime fmt (to_float time))
    | Stateless (SL2 (Index, str, chr)) ->
        apply_2 (expr str) (expr chr) (fun str chr ->
          let res = find_substring true_ (string_of_char chr) str in
          let_ ~name:"index_" res (fun res ->
            if_null res
              ~then_:(i32_of_int ~-1)
              ~else_:(convert DT.i32 (force ~what:"Index" res))))
    | Stateless (SL3 (SubString, str, start, stop)) ->
        apply_3 (expr str) (expr start) (expr stop) (fun str start stop ->
          substring str start stop)
    | Stateless (SL3 (MapSet, map, e_k, e_v)) ->
        (match map.E.typ.DT.typ with
        (* Fetch the expected key and value type for the map type of m,
         * and convert the actual k and v into those types: *)
        | TMap (key_t, val_t) ->
            apply_3 (expr map) (expr e_k) (expr e_v) (fun map k v ->
              (* map_set takes only non-nullable keys and values, despite the
               * overall MapSet operator accepting nullable keys/values.
               * In theory, if we declare the map as accepting nullable keys
               * or values then we should really insert NULLs (as keys or
               * values) in the map. But we have to implement actual encoding
               * of any type into strings for that to work. Then, we would
               * serialize those values into strings and use map_set on those
               * strings. *)
              let k = convert DT.(required key_t.typ) k
              and v = convert DT.(required val_t.typ) v in
              apply (ext_identifier "CodeGenLib.Globals.map_set")
                    [ map ; k ; v ])
        | _ ->
            assert false (* Because of type checking *))
    | Stateless (SL2 (Percentile, e1, percs)) ->
        apply_2 (expr e1) (expr percs) (fun d1 d2 ->
          match e.E.typ.DT.typ with
          | TVec _ ->
              DS.percentiles d1 e1.E.typ d2 percs.E.typ.DT.typ
          | _ ->
              DS.percentiles d1 e1.E.typ (make_vec [ d2 ])
                             DT.(vec 1 (required percs.typ.typ)) |>
              unsafe_nth (u8_of_int 0))
    (*
     * Stateful functions:
     * When the argument is a list then those functions are actually stateless:
     *)
    (* FIXME: do not store a state for those in any state vector *)
    | Stateful (_, skip_nulls, SF1 (aggr, e_list))
      when E.is_a_list e_list ->
        let state, _ = init_state ~r_env e in
        let list_item_t =
          match e_list.E.typ with
          | DT.{ typ = (TVec (_, mn) | TArr mn | TSet (_, mn)) ; _ } ->
              mn
          | _ ->
              assert false (* Because 0f `E.is_a_list list` *) in
        let do_fold list item_t =
          let_ ~name:"state_ref" (make_ref state) (fun state_ref ->
            let state = get_ref state_ref in
            seq [
              for_each ~name:"item" list (fun item ->
                let update_state item item_t =
                  let new_state, mutated =
                    update_state_sf1 aggr item item_t state
                                     e.typ.DT.typ in
                  if mutated then new_state
                  else set_ref state_ref new_state in
                if skip_nulls && item_t.DT.nullable then
                  if_null item
                    ~then_:nop
                    ~else_:(
                      update_state (force ~what:"do_fold" item)
                                   { item_t with nullable = false })
                else
                  update_state item item_t) ;
              state ]) in
        let list = expr e_list in
        let state =
          null_map list (fun list ->
            do_fold list list_item_t) in
        (* Finalize the state: *)
        null_map state (fun state ->
          finalize_sf1 aggr state e.typ)
    | Stateful (state_lifespan, _, SF1 (aggr, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        finalize_sf1 aggr state e.typ
    | Stateful (state_lifespan, _, SF2 (Lag, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let_ ~name:"lag_state" (get_state state_rec e) (fun state ->
          let past_vals = get_field "past_values" state
          and oldest_index = get_field "oldest_index" state in
          nth (get_ref oldest_index) past_vals)
    | Stateful (state_lifespan, _, SF2 (ExpSmooth, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        force ~what:"finalize ExpSmooth" state
    | Stateful (state_lifespan, skip_nulls, SF2 (Sample, _, item_e)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        (* If the result is nullable then empty-set is Null. Otherwise
         * an empty set is not possible according to type-checking. *)
        let_ ~name:"sample_set" state (fun set ->
          if e.E.typ.DT.nullable then
            let item_t = item_type_for_sample skip_nulls item_e in
            if_ (eq (cardinality set) (u32_of_int 0))
              ~then_:(null (DT.set Sampling item_t))
              ~else_:(not_null set)
          else
            set)
    | Stateful (state_lifespan, _, SF3 (MovingAvg, _, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let_ ~name:"mavg_state" (get_state state_rec e) (fun state ->
          let values = get_field "values" state in
          let_ ~name:"values" values (fun values ->
            let count = get_ref (get_field "count" state) in
            if_ (lt count (cardinality values))
              ~then_:(null TFloat)
              ~else_:(
                let sum =
                  (* FIXME: must not take the last added value in that
                   * average! *)
                  let_ ~name:"mvavg_sum" (make_ref (float 0.)) (fun s_ref ->
                    let s = get_ref s_ref in
                    seq [
                      for_each ~name:"x" values (fun x ->
                        set_ref s_ref (add s (to_float x))) ;
                      s ]) in
                (* [div] already returns a nullable *)
                div sum (to_float (cardinality values)))))
    | Stateful (state_lifespan, _, SF3 (Hysteresis, _, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        state (* The state is the final result *)
    | Stateful (state_lifespan, _,
                SF4s (Largest { up_to ; _ }, max_len, but, _, by)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        let values = get_field "values" state in
        let_ ~name:"values" values (fun values ->
          let but = to_u32 (expr but) in
          let_ ~name:"but" but (fun but ->
            let heap_len = cardinality values in
            let_ ~name:"heap_len" heap_len (fun heap_len ->
              let max_len = to_u32 (expr max_len) in
              let_ ~name:"max_len" max_len (fun max_len ->
                let item_t = item_type_for_largest e by in
                let proj =
                  func2 DT.void item_t (fun _ item ->
                    get_item 0 item) in
                let res =
                  not_null (chop_end (map_ nop proj (arr_of_set values)) but) in
                let cond = lt heap_len max_len in
                let cond =
                  if up_to then and_ cond (le heap_len but)
                           else cond in
                if_ cond
                  ~then_:(null e.E.typ.DT.typ)
                  ~else_:res))))
    | Stateful (state_lifespan, _,
                SF4 (Remember _, _, _, _, _)) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        apply (ext_identifier "CodeGenLib.Remember.finalize") [ state ]
    | Stateful (state_lifespan, _, Past { tumbling ; _ }) ->
        let state_rec = pick_state r_env e state_lifespan in
        let_ ~name:"past_state" (get_state state_rec e) (fun state ->
          let values = get_field "values" state in
          let v_t = lst_item_type e in
          let item_t = past_item_t v_t in
          let_ ~name:"values" values (fun values ->
            let proj =
              func2 DT.void item_t (fun _ heap_item ->
                (convert v_t (get_item 0 heap_item))) in
            (if tumbling then
              let tumbled = get_field "tumbled" state in
              null_map tumbled (fun tumbled ->
                not_null (map_ nop proj (arr_of_set tumbled)))
            else
              not_null (map_ nop proj (arr_of_set values)))))
    | Stateful (state_lifespan, _, Top { top_what ; output ; _ }) ->
        let state_rec = pick_state r_env e state_lifespan in
        let state = get_state state_rec e in
        let top = get_field "top" state in
        (match output with
        | Rank ->
            todo "Top RANK"
        | Membership ->
            let what = expr top_what in
            member what top
        | List ->
            arr_of_set top)
    | _ ->
        Printf.sprintf2 "RaQL2DIL.expression for %a"
          (E.print false) e |>
        todo
  )

(*$= expression & ~printer:identity
  "(u8 1)" (expression ~r_env:[] (E.of_u8 1) |> \
            DessserTypeCheck.type_check DE.no_env |> \
            DessserEval.peval DE.no_env |> \
            IO.to_string DE.print)
*)

and finalize_sf1 aggr state res_mn =
  match aggr with
  | E.AggrMax | AggrMin | AggrFirst | AggrLast ->
      get_item 1 state
  | AggrSum when res_mn.DT.typ = DT.TFloat ->
      null_map state DS.Kahan.finalize
  | AggrSum ->
      state
  | AggrAvg ->
      let_ ~name:"avg_state" state (fun state ->
        let count = get_item 0 state
        and ksum = get_item 1 state in
        div (DS.Kahan.finalize ksum)
            (convert DT.float count))
  | AggrAnd | AggrOr | AggrBitAnd | AggrBitOr | AggrBitXor | Group |
    Count ->
      (* The state is the final value: *)
      state
  | Distinct ->
      let b = get_item 1 state in
      get_ref b
  | _ ->
      todo "finalize_sf1"

(* Call f with non null expressions, or propagate the null.
 * If [convert_in] is not empty, convert all input to the largest of those
 * types. *)
and apply_lst ?(convert_in=[]) ds f =
  (* neither of the [ds] are nullable at that point: *)
  let no_prop ds =
    let largest =
      match convert_in with
      | [] -> None
      | [ t ] -> Some t
      | lst -> Some (T.largest_type lst) in
    let ds =
      List.fold_left (fun ds d ->
        let d =
          match largest with
          | None -> d
          | Some typ -> convert DT.(required typ) d in
        d :: ds
      ) [] ds in
    let ds = List.rev ds in
    f ds in
  let rec prop_loop ds = function
    | [] ->
        no_prop (List.rev ds)
    | d1 :: rest ->
        null_map d1 (fun d ->
          prop_loop (d :: ds) rest)
  in
  prop_loop [] ds

(* [apply_2] takes a DIL expression and propagate null or apply [f] on it.
 * Also optionally convert the input before passing it to [f] *)
and apply_2 ?convert_in d1 d2 f =
  apply_lst ?convert_in [ d1 ; d2 ]
            (function
    | [ d1 ; d2 ] -> f d1 d2
    | _ -> assert false)

(* Same as [apply_1] for three arguments: *)
and apply_3 ?convert_in d1 d2 d3 f =
  apply_lst ?convert_in [ d1 ; d2 ; d3 ]
            (function
    | [ d1 ; d2 ; d3 ] -> f d1 d2 d3
    | _ -> assert false)

(* Update the state(s) used by the expression [e]. *)
let update_state_for_expr ~r_env ~what e =
  let with_state state_rec e f =
    let open DE.Ops in
    let state = get_state state_rec e in
    let _, state_t = init_state ~r_env e in
    let_ ~name:"state" state (fun state -> f state state_t) in
  (* Either call [f] with a DIL variable holding the (forced, if [skip_nulls])
   * value of [e], or do nothing if [skip_nulls] and [e] is null: *)
  let with_expr ~skip_nulls e f =
    let d = expression ~r_env e in
    let_ ~name:"state_update_expr" d (fun d ->
      if e.E.typ.DT.nullable && skip_nulls then
        if_null d
          ~then_:nop
          ~else_:(let_ ~name:"forced_op" (force ~what:"with_expr" d) f)
      else
        f d) in
  let with_exprs ~skip_nulls es f =
    let rec loop ds = function
      | [] ->
          f []
      | [ e ] ->
          with_expr ~skip_nulls e (fun d ->
            f (List.rev (d :: ds)))
      | e :: es ->
          with_expr ~skip_nulls e (fun d ->
            loop (d :: ds) es) in
    loop [] es in
  let cmt = "update state for "^ what in
  E.unpure_fold [] (fun _s lst e ->
    let may_set state_rec state_t (new_state, mutated) =
      if mutated then
        new_state
      else
        set_state state_rec state_t e new_state in
    match e.E.text with
    | Stateful (_, _, SF1 (_, e1)) when E.is_a_list e1 ->
        (* Those are not actually stateful, see [expression] where those are
         * handled as stateless operators. *)
        lst
    | Stateful (state_lifespan, skip_nulls, SF1 (aggr, e1)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls e1 (fun d1 ->
          with_state state_rec e (fun state state_t ->
            update_state_sf1 aggr d1 e1.E.typ state e.typ.DT.typ |>
            may_set state_rec state_t)
        ) :: lst
    (* FIXME: not all parameters are subject to skip_nulls! *)
    | Stateful (state_lifespan, skip_nulls, SF2 (aggr, e1, e2)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls e1 (fun d1 ->
          with_expr ~skip_nulls e2 (fun d2 ->
            with_state state_rec e (fun state state_t ->
              update_state_sf2 ~convert_in:true aggr d1 e1.E.typ d2 e2.E.typ state
                               e.typ.typ |>
              may_set state_rec state_t))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF3 (aggr, e1, e2, e3)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls e1 (fun d1 ->
          with_expr ~skip_nulls e2 (fun d2 ->
            with_expr ~skip_nulls e3 (fun d3 ->
              with_state state_rec e (fun state state_t ->
                update_state_sf3 aggr d1 e1.E.typ d2 e2.E.typ
                                 d3 e3.E.typ state state_t |>
                may_set state_rec state_t)))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF4 (aggr, e1, e2, e3, e4)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls e1 (fun d1 ->
          with_expr ~skip_nulls e2 (fun d2 ->
            with_expr ~skip_nulls e3 (fun d3 ->
              with_expr ~skip_nulls e4 (fun d4 ->
                with_state state_rec e (fun state state_t ->
                  update_state_sf4 ~convert_in:true aggr d1 e1.E.typ d2 e2.E.typ
                                   d3 e3.E.typ d4 e4.E.typ state state_t |>
                  may_set state_rec state_t))))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, SF4s (aggr, e1, e2, e3, e4s)) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls e1 (fun d1 ->
          with_expr ~skip_nulls e2 (fun d2 ->
            with_expr ~skip_nulls e3 (fun d3 ->
              with_exprs ~skip_nulls e4s (fun d4s ->
                with_state state_rec e (fun state state_t ->
                  update_state_sf4s aggr d1 d2 d3 d4s state |>
                  may_set state_rec state_t))))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls, Past { what ; time ; tumbling ; _ }) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls what (fun what ->
          with_expr ~skip_nulls time (fun time ->
            with_state state_rec e (fun state state_t ->
              let v_t = lst_item_type e in
              update_state_past ~convert_in:true tumbling what time state v_t |>
              may_set state_rec state_t))
        ) :: lst
    | Stateful (state_lifespan, skip_nulls,
                Top { top_what ; by ; top_time ; duration ; _ }) ->
        let state_rec = pick_state r_env e state_lifespan in
        with_expr ~skip_nulls top_what (fun what ->
          with_expr ~skip_nulls by (fun by ->
            with_expr ~skip_nulls top_time (fun time ->
              with_expr ~skip_nulls duration (fun duration ->
                with_state state_rec e (fun state state_t ->
                  let decay =
                    neg (force ~what:"top1"
                          (div (force ~what:"top2" (log_ (float 0.5)))
                               (mul (float 0.5) (to_float duration)))) in
                  let_ ~name:"decay" decay (fun decay ->
                    update_state_top ~convert_in:true what by decay time state |>
                    may_set state_rec state_t)))))
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
        | DessserMiscTypes.OCaml -> def
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
      DT.(func2 (required (ext "globals_map")) string
                (optional TString)) ;
    "CodeGenLib.Globals.map_set",
      DT.(func3 (required (ext "globals_map")) string string string) ;
    "CodeGenLib.Remember.init",
      DT.(func2 float float (required (ext "remember_state"))) ;
    (* There is no way to call a function accepting any type so we will have
     * to encode that code manually with add_verbatim_definition:
    "CodeGenLib.Remember.add",
      DT.(func3 (required (ext "remember_state")) float XXX (ext "remember_state")) ; *)
    "CodeGenLib.Remember.finalize",
      DT.(func1 (required (ext "remember_state")) bool) ;
    "CodeGenLib.LinReg.fit_simple",
      DT.(func1 (required (arr (optional TFloat)))
                (optional TFloat)) ;
    "CodeGenLib.LinReg.fit",
      DT.(func1 (required (arr (optional (TArr float))))
                (optional TFloat)) ] |>
  List.fold_left (fun compunit (name, typ) ->
    DU.add_external_identifier compunit name typ
  ) compunit
