(* Make use of external library Dessser for code generation *)
open Batteries
open Dessser
open Stdint

open RamenConsts
open RamenHelpers
open RamenHelpersNoLog
open RamenLang
open RamenLog

module C = RamenConf
module DC = DessserConversions
module DE = DessserExpressions
module Default = RamenConstsDefault
module DP = DessserPrinter
module DT = DessserTypes
module DU = DessserCompilationUnit
module E = RamenExpr
module EntryPoints = RamenConstsEntryPoints
module Files = RamenFiles
module Helpers = CodeGen_Helpers
module N = RamenName
module O = RamenOperation
module Orc = RamenOrc
module RaQL2DIL = CodeGen_RaQL2DIL
module T = RamenTypes
module Variable = RamenVariable

module Value2RingBuf = DessserHeapValue.Serialize (DessserRamenRingBuffer.Ser)
module RingBuf2Value = DessserHeapValue.Materialize (DessserRamenRingBuffer.Des)

module RowBinary2Value = DessserHeapValue.Materialize (DessserRowBinary.Des)

let rowbinary_to_value ?config mn compunit =
  let open DE.Ops in
  let compunit, e = RowBinary2Value.make ?config mn compunit in
  compunit,
  comment "Function deserializing the rowbinary into a heap value:" e

module Csv2Value = DessserHeapValue.Materialize (DessserCsv.Des)

open Raql_binding_key.DessserGen

let csv_to_value ?config mn compunit =
  let open DE.Ops in
  let compunit, e = Csv2Value.make ?config mn compunit in
  compunit,
  comment "Function deserializing the CSV into a heap value:" e

(* Returns a DIL function that returns the total size of the serialized value
 * filtered by the passed fieldmask: *)
let sersize_of_type mn compunit =
  let cmt =
    Printf.sprintf2 "Compute the serialized size of values of type %a"
      DT.print_mn mn in
  let open DE.Ops in
  let compunit, e = Value2RingBuf.sersize mn compunit in
  compunit,
  comment cmt e

let ptr_of_tx tx =
  let open DE.Ops in
  let addr = apply (ext_identifier "RingBuf.tx_address") [ tx ] in
  let len = apply (ext_identifier "RingBuf.tx_size") [ tx ] in
  ptr_of_address (address_of_u64 addr) len

(* Takes a fieldmask, a tx, an offset and a heap value, and returns the new
 * offset. *)
let serialize mn compunit =
  let cmt =
    Printf.sprintf2 "Serialize a value of type %a"
      DT.print_mn mn in
  let open DE.Ops in
  let tx_t = DT.(required (Ext "tx")) in
  let compunit, ser = Value2RingBuf.serialize mn compunit in
  compunit,
  DE.func4 ~l:DE.no_env DT.mask tx_t DT.size mn
    (fun _d_env ma tx start_offs v ->
      let dst = ptr_of_tx tx in
      let dst = ptr_add dst start_offs in
      let dst = apply ser [ ma ; v ; dst ] in
      offset dst) |>
  comment cmt

(* The [generate_tuples_] function is the final one that's called after the
 * decision is taken to output a tuple. What we had so far as out [out_type]
 * was in fact [CodeGenLib_Skeleton]'s [`generator_out], which could still
 * have generators instead of proper values in some places (a generator is a
 * function generating several outputs that we want on distinct tuples.
 * [generate_tuples_] function takes an external callback function taking the
 * generated tuple_out and returning nothing, and a tuple_in and the
 * generator_out, and returns nothing. The callback is therefore in charge of
 * actually sending the generated tuples. *)
let generate_tuples in_type out_type out_fields =
  let open Raql_select_field.DessserGen in
  let has_generator =
    List.exists (fun sf ->
      E.is_generator sf.expr)
      out_fields in
  let callback_t = DT.func [| out_type |] DT.void in
  DE.func3 ~l:DE.no_env callback_t in_type out_type
    (fun _l f _it ot ->
      let open DE.Ops in
      if not has_generator then apply f [ ot ]
      else todo "generators")

(* Wrap around add_identifier_of_expression to display the full expression in case
 * type_check fails: *)
let add_identifier_of_expression ?name f compunit e =
  try
    f compunit ?name e
  with exn ->
    let fname = Filename.get_temp_dir_name () ^"/dessser_type_error.last" in
    ignore_exceptions (fun () ->
      let mode = [ `create ; `trunc ; `text ] in
      File.with_file_out fname ~mode (fun oc ->
        DE.print ?max_depth:None oc e)) () ;
    !logger.error "Invalid expression: %a (see complete expression in %s), %s"
      (DE.print ~max_depth:3) e
      fname
      (Printexc.to_string exn) ;
    raise exn

let init_compunit () =
  let compunit = DU.make () in
  let compunit = RaQL2DIL.init compunit in
  (* External types that are going to be used in external functions: *)
  let compunit =
    [ "ramen_ip", "RamenIp.t" ;
      "ramen_cidr", "RamenIp.Cidr.t" ;
      "float_pair", "(float * float)" ;
      "string_pair", "(string * string)" ;
      "factor_value", "(string * RamenTypes.value)" ;
      "tx", "RingBuf.tx" ;
      "ramen_value", "RamenTypes.value" ] |>
    List.fold_left (fun compunit (name, def) ->
      DU.register_external_type compunit name (fun _ps -> function
        | DessserMiscTypes.OCaml -> def
        | _ -> todo "codegen for other backends than OCaml")
    ) compunit in
  compunit

module OCaml =
struct
  module BE = DessserBackEndOCaml

  (* Here we rewrite Dessser heap values as internal value, converting
   * user types into our owns, etc, recursively.
   * FIXME: Use the same representation for records than dessser
   *        and keep heap_value as is. *)
  let rec emit_ramen_of_dessser_value ?(depth=0) mn oc vname =
    (* Not all values need conversion though. For performance, reuse as
     * much as the heap value as possible: *)
    let rec need_conversion mn =
      match mn.DT.typ with
      | Usr { name = ("Ip" | "Cidr") ; _ } ->
          true
      | Usr { def ; _ } ->
          need_conversion DT.{ nullable = false ; typ = def }
      | Vec (_, mn) | Arr mn | Set (_, mn) ->
          need_conversion mn
      | Rec _ ->
          (* Represented as records in Dessser but tuples in Ramen: *)
          true
      | Tup mns ->
          Array.exists need_conversion mns
      | Sum mns ->
          Array.exists (need_conversion % snd) mns
      | Map (k, v) ->
          need_conversion k || need_conversion v
      | _ ->
          false
    in
    if need_conversion mn then (
      emit oc depth "(" ;
      let vname', depth' =
        if mn.nullable then (
          emit oc (depth+1) "BatOption.map (fun x ->" ;
          "x", depth + 2
        ) else (
          vname, depth + 1
        ) in
      (match mn.typ with
      (* Convert Dessser makeshift type into Ramen's: *)
      | Usr { name = "Ip" ; _ } ->
          emit oc depth'
            "match %s with DessserGen.%s x -> RamenIp.V4 x \
             | %s x -> RamenIp.V6 x"
            vname' (BE.uniq_cstr_name mn.typ "v4")
            (BE.uniq_cstr_name mn.typ "v6")
      | Usr { name = "Cidr" ; _ } ->
          emit oc depth'
            "match %s with DessserGen.%s x -> RamenIp.Cidr.V4 (x.%s, x.%s) \
             | %s x -> RamenIp.Cidr.V6 (x.%s, x.%s)"
             vname'
             (BE.uniq_cstr_name mn.typ "v4")
             (BE.uniq_field_name T.cidrv4 "ip") (BE.uniq_field_name T.cidrv4 "mask")
             (BE.uniq_cstr_name mn.typ "v6")
             (BE.uniq_field_name T.cidrv6 "ip") (BE.uniq_field_name T.cidrv6 "mask")
      | Usr { def ; _ } ->
          let mn = DT.{ typ = def ; nullable = false } in
          emit_ramen_of_dessser_value ~depth mn oc vname'
      | Vec (_, mn) | Arr mn ->
          emit oc depth' "Array.map (fun x ->" ;
          emit_ramen_of_dessser_value ~depth:depth' mn oc "x" ;
          emit oc depth' ") %s" vname'
      | Rec mns as vt ->
          (* Emits an array of maybe_nullable values. [mns] has the field names
           * that must be prefixed with the module name to reach the fields in
           * Dessser generated code. *)
          Array.iteri (fun i (field_name, mn) ->
            let be_field_name = BE.uniq_field_name vt field_name in
            let n = vname' ^".DessserGen."^ be_field_name in
            let v =
              Printf.sprintf2 "%a" (emit_ramen_of_dessser_value ~depth:depth' mn) n in
            (* Remove the last newline for cosmetic: *)
            let v =
              let l = String.length v in
              if l > 0 && v.[l-1] = '\n' then String.rchop v else v in
            emit oc 0 "%s%s" v (if i < Array.length mns - 1 then "," else "")
          ) mns
      | Tup mns ->
          let varname i = "x"^ string_of_int i ^"_" in
          emit oc depth' "let %a = %s in"
            (CodeGen_OCaml.array_print_as_tuple_i (fun oc i _ ->
              String.print oc (varname i))) mns
            vname ;
          emit oc depth' "%a"
            (CodeGen_OCaml.array_print_as_tuple_i (fun oc i mn ->
              let v = varname i in
              emit_ramen_of_dessser_value ~depth:depth' mn oc v)) mns
      | Sum _ ->
          (* No values of type Sum yet *)
          assert false
      | _ ->
          emit oc depth' "%s" vname') ;
      if mn.DT.nullable then emit oc depth' ") %s" vname ;
      emit oc depth ")"
    ) else ( (* No need_conversion *)
      emit oc depth "%s" vname
    )

  (* Emits the function that reads a CSV line and returns an out tuple.
   * Notice that in_mn and out_mn differs only in ordering. *)
  let emit_reader deserializer in_mn out_mn oc =
    let p fmt = emit oc 0 fmt in
    let compunit = init_compunit () in
    let compunit, _, value_of_ser =
      let compunit, e = deserializer in_mn compunit in
      add_identifier_of_expression
        ~name:"value_of_ser" U.add_identifier_of_expression compunit e in
(* Unused for now, require the output type [mn] to be record-sorted:
    let compunit, _, _sersize_of_type =
      sersize_of_type mn DE.Ops.copy_field |>
      add_identifier_of_expression ~name:"sersize_of_type" DU.add_identifier_of_expression compunit in
    let compunit, _, _serialize =
      serialize mn |>
      add_identifier_of_expression ~name:"serialize" DU.add_identifier_of_expression compunit in
*)
    p "(* Helpers for deserializing type:\n\n%a\n\n*)\n"
      DT.print_mn in_mn ;
    BE.print_definitions oc compunit ;
    (* A public entry point to unserialize the tuple with a more meaningful
     * name, and which also convert the tuple representation.
     * Indeed, CodeGen_OCaml uses tuples for records whereas Dessser uses
     * actual records.
     * Assuming there are no embedded records, then it's enough to convert
     * the outer level which is trivially done here. *)
    p "" ;
    p "open DessserOCamlBackEndHelpers" ;
    p "" ;
    p "let read_tuple buffer start stop _has_more =" ;
    p "  assert (stop >= start) ;" ;
    p "  let src = Pointer.of_pointer (pointer_of_bytes buffer) start stop in" ;
    p "  let heap_value, src' = DessserGen.%s src in" value_of_ser ;
    p "  let read_sz = Pointer.sub src' src" ;
    p "  and tuple =" ;
    emit_ramen_of_dessser_value ~depth:2 out_mn oc "heap_value" ;
    p "  in" ;
    p "  tuple, read_sz"
end

module CPP =
struct
  module BE = DessserBackEndCPP

  let emit_reader deserializer in_mn _out_mn oc =
    let compunit = DU.make () in
    (* let compunit = RaQL2DIL.init compunit in TODO *)
    let compunit, _, _value_of_ser =
      let compunit, e = deserializer in_mn compunit in
      add_identifier_of_expression
        ~name:"value_of_ser" DU.add_identifier_of_expression compunit e in
(* Unused for now, require the output type [mn] to be record-sorted:
    let compunit, _, _sersize_of_type =
      sersize_of_type mn DE.Ops.copy_field |>
      add_identifier_of_expression ~name:"sersize_of_type" DU.add_identifier_of_expression compunit in
    let compunit, _, _serialize =
      serialize mn |>
      add_identifier_of_expression ~name:"serialize" DU.add_identifier_of_expression compunit in
*)
    Printf.fprintf oc "/* Helpers for function:\n\n%a\n\n*/\n"
      DT.print_mn in_mn ;
    BE.print_definitions oc compunit ;
    (* Also add some OCaml wrappers: *)
    String.print oc {|

/* OCaml wrappers */

extern "C" {
# define CAML_NAME_SPACE
# include <caml/mlvalues.h>
# include <caml/memory.h>
# include <caml/alloc.h>

  /* Takes an ocaml byte string containing the data and an offset (int), a max
   * offset (int), and a "more to come" flag (bool), and returns a pair
   * with the tuple (as a custom value) and the next offset. */
  CAMLprim value dessser_read_tuple(value data_, value offset_, value max_sz_,
                                    value has_more_)
  {
    CAMLparam4(data_, offset_, max_sz_, has_more_);
    CAMLlocal1(ret);
    ret = caml_alloc_tuple(2);
    Store_field(ret, 0, Val_unit);  // TODO
    Store_field(ret, 1, Val_long(100)); // TODO
    CAMLreturn(ret);
  }

  /* Takes a tuple (as a custom value) and return its size */
  CAMLprim value dessser_sersize_of_tuple(value tuple_)
  {
    CAMLparam1(tuple_);
    CAMLreturn(Val_long(42)); // TODO
  }

  /* Takes a TX, an offset and a tuple (as a custom value) and serialize
   * the tuple into the TX. Returns the actual size. */
  CAMLprim value dessser_serialize_tuple(value tx_, value offset_, value tuple_)
  {
    CAMLparam3(tx_, offset_, tuple_);
    CAMLreturn(Val_long(42)); // TODO
  }
}
|}
end

let dessser_type_of_ramen_tuple tup =
  if tup = [] then
    DT.void
  else
    let tup = Array.of_list tup in
    DT.required (Rec (Array.map (fun ft ->
      (ft.RamenTuple.name :> string), ft.typ
    ) tup))

(* Emit the function initializing the state vector for either the global or
 * the group state. If for the group state then the global_state_type is passed
 * to the init function. *)
let state_init ~r_env ~d_env state_lifespan global_state_type
               where commit_cond out_fields =
  let fold_unpure_fun i f =
    CodeGen_OCaml.fold_unpure_fun_my_lifespan
      state_lifespan out_fields ~where ~commit_cond i f in
  let cmt =
    Printf.sprintf2 "Initialize the %s state"
      (E.string_of_state_lifespan state_lifespan) in
  let param_t =
    if state_lifespan = E.GlobalState then DT.void
    else global_state_type
  in
  let open DE.Ops in
  DE.func1 ~l:d_env param_t (fun d_env _p ->
    (* TODO: add paramerter p to the env (if it's the global state, depending
     * on state_lifespan *)
    make_rec (
      fold_unpure_fun [] (fun l f ->
        let d = RaQL2DIL.init_state ~r_env ~d_env f in
        if DE.eq d (seq []) then l else
        let n = RaQL2DIL.field_name_of_state f in
        (n, make_vec [ d ]) :: l))) |>
  comment cmt

(* Emit the function that will return the next input tuple read from the input
 * ringbuffer, from the passed tx and start offset.
 * The function has to return the deserialized value. *)
let deserialize_tuple mn compunit =
  let cmt =
    Printf.sprintf2 "Deserialize a tuple of type %a"
      DT.print_mn mn in
  let open DE.Ops in
  let tx_t = DT.(required (Ext "tx")) in
  let compunit, des = RingBuf2Value.make mn compunit in
  compunit,
  DE.func2 ~l:DE.no_env tx_t DT.size (fun _d_env tx start_offs ->
    if DT.eq mn.DT.typ Void then (
      if mn.DT.nullable then
        not_null void
      else
        void
    ) else (
      let src = ptr_of_tx tx in
      let src = ptr_add src start_offs in
      let v_src = apply des [ src ] in
      first v_src
    )) |>
  comment cmt

(* Emit the where functions *)
let where_clause ?(with_group=false) ~r_env ~d_env in_type out_prev_type
                 global_state_type group_state_type expr =
  let open DE.Ops in
  let args =
    if with_group then
      DT.[| global_state_type ;
            in_type ;
            out_prev_type ;
            group_state_type |]
    else
      DT.[| global_state_type ;
            in_type ;
            out_prev_type |] in
  DE.func ~l:d_env args (fun d_env fid ->
    (* Add the function parameters to the environment for getting global
     * states, input and previous output tuples, and optional local states *)
    let r_env =
      (RecordValue GlobalState, param fid 0) ::
      (RecordValue In, param fid 1) ::
      (RecordValue OutPrevious, param fid 2) :: r_env in
    let r_env =
      if with_group then
        (RecordValue GroupState, param fid 3) :: r_env
      else
        r_env in
    seq
      [ (* Update the states used by that expression: *)
        RaQL2DIL.update_state_for_expr ~r_env ~d_env ~what:"where clause" expr ;
        (* Compute the boolean result: *)
        RaQL2DIL.expression ~r_env ~d_env expr ])

let cmp_for typ left_nullable right_nullable =
  let open DE.Ops in
  (* Start from a normal comparison function that returns -1/0/1: *)
  let base_cmp a b =
    if_ (gt a b)
      ~then_:(i8 Int8.one)
      ~else_:(
        if_ (gt b a)
          ~then_:(i8 (Int8.of_int ~-1))
          ~else_:(i8 Int8.zero)) in
  DE.func2 ~l:DE.no_env
    DT.(maybe_nullable ~nullable:left_nullable typ)
    DT.(maybe_nullable ~nullable:right_nullable typ) (fun _d_env a b ->
    match left_nullable, right_nullable with
    | false, false ->
        base_cmp a b
    | true, true ->
        if_ (and_ (is_null a) (is_null b))
          ~then_:(i8 Int8.zero)
          ~else_:(
            if_null a
              ~then_:(i8 (Int8.of_int ~-1))
              ~else_:(
              if_null b
                ~then_:(i8 Int8.one)
                ~else_:(base_cmp a b)))
    | true, false ->
        if_null a
          ~then_:(i8 (Int8.of_int ~-1))
          ~else_:(base_cmp a b)
    | false, true ->
        if_null b
          ~then_:(i8 Int8.one)
          ~else_:(base_cmp a b))

let emit_cond0_in ~r_env ~d_env ~to_typ in_type global_state_type e =
  let cmt =
    Printf.sprintf2 "The part of the commit condition that depends solely on \
                     the input tuple: %a"
      (E.print false) e in
  let open DE.Ops in
  (* input tuple -> global state -> something *)
  DE.func2 ~l:d_env in_type global_state_type
    (fun d_env in_ global_state ->
      let r_env =
        (RecordValue In, in_) ::
        (RecordValue GlobalState, global_state) :: r_env in
      let what = "commit clause 0, in" in
      seq
        [ RaQL2DIL.update_state_for_expr ~r_env ~d_env ~what e ;
          RaQL2DIL.expression ~r_env ~d_env e |>
          DC.conv ~to_:to_typ d_env ]) |>
  comment cmt

let emit_cond0_out ~r_env ~d_env ~to_typ
                   minimal_type out_prev_type global_state_type
                   group_state_type e =
  let cmt =
    Printf.sprintf2 "The part of the commit condition that depends on the \
                     output tuple: %a"
      (E.print false) e in
  let open DE.Ops in
  (* minimal tuple -> previous out -> global state -> local state -> thing *)
  DE.func4 ~l:d_env
    minimal_type out_prev_type
    group_state_type global_state_type
    (fun d_env min out_previous group_state global_state ->
      let r_env =
        (RecordValue Out, min) ::
        (RecordValue OutPrevious, out_previous) ::
        (RecordValue GroupState, group_state) ::
        (RecordValue GlobalState, global_state) :: r_env in
      let what = "commit clause 0, out" in
      seq
        [ RaQL2DIL.update_state_for_expr ~r_env ~d_env ~what e ;
          RaQL2DIL.expression ~r_env ~d_env e |>
          DC.conv ~to_:to_typ d_env ]) |>
  comment cmt

let commit_when_clause ~r_env ~d_env in_type minimal_type out_prev_type
                       global_state_type group_state_type e =
  let cmt =
    Printf.sprintf2 "The bulk of the commit condition: %a"
      (E.print false) e in
  let open DE.Ops in
  (* input tuple -> out nullable -> local state -> global staye ->
   * out previous -> bool *)
  DE.func5 ~l:d_env in_type out_prev_type
           group_state_type global_state_type
           minimal_type
    (fun d_env in_ out_previous group_state global_state min ->
      let r_env =
        (RecordValue In, in_) ::
        (RecordValue OutPrevious, out_previous) ::
        (RecordValue GroupState, group_state) ::
        (RecordValue GlobalState, global_state) ::
        (RecordValue Out, min) :: r_env in
      let what = "commit clause" in
      seq
        [ RaQL2DIL.update_state_for_expr ~r_env ~d_env ~what e ;
          RaQL2DIL.expression ~r_env ~d_env e ]) |>
  comment cmt

(* Build a dummy functio  of the desired type: *)
let dummy_function ins out =
  DE.func ~l:DE.no_env ins (fun _l _fid ->
    DE.default_mn ~allow_null:true out)

(* When there is no way to extract a numeric value to order group for
 * optimising the commit condition, pass those placeholder functions to
 * [CodeGenLib_Skeleton.aggregate]: *)
let default_commit_cond commit_cond in_type minimal_type
                        out_prev_type group_state_type global_state_type =
  (* We are free to pick whatever type for group_order_type: *)
  let group_order_type = DT.void in
  let dummy_cond0_left_op =
    dummy_function [| in_type ; global_state_type |] group_order_type
  and dummy_cond0_right_op =
    dummy_function [| minimal_type ; out_prev_type ; group_state_type ;
                      global_state_type |] group_order_type
  and dummy_cond0_cmp =
    dummy_function [| group_order_type ; group_order_type |] DT.(required (Base I8)) in
  false,
  dummy_cond0_left_op,
  dummy_cond0_right_op,
  dummy_cond0_cmp,
  false,
  commit_cond

(* Returns the set of functions/flags required for
 * [CodeGenLib_Skeleton.aggregate] to process the commit condition, trying
 * to optimize by splitting the condition in two, with one sortable part: *)
let optimize_commit_cond ~r_env ~d_env func_name in_type minimal_type out_prev_type
                         group_state_type global_state_type commit_cond =
  let es = E.as_nary E.And commit_cond in
  (* TODO: take the best possible sub-condition not the first one: *)
  let rec loop rest = function
    | [] ->
        !logger.warning "Cannot find a way to optimise the commit \
                         condition of function %a"
          N.func_print func_name ;
        default_commit_cond commit_cond in_type minimal_type
                            out_prev_type group_state_type global_state_type
    | e :: es ->
        (match Helpers.defined_order e with
        | exception Not_found ->
            !logger.debug "Expression %a does not define an ordering"
              (E.print false) e ;
            loop (e :: rest) es
        | f, neg, op, g ->
            !logger.debug "Expression %a defines an ordering"
              (E.print false) e ;
            let true_when_eq = op = Ge in
            (* The type of the values used to sort the commit condition: *)
            let group_order_type = T.large_enough_for f.typ.typ g.typ.typ in
            let may_neg e =
              (* Let's add an unary minus in front of [ e ] it we are supposed
               * to neg the Greater operator, and type it by hand: *)
              if neg then
                E.make ~typ:e.E.typ.DT.typ ~nullable:e.typ.DT.nullable
                       ?units:e.units (Stateless (SL1 (Minus, e)))
              else e in
            let cond0_cmp =
              cmp_for group_order_type f.typ.DT.nullable g.typ.DT.nullable in
            let cond0_in =
              emit_cond0_in ~r_env ~d_env ~to_typ:group_order_type
                            in_type global_state_type (may_neg f) in
            let cond0_out =
              emit_cond0_out ~r_env ~d_env ~to_typ:group_order_type
                             minimal_type out_prev_type global_state_type
                             group_state_type (may_neg g) in
            let rem_cond =
              E.of_nary ~typ:commit_cond.typ.typ
                        ~nullable:commit_cond.typ.DT.nullable
                        ~units:commit_cond.units
                        E.And (List.rev_append rest es) in
            true, cond0_in, cond0_out, cond0_cmp, true_when_eq, rem_cond) in
  loop [] es

(* Similar to emit_field_selection but with less options, no concept of star and no
 * naming of the fields as the fields from out, since that's not the out tuple
 * we are constructing: *)
let key_of_input ~r_env ~d_env in_type key =
  let cmt =
    Printf.sprintf2 "The group-by key: %a"
      (List.print (E.print false)) key in
  let open DE.Ops in
  DE.func1 ~l:d_env in_type (fun d_env in_ ->
    (* Add in_ in the environment: *)
    let r_env = (RecordValue In, in_) :: r_env in
    make_tup (List.map (RaQL2DIL.expression ~r_env ~d_env) key)) |>
  comment cmt

(* The vectors OutPrevious is nullable: the commit when and
 * select clauses of aggregate operations either have it or not.
 * Each time they need access to a field they call a function "maybe_XXX_"
 * with that nullable tuple, which avoids propagating out_typ down to
 * emit_expr - but hopefully the compiler will inline this. *)
let maybe_field out_type field_name field_type =
  let cmt =
    Printf.sprintf2 "Extract field of %a (type %a) from optional prev-tuple"
      N.field_print field_name
      DT.print_mn field_type in
  let nullable_out_type = DT.{ out_type with nullable = true } in
  let open DE.Ops in
  DE.func1 ~l:DE.no_env nullable_out_type (fun _l prev_out ->
    if_null prev_out
      ~then_:(null field_type.DT.typ)
      ~else_:(
        let field_val =
          get_field (field_name :> string) (force prev_out) in
        if field_type.DT.nullable then
          (* Return the field as is: *)
          field_val
        else
          (* Make it nullable: *)
          not_null field_val)) |>
  comment cmt

let fold_fields mn i f =
  match mn.DT.typ with
  | DT.Rec mns ->
      Array.fold_left (fun i (field_name, field_type) ->
        f i (N.field field_name) field_type
      ) i mns
  | _ ->
      !logger.warning "Type %a has no fields!"
        DT.print_mn mn ;
      i

(* If [build_minimal] is true, the env is updated and only those fields present
 * in minimal tuple are build (only those required by commit_cond and
 * update_states), and the function outputs a minimal_out record. If false, the
 * minimal tuple computed above is passed as an extra parameter to the
 * function, which only have to build the final out_tuple (taking advantage of
 * the fields already computed in minimal_type), and need not update states.
 * Notice that there are no notion of deep selection at this point, as input
 * fields have been flattened by now. *)
let select_record ~r_env ~d_env ~build_minimal min_fields out_fields in_type
                  minimal_type out_prev_type
                  global_state_type group_state_type =
  let open Raql_select_field.DessserGen in
  let field_in_minimal field_name =
    Array.exists (fun (n, _t) ->
      n = (field_name : N.field :> string)
    ) min_fields in
  let must_output_field field_name =
    not build_minimal || field_in_minimal field_name in
  let open DE.Ops in
  let args =
    if build_minimal then
      DT.[| in_type ; out_prev_type ; group_state_type ;
            global_state_type |]
    else
      DT.[| in_type ; out_prev_type ; group_state_type ;
            global_state_type ; minimal_type |]
    in
  let cmt =
    Printf.sprintf2 "output an out_tuple of type %a"
      (List.print (fun oc sf -> N.field_print oc sf.alias)) out_fields in
  DE.func ~l:d_env args (fun d_env fid ->
    let group_state = param fid 2
    and global_state = param fid 3 in
    let r_env =
      (RecordValue In, param fid 0) ::
      (RecordValue OutPrevious, param fid 1) ::
      (RecordValue GroupState, group_state) ::
      (RecordValue GlobalState, global_state) :: r_env in
    let r_env =
      if not build_minimal then
        (RecordValue Out, param fid 4) :: r_env
      else
        r_env in
    (* Bind each expression to a variable in the order of the select clause
     * (aka. user order) so that previously bound variables can be used in
     * the following expressions.
     * Those identifiers are named after the fields themselves, but must not
     * shadow the passed function parameters ("in_", "global_", etc) so they are
     * transformed by `id_of_field_name]: *)
    let id_of_field_name f =
      "id_"^ DessserBackEndOCaml.valid_identifier
              (f : N.field :> string) ^"_" in
    let rec loop r_env d_env rec_args = function
      | [] ->
          (* Once all the values are bound to identifiers, build the record: *)
          (* Note: field order does not matter for a record *)
          make_rec rec_args
      | sf :: out_fields ->
          if must_output_field sf.alias then (
            let updater =
              if build_minimal then (
                (* Update the states as required for this field, just before
                 * computing the field actual value. *)
                let what = (sf.alias :> string) in
                RaQL2DIL.update_state_for_expr ~r_env ~d_env ~what sf.expr
              ) else nop in
            !logger.debug "Updater for field %a:%s"
              N.field_print sf.alias
              (DE.to_pretty_string ?max_depth:None updater) ;
            let id_name = id_of_field_name sf.alias in
            let cmt =
              Printf.sprintf2 "Output field %a of type %a"
                N.field_print sf.alias
                DT.print_mn sf.expr.typ in
            let value =
              if not build_minimal && field_in_minimal sf.alias then (
                (* We already have this binding in the parameter: *)
                get_field (sf.alias :> string) (param fid 4 (* minimal *))
              ) else (
                (* So that we have a single out_type both before and after tuples
                 * generation: *)
                if E.is_generator sf.expr then nop
                else RaQL2DIL.expression ~r_env ~d_env sf.expr
              ) in
            !logger.debug "Expression for field %a:%s (of type %a)"
              N.field_print sf.alias
              (DE.to_pretty_string ?max_depth:None value)
              DT.print_mn (DE.type_of d_env value) ;
            seq [ updater ;
                  let_ ~l:d_env ~name:id_name value (fun d_env id ->
                    (* Beware that [let_] might optimise away the actual
                     * binding so better remember that [id]: *)
                    let rec_args = ((sf.alias :> string), id) :: rec_args in
                    (* Also install an override for this field so that if
                     * out.$this_field is referenced in what follows the we
                     * will read [id] instead. *)
                    let r_env = (RecordField (Out, sf.alias), id) :: r_env in
                    loop r_env d_env rec_args out_fields) ] |>
            comment cmt
          ) else (
            (* This field is not part of minimal_out, but we want minimal out
             * to have as many fields as out_type, with units as placeholders
             * for missing fields.
             * Note: the exact same type than minimal_out must be output, ie.
             * same field name for the placeholder and unit type. *)
            let cmt =
              Printf.sprintf2 "Placeholder for field %a"
                N.field_print sf.alias in
            let fname = Helpers.not_minimal_field_name sf.alias in
            let rec_args = ((fname :> string), void) :: rec_args in
            loop r_env d_env rec_args out_fields |>
            comment cmt
          ) in
    loop r_env d_env [] out_fields) |>
  comment cmt

let select_clause ~r_env ~d_env ~build_minimal out_fields in_type minimal_type
                  out_type out_prev_type global_state_type group_state_type =
  let open DE.Ops in
  let cmt =
    Printf.sprintf2 "Build the %s tuple of type %a"
      (if build_minimal then "minimal" else "output")
      DT.print_mn
        (if build_minimal then minimal_type else out_type) in
  (* TODO: non record types for I/O: *)
  (match minimal_type.DT.typ, out_type.typ with
  | DT.Rec min_fields, DT.Rec _ ->
      select_record ~r_env ~d_env ~build_minimal min_fields out_fields in_type
                    minimal_type out_prev_type
                    global_state_type group_state_type
  | _ ->
      todo "select_clause for non-record types") |>
  comment cmt

(* Fields that are part of the minimal tuple have had their states updated
 * while the minimal tuple was computed, but others have not. Let's do this
 * here: *)
let update_states ~r_env ~d_env in_type minimal_type out_prev_type
                  group_state_type global_state_type out_fields =
  let open Raql_select_field.DessserGen in
  let field_in_minimal field_name =
    match minimal_type.DT.typ with
    | DT.Void ->
        false
    | Rec mns ->
        Array.exists (fun (n, _) -> n = (field_name : N.field :> string)) mns
    | _ ->
        assert false
  in
  let open DE.Ops in
  let cmt =
    Printf.sprintf2 "Updating the state of fields not in the minimal tuple (%a)"
      DT.print_mn minimal_type in
  let l = d_env (* TODO *) in
  DE.func5 ~l in_type out_prev_type group_state_type
           global_state_type minimal_type
    (fun d_env in_ out_previous group_state global_state min_ ->
      let r_env =
        (RecordValue In, in_) ::
        (RecordValue OutPrevious, out_previous) ::
        (RecordValue GroupState, group_state) ::
        (RecordValue GlobalState, global_state) ::
        (RecordValue Out, min_) :: r_env in
      List.fold_left (fun l sf ->
        if field_in_minimal sf.alias then l else (
          (* Update the states as required for this field, just before
           * computing the field actual value. *)
          let what = (sf.alias :> string) in
          RaQL2DIL.update_state_for_expr ~r_env ~d_env ~what sf.expr :: l)
      ) [] out_fields |>
      List.rev |>
      seq) |>
  comment cmt

let id_of_prefix tuple =
  String.nreplace (Variable.to_string tuple) "." "_"

(* Returns a DIL pair consisting of the start and end time (as floats)
 * of a given output tuple *)
let event_time ~r_env ~d_env et out_type params =
  let open DE.Ops in
  let expr_of_field_name ~tuple field_name =
    let field =
      match (field_name : N.field :> string) with
      (* Note: we have a '#count' for the sort tuple. *)
      | "#count" -> todo "#count" (* "virtual_"^ id_of_prefix tuple ^"_count_" *)
      | _ -> field_name in
    try List.assoc (RecordField (tuple, field)) r_env
    with Not_found ->
      (* If not, that means this field has not been overridden but we may
       * still find the record it's from and pretend we have a Get from
       * the Variable instead: *)
      (match List.assoc (RecordValue tuple) r_env with
      | exception Not_found ->
          Printf.sprintf2 "Cannot find RecordField %a.%a in environment (%a)"
            Variable.print tuple
            N.field_print field
            RaQL2DIL.print_r_env r_env |>
          failwith
      | binding ->
          get_field (field :> string) binding) in
  let (sta_field, sta_src, sta_scale), dur = et in
  let open RamenEventTime in
  let open Event_time_field.DessserGen in
  let open DE.Ops in
  let default_zero t e =
    if t.DT.nullable then
      if_null e ~then_:(float 0.) ~else_:e
    else
      e in
  let field_value_to_float d_env field_name = function
    | OutputField ->
        (* This must not fail if RamenOperation.check did its job *)
        (match out_type.DT.typ with
        | DT.Rec mns ->
            let f = array_assoc (field_name : N.field :> string) mns in
            let e =
              DC.conv_mn
                ~to_:DT.(required (Base Float)) d_env
                (expr_of_field_name ~tuple:Out field_name) in
            default_zero f e
        | _ ->
            assert false) (* Event time output field only usable on records *)
    | Parameter ->
        let param = RamenTuple.params_find field_name params in
        let e =
          DC.conv ~to_:(Base Float) d_env
                  (expr_of_field_name ~tuple:Param field_name) in
        default_zero param.ptyp.typ e
  in
  let_ ~name:"start_" ~l:d_env
       (mul (field_value_to_float d_env sta_field sta_src)
            (float sta_scale))
    (fun l start ->
      let stop =
        match dur with
        | DurationConst d ->
            add start (float d)
        | DurationField (dur_field, dur_src, dur_scale) ->
            add start
                (mul (field_value_to_float l dur_field dur_src)
                     (float dur_scale))
        | StopField (sto_field, sto_src, sto_scale) ->
            mul (field_value_to_float l sto_field sto_src)
                (float sto_scale) in
      make_pair start stop)

(* Returns a DIL function returning the optional start and end times of a
 * given output tuple *)
let time_of_tuple ~r_env ~d_env et_opt out_type params =
  let open DE.Ops in
  DE.func1 ~l:d_env out_type (fun d_env tuple ->
    let r_env = (RecordValue Out, tuple) :: r_env in
    match et_opt with
    | None ->
        seq [ ignore_ tuple ; null (Ext "float_pair") ]
    | Some et ->
        let sta_sto = event_time ~r_env ~d_env et out_type params in
        DE.with_sploded_pair "start_stop" ~l:d_env sta_sto (fun _d_env sta sto ->
          not_null (
            apply (ext_identifier "CodeGenLib_Dessser.make_float_pair")
                  [ sta ; sto ])))

(* The sort_expr functions take as parameters the number of entries sorted, the
 * first entry, the last, the smallest and the largest, and compute a value
 * used to sort incoming entries, either a boolean (for sort_until) or any
 * value that can be mapped to numerics (for sort_by). *)
let sort_expr ~r_env ~d_env in_t es =
  let open DE.Ops in
  let cmt = "Sort helper" in
  let l = d_env (* TODO *) in
  DE.func5 ~l DT.u64 in_t in_t in_t in_t
           (fun d_env _count first last smallest greatest ->
    (* TODO: count *)
    let r_env =
      (RecordValue SortFirst, first) ::
      (RecordValue In, last) ::
      (RecordValue SortSmallest, smallest) ::
      (RecordValue SortGreatest, greatest) :: r_env in
    match es with
    | [] ->
        (* The default sort_until clause must be false.
         * If there is no sort_by clause, any constant will do: *)
        false_
    | es ->
        (* Output a tuple made of all the expressions: *)
        make_tup (List.map (RaQL2DIL.expression ~r_env ~d_env) es)) |>
  comment cmt

(* Returns an expression that convert an OCaml value into a RamenTypes.value of
 * the given RamenTypes.t. This is useful for instance to get hand off the
 * factors to CodeGenLib. [v] is the DIL expression to get the runtime value. *)
(* TODO: Move this function into RamenValue aka RamenTypes *)
let rec raql_of_dil_value ~d_env mn v =
  let open DE.Ops in
  let_ ~name:"v" ~l:d_env v (fun d_env v ->
    if mn.DT.nullable then
      if_null v
        ~then_:(ext_identifier "Raql_value.VNull")
        ~else_:(
          let mn' = DT.{ mn with nullable = false } in
          raql_of_dil_value ~d_env mn' (force v))
    else
      (* As far as Dessser's OCaml backend is concerned, constructor are like
       * functions: *)
      let p f = apply (ext_identifier ("Raql_value."^ f)) [ v ] in
      match mn.DT.typ with
      | Void -> ext_identifier "Raql_value.VUnit"
      | Base Float -> p "VFloat"
      | Base String -> p "VString"
      | Base Bool -> p "VBool"
      | Base Char -> p "VChar"
      | Base U8 -> p "VU8"
      | Base U16 -> p "VU16"
      | Base U24 -> p "VU24"
      | Base U32 -> p "VU32"
      | Base U40 -> p "VU40"
      | Base U48 -> p "VU48"
      | Base U56 -> p "VU56"
      | Base U64 -> p "VU64"
      | Base U128 -> p "VU128"
      | Base I8 -> p "VI8"
      | Base I16 -> p "VI16"
      | Base I24 -> p "VI24"
      | Base I32 -> p "VI32"
      | Base I40 -> p "VI40"
      | Base I48 -> p "VI48"
      | Base I56 -> p "VI56"
      | Base I64 -> p "VI64"
      | Base I128 -> p "VI128"
      | Usr { name = "Eth" ; _ } -> p "VEth"
      | Usr { name = "Ip4" ; _ } -> p "VIpv4"
      | Usr { name = "Ip6" ; _ } -> p "VIpv6"
      | Usr { name = "Ip" ; _ } ->
          apply (ext_identifier "Raql_value.VIp") [
            if_ (eq (u16_of_int 0) (label_of v))
              ~then_:(apply (ext_identifier "RamenIp.make_v4") [ get_alt "v4" v ])
              ~else_:(apply (ext_identifier "RamenIp.make_v6") [ get_alt "v6" v ]) ]
      | Usr { name = "Cidr4" ; _ } ->
          apply (ext_identifier "Raql_value.VCidrv4") [
            make_pair (get_field "ip" v) (get_field "mask" v) ]
      | Usr { name = "Cidr6" ; _ } ->
          apply (ext_identifier "Raql_value.VCidrv6") [
            make_pair (get_field "ip" v) (get_field "mask" v) ]
      | Usr { name = "Cidr" ; _ } ->
          apply (ext_identifier "Raql_value.VCidr") [
            if_ (eq (u16_of_int 0) (label_of v))
              ~then_:(apply (ext_identifier "RamenIp.Cidr.make_v4")
                            [ get_field "ip" (get_alt "v4" v) ;
                              get_field "mask" (get_alt "v4" v) ])
              ~else_:(apply (ext_identifier "RamenIp.Cidr.make_v6")
                            [ get_field "ip" (get_alt "v6" v) ;
                              get_field "mask" (get_alt "v6" v) ]) ]
      | Usr { def ; name } ->
          let d =
            raql_of_dil_value ~d_env DT.(required (develop def)) v in
          make_usr name [ d ]
      | Tup mns ->
          apply (ext_identifier "RamenTypes.make_vtup") ( (* TODO as well it seams *)
            Array.mapi (fun i mn ->
              raql_of_dil_value ~d_env mn (get_item i v)
            ) mns |> Array.to_list)
      | Rec _
      | Vec _
      | Arr _ ->
          todo "raql_of_dil_value for rec/vec/arr"
      | Map _ -> assert false (* No values of that type *)
      | Sum _ -> invalid_arg "raql_of_dil_value for Sum type"
      | Set _ -> assert false (* No values of that type *)
      | _ -> assert false)

(* Returns a DIL function that returns a Arr of [factor_value]s *)
let factors_of_tuple func_op out_type =
  let cmt = "Extract factors from the output tuple" in
  let typ = O.out_type_of_operation ~with_priv:false func_op in
  let factors = O.factors_of_operation func_op in
  let open DE.Ops in
  (* Note: we need no environment at the start of [factors_of_tuple] *)
  DE.func1 ~l:DE.no_env out_type (fun d_env v_out ->
    List.map (fun factor ->
      let t = (List.find (fun t -> t.RamenTuple.name = factor) typ).typ in
      apply (ext_identifier "CodeGenLib_Dessser.make_factor_value")
            [ string (factor :> string) ;
              raql_of_dil_value ~d_env t (get_field (factor :> string) v_out) ]
    ) factors |>
    make_arr DT.(required (Ext "factor_value"))) |>
  comment cmt

let print_path oc path =
  List.print (Tuple2.print DT.print_mn Int.print) oc path

let print_path2 oc path =
  List.print (fun oc (mn, u) ->
    Printf.fprintf oc "(%a, %s)"
      DT.print_mn mn
      (Uint32.to_string u)) oc path

(* Return the expression reaching path [path] in heap value [v]: *)
let rec extractor path ~d_env v =
  let open DE.Ops in
  match path with
  | (mn, u) :: [] when u = Uint32.zero ->
      raql_of_dil_value ~d_env mn v
  | (DT.{ typ = Vec _ ; nullable = false }, i) :: rest ->
      let v = nth (u32 i) v in
      extractor rest ~d_env v
  | (DT.{ typ = Tup _ ; nullable = false }, i) :: rest ->
      let v = get_item (Uint32.to_int i) v in
      extractor rest ~d_env v
  | (DT.{ typ = Rec mns ; nullable = false }, i) :: rest ->
      let v = get_field (fst mns.(Uint32.to_int i)) v in
      extractor rest ~d_env v
  | (DT.{ nullable = true ; typ }, i) :: rest ->
      if_null v
        ~then_:(ext_identifier "Raql_value.VNull")
        ~else_:(
          let path = (DT.{ nullable = false ; typ }, i) :: rest in
          extractor path ~d_env (force v))
  | _ ->
      !logger.error "Cannot build extractor for path %a"
        print_path2 path ;
      assert false

let extractor_t out_type =
  DT.(func [| out_type |] (required (Ext "ramen_value")))

let scalar_extractors out_type =
  let open DE.Ops in
  let cmt = "Extract scalar values from the output tuple" in
  let extractors = ref (eol (extractor_t out_type)) in
  O.iter_scalars_with_path out_type (fun i path ->
    let cmt =
      Printf.sprintf2 "extractor #%d for path %a" i print_path2 path in
    let f =
      DE.func1 ~l:DE.no_env out_type (fun d_env v_out ->
        extractor path ~d_env v_out) |>
      comment cmt in
    !logger.debug "Extractor for scalar %a:%s"
      print_path2 path
      (DE.to_pretty_string ?max_depth:None f) ;
    extractors := cons f !extractors) ;
  apply (ext_identifier "CodeGenLib_Dessser.make_extractors_vector")
        [ !extractors ] |>
  comment cmt

(* Generate a function that, given the out tuples, will return the list of
 * notification names to send, along with all output values as strings: *)
(* TODO: shouldn't CodeGenLib pass this func the global and also maybe
 * the group states? *)
let get_notifications ~r_env ~d_env out_type es =
  let open DE.Ops in
  let cmt = "List of notifications" in
  let string_t = DT.(required (Base String)) in
  let string_pair_t = DT.(required (Ext "string_pair")) in
  DE.func1 ~l:d_env out_type (fun d_env v_out ->
    let r_env = (RecordValue Out, v_out) :: r_env in
    if es = [] then
      make_pair (make_arr string_t []) (make_arr string_pair_t [])
    else
      let names =
        make_arr string_t (List.map (RaQL2DIL.expression ~r_env ~d_env) es) in
      let values =
        T.map_fields (fun n _ ->
          apply (ext_identifier "CodeGenLib_Dessser.make_string_pair")
            [ string n ;
              DC.conv_mn ~to_:string_t d_env
                         (get_field n v_out) ]
        ) out_type.DT.typ |>
        Array.to_list |>
        make_arr string_pair_t in
      make_pair names values) |>
  comment cmt

let call_aggregate compunit id_name sort key commit_before flush_how
                   check_commit_for_all =
  let f_name = "CodeGenLib_Skeletons.aggregate" in
  let compunit =
    let l = DU.environment compunit in
    let aggregate_t =
      let open DE.Ops in
      DT.func [|
        DE.type_of l (identifier "read_in_tuple_") ;
        DE.type_of l (identifier "sersize_of_tuple_") ;
        DE.type_of l (identifier "time_of_tuple_") ;
        DE.type_of l (identifier "factors_of_tuple_") ;
        DE.type_of l (identifier "scalar_extractors_") ;
        DE.type_of l (identifier "serialize_tuple_") ;
        DE.type_of l (identifier "generate_tuples_") ;
        DE.type_of l (identifier "minimal_tuple_of_group_") ;
        DE.type_of l (identifier "update_states_") ;
        DE.type_of l (identifier "out_tuple_of_minimal_tuple_") ;
        DT.u32 ;
        DE.type_of l (identifier "sort_until_") ;
        DE.type_of l (identifier "sort_by_") ;
        DE.type_of l (identifier "where_fast_") ;
        DE.type_of l (identifier "where_slow_") ;
        DE.type_of l (identifier "key_of_input_") ;
        DT.bool ;
        DE.type_of l (identifier "commit_cond_") ;
        DE.type_of l (identifier "commit_has_commit_cond_") ;
        DE.type_of l (identifier "commit_cond0_left_op_") ;
        DE.type_of l (identifier "commit_cond0_right_op_") ;
        DE.type_of l (identifier "commit_cond0_cmp_") ;
        DE.type_of l (identifier "commit_cond0_true_when_eq_") ;
        DT.bool ;
        DT.bool ;
        DT.bool ;
        DE.type_of l (identifier "global_init_") ;
        DE.type_of l (identifier "group_init_") ;
        DE.type_of l (identifier "get_notifications_") ;
        DE.type_of l (identifier "every_") ;
        DE.type_of l (identifier "default_in_") ;
        DE.type_of l (identifier "default_out_") ;
        DE.type_of l (identifier "orc_make_handler_") ;
        DE.type_of l (ext_identifier "orc_write") ;
        DE.type_of l (ext_identifier "orc_close") |]
      DT.void in
    DU.add_external_identifier compunit f_name aggregate_t in
  let cmt = "Entry point for aggregate full worker" in
  let open DE.Ops in
  let e =
    DE.func0 ~l:DE.no_env (fun _l ->
      apply (ext_identifier f_name) [
        identifier "read_in_tuple_" ;
        identifier "sersize_of_tuple_" ;
        identifier "time_of_tuple_" ;
        identifier "factors_of_tuple_" ;
        identifier "scalar_extractors_" ;
        identifier "serialize_tuple_" ;
        identifier "generate_tuples_" ;
        identifier "minimal_tuple_of_group_" ;
        identifier "update_states_" ;
        identifier "out_tuple_of_minimal_tuple_" ;
        u32 (match sort with None -> Uint32.zero | Some (n, _, _) -> n) ;
        identifier "sort_until_" ;
        identifier "sort_by_" ;
        identifier "where_fast_" ;
        identifier "where_slow_" ;
        identifier "key_of_input_" ;
        bool (key = []) ;
        identifier "commit_cond_" ;
        identifier "commit_has_commit_cond_" ;
        identifier "commit_cond0_left_op_" ;
        identifier "commit_cond0_right_op_" ;
        identifier "commit_cond0_cmp_" ;
        identifier "commit_cond0_true_when_eq_" ;
        bool commit_before ;
        bool (flush_how <> Raql_flush_method.DessserGen.Never) ;
        bool check_commit_for_all ;
        identifier "global_init_" ;
        identifier "group_init_" ;
        identifier "get_notifications_" ;
        identifier "every_" ;
        identifier "default_in_" ;
        identifier "default_out_" ;
        identifier "orc_make_handler_" ;
        ext_identifier "orc_write" ;
        ext_identifier "orc_close" ]) |>
    comment cmt in
  let compunit, _, _ =
    DU.add_identifier_of_expression compunit ~name:id_name e in
  compunit

let where_top ~r_env ~d_env where_fast in_type =
  (* The filter used by the top-half must be a partition of the normal
   * where_fast filter selecting only the part that use only pure
   * functions and no previous out tuple.
   * A partition of a filter is the separation of the ANDed clauses of a
   * filter according a any criteria on expressions (first part being the
   * part of the condition which all expressions fulfill the condition).
   * We could then reuse filter partitioning to optimise the filtering in
   * the normal case by moving part of the where into the where_fast,
   * before here we use partitioning again to extract the top-half
   * version of the where_fast.
   * Note that the tuples surviving the top-half filter will again be
   * filtered against the full fast_filter. *)
  let expr_needs_global_tuples =
    Helpers.expr_needs_tuple_from
      [ OutPrevious; SortFirst; SortSmallest; SortGreatest ] in
  let where, _ =
    E.and_partition (fun e ->
      E.is_pure e && not (expr_needs_global_tuples e)
    ) where_fast in
  let open DE.Ops in
  DE.func1 ~l:d_env in_type (fun d_env in_ ->
    let r_env = (RecordValue In, in_) :: r_env in
    RaQL2DIL.expression ~r_env ~d_env where) |>
  comment "where clause for top-half"

let call_top_half compunit id_name =
  let f_name = "CodeGenLib_Skeletons.top_half" in
  let compunit =
    let l = DU.environment compunit in
    let top_half_t =
      let open DE.Ops in
      DT.func [|
        DE.type_of l (identifier "read_in_tuple_") ;
        DE.type_of l (identifier "top_where_") |]
      DT.void in
    DU.add_external_identifier compunit f_name top_half_t in
  let cmt = "Entry point for aggregate top-half worker" in
  let open DE.Ops in
  let e =
    DE.func1 ~l:DE.no_env DT.void (fun _l _unit ->
      apply (ext_identifier f_name) [
        identifier "read_in_tuple_" ;
        identifier "top_where_" ]) |>
      comment cmt in
  let compunit, _, _ =
    DU.add_identifier_of_expression compunit ~name:id_name e in
  compunit

(* Output the code required for the function operation and returns the new
 * code.
 * Named [emit_full_operation] in reference to [emit_half_operation] for half
 * workers. *)
let emit_aggregate ~r_env compunit func_op func_name in_type params =
  let out_type = O.out_record_of_operation ~with_priv:true func_op in
  let add_expr compunit name d =
    !logger.debug "%s:%s" name (DE.to_pretty_string ?max_depth:None d) ;
    let compunit, _, _ = DU.add_identifier_of_expression compunit ~name d in
    compunit in
  (* Gather the globals that have already been declared (envs, params and
   * globals): *)
  let d_env = DU.environment compunit in
  (* The input type (computed from parent output type and the deep selection
   * of field), aka `tuple_in in CodeGenLib_Skeleton: *)
  let in_type = dessser_type_of_ramen_tuple in_type in
  (* That part of the output value that needs to be computed for every input
   * even when no output is emitted, aka `minimal_out: *)
  let minimal_type = Helpers.minimal_type func_op |>
                     dessser_type_of_ramen_tuple in
  let global_stateful_exprs, group_stateful_exprs =
    Helpers.stateful_expressions func_op in
  (* The tuple storing the global state, aka `global_state: *)
  let global_state_type =
    RaQL2DIL.state_rec_type_of_expressions ~r_env ~d_env global_stateful_exprs
  (* The tuple storing the group local state, aka `group_state: *)
  and group_state_type =
    RaQL2DIL.state_rec_type_of_expressions ~r_env ~d_env group_stateful_exprs in
  (* The output type of values passed to the final output generator: *)
  let generator_out_type = out_type in (* TODO *)
  (* Same, nullable: *)
  let out_prev_type = DT.{ generator_out_type with nullable = true } in
  (* Extract required info from the operation definition: *)
  let where, commit_before, commit_cond, key, out_fields, sort, flush_how,
      notifications, every =
    match func_op with
    | O.Aggregate { where ; commit_before ; commit_cond ; key ; sort ;
                    aggregate_fields ; flush_how ; notifications ; every ; _ } ->
        where, commit_before, commit_cond, key, aggregate_fields, sort,
        flush_how, notifications, every
    | _ -> assert false in
  (* The worker will have a global state and a local one per group, stored in
   * its snapshotted state. The first functions needed are those that create
   * the initial state for the global and local states. *)
  let compunit =
    fail_with_context "coding for global state initializer" (fun () ->
      state_init ~r_env ~d_env E.GlobalState global_state_type
                 where commit_cond out_fields |>
      add_expr compunit "global_init_") in
  let compunit =
    fail_with_context "coding for group state initializer" (fun () ->
      state_init ~r_env ~d_env E.LocalState global_state_type
                 where commit_cond out_fields |>
      add_expr compunit "group_init_") in
  (* When filtering, the worker has two options:
   * It can check an incoming tuple as soon as it receives it, or it can
   * first compute the group key and retrieve the group state, and then
   * check the tuple. The later, slower option is required when the WHERE
   * expression uses anything from the group state (such as local function
   * states or group tuple).
   * It is best to partition the WHERE expression in two so that as much of
   * it can be checked as early as possible. *)
  let where_fast, where_slow =
    E.and_partition (not % CodeGen_OCaml.expr_needs_group) where in
  let d_env = DU.environment compunit in
  let compunit =
    fail_with_context "coding for tuple reader" (fun () ->
      let compunit, e = deserialize_tuple in_type compunit in
      add_expr compunit "read_in_tuple_" e) in
  let compunit =
    fail_with_context "coding for where-fast function" (fun () ->
      where_clause ~r_env ~d_env in_type out_prev_type global_state_type
                   group_state_type where_fast |>
      add_expr compunit "where_fast_") in
  let compunit =
    fail_with_context "coding for where-slow function" (fun () ->
      where_clause ~r_env ~d_env ~with_group:true in_type out_prev_type
                   global_state_type group_state_type where_slow |>
      add_expr compunit "where_slow_") in
  let check_commit_for_all = Helpers.check_commit_for_all commit_cond in
  let has_commit_cond, cond0_left_op, cond0_right_op, cond0_cmp,
      cond0_true_when_eq, commit_cond_rest =
    if check_commit_for_all then
      fail_with_context "coding for optimized commit condition" (fun () ->
        optimize_commit_cond ~r_env ~d_env func_name in_type
                             minimal_type out_prev_type group_state_type
                             global_state_type commit_cond)
    else
      (* No need to optimize: *)
      default_commit_cond commit_cond in_type minimal_type
                          out_prev_type group_state_type global_state_type in
  let compunit =
    add_expr compunit "commit_has_commit_cond_" (DE.Ops.bool has_commit_cond) in
  let compunit =
    add_expr compunit "commit_cond0_left_op_" cond0_left_op in
  let compunit =
    add_expr compunit "commit_cond0_right_op_" cond0_right_op in
  let compunit =
    add_expr compunit "commit_cond0_cmp_" cond0_cmp in
  let compunit =
    add_expr compunit "commit_cond0_true_when_eq_"
             (DE.Ops.bool cond0_true_when_eq) in
  let d_env = DU.environment compunit in
  let compunit =
    fail_with_context "coding for commit condition function" (fun () ->
      commit_when_clause
        ~r_env ~d_env in_type minimal_type out_prev_type
        global_state_type group_state_type commit_cond_rest |>
      add_expr compunit "commit_cond_") in
  let compunit =
    fail_with_context "coding for key extraction function" (fun () ->
      key_of_input ~r_env ~d_env in_type key |>
      add_expr compunit "key_of_input_") in
  let compunit =
    fail_with_context "coding for optional-field getter functions" (fun () ->
      fold_fields out_type compunit (fun compunit field_name field_type ->
        let fun_name = "maybe_"^ (field_name : N.field :> string) ^"_" in
        maybe_field out_type field_name field_type |>
        add_expr compunit fun_name)) in
  let d_env = DU.environment compunit in
  let compunit =
    fail_with_context "coding for select-clause function" (fun () ->
      select_clause ~r_env ~d_env ~build_minimal:true out_fields
                    in_type minimal_type out_type out_prev_type
                    global_state_type group_state_type |>
      add_expr compunit "minimal_tuple_of_group_") in
  let compunit =
    fail_with_context "coding for output function" (fun () ->
      select_clause ~r_env ~d_env ~build_minimal:false out_fields
                    in_type minimal_type out_type out_prev_type
                    global_state_type group_state_type |>
      add_expr compunit "out_tuple_of_minimal_tuple_") in
  let compunit =
    fail_with_context "coding for state update function" (fun () ->
      update_states ~r_env ~d_env in_type minimal_type out_prev_type
                    group_state_type global_state_type out_fields |>
      add_expr compunit "update_states_") in
  let compunit =
    fail_with_context "coding for sersize-of-tuple function" (fun () ->
      let compunit, e = sersize_of_type out_type compunit in
      add_expr compunit "sersize_of_tuple_" e) in
  let compunit =
    fail_with_context "coding for time-of-tuple function" (fun () ->
      let et = O.event_time_of_operation func_op in
      time_of_tuple ~r_env ~d_env et out_type params |>
      add_expr compunit "time_of_tuple_") in
  let compunit =
    fail_with_context "coding for tuple serializer" (fun () ->
      let compunit, e = serialize out_type compunit in
      add_expr compunit "serialize_tuple_" e) in
  let compunit =
    fail_with_context "coding for tuple generator" (fun () ->
      generate_tuples in_type out_type out_fields |>
      add_expr compunit "generate_tuples_") in
  let compunit =
    fail_with_context "coding for sort-until function" (fun () ->
      sort_expr ~r_env ~d_env in_type
                (match sort with Some (_, Some u, _) -> [u] | _ -> []) |>
      add_expr compunit "sort_until_") in
  let compunit =
    fail_with_context "coding for sort-by function" (fun () ->
      sort_expr ~r_env ~d_env in_type
                (match sort with Some (_, _, b) -> b | None -> []) |>
      add_expr compunit "sort_by_") in
  let compunit =
    fail_with_context "coding for notification extraction function" (fun () ->
      get_notifications ~r_env ~d_env out_type notifications |>
      add_expr compunit "get_notifications_") in
  let compunit =
    fail_with_context "coding for default input tuples" (fun () ->
      DE.default_mn in_type |>
      add_expr compunit "default_in_") in
  let compunit =
    fail_with_context "coding for default output tuples" (fun () ->
      DE.default_mn out_type |>
      add_expr compunit "default_out_") in
  let compunit =
    fail_with_context "coding for the 'every' clause" (fun () ->
      (match every with
      | Some e ->
          RaQL2DIL.expression ~r_env ~d_env e |>
          DC.conv ~to_:DT.(Base Float) d_env
      | None ->
          DE.Ops.float 0.) |>
      add_expr compunit "every_") in
  let compunit =
    fail_with_context "coding for aggregate entry point" (fun () ->
      call_aggregate compunit EntryPoints.worker sort key commit_before
                     flush_how check_commit_for_all) in
  let compunit =
    fail_with_context "coding for top-where function" (fun () ->
      where_top ~r_env ~d_env where_fast in_type |>
      add_expr compunit "top_where_") in
  let compunit =
    fail_with_context "coding for top-half aggregate entry point" (fun () ->
      call_top_half compunit EntryPoints.top_half) in
  compunit

let unchecked_t = DT.void

(* Generate a data provider that reads blocks of bytes from a file: *)
let emit_read_file ~r_env compunit field_of_params func_name specs =
  let d_env = DU.environment compunit in
  let compunit, _, _ =
    fail_with_context "coding the unlink condition" (fun () ->
      RaQL2DIL.expression ~r_env ~d_env specs.O.unlink |>
      DU.add_identifier_of_expression compunit ~name:"unlink_") in
  let compunit, _, _ =
    fail_with_context "coding the file(s) name" (fun () ->
      RaQL2DIL.expression ~r_env ~d_env specs.fname |>
      DU.add_identifier_of_expression compunit ~name:"filename_") in
  let compunit, _, _ =
    fail_with_context "coding the preprocessor command" (fun () ->
      let d =
        match specs.preprocessor with
        | None -> DE.Ops.string ""
        | Some e -> RaQL2DIL.expression ~r_env ~d_env e in
      DU.add_identifier_of_expression compunit ~name:"preprocessor_" d) in
  let compunit =
    let dependencies =
      [ field_of_params ; "unlink_" ; "filename_" ; "preprocessor_" ]
    and backend = DessserMiscTypes.OCaml
    and typ = unchecked_t in
    DU.add_verbatim_definition
      compunit ~name:func_name ~dependencies ~typ ~backend (fun oc _ps ->
        let p fmt = emit oc 0 fmt in
        p "let %s =" func_name ;
        p "  let tuples_ = [ [ \"param\" ], %s ;" field_of_params ;
        p "                  [ \"env\" ], Sys.getenv ] in" ;
        p "  let preprocessor_ =" ;
        p "    RamenHelpers.subst_tuple_fields tuples_ preprocessor_ in" ;
        p "  CodeGenLib_IO.read_glob_file filename_ preprocessor_ unlink_\n\n") in
  compunit

let emit_read_kafka ~r_env compunit field_of_params func_name specs =
  let d_env = DU.environment compunit in
  let open DE.Ops in
  let topic_opts, consumer_opts =
    let topic_pref = kafka_topic_option_prefix in
    List.fold_left (fun (topic_opts, consumer_opts) (n, e) ->
      if String.starts_with n topic_pref then
        (String.lchop ~n:(String.length topic_pref) n, e) :: topic_opts,
        consumer_opts
      else
        topic_opts,
        (n, e) :: consumer_opts
    ) ([], []) specs.O.options in
  let to_alist l =
    List.fold_left (fun l (n, e) ->
      cons (make_pair (string n) (RaQL2DIL.expression ~r_env ~d_env e)) l
    ) (end_of_list DT.(pair string string)) l in
  let compunit, _, _ =
    fail_with_context "coding Kafka topic options" (fun () ->
      to_alist topic_opts |>
      DU.add_identifier_of_expression compunit ~name:"kafka_topic_options_") in
  let compunit, _, _ =
    fail_with_context "coding Kafka consumer options" (fun () ->
      to_alist consumer_opts |>
      DU.add_identifier_of_expression compunit ~name:"kafka_consumer_options_") in
  let compunit, _, _ =
    fail_with_context "coding the Kafka topic" (fun () ->
      RaQL2DIL.expression ~r_env ~d_env specs.topic |>
      DU.add_identifier_of_expression compunit ~name:"kafka_topic_") in
  let compunit, _, _ =
    fail_with_context "coding the Kafka partitions" (fun () ->
      let partition_t = DT.{ typ = Base I32 ; nullable = false } in
      let partitions_t = DT.Arr partition_t in
      let partitions =
        match specs.partitions with
        | None ->
            make_arr partition_t []
        | Some p ->
            let partitions = RaQL2DIL.expression ~r_env ~d_env p in
            if p.E.typ.DT.nullable then
              if_null partitions
                ~then_:(make_arr partition_t [])
                ~else_:(DC.conv ~to_:partitions_t d_env (force partitions))
            else
              DC.conv ~to_:partitions_t d_env partitions in
      DU.add_identifier_of_expression compunit ~name:"kafka_partitions_"
                                      partitions) in
  let compunit =
    let dependencies =
      [ "kafka_topic_options_" ; "kafka_consumer_options_" ; "kafka_topic_" ;
        "kafka_partitions_" ]
    and backend = DessserMiscTypes.OCaml
    and typ = unchecked_t in
    DU.add_verbatim_definition
      compunit ~name:func_name ~dependencies ~typ ~backend (fun oc _ps ->
        let p fmt = emit oc 0 fmt in
        p "let %s =" func_name ;
        p "  let tuples_ = [ [ \"param\" ], %s ;" field_of_params ;
        p "                  [ \"env\" ], Sys.getenv ] in" ;
        p "  let consumer_ = Kafka.new_consumer kafka_consumer_options_ in" ;
        p "  let topic_ = \
               RamenHelpers.subst_tuple_fields tuples_ kafka_topic_ in" ;
        p "  let topic_ = \
               Kafka.new_topic consumer_ topic_ kafka_topic_options_ in" ;
        p "  let partitions_ =" ;
        p "    if kafka_partitions_ = [||] then" ;
        p "      (Kafka.topic_metadata consumer_ topic_).topic_partitions" ;
        p "    else" ;
        p "      Array.fold_left (fun l p -> \
                   Int32.to_int p :: l) [] kafka_partitions_ in" ;
        p "  let offset_ = %a in"
          (fun oc -> function
          | O.Beginning ->
              String.print oc "Kafka.offset_beginning"
          | O.OffsetFromEnd e ->
              let o = option_get "OffsetFromEnd" __LOC__ (E.int_of_const e) in
              if o = 0 then
                String.print oc "Kafka.offset_end"
              else
                Printf.fprintf oc "Kafka.offset_tail %d" o
          | O.SaveInState ->
              todo "SaveInState"
          | O.UseKafkaGroupCoordinator _ -> (* TODO: snapshot period *)
              String.print oc "Kafka.offset_stored")
            specs.restart_from ;
        p "  CodeGenLib_IO.read_kafka_topic \
               consumer_ topic_ partitions_ offset_\n\n") in
  compunit

let emit_parse_external compunit func_name format_name =
  let compunit =
    let dependencies =
      [ "read_tuple" ]
    and backend = DessserMiscTypes.OCaml
    and typ = unchecked_t in
    DU.add_verbatim_definition
      compunit ~name:func_name ~dependencies ~typ ~backend (fun oc _ps ->
        let p fmt = emit oc 0 fmt in
        p "let %s per_tuple_cb buffer start stop has_more =" func_name ;
        p "    match read_tuple buffer start stop has_more with" ;
        (* Catch only NotEnoughInput so that genuine encoding errors can crash the
         * worker before we have accumulated too many tuples in the read buffer: *)
        p "    | exception (DessserOCamlBackEndHelpers.NotEnoughInput _ as e) ->" ;
        p "        !RamenLog.logger.error \
                      \"While decoding %%s @%%d..%%d%%s: %%s\"" ;
        p "          %S start stop (if has_more then \"(...)\" else \".\")"
          format_name ;
        p "          (Printexc.to_string e) ;" ;
        p "        0" ;
        p "    | tuple, read_sz ->" ;
        p "        per_tuple_cb tuple ;" ;
        p "        read_sz\n\n") in
  compunit

let call_read compunit id_name reader_name parser_name =
  let f_name = "CodeGenLib_Skeletons.read" in
  let l = DU.environment compunit in
  let compunit =
    let read_t =
      let open DE.Ops in
      DT.func [|
        DE.type_of l (identifier reader_name) ;
        DE.type_of l (identifier parser_name) ;
        DE.type_of l (identifier "sersize_of_tuple_") ;
        DE.type_of l (identifier "time_of_tuple_") ;
        DE.type_of l (identifier "factors_of_tuple_") ;
        DE.type_of l (identifier "scalar_extractors_") ;
        DE.type_of l (identifier "serialize_tuple_") ;
        DE.type_of l (identifier "orc_make_handler_") ;
        DE.type_of l (ext_identifier "orc_write") ;
        DE.type_of l (ext_identifier "orc_close") |]
      DT.void in
    DU.add_external_identifier compunit f_name read_t in
  let cmt = "Entry point for reader worker" in
  let open DE.Ops in
  let e =
    DE.func0 ~l (fun _l ->
      apply (ext_identifier f_name) [
        identifier reader_name ;
        identifier parser_name ;
        identifier "sersize_of_tuple_" ;
        identifier "time_of_tuple_" ;
        identifier "factors_of_tuple_" ;
        identifier "scalar_extractors_" ;
        identifier "serialize_tuple_" ;
        identifier "orc_make_handler_" ;
        ext_identifier "orc_write" ;
        ext_identifier "orc_close" ]) |>
    comment cmt in
  let compunit, _, _ =
    DU.add_identifier_of_expression compunit ~name:id_name e in
  compunit

let emit_reader ~r_env compunit field_of_params func_op
                source format func_name params =
  let add_expr compunit name d =
    let compunit, _, _ = DU.add_identifier_of_expression compunit ~name d in
    compunit in
  let d_env = DU.environment compunit in
  let reader_name = (func_name : N.func :> string) ^"_reader"
  and parser_name = (func_name :> string) ^"_format"
  and format_name =
    match format with O.CSV _ -> "CSV" | O.RowBinary _ -> "RowBinary" in
  (* Generate the function to unserialize the values: *)
  let out_type = O.out_record_of_operation ~with_priv:true func_op in
  let in_typ =
    O.out_record_of_operation ~reorder:false ~with_priv:true func_op in
  let deserializer =
    match format with
    | CSV specs ->
        let config = DessserCsv.{
          separator = specs.separator ;
          newline = Some '\n' ;
          (* FIXME: Dessser do not do "maybe" quoting yet *)
          quote = if specs.may_quote then Some '"' else None ;
          null = specs.null ;
          (* FIXME: make this configurable from RAQL: *)
          true_ = Default.csv_true ;
          false_ = Default.csv_false ;
          clickhouse_syntax = specs.clickhouse_syntax ;
          vectors_of_chars_as_string =
            specs.vectors_of_chars_as_string } in
        csv_to_value ~config
    | RowBinary _ ->
        rowbinary_to_value ?config:None in
  let compunit, _, _ =
    let compunit, e = deserializer in_typ compunit in
    DE.Ops.comment "Deserialize tuple" e |>
    DU.add_identifier_of_expression compunit ~name:"value_of_ser" in
  let compunit =
    let name = "read_tuple" in
    let dependencies = [ "value_of_ser" ]
    and backend = DessserMiscTypes.OCaml
    and typ = unchecked_t in
    DU.add_verbatim_definition
      compunit ~name ~dependencies ~typ ~backend (fun oc _ps ->
      let p fmt = emit oc 0 fmt in
      p "let read_tuple buffer start stop _has_more =" ;
      p "  assert (stop >= start) ;" ;
      p "  let src = Pointer.of_pointer (pointer_of_bytes buffer) start stop in" ;
      p "  let tuple, src' = value_of_ser src in" ;
      p "  let read_sz = Pointer.sub src' src in" ;
      p "  tuple, read_sz") in
  let compunit =
    match source with
    | O.File specs ->
        emit_read_file ~r_env compunit field_of_params reader_name specs
    | O.Kafka specs ->
        emit_read_kafka ~r_env compunit field_of_params reader_name specs in
  let compunit =
    emit_parse_external compunit parser_name format_name in
  let compunit =
    fail_with_context "coding for sersize-of-tuple function" (fun () ->
      let compunit, e = sersize_of_type out_type compunit in
      add_expr compunit "sersize_of_tuple_" e) in
  let compunit =
    fail_with_context "coding for time-of-tuple function" (fun () ->
      let et = O.event_time_of_operation func_op in
      time_of_tuple ~r_env ~d_env et out_type params |>
      add_expr compunit "time_of_tuple_") in
  let compunit =
    fail_with_context "coding for tuple serializer" (fun () ->
      let compunit, e = serialize out_type compunit in
      add_expr compunit "serialize_tuple_" e) in
  let compunit =
    fail_with_context "coding for read entry point" (fun () ->
      call_read compunit EntryPoints.worker reader_name parser_name) in
  compunit

let orc_wrapper out_type orc_write_func orc_read_func ps oc =
  let p fmt = emit oc 0 fmt in
  let pub = T.filter_out_private out_type in
  p "(* A handler to be passed to the function generated by \
        emit_write_value: *)" ;
  p "type handler" ;
  p "" ;
  p "external orc_write : handler -> %s -> float -> float -> unit = %S"
    (DessserBackEndOCaml.type_identifier ps out_type.DT.typ)
    orc_write_func ;
  p "external orc_read_pub : \
       RamenName.path -> int -> (%s -> unit) -> (int * int) = %S"
    (DessserBackEndOCaml.type_identifier ps pub.DT.typ)
    orc_read_func ;
  (* Destructor do not seems to be called when the OCaml program exits: *)
  p "external orc_close : handler -> unit = \"orc_handler_close\"" ;
  p "" ;
  p "(* Parameters: schema * path * index * row per batch * batches per file * archive *)" ;
  p "external orc_make_handler : \
       string -> RamenName.path -> bool -> int -> int -> bool -> handler =" ;
  p "  \"orc_handler_create_bytecode_lol\" \"orc_handler_create\"" ;
  p "" ;
  (* A wrapper that inject missing private fields: *)
  p "let orc_read fname_ batch_sz_ k_ =" ;
  p "  orc_read_pub fname_ batch_sz_ (fun t_ -> k_ (out_of_pub_ t_))" ;
  p ""

let make_orc_handler name out_type oc _ps =
  let p fmt = emit oc 0 fmt in
  let schema = Orc.of_value_type out_type.DT.typ |>
               IO.to_string Orc.print in
  p "let %s = orc_make_handler %S" name schema

let out_of_pub out_type pub_type =
  let cmt = "add fake private fields to public tuple" in
  let open DE.Ops in
  let rec full mn pub =
    (* Important optimisation, as no compiler is going to notice if we end up
     * copying the same structure: *)
    if not (T.has_private_fields mn) then pub else
    let e =
      match mn.typ with
      | DT.Rec mns ->
          make_rec (
            Array.map (fun (n, mn) ->
              let v =
                if N.is_private (N.field n) then
                  DE.default_mn ~allow_null:true mn
                else
                  get_field n pub in
              n, v
            ) mns |> Array.to_list)
      | Vec (_, mn) | Arr mn | Set (_, mn) ->
          map_ nop DE.(func2 ~l:DE.no_env DT.void mn (fun _l _ v ->
            full mn v)
          ) pub
      | Tup mns ->
          make_tup (
            List.init (Array.length mns) (fun i ->
              let pub = get_item i pub in
              full mns.(i) pub))
      | Sum mns ->
          assert (Array.length mns > 0) ; (* because has_private_fields *)
          let rec next_alt i =
            let copy_v () =
              let label, mn = mns.(i) in
              let pub = get_alt label pub in
              if T.has_private_fields mn then
                construct mns i (full mn pub)
              else
                pub in
            if i = Array.length mns - 1 then copy_v () else
            if_ (eq (label_of pub) (u16_of_int i))
              ~then_:(copy_v ())
              ~else_:(next_alt (i + 1)) in
          next_alt 0
      | _ ->
          pub in
    if mn.nullable then
      if_null pub ~then_:pub ~else_:(not_null e)
    else e in
  DE.func1 ~l:DE.no_env pub_type (fun _l pub ->
    full out_type pub) |>
  comment cmt

(* A function that reads the history and writes it according to some out_ref
 * under a given channel: *)
let replay compunit id_name func_op =
  let d_env = DU.environment compunit in
  let pub_typ = O.out_record_of_operation ~with_priv:false func_op in
  let compunit, _, _ =
    fail_with_context "coding for tuple reader" (fun () ->
      let compunit, e = deserialize_tuple pub_typ compunit in
      DU.add_identifier_of_expression compunit ~name:"read_pub_tuple_" e) in
  let open DE.Ops in
  let compunit, _, _ =
    fail_with_context "coding for read_out_tuple" (fun () ->
      let tx_t = DT.required (Ext "tx") in
      DE.func2 ~l:d_env tx_t DT.size (fun _l tx offs ->
        let tup =
          apply (identifier "read_pub_tuple_") [ tx ; offs ] in
        apply (identifier "out_of_pub_") [ tup ]) |>
      DU.add_identifier_of_expression compunit ~name:"read_out_tuple_") in
  let f_name = "CodeGenLib_Skeletons.replay" in
  let l = DU.environment compunit in
  let compunit =
    let aggregate_t =
      let open DE.Ops in
      DT.func [|
        DE.type_of l (identifier "read_out_tuple_") ;
        DE.type_of l (identifier "sersize_of_tuple_") ;
        DE.type_of l (identifier "time_of_tuple_") ;
        DE.type_of l (identifier "factors_of_tuple_") ;
        DE.type_of l (identifier "scalar_extractors_") ;
        DE.type_of l (identifier "serialize_tuple_") ;
        DE.type_of l (identifier "orc_make_handler_") ;
        DE.type_of l (ext_identifier "orc_write") ;
        DE.type_of l (ext_identifier "orc_read") ;
        DE.type_of l (ext_identifier "orc_close") |]
      DT.void in
    DU.add_external_identifier compunit f_name aggregate_t in
  let cmt = "Entry point for the replay worker" in
  let e =
    DE.func0 ~l (fun _l ->
      apply (ext_identifier f_name) [
        identifier "read_out_tuple_" ;
        identifier "sersize_of_tuple_" ;
        identifier "time_of_tuple_" ;
        identifier "factors_of_tuple_" ;
        identifier "scalar_extractors_" ;
        identifier "serialize_tuple_" ;
        identifier "orc_make_handler_" ;
        ext_identifier "orc_write" ;
        ext_identifier "orc_read" ;
        ext_identifier "orc_close" ]) |>
    comment cmt in
  let compunit, _, _ =
    DU.add_identifier_of_expression compunit ~name:id_name e in
  compunit

(* Trying to be backend agnostic, generate all the DIL functions required for
 * the given RaQL function.
 * Eventually those will be compiled to OCaml in order to be easily mixed
 * with [CodeGenLib_Skeleton]. *)
let generate_function
      conf func_name func_op in_type
      obj_name params_mod_name
      orc_write_func orc_read_func params
      (* Name of the module generated by [generate_global_env]: *)
      globals_mod_name
      envs_t params_t globals_t =
  (* The output type, in serialization order: *)
  let out_type = O.out_record_of_operation ~with_priv:true func_op in
  let pub_type = O.out_record_of_operation ~with_priv:false func_op in
  let backend = (module DessserBackEndOCaml : BACKEND) in (* TODO: a parameter *)
  let module BE = (val backend : BACKEND) in
  let compunit = init_compunit () in
  let add_expr compunit name d =
    let compunit, _, _ = DU.add_identifier_of_expression compunit ~name d in
    compunit in
  (* The initial environment gives access to envs, params and globals: *)
  let r_env, compunit =
    let open DE.Ops in
    Variable.[
      Env, "envs_" , envs_t ;
      Param, "params_", params_t ;
      GlobalVar, "globals_", globals_t ] |>
    List.fold_left (fun (r_env, compunit) (var, var_name, var_t) ->
      let name = globals_mod_name ^"."^ var_name
                 (* Prepare the name so that appending the field name will
                  * point at the record defined in the globals module not the
                  * local incarnation with the same name.
                  * TODO: Name the types used for the globals module (with
                  * [DU.name_type]) so we do not redefine the same type that
                  * OCaml compiler would refuse to unify - requires to know
                  * the module name (ie. type_id) though: *)
                 ^"."^ globals_mod_name ^".DessserGen" in
      let id = ext_identifier name in
(*      let type_id =
        DT.uniq_id var_t.DT.typ |> DessserBackEndOCaml.valid_module_name in *)
      let r_env = (RecordValue var, id) :: r_env
      and compunit = DU.add_external_identifier compunit name var_t in
      (* Also, some fields will use a type that's defined (identically) in both
       * modules. OCaml compiler will not accept to copy values from one to the
       * other without a conversion. Let's work around this with special
       * RecordValue accessors: *)
      let rec need_type_defs = function
        (* Only tuples, records and sums are given new type definitions: *)
        | DT.Tup _  | Rec _ | Sum _ -> true
        (* Other compound types may still require some new type definitions
         * due to their subtypes: *)
        | Usr { def ; _ } -> need_type_defs def
        | Arr mn -> need_type_defs mn.DT.typ
        | _ -> false in
      let r_env =
        match var_t with
        | DT.{ nullable = false ; typ = Rec mns } ->
            Array.fold_left (fun r_env (n, mn) ->
              if need_type_defs mn.DT.typ then (
                !logger.debug "Adding a specific binding to copy %s.%s"
                  var_name n ;
                let be_n = DessserBackEndOCaml.uniq_field_name var_t.typ n in
                (
                  RecordField (var, N.field n),
                  verbatim
                    [ DessserMiscTypes.OCaml, "Obj.magic "^ name ^"."^ be_n ]
                    mn []
                ) :: r_env
              ) else
                r_env
            ) r_env mns
        | DT.{ nullable = false ; typ = Void } ->
            (* No surprise *)
            r_env
        | mn ->
            !logger.warning "Variable %S from global module %s is a %a?!"
              var_name
              globals_mod_name
              DT.print_mn mn ;
            r_env in
      r_env, compunit
    ) ([], compunit) in
  let field_of_params = params_mod_name ^".field_of_params_" in
  let compunit =
    DU.add_external_identifier compunit field_of_params unchecked_t in
  (* Those three are just passed to the external function [aggregate] and so
   * their exact type is irrelevant.
   * We could have an explicit "Unchecked" type in Dessser for such cases
   * but we could as well pick any value for Unchecked such as Unit. *)
  let compunit =
    [ "orc_write" ; "orc_read" ; "orc_close" ] |>
    List.fold_left (fun compunit name ->
      DU.add_external_identifier compunit name unchecked_t
    ) compunit in
  (* This will actually not only define orc_make_handler_ but also orc_write
   * etc, but since they are all used together dependency on only one of them
   * is good enough. *)
  let compunit =
    let name = "orc_make_handler_" in
    let dependencies = [ "out_of_pub_" ]
    and backend = DessserMiscTypes.OCaml
    and typ = unchecked_t in
    DU.add_verbatim_definition
      compunit ~name ~dependencies ~typ ~backend (fun oc ps ->
        orc_wrapper out_type orc_write_func orc_read_func ps oc ;
        make_orc_handler name out_type oc ps) in
  let compunit =
    fail_with_context "coding for out_of_pub_ function" (fun () ->
      out_of_pub out_type pub_type |>
      add_expr compunit "out_of_pub_") in
  let compunit =
    let name = "scalar_extractors" in
    DU.register_external_type compunit name (fun ps -> function
      | DessserMiscTypes.OCaml ->
          BE.type_identifier_mn ps (extractor_t out_type) ^" array"
      | _ -> todo "codegen for other backends than OCaml") in
  (* We will also need a few helper functions: *)
  let compunit =
    let name = "RingBuf.tx_address" in
    let tx_address_t =
      DT.func [| DT.(required (Ext "tx")) |] DT.u64 in
    DU.add_external_identifier compunit name tx_address_t in
  let compunit =
    let name = "RingBuf.tx_size" in
    let tx_size_t =
      (* Size representation is a Size.t = int *)
      DT.func [| DT.(required (Ext "tx")) |] DT.size in
    DU.add_external_identifier compunit name tx_size_t in
  (* Register all RamenType.value types: *)
  let compunit =
    let name = "Raql_value.VNull" in
    let t = DT.(required (Ext "ramen_value")) in
    (* Note on the above "required": a "ramen_value" is a RamenType.value, which
     * is never going to be nullable, since it's not even a maybe_nullable,
     * even when it's VNull. *)
    DU.add_external_identifier compunit name t in
  let compunit =
    let name = "Raql_value.VUnit" in
    let t = DT.(required (Ext "ramen_value")) in
    DU.add_external_identifier compunit name t in
  (* Those are function-like: *)
  let compunit =
    DT.[ "VFloat", float ; "VString", string ; "VBool", bool ; "VChar", char ;
         "VU8", u8 ; "VU16", u16 ; "VU24", u24 ; "VU32", u32 ;
         "VU40", u40 ; "VU48", u48 ; "VU56", u56 ; "VU64", u64 ;
         "VU128", u128 ;
         "VI8", i8 ; "VI16", i16 ; "VI24", i24 ; "VI32", i32 ;
         "VI40", i40 ; "VI48", i48 ; "VI56", i56 ; "VI64", i64 ;
         "VI128", i128 ;
         (* Although RamenIpv4/6 are equivalent to Dessser's (being mere Stdint
          * integers), Ip and Cidr are not and must be converted.
          * RamenIp.t and RamenIp.Cidr.t are referred to as external types
          * "ramen_ip" and "ramen_cidr" in dessser. *)
         "VEth", required (get_user_type "Eth") ;
         "VIpv4", required (get_user_type "Ip4") ;
         "VIpv6", required (get_user_type "Ip6") ;
         "VIp", required (DT.Ext "ramen_ip") ;
         "VCidrv4", DT.(pair u32 u8) ; "VCidrv6", DT.(pair u128 u8) ;
         "VCidr", required (DT.Ext "ramen_cidr") ] |>
    List.fold_left (fun compunit (n, in_t) ->
      let name = "Raql_value."^ n in
      let out_t = DT.(required (Ext "ramen_value")) in
      let t = DT.func [| in_t |] out_t in
      DU.add_external_identifier compunit name t
    ) compunit in
  let compunit =
    let t = DT.(func [| DT.u32 |] (required (Ext "ramen_ip"))) in
    DU.add_external_identifier compunit "RamenIp.make_v4" t in
  let compunit =
    let t = DT.(func [| DT.u128 |] (required (Ext "ramen_ip"))) in
    DU.add_external_identifier compunit "RamenIp.make_v6" t in
  let compunit =
    let t = DT.(func [| DT.u32 ; DT.u8 |] (required (Ext "ramen_cidr"))) in
    DU.add_external_identifier compunit "RamenIp.Cidr.make_v4" t in
  let compunit =
    let t = DT.(func [| DT.u128 ; DT.u8 |] (required (Ext "ramen_cidr"))) in
    DU.add_external_identifier compunit "RamenIp.Cidr.make_v6" t in
  let compunit =
    let name = "CodeGenLib_Dessser.make_float_pair" in
    let t =
      DT.(func [| float ; float |] (required (Ext "float_pair"))) in
    DU.add_external_identifier compunit name t in
  let compunit =
    let name = "CodeGenLib_Dessser.make_string_pair" in
    let t =
      DT.(func [| string ; string |] (required (Ext "string_pair"))) in
    DU.add_external_identifier compunit name t in
  let compunit =
    let name = "CodeGenLib_Dessser.make_factor_value" in
    let t =
      DT.(func [| string ; required (Ext "ramen_value") |]
               (required (Ext "factor_value"))) in
    DU.add_external_identifier compunit name t in
  let compunit =
    let name = "CodeGenLib_Dessser.make_extractors_vector" in
    let t =
      DT.(func [| required (lst (extractor_t out_type)) |]
               (required (Ext "scalar_extractors"))) in
    DU.add_external_identifier compunit name t in
  (* Coding for factors extractor *)
  let compunit =
    fail_with_context "coding for factors extractor" (fun () ->
      factors_of_tuple func_op out_type |>
      add_expr compunit "factors_of_tuple_") in
  (* Coding for the vector of scalar extractors: *)
  let compunit =
    fail_with_context "coding for scalar extractors" (fun () ->
      scalar_extractors out_type |>
      add_expr compunit "scalar_extractors_") in
  (* Default top-half (for non-aggregate operations): a NOP *)
  let compunit, _, _ =
    let open DE.Ops in
    DE.func0 ~l:DE.no_env (fun _l -> nop) |>
    DU.add_identifier_of_expression compunit ~name:EntryPoints.top_half in
  (* Coding for all functions required to implement the worker: *)
  let compunit =
    match func_op with
    | O.Aggregate _ ->
        emit_aggregate ~r_env compunit func_op func_name in_type params
    | O.ReadExternal { source ; format ; _ } ->
        emit_reader ~r_env compunit field_of_params func_op
                    source format func_name params
    | _ ->
        todo "Non aggregate functions" in
  (* Coding for replay worker: *)
  let compunit =
    fail_with_context "coding for replay function" (fun () ->
      replay compunit EntryPoints.replay func_op) in
  (* Coding for archive convert functions: *)
  (* TODO *)
  (* Now write all those definitions into a file and compile it: *)
  let src_file =
    RamenOCamlCompiler.with_code_file_for
      obj_name conf.C.reuse_prev_files (fun oc ->
        fail_with_context "emitting worker code" (fun () ->
          let p fmt = emit oc 0 fmt in
          p "(* Dessser definitions for worker %a: *)"
            N.func_print func_name ;
          BE.print_definitions oc compunit ;
          Printf.fprintf oc "let %s = DessserGen.%s\n"
            EntryPoints.worker
            EntryPoints.worker ;
          Printf.fprintf oc "let %s = DessserGen.%s\n"
            EntryPoints.top_half
            EntryPoints.top_half ;
          Printf.fprintf oc "let %s = DessserGen.%s\n"
            EntryPoints.replay
            EntryPoints.replay ;
          Printf.fprintf oc "let %s _ _ _ _ = assert false\n"
            EntryPoints.convert)
      ) in
  let what = "function "^ N.func_color func_name in
  RamenOCamlCompiler.compile conf ~keep_temp_files:conf.C.keep_temp_files
                             what src_file obj_name

(* Helper functions to get the internal representation of a value of a
 * given type. Cannot be shared with CodeGen_OCaml since the internal
 * representations are not identical. *)

let rec emit_value_of_string
    indent mn str_var offs_var emit_is_null fins may_quote oc =
  let module BE = DessserBackEndOCaml in
  let p fmt = emit oc indent fmt in
  if mn.DT.nullable then (
    p "let is_null_, o_ = %t in" (emit_is_null fins str_var offs_var) ;
    p "if is_null_ then None, o_ else" ;
    p "let x_, o_ =" ;
    let mn = { mn with nullable = false } in
    emit_value_of_string (indent+1) mn str_var "o_" emit_is_null fins may_quote oc ;
    p "  in" ;
    p "Some x_, o_"
  ) else (
    let emit_parse_list indent mn oc =
      let p fmt = emit oc indent fmt in
      p "let rec read_next_ prevs_ o_ =" ;
      p "  let o_ = string_skip_blanks %s o_ in" str_var ;
      p "  if o_ >= String.length %s then" str_var ;
      p "    failwith \"List interrupted by end of string\" ;" ;
      p "  if %s.[o_] = ']' then prevs_, o_ + 1 else" str_var ;
      p "  let x_, o_ =" ;
      emit_value_of_string
        (indent + 2) mn str_var "o_" emit_is_null (';' :: ']' :: fins) may_quote oc ;
      p "    in" ;
      p "  let prevs_ = x_ :: prevs_ in" ;
      p "  let o_ = string_skip_blanks %s o_ in" str_var ;
      p "  if o_ >= String.length %s then" str_var ;
      p "    failwith \"List interrupted by end of string\" ;" ;
      p "  if %s.[o_] = ';' then read_next_ prevs_ (o_ + 1) else"
        str_var ;
      p "  if %s.[o_] = ']' then prevs_, o_+1 else" str_var ;
      p "  Printf.sprintf \"Unexpected %%C while parsing a list\"" ;
      p "    %s.[o_] |> failwith in" str_var ;
      p "let offs_ = string_skip_blanks_until '[' %s %s + 1 in"
        str_var offs_var ;
      p "let lst_, offs_ = read_next_ [] offs_ in" ;
      p "Array.of_list (List.rev lst_), offs_" in
    let emit_parse_record indent is_tuple kts oc =
      (* Look for '(' *)
      p "let offs_ = string_skip_blanks_until '(' %s %s + 1 in"
        str_var offs_var ;
      p "if offs_ >= String.length %s then" str_var ;
      p "  failwith \"Record interrupted by end of string\" ;" ;
      let num_fields = Array.length kts in
      for i = 0 to num_fields - 1 do
        let fn, mn = kts.(i) in
        let fn = N.field fn in
        (* FIXME: is this special case still required?! *)
        if N.is_private fn then (
          p "let x%d_ = %s in" i (Helpers.dummy_var_name fn)
        ) else (
          p "(* Read field %a *)" N.field_print fn ;
          p "let x%d_, offs_ =" i ;
          let fins = ';' :: fins in
          let fins = if i = num_fields - 1 then ')' :: fins else fins in
          emit_value_of_string
            (indent + 1) mn str_var "offs_" emit_is_null fins may_quote oc ;
          p "  in"
        ) ;
        p "let offs_ = string_skip_blanks %s offs_ in" str_var ;
        p "let offs_ =" ;
        if i = num_fields - 1 then (
          (* Last separator is optional *)
          p "  if offs_ < String.length %s && %s.[offs_] = ';' then"
            str_var str_var ;
          p "    offs_ + 1 else offs_ in"
        ) else (
          p "  if offs_ >= String.length %s || %s.[offs_] <> ';' then"
            str_var str_var ;
          p "    Printf.sprintf \"Expected separator ';' at offset %%d\" offs_ |>" ;
          p "    failwith" ;
          p "  else offs_ + 1 in"
        )
      done ;
      p "let offs_ = string_skip_blanks_until ')' %s offs_ + 1 in"
        str_var ;
      if is_tuple then
        p "%a, offs_"
          (array_print_i ~first:"(" ~last:")" ~sep:"; "
            (fun i oc _ -> Printf.fprintf oc "x%d_" i)) kts
      else
        (* Records are more convoluted as we need to retrieve the actual
         * field names; *)
        p "%a, offs_"
          (array_print_i ~first:"{ " ~last:" }" ~sep:"; "
            (fun i oc (field_name, _) ->
              let be_n = BE.uniq_field_name mn.DT.typ field_name in
              Printf.fprintf oc "%s = x%d_" be_n i)) kts
    in
    match mn.DT.typ with
    | Vec (d, mn) ->
        p "let lst_, offs_ as res_ =" ;
        emit_parse_list (indent + 1) mn oc ;
        p "in" ;
        p "if Array.length lst_ <> %d then" d ;
        p "  Printf.sprintf \"Was expecting %d values but got %%d\"" d ;
        p "    (Array.length lst_) |> failwith ;" ;
        p "res_"
    | Arr mn ->
        emit_parse_list indent mn oc
    | Tup ts ->
        let kts = Array.mapi (fun i t -> "item_"^ string_of_int i, t) ts in
        emit_parse_record indent true kts oc
    | Rec kts ->
        (* When reading values from a string (command line param values, CSV
         * files...) fields are expected to be given in definition order (as
         * opposed to serialization order).
         * Similarly, private fields are expected to be missing, and are thus
         * replaced by dummy values (so that we return the proper type). *)
        emit_parse_record indent false kts oc
    | Base String ->
        (* This one is a bit harder than the others due to optional quoting
         * (from the command line parameters, as CSV strings have been unquoted
         * already), and could benefit from [fins]: *)
        p "RamenTypeConverters.string_of_string ~fins:%a ~may_quote:%b %s %s"
          (List.print char_print_quoted) fins
          may_quote str_var offs_var
    (* Sum-based user types must be converted from Ramen's internal definition
     * to Dessser's ad-hoc one: *)
    | Usr { name = "Ip" ; _ } ->
        p "(match RamenTypeConverters.%s_of_string %s %s with"
          (Helpers.id_of_typ mn.DT.typ) str_var offs_var ;
        p "| RamenIp.V4 x, o -> make_ip_v4 x, o" ;
        p "| RamenIp.V6 x, o -> make_ip_v6 x, o)"
    | Usr { name = "Cidr" ; _ } ->
        p "(match RamenTypeConverters.%s_of_string %s %s with"
          (Helpers.id_of_typ mn.DT.typ) str_var offs_var ;
        p "| RamenIp.Cidr.V4 (i, m), o -> make_cidr_v4 i m, o" ;
        p "| RamenIp.Cidr.V6 (i, m), o -> make_cidr_v6 i m, o)"
    | _ ->
        p "RamenTypeConverters.%s_of_string %s %s"
          (Helpers.id_of_typ mn.DT.typ) str_var offs_var
  )

(* Emit a function that either returns the parameter value or exit: *)
let emit_string_parser oc name mn =
  let p fmt = emit oc 0 fmt in
  p "let %s s_ =" name ;
  p "  try" ;
  p "    let parsed_ =" ;
  let emit_is_null fins str_var offs_var oc =
    Printf.fprintf oc
      "if looks_like_null ~offs:%s %s && \
          string_is_term %a %s (%s + 4) then \
       true, %s + 4 else false, %s"
      offs_var str_var
      (List.print char_print_quoted) fins str_var offs_var
      offs_var offs_var in
  emit_value_of_string 3 mn "s_" "0" emit_is_null [] true oc ;
  p "    in" ;
  p "    check_parse_all s_ parsed_" ;
  p "  with e_ ->" ;
  p "    let what_ =" ;
  p "      Printf.sprintf \"Cannot parse value %%S for parameter %%s: %%s\"" ;
  p "                     s_ %S (Printexc.to_string e_) in" name ;
  p "    RamenHelpers.print_exception ~what:what_ e_ ;" ;
  p "    exit RamenConstsExitCodes.cannot_parse_param\n"

(* Output the code but also returns the compunit *)
let generate_global_env
      oc
      (* Name of the module generated by [CodeGen_OCaml.GlobalVariables.emit]: *)
      globals_mod_name
      params envvars globals =
  Printf.fprintf oc "open RamenHelpersNoLog\n" ;
  let backend = (module DessserBackEndOCaml : BACKEND) in (* TODO: a parameter *)
  let module BE = (val backend : BACKEND) in
  let compunit = init_compunit () in
  let open DE.Ops in
  (* We need a few converter from Ramen internal representation of user types and
   * Dessser's: *)
  let compunit, _, _ =
    DE.func1 ~l:DE.no_env DT.u32 (fun _d_env ip ->
      make_usr "Ip" [ ip ]) |>
    comment "Build a user type Ip (v4) from an u32" |>
    DU.add_identifier_of_expression compunit ~name:"make_ip_v4" in
  let compunit, _, _ =
    DE.func1 ~l:DE.no_env DT.u128 (fun _d_env ip ->
      make_usr "Ip" [ ip ]) |>
    comment "Build a user type Ip (v6) from an u128" |>
    DU.add_identifier_of_expression compunit ~name:"make_ip_v6" in
  let compunit, _, _ =
    DE.func2 ~l:DE.no_env DT.u32 DT.u8 (fun _d_env ip mask ->
      make_usr "Cidr" [ make_usr "Cidr4" [ ip ; mask ] ]) |>
    comment "Build a user type Cidr (v4) from an ipv4 and a mask" |>
    DU.add_identifier_of_expression compunit ~name:"make_cidr_v4" in
  let compunit, _, _ =
    DE.func2 ~l:DE.no_env DT.u128 DT.u8 (fun _d_env ip mask ->
      make_usr "Cidr" [ make_usr "Cidr6" [ ip ; mask ] ]) |>
    comment "Build a user type Cidr (v6) from an ipv6 and a mask" |>
    DU.add_identifier_of_expression compunit ~name:"make_cidr_v6" in
  let compunit, _, _ =
    List.map (fun f ->
      let n = (f : N.field :> string) in
      let v = getenv (string n) in
      n, v
    ) envvars |>
    make_rec |>
    comment "Used environment variables" |>
    DU.add_identifier_of_expression compunit ~name:"envs_" in
  (* We need to parse values from friendly text form into internal dessser
   * encoding, for which a value parser for each used type is needed: *)
  let open Program_parameter.DessserGen in
  let parser_name p =
    "param_"^ (p.ptyp.name :> string) ^"_value_of_string_" in
  let def_value_name p =
    "params_"^ (p.ptyp.name :> string) ^"_default_value_" in
  let compunit =
    List.fold_left (fun compunit p ->
      let name = parser_name p
      and backend = DessserMiscTypes.OCaml
      and typ = DT.func [| DT.string |] p.ptyp.typ
      and dependencies =
        [ "make_ip_v4" ; "make_ip_v6" ; "make_cidr_v4" ; "make_cidr_v6" ] in
      DU.add_verbatim_definition
        compunit ~name ~typ ~backend ~dependencies (fun oc _ps ->
          emit_string_parser oc name p.ptyp.typ)
    ) compunit params in
  (* Also adds the default value for each parameters: *)
  let compunit =
    List.fold_left (fun compunit p ->
      let name = def_value_name p in
      let compunit, _, _ =
        RaQL2DIL.constant p.ptyp.typ p.value |>
        comment ("Default value for "^ (p.ptyp.name :> string)) |>
        DU.add_identifier_of_expression compunit ~name in
      compunit
    ) compunit params in
  (* Build the params global vector, which also depends on the environment: *)
  let compunit, fields =
    let d_env = DU.environment compunit in
    List.fold_left (fun (compunit, fields) p ->
      let def_value = identifier (def_value_name p)
      and str_parser = identifier (parser_name p)
      and n = (p.ptyp.name :> string) in
      let v =
        let_ ~name:"env" ~l:d_env
          (getenv (string (param_envvar_prefix ^ n))) (fun _l env ->
          if_null env
            ~then_:def_value
            ~else_:(apply str_parser [ force env ])) in
      compunit, (n, v) :: fields
    ) (compunit, []) params in
  let compunit, _, _ =
    make_rec fields |>
    comment "Command line parameters" |>
    DU.add_identifier_of_expression compunit ~name:"params_" in
  (* Declare all identifiers taken from the globals module.
   * Those are functions from unit to a pair of getter/setter for each lmdb
   * map. The actual type need to be specified as it's going to be written to
   * declare the global vector. *)
  let globals_map_t = DT.(required (Ext "globals_map")) in
  let compunit, global_fields =
    List.fold_left (fun (compunit, global_fields) g ->
      let n =
        assert (globals_mod_name <> "") ;
        globals_mod_name ^"."^ CodeGen_OCaml.id_of_global g in
      let v = ext_identifier n in
      let compunit = DU.add_external_identifier compunit n globals_map_t in
      compunit, ((g.name :> string), v) :: global_fields
    ) (compunit, []) globals in
  let compunit, _, _ =
    make_rec global_fields |>
    comment "Used globals" |>
    DU.add_identifier_of_expression compunit ~name:"globals_" in
  BE.print_definitions oc compunit ;
  List.iter (fun n ->
    Printf.fprintf oc "let %s = DessserGen.%s\n" n n
  ) [ "envs_" ; "params_" ; "globals_" ] ;
  compunit
