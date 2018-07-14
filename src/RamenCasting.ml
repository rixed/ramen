(* Casting is a stage of the typing, happening after types have been assigned
 * to expressions (if only "partial types", such as TNum, TInt or TUInt, but
 * not TAny).
 * Casting changes an expression in order to insert casts so that all types
 * matches exactly, therefore relieving code generator to check any type
 * concordance and perform any conversion beside the explicit casts.
 * While doing so, the casting phase also choose the "best" possible types to
 * resolve partial types (best as in: limits the number of required casts).
 *
 *
 * Notes regarding partial types:
 * ------------------------------
 *
 * Why doesn't the SMT resolve partial types?
 * Short answer: because we do not want to rely on a given solver and so have
 * to stick to the smtlib2 specifications, which does not include optimization.
 * Therefore, we cannot minimize, for instance, the width of an int.
 * If we constrained numbers to be "(or signed unsigned float)", then all
 * operators that must have, say, 2 operands with identical type, would fail
 * when casting is required ("1 + 0.1" would not type-check). And if we
 * loosen that constraint (make, for instance, the addition operator to only
 * require that the first operand is either a signed, unsigned or float, and
 * that the second operand is also, independently, either a signed, unsigned
 * or float, then the solver would output any type that float its boat as long
 * as it satisfy the constraint ("1 + 0.1" would indeed type-check, but maybe
 * 1 would be assigned to a 128 bits signed integer). And there is no way to
 * say which value we prefer (due to the afore-mentioned lack of optimisation
 * constraint).
 *
 * There are 2 kinds of partial types:
 *
 * - numbers (TNum), that have to be replaced by either TFloat or any of
 *   the signed or unsigned integer type;
 * - generic Cidr/Ip which IP version is unspecified;
 *
 * In both cases, literal constants always have a total (non-partial) type,
 * and operators just propagate it around, but for some operator with
 * specific need that are handled explicitly below. This propagation always
 * goes from the leaves to the trunk (in case of "1 / f", which type is
 * known
 * to be TFloat but where f has the partial type TInt, then we make no attempt
 * at turning f into a float - in that case we merely wait until we know the
 * actual type or f and maybe insert a cast.
 *
 * For fields with partial types (aka input fields from same-program parents
 * or output fields), we reach again a cyclic dependency, meaning we will
 * look for a fixed point as the internal type-checker did. This search is
 * made faster using the "final" boolean flag of RamenExpr.typ, that is set
 * whenever all operands are final.
 *
 * Notes regarding casts:
 * ----------------------
 *
 * As an optimisation we can perform the following replacements:
 *
 * - cast(t1, const(t2, x)) -> const(t1, x)
 * - cast(t1, cast(t2, x)) -> cast(t1, x)
 *
 * This second example may arise in case of explicit (but misguided) casts.
 *)
open Batteries
open RamenHelpers
open RamenLog
open RamenExpr
open RamenTypingHelpers

(* Helper function to make an expression type more specific: *)
let narrow_down_structure e structure =
  let t = typ_of e in
  (if RamenExperiments.typer_choice.variant = 2 then !logger.debug
   else !logger.warning)
    "Type finalization: from %a to %a for %a"
    (Option.print RamenTypes.print_structure) t.scalar_typ
    RamenTypes.print_structure structure
    (print false) e ;
  (* Check we make the type more specific: *)
  assert (t.scalar_typ = None ||
          RamenTypes.can_enlarge ~from:structure
                                 ~to_:(Option.get t.scalar_typ)) ;
  t.scalar_typ <- Some structure

(* Improve type in e toward a "more final" type, and returns if anything
 * changed. *)
let rec finalize_types e =
  let t = typ_of e in
  if t.final then (
    !logger.debug "expr %a is final" (print true) e ;
    false
  ) else match t.scalar_typ with
    | Some (TCidr | TIp | TNum) ->
      improve_type e
    | _ -> (* Just recurse *)
      fold_subexpressions (fun changed e ->
        finalize_types e || changed
      ) false e

(* Once we know an expression need specialization, specialize it: *)
and improve_type = function
  (* Some expressions have specific behavior: *)
  (* Recursive types are a bit of a pain: *)
  (* By default, make an expression the largest type of its operands: *)
  | e ->
      do_propagate e

(* Specialize according to the largest of operands: *)
and do_propagate e =
  (* Loop over all operands and compute the AND of their final flag, and the
   * largest of their type.
   * Notice that if there is no sub-expressions then final will be set: *)
  let changed, final, largest =
    fold_subexpressions (fun (changed, final, largest) e' ->
      let t = typ_of e' in
      (* Beware that order of evaluation is not specified: *)
      let changed = finalize_types e' || changed in
      changed,
      final && t.final,
      match largest with
      | None -> t.scalar_typ
      | Some largest ->
          Option.map (RamenTypes.larger_structure largest) t.scalar_typ
    ) (false, true, None) e
  in
  !logger.debug "largest type of operands is %a"
    (Option.print RamenTypes.print_structure) largest ;
  let t = typ_of e in
  if final || largest <> None && largest <> t.scalar_typ then (
    t.final <- final ;
    if largest <> None then narrow_down_structure e (Option.get largest) ;
    true
  ) else changed

let finalize_types funcs =
  !logger.debug "Finalizing types..." ;
  reach_fixed_point ~max_try:100 (fun () ->
    Hashtbl.fold (fun _ func changed ->
      !logger.debug "Looking at function %s" (RamenName.string_of_func func.Func.name) ;
      RamenOperation.fold_top_level_expr changed (fun changed e ->
        finalize_types e || changed
      ) (Option.get func.Func.operation)
      (* TODO: if anything changed, propagate output fields to input
       * fields! *)
    ) funcs false)
