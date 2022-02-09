(* Transpilation of alerts into RaQL *)
open Batteries
open Stdint

open RamenLog
open RamenHelpersNoLog
module DT = DessserTypes
module E = RamenExpr
module N = RamenName
module O = RamenOperation
module T = RamenTypes
module VA = RamenSync.Value.Alert
module Variable = RamenVariable
module VSI = RamenSync.Value.SourceInfo

(* We look for all keys which are simple fields (but not start/stop), then look
 * for an output field forwarding that field, and return its name (in theory
 * not only fields but any expression yielding the same results.) *)
let group_keys_of_operation =
  let open Raql_path_comp.DessserGen in
  let open Raql_select_field.DessserGen in
  function
  | O.Aggregate { aggregate_fields ; key ; _ } ->
      let simple_keys =
        List.filter_map (fun e ->
          match e.E.text with
          | Stateless (SL0 (Path [ Name n ]))
            when n <> N.field "start" &&
                 n <> N.field "stop" ->
              Some (Variable.In, n)
          | Stateless (SL2 (
                Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                     { text = Stateless (SL0 (Variable pref)) ; _ }))
            when n <> "start" && n <> "stop" ->
              Some (pref, N.field n)
          | _ -> None
        ) key in
      List.filter_map (fun sf ->
        match sf.expr.text with
        | Stateless (SL0 (Path [ Name n ]))
          when List.mem (Variable.In, n) simple_keys ->
            Some sf.alias
        | Stateless (SL2 (
              Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                   { text = Stateless (SL0 (Variable pref)) ; _ }))
          when List.mem (pref, N.field n) simple_keys ->
            Some sf.alias
        | _ -> None
      ) aggregate_fields
  | _ -> []

(* This function turns an alert into a ramen program. It is called by the
 * compiler (via RamenMake). *)
let generate get_program (src_file : N.path) a =
  let func_of_program fq =
    let pn, fn = N.fq_parse fq in
    match get_program pn with
    | exception Not_found ->
        Printf.sprintf "Program %s does not exist"
          (pn :> string) |>
        failwith
    | prog ->
        (try List.find (fun f -> f.VSI.name = fn) prog.VSI.funcs
        with Not_found ->
          Printf.sprintf "No function %s in program %s"
            (fn :> string)
            (pn :> string) |>
          failwith) in
  let func = func_of_program a.VA.table in
  let field_type_of_column column =
    let open RamenTuple in
    try
      O.out_type_of_operation ~with_priv:false func.VSI.operation |>
      List.find (fun ft -> ft.RamenTuple.name = column)
    with Not_found ->
      Printf.sprintf2 "No column %a in table %a"
        N.field_print column
        N.fq_print a.table |>
      failwith in
  let type_of_column column =
    (field_type_of_column column).RamenTuple.typ
  in
  File.with_file_out ~mode:[`create; `text ; `trunc]
                     (src_file :> string) (fun oc ->
    Printf.fprintf oc
      "-- Alert automatically generated by ramen %s at %s\n\n"
      RamenVersions.release_tag (string_of_time (Unix.time ())) ;
    (* TODO: if this sticks make the URL a parameter: *)
    let with_desc_link s =
      let t =
        Printf.sprintf "&id=%s" (CodecUrl.encode a.id) in
      Printf.sprintf
        "%S || COALESCE(\"See \" || env.VIEW_ALERT_URL || %S \
                                 || \" for details\", \
                        \"\")" s t in
    let desc_firing =
      if a.desc_firing <> "" then String.quote a.desc_firing else
        Printf.sprintf2 "%s went %s the configured threshold (%a).\n"
          (a.column :> string)
          (if a.hysteresis <= 0. then "above" else "below")
          VA.print_threshold a.threshold |>
        with_desc_link
    and desc_recovery =
      if a.desc_recovery <> "" then String.quote a.desc_recovery else
        Printf.sprintf "The value of %s recovered.\n"
          (a.column :> string) |>
        with_desc_link
    and field_expr fn =
      match func.VSI.operation with
      | O.Aggregate { aggregate_fields ; _ } ->
          (* We know it's there because of field_type_of_column: *)
          let sf =
            List.find (fun sf ->
              sf.Raql_select_field.DessserGen.alias = fn
            ) aggregate_fields in
          sf.expr
      | _ ->
          (* Should not happen that we have "same" then *)
          assert false in
    (* Do we need to reaggregate?
     * We need to if the where filter leaves us with several groups.
     * It is clear that a filter selecting only one group, corresponding
     * to a "group by thing, thung" followed by "where thing=42 and thung=43",
     * just plainly cancels the group-by.
     * Also, it is clear that if the filter does not cancel the group-by
     * entirely (for instance "group by thing, thung, thong"), then values
     * have to be re-aggregated grouped by time only.
     *
     * It is less clear what to do when the filter uses another operator
     * than "=". What's the intent of "thing>42"? Do we want to aggregate all
     * values for those things into one, and alert on the aggregate? Or do we
     * intend to alert on each metric grouped by thing, for those greater than
     * 42?
     *
     * In the following, we assume the later, so that the same alert can be
     * defined for many groups and run independently in a single worker.
     * So in that case, even if the group-by is fully cancelled we still need
     * to group-by the selected fields (but not necessarily by time), so that
     * we have one alerting context (hysteresis) per group. *)
    let group_keys = group_keys_of_operation func.VSI.operation |>
                     Array.of_list in
    Array.fast_sort N.compare group_keys ;
    (* Reaggregation is needed if we set time_step: *)
    let need_reaggr, group_by =
      match a.group_by with
      | None ->
          (* No explicit group-by, try to do the right thing automatically: *)
          let need_reaggr = a.time_step > 0. in
          Array.fold_left (fun (need_reaggr, group_by) group_key ->
            (* Reaggregation is also needed as soon as the group_keys have a field
             * which is not paired with a WHERE condition (remember
             * group_keys_of_operation leaves group by time aside): *)
            need_reaggr || not (
              Array.exists (fun w ->
                w.Simple_filter.DessserGen.op = "=" && w.lhs = group_key
              ) a.where
            ),
            (* group by any keys which is used in the where but not with an
             * equality. Used both when reaggregating and to have one alert
             * per group even when not reagregating *)
            Array.fold_left (fun group_by w ->
              if w.Simple_filter.DessserGen.op <> "=" && w.lhs = group_key then
                (w.lhs :> string) :: group_by
              else
                group_by
            ) group_by a.where
          ) (need_reaggr, []) group_keys
      | Some group_by ->
          (* Explicit group-by replaces all the above logic: *)
          Array.fast_sort N.compare group_by ;
          let need_reaggr = a.time_step > 0. || group_by <> group_keys in
          need_reaggr, ((Array.to_list group_by) :> string list) in
    Printf.fprintf oc "-- Alerting program\n\n" ;
    (* TODO: get rid of 'filtered' if a.where is empty and not need_reaggr *)
    (* The first function, filtered, as the name suggest, performs the WHERE
     * filter; but it also reaggregate (if needed).
     * To reaggregate the parent we use either the specified default
     * aggregation functions or, if "same", reuse the expression as is, but
     * take care to also define (and aggregate) the fields that are depended
     * upon. So we start by building the set of all the fields that we need,
     * then we will need to output them in the same order as in the parent: *)
    let filtered_fields = ref Set.String.empty in
    let iter_in_order f =
      O.out_type_of_operation ~reorder:false ~with_priv:true func.operation |>
      List.iter (fun ft ->
        if not (N.is_private ft.RamenTuple.name) then (
          if Set.String.mem (ft.name :> string) !filtered_fields then
            f ft.name
        )) in
    let add_field fn =
      filtered_fields :=
        Set.String.add (fn : N.field :> string) !filtered_fields in
    add_field a.column ;
    List.iter (fun f -> add_field (N.field f)) group_by ;
    Array.iter (fun f -> add_field f.Simple_filter.DessserGen.lhs) a.having ;
    Array.iter add_field a.carry_fields ;
    let default_aggr_of_field fn =
      if List.mem (fn : N.field :> string) group_by then "" else
      let ft = field_type_of_column fn in
      let e = field_expr fn in
      let default =
        match e.E.text with
        (* If the field we re-aggregate is a min, then we can safely aggregate
         * it again with the min function. Same for max, and sum. *)
        | E.Stateful { operation = SF1 (AggrMin, _) ; _ } -> "min"
        | E.Stateful { operation = SF1 (AggrMax, _) ; _ } -> "max"
        | E.Stateful { operation = SF1 (AggrSum, _) ; _ } -> "sum"
        (* If the field was an average, then our last hope is to reuse the
         * same expression, hoping that the components will be available: *)
        | E.Stateful { operation = SF1 (AggrAvg, _) ; _ } -> "same"
        | _ ->
            (* Beware that the carry_fields need not be numeric: *)
            if DT.is_numeric ft.RamenTuple.typ.DT.typ then "sum"
                                                      else "first" in
      ft.RamenTuple.aggr |? default in
    let reaggr_field fn =
      let aggr = default_aggr_of_field fn in
      if aggr = "same" then (
        (* Alias field: supposedly, we can recompute it from its expression.
         * This is only possible if all fields this one depends on are
         * available already. *)
        let e = field_expr fn in
        IO.to_string (E.print false) e
      ) else if aggr <> "" then (
        aggr ^" "^ (ramen_quote (fn :> string))
      ) else (
        ramen_quote (fn :> string)
      )
    in
    (* From now on group_by is a list of strings in RAQL format: *)
    let group_by_raql = List.map ramen_quote group_by in
    (* TOP fields must not be aggregated, the TOP expression must really have
     * those fields straight from parent *)
    Printf.fprintf oc "DEFINE filtered AS\n" ;
    Printf.fprintf oc "  FROM %s\n" (ramen_quote (a.table :> string)) ;
    Printf.fprintf oc "  WHERE %a\n"
      (CodeGen_SimpleFilter2RaQL.print type_of_column) a.where ;
    Printf.fprintf oc "  SELECT\n" ;
    (* For each top expression, compute the list of top contributing values *)
    (* XXX: filtered does group-by and therefore we want a top per group. But
     * we want the duration of that top to outlive the group :/
     * If we perform the top in the ok function then we have already lost or,
     * at best, aggregated the contributors. The only way out is to implement
     * distinct flush process per stateful expression, so that a group can be
     * cleared but for some fields that are kept (must be specified with the
     * operation not the commit condition, as not all stateful function is a
     * field).
     * Meanwhile, limit the top accuracy by only considering the last timeStep
     * interval, and in the 'alert' function only take the last top. Later
     * have something like "list top 10 thing locally by value for the past
     * $duration seconds"*)
    let add_tops () =
      if a.tops <> [||] then
        Printf.fprintf oc "    -- TOPs:\n" ;
      Array.iteri (fun i fn ->
        Printf.fprintf oc "    LIST TOP 10 %s LOCALLY BY value\n     "
          (ramen_quote (fn : N.field :> string)) ;
        (*if a.duration > 0. then
          Printf.fprintf oc " FOR THE LAST %a SECONDS"
            print_nice_float a.duration ;*)
        Printf.fprintf oc " ABOVE 2 SIGMAS\n" ;
        Printf.fprintf oc "      AS top_%d,\n" i ;
      ) a.tops in
    if need_reaggr then (
      (* Returns the fields from out in the given expression: *)
      let rec depended fn =
        let aggr = default_aggr_of_field fn in
        if aggr <> "same" then Set.String.empty else
        let e = field_expr fn in
        E.fold (fun _stack deps -> function
          | E.{ text = Stateless (SL2 (
                Get, { text = Stateless (SL0 (Const (VString fn))) ; _ },
                     { text = Stateless (SL0 (Variable Out)) ; })) } ->
              (* Add the field [fn]... *)
              Set.String.add fn deps |>
              (* ...and recursively any field it depends on: *)
              Set.String.union (depended (N.field fn))
          | _ ->
              deps) Set.String.empty e
      in
      (* Recursively also add the dependencies for aggr using "same": *)
      let deps =
        Set.String.fold (fun fn deps ->
          Set.String.union deps (depended (N.field fn))
        ) !filtered_fields Set.String.empty in
      filtered_fields := Set.String.union deps !filtered_fields ;
      !logger.debug "List of fields required to reaggregate: %a"
        (Set.String.print String.print) !filtered_fields ;
      Printf.fprintf oc "    -- Re-aggregations:\n" ;
      (* First we need to re-sample the TS with the desired time step,
       * aggregating all values for the desired column: *)
      if a.time_step > 0. then (
        Printf.fprintf oc "    TRUNCATE(start, %a) AS start,\n"
          print_nice_float a.time_step ;
        Printf.fprintf oc "    start + %a AS stop,\n"
          print_nice_float a.time_step ;
      ) else (
        Printf.fprintf oc "    MIN(start) AS start,\n" ;
        Printf.fprintf oc "    MAX(stop) AS stop,\n"
      ) ;
      (* Then all fields that have been selected: *)
      let aggr_field field =
        Printf.fprintf oc "    %s AS %s,\n"
          (reaggr_field field)
          (ramen_quote (field :> string)) in
      iter_in_order aggr_field ;
      (* Now that everything has been reaggregated: *)
      Printf.fprintf oc "    %s AS value, -- alias for simplicity\n"
        (ramen_quote (a.column :> string)) ;
      add_tops () ;
      Printf.fprintf oc "    min value,\n" ;
      Printf.fprintf oc "    max value\n" ;
      let group_by_raql =
        if a.time_step > 0. then
          (Printf.sprintf2 "start // %a" print_nice_float a.time_step) ::
          group_by_raql
        else
          group_by_raql in
      if group_by_raql <> [] then
        Printf.fprintf oc "  GROUP BY %a\n"
          (List.print ~first:"" ~last:"" ~sep:", " String.print) group_by_raql ;
      (* This wait for late points for half the time_step. Maybe too
       * conservative?
       * In case no time_step is given assume 1min (FIXME) *)
      Printf.fprintf oc "  COMMIT AFTER in.start > out.start + 1.5 * %a;\n\n"
        print_nice_float (Float.max a.time_step 60.) ;
    ) else (
      !logger.debug "No need to reaggregate! List of required fields: %a"
        (Set.String.print String.print) !filtered_fields ;
      Printf.fprintf oc "    -- Pass used fields along:\n" ;
      iter_in_order (fun fn ->
        Printf.fprintf oc "    %s,\n"
          (ramen_quote (fn :> string))) ;
      Printf.fprintf oc "    %s AS value, -- alias for simplicity\n"
        (ramen_quote (a.column :> string)) ;
      add_tops () ;
      Printf.fprintf oc "    start, stop;\n\n" ;
    ) ;
    (* Then we want for each point to find out if it's within the acceptable
     * boundaries or not, using hysteresis: *)
    Printf.fprintf oc "DEFINE ok AS\n" ;
    Printf.fprintf oc "  FROM filtered\n" ;
    Printf.fprintf oc "  SELECT *,\n" ;
    if need_reaggr then
      Printf.fprintf oc "    min_value, max_value,\n" ;
    Printf.fprintf oc "    IF %a THEN value AS filtered_value,\n"
       (CodeGen_SimpleFilter2RaQL.print type_of_column) a.having ;
    let threshold, group_by_period =
      match a.threshold with
      | Constant threshold ->
          nice_string_of_float threshold,
          None
      | Baseline b ->
          Printf.fprintf oc "    -- Compute the baseline:\n" ;
          Printf.fprintf oc
            "    SAMPLE %s OF THE PAST %a OF filtered_value AS _recent_values,\n"
            (Uint32.to_string b.sample_size)
            print_as_duration b.avg_window ;
          Printf.fprintf oc "    ONCE EVERY %a SECONDS _recent_values AS _values,\n"
            print_nice_float b.avg_window ;
          Printf.fprintf oc "    %ath PERCENTILE _values AS _perc,\n"
            print_nice_float b.percentile ;
          Printf.fprintf oc "    SMOOTH (%a, _perc) AS baseline,\n"
            print_nice_float b.smooth_factor ;
          (match b.max_distance with
          | Absolute v ->
              Printf.fprintf oc "    baseline + %a"
                print_nice_float v
          | Relative v ->
              Printf.fprintf oc "    baseline %s ABS (baseline * %a)"
                (if a.hysteresis <= 0. then "+" else "-")
                print_nice_float v) ;
          Printf.fprintf oc " AS threshold,\n" ;
          "threshold",
          Some (
            Printf.sprintf2 "(start // %a) %% %s"
              print_nice_float b.avg_window
              (Uint32.to_string b.seasonality))
    in
    let recovery =
      let op = if a.hysteresis >= 0. then " + " else "" in
      threshold ^ op ^ nice_string_of_float a.hysteresis in
    Printf.fprintf oc "    COALESCE(\n" ;
    Printf.fprintf oc "      HYSTERESIS (filtered_value, %s, %s),\n"
      recovery threshold ;
    (* Be healthy when filtered_value is NULL: *)
    Printf.fprintf oc "    true) AS ok\n" ;
    if group_by_raql <> [] || group_by_period <> None then (
      Printf.fprintf oc "  GROUP BY\n" ;
      if group_by_raql <> [] then (
        !logger.debug "Combined alert for group keys %a"
          (List.print String.print) group_by_raql ;
        Printf.fprintf oc "    %a%s\n"
          (List.print ~first:"" ~last:"" ~sep:", " String.print) group_by_raql
          (if group_by_period <> None then "," else "")
      ) ;
      if group_by_period <> None then (
        let period = Option.get group_by_period in
        !logger.debug "Grouping by seasons of period %s" period ;
        Printf.fprintf oc "    %s\n" period
      )
    ) ;
    (* The HYSTERESIS above use the local context and so regardless of
     * whether we group-by or not we want the keep the group intact from
     * tuple to tuple: *)
    Printf.fprintf oc "  KEEP;\n\n" ;
    (* Then we fire an alert if too many values are unhealthy: *)
    if a.enabled then (
      Printf.fprintf oc "DEFINE alert AS\n" ;
      Printf.fprintf oc "  FROM ok\n" ;
      Printf.fprintf oc "  SELECT *,\n" ;
      if need_reaggr then
        Printf.fprintf oc "    max_value, min_value,\n" ;
      if a.duration > 0. then
        Printf.fprintf oc
          "    COALESCE(AVG(PAST %a SECONDS OF float(not ok)) >= %a, false)\n"
          print_nice_float a.duration print_nice_float a.ratio
      else (* Look only at the last point: *)
        Printf.fprintf oc "    not ok\n" ;
      Printf.fprintf oc "      AS firing,\n" ;
      Printf.fprintf oc "    %S AS id,\n" a.id ;
      Array.iter (fun cst ->
        Printf.fprintf oc "    %S AS %s,\n"
          cst.VA.value
          (ramen_quote (cst.name : N.field :> string))
      ) a.carry_csts ;
      Printf.fprintf oc "    1 AS certainty,\n" ;
      (* This cast to string can handle the NULL case: *)
      if need_reaggr then (
        Printf.fprintf oc "    string(min_value) || \",\" || string(max_value)\n" ;
        Printf.fprintf oc "      AS values,\n") ;
      Printf.fprintf oc "    %S AS column,\n" (a.column :> string) ;
      (* Very likely unused: *)
      Printf.fprintf oc "    %s AS thresholds,\n" threshold ;
      Printf.fprintf oc "    %a AS duration,\n" print_nice_float a.duration ;
      Printf.fprintf oc "    (IF firing THEN %s\n" desc_firing ;
      Printf.fprintf oc "     ELSE %s) AS desc\n" desc_recovery ;
      if group_by_raql <> [] then (
        Printf.fprintf oc "  GROUP BY %a\n"
          (List.print ~first:"" ~last:"" ~sep:", " String.print) group_by_raql ;
      ) ;
      Printf.fprintf oc "  AFTER CHANGED firing |? firing\n" ;
      let notif_name =
        if a.id <> "" then
          Printf.sprintf2 "%s on %a (%a) triggered"
            a.id N.field_print a.column N.fq_print a.table
        else if a.desc_title <> "" then
          Printf.sprintf2 "%s on %a (%a) triggered"
            a.desc_title N.field_print a.column N.fq_print a.table
        else
          Printf.sprintf2 "%a (%a) triggered"
            N.field_print a.column N.fq_print a.table
      in
      if group_by_raql = [] then
        Printf.fprintf oc "  NOTIFY %S\n" notif_name
      else
        (* When we group by we want a distinct notification name for each group: *)
        Printf.fprintf oc "  NOTIFY %S || \" for \" || %a\n"
          notif_name
          (List.print ~first:"" ~last:"" ~sep:" \", \" || "
            (fun oc field ->
              Printf.fprintf oc "%S || string(%s)"
                (field ^ ":")
                field)
          ) group_by_raql ;
      Printf.fprintf oc "    AND KEEP;\n"))
