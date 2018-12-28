(* This module parses operations (and offer a few utilities related to
 * operations).
 *
 * An operation is the body of a function, ie. the actual operation that
 * workers will execute.
 *
 * The main operation is the `SELECT / GROUP BY` operation, but there are a few
 * others of lesser importance for data input and output.
 *
 * Operations are made of expressions, parsed in RamenExpr, and assembled into
 * programs (the compilation unit) in RamenProgram.
 *)
open Batteries
open RamenLang
open RamenHelpers
open RamenLog
module E = RamenExpr

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint
*)

(* Represents an output field from the select clause
 * 'SELECT expr AS alias' *)
type selected_field =
  { expr : E.t ;
    alias : RamenName.field ;
    doc : string ;
    (* FIXME: Have a variant and use it in RamenTimeseries as well. *)
    aggr : string option }
  [@@ppp PPP_OCaml]

let print_selected_field oc f =
  let need_alias =
    match f.expr with
    | E.Field (_, tuple, field)
      when !tuple = TupleIn && f.alias = field -> false
    | _ -> true in
  if need_alias then (
    Printf.fprintf oc "%a AS %s"
      (E.print false) f.expr
      (RamenName.string_of_field f.alias) ;
    if f.doc <> "" then Printf.fprintf oc " %S" f.doc
  ) else (
    E.print false oc f.expr ;
    if f.doc <> "" then Printf.fprintf oc " DOC %S" f.doc
  )

(* Represents what happens to a group after its value is output: *)
type flush_method =
  | Reset (* it can be deleted (tumbling windows) *)
  | Never (* or we may just keep the group as it is *)
  [@@ppp PPP_OCaml]

let print_flush_method oc = function
  | Reset ->
    Printf.fprintf oc "FLUSH"
  | Never ->
    Printf.fprintf oc "KEEP"
  [@@ppp PPP_OCaml]

(* Represents an input CSV format specifications: *)
type file_spec = { fname : E.t ; unlink : E.t }
  [@@ppp PPP_OCaml]
type csv_specs =
  { separator : string ; null : string ; fields : RamenTuple.typ }
  [@@ppp PPP_OCaml]

let print_csv_specs oc specs =
  Printf.fprintf oc "SEPARATOR %S NULL %S %a"
    specs.separator specs.null
    RamenTuple.print_typ_names specs.fields

let print_file_spec oc specs =
  Printf.fprintf oc "READ%a FILES %a"
    (fun oc unlink ->
      Printf.fprintf oc " AND DELETE IF %a" (E.print false) unlink)
      specs.unlink
    (E.print false) specs.fname

(* Type of notifications.
 * Certainty is about how likely we are the alert is not a false positive.
 * For now, if unsure, set a middle value like 0.5. *)

type notification =
  { (* Act as the alert identifier _and_ the selector for who it's aimed at.
       So use names such as "team: service ${X} for ${Y} is on fire" *)
    notif_name : E.t ;
    parameters : (string * E.t) list }
  [@@ppp PPP_OCaml]

let print_notification oc notif =
  Printf.fprintf oc "NOTIFY %a"
    (E.print false) notif.notif_name ;
  if notif.parameters <> [] then
    List.print ~first:" WITH PARAMETERS " ~last:"" ~sep:", "
      (fun oc (n, v) ->
        Printf.fprintf oc "%a AS '%s'" (E.print false) v n) oc
      notif.parameters

(* Type of an operation: *)

type t =
  (* Aggregation of several tuples into one based on some key. Superficially
   * looks like a select but much more involved. Most clauses being optional,
   * this is really the Swiss-army knife for all data manipulation in Ramen: *)
  | Aggregate of {
      fields : selected_field list ; (* Composition of the output tuple *)
      and_all_others : bool ; (* also "select *" *)
      merge : merge ;
      (* Optional buffering of N tuples for sorting according to some
       * expression: *)
      sort : (int * E.t option (* until *) * E.t list (* by *)) option ;
      (* Simple way to filter out incoming tuples: *)
      where : E.t ;
      (* How to compute the time range for that event: *)
      event_time : RamenEventTime.t option ;
      (* Will send these notification commands to the alerter: *)
      notifications : notification list ;
      key : E.t list (* Grouping key *) ;
      commit_cond : E.t (* Output the group after/before this condition holds *) ;
      commit_before : bool ; (* Commit first and aggregate later *)
      flush_how : flush_method ; (* How to flush: reset or slide values *)
      (* List of funcs (or sub-queries) that are our parents: *)
      from : data_source list ;
      (* Pause in between two productions (useful for operations with no
       * parents: *)
      every : float ;
      (* Fields with expected small dimensionality, suitable for breaking down
       * the time series: *)
      factors : RamenName.field list }
  | ReadCSVFile of {
      where : file_spec ;
      what : csv_specs ;
      preprocessor : E.t option ;
      event_time : RamenEventTime.t option ;
      factors : RamenName.field list }
  | ListenFor of {
      net_addr : Unix.inet_addr ;
      port : int ;
      proto : RamenProtocols.net_protocol ;
      factors : RamenName.field list }
  (* For those factors, event time etc are hardcoded, and data sources
   * can not be sub-queries: *)
  | Instrumentation of { from : data_source list }
  | Notifications of { from : data_source list }
  [@@ppp PPP_OCaml]

and merge =
  (* Number of entries to buffer (default 1), expression to merge-sort
   * the parents, and timeout: *)
  { last : int ; on : E.t list ; timeout : float }
  [@@ppp PPP_OCaml]

(* Possible FROM sources: other function (optionally from another program),
 * sub-query or internal instrumentation: *)
and data_source =
  | NamedOperation of (RamenName.rel_program option * RamenName.func)
  | SubQuery of t
  | GlobPattern of string
  [@@ppp PPP_OCaml]

let rec print_data_source oc = function
  | NamedOperation (Some rel_p, f) ->
      Printf.fprintf oc "%s/%s"
        (RamenName.string_of_rel_program rel_p)
        (RamenName.string_of_func f)
  | NamedOperation (None, f) ->
      String.print oc (RamenName.string_of_func f)
  | SubQuery q ->
      Printf.fprintf oc "(%a)" print q
  | GlobPattern s ->
      String.print oc s

and print oc =
  let sep = ", " in
  function
  | Aggregate { fields ; and_all_others ; merge ; sort ; where ;
                notifications ; key ; commit_cond ; commit_before ;
                flush_how ; from ; every ; _ } ->
    if from <> [] then
      List.print ~first:"FROM " ~last:"" ~sep print_data_source oc from ;
    if merge.on <> [] then (
      Printf.fprintf oc " MERGE LAST %d ON %a"
        merge.last
        (List.print ~first:"" ~last:"" ~sep:", " (E.print false)) merge.on ;
      if merge.timeout > 0. then
        Printf.fprintf oc " TIMEOUT AFTER %g SECONDS" merge.timeout) ;
    Option.may (fun (n, u_opt, b) ->
      Printf.fprintf oc " SORT LAST %d" n ;
      Option.may (fun u ->
        Printf.fprintf oc " OR UNTIL %a"
          (E.print false) u) u_opt ;
      Printf.fprintf oc " BY %a"
        (List.print ~first:"" ~last:"" ~sep:", " (E.print false)) b
    ) sort ;
    if fields <> [] || not and_all_others then
      Printf.fprintf oc " SELECT %a%s%s"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "") ;
    if every > 0. then
      Printf.fprintf oc " EVERY %g SECONDS" every ;
    if not (E.is_true where) then
      Printf.fprintf oc " WHERE %a"
        (E.print false) where ;
    if key <> [] then
      Printf.fprintf oc " GROUP BY %a"
        (List.print ~first:"" ~last:"" ~sep:", " (E.print false)) key ;
    if not (E.is_true commit_cond) ||
       flush_how <> Reset ||
       notifications <> [] then (
      let sep = ref " " in
      if flush_how = Reset && notifications = [] then (
        Printf.fprintf oc "%sCOMMIT" !sep ; sep := ", ") ;
      if flush_how <> Reset then (
        Printf.fprintf oc "%s%a" !sep print_flush_method flush_how ;
        sep := ", ") ;
      if notifications <> [] then (
        List.print ~first:!sep ~last:"" ~sep:!sep print_notification
          oc notifications ;
        sep := ", ") ;
      if not (E.is_true commit_cond) then
        Printf.fprintf oc " %s %a"
          (if commit_before then "BEFORE" else "AFTER")
          (E.print false) commit_cond)
  | ReadCSVFile { where = file_spec ; what = csv_specs ; preprocessor ; _} ->
    Printf.fprintf oc "%a %s %a"
      print_file_spec file_spec
      (Option.map_default (fun e ->
         Printf.sprintf2 "PREPROCESS WITH %a" (E.print false) e
       ) "" preprocessor)
      print_csv_specs csv_specs
  | ListenFor { net_addr ; port ; proto } ->
    Printf.fprintf oc "LISTEN FOR %s ON %s:%d"
      (RamenProtocols.string_of_proto proto)
      (Unix.string_of_inet_addr net_addr)
      port
  | Instrumentation { from } ->
    Printf.fprintf oc "LISTEN FOR INSTRUMENTATION%a"
      (List.print ~first:" FROM " ~last:"" ~sep:", "
        print_data_source) from
  | Notifications { from } ->
    Printf.fprintf oc "LISTEN FOR NOTIFICATIONS%a"
      (List.print ~first:" FROM " ~last:"" ~sep:", "
        print_data_source) from

(* We need some tools to fold/iterate over all expressions contained in an
 * operation. We always do so depth first. *)

let fold_top_level_expr init f = function
  | ListenFor _ | Instrumentation _ | Notifications _ -> init
  | ReadCSVFile { where = { fname ; unlink } ; preprocessor ; _ } ->
      let x =
        Option.map_default (f init "CSV preprocessor") init preprocessor in
      let x = f x "CSV filename" fname in
      f x "CSV DELETE-IF clause" unlink
  | Aggregate { fields ; merge ; sort ; where ; key ; commit_cond ;
                notifications ; _ } ->
      let x =
        List.fold_left (fun prev sf ->
            let what = Printf.sprintf "field %S" (RamenName.string_of_field sf.alias) in
            f prev what sf.expr
          ) init fields in
      let x = List.fold_left (fun prev me ->
            f prev "MERGE-ON clause" me
          ) x merge.on in
      let x = f x "WHERE clause" where in
      let x = List.fold_left (fun prev ke ->
            f prev "GROUP-BY clause" ke
          ) x key in
      let x = List.fold_left (fun prev notif ->
            let prev = f prev "NOTIFY name" notif.notif_name in
            List.fold_left (fun prev (_, v) ->
              f prev "NOTIFY parameter" v) prev notif.parameters
          ) x notifications in
      let x = f x "COMMIT clause" commit_cond in
      let x = match sort with
        | None -> x
        | Some (_, u_opt, b) ->
            let x = match u_opt with
              | None -> x
              | Some u -> f x "SORT-UNTIL clause" u in
            List.fold_left (fun prev e ->
              f prev "SORT-BY clause" e
            ) x b in
      x

let iter_top_level_expr f = fold_top_level_expr () (fun () -> f)

let fold_expr init f =
  fold_top_level_expr init (fun i _ -> E.fold_by_depth f i)

let iter_expr f op =
  fold_expr () (fun () e -> f e) op

(* Various functions to inspect an operation: *)

let is_merging = function
  | Aggregate { merge ; _ } when merge.on <> [] -> true
  | _ -> false

(* BEWARE: you might have an event_time set in the Func.t that is inherited
 * and therefore not in the operation! *)
let event_time_of_operation op =
  let event_time, fields =
    match op with
    | Aggregate { event_time ; fields ; _ } ->
        event_time, List.map (fun sf -> sf.alias) fields
    | ReadCSVFile { event_time ; what ; _ } ->
        event_time, List.map (fun ft -> ft.RamenTuple.name) what.fields
    | ListenFor { proto ; _ } ->
        RamenProtocols.event_time_of_proto proto, []
    | Instrumentation _ ->
        RamenBinocle.event_time, []
    | Notifications _ ->
        RamenNotification.event_time, []
  and event_time_from_fields fields =
    let fos = RamenName.field_of_string in
    let start = fos "start"
    and stop = fos "stop"
    and duration = fos "duration" in
    if List.mem start fields then
      Some RamenEventTime.(
        (start, ref OutputField, 1.),
        if List.mem stop fields then
          StopField (stop, ref OutputField, 1.)
        else if List.mem duration fields then
          DurationField (duration, ref OutputField, 1.)
        else
          DurationConst 0.)
    else None
  in
  if event_time <> None then event_time else
  event_time_from_fields fields

let func_id_of_data_source = function
  | NamedOperation id -> id
  | SubQuery _
      (* Should have been replaced by a hidden function
       * by the time this is called *)
  | GlobPattern _ ->
      (* Should not be called on instrumentation operation *)
      assert false

let parents_of_operation = function
  | ListenFor _ | ReadCSVFile _
  (* Note that those have a from clause but no actual parents: *)
  | Instrumentation _ | Notifications _ -> []
  | Aggregate { from ; _ } ->
      List.map func_id_of_data_source from

let factors_of_operation = function
  | ReadCSVFile { factors ; _ }
  | Aggregate { factors ; _ } -> factors
  | ListenFor { factors ; proto ; _ } ->
      if factors <> [] then factors
      else RamenProtocols.factors_of_proto proto
  | Instrumentation _ -> RamenBinocle.factors
  | Notifications _ -> RamenNotification.factors

(* Return the (likely) untyped output tuple *)
let out_type_of_operation ~with_private = function
  | Aggregate { fields ; and_all_others ; _ } ->
      assert (not and_all_others) ;
      List.fold_left (fun lst sf ->
        if not with_private && RamenName.is_private sf.alias then lst else
        let expr_typ = RamenExpr.typ_of sf.expr in
        RamenTuple.{
          name = sf.alias ;
          doc = sf.doc ;
          aggr = sf.aggr ;
          typ = expr_typ.typ |?
                  { structure = TAny ; nullable = true } ;
          units = expr_typ.units } :: lst
      ) [] fields |> List.rev
  | ReadCSVFile { what = { fields ; _ } ; _ } ->
      fields
  | ListenFor { proto ; _ } ->
      RamenProtocols.tuple_typ_of_proto proto
  | Instrumentation _ ->
      RamenBinocle.tuple_typ
  | Notifications _ ->
      RamenNotification.tuple_typ

(* Return the (likely) untyped in_type of the given operation: *)
let in_type_of_operation = function
  | Aggregate _ as op ->
      let input = ref [] in
      iter_expr (function
        | Field (expr_typ, tuple, name)
          when RamenLang.tuple_has_type_input !tuple &&
               not (RamenName.is_virtual name) ->
            if RamenName.is_private name then
              Printf.sprintf2 "Can not use input field %a, which is private."
                RamenName.field_print name |>
              failwith ;
            if not (List.exists (fun ft ->
                      ft.RamenTuple.name = name) !input) then (
              let t = RamenTuple.{
                name = name ;
                (* Actual types/units will be copied from parents after
                 * typing: *)
                typ = expr_typ.RamenExpr.typ |?
                        { structure = TAny ; nullable = true } ;
                units = expr_typ.RamenExpr.units ;
                (* We don't mind the doc/aggr of an input field: *)
                doc = "" ; aggr = None } in
              input := t :: !input)
        | _ -> ()
      ) op ;
      !input
    | _ -> [] (* No inputs *)

let envvars_of_operation op =
  fold_expr Set.empty (fun s -> function
    | Field (_, { contents = TupleEnv }, n) -> Set.add n s
    | _ -> s) op |>
  Set.to_list

(* Unless it's a param, assume TupleUnknow belongs to def: *)
let prefix_def params def =
  E.iter (function
    | Field (_, ({ contents = TupleUnknown } as pref), alias) ->
        if RamenTuple.params_mem alias params then
          pref := TupleParam
        else
          pref := def
    | _ -> ())

let use_event_time op =
  fold_expr false (fun b -> function
    | StatelessFun0 (_, (EventStart|EventStop)) -> true
    | _ -> b
  ) op

(* Check that the expression is valid, or return an error message.
 * Also perform some optimisation, numeric promotions, etc...
 * This is done after the parse rather than Rejecting the parsing
 * result for better error messages, and also because we need the
 * list of available parameters. *)
let check params op =
  let check_pure clause =
    E.unpure_iter (fun _ ->
      failwith ("Stateful function not allowed in "^ clause))
  and check_no_state state clause =
    E.unpure_iter (function
      | StatefulFun (_, s, _, _) when s = state ->
          failwith ("Steful function not allowed in "^ clause)
      | _ -> ())
  and check_fields_from lst where =
    E.iter (function
      | E.Field (_, tuple, _) ->
        if not (List.mem !tuple lst) then (
          Printf.sprintf2 "Tuple %s not allowed in %s (only %a)"
            (RamenLang.string_of_prefix !tuple)
            where (pretty_list_print RamenLang.tuple_prefix_print) lst |>
          failwith
        )
      | _ -> ())
  and check_field_exists field_names f =
    if not (List.mem f field_names) then
      Printf.sprintf2 "Field %a is not in output tuple (only %a)"
        RamenName.field_print f
        (pretty_list_print RamenName.field_print) field_names |>
      failwith in
  let check_event_time field_names (start_field, duration) =
    let check_field (f, src, _scale) =
      if RamenTuple.params_mem f params then
        (* FIXME: check that the type is compatible with TFloat!
         *        And not nullable! *)
        src := RamenEventTime.Parameter
      else
        check_field_exists field_names f
    in
    check_field start_field ;
    match duration with
    | RamenEventTime.DurationConst _ -> ()
    | RamenEventTime.DurationField f
    | RamenEventTime.StopField f -> check_field f
  and check_factors field_names =
    List.iter (check_field_exists field_names)
  and check_no_group = check_no_state LocalState
  in
  match op with
  | Aggregate { fields ; and_all_others ; merge ; sort ; where ; key ;
                commit_cond ; event_time ; notifications ; from ; every ;
                factors ; _ } ->
    (* Set of fields known to come from in (to help prefix_smart): *)
    let fields_from_in = ref Set.empty in
    iter_expr (function
      | Field (_, { contents = TupleIn }, alias) ->
          fields_from_in := Set.add alias !fields_from_in
      | _ -> ()) op ;
    let is_selected_fields ?i alias = (* Tells if a field is in _out_ *)
      list_existsi (fun i' sf ->
        sf.alias = alias &&
        Option.map_default (fun i -> i' < i) true i) fields in
    (* Resolve TupleUnknown into either TupleParam (if the alias is in
     * params), TupleIn or TupleOut (depending on the presence of this alias
     * in selected_fields -- optionally, only before position i) *)
    let prefix_smart ?i =
      E.iter (function
        | Field (_, ({ contents = TupleUnknown } as pref), alias) ->
            if RamenTuple.params_mem alias params then
              pref := TupleParam
            else if Set.mem alias !fields_from_in then
              pref := TupleIn
            else if is_selected_fields ?i alias then
              pref := TupleOut
            else (
              pref := TupleIn ;
              fields_from_in := Set.add alias !fields_from_in) ;
            !logger.debug "Field %a thought to belong to %s"
              RamenName.field_print alias
              (string_of_prefix !pref)
        | _ -> ()) in
    List.iteri (fun i sf -> prefix_smart ~i sf.expr) fields ;
    List.iter (prefix_def params TupleIn) merge.on ;
    Option.may (fun (_, u_opt, b) ->
      List.iter (prefix_def params TupleIn) b ;
      Option.may (prefix_def params TupleIn) u_opt) sort ;
    prefix_smart where ;
    List.iter (prefix_def params TupleIn) key ;
    List.iter (fun notif ->
      prefix_smart notif.notif_name ;
      List.iter (fun (_n, v) -> prefix_smart v) notif.parameters
    ) notifications ;
    prefix_smart commit_cond ;
    (* Check that we use the TupleGroup only for virtual fields: *)
    iter_expr (function
      | Field (_, { contents = TupleGroup }, alias) ->
        if not (RamenName.is_virtual alias) then
          Printf.sprintf2 "Tuple group has only virtual fields (no %a)"
            RamenName.field_print alias |>
          failwith
      | _ -> ()) op ;
    (* Now check what tuple prefixes are used: *)
    List.fold_left (fun prev_aliases sf ->
        check_fields_from
          [ TupleParam; TupleEnv; TupleIn; TupleGroup;
            TupleOut (* FIXME: only if defined earlier *);
            TupleOutPrevious ] "SELECT clause" sf.expr ;
        (* Check unicity of aliases *)
        if List.mem sf.alias prev_aliases then
          Printf.sprintf2 "Alias %a is not unique"
            RamenName.field_print sf.alias |>
          failwith ;
        sf.alias :: prev_aliases
      ) [] fields |> ignore;
    if not and_all_others then (
      let field_names = List.map (fun sf -> sf.alias) fields in
      Option.may (check_event_time field_names) event_time ;
      check_factors field_names factors
    ) ;
    (* Disallow group state in WHERE because it makes no sense: *)
    check_no_group "WHERE clause" where ;
    check_fields_from
      [ TupleParam; TupleEnv; TupleIn;
        TupleGroup; TupleOutPrevious; TupleMergeGreatest ]
      "WHERE clause" where ;
    List.iter (fun k ->
      check_pure "GROUP-BY clause" k ;
      check_fields_from
        [ TupleParam; TupleEnv; TupleIn ] "Group-By KEY" k
    ) key ;
    List.iter (fun notif ->
      let check w e =
        check_fields_from [ TupleParam; TupleEnv; TupleIn; TupleOut; ] w e in
      check "notification name" notif.notif_name ;
      List.iter (fun (n, v) ->
        check ("notification parameter ("^ n ^")") v
      ) notif.parameters
    ) notifications ;
    check_fields_from
      [ TupleParam; TupleEnv; TupleIn;
        TupleOut; TupleOutPrevious;
        TupleGroup ]
      "COMMIT WHEN clause" commit_cond ;
    Option.may (fun (_, until_opt, bys) ->
      Option.may (fun until ->
        check_fields_from
          [ TupleParam; TupleEnv;
            TupleSortFirst; TupleSortSmallest; TupleSortGreatest ]
          "SORT-UNTIL clause" until
      ) until_opt ;
      List.iter (fun by ->
        check_fields_from
          [ TupleParam; TupleEnv; TupleIn ]
          "SORT-BY clause" by
      ) bys
    ) sort ;
    List.iter (fun e ->
      check_fields_from
        [ TupleParam; TupleEnv; TupleIn ]
        "MERGE-ON clause" e
    ) merge.on ;
    if every > 0. && from <> [] then
      failwith "Cannot have both EVERY and FROM" ;
    (* Check that we do not use any fields from out that is generated: *)
    let generators = List.filter_map (fun sf ->
        if E.is_generator sf.expr then Some sf.alias else None
      ) fields in
    iter_expr (function
        | Field (_, tuple_ref, alias)
          when !tuple_ref = TupleOutPrevious ->
            if List.mem alias generators then
              Printf.sprintf2 "Cannot use a generated output field %a"
                RamenName.field_print alias |>
              failwith
        | _ -> ()) op

    (* TODO: notifications: check field names from text templates *)

  | ListenFor { proto ; factors ; _ } ->
    let tup_typ = RamenProtocols.tuple_typ_of_proto proto in
    let field_names = List.map (fun t -> t.RamenTuple.name) tup_typ in
    check_factors field_names factors

  | ReadCSVFile { what ; where = { fname ; unlink } ; event_time ; factors ;
                  preprocessor ; _ } ->
    let field_names = List.map (fun t -> t.RamenTuple.name) what.fields in
    Option.may (check_event_time field_names) event_time ;
    check_factors field_names factors ;
    (* Default to In if not a param, and then disallow In ):-) *)
    Option.may (fun p ->
      (* prefix_def will select Param if it is indeed in param, and only
       * if not will it assume it's in env; which makes sense as that's the
       * only two possible tuples here: *)
      prefix_def params TupleEnv p ;
      check_fields_from [ TupleParam; TupleEnv ] "PREPROCESSOR" p
    ) preprocessor ;
    prefix_def params TupleEnv fname ;
    check_fields_from [ TupleParam; TupleEnv ] "FILE NAMES" fname ;
    prefix_def params TupleEnv unlink ;
    check_fields_from [ TupleParam; TupleEnv ] "DELETE-IF" unlink ;
    check_pure "DELETE-IF" unlink
    (* FIXME: check the field type declarations use only scalar types *)

  | Instrumentation _ | Notifications _ -> ()

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let rec default_alias =
    let open E in
    let force_public field =
      if String.length field = 0 || field.[0] <> '_' then field
      else String.lchop field in
    function
    | Field (_, _, field)
        when not (RamenName.is_virtual field) ->
        force_public (RamenName.string_of_field field)
    (* Provide some default name for common aggregate functions: *)
    | StatefulFun (_, _, _, AggrMin e) -> "min_"^ default_alias e
    | StatefulFun (_, _, _, AggrMax e) -> "max_"^ default_alias e
    | StatefulFun (_, _, _, AggrSum e) -> "sum_"^ default_alias e
    | StatefulFun (_, _, _, AggrAvg e) -> "avg_"^ default_alias e
    | StatefulFun (_, _, _, AggrAnd e) -> "and_"^ default_alias e
    | StatefulFun (_, _, _, AggrOr e) -> "or_"^ default_alias e
    | StatefulFun (_, _, _, AggrFirst e) -> "first_"^ default_alias e
    | StatefulFun (_, _, _, AggrLast e) -> "last_"^ default_alias e
    | StatefulFun (_, _, _, AggrHistogram (e, _, _, _)) -> default_alias e ^"_histogram"
    | StatelessFun2 (_, Percentile, Const (_, p), e)
      when RamenTypes.is_round_integer p ->
      Printf.sprintf "%s_%sth" (default_alias e) (IO.to_string RamenTypes.print p)
    (* Some functions better leave no traces: *)
    | StatelessFunMisc (_, Print es) when es <> [] ->
      default_alias (List.hd es)
    | StatelessFun1 (_, Cast _, e)
    | StatefulFun (_, _, _, Group e) ->
      default_alias e
    | _ -> raise (Reject "must set alias")

  (* Either `expr` or `expr AS alias` or `expr AS alias "doc"`, or
   * `expr doc "doc"`: *)
  let selected_field m =
    let m = "selected field" :: m in
    (
      E.Parser.p ++ (
        optional ~def:(None, "") (
          blanks -- strinG "as" -- blanks -+ some non_keyword ++
          optional ~def:"" (blanks -+ quoted_string)) |||
        (blanks -- strinG "doc" -- blanks -+ quoted_string >>:
         fun doc -> None, doc)) ++
      optional ~def:None (
        blanks -+ some RamenTuple.Parser.default_aggr) >>:
      fun ((expr, (alias, doc)), aggr) ->
        let alias =
          Option.default_delayed (fun () -> default_alias expr) alias in
        let alias = RamenName.field_of_string alias in
        { expr ; alias ; doc ; aggr }
    ) m

  let event_time_clause m =
    let m = "event time clause" :: m in
    let scale m =
      let m = "scale event field" :: m in
      (optional ~def:1. (
        (optional ~def:() blanks -- star --
         optional ~def:() blanks -+ number ))
      ) m
    in (
      let open RamenEventTime in
      strinG "event" -- blanks -- (strinG "starting" ||| strinG "starts") --
      blanks -- strinG "at" -- blanks -+ non_keyword ++ scale ++
      optional ~def:(DurationConst 0.) (
        (blanks -- optional ~def:() ((strinG "and" ||| strinG "with") -- blanks) --
         strinG "duration" -- blanks -+ (
           (non_keyword ++ scale >>: fun (n, s) ->
              let n = RamenName.field_of_string n in
              DurationField (n, ref OutputField, s)) |||
           (duration >>: fun n -> DurationConst n)) |||
         blanks -- strinG "and" -- blanks --
         (strinG "stops" ||| strinG "stopping" |||
          strinG "ends" ||| strinG "ending") -- blanks --
         strinG "at" -- blanks -+
           (non_keyword ++ scale >>: fun (n, s) ->
              let n = RamenName.field_of_string n in
              StopField (n, ref OutputField, s)))) >>:
      fun ((sta, sca), dur) ->
        let sta = RamenName.field_of_string sta in
        (sta, ref OutputField, sca), dur
    ) m

  let every_clause m =
    let m = "every clause" :: m in
    (strinG "every" -- blanks -+ duration >>: fun every ->
       if every < 0. then
         raise (Reject "sleep duration must be greater than 0") ;
       every) m

  let select_clause m =
    let m = "select clause" :: m in
    ((strinG "select" ||| strinG "yield") -- blanks -+
     several ~sep:list_sep
             ((star >>: fun _ -> None) |||
              some selected_field)) m

  let event_time_start () =
    let open RamenExpr in
    Field (make_typ "start", ref TupleIn, RamenName.field_of_string "start")

  let merge_clause m =
    let m = "merge clause" :: m in
    (
      strinG "merge" -+
      optional ~def:1 (
        blanks -- strinG "last" -- blanks -+
        pos_decimal_integer "Merge buffer size") ++
      optional ~def:[] (
        blanks -- strinG "on" -- blanks -+
        several ~sep:list_sep E.Parser.p) ++
      optional ~def:0. (
        blanks -- strinG "timeout" -- blanks -- strinG "after" -- blanks -+
        duration) >>:
      fun ((last, on), timeout) ->
        (* We do not make it the default to avoid creating a new type at
         * every parsing attempt: *)
        let on =
          if on = [] then [ event_time_start () ] else on in
        { last ; on ; timeout }
    ) m

  let sort_clause m =
    let m = "sort clause" :: m in
    (
      strinG "sort" -- blanks -- strinG "last" -- blanks -+
      pos_decimal_integer "Sort buffer size" ++
      optional ~def:None (
        blanks -- strinG "or" -- blanks -- strinG "until" -- blanks -+
        some E.Parser.p) ++
      optional ~def:[] (
        blanks -- strinG "by" -- blanks -+
        several ~sep:list_sep E.Parser.p) >>:
      fun ((l, u), b) ->
        let b =
          if b = [] then [ event_time_start () ] else b in
        l, u, b
    ) m

  let where_clause m =
    let m = "where clause" :: m in
    ((strinG "where" ||| strinG "when") -- blanks -+ E.Parser.p) m

  let group_by m =
    let m = "group-by clause" :: m in
    (strinG "group" -- blanks -- strinG "by" -- blanks -+
     several ~sep:list_sep E.Parser.p) m

  type commit_spec =
    | NotifySpec of notification
    | FlushSpec of flush_method
    | CommitSpec (* we would commit anyway, just a placeholder *)

  let notification_clause m =
    let kv m =
      let m = "key-value list" :: m in
      (
        E.Parser.p ++
        optional ~def:None (
          blanks -- strinG "as" -- blanks -+ some non_keyword) >>:
        fun (v, n_opt) ->
          let n =
            Option.default_delayed (fun () -> default_alias v) n_opt in
          let typ =
            RamenTypes.{ structure = TString ; nullable = false } in
          n, E.(StatelessFun1 (make_typ "cast", Cast typ,  v))
      ) m in
    let notify_cmd m =
      let m = "notification" :: m in
      (strinG "notify" -- blanks -+ E.Parser.p ++
       optional ~def:[]
         (blanks -- strinGs "with" -- blanks -+
          several ~sep:list_sep_and kv) >>:
      fun (notif_name, parameters) ->
        { notif_name ; parameters }) m
    in
    let m = "notification clause" :: m in
    (notify_cmd >>: fun s -> NotifySpec s) m

  let flush m =
    let m = "flush clause" :: m in
    ((strinG "flush" >>: fun () -> Reset) |||
     (strinG "keep" -- optional ~def:() (blanks -- strinG "all") >>:
       fun () -> Never) >>:
     fun s -> FlushSpec s) m

  let dummy_commit m =
    (strinG "commit" >>: fun () -> CommitSpec) m

  let default_commit_cond = E.expr_true ()

  let commit_clause m =
    let m = "commit clause" :: m in
    (several ~sep:list_sep_and ~what:"commit clauses"
       (dummy_commit ||| notification_clause ||| flush) ++
     optional ~def:(false, default_commit_cond)
      (blanks -+
       ((strinG "after" >>: fun _ -> false) |||
        (strinG "before" >>: fun _ -> true)) +- blanks ++
       E.Parser.p)) m

  let default_port_of_protocol = function
    | RamenProtocols.Collectd -> 25826
    | RamenProtocols.NetflowV5 -> 2055
    | RamenProtocols.Graphite -> 2003

  let net_protocol m =
    let m = "network protocol" :: m in
    (
      (strinG "collectd" >>: fun () -> RamenProtocols.Collectd) |||
      ((strinG "netflow" ||| strinG "netflowv5") >>: fun () ->
        RamenProtocols.NetflowV5) |||
      (strinG "graphite" >>: fun () -> RamenProtocols.Graphite)
    ) m

  let network_address =
    several ~sep:none (cond "inet address" (fun c ->
      (c >= '0' && c <= '9') ||
      (c >= 'a' && c <= 'f') ||
      (c >= 'A' && c <= 'A') ||
      c == '.' || c == ':') '0') >>:
    fun s ->
      let s = String.of_list s in
      try Unix.inet_addr_of_string s
      with Failure x -> raise (Reject x)

  let inet_addr m =
    let m = "network address" :: m in
    ((string "*" >>: fun () -> Unix.inet_addr_any) |||
     (string "[*]" >>: fun () -> Unix.inet6_addr_any) |||
     (network_address)) m

  let host_port m =
    let m = "host and port" :: m in
    (
      inet_addr ++
      optional ~def:None (
        char ':' -+
        some (decimal_integer_range ~min:0 ~max:65535 "port number"))
    ) m

  let listen_clause m =
    let m = "listen on operation" :: m in
    (strinG "listen" -- blanks --
     optional ~def:() (strinG "for" -- blanks) -+
     net_protocol ++
     optional ~def:None (
       blanks --
       optional ~def:() (strinG "on" -- blanks) -+
       some host_port) >>:
     fun (proto, addr_opt) ->
        let net_addr, port =
          match addr_opt with
          | None -> Unix.inet_addr_any, default_port_of_protocol proto
          | Some (addr, None) -> addr, default_port_of_protocol proto
          | Some (addr, Some port) -> addr, port in
        net_addr, port, proto) m

  let instrumentation_clause m =
    let m = "read instrumentation operation" :: m in
    (strinG "listen" -- blanks --
     optional ~def:() (strinG "for" -- blanks) -+
     (that_string "instrumentation" ||| that_string "notifications")) m

let fields_schema m =
  let m = "tuple schema" :: m in
  (
    char '(' -- opt_blanks -+
      several ~sep:list_sep RamenTuple.Parser.field +-
    opt_blanks +- char ')'
  ) m

  (* FIXME: It should be allowed to enter separator, null, preprocessor in
   * any order *)
  let read_file_specs m =
    let m = "read file operation" :: m in
    (
      strinG "read" -- blanks -+
      optional ~def:(RamenExpr.expr_false ()) (
        strinG "and" -- blanks -- strinG "delete" -- blanks -+
        optional ~def:(RamenExpr.expr_true ()) (
          strinG "if" -- blanks -+ E.Parser.p +- blanks)) +-
      strinGs "file" +- blanks ++
      E.Parser.p >>: fun (unlink, fname) ->
        { unlink ; fname }
    ) m

  let csv_specs m =
    let m = "CSV format" :: m in
    (optional ~def:"," (
       strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) ++
     optional ~def:"" (
       strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) ++
     fields_schema >>:
     fun ((separator, null), fields) ->
       if separator = null || separator = "" then
         raise (Reject "Invalid CSV separator") ;
       { separator ; null ; fields }) m

  let preprocessor_clause m =
    let m = "file preprocessor" :: m in
    (
      strinG "preprocess" -- blanks -- strinG "with" -- opt_blanks -+
      E.Parser.p
    ) m

  let factor_clause m =
    let m = "factors" :: m
    and field = non_keyword >>: RamenName.field_of_string in
    ((strinG "factor" ||| strinG "factors") -- blanks -+
     several ~sep:list_sep_and field) m

  type select_clauses =
    | SelectClause of selected_field option list
    | MergeClause of merge
    | SortClause of (int * E.t option (* until *) * E.t list (* by *))
    | WhereClause of E.t
    | EventTimeClause of RamenEventTime.t
    | FactorClause of RamenName.field list
    | GroupByClause of E.t list
    | CommitClause of (commit_spec list * (bool (* before *) * E.t))
    | FromClause of data_source list
    | EveryClause of float
    | ListenClause of (Unix.inet_addr * int * RamenProtocols.net_protocol)
    | InstrumentationClause of string
    | ExternalDataClause of file_spec
    | PreprocessorClause of E.t option
    | CsvSpecsClause of csv_specs

  (* A special from clause that accept globs, used to match workers in
   * instrumentation operations. *)
  let from_pattern m =
    let what = "pattern" in
    let m = what :: m in
    let first_char = letter ||| underscore ||| char '/' ||| star in
    let any_char = first_char ||| decimal_digit in
    (* It must have a star, or it will be parsed as a func_identifier
     * instead: *)
    let checked s =
      if String.contains s '*' then s else
        raise (Reject "Not a glob") in
    let unquoted =
      first_char ++ repeat_greedy ~sep:none ~what any_char >>: fun (c, s) ->
        checked (String.of_list (c :: s))
    and quoted =
      id_quote -+ repeat_greedy ~sep:none ~what (
        cond "quoted program identifier" ((<>) '\'') 'x') +-
      id_quote >>: fun s ->
      checked (String.of_list s) in
    (unquoted ||| quoted) m

  let rec from_clause m =
    let m = "from clause" :: m in
    (
      strinG "from" -- blanks -+
      several ~sep:list_sep_and (
        (func_identifier >>: fun id -> NamedOperation id) |||
        (char '(' -- opt_blanks -+ p +- opt_blanks +- char ')' >>:
          fun t -> SubQuery t) |||
        (from_pattern >>: fun t -> GlobPattern t))
    ) m

  and p m =
    let m = "operation" :: m in
    let part =
      (select_clause >>: fun c -> SelectClause c) |||
      (merge_clause >>: fun c -> MergeClause c) |||
      (sort_clause >>: fun c -> SortClause c) |||
      (where_clause >>: fun c -> WhereClause c) |||
      (event_time_clause >>: fun c -> EventTimeClause c) |||
      (group_by >>: fun c -> GroupByClause c) |||
      (commit_clause >>: fun c -> CommitClause c) |||
      (from_clause >>: fun c -> FromClause c) |||
      (every_clause >>: fun c -> EveryClause c) |||
      (listen_clause >>: fun c -> ListenClause c) |||
      (instrumentation_clause >>: fun c -> InstrumentationClause c) |||
      (read_file_specs >>: fun c -> ExternalDataClause c) |||
      (preprocessor_clause >>: fun c -> PreprocessorClause (Some c)) |||
      (csv_specs >>: fun c -> CsvSpecsClause c) |||
      (factor_clause >>: fun c -> FactorClause c) in
    (several ~sep:blanks part >>: fun clauses ->
      (* Used for its address: *)
      let default_select_fields = []
      and default_star = true
      and default_merge = { last = 1 ; on = [] ; timeout = 0. }
      and default_sort = None
      and default_where = E.expr_true ()
      and default_event_time = None
      and default_key = []
      and default_commit = ([], (false, default_commit_cond))
      and default_from = []
      and default_every = 0.
      and default_listen = None
      and default_instrumentation = ""
      and default_ext_data = None
      and default_preprocessor = None
      and default_csv_specs = None
      and default_factors = [] in
      let default_clauses =
        default_select_fields, default_star, default_merge, default_sort,
        default_where, default_event_time, default_key,
        default_commit, default_from, default_every,
        default_listen, default_instrumentation, default_ext_data,
        default_preprocessor, default_csv_specs, default_factors in
      let select_fields, and_all_others, merge, sort, where,
          event_time, key, commit, from, every, listen, instrumentation,
          ext_data, preprocessor, csv_specs, factors =
        List.fold_left (
          fun (select_fields, and_all_others, merge, sort, where,
               event_time, key, commit, from, every, listen,
               instrumentation, ext_data, preprocessor, csv_specs,
               factors) ->
            (* FIXME: in what follows, detect and signal cases when a new value
             * replaces an old one (but the default), such as when two WHERE
             * clauses are given. *)
            function
            | SelectClause fields_or_stars ->
              let fields, and_all_others =
                List.fold_left (fun (fields, and_all_others) -> function
                    | Some f -> f::fields, and_all_others
                    | None when not and_all_others -> fields, true
                    | None -> raise (Reject "All fields (\"*\") included several times")
                  ) ([], false) fields_or_stars in
              (* The above fold_left inverted the field order. *)
              let select_fields = List.rev fields in
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | MergeClause merge ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | SortClause sort ->
              select_fields, and_all_others, merge, Some sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | WhereClause where ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | EventTimeClause event_time ->
              select_fields, and_all_others, merge, sort, where,
              Some event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | GroupByClause key ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | CommitClause commit' ->
              if commit != default_commit then
                raise (Reject "Cannot have several commit clauses") ;
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit', from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | FromClause from' ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, (List.rev_append from' from),
              every, listen, instrumentation, ext_data, preprocessor,
              csv_specs, factors
            | EveryClause every ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | ListenClause l ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, Some l,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | InstrumentationClause c ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen, c,
              ext_data, preprocessor, csv_specs, factors
            | ExternalDataClause c ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, Some c, preprocessor, csv_specs,
              factors
            | PreprocessorClause preprocessor ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | CsvSpecsClause c ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, Some c,
              factors
            | FactorClause factors ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
          ) default_clauses clauses in
      let commit_specs, (commit_before, commit_cond) = commit in
      (* Try to catch when we write "commit when" instead of "commit
       * after/before": *)
      if commit_specs = [ CommitSpec ] &&
         commit_cond = default_commit_cond then
        raise (Reject "Lone COMMIT makes no sense. \
                       Do you mean COMMIT AFTER/BEFORE?") ;
      (* Distinguish between Aggregate, Read, ListenFor...: *)
      let not_aggregate =
        select_fields == default_select_fields && sort == default_sort &&
        where == default_where && key == default_key &&
        commit == default_commit
      and not_listen = listen = None || from != default_from || every <> 0.
      and not_instrumentation = instrumentation = ""
      and not_csv =
        ext_data = None && preprocessor == default_preprocessor &&
        csv_specs = None || from != default_from || every <> 0.
      and not_event_time = event_time = default_event_time
      and not_factors = factors == default_factors in
      if not_listen && not_csv && not_instrumentation then
        let flush_how, notifications =
          List.fold_left (fun (f, n) -> function
            | CommitSpec -> f, n
            | NotifySpec n' -> f, n'::n
            | FlushSpec f' ->
                if f = None then (Some f', n)
                else raise (Reject "Several flush clauses")
          ) (None, []) commit_specs in
        let flush_how = flush_how |? Reset in
        Aggregate { fields = select_fields ; and_all_others ; merge ; sort ;
                    where ; event_time ; notifications ; key ;
                    commit_before ; commit_cond ; flush_how ; from ;
                    every ; factors }
      else if not_aggregate && not_csv && not_event_time &&
              not_instrumentation && listen <> None then
        let net_addr, port, proto = Option.get listen in
        ListenFor { net_addr ; port ; proto ; factors }
      else if not_aggregate && not_listen &&
              not_instrumentation &&
              ext_data <> None && csv_specs <> None then
        ReadCSVFile { where = Option.get ext_data ;
                      what = Option.get csv_specs ;
                      preprocessor ; event_time ; factors }
      else if not_aggregate && not_listen && not_csv && not_listen &&
              not_factors
      then
        if String.lowercase instrumentation = "instrumentation" then
          Instrumentation { from }
        else
          Notifications { from }
      else
        raise (Reject "Incompatible mix of clauses")
    ) m

  (*$= p & ~printer:(test_printer print)
    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "start")) ;\
            alias = RamenName.field_of_string "start" ; doc = "" ; aggr = None } ;\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "stop")) ;\
            alias = RamenName.field_of_string "stop" ; doc = "" ; aggr = None } ;\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "itf_clt")) ;\
            alias = RamenName.field_of_string "itf_src" ; doc = "" ; aggr = None } ;\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "itf_srv")) ;\
            alias = RamenName.field_of_string "itf_dst" ; doc = "" ; aggr = None } ] ;\
        and_all_others = false ;\
        merge = { on = []; timeout = 0.; last = 1 } ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        notifications = [] ;\
        key = [] ;\
        commit_cond = replace_typ (E.expr_true ()) ;\
        commit_before = false ;\
        flush_how = Reset ;\
        event_time = None ;\
        from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (67, [])))\
      (test_op p "from foo select start, stop, itf_clt as itf_src, itf_srv as itf_dst" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [] ;\
        and_all_others = true ;\
        merge = { on = []; timeout = 0.; last = 1 } ;\
        sort = None ;\
        where = E.(\
          StatelessFun2 (typ, Gt, \
            Field (typ, ref TupleIn, RamenName.field_of_string "packets"),\
            Const (typ, VU32 Uint32.zero))) ;\
        event_time = None ; notifications = [] ;\
        key = [] ;\
        commit_cond = replace_typ (E.expr_true ()) ;\
        commit_before = false ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (26, [])))\
      (test_op p "from foo where packets > 0" |> replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "t")) ;\
            alias = RamenName.field_of_string "t" ; doc = "" ; aggr = None } ;\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "value")) ;\
            alias = RamenName.field_of_string "value" ; doc = "" ; aggr = Some "max" } ] ;\
        and_all_others = false ;\
        merge = { on = []; timeout = 0.; last = 1 } ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = Some ((RamenName.field_of_string "t", ref RamenEventTime.OutputField, 10.), DurationConst 60.) ;\
        notifications = [] ;\
        key = [] ;\
        commit_cond = replace_typ (E.expr_true ()) ;\
        commit_before = false ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (86, [])))\
      (test_op p "from foo select t, value aggregates using max event starting at t*10 with duration 60s" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "t1")) ;\
            alias = RamenName.field_of_string "t1" ; doc = "" ; aggr = None } ;\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "t2")) ;\
            alias = RamenName.field_of_string "t2" ; doc = "" ; aggr = None } ;\
          { expr = E.(Field (typ, ref TupleIn, RamenName.field_of_string "value")) ;\
            alias = RamenName.field_of_string "value" ; doc = "" ; aggr = None } ] ;\
        and_all_others = false ;\
        merge = { on = []; timeout = 0.; last = 1 } ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = Some ((RamenName.field_of_string "t1", ref RamenEventTime.OutputField, 10.), \
                           StopField (RamenName.field_of_string "t2", ref RamenEventTime.OutputField, 10.)) ;\
        notifications = [] ; key = [] ;\
        commit_cond = replace_typ (E.expr_true ()) ;\
        commit_before = false ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (75, [])))\
      (test_op p "from foo select t1, t2, value event starting at t1*10 and stopping at t2*10" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [] ;\
        and_all_others = true ;\
        merge = { on = []; timeout = 0.; last = 1 } ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = None ;\
        notifications = [ \
          { notif_name = E.Const (typ, VString "ouch") ;\
            parameters = [] } ] ;\
        key = [] ;\
        commit_cond = replace_typ (E.expr_true ()) ;\
        commit_before = false ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (22, [])))\
      (test_op p "from foo NOTIFY \"ouch\"" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.(\
              StatefulFun (typ, LocalState, true, AggrMin (\
                Field (typ, ref TupleIn, RamenName.field_of_string "start")))) ;\
            alias = RamenName.field_of_string "start" ; doc = "" ; aggr = None } ;\
          { expr = E.(\
              StatefulFun (typ, LocalState, true, AggrMax (\
                Field (typ, ref TupleIn, RamenName.field_of_string "stop")))) ;\
            alias = RamenName.field_of_string "max_stop" ; doc = "" ; aggr = None } ;\
          { expr = E.(\
              StatelessFun2 (typ, Div, \
                StatefulFun (typ, LocalState, true, AggrSum (\
                  Field (typ, ref TupleIn, RamenName.field_of_string "packets"))),\
                Field (typ, ref TupleParam, RamenName.field_of_string "avg_window"))) ;\
            alias = RamenName.field_of_string "packets_per_sec" ; doc = "" ; aggr = None } ] ;\
        and_all_others = false ;\
        merge = { on = []; timeout = 0.; last = 1 } ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = None ; \
        notifications = [] ;\
        key = [ E.(\
          StatelessFun2 (typ, Div, \
            Field (typ, ref TupleIn, RamenName.field_of_string "start"),\
            StatelessFun2 (typ, Mul, \
              Const (typ, VU32 (Uint32.of_int 1_000_000)),\
              Field (typ, ref TupleParam, RamenName.field_of_string "avg_window")))) ] ;\
        commit_cond = E.(\
          StatelessFun2 (typ, Gt, \
            StatelessFun2 (typ, Add, \
              StatefulFun (typ, LocalState, true, AggrMax (\
                Field (typ, ref TupleIn, RamenName.field_of_string "start"))),\
              Const (typ, VU32 (Uint32.of_int 3600))),\
            Field (typ, ref TupleOut, RamenName.field_of_string "start"))) ; \
        commit_before = false ;\
        flush_how = Reset ;\
        from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
        (190, [])))\
        (test_op p "select min start as start, \\
                           max stop as max_stop, \\
                           (sum packets)/avg_window as packets_per_sec \\
                   from foo \\
                   group by start / (1_000_000 * avg_window) \\
                   commit after out.start < (max in.start) + 3600" |>\
         replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.Const (typ, VU32 Uint32.one) ;\
            alias = RamenName.field_of_string "one" ; doc = "" ; aggr = None } ] ;\
        and_all_others = false ;\
        merge = { on = []; timeout = 0.; last = 1 } ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = None ; \
        notifications = [] ;\
        key = [] ;\
        commit_cond = E.(\
          StatelessFun2 (typ, Ge, \
            StatefulFun (typ, LocalState, true, AggrSum (\
              Const (typ, VU32 Uint32.one))),\
            Const (typ, VU32 (Uint32.of_int 5)))) ;\
        commit_before = true ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
        (49, [])))\
        (test_op p "select 1 as one from foo commit before sum 1 >= 5" |>\
         replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.Field (typ, ref TupleIn, RamenName.field_of_string "n") ; \
            alias = RamenName.field_of_string "n" ; doc = "" ; aggr = None } ;\
          { expr = E.(\
              StatefulFun (typ, GlobalState, true, E.Lag (\
              E.Const (typ, VU32 (Uint32.of_int 2)), \
              E.Field (typ, ref TupleIn, RamenName.field_of_string "n")))) ;\
            alias = RamenName.field_of_string "l" ; doc = "" ; aggr = None } ] ;\
        and_all_others = false ;\
        merge = { on = []; timeout = 0.; last = 1 } ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = None ; \
        notifications = [] ;\
        key = [] ;\
        commit_cond = replace_typ (E.expr_true ()) ;\
        commit_before = false ;\
        from = [NamedOperation (Some (RamenName.rel_program_of_string "foo"), RamenName.func_of_string "bar")] ;\
        flush_how = Reset ; every = 0. ; factors = [] },\
        (37, [])))\
        (test_op p "SELECT n, lag(2, n) AS l FROM foo/bar" |>\
         replace_typ_in_op)

    (Ok (\
      ReadCSVFile { where = { fname = RamenExpr.Const (typ, RamenTypes.VString "/tmp/toto.csv") ;\
                              unlink = RamenExpr.Const (typ, RamenTypes.VBool false) } ; \
                    preprocessor = None ; event_time = None ; \
                    what = { \
                      separator = "," ; null = "" ; \
                      fields = [ \
                        { name = RamenName.field_of_string "f1" ; \
                          typ = { structure = TBool ; nullable = true } ; \
                          units = None ; doc = "" ; aggr = None } ; \
                        { name = RamenName.field_of_string "f2" ; \
                          typ = { structure = TI32 ; nullable = false } ; \
                          units = None ; doc = "" ; aggr = None } ] } ;\
                    factors = [] },\
      (44, [])))\
      (test_op p "read file \"/tmp/toto.csv\" (f1 bool?, f2 i32)" |>\
       replace_typ_in_op)

    (Ok (\
      ReadCSVFile { where = { fname = RamenExpr.Const (typ, RamenTypes.VString "/tmp/toto.csv") ;\
                              unlink = RamenExpr.Const (typ, RamenTypes.VBool true) } ; \
                    preprocessor = None ; event_time = None ; \
                    what = { \
                      separator = "," ; null = "" ; \
                      fields = [ \
                        { name = RamenName.field_of_string "f1" ; \
                          typ = { structure = TBool ; nullable = true } ; \
                          units = None ; doc = "" ; aggr = None } ; \
                        { name = RamenName.field_of_string "f2" ; \
                          typ = { structure = TI32 ; nullable = false } ; \
                          units = None ; doc = "" ; aggr = None } ] } ;\
                    factors = [] },\
      (55, [])))\
      (test_op p "read and delete file \"/tmp/toto.csv\" (f1 bool?, f2 i32)" |>\
       replace_typ_in_op)

    (Ok (\
      ReadCSVFile { where = { fname = RamenExpr.Const (typ, RamenTypes.VString "/tmp/toto.csv") ;\
                              unlink = RamenExpr.Const (typ, RamenTypes.VBool false) } ; \
                    preprocessor = None ; event_time = None ; \
                    what = { \
                      separator = "\t" ; null = "<NULL>" ; \
                      fields = [ \
                        { name = RamenName.field_of_string "f1" ; \
                          typ = { structure = TBool ; nullable = true } ; \
                          units = None ; doc = "" ; aggr = None } ;\
                        { name = RamenName.field_of_string "f2" ; \
                          typ = { structure = TI32 ; nullable = false } ; \
                          units = None ; doc = "" ; aggr = None } ] } ;\
                    factors = [] },\
      (73, [])))\
      (test_op p "read file \"/tmp/toto.csv\" \\
                      separator \"\\t\" null \"<NULL>\" \\
                      (f1 bool?, f2 i32)" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [ { expr = E.Const (typ, VU32 Uint32.one) ; \
                     alias = RamenName.field_of_string "one" ; doc = "" ; aggr = None } ] ;\
        every = 1. ; event_time = None ;\
        and_all_others = false ; merge = { on = []; timeout = 0.; last = 1 } ; sort = None ;\
        where = E.Const (typ, VBool true) ;\
        notifications = [] ; key = [] ;\
        commit_cond = replace_typ (E.expr_true ()) ;\
        commit_before = false ; flush_how = Reset ; from = [] ;\
        factors = [] },\
        (29, [])))\
        (test_op p "YIELD 1 AS one EVERY 1 SECOND" |>\
         replace_typ_in_op)
  *)

  (*$>*)
end
