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
type selected_field = { expr : E.t ; alias : string }

let print_selected_field fmt f =
  let need_alias =
    match f.expr with
    | E.Field (_, tuple, field)
      when !tuple = TupleIn && f.alias = field -> false
    | _ -> true in
  if need_alias then
    Printf.fprintf fmt "%a AS %s"
      (E.print false) f.expr
      f.alias
  else
    E.print false fmt f.expr

(* Represents what happens to a group after its value is output: *)
type flush_method =
  | Reset (* it can be deleted (tumbling windows) *)
  | Slide of int (* or entries can be shifted (sliding windows) *)
  | KeepOnly of E.t (* or only some matching entries can be kept *)
  | RemoveAll of E.t (* or inversely, expunged from the group *)
  | Never (* or we may just keep the group as it is *)

let print_flush_method oc = function
  | Reset ->
    Printf.fprintf oc "FLUSH"
  | Never ->
    Printf.fprintf oc "KEEP ALL"
  | Slide n ->
    Printf.fprintf oc "SLIDE %d" n
  | KeepOnly e ->
    Printf.fprintf oc "KEEP (%a)" (E.print false) e
  | RemoveAll e ->
    Printf.fprintf oc "REMOVE (%a)" (E.print false) e

(* Represents an input CSV format specifications: *)
type file_spec = { fname : string ; unlink : bool }
type csv_specs =
  { separator : string ; null : string ; fields : RamenTuple.typ }

let print_csv_specs fmt specs =
  Printf.fprintf fmt "SEPARATOR %S NULL %S %a"
    specs.separator specs.null
    RamenTuple.print_typ specs.fields

let print_file_spec fmt specs =
  Printf.fprintf fmt "READ%s FILES %S"
    (if specs.unlink then " AND DELETE" else "") specs.fname

(* Type of notifications. As those are transmitted in a single text field we
 * convert them using PPP. We could as well use Marshal but PPP is friendlier
 * to `ramen tail`. We could also use the ramen language syntax but parsing
 * it again from the notifier looks wasteful.
 * The idea is that some alerts require immediate action while some others can
 * be dealt with later (like during office hours).
 *
 * Notice that there is no overlap between severity and the concept of
 * certainty (which is merely encoded as a floating number).  The severity is
 * about how quickly a human must have a look (equivalently: should a
 * synchronous or asynchronous communication channel be used?) while certainty
 * is about the likelihood of a false positive (how certain we are that there
 * is an actual problem and that this is not a false positive). Both are
 * independent. *)

type severity = Urgent | Deferrable
  [@@ppp PPP_OCaml]

type notification =
  { (* Act as the alert identifier _and_ the selector for who it's aimed at.
       So use names such as "team: service ${X} for ${Y} is on fire" *)
    notif_name : string ;
    severity : severity
      [@ppp_default Urgent] ;
    parameters : (string * string) list
      [@ppp_default []] }
  [@@ppp PPP_OCaml]

let print_notification oc notif =
  Printf.fprintf oc "NOTIFY %S %s"
    notif.notif_name
    (match notif.severity with
     | Urgent -> "URGENT"
     | Deferrable -> "DEFERRABLE") ;
  if notif.parameters <> [] then
    List.print ~first:" WITH PARAMETERS " ~last:"" ~sep:", "
      (fun oc (n, v) -> Printf.fprintf oc "%S=%S" n v) oc
      notif.parameters

(* Type of an operation: *)

type t =
  (* Aggregation of several tuples into one based on some key. Superficially
   * looks like a select but much more involved. Most clauses being optional,
   * this is really the Swiss-army knife for all data manipulation in Ramen: *)
  | Aggregate of {
      fields : selected_field list ; (* Composition of the output tuple *)
      and_all_others : bool ; (* also "select *" *)
      (* Expression to merge-sort the parents, and timeout: *)
      merge : E.t list * float ;
      (* Optional buffering of N tuples for sorting according to some
       * expression: *)
      sort : (int * E.t option (* until *) * E.t list (* by *)) option ;
      (* Simple way to filter out incoming tuples: *)
      where : E.t ;
      (* How to compute the time range for that event: *)
      event_time : RamenEventTime.t option ;
      (* Will send these notification commands to the notifier: *)
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
      factors : string list }
  | ReadCSVFile of {
      where : file_spec ;
      what : csv_specs ;
      preprocessor : string ;
      event_time : RamenEventTime.t option ;
      factors : string list }
  | ListenFor of {
      net_addr : Unix.inet_addr ;
      port : int ;
      proto : RamenProtocols.net_protocol ;
      factors : string list }
  | Instrumentation of {
      from : data_source list ;
      (* factors are hardcoded *) }

(* Possible FROM sources: other function (optionally from another program),
 * sub-query or internal instrumentation: *)
and data_source =
  | NamedOperation of ((RamenName.program * RamenName.params) option * RamenName.func)
  | SubQuery of t
  | GlobPattern of string

let rec print_data_source oc = function
  | NamedOperation id ->
      print_expansed_function oc id
  | SubQuery q ->
      Printf.fprintf oc "(%a)" print q
  | GlobPattern s ->
      String.print oc s

and print fmt =
  let sep = ", " in
  function
  | Aggregate { fields ; and_all_others ; merge ; sort ; where ; event_time ;
                notifications ; key ; commit_cond ;
                commit_before ; flush_how ; from ; every } ->
    if from <> [] then
      List.print ~first:"FROM " ~last:"" ~sep print_data_source fmt from ;
    if fst merge <> [] then (
      Printf.fprintf fmt " MERGE ON %a"
        (List.print ~first:"" ~last:"" ~sep:", " (E.print false)) (fst merge) ;
      if snd merge > 0. then
        Printf.fprintf fmt " TIMEOUT AFTER %g SECONDS" (snd merge)) ;
    Option.may (fun (n, u_opt, b) ->
      Printf.fprintf fmt " SORT LAST %d" n ;
      Option.may (fun u ->
        Printf.fprintf fmt " OR UNTIL %a"
          (E.print false) u) u_opt ;
      Printf.fprintf fmt " BY %a"
        (List.print ~first:"" ~last:"" ~sep:", " (E.print false)) b
    ) sort ;
    if fields <> [] || not and_all_others then
      Printf.fprintf fmt " SELECT %a%s%s"
        (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "") ;
    if every > 0. then
      Printf.fprintf fmt " EVERY %g SECONDS" every ;
    if not (E.is_true where) then
      Printf.fprintf fmt " WHERE %a"
        (E.print false) where ;
    if key <> [] then
      Printf.fprintf fmt " GROUP BY %a"
        (List.print ~first:"" ~last:"" ~sep:", " (E.print false)) key ;
    if not (E.is_true commit_cond) ||
       flush_how <> Reset ||
       notifications <> [] then (
      let sep = ref " " in
      if flush_how = Reset && notifications = [] then (
        Printf.fprintf fmt "%sCOMMIT" !sep ; sep := ", ") ;
      if flush_how <> Reset then (
        Printf.fprintf fmt "%s%a" !sep print_flush_method flush_how ;
        sep := ", ") ;
      if notifications <> [] then (
        List.print ~first:!sep ~last:"" ~sep:!sep print_notification
          fmt notifications ;
        sep := ", ") ;
      if not (E.is_true commit_cond) then
        Printf.fprintf fmt " %s %a"
          (if commit_before then "BEFORE" else "AFTER")
          (E.print false) commit_cond)
  | ReadCSVFile { where = file_spec ;
                  what = csv_specs ; preprocessor ; event_time } ->
    Printf.fprintf fmt "%a %s %a"
      print_file_spec file_spec
      (if preprocessor = "" then ""
        else Printf.sprintf "PREPROCESS WITH %S" preprocessor)
      print_csv_specs csv_specs
  | ListenFor { net_addr ; port ; proto } ->
    Printf.fprintf fmt "LISTEN FOR %s ON %s:%d"
      (RamenProtocols.string_of_proto proto)
      (Unix.string_of_inet_addr net_addr)
      port
  | Instrumentation { from } ->
    Printf.fprintf fmt "LISTEN FOR INSTRUMENTATION%a"
      (List.print ~first:" FROM " ~last:"" ~sep:", "
        print_data_source) from

let func_id_of_data_source = function
  | NamedOperation id -> id
  | SubQuery _
      (* Should have been replaced by a hidden function
       * by the time this is called *)
  | GlobPattern _ ->
      (* Should not be called on instrumentation operation *)
      assert false

let is_merging = function
  | Aggregate { merge ; _ } when fst merge <> [] -> true
  | _ -> false

let event_time_of_operation = function
  | Aggregate { event_time ; _ } -> event_time
  | ReadCSVFile { event_time ; _ } -> event_time
  | ListenFor { proto ; _ } ->
      RamenProtocols.event_time_of_proto proto
  | Instrumentation _ ->
      RamenBinocle.event_time

let parents_of_operation = function
  | ListenFor _ | ReadCSVFile _
  (* Note that instrumentation has a from clause but no actual parents: *)
  | Instrumentation _ -> []
  | Aggregate { from ; _ } ->
      List.map func_id_of_data_source from

let factors_of_operation = function
  | ReadCSVFile { factors ; _ }
  | Aggregate { factors ; _ } -> factors
  | ListenFor { factors ; proto ; _ } ->
      if factors <> [] then factors
      else RamenProtocols.factors_of_proto proto
  | Instrumentation _ -> RamenBinocle.factors

(* We need some tools to fold/iterate over all expressions contained in an
 * operation. We always do so depth first. *)

let fold_top_level_expr init f = function
  | ListenFor _ | ReadCSVFile _ | Instrumentation _ -> init
  | Aggregate { fields ; merge ; sort ; where ; key ; commit_cond ;
                flush_how ; _ } ->
      let x =
        List.fold_left (fun prev sf ->
            f prev sf.expr
          ) init fields in
      let x = List.fold_left (fun prev me ->
            f prev me
          ) x (fst merge) in
      let x = f x where in
      let x = List.fold_left (fun prev ke ->
            f prev ke
          ) x key in
      let x = f x commit_cond in
      let x = match sort with
        | None -> x
        | Some (_, u_opt, b) ->
            let x = match u_opt with
              | None -> x
              | Some u -> f x u in
            List.fold_left (fun prev e ->
              f prev e
            ) x b in
      match flush_how with
      | Slide _ | Never | Reset -> x
      | RemoveAll e | KeepOnly e ->
        f x e

let fold_expr init f =
  fold_top_level_expr init (E.fold_by_depth f)

let iter_expr f op =
  fold_expr () (fun () e -> f e) op

let envvars_of_operation =
  fold_expr [] (fun lst -> function
    | Field (_, { contents = TupleEnv }, n) -> n :: lst
    | _ -> lst)

(* Check that the expression is valid, or return an error message.
 * Also perform some optimisation, numeric promotions, etc...
 * This is done after the parse rather than Rejecting the parsing
 * result for better error messages, and also because we need the
 * list of available parameters. *)
let check params =
  let pure_in clause = StatefulNotAllowed { clause }
  and no_group clause = StateNotAllowed { state = "local" ; clause }
  and fields_must_be_from tuple where allowed =
    TupleNotAllowed { tuple ; where ; allowed } in
  let pure_in_key = pure_in "GROUP-BY"
  and check_pure e =
    E.unpure_iter (fun _ -> raise (SyntaxError e))
  and check_no_state state e =
    E.unpure_iter (function
      | StatefulFun (_, s, _) when s = state -> raise (SyntaxError e)
      | _ -> ())
  and check_fields_from lst where =
    E.iter (function
      | E.Field (_, tuple, _) ->
        if not (List.mem !tuple lst) then (
          let m = fields_must_be_from !tuple where lst in
          raise (SyntaxError m)
        )
      | _ -> ())
  and check_field_exists field_names f =
    if not (List.mem f field_names) then
      let m =
        let tuple_type =
          IO.to_string (List.print String.print) field_names in
        FieldNotInTuple { field = f ; tuple = TupleOut ; tuple_type } in
      raise (SyntaxError m) in
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
  (* Unless it's a param, assume TupleUnknow belongs to def: *)
  and prefix_def def =
    E.iter (function
      | Field (_, ({ contents = TupleUnknown } as pref), alias) ->
          if RamenTuple.params_mem alias params then
            pref := TupleParam
          else
            pref := def
      | _ -> ())
  and check_no_group = check_no_state LocalState
  in
  function
  | Aggregate { fields ; and_all_others ; merge ; sort ; where ; key ;
                commit_cond ; flush_how ; event_time ;
                from ; every ; factors } as op ->
    (* Set of fields known to come from in (to help prefix_smart): *)
    let fields_from_in = ref Set.empty in
    iter_expr (function
      | Field (_, { contents = (TupleIn|TupleLastIn|TupleSelected|
                                TupleLastSelected|TupleUnselected|
                                TupleLastUnselected) }, alias) ->
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
            !logger.debug "Field %S thought to belong to %s"
              alias (string_of_prefix !pref)
        | _ -> ()) in
    List.iteri (fun i sf -> prefix_smart ~i sf.expr) fields ;
    List.iter (prefix_def TupleIn) (fst merge) ;
    Option.may (fun (_, u_opt, b) ->
      List.iter (prefix_def TupleIn) b ;
      Option.may (prefix_def TupleIn) u_opt) sort ;
    prefix_smart where ;
    List.iter (prefix_def TupleIn) key ;
    prefix_smart commit_cond ;
    (match flush_how with
    | KeepOnly e | RemoveAll e -> prefix_def TupleGroup e
    | _ -> ()) ;
    (* Check that we use the TupleGroup only for virtual fields: *)
    iter_expr (function
      | Field (_, { contents = TupleGroup }, alias) ->
        if not (is_virtual_field alias) then
          raise (SyntaxError (TupleHasOnlyVirtuals { tuple = TupleGroup ;
                                                     alias }))
      | _ -> ()) op ;
    (* Now check what tuple prefix are used: *)
    List.fold_left (fun prev_aliases sf ->
        check_fields_from
          [ TupleParam; TupleEnv; TupleLastIn; TupleIn; TupleGroup;
            TupleSelected; TupleLastSelected; TupleUnselected;
            TupleLastUnselected; TupleGroupFirst; TupleGroupLast;
            TupleOut (* FIXME: only if defined earlier *);
            TupleGroupPrevious; TupleOutPrevious ] "SELECT clause" sf.expr ;
        (* Check unicity of aliases *)
        if List.mem sf.alias prev_aliases then
          raise (SyntaxError (AliasNotUnique sf.alias)) ;
        sf.alias :: prev_aliases
      ) [] fields |> ignore;
    if not and_all_others then (
      let field_names = List.map (fun sf -> sf.alias) fields in
      Option.may (check_event_time field_names) event_time ;
      check_factors field_names factors
    ) ;
    (* Disallow group state in WHERE because it makes no sense: *)
    check_no_group (no_group "WHERE") where ;
    check_fields_from
      [ TupleParam; TupleEnv; TupleLastIn; TupleIn; TupleSelected;
        TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroup;
        TupleGroupFirst; TupleGroupLast; TupleOutPrevious ]
      "WHERE clause" where ;
    List.iter (fun k ->
      check_pure pure_in_key k ;
      check_fields_from
        [ TupleParam; TupleEnv; TupleIn ] "Group-By KEY" k
    ) key ;
    check_fields_from
      [ TupleParam; TupleEnv; TupleLastIn; TupleIn; TupleSelected;
        TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleOut;
        TupleGroupPrevious; TupleOutPrevious; TupleGroupFirst; TupleGroupLast;
        TupleGroup; TupleSelected; TupleLastSelected ]
      "COMMIT WHEN clause" commit_cond ;
    (match flush_how with
    | Reset | Never | Slide _ -> ()
    | RemoveAll e | KeepOnly e ->
      let m = StatefulNotAllowed { clause = "KEEP/REMOVE" } in
      check_pure m e ;
      check_fields_from
        [ TupleParam; TupleEnv; TupleGroup ] "REMOVE clause" e) ;
    if every > 0. && from <> [] then
      raise (SyntaxError (EveryWithFrom)) ;
    (* Check that we do not use any fields from out that is generated: *)
    let generators = List.filter_map (fun sf ->
        if E.is_generator sf.expr then Some sf.alias else None
      ) fields in
    iter_expr (function
        | Field (_, tuple_ref, alias)
          when !tuple_ref = TupleOutPrevious ||
               !tuple_ref = TupleGroupPrevious ->
            if List.mem alias generators then
              let e = NoAccessToGeneratedFields { alias } in
              raise (SyntaxError e)
        | _ -> ()) op

    (* TODO: notifications: check field names from text templates *)

  | ListenFor { proto ; factors ; _ } ->
    let tup_typ = RamenProtocols.tuple_typ_of_proto proto in
    let field_names = List.map (fun t -> t.RamenTuple.typ_name) tup_typ in
    check_factors field_names factors

  | ReadCSVFile { what ; event_time ; factors ; _ } ->
    let field_names = List.map (fun t -> t.RamenTuple.typ_name) what.fields in
    Option.may (check_event_time field_names) event_time ;
    check_factors field_names factors
    (* FIXME: check the field type declarations use only scalar types *)

  | Instrumentation _ -> ()

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
        when not (is_virtual_field field) -> field
    (* Provide some default name for common aggregate functions: *)
    | StatefulFun (_, _, AggrMin (Field (_, _, field))) -> "min_"^ force_public field
    | StatefulFun (_, _, AggrMax (Field (_, _, field))) -> "max_"^ force_public field
    | StatefulFun (_, _, AggrSum (Field (_, _, field))) -> "sum_"^ force_public field
    | StatefulFun (_, _, AggrAvg (Field (_, _, field))) -> "avg_"^ force_public field
    | StatefulFun (_, _, AggrAnd (Field (_, _, field))) -> "and_"^ force_public field
    | StatefulFun (_, _, AggrOr (Field (_, _, field))) -> "or_"^ force_public field
    | StatefulFun (_, _, AggrFirst (Field (_, _, field))) -> "first_"^ force_public field
    | StatefulFun (_, _, AggrLast (Field (_, _, field))) -> "last_"^ force_public field
    | StatefulFun (_, _, AggrHistogram (Field (_, _, field), _, _, _)) -> force_public field ^"_histogram"
    | StatefulFun (_, _, AggrPercentile (Const (_, p), Field (_, _, field)))
      when RamenTypes.is_round_integer p ->
      Printf.sprintf "%s_%sth" (force_public field) (IO.to_string RamenTypes.print p)
    | StatelessFunMisc (_, Print es) when es <> [] ->
      default_alias (List.hd es)
    | _ -> raise (Reject "must set alias")

  let selected_field m =
    let m = "selected field" :: m in
    (E.Parser.p ++ optional ~def:None (
       blanks -- strinG "as" -- blanks -+ some non_keyword) >>:
     fun (expr, alias) ->
      let alias =
        Option.default_delayed (fun () -> default_alias expr) alias in
      { expr ; alias }) m

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
              DurationField (n, ref OutputField, s)) |||
           (duration >>: fun n -> DurationConst n)) |||
         blanks -- strinG "and" -- blanks --
         (strinG "stops" ||| strinG "stopping" |||
          strinG "ends" ||| strinG "ending") -- blanks --
         strinG "at" -- blanks -+
           (non_keyword ++ scale >>: fun (n, s) ->
              StopField (n, ref OutputField, s)))) >>:
      fun ((sta, sca), dur) -> (sta, ref OutputField, sca), dur
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

  let merge_clause m =
    let m = "merge clause" :: m in
    (strinG "merge" -- blanks -- strinG "on" -- blanks -+
     several ~sep:list_sep E.Parser.p ++ optional ~def:0. (
       blanks -- strinG "timeout" -- blanks -- strinG "after" -- blanks -+
       duration)) m

  let sort_clause m =
    let m = "sort clause" :: m in
    (strinG "sort" -- blanks -- strinG "last" -- blanks -+
     pos_decimal_integer "Sort buffer size" ++
     optional ~def:None (
       blanks -- strinG "or" -- blanks -- strinG "until" -- blanks -+
       some E.Parser.p) +- blanks +-
     strinG "by" +- blanks ++ several ~sep:list_sep E.Parser.p >>:
      fun ((l, u), b) -> l, u, b) m

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
    let kv_list m =
      let m = "key-value list" :: m in
      (quoted_string +- opt_blanks +- (char ':' ||| char '=') +-
       opt_blanks ++ quoted_string) m in
    let opt_with = optional ~def:() (blanks -- strinG "with") in
    let notify_cmd m =
      let severity m =
        let m = "notification severity" :: m in
        ((strinG "urgent" >>: fun () -> Urgent) |||
         (strinG "deferrable" >>: fun () -> Deferrable)) m in
      let m = "notification" :: m in
      (strinG "notify" -- blanks -+ quoted_string ++
       optional ~def:Urgent (blanks -+ severity) ++
       optional ~def:[]
         (opt_with -- blanks -- strinGs "parameter" -- blanks -+
          several ~sep:list_sep_and kv_list) >>:
      fun ((notif_name, severity), parameters) ->
        { notif_name ; severity ; parameters }) m
    in
    let m = "notification clause" :: m in
    (notify_cmd >>: fun s -> NotifySpec s) m

  let flush m =
    let m = "flush clause" :: m in
    ((strinG "flush" >>: fun () -> Reset) |||
     (strinG "slide" -- blanks -+ (pos_decimal_integer "Sliding amount" >>:
        fun n -> Slide n)) |||
     (strinG "keep" -- blanks -- strinG "all" >>: fun () ->
       Never) |||
     (strinG "keep" -- blanks -+ E.Parser.p >>: fun e ->
       KeepOnly e) |||
     (strinG "remove" -- blanks -+ E.Parser.p >>: fun e ->
       RemoveAll e) >>:
     fun s -> FlushSpec s) m

  let dummy_commit m =
    (strinG "commit" >>: fun () -> CommitSpec) m

  let default_commit_cond = E.expr_true

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

  let net_protocol m =
    let m = "network protocol" :: m in
    ((strinG "collectd" >>: fun () -> RamenProtocols.Collectd) |||
     ((strinG "netflow" ||| strinG "netflowv5") >>: fun () ->
        RamenProtocols.NetflowV5)) m

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

  let listen_clause m =
    let m = "listen on operation" :: m in
    (strinG "listen" -- blanks --
     optional ~def:() (strinG "for" -- blanks) -+
     net_protocol ++
     optional ~def:None (
       blanks --
       optional ~def:() (strinG "on" -- blanks) -+
       some (inet_addr ++
             optional ~def:None (
              char ':' -+
              some (decimal_integer_range ~min:0 ~max:65535
                      "port number")))) >>:
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
     optional ~def:() (strinG "for" -- blanks) --
     strinG "instrumentation") m

  (* FIXME: It should be possible to enter separator, null, preprocessor in any order *)
  let read_file_specs m =
    let m = "read file operation" :: m in
    (strinG "read" -- blanks -+
     optional ~def:false (
       strinG "and" -- blanks -- strinG "delete" -- blanks >>:
         fun () -> true) +-
     (strinG "file" ||| strinG "files") +- blanks ++
     quoted_string >>: fun (unlink, fname) ->
       { unlink ; fname }) m

  let csv_specs m =
    let m = "CSV format" :: m in
    (optional ~def:"," (
       strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) ++
     optional ~def:"" (
       strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) +-
     char '(' +- opt_blanks ++
       several ~sep:list_sep RamenTuple.Parser.field +-
     opt_blanks +- char ')' >>:
     fun ((separator, null), fields) ->
       if separator = null || separator = "" then
         raise (Reject "Invalid CSV separator") ;
       { separator ; null ; fields }) m

  let preprocessor_clause m =
    let m = "file preprocessor" :: m in
    (strinG "preprocess" -- blanks -- strinG "with" -- opt_blanks -+
     quoted_string) m

  let factor_clause m =
    let m = "factors" :: m in
    ((strinG "factor" ||| strinG "factors") -- blanks -+
     several ~sep:list_sep_and non_keyword) m

  type select_clauses =
    | SelectClause of selected_field option list
    | MergeClause of (E.t list * float)
    | SortClause of (int * E.t option (* until *) * E.t list (* by *))
    | WhereClause of E.t
    | EventTimeClause of RamenEventTime.t
    | FactorClause of string list
    | GroupByClause of E.t list
    | CommitClause of (commit_spec list * (bool (* before *) * E.t))
    | FromClause of data_source list
    | EveryClause of float
    | ListenClause of (Unix.inet_addr * int * RamenProtocols.net_protocol)
    | InstrumentationClause
    | ExternalDataClause of file_spec
    | PreprocessorClause of string
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
      (instrumentation_clause >>: fun () -> InstrumentationClause) |||
      (read_file_specs >>: fun c -> ExternalDataClause c) |||
      (preprocessor_clause >>: fun c -> PreprocessorClause c) |||
      (csv_specs >>: fun c -> CsvSpecsClause c) |||
      (factor_clause >>: fun c -> FactorClause c) in
    (several ~sep:blanks part >>: fun clauses ->
      (* Used for its address: *)
      let default_select_fields = []
      and default_star = true
      and default_merge = [], 0.
      and default_sort = None
      and default_where = E.expr_true
      and default_event_time = None
      and default_key = []
      and default_commit = ([], (false, default_commit_cond))
      and default_from = []
      and default_every = 0.
      and default_listen = None
      and default_instrumentation = false
      and default_ext_data = None
      and default_preprocessor = ""
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
               instrumentation, ext_data, preprocessor, csv_specs, factors) ->
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
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | MergeClause merge ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | SortClause sort ->
              select_fields, and_all_others, merge, Some sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | WhereClause where ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | EventTimeClause event_time ->
              select_fields, and_all_others, merge, sort, where,
              Some event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | GroupByClause key ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | CommitClause commit' ->
              if commit != default_commit then
                raise (Reject "Cannot have several commit clauses") ;
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit', from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | FromClause from' ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, (List.rev_append from' from),
              every, listen, instrumentation, ext_data, preprocessor,
              csv_specs, factors
            | EveryClause every ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | ListenClause l ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, Some l,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | InstrumentationClause ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen, true,
              ext_data, preprocessor, csv_specs, factors
            | ExternalDataClause c ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, Some c, preprocessor, csv_specs, factors
            | PreprocessorClause preprocessor ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
            | CsvSpecsClause c ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, Some c, factors
            | FactorClause factors ->
              select_fields, and_all_others, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs, factors
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
      and not_instrumentation = instrumentation = false
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
        Instrumentation { from }
      else
        raise (Reject "Incompatible mix of clauses")
    ) m

  (*$= p & ~printer:(test_printer print)
    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.(Field (typ, ref TupleIn, "start")) ;\
            alias = "start" } ;\
          { expr = E.(Field (typ, ref TupleIn, "stop")) ;\
            alias = "stop" } ;\
          { expr = E.(Field (typ, ref TupleIn, "itf_clt")) ;\
            alias = "itf_src" } ;\
          { expr = E.(Field (typ, ref TupleIn, "itf_srv")) ;\
            alias = "itf_dst" } ] ;\
        and_all_others = false ;\
        merge = [], 0. ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        notifications = [] ;\
        key = [] ;\
        commit_cond = replace_typ E.expr_true ;\
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
        merge = [], 0. ;\
        sort = None ;\
        where = E.(\
          StatelessFun2 (typ, Gt, \
            Field (typ, ref TupleIn, "packets"),\
            Const (typ, VU32 Uint32.zero))) ;\
        event_time = None ; notifications = [] ;\
        key = [] ;\
        commit_cond = replace_typ E.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (26, [])))\
      (test_op p "from foo where packets > 0" |> replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.(Field (typ, ref TupleIn, "t")) ;\
            alias = "t" } ;\
          { expr = E.(Field (typ, ref TupleIn, "value")) ;\
            alias = "value" } ] ;\
        and_all_others = false ;\
        merge = [], 0. ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = Some (("t", ref RamenEventTime.OutputField, 10.), DurationConst 60.) ;\
        notifications = [] ;\
        key = [] ;\
        commit_cond = replace_typ E.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (65, [])))\
      (test_op p "from foo select t, value event starting at t*10 with duration 60s" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.(Field (typ, ref TupleIn, "t1")) ;\
            alias = "t1" } ;\
          { expr = E.(Field (typ, ref TupleIn, "t2")) ;\
            alias = "t2" } ;\
          { expr = E.(Field (typ, ref TupleIn, "value")) ;\
            alias = "value" } ] ;\
        and_all_others = false ;\
        merge = [], 0. ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = Some (("t1", ref RamenEventTime.OutputField, 10.), \
                           StopField ("t2", ref RamenEventTime.OutputField, 10.)) ;\
        notifications = [] ; key = [] ;\
        commit_cond = replace_typ E.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (75, [])))\
      (test_op p "from foo select t1, t2, value event starting at t1*10 and stopping at t2*10" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [] ;\
        and_all_others = true ;\
        merge = [], 0. ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = None ;\
        notifications = [ \
          { notif_name = "ouch" ; severity = Urgent ; parameters = [] } ] ;\
        key = [] ;\
        commit_cond = replace_typ E.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
      (22, [])))\
      (test_op p "from foo NOTIFY \"ouch\"" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.(\
              StatefulFun (typ, LocalState, AggrMin (\
                Field (typ, ref TupleIn, "start")))) ;\
            alias = "start" } ;\
          { expr = E.(\
              StatefulFun (typ, LocalState, AggrMax (\
                Field (typ, ref TupleIn, "stop")))) ;\
            alias = "max_stop" } ;\
          { expr = E.(\
              StatelessFun2 (typ, Div, \
                StatefulFun (typ, LocalState, AggrSum (\
                  Field (typ, ref TupleIn, "packets"))),\
                Field (typ, ref TupleParam, "avg_window"))) ;\
            alias = "packets_per_sec" } ] ;\
        and_all_others = false ;\
        merge = [], 0. ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = None ; \
        notifications = [] ;\
        key = [ E.(\
          StatelessFun2 (typ, Div, \
            Field (typ, ref TupleIn, "start"),\
            StatelessFun2 (typ, Mul, \
              Const (typ, VU32 (Uint32.of_int 1_000_000)),\
              Field (typ, ref TupleParam, "avg_window")))) ] ;\
        commit_cond = E.(\
          StatelessFun2 (typ, Gt, \
            StatelessFun2 (typ, Add, \
              StatefulFun (typ, LocalState, AggrMax (\
                Field (typ, ref TupleGroupFirst, "start"))),\
              Const (typ, VU32 (Uint32.of_int 3600))),\
            Field (typ, ref TupleOut, "start"))) ; \
        commit_before = false ;\
        flush_how = Reset ;\
        from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] },\
        (199, [])))\
        (test_op p "select min start as start, \\
                           max stop as max_stop, \\
                           (sum packets)/avg_window as packets_per_sec \\
                   from foo \\
                   group by start / (1_000_000 * avg_window) \\
                   commit after out.start < (max group.first.start) + 3600" |>\
         replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = E.Const (typ, VU32 Uint32.one) ;\
            alias = "one" } ] ;\
        and_all_others = false ;\
        merge = [], 0. ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = None ; \
        notifications = [] ;\
        key = [] ;\
        commit_cond = E.(\
          StatelessFun2 (typ, Ge, \
            StatefulFun (typ, LocalState, AggrSum (\
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
          { expr = E.Field (typ, ref TupleIn, "n") ; alias = "n" } ;\
          { expr = E.(\
              StatefulFun (typ, GlobalState, E.Lag (\
              E.Const (typ, VU32 (Uint32.of_int 2)), \
              E.Field (typ, ref TupleIn, "n")))) ;\
            alias = "l" } ] ;\
        and_all_others = false ;\
        merge = [], 0. ;\
        sort = None ;\
        where = E.Const (typ, VBool true) ;\
        event_time = None ; \
        notifications = [] ;\
        key = [] ;\
        commit_cond = replace_typ E.expr_true ;\
        commit_before = false ;\
        from = [NamedOperation (Some (RamenName.program_of_string "foo", []), RamenName.func_of_string "bar")] ;\
        flush_how = Reset ; every = 0. ; factors = [] },\
        (37, [])))\
        (test_op p "SELECT n, lag(2, n) AS l FROM foo/bar" |>\
         replace_typ_in_op)

    (Ok (\
      ReadCSVFile { where = { fname = "/tmp/toto.csv" ; unlink = false } ; \
                    preprocessor = "" ; event_time = None ; \
                    what = { \
                      separator = "," ; null = "" ; \
                      fields = [ \
                        { typ_name = "f1" ; typ = { structure = TBool ; nullable = Some true } } ;\
                        { typ_name = "f2" ; typ = { structure = TI32 ; nullable = Some false } } ] } ;\
                    factors = [] },\
      (44, [])))\
      (test_op p "read file \"/tmp/toto.csv\" (f1 bool?, f2 i32)")

    (Ok (\
      ReadCSVFile { where = { fname = "/tmp/toto.csv" ; unlink = true } ; \
                    preprocessor = "" ; event_time = None ; \
                    what = { \
                      separator = "," ; null = "" ; \
                      fields = [ \
                        { typ_name = "f1" ; typ = { structure = TBool ; nullable = Some true } } ;\
                        { typ_name = "f2" ; typ = { structure = TI32 ; nullable = Some false } } ] } ;\
                    factors = [] },\
      (55, [])))\
      (test_op p "read and delete file \"/tmp/toto.csv\" (f1 bool?, f2 i32)")

    (Ok (\
      ReadCSVFile { where = { fname = "/tmp/toto.csv" ; unlink = false } ; \
                    preprocessor = "" ; event_time = None ; \
                    what = { \
                      separator = "\t" ; null = "<NULL>" ; \
                      fields = [ \
                        { typ_name = "f1" ; typ = { structure = TBool ; nullable = Some true } } ;\
                        { typ_name = "f2" ; typ = { structure = TI32 ; nullable = Some false } } ] } ;\
                    factors = [] },\
      (73, [])))\
      (test_op p "read file \"/tmp/toto.csv\" \\
                      separator \"\\t\" null \"<NULL>\" \\
                      (f1 bool?, f2 i32)")

    (Ok (\
      Aggregate {\
        fields = [ { expr = E.Const (typ, VU32 Uint32.one) ; alias = "one" } ] ;\
        every = 1. ; event_time = None ;\
        and_all_others = false ; merge = [], 0. ; sort = None ;\
        where = E.Const (typ, VBool true) ;\
        notifications = [] ; key = [] ;\
        commit_cond = replace_typ E.expr_true ;\
        commit_before = false ; flush_how = Reset ; from = [] ;\
        factors = [] },\
        (29, [])))\
        (test_op p "YIELD 1 AS one EVERY 1 SECOND" |>\
         replace_typ_in_op)
  *)

  (*$>*)
end
