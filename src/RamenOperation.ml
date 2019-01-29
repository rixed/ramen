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
module T = RamenTypes

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint
*)

(* Represents what happens to a group after its value is output: *)
type flush_method =
  | Reset (* it can be deleted (tumbling windows) *)
  | Never (* or we may just keep the group as it is *)
  [@@ppp PPP_OCaml]

let print_flush_method oc m =
  String.print oc (match m with Reset -> "FLUSH" | Never -> "KEEP")

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

(* Type of a function: *)

type t =
  (* Aggregation of several tuples into one based on some key. Superficially
   * looks like a select but much more involved. Most clauses being optional,
   * this is really the Swiss-army knife for all data manipulation in Ramen: *)
  | Aggregate of {
      output : E.t ;
      merge : merge ;
      (* Optional buffering of N tuples for sorting according to some
       * expression: *)
      sort : (int * E.t option (* until *) * E.t list (* by *)) option ;
      (* Simple way to filter out incoming tuples: *)
      where : E.t ;
      (* How to compute the time range for that event: *)
      event_time : RamenEventTime.t option ;
      (* Will send these notification to the alerter: *)
      notifications : E.t list ;
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
  | Aggregate { output ; merge ; sort ; where ; notifications ; key ;
                commit_cond ; commit_before ; flush_how ; from ; every ; _ } ->
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
    Printf.fprintf oc " SELECT %a" (E.print false) output ;
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
        List.print ~first:!sep ~last:"" ~sep:!sep
          (fun oc n -> Printf.fprintf oc "NOTIFY %a" (E.print false) n)
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

module Env = struct

  open E.Env

  (* Different top expressions start with a different environment: some
   * expressions have access to In, other to Out, most have access to Params,
   * etc. This is unrelated to short-hands (foo -> param.foo), which is
   * another process of AST rewrite independent of the environment. *)

  let map_top f env op =
    match op with
    | ListenFor _ | Instrumentation _ | Notifications _ -> op

    | ReadCSVFile ({ where = { fname ; unlink } ; preprocessor ; _ } as o) ->
        let env = env_env :: env_param :: env in
        let preprocessor =
          Option.map (f "CSV preprocessor" env) preprocessor in
        let fname = f "CSV filename" env fname in
        let unlink = f "CSV DELETE-IF clause" env unlink in
        ReadCSVFile { o with where = { fname ; unlink } ; preprocessor }

    | Aggregate ({ output ; merge ; sort ; where ; key ; commit_cond ;
                   notifications ; _ } as o) ->
        let env = env_env :: env_param :: env in
        let output =
          let env = env_in :: env_group :: env_previous :: env in
          f "SELECT clause" env output in
        let merge_on =
          List.map (fun e ->
            f "MERGE-ON clause" (env_in :: env) e
          ) merge.on in
        let where =
          let env =
            env_in :: env_group :: env_previous :: env_greatest :: env in
          f "WHERE clause" env where in
        let key =
          List.map (fun e ->
            f "GROUP-BY clause" (env_in :: env) e
          ) key in
        let notifications =
          List.map (fun e ->
            f "NOTIFY" (env_in :: env) e
          ) notifications in
        let commit_cond =
          f "COMMIT clause" (env_in :: env_previous :: env_group :: env)
            commit_cond in
        let sort =
          Option.map (fun (n, u_opt, b) ->
            let u_opt =
              Option.map (fun u ->
                let env = env_first :: env_smallest :: env_greatest :: env in
                f "SORT-UNTIL clause" env u
              ) u_opt in
            let b =
              List.map (fun e -> f "SORT-BY clause" (env_in :: env) e) b in
            n, u_opt, b
          ) sort in
        Aggregate { o with
          output ; merge = { merge with on = merge_on } ; sort ; where ;
          key ; commit_cond ; notifications }

  let map f env op = map_top (E.map f) env op

  let fold_top f i env = function
    | ListenFor _ | Instrumentation _ | Notifications _ -> i
    | ReadCSVFile { where = { fname ; unlink } ; preprocessor ; _ } ->
        let env = env_env :: env_param :: env in
        let i =
          Option.map_default (f "CSV preprocessor" i env) i preprocessor in
        let i = f "CSV filename" i env fname in
        f "CSV DELETE-IF clause" i env unlink
    | Aggregate { output ; merge ; sort ; where ; key ; commit_cond ;
                  notifications ; _ } ->
        let env = env_env :: env_param :: env in
        let i =
          let env = env_in :: env_group :: env_previous :: env in
          f "SELECT clause" i env output in
        let i =
          List.fold_left (fun i e ->
            f "MERGE-ON clause" i (env_in :: env) e
          ) i merge.on in
        let i =
          let env =
            env_in :: env_group :: env_previous :: env_greatest :: env in
          f "WHERE clause" i env where in
        let i =
          List.fold_left (fun i e ->
            f "GROUP-BY clause" i (env_in :: env) e
          ) i key in
        let i =
          List.fold_left (fun i e ->
            f "NOTIFY" i (env_in :: env) e
          ) i notifications in
        let i =
          f "COMMIT clause" i (env_in :: env_previous :: env_group :: env)
            commit_cond in
        let i = match sort with
          | None -> i
          | Some (_, u_opt, b) ->
              let i = match u_opt with
                | None -> i
                | Some u ->
                    let env =
                      env_first :: env_smallest :: env_greatest :: env in
                    f "SORT-UNTIL clause" i env u in
              List.fold_left (fun i e ->
                f "SORT-BY clause" i (env_in :: env) e
              ) i b in
        i

  (* Fold over all expressions, maintaining the dynamic environment: *)
  let fold f = fold_top (fun what -> E.Env.fold (f what))

  let iter f = fold_top (fun what () -> E.Env.iter (f what)) ()
end

(* Same as above but with no environment: *)
let fold_top_level_expr i f op =
  Env.fold_top (fun what i _env e -> f i what e) i [] op

let iter_top_level_expr f = fold_top_level_expr () (fun () -> f)

let fold_expr ?(expr_folder=E.fold_up) init f =
  fold_top_level_expr init (fun i _ -> expr_folder f i)

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
    | Aggregate { event_time ; output ; _ } ->
        event_time,
        (match output.text with
        | Record (_, sfs) ->
            List.map (fun sf -> sf.E.alias) sfs
        | _ -> [])
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

let operation_with_event_time op event_time = match op with
  | Aggregate s -> Aggregate { s with event_time }
  | ReadCSVFile s -> ReadCSVFile { s with event_time }
  | ListenFor _ -> op
  | Instrumentation _ -> op
  | Notifications _ -> op

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

let operation_with_factors op factors = match op with
  | ReadCSVFile s -> ReadCSVFile { s with factors }
  | Aggregate s -> Aggregate { s with factors }
  | ListenFor s -> ListenFor { s with factors }
  | Instrumentation _ -> op
  | Notifications _ -> op

(* Return the (likely untyped) output tuple *)
let out_type_of_operation = function
  | Aggregate { output ; _ } ->
      output.E.typ
  | ReadCSVFile { what = { fields ; _ } ; _ } ->
      T.make ~nullable:false (T.TRecord (
        List.enum fields /@
        (fun ft -> RamenName.string_of_field ft.RamenTuple.name,
                   ft.RamenTuple.typ) |>
        Array.of_enum))
  | ListenFor { proto ; _ } ->
      RamenProtocols.typ_of_proto proto
  | Instrumentation _ ->
      RamenBinocle.typ
  | Notifications _ ->
      RamenNotification.typ

(* Often time we want to iterate over the "fields" of the output record: *)
let fields_of_operation = function
  | Aggregate { output ; _ } ->
      E.fields_of_expression output
  | _ -> Enum.empty ()

(* Often time we want to iterate over the "fields" of the output record: *)
let field_types_of_operation =
  T.fields_of_type % out_type_of_operation

(* Returns the list of environment variables (as in: UNIX environment)
 * that are used. We find this by looking for bindings to the "env" variable
 * in the AST environment. Don't be confused. *)
let envvars_of_operation op =
  Env.fold (fun what s env e ->
    match e.text with
    (* Will skip over "env.foo.bar", while a warning would be nice. *)
    | Stateless (SL2 (Get, { text = Const (VString field_name) ; _ },
                           { text = Variable var_name ; _ })) ->
        (match E.Env.lookup what env var_name with
        | exception Not_found -> E.Env.unbound_var what env var_name
        | TupleEnv -> Set.add (RamenName.field_of_string field_name) s
        | _ -> s)
    | _ -> s) Set.empty [] op |>
  Set.to_list

let use_event_time op =
  try
    iter_expr (fun e ->
      match e.text with
      | Stateless (SL0 (EventStart|EventStop)) -> raise Exit
      | _ -> ()
    ) op ;
    false
  with Exit -> true

(* Check that the expression is valid, or return an error message.
 * Also perform some transformations such as grounding loose variables. *)
let check params op =
  let check_pure clause =
    E.unpure_iter (fun _ ->
      failwith ("Stateful function not allowed in "^ clause))
  and check_no_group clause =
    E.unpure_iter (fun e ->
      match e.text with
      | Stateful (LocalState, _, _) ->
          failwith ("Aggregate function not allowed in "^ clause)
      | _ -> ())
  in
  (* Check the operation-related constraints: *)
  (match op with
  | Aggregate { where ; key ; from ; every ; _ } ->
    (* Disallow group state in WHERE because it makes no sense: *)
    check_no_group "WHERE clause" where ;
    List.iter (check_pure "GROUP-BY clause") key ;
    if every > 0. && from <> [] then
      failwith "Cannot have both EVERY and FROM"

  | ReadCSVFile { where = { unlink ; _ } ; _ } ->
    check_pure "DELETE-IF" unlink
    (* FIXME: check the field type declarations use only scalar types *)

  | ListenFor _ | Instrumentation _ | Notifications _ -> ()) ;
  (* Ground loose variables: *)
  let op =
    Env.map_top (fun what env e ->
      E.Env.ground_on params what env e) [] op in
  (* Check all expressions with proper environment: *)
  Env.iter E.check [] op ;
  op

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let event_time_clause m =
    let m = "event time clause" :: m in
    let scale m =
      let m = "scale event field" :: m in
      (
        optional ~def:1. (
          optional ~def:() blanks -- star --
          optional ~def:() blanks -+ number)
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
    (
      strinG "every" -- blanks -+ duration >>:
      fun every ->
        if every < 0. then
          raise (Reject "sleep duration must be greater than 0") ;
        every
    ) m

  let select_clause m =
    let m = "select clause" :: m in
    (
      (strinG "select" ||| strinG "yield") -- blanks -+ E.Parser.p
    ) m

  (* Contrary to #start, which is out.start! *)
  let in_start () =
    E.(make (Stateless (SL2 (Get,
      E.of_string "start",
      make (Variable (RamenName.field_of_string "in"))))))

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
          if on = [] then [ in_start () ] else on in
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
          if b = [] then [ in_start () ] else b in
        l, u, b
    ) m

  let where_clause m =
    let m = "where clause" :: m in
    (
      (strinG "where" ||| strinG "when") -- blanks -+ E.Parser.p
    ) m

  let group_by m =
    let m = "group-by clause" :: m in
    (
      strinG "group" -- blanks -- strinG "by" -- blanks -+
      several ~sep:list_sep E.Parser.p
    ) m

  type commit_spec =
    | NotifySpec of E.t
    | FlushSpec of flush_method
    | CommitSpec (* we would commit anyway, just a placeholder *)

  let notification_clause m =
    let m = "notification" :: m in
    (
      strinG "notify" -- blanks -+
      optional ~def:None (some E.Parser.p) >>: fun name ->
        NotifySpec (name |? E.of_string "Don't Panic!")
    ) m

  let flush m =
    let m = "flush clause" :: m in
    (
      (strinG "flush" >>: fun () -> Reset) |||
      (
        strinG "keep" -- optional ~def:() (blanks -- strinG "all") >>:
        fun () -> Never
      ) >>:
      fun s -> FlushSpec s
    ) m

  let dummy_commit =
    strinG "commit" >>: fun () -> CommitSpec

  let default_commit_cond = E.of_bool true

  let commit_clause m =
    let m = "commit clause" :: m in
    (
      several ~sep:list_sep_and ~what:"commit clauses" (
        dummy_commit ||| notification_clause ||| flush
      ) ++
      optional ~def:(false, default_commit_cond) (
        blanks -+ (
          (strinG "after" >>: fun _ -> false) |||
          (strinG "before" >>: fun _ -> true)
        ) +- blanks ++ E.Parser.p
      )
    ) m

  let default_port_of_protocol =
    let open RamenProtocols in
    function
    | Collectd -> 25826
    | NetflowV5 -> 2055
    | Graphite -> 2003

  let net_protocol m =
    let m = "network protocol" :: m in
    (
      (strinG "collectd" >>: fun () -> RamenProtocols.Collectd) |||
      (
        (strinG "netflow" ||| strinG "netflowv5") >>:
        fun () -> RamenProtocols.NetflowV5
      ) ||| (
        strinG "graphite" >>: fun () -> RamenProtocols.Graphite
      )
    ) m

  let network_address =
    several ~sep:none (
      cond "inet address" (fun c ->
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
    (
      (string "*" >>: fun () -> Unix.inet_addr_any) |||
      (string "[*]" >>: fun () -> Unix.inet6_addr_any) |||
      network_address
    ) m

  let host_port m =
    let m = "host and port" :: m in
    (
      inet_addr ++
      optional ~def:None (
        char ':' -+
        some (decimal_integer_range ~min:0 ~max:65535 "port number")
      )
    ) m

  let listen_clause m =
    let m = "listen on operation" :: m in
    (
      strinG "listen" -- blanks --
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
        net_addr, port, proto
    ) m

  let instrumentation_clause m =
    let m = "read instrumentation operation" :: m in
    (
      strinG "listen" -- blanks --
      optional ~def:() (strinG "for" -- blanks) -+
      (that_string "instrumentation" ||| that_string "notifications")
    ) m

  let fields_schema m =
    let m = "tuple schema" :: m in
    (
      char '(' -- opt_blanks -+
      several ~sep:list_sep RamenTuple.Parser.field +- opt_blanks +-
      char ')'
    ) m

  (* FIXME: It should be allowed to enter separator, null, preprocessor in
   * any order *)
  let read_file_specs m =
    let m = "read file operation" :: m in
    (
      strinG "read" -- blanks -+
      optional ~def:(E.of_bool false) (
        strinG "and" -- blanks -- strinG "delete" -- blanks -+
        optional ~def:(E.of_bool true) (
          strinG "if" -- blanks -+ E.Parser.p +- blanks)) +-
      strinGs "file" +- blanks ++
      E.Parser.p >>:
      fun (unlink, fname) -> { unlink ; fname }
    ) m

  let csv_specs m =
    let m = "CSV format" :: m in
    (
      optional ~def:"," (
        strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) ++
      optional ~def:"" (
        strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) ++
      fields_schema >>:
      fun ((separator, null), fields) ->
        if separator = null || separator = "" then
          raise (Reject "Invalid CSV separator") ;
        { separator ; null ; fields }
    ) m

  let preprocessor_clause m =
    let m = "file preprocessor" :: m in
    (
      strinG "preprocess" -- blanks -- strinG "with" -- opt_blanks -+
      E.Parser.p
    ) m

  let factor_clause m =
    let m = "factors" :: m
    and field = non_keyword >>: RamenName.field_of_string in
    (
      (strinG "factor" ||| strinG "factors") -- blanks -+
      several ~sep:list_sep_and field
    ) m

  type select_clauses =
    | SelectClause of E.t
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
    (
      unquoted ||| quoted
    ) m

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
    (* Used for its address: *)
    let default_select = E.null ()
    and default_merge = { last = 1 ; on = [] ; timeout = 0. }
    and default_sort = None
    and default_where = E.of_bool true
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
      default_select, default_merge, default_sort,
      default_where, default_event_time, default_key,
      default_commit, default_from, default_every,
      default_listen, default_instrumentation, default_ext_data,
      default_preprocessor, default_csv_specs, default_factors in
    (
      several ~sep:blanks part >>:
      fun clauses ->
      let select, merge, sort, where,
          event_time, key, commit, from, every, listen, instrumentation,
          ext_data, preprocessor, csv_specs, factors =
        List.fold_left (
          fun (select, merge, sort, where,
               event_time, key, commit, from, every, listen,
               instrumentation, ext_data, preprocessor, csv_specs,
               factors) ->
            (* FIXME: in what follows, detect and signal cases when a new value
             * replaces an old one (but the default), such as when two WHERE
             * clauses are given. *)
            function
            | SelectClause select ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | MergeClause merge ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | SortClause sort ->
              select, merge, Some sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | WhereClause where ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | EventTimeClause event_time ->
              select, merge, sort, where,
              Some event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | GroupByClause key ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | CommitClause commit' ->
              if commit != default_commit then
                raise (Reject "Cannot have several commit clauses") ;
              select, merge, sort, where,
              event_time, key, commit', from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | FromClause from' ->
              select, merge, sort, where,
              event_time, key, commit, (List.rev_append from' from),
              every, listen, instrumentation, ext_data, preprocessor,
              csv_specs, factors
            | EveryClause every ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | ListenClause l ->
              select, merge, sort, where,
              event_time, key, commit, from, every, Some l,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | InstrumentationClause c ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen, c,
              ext_data, preprocessor, csv_specs, factors
            | ExternalDataClause c ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, Some c, preprocessor, csv_specs,
              factors
            | PreprocessorClause preprocessor ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, csv_specs,
              factors
            | CsvSpecsClause c ->
              select, merge, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, ext_data, preprocessor, Some c,
              factors
            | FactorClause factors ->
              select, merge, sort, where,
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
        select == default_select && sort == default_sort &&
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
        Aggregate { output = select ; merge ; sort ; where ; event_time ;
                    notifications ; key ; commit_before ; commit_cond ;
                    flush_how ; from ; every ; factors }
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
        commit_cond = replace_typ (E.of_bool true) ;\
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
        commit_cond = replace_typ (E.of_bool true) ;\
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
        commit_cond = replace_typ (E.of_bool true) ;\
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
        commit_cond = replace_typ (E.of_bool true) ;\
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
        notifications = [ E.Const (typ, VString "ouch") ] ;\
        key = [] ;\
        commit_cond = replace_typ (E.of_bool true) ;\
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
        commit_cond = replace_typ (E.of_bool true) ;\
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
        commit_cond = replace_typ (E.of_bool true) ;\
        commit_before = false ; flush_how = Reset ; from = [] ;\
        factors = [] },\
        (29, [])))\
        (test_op p "YIELD 1 AS one EVERY 1 SECOND" |>\
         replace_typ_in_op)
  *)

  (*$>*)
end
