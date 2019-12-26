(* This module parses operations (and offer a few utilities related to
 * operations).
 *
 * An operation is the body of a function, ie. the actual operation that
 * workers will execute.
 *
 * The main operation is the `SELECT / GROUP BY` operation, but there are a
 * few others of lesser importance for data input and output.
 *
 * Operations are made of expressions, parsed in RamenExpr, and assembled
 * into programs (the compilation unit) in RamenProgram.
 *)
open Batteries
open RamenLang
open RamenHelpers
open RamenLog
open RamenConsts
module E = RamenExpr
module T = RamenTypes
module Globals = RamenGlobalVariables

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint
*)

(* Represents an output field from the select clause
 * 'SELECT expr AS alias' *)
type selected_field =
  { expr : E.t ;
    alias : N.field ;
    doc : string ;
    (* FIXME: Have a variant and use it in RamenTimeseries as well. *)
    aggr : string option }

let print_selected_field with_types oc f =
  let need_alias =
    match f.expr.text with
    | Stateless (SL0 (Path [ Name n ]))
      when f.alias = n -> false
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                           { text = Variable In ; _ }))
      when (f.alias :> string) = n -> false
    | _ -> true in
  if need_alias then (
    Printf.fprintf oc "%a AS %s"
      (E.print with_types) f.expr
      (f.alias :> string) ;
    if f.doc <> "" then Printf.fprintf oc " %S" f.doc
  ) else (
    E.print with_types oc f.expr ;
    if f.doc <> "" then Printf.fprintf oc " DOC %S" f.doc
  )

(* Represents what happens to a group after its value is output: *)
type flush_method =
  | Reset (* it can be deleted (tumbling windows) *)
  | Never (* or we may just keep the group as it is *)

let print_flush_method oc = function
  | Reset ->
    Printf.fprintf oc "FLUSH"
  | Never ->
    Printf.fprintf oc "KEEP"

(* External data sources:
 * When not SELECTing from other ramen workers or LISTENing to known protocols
 * a worker can READ data from an external source. In that case, not only the
 * external source must be described (see external_source) but also the
 * format describing how the data is encoded must be specified (see
 * external_format).
 * In theory, both are independent, but in practice of course the container
 * specification leaks into the format specification. *)
type external_source =
  | File of file_specs
  | Kafka of kafka_specs
  (* TODO: others such as Fifo... *)

and file_specs =
  { fname : E.t ;
    preprocessor : E.t option ;
    unlink : E.t }

(* The consumer is configured with the standard configuration
 * parameters, of which "metadata.broker.list" is mandatory.
 * See https://kafka.apache.org/documentation.html#consumerconfigs
 * In particular, pay attention to "bootstrap.servers" (can be used to get
 * to the actual leader for the partition, as usual with Kafka),
 * "group.id" (the consumer group name), "client.id" to help reading Kafka's
 * logs.
 *
 * Regarding consumer groups:
 * The easiest is to use only one consumer and one consumer group. In
 * that case, that worker will receive all messages from all partitions.
 * But we may want instead to partition Kafka's topic with the key we
 * intend to group by (or just part of that key), and start several
 * workers. Now if all those workers have the same consumer group, they
 * will be send all messages from distinct partitions, thus parallelizing
 * the work.
 * If two different functions both wants to read from the same topic,
 * each of these workers willing to receive all the messages
 * notwithstanding the other workers also reading this very topic, the
 * different consumer group names have to be used.
 * A good value is the FQ name of the function.
 *
 * Regarding restarts:
 * By default, each worker saves its own kafka partitions offset in its
 * state file. The downside of course is that when that statefile is
 * obsoleted by a worker code change then the worker will have to restart
 * from fresh.
 * The other alternative is to store this offset in another file, thus
 * keeping it across code change. In that case the user specify the file
 * name in which the offset will be written in user friendly way, so she
 * can manage it herself (by deleting the file or manually altering that
 * offset).
 * Finally, it is also possible to use Kafka group coordinator to manage
 * those offset for us. *)
and kafka_specs =
  { options : (string * E.t) list ;
    topic : E.t ;
    partitions : E.t list ;
    restart_from : kafka_restart_specs }

and kafka_restart_specs =
  | Beginning
  (* Expecting a positive int here for negative offset from the end, as in
   * rdkafka lib: *)
  | OffsetFromEnd of E.t
  | SaveInState
  | UseKafkaGroupCoordinator of snapshot_period_specs

and snapshot_period_specs =
  { after_max_secs : E.t ; after_max_events : E.t }

let fold_snapshot_period_specs init f specs =
  let x = f init "snapshot-every-secs" specs.after_max_secs in
  f x "snapshot-every-events" specs.after_max_events

let fold_kafka_restart_specs init f = function
  | Beginning | SaveInState ->
      init
  | OffsetFromEnd e ->
      f init "offset-from-end" e
  | UseKafkaGroupCoordinator s ->
      fold_snapshot_period_specs init f s

let fold_external_source init f = function
  | File specs ->
      let x =
        Option.map_default (f init "preprocessor") init specs.preprocessor in
      let x = f x "filename" specs.fname in
      f x "DELETE-IF clause" specs.unlink
  | Kafka specs ->
      let x =
        List.fold_left (fun x (_, e) ->
          f x "kafka-option" e
        ) init specs.options in
      let x = f x "kafka-topic" specs.topic in
      let x =
        List.fold_left (fun x e ->
          f x "kafka-partitions" e
        ) x specs.partitions in
      fold_kafka_restart_specs x f specs.restart_from

let iter_external_source f =
  fold_external_source () (fun () -> f)

let map_snapshot_period_specs f specs =
  { after_max_secs = f specs.after_max_secs ;
    after_max_events = f specs.after_max_events }

let map_kafka_restart_specs f specs =
  match specs with
  | Beginning | SaveInState ->
      specs
  | OffsetFromEnd e ->
      OffsetFromEnd (f e)
  | UseKafkaGroupCoordinator s ->
      UseKafkaGroupCoordinator (map_snapshot_period_specs f s)

let map_external_source f = function
  | File { fname ; preprocessor ; unlink } ->
      File {
        fname = f fname ;
        preprocessor = Option.map f preprocessor ;
        unlink = f unlink }
  | Kafka { options ; topic ; partitions ; restart_from } ->
      Kafka {
        options = List.map (fun (n, e) -> n, f e) options ;
        topic = f topic ;
        partitions = List.map f partitions ;
        restart_from = map_kafka_restart_specs f restart_from }

let print_file_specs with_types oc specs =
  Printf.fprintf oc "FILES %a%s%a"
    (E.print with_types) specs.fname
    (Option.map_default (fun e ->
       Printf.sprintf2 " PREPROCESSED WITH %a" (E.print with_types) e
     ) "" specs.preprocessor)
    (fun oc unlink ->
      if not (E.is_false unlink) then
        if E.is_true unlink then
          Printf.fprintf oc " THEN DELETE"
        else
          Printf.fprintf oc " THEN DELETE IF %a" (E.print with_types) unlink)
      specs.unlink

let print_kafka_specs with_types oc specs =
  (* TODO: restart offset *)
  Printf.fprintf oc "KAFKA TOPIC %a"
    (E.print with_types) specs.topic ;
  if specs.partitions <> [] then
    Printf.fprintf oc " PARTITION%s %a"
      (if list_longer_than 1 specs.partitions then "S" else "")
      (pretty_list_print ~uppercase:true (E.print with_types))
        specs.partitions ;
  Printf.fprintf oc " WITH OPTIONS %a"
    (pretty_list_print ~uppercase:true (fun oc (n, e) ->
      Printf.fprintf oc "%S = %a" n (E.print with_types) e))
      specs.options

let print_external_source with_types oc = function
  | File specs ->
      print_file_specs with_types oc specs
  | Kafka specs ->
      print_kafka_specs with_types oc specs

type external_format =
  | CSV of csv_specs
  (* ClickHouse RowBinary format taken from NamesAndTypes.cpp for version 1 *)
  | RowBinary of RamenTuple.typ
  (* TODO: others such as Ringbuffer, Orc, Avro... *)

and csv_specs =
  { separator : string ;
    null : string ;
    (* If true, expect some quoted fields. Otherwise quotes are just part
     * of the value. *)
    may_quote : bool [@ppp_default false] ;
    escape_seq : string [@ppp_default ""] ;
    fields : RamenTuple.typ }

let fold_external_format init _f = function
  | CSV _ -> init
  | RowBinary _ -> init

let map_external_format _f x = x

let fields_of_external_format = function
  | CSV { fields ; _ }
  | RowBinary fields ->
      fields

let print_csv_specs oc specs =
  Printf.fprintf oc "AS CSV" ;
  if specs.separator <> Default.csv_separator then
    Printf.fprintf oc " SEPARATOR %S" specs.separator ;
  if specs.null <> Default.csv_null then
    Printf.fprintf oc " NULL %S" specs.null ;
  if not specs.may_quote then
    Printf.fprintf oc " NO QUOTES" ;
  if specs.escape_seq <> "" then
    Printf.fprintf oc " ESCAPE WITH %S" specs.escape_seq ;
  Printf.fprintf oc " %a"
    RamenTuple.print_typ specs.fields

let print_row_binary_specs oc fields =
  let print_type_as_clickhouse oc typ =
    if typ.T.nullable then Printf.fprintf oc "Nullable(" ;
    String.print oc (match typ.structure with
      | T.TU8 -> "UInt8"
      | T.TU16 -> "UInt16"
      | T.TU32 -> "UInt32"
      | T.TU64 -> "UInt64"
      | T.TU128 -> "UUID"
      | T.TI8 -> "Int8"
      | T.TI16 -> "Int16"
      | T.TI32 -> "Int32"
      | T.TI64 -> "Int64"
      | T.TI128 -> "Decimal128"
      | T.TFloat -> "Float64"
      | T.TString -> "String"
      | T.TVec (d, { nullable = false ; structure = TChar }) ->
          Printf.sprintf "FixedString(%d)" d
      | _ ->
          Printf.sprintf2 "ClickHouseFor(%a)" T.print_typ typ) ;
    if typ.nullable then Printf.fprintf oc ")" in
  Printf.fprintf oc "AS ROWBINARY\n" ;
  Printf.fprintf oc "  columns format version: 1\n" ;
  Printf.fprintf oc "  %d columns:" (List.length fields) ;
  List.iter (fun f ->
    Printf.fprintf oc "\n  `%a` %a"
      N.field_print f.RamenTuple.name
      print_type_as_clickhouse f.typ
  ) fields ;
  Printf.fprintf oc ";"

let print_external_format oc = function
  | CSV specs ->
      print_csv_specs oc specs
  | RowBinary fields ->
      print_row_binary_specs oc fields

(* Type of an operation: *)

type t =
  (* Aggregation of several tuples into one based on some key. Superficially
   * looks like a select but much more involved. Most clauses being optional,
   * this is really the Swiss-army knife for all data manipulation in Ramen: *)
  | Aggregate of {
      fields : selected_field list ; (* Composition of the output tuple *)
      and_all_others : bool ; (* also "select *" *)
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
      every : E.t option ;
      (* Fields with expected small dimensionality, suitable for breaking down
       * the time series: *)
      factors : N.field list }
  | ReadExternal of {
      source : external_source ;
      format : external_format ;
      event_time : RamenEventTime.t option ;
      factors : N.field list }
  | ListenFor of {
      net_addr : Unix.inet_addr ;
      port : int ;
      proto : RamenProtocols.net_protocol ;
      factors : N.field list }
  (* For those factors, event time etc are hardcoded, and data sources
   * can not be sub-queries: *)
  | Instrumentation of { from : data_source list }
  | Notifications of { from : data_source list }

(* Possible FROM sources: other function (optionally from another program),
 * sub-query or internal instrumentation: *)
and data_source =
  | NamedOperation of parent
  | SubQuery of t
  | GlobPattern of Globs.t

and parent =
  site_identifier * N.rel_program option * N.func

and site_identifier =
  | AllSites
  | TheseSites of Globs.t
  | ThisSite

let print_site_identifier oc = function
  | AllSites -> ()
  | TheseSites s ->
      Printf.fprintf oc "ON SITE %a"
        (RamenParsing.print_quoted Globs.print) s
  | ThisSite ->
      String.print oc "ON THIS SITE"

let rec print_data_source with_types oc = function
  | NamedOperation (site, Some rel_p, f) ->
      let fq = (rel_p :> string) ^"/"^ (f :> string) in
      Printf.fprintf oc "%a%a"
        (RamenParsing.print_quoted String.print) fq
        print_site_identifier site
  | NamedOperation (site, None, f) ->
      Printf.fprintf oc "%a%a"
        (RamenParsing.print_quoted N.func_print) f
        print_site_identifier site
  | SubQuery q ->
      Printf.fprintf oc "(%a)"
        (print with_types) q
  | GlobPattern s ->
      Globs.print oc s

and print with_types oc op =
  let sep = ", " in
  let sp =
    let had_output = ref false in
    fun oc ->
      String.print oc (if !had_output then " " else "") ;
      had_output := true in
  match op with
  | Aggregate { fields ; and_all_others ; sort ; where ;
                notifications ; key ; commit_cond ; commit_before ;
                flush_how ; from ; every ; event_time ; _ } ->
    if from <> [] then
      Printf.fprintf oc "%tFROM %a" sp
        (List.print ~first:"" ~last:"" ~sep
          (print_data_source with_types)) from ;
    Option.may (fun (n, u_opt, b) ->
      Printf.fprintf oc "%tSORT LAST %d" sp n ;
      Option.may (fun u ->
        Printf.fprintf oc "%tOR UNTIL %a" sp
          (E.print with_types) u) u_opt ;
      Printf.fprintf oc " BY %a"
        (List.print ~first:"" ~last:"" ~sep (E.print with_types)) b
    ) sort ;
    if fields <> [] || not and_all_others then
      Printf.fprintf oc "%tSELECT %a%s%s" sp
        (List.print ~first:"" ~last:"" ~sep
          (print_selected_field with_types)) fields
        (if fields <> [] && and_all_others then sep else "")
        (if and_all_others then "*" else "") ;
    Option.may (fun every ->
      Printf.fprintf oc "%tEVERY %a"
        sp (E.print with_types) every) every ;
    if not (E.is_true where) then
      Printf.fprintf oc "%tWHERE %a" sp
        (E.print with_types) where ;
    if key <> [] then
      Printf.fprintf oc "%tGROUP BY %a" sp
        (List.print ~first:"" ~last:"" ~sep (E.print with_types)) key ;
    if not (E.is_true commit_cond) ||
       flush_how <> Reset ||
       notifications <> [] then (
      let sep = ref " " in
      if flush_how = Reset && notifications = [] then (
        Printf.fprintf oc "%tCOMMIT" sp ;
        sep := ", ") ;
      if flush_how <> Reset then (
        Printf.fprintf oc "%s%a" !sep print_flush_method flush_how ;
        sep := ", ") ;
      if notifications <> [] then (
        List.print ~first:!sep ~last:"" ~sep:!sep
          (fun oc n -> Printf.fprintf oc "NOTIFY %a" (E.print with_types) n)
          oc notifications ;
        sep := ", ") ;
      if not (E.is_true commit_cond) then
        Printf.fprintf oc "%t%s %a" sp
          (if commit_before then "BEFORE" else "AFTER")
          (E.print with_types) commit_cond) ;
      Option.may (fun et ->
        sp oc ;
        RamenEventTime.print oc et
      ) event_time

  | ReadExternal { source ; format ; event_time ; _ } ->
    Printf.fprintf oc "%tREAD FROM %a %a" sp
      (print_external_source with_types) source
      (print_external_format) format ;
    Option.may (fun et ->
      sp oc ;
      RamenEventTime.print oc et
    ) event_time

  | ListenFor { net_addr ; port ; proto } ->
    Printf.fprintf oc "%tLISTEN FOR %s ON %s:%d" sp
      (RamenProtocols.string_of_proto proto)
      (Unix.string_of_inet_addr net_addr)
      port

  | Instrumentation { from } ->
    Printf.fprintf oc "%tLISTEN FOR INSTRUMENTATION%a" sp
      (List.print ~first:" FROM " ~last:"" ~sep:", "
        (print_data_source with_types)) from

  | Notifications { from } ->
    Printf.fprintf oc "%tLISTEN FOR NOTIFICATIONS%a" sp
      (List.print ~first:" FROM " ~last:"" ~sep:", "
        (print_data_source with_types)) from

(* We need some tools to fold/iterate over all expressions contained in an
 * operation. We always do so depth first. *)

let fold_top_level_expr init f = function
  | ListenFor _ | Instrumentation _ | Notifications _ -> init
  | ReadExternal { source ; format ; _ } ->
      let x = fold_external_source init f source in
      fold_external_format x f format
  | Aggregate { fields ; sort ; where ; key ; commit_cond ;
                notifications ; every ; _ } ->
      let x =
        List.fold_left (fun prev sf ->
            let what = Printf.sprintf "field %s" (N.field_color sf.alias) in
            f prev what sf.expr
          ) init fields in
      let x = f x "WHERE clause" where in
      let x = List.fold_left (fun prev ke ->
            f prev "GROUP-BY clause" ke
          ) x key in
      let x = List.fold_left (fun prev notif ->
            f prev "NOTIFY" notif
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
      let x =
        Option.map_default
          (fun every -> f x "EVERY clause" every)
          x every in
      x

let iter_top_level_expr f =
  fold_top_level_expr () (fun () -> f)

let fold_expr init f =
  fold_top_level_expr init (fun i c -> E.fold (f c) [] i)

let iter_expr f =
  fold_expr () (fun c s () e -> f c s e)

let map_top_level_expr f op =
  match op with
  | ListenFor _ | Instrumentation _ | Notifications _ -> op
  | ReadExternal ({ source ; format ; _ } as a) ->
      ReadExternal { a with
        source = map_external_source f source ;
        format = map_external_format f format }
  | Aggregate ({ fields ; sort ; where ; key ; commit_cond ;
                  notifications ; _ } as a) ->
      Aggregate { a with
        fields =
          List.map (fun sf ->
            { sf with expr = f sf.expr }
          ) fields ;
        where = f where ;
        key = List.map f key ;
        notifications = List.map f notifications ;
        commit_cond = f commit_cond ;
        sort =
          Option.map (fun (i, u_opt, b) ->
            i,
            Option.map f u_opt,
            List.map f b
          ) sort }

let map_expr f =
  map_top_level_expr (E.map f [])

(* Various functions to inspect an operation: *)

(* BEWARE: you might have an event_time set in the Func.t that is inherited
 * and therefore not in the operation! *)
let event_time_of_operation op =
  let event_time, fields =
    match op with
    | Aggregate { event_time ; fields ; _ } ->
        event_time, List.map (fun sf -> sf.alias) fields
    | ReadExternal { event_time ; format ; _ } ->
        event_time,
        fields_of_external_format format |>
        List.map (fun ft -> ft.RamenTuple.name)
    | ListenFor { proto ; _ } ->
        RamenProtocols.event_time_of_proto proto, []
    | Instrumentation _ ->
        RamenWorkerStats.event_time, []
    | Notifications _ ->
        RamenNotification.event_time, []
  and event_time_from_fields fields =
    let fos = N.field in
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
  | ReadExternal s -> ReadExternal { s with event_time }
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

let print_parent oc parent =
  match parent with
  | site, None, f ->
      Printf.fprintf oc "%a%s"
        print_site_identifier site
        (f : N.func :> string)
  | site, Some p, f ->
      Printf.fprintf oc "%a%s/%s"
        print_site_identifier site
        (p : N.rel_program :> string) (f :> string)

(* TODO: takes a func instead of child_prog? *)
let program_of_parent_prog child_prog = function
  | None -> child_prog
  | Some rel_prog ->
      N.(program_of_rel_program child_prog rel_prog)

let parents_of_operation = function
  | ListenFor _ | ReadExternal _
  (* Note that those have a from clause but no actual parents: *)
  | Instrumentation _ | Notifications _ -> []
  | Aggregate { from ; _ } ->
      List.map func_id_of_data_source from

let factors_of_operation = function
  | ReadExternal { factors ; _ }
  | Aggregate { factors ; _ } -> factors
  | ListenFor { factors ; proto ; _ } ->
      if factors <> [] then factors
      else RamenProtocols.factors_of_proto proto
  | Instrumentation _ -> RamenWorkerStats.factors
  | Notifications _ -> RamenNotification.factors

let operation_with_factors op factors = match op with
  | ReadExternal s -> ReadExternal { s with factors }
  | Aggregate s -> Aggregate { s with factors }
  | ListenFor s -> ListenFor { s with factors }
  | Instrumentation _ -> op
  | Notifications _ -> op

(* Return the (likely) untyped output tuple *)
let out_type_of_operation ~with_private = function
  | Aggregate { fields ; and_all_others ; _ } ->
      assert (not and_all_others) ;
      List.fold_left (fun lst sf ->
        if not with_private && N.is_private sf.alias then lst else
        RamenTuple.{
          name = sf.alias ;
          doc = sf.doc ;
          aggr = sf.aggr ;
          typ = sf.expr.typ ;
          units = sf.expr.units } :: lst
      ) [] fields |> List.rev
  | ReadExternal { format ; _ } ->
      (* It is possible to suppress a field from the CSV files by prefixing
       * its name with an underscore: *)
      fields_of_external_format format |>
      List.filter (fun ft ->
        with_private || not (N.is_private ft.RamenTuple.name))
  | ListenFor { proto ; _ } ->
      RamenProtocols.tuple_typ_of_proto proto
  | Instrumentation _ ->
      RamenWorkerStats.tuple_typ
  | Notifications _ ->
      RamenNotification.tuple_typ

(* Same as above, but return the output type as a TRecord (the way it's
 * supposed to be!) *)
let out_record_of_operation ~with_private op =
  T.make ~nullable:false
    (T.TRecord (
      (out_type_of_operation ~with_private op |> List.enum) |>
      Enum.map (fun ft ->
        (ft.RamenTuple.name :> string), ft.typ) |>
      Array.of_enum))

let vars_of_operation tup_type op =
  fold_expr Set.empty (fun _ _ s e ->
    match e.E.text with
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                           { text = Variable tt ; _ }))
      when tt = tup_type ->
        Set.add (N.field n) s
    | _ -> s) op

let to_sorted_list s =
  Set.to_list s |> List.fast_sort N.compare

let envvars_of_operation = to_sorted_list % vars_of_operation Env
let params_of_operation = to_sorted_list % vars_of_operation Param

let notifications_of_operation = function
  | Aggregate { notifications ; _ } -> notifications
  | _ -> []

let use_event_time op =
  fold_expr false (fun _ _ b e ->
    match e.E.text with
    | Stateless (SL0 (EventStart|EventStop)) -> true
    | _ -> b
  ) op

let has_notifications = function
  | ListenFor _ | ReadExternal _
  | Instrumentation _ | Notifications _ -> false
  | Aggregate { notifications ; _ } ->
      notifications <> []

let resolve_unknown_variable resolver e =
  E.map (fun stack e ->
    let resolver = function
      | [] | E.Idx _ :: _ as path -> (* Idx is TODO *)
          Printf.sprintf2 "Cannot resolve unknown path %a"
            E.print_path path |>
          failwith
      | E.Name n :: _ ->
          resolver stack n
    in
    match e.E.text with
    | Stateless (SL2 (Get, n, ({ text = Variable Unknown ; _ } as x))) ->
        let pref =
          match E.int_of_const n with
          | Some n -> resolver [ Idx n ]
          | None ->
              (match E.string_of_const n with
              | Some n ->
                  let n = N.field n in
                  resolver [ Name n ]
              | None ->
                  Printf.sprintf2 "Cannot resolve unknown tuple in %a"
                    (E.print false) e |>
                  failwith) in
        { e with text =
          Stateless (SL2 (Get, n, { x with text = Variable pref })) }
    | _ -> e
  ) [] e

let field_in_globals globals n =
  List.exists (fun g -> g.Globals.name = n) globals

(* Also used by [RamenProgram] to check running condition *)
let prefix_def params globals def =
  resolve_unknown_variable (fun _stack n ->
    if RamenTuple.params_mem n params then Param else
    if field_in_globals globals n then Global else
    def)

(* Replace the expressions with [Unknown] with their likely tuple. *)
let resolve_unknown_variables params globals op =
  (* Unless it's a param (TODO: or an opened record), assume Unknow
   * belongs to def: *)
  match op with
  | Aggregate ({ fields ; sort ; where ; key ; commit_cond ;
                 notifications ; every ; _ } as aggr) ->
      let is_selected_fields ?i name = (* Tells if a field is in _out_ *)
        list_existsi (fun i' sf ->
          sf.alias = name &&
          Option.map_default (fun i -> i' < i) true i
        ) fields in
      (* Resolve Unknown into either Param (if the name is in
       * params), or Globals (if the name is in, you guessed it, globals), In
       * or Out (depending on the presence of this alias in selected_fields --
       * optionally, only before position i). It will also keep track of
       * opened records and look up there first. *)
      let prefix_smart ?(allow_out=true) ?i =
        resolve_unknown_variable (fun stack n ->
          (* First, lookup for an opened record: *)
          if List.exists (fun e ->
               match e.E.text with
               | Record kvs ->
                   (* Notice that we look into _all_ fields, not only the
                    * ones defined previously. Not sure if better or
                    * worse. *)
                   List.exists (fun (k, _) -> k = n) kvs
               | _ -> false
             ) stack
          then (
            (* Notice we do not keep a reference on the actual expression.
             * That's much safer to look it up again whenever we need it,
             * so that we are free to map the AST. *)
            !logger.debug "Field %a though to belong to an opened record"
              N.field_print n ;
            Record
          ) else (
            let pref =
              (* Look into predefined records: *)
              if RamenTuple.params_mem n params then
                Param
              else if field_in_globals globals n then
                Global
              (* Then into fields that have been defined before: *)
              else if allow_out && is_selected_fields ?i n then
                Out
              (* Then finally assume input: *)
              else In in
            !logger.debug "Field %a thought to belong to %s"
              N.field_print n
              (string_of_variable pref) ;
            pref
          )
        )
      in
      let fields =
        List.mapi (fun i sf ->
          { sf with expr = prefix_smart ~i sf.expr }
        ) fields in
      let sort =
        Option.map (fun (n, u_opt, b) ->
          n,
          Option.map (prefix_def params globals In) u_opt,
          List.map (prefix_def params globals In) b
        ) sort in
      let where = prefix_smart ~allow_out:false where in
      let key = List.map (prefix_def params globals In) key in
      let commit_cond = prefix_smart commit_cond in
      let notifications = List.map prefix_smart notifications in
      let every = Option.map (prefix_def params globals In) every in
      Aggregate { aggr with
        fields ; sort ; where ; key ; commit_cond ; notifications ;
        every }

  | ReadExternal _ ->
      (* Default to In if not a param, and then disallow In *)
      (* prefix_def will select Param if it is indeed in param, and only
       * if not will it assume it's in env; which makes sense as that's the
       * only two possible tuples here: *)
      map_top_level_expr (prefix_def params globals Env) op

  | op -> op

exception DependsOnInvalidVariable of variable
let check_depends_only_on lst =
  let check_can_use tuple =
    if not (List.mem tuple lst) then
      raise (DependsOnInvalidVariable tuple)
  in
  E.iter (fun _ e ->
    match e.E.text with
    | Variable tuple
    | Binding (RecordField (tuple, _))
    | Binding (RecordValue tuple) ->
        check_can_use tuple
    | Stateless (SL0 (EventStart|EventStop)) ->
      (* Be conservative for now.
       * TODO: Actually check the event time expressions.
       * Also, we may not know yet the event time (if it's inferred from
       * a parent).
       * TODO: Perform those checks only after factors/time inference.
       * And finally, we will do all this for nothing, as the fields are
       * taken from output event when they are just transferred from input.
       * So when the field used in the time expression can be computed only
       * from the input tuple (with no use of another out field) we could
       * as well recompute it - at least when it's just forwarded.
       * But then we would need to be smarter in
       * CodeGen_OCaml.emit_event_time will need more context (is out
       * available) and how is it computed. So for now, let's assume any
       * mention of #start/#stop is from out.  *)
      check_can_use Out
    | _ -> ())

let default_commit_cond = E.of_bool true

(* Check that the expression is valid, or return an error message.
 * Also perform some optimisation, numeric promotions, etc...
 * This is done after the parse rather than Rejecting the parsing
 * result for better error messages, and also because we need the
 * list of available parameters. *)
let checked params globals op =
  let op = resolve_unknown_variables params globals op in
  let check_pure clause =
    E.unpure_iter (fun _ _ ->
      failwith ("Stateful functions not allowed in "^ clause))
  and warn_no_group clause =
    E.unpure_iter (fun _ e ->
      match e.E.text with
      | Stateful (LocalState, skip, stateful) ->
          !logger.warning
            "In %s: Locally stateful function without aggregation. \
             Do you mean %a?"
            clause
            (E.print_text ~max_depth:1 false)
              (Stateful (GlobalState, skip, stateful))
      | _ -> ())
  and check_fields_from lst where e =
    try check_depends_only_on lst e
    with DependsOnInvalidVariable tuple ->
      Printf.sprintf2 "Variable %s not allowed in %s (only %a)"
        (RamenLang.string_of_variable tuple)
        where (pretty_list_print RamenLang.variable_print) lst |>
      failwith
  and check_field_exists field_names f =
    if not (List.mem f field_names) then
      Printf.sprintf2 "Field %a is not in output tuple (only %a)"
        N.field_print f
        (pretty_list_print N.field_print) field_names |>
      failwith
  and check_field_is_public f =
    if N.is_private f then
      Printf.sprintf2 "Field %a must not be private" N.field_print f |>
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
    List.iter (fun fn ->
      check_field_exists field_names fn ;
      check_field_is_public fn)
  in
  (match op with
  | Aggregate { fields ; and_all_others ; sort ; where ; key ;
                commit_cond ; event_time ; notifications ; from ; every ;
                factors ; flush_how ; _ } ->
    (* Check that we use the Group only for virtual fields: *)
    iter_expr (fun _ _ e ->
      match e.E.text with
      | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                             { text = Variable Group ; _ })) ->
          let n = N.field n in
          if not (N.is_virtual n) then
            Printf.sprintf2 "Variable group has only virtual fields (no %a)"
              N.field_print n |>
            failwith
      | _ -> ()) op ;
    (* Now check what tuple prefixes are used: *)
    List.fold_left (fun prev_aliases sf ->
        check_fields_from
          [ Param; Env; Global; In; Group;
            Out (* FIXME: only if defined earlier *);
            OutPrevious ; Record ] "SELECT clause" sf.expr ;
        (* Check unicity of aliases *)
        if List.mem sf.alias prev_aliases then
          Printf.sprintf2 "Alias %a is not unique"
            N.field_print sf.alias |>
          failwith ;
        sf.alias :: prev_aliases
      ) [] fields |> ignore;
    if not and_all_others then (
      let field_names = List.map (fun sf -> sf.alias) fields in
      Option.may (check_event_time field_names) event_time ;
      check_factors field_names factors
    ) ;
    check_fields_from
      [ Param; Env; Global; In;
        Group; OutPrevious; Record ]
      "WHERE clause" where ;
    List.iter (fun k ->
      check_pure "GROUP-BY clause" k ;
      check_fields_from
        [ Param; Env; Global; In ; Record ] "Group-By KEY" k
    ) key ;
    List.iter (fun name ->
      check_fields_from [ Param; Env; Global; In; Out; Record ]
                        "notification" name
    ) notifications ;
    check_fields_from
      [ Param; Env; Global; In;
        Out; OutPrevious;
        Group; Record ]
      "COMMIT WHEN clause" commit_cond ;
    Option.may (fun (_, until_opt, bys) ->
      Option.may (fun until ->
        check_fields_from
          [ Param; Env; Global;
            SortFirst; SortSmallest; SortGreatest; Record ]
          "SORT-UNTIL clause" until
      ) until_opt ;
      List.iter (fun by ->
        check_fields_from
          [ Param; Env; Global; In; Record ]
          "SORT-BY clause" by
      ) bys
    ) sort ;
    Option.may
      (check_fields_from [ Param; Env; Global ] "EVERY clause") every ;
    if every <> None && from <> [] then
      failwith "Cannot have both EVERY and FROM" ;
    (* Check that we do not use any fields from out that is generated: *)
    let generators = List.filter_map (fun sf ->
        if E.is_generator sf.expr then Some sf.alias else None
      ) fields in
    iter_expr (fun _ _ e ->
      match e.E.text with
      | Stateless (SL2 (Get, { text = Const (VString n) ; _ },
                             { text = Variable OutPrevious ; _ })) ->
          let n = N.field n in
          if List.mem n generators then
            Printf.sprintf2 "Cannot use a generated output field %a"
              N.field_print n |>
            failwith
      | _ -> ()
    ) op ;
    (* Finally, check that if there is no aggregation then no
     * LocalState is used anywhere: *)
    if commit_cond == default_commit_cond &&
       flush_how = Reset &&
       key = [] then
      iter_top_level_expr warn_no_group op

  | ListenFor { proto ; factors ; _ } ->
    let tup_typ = RamenProtocols.tuple_typ_of_proto proto in
    let field_names = List.map (fun t -> t.RamenTuple.name) tup_typ in
    check_factors field_names factors

  | ReadExternal { source ; format ; event_time ; factors ; _ } ->
    let field_names = fields_of_external_format format |>
                      List.map (fun t -> t.RamenTuple.name) in
    Option.may (check_event_time field_names) event_time ;
    check_factors field_names factors ;
    (* Unknown tuples has been defaulted to Param/Env already.
     * Let's now forbid explicit references to input: *)
    iter_top_level_expr (check_fields_from [ Param; Env; Global ]) op ;
    (* additionally, all expressions used for defining the source must be
     * stateless: *)
    iter_external_source (check_pure) source
    (* FIXME: check the field type declarations of CSV format use only
     * scalar types *)

  | Instrumentation _ | Notifications _ -> ()) ;
  (* Now that we have inferred the IO tuples, run some additional checks on
   * the expressions: *)
  iter_expr (fun _ _ e -> E.check e) op ;
  op

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let rec default_alias e =
    let strip_leading_underscore s =
      let l = String.length s in
      if l > 1 && s.[0] = '_' then String.lchop s else s in
    match e.E.text with
    | Stateless (SL0 (Path [ Name name ]))
      when not (N.is_virtual name) ->
        strip_leading_underscore (name :> string)
    | Stateless (SL2 (Get, { text = Const (VString n) ; _ }, _))
      when not (N.is_virtual (N.field n)) ->
        strip_leading_underscore n
    (* Provide some default name for common aggregate functions: *)
    | Stateful (_, _, SF1 (AggrMin, e)) -> "min_"^ default_alias e
    | Stateful (_, _, SF1 (AggrMax, e)) -> "max_"^ default_alias e
    | Stateful (_, _, SF1 (AggrSum, e)) -> "sum_"^ default_alias e
    | Stateful (_, _, SF1 (AggrAvg, e)) -> "avg_"^ default_alias e
    | Stateful (_, _, SF1 (AggrAnd, e)) -> "and_"^ default_alias e
    | Stateful (_, _, SF1 (AggrOr, e)) -> "or_"^ default_alias e
    | Stateful (_, _, SF1 (AggrFirst, e)) -> "first_"^ default_alias e
    | Stateful (_, _, SF1 (AggrLast, e)) -> "last_"^ default_alias e
    | Stateful (_, _, SF1 (AggrHistogram _, e)) ->
        default_alias e ^"_histogram"
    | Stateless (SL2 (Percentile, e,
        { text = (Const p | Vector [ { text = Const p ; _ } ]) ; _ }))
      when T.is_round_integer p ->
        Printf.sprintf2 "%s_%ath" (default_alias e) T.print p
    (* Some functions better leave no traces: *)
    | Stateless (SL1s (Print, e::_)) -> default_alias e
    | Stateless (SL1 ((Cast _|UuidOfU128), e)) -> default_alias e
    | Stateful (_, _, SF1 (Group, e)) -> default_alias e
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
        let alias = N.field alias in
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
              let n = N.field n in
              DurationField (n, ref OutputField, s)) |||
           (duration >>: fun n -> DurationConst n)) |||
         blanks -- strinG "and" -- blanks --
         (strinG "stops" ||| strinG "stopping" |||
          strinG "ends" ||| strinG "ending") -- blanks --
         strinG "at" -- blanks -+
           (non_keyword ++ scale >>: fun (n, s) ->
              let n = N.field n in
              StopField (n, ref OutputField, s)))) >>:
      fun ((sta, sca), dur) ->
        let sta = N.field sta in
        (sta, ref OutputField, sca), dur
    ) m

  let every_clause m =
    let m = "every clause" :: m in
    (
      strinG "every" -- blanks -+ E.Parser.p
    ) m

  let select_clause m =
    let m = "select clause" :: m in
    ((strinG "select" ||| strinG "yield") -- blanks -+
     several ~sep:list_sep
             ((star >>: fun _ -> None) |||
              some selected_field)) m

  let event_time_start () =
    E.make (Stateless (SL2 (Get, E.of_string "start",
                                 E.make (Variable In))))

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
    ((strinG "flush" >>: fun () -> Reset) |||
     (strinG "keep" -- optional ~def:() (blanks -- strinG "all") >>:
       fun () -> Never) >>:
     fun s -> FlushSpec s) m

  let dummy_commit m =
    (strinG "commit" >>: fun () -> CommitSpec) m

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

  let csv_specs m =
    let fields_schema m =
      let m = "tuple schema" :: m in
      (
        char '(' -- opt_blanks -+
          several ~sep:list_sep RamenTuple.Parser.field +-
        opt_blanks +- char ')'
      ) m in
    let m = "CSV format" :: m in
    (optional ~def:Default.csv_separator (
       strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) ++
     optional ~def:Default.csv_null (
       strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) ++
     optional ~def:true (
       optional ~def:true (strinG "no" -- blanks >>: fun () -> false) +-
       strinGs "quote" +- blanks) ++
     optional ~def:"" (
       strinG "escape" -- optional ~def:() (blanks -- strinG "with") --
       opt_blanks -+ quoted_string +- opt_blanks) ++
     fields_schema >>:
     fun ((((separator, null), may_quote), escape_seq), fields) ->
       if separator = null || separator = "" then
         raise (Reject "Invalid CSV separator") ;
       { separator ; null ; may_quote ; escape_seq ; fields }) m

  let row_binary_specs m =
    let m = "RowBinary format" :: m in
    let backquoted_string_with_sql_style m =
      let m = "Backquoted field name" :: m in
      (
        char '`' -+
        repeat_greedy ~sep:none (
          cond "field name" ((<>) '`') 'x') +-
        char '`' >>: N.field % String.of_list
      ) m in
    let rec ptype m =
      let with_param np ap =
        np -- opt_blanks -- char '(' -+ ap +- char ')' in
      let with_2_params np p1 p2 =
        let ap = p1 -+ opt_blanks +- char ',' +- opt_blanks ++ p2 in
        with_param np ap in
      let unsigned =
        integer >>: fun n ->
          let i = Num.to_int n in
          if i < 0 then raise (Reject "Type parameter must be >0") ;
          i in
      let with_num_param s =
        with_param (strinG s) unsigned in
      let with_2_num_params s =
        with_2_params (strinG s) number number in
      let with_typ_param s =
        with_param (strinG s) ptype in
      let m = "Type name" :: m in
      (
        let notnull = T.(make ~nullable:false) in
        (* Look only for simple types, starting with numerics: *)
        (strinG "UInt8" >>: fun () -> notnull T.TU8) |||
        (strinG "UInt16" >>: fun () -> notnull T.TU16) |||
        (strinG "UInt32" >>: fun () -> notnull T.TU32) |||
        (strinG "UInt64" >>: fun () -> notnull T.TU64) |||
        ((strinG "Int8" ||| strinG "TINYINT") >>:
          fun () -> notnull T.TI8) |||
        ((strinG "Int16" ||| strinG "SMALLINT") >>:
          fun () -> notnull T.TI16) |||
        ((strinG "Int32" ||| strinG "INT" ||| strinG "INTEGER") >>:
          fun () -> notnull T.TI32) |||
        ((strinG "Int64" ||| strinG "BIGINT") >>:
          fun () -> notnull T.TI64) |||
        ((strinG "Float32" ||| strinG "Float64" |||
          strinG "FLOAT" ||| strinG "DOUBLE") >>:
          fun () -> notnull T.TFloat) |||
        (* Assuming UUIDs are just plain U128 with funny-printing: *)
        (strinG "UUID" >>: fun () -> notnull T.TU128) |||
        (* Decimals: for now forget about the size of the decimal part,
         * just map into corresponding int type*)
        (with_num_param "Decimal32" >>: fun _p -> notnull T.TI32) |||
        (with_num_param "Decimal64" >>: fun _p -> notnull T.TI64) |||
        (with_num_param "Decimal128" >>: fun _p -> notnull T.TI128) |||
        (* TODO: actually do something with the size: *)
        ((with_2_num_params "Decimal" ||| with_2_num_params "DEC") >>:
          fun (_n, _m)  -> notnull T.TI128) |||
        ((strinG "DateTime" ||| strinG "TIMESTAMP") >>:
          fun () -> notnull T.TU32) |||
        (strinG "Date" >>: fun () -> notnull T.TU16) |||
        ((strinG "String" ||| strinG "CHAR" ||| strinG "VARCHAR" |||
          strinG "TEXT" ||| strinG "TINYTEXT" ||| strinG "MEDIUMTEXT" |||
          strinG "LONGTEXT" ||| strinG "BLOB" ||| strinG "TINYBLOB" |||
          strinG "MEDIUMBLOB" ||| strinG "LONGBLOB") >>:
          fun () -> notnull T.TString) |||
        ((with_num_param "FixedString" ||| with_num_param "BINARY") >>:
          fun d -> T.(notnull T.(TVec (d, notnull TChar)))) |||
        (with_typ_param "Nullable" >>:
          fun t -> T.{ t with nullable = true }) |||
        (* Just ignore those ones (for now): *)
        (with_typ_param "LowCardinality")
        (* Etc... *)
      ) m
    in
    (
      char '(' -- opt_blanks -+
      optional ~def:() (
        string "columns format version: " -- number -- blanks) --
      optional ~def:() (
        number -- blanks -- string "columns:" -- blanks) -+
      several ~sep:blanks (
        backquoted_string_with_sql_style +- blanks ++ ptype) +-
      opt_blanks +- char ')'
    ) m

  let external_format m =
    let m = "external data format" :: m in
    (
      strinG "as" -- blanks -+ (
        (strinG "csv" -- blanks -+ csv_specs >>: fun s -> CSV s) |||
        (strinG "rowbinary" -- blanks -+ row_binary_specs >>:
          fun s ->
            RowBinary (
              List.map (fun (name, typ) ->
                RamenTuple.{ name ; typ ; units = None ;
                             doc = "" ; aggr = None }
              ) s))
      )
    ) m

  let file_specs m =
    let m = "read file operation" :: m in
    (
      E.Parser.p ++
      optional ~def:None (
        blanks -+
        (strinG "preprocess" ||| strinG "preprocessed") -- blanks --
        strinG "with" -- opt_blanks -+
        some E.Parser.p) ++
      optional ~def:(E.of_bool false) (
        blanks -- strinG "then" -- blanks -- strinG "delete" -+
        optional ~def:(E.of_bool true) (
          blanks -- strinG "if" -- blanks -+ E.Parser.p)) >>:
        fun ((fname, preprocessor), unlink) -> { fname ; preprocessor ; unlink }
    ) m

  let kafka_specs m =
    let kafka_option m =
      let m = "option" :: m in
      (
        quoted_string +- opt_blanks +- char '=' +- opt_blanks ++ E.Parser.p
      ) m
    in
    let m = "kafka specifications" :: m in
    (
      strinG "topic" -- blanks -+ E.Parser.p +- blanks ++
      optional ~def:[] (
        strinGs "partition" -- blanks -+
        (* Do not accept any expression or list separator AND will be
         * parsed as the logical operator: *)
        several ~sep:list_sep_and E.Parser.(const ||| param) +- blanks) +-
      strinG "with" +- blanks +- strinG "options" +- blanks ++
        several ~sep:list_sep_and kafka_option >>:
      fun ((topic, partitions), options) ->
        let restart_from = OffsetFromEnd (E.zero ()) in (* TODO *)
        { options ; topic ; partitions ; restart_from }
    ) m

  let external_source m =
    let m = "external data source" :: m in
    (
      optional ~def:() (strinG "from" -- blanks) -+ (
        (strinGs "file" -- blanks -+ file_specs >>: fun s -> File s) |||
        (strinG "kafka" -- blanks -+ kafka_specs >>: fun s -> Kafka s)
      )
    ) m

  let read_clause m =
    let m = "read" :: m in
    (
      strinG "read" -- blanks -+ (
        (
          external_source +- blanks ++ external_format
        ) ||| (
          external_format +- blanks ++ external_source >>: fun (a, b) -> b, a
        )
      )
    ) m

  let factor_clause m =
    let m = "factors" :: m
    and field = non_keyword >>: N.field in
    ((strinG "factor" ||| strinG "factors") -- blanks -+
     several ~sep:list_sep_and field) m

  type select_clauses =
    | SelectClause of selected_field option list
    | SortClause of (int * E.t option (* until *) * E.t list (* by *))
    | WhereClause of E.t
    | EventTimeClause of RamenEventTime.t
    | FactorClause of N.field list
    | GroupByClause of E.t list
    | CommitClause of (commit_spec list * (bool (* before *) * E.t))
    | FromClause of data_source list
    | EveryClause of E.t option
    | ListenClause of (Unix.inet_addr * int * RamenProtocols.net_protocol)
    | InstrumentationClause of string
    | ReadClause of (external_source * external_format)
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

  type tmp_data_source =
    | Named_ of (N.rel_program option * N.func)
    | Pattern_ of string

  let rec from_clause m =
    let m = "from clause" :: m in
    (
      strinG "from" -- blanks -+
      several ~sep:list_sep_and (
        (
          char '(' -- opt_blanks -+ p +- opt_blanks +- char ')' >>:
            fun t -> SubQuery t
        ) ||| (
          from_pattern >>: fun s -> GlobPattern (Globs.compile s)
        ) ||| (
          func_identifier ++
          optional ~def:AllSites (
            blanks -- strinG "on" -- blanks -+ (
              (
                strinG "this" -- blanks -- strinG "site" >>:
                fun () -> ThisSite
              ) ||| (
                strinGs "site" -- blanks -+ site_identifier >>:
                fun s -> TheseSites (Globs.compile s)
              )
            )
          ) >>: fun ((p, f), s) -> NamedOperation (s, p, f)
        )
      )
    ) m

  and p m =
    let m = "operation" :: m in
    let part =
      (select_clause >>: fun c -> SelectClause c) |||
      (sort_clause >>: fun c -> SortClause c) |||
      (where_clause >>: fun c -> WhereClause c) |||
      (event_time_clause >>: fun c -> EventTimeClause c) |||
      (group_by >>: fun c -> GroupByClause c) |||
      (commit_clause >>: fun c -> CommitClause c) |||
      (from_clause >>: fun c -> FromClause c) |||
      (every_clause >>: fun c -> EveryClause (Some c)) |||
      (listen_clause >>: fun c -> ListenClause c) |||
      (instrumentation_clause >>: fun c -> InstrumentationClause c) |||
      (read_clause >>: fun c -> ReadClause c) |||
      (factor_clause >>: fun c -> FactorClause c) in
    (several ~sep:blanks part >>: fun clauses ->
      (* Used for its address: *)
      let default_select_fields = []
      and default_star = true
      and default_sort = None
      and default_where = E.of_bool true
      and default_event_time = None
      and default_key = []
      and default_commit = ([], (false, default_commit_cond))
      and default_from = []
      and default_every = None
      and default_listen = None
      and default_instrumentation = ""
      and default_read_clause = None
      and default_factors = [] in
      let default_clauses =
        default_select_fields, default_star, default_sort,
        default_where, default_event_time, default_key,
        default_commit, default_from, default_every,
        default_listen, default_instrumentation, default_read_clause,
        default_factors in
      let select_fields, and_all_others, sort, where,
          event_time, key, commit, from, every, listen, instrumentation,
          read, factors =
        List.fold_left (
          fun (select_fields, and_all_others, sort, where,
               event_time, key, commit, from, every, listen,
               instrumentation, read, factors) ->
            (* FIXME: in what follows, detect and signal cases when a new value
             * replaces an old one (but the default), such as when two WHERE
             * clauses are given. *)
            function
            | SelectClause fields_or_stars ->
              let fields, and_all_others =
                List.fold_left (fun (fields, and_all_others) -> function
                  | Some f -> f::fields, and_all_others
                  | None when not and_all_others -> fields, true
                  | None -> raise (Reject "All fields (\"*\") included \
                                   several times")
                ) ([], false) fields_or_stars in
              (* The above fold_left inverted the field order. *)
              let select_fields = List.rev fields in
              select_fields, and_all_others, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | SortClause sort ->
              select_fields, and_all_others, Some sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | WhereClause where ->
              select_fields, and_all_others, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | EventTimeClause event_time ->
              select_fields, and_all_others, sort, where,
              Some event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | GroupByClause key ->
              select_fields, and_all_others, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | CommitClause commit' ->
              if commit != default_commit then
                raise (Reject "Cannot have several commit clauses") ;
              select_fields, and_all_others, sort, where,
              event_time, key, commit', from, every, listen,
              instrumentation, read, factors
            | FromClause from' ->
              select_fields, and_all_others, sort, where,
              event_time, key, commit, (List.rev_append from' from),
              every, listen, instrumentation, read, factors
            | EveryClause every ->
              select_fields, and_all_others, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | ListenClause l ->
              select_fields, and_all_others, sort, where,
              event_time, key, commit, from, every, Some l,
              instrumentation, read, factors
            | InstrumentationClause c ->
              select_fields, and_all_others, sort, where,
              event_time, key, commit, from, every, listen, c,
              read, factors
            | ReadClause c ->
              select_fields, and_all_others, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, Some c, factors
            | FactorClause factors ->
              select_fields, and_all_others, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
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
      and not_listen =
        listen = None || from != default_from || every != default_every
      and not_instrumentation = instrumentation = ""
      and not_read =
        read = None || from != default_from || every != default_every
      and not_event_time = event_time = default_event_time
      and not_factors = factors == default_factors in
      if not_listen && not_read && not_instrumentation then
        let flush_how, notifications =
          List.fold_left (fun (f, n) -> function
            | CommitSpec -> f, n
            | NotifySpec n' -> f, n'::n
            | FlushSpec f' ->
                if f = None then (Some f', n)
                else raise (Reject "Several flush clauses")
          ) (None, []) commit_specs in
        let flush_how = flush_how |? Reset in
        Aggregate { fields = select_fields ; and_all_others ; sort ;
                    where ; event_time ; notifications ; key ;
                    commit_before ; commit_cond ; flush_how ; from ;
                    every ; factors }
      else if not_aggregate && not_read && not_event_time &&
              not_instrumentation && listen <> None then
        let net_addr, port, proto = Option.get listen in
        ListenFor { net_addr ; port ; proto ; factors }
      else if not_aggregate && not_listen &&
              not_instrumentation &&
              read <> None then
        let source, format = Option.get read in
        ReadExternal { source ; format ; event_time ; factors }
      else if not_aggregate && not_listen && not_read && not_listen &&
              not_factors
      then
        if String.lowercase instrumentation = "instrumentation" then
          Instrumentation { from }
        else
          Notifications { from }
      else
        raise (Reject "Incompatible mix of clauses")
    ) m

  (*$inject
    let test_op s =
      (match test_p p s with
      | Ok (res, rem) ->
        let params =
          [ RamenTuple.{
              ptyp = { name = N.field "avg_window" ;
                       typ = { structure = T.TI32 ;
                               nullable = false } ;
                       units = None ; doc = "" ; aggr = None } ;
              value = T.VI32 10l }] in
        BatPervasives.Ok (
          RamenOperation.checked params [] res,
          rem)
      | x -> x) |>
      TestHelpers.test_printer (RamenOperation.print false)
  *)
  (*$= test_op & ~printer:BatPervasives.identity
    "FROM 'foo' SELECT in.start, in.stop, in.itf_clt AS itf_src, in.itf_srv AS itf_dst" \
      (test_op "from foo select start, stop, itf_clt as itf_src, itf_srv as itf_dst")

    "FROM 'foo' WHERE (in.packets) > (0)" \
      (test_op "from foo where packets > 0")

    "FROM 'foo' SELECT in.t, in.value EVENT STARTING AT t*10. AND DURATION 60." \
      (test_op "from foo select t, value aggregates using max event starting at t*10 with duration 60s")

    "FROM 'foo' SELECT in.t1, in.t2, in.value EVENT STARTING AT t1*10. AND STOPPING AT t2*10." \
      (test_op "from foo select t1, t2, value event starting at t1*10. and stopping at t2*10.")

    "FROM 'foo' NOTIFY \"ouch\"" \
      (test_op "from foo NOTIFY \"ouch\"")

    "FROM 'foo' SELECT MIN LOCALLY skip nulls(in.start) AS start, \\
       MAX LOCALLY skip nulls(in.stop) AS max_stop, \\
       (SUM LOCALLY skip nulls(in.packets)) / \\
         (param.avg_window) AS packets_per_sec \\
     GROUP BY (in.start) / ((1000000) * (param.avg_window)) \\
     COMMIT AFTER \\
       ((MAX LOCALLY skip nulls(in.start)) + (3600)) > (out.start)" \
        (test_op "select min start as start, \\
                           max stop as max_stop, \\
                           (sum packets)/avg_window as packets_per_sec \\
                   from foo \\
                   group by start / (1_000_000 * avg_window) \\
                   commit after out.start < (max in.start) + 3600")

    "FROM 'foo' SELECT 1 AS one GROUP BY true COMMIT BEFORE (SUM LOCALLY skip nulls(1)) >= (5)" \
        (test_op "select 1 as one from foo commit before sum 1 >= 5 group by true")

    "FROM 'foo/bar' SELECT in.n, LAG GLOBALLY skip nulls(2, out.n) AS l" \
        (test_op "SELECT n, lag globally(2, n) AS l FROM foo/bar")

    "READ FROM FILES \"/tmp/toto.csv\" \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read file \"/tmp/toto.csv\" as csv (f1 bool?, f2 i32)")

    "READ FROM FILES \\
      CASE WHEN env.glop THEN \"glop.csv\" ELSE \"pas glop.csv\" END \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read from files (IF glop THEN \"glop.csv\" ELSE \"pas glop.csv\") as csv (f1 bool?, f2 i32)")

    "READ FROM FILES \"/tmp/toto.csv\" THEN DELETE \\
      AS CSV NO QUOTES (f1 BOOL?, f2 I32)" \
      (test_op "read from file \"/tmp/toto.csv\" then delete as csv no quote (f1 bool?, f2 i32)")

    "READ FROM FILES \"foo\" THEN DELETE IF env.delete_flag \\
      AS CSV NO QUOTES (x BOOL)" \
      (test_op "read from file \"foo\" then delete if delete_flag as csv no quote (x bool)")

    "READ FROM FILES \"/tmp/toto.csv\" \\
      AS CSV SEPARATOR \"\\t\" NULL \"<NULL>\" (f1 BOOL?, f2 I32)" \
      (test_op "read from file \"/tmp/toto.csv\" as csv \\
                      separator \"\\t\" null \"<NULL>\" \\
                      (f1 bool?, f2 i32)")

   "READ FROM KAFKA TOPIC \"foo\" WITH OPTIONS \\
      \"foo.bar\" = \"glop\" AND \"metadata.broker.list\" = \"localhost:9002\" \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read from kafka topic \"foo\" with options \\
        \"foo.bar\"=\"glop\", \"metadata.broker.list\" = \"localhost:9002\" \\
        as csv (f1 bool?, f2 i32)")

   "READ FROM KAFKA TOPIC \"foo\" PARTITION 0 WITH OPTIONS \\
      \"foo.bar\" = \"glop\" AND \"metadata.broker.list\" = \"localhost:9002\" \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read from kafka topic \"foo\" partition 0 with options \\
        \"foo.bar\"=\"glop\", \"metadata.broker.list\" = \"localhost:9002\" \\
        as csv (f1 bool?, f2 i32)")

    "READ FROM KAFKA TOPIC \"foo\" PARTITIONS 0 AND 1 WITH OPTIONS \\
      \"foo.bar\" = \"glop\" AND \"metadata.broker.list\" = \"localhost:9002\" \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read from kafka topic \"foo\" partitions 0 and 1 with options \\
        \"foo.bar\"=\"glop\", \"metadata.broker.list\" = \"localhost:9002\" \\
        as csv (f1 bool?, f2 i32)")

    "SELECT 1 AS one EVERY 1{seconds}" \
        (test_op "YIELD 1 AS one EVERY 1 SECONDS")
  *)

  (*$>*)
end
