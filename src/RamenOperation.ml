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
open Stdint

open RamenLang
open RamenHelpersNoLog
open RamenLog
open RamenConsts
module Default = RamenConstsDefault
module DT = DessserTypes
module E = RamenExpr
module Globals = RamenGlobalVariables
module N = RamenName
module T = RamenTypes
module Variable = RamenVariable

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint
*)

open Raql_flush_method.DessserGen
open Raql_select_field.DessserGen
include Raql_operation.DessserGen
type selected_field = Raql_select_field.DessserGen.t
type flush_method = Raql_flush_method.DessserGen.t

let print_selected_field with_types oc f =
  let open Raql_select_field.DessserGen in
  let need_alias =
    match f.expr.text with
    | Stateless (SL0 (Path [ Name n ]))
      when f.alias = n -> false
    | Stateless (SL2 (Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                           { text = Stateless (SL0 (Variable In)) ; _ }))
      when (f.alias :> string) = n -> false
    | _ -> true in
  if need_alias then (
    Printf.fprintf oc "%a AS %s"
      (E.print with_types) f.expr
      (ramen_quote (f.alias :> string)) ;
    if f.doc <> "" then Printf.fprintf oc " %S" f.doc
  ) else (
    E.print with_types oc f.expr ;
    if f.doc <> "" then Printf.fprintf oc " DOC %S" f.doc
  )

let print_flush_method oc = function
  | Reset ->
    Printf.fprintf oc "FLUSH"
  | Never ->
    Printf.fprintf oc "KEEP"

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
        match specs.partitions with
        | None -> x
        | Some e -> f x "kafka-partitions" e in
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
        partitions = Option.map f partitions ;
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
  Option.may (fun partitions ->
    Printf.fprintf oc " PARTITIONS %a"
      (E.print with_types) partitions
  ) specs.partitions ;
  Printf.fprintf oc " WITH OPTIONS %a"
    (pretty_list_print ~uppercase:true (fun oc (n, e) ->
      Printf.fprintf oc "%S = %a" n (E.print with_types) e))
      specs.options

let print_external_source with_types oc = function
  | File specs ->
      print_file_specs with_types oc specs
  | Kafka specs ->
      print_kafka_specs with_types oc specs

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
    Printf.fprintf oc " SEPARATOR %a" RamenParsing.print_char specs.separator ;
  if specs.null <> Default.csv_null then
    Printf.fprintf oc " NULL %S" specs.null ;
  if not specs.may_quote then
    Printf.fprintf oc " NO QUOTES" ;
  if specs.escape_seq <> "" then
    Printf.fprintf oc " ESCAPE WITH %S" specs.escape_seq ;
  if specs.vectors_of_chars_as_string then
    String.print oc " VECTORS OF CHARS AS STRING" ;
  if specs.clickhouse_syntax then
    Printf.fprintf oc " CLICKHOUSE SYNTAX" ;
  Printf.fprintf oc " %a"
    RamenTuple.print_typ specs.fields

let print_row_binary_specs oc fields =
  let print_type_as_clickhouse oc typ =
    if typ.DT.nullable then Printf.fprintf oc "Nullable(" ;
    String.print oc (
      match DT.develop typ.DT.typ with
      | Base U8 -> "UInt8"
      | Base U16 -> "UInt16"
      | Base (U24 | U32) -> "UInt32"
      | Base (U40 | U48 | U56 | U64) -> "UInt64"
      | Base U128 -> "UUID"
      | Base I8 -> "Int8"
      | Base I16 -> "Int16"
      | Base (I24 | I32) -> "Int32"
      | Base (I40 | I48 | I56 | I64) -> "Int64"
      | Base I128 -> "Decimal128"
      | Base Float -> "Float64"
      | Base String -> "String"
      | Vec (d, { typ = Base Char ; _ }) ->
          Printf.sprintf "FixedString(%d)" d
      | _ ->
          Printf.sprintf2 "ClickHouseFor(%a)" DT.print_mn typ) ;
    if typ.DT.nullable then Printf.fprintf oc ")" in
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

let print_site_identifier oc = function
  | AllSites -> ()
  | TheseSites s ->
      Printf.fprintf oc " ON SITE %S" s
  | ThisSite ->
      String.print oc " ON THIS SITE"

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

and print with_types oc op =
  let sep = ", " in
  let sp =
    let had_output = ref false in
    fun oc ->
      String.print oc (if !had_output then " " else "") ;
      had_output := true in
  match op with
  | Aggregate { aggregate_fields ; and_all_others ; sort ; where ;
                notifications ; key ; commit_cond ; commit_before ;
                flush_how ; from ; every ; aggregate_event_time ; _ } ->
    if from <> [] then
      Printf.fprintf oc "%tFROM %a" sp
        (List.print ~first:"" ~last:"" ~sep
          (print_data_source with_types)) from ;
    Option.may (fun (n, u_opt, b) ->
      Printf.fprintf oc "%tSORT LAST %s" sp (Uint32.to_string n) ;
      Option.may (fun u ->
        Printf.fprintf oc "%tOR UNTIL %a" sp
          (E.print with_types) u) u_opt ;
      Printf.fprintf oc " BY %a"
        (List.print ~first:"" ~last:"" ~sep (E.print with_types)) b
    ) sort ;
    if aggregate_fields <> [] || and_all_others <> None then (* if there is a select clause *)
      Printf.fprintf oc "%tSELECT %s%s%a" sp
        (match and_all_others with None -> ""
        | Some l ->
            List.fold_left (fun s f ->
              s ^" -"^ ramen_quote f
            ) "*" (l :> string list))
        (if and_all_others <> None && aggregate_fields <> [] then sep else "")
        (List.print ~first:"" ~last:"" ~sep
          (print_selected_field with_types)) aggregate_fields ;
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
      ) aggregate_event_time

  | ReadExternal { source ; format ; event_time ; _ } ->
    Printf.fprintf oc "%tREAD FROM %a %a" sp
      (print_external_source with_types) source
      (print_external_format) format ;
    Option.may (fun et ->
      sp oc ;
      RamenEventTime.print oc et
    ) event_time

  | ListenFor { net_addr ; port ; proto } ->
    Printf.fprintf oc "%tLISTEN FOR %s ON %s:%s" sp
      (RamenProtocols.string_of_proto proto)
      net_addr
      (Uint16.to_string port)

(* We need some tools to fold/iterate over all expressions contained in an
 * operation. We always do so depth first. *)

let fold_top_level_expr init f = function
  | ListenFor _ -> init
  | ReadExternal { source ; format ; _ } ->
      let x = fold_external_source init f source in
      fold_external_format x f format
  | Aggregate { aggregate_fields ; sort ; where ; key ; commit_cond ;
                notifications ; every ; _ } ->
      let open Raql_select_field.DessserGen in
      let x =
        List.fold_left (fun prev sf ->
            let what = Printf.sprintf "field %s" (N.field_color sf.alias) in
            f prev what sf.expr
          ) init aggregate_fields in
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
  fold_top_level_expr init (fun i c -> E.fold (f c) i)

let iter_expr f =
  fold_expr () (fun c s () e -> f c s e)

let map_top_level_expr f op =
  match op with
  | ListenFor _ -> op
  | ReadExternal ({ source ; format ; _ } as a) ->
      ReadExternal { a with
        source = map_external_source f source ;
        format = map_external_format f format }
  | Aggregate ({ aggregate_fields ; sort ; where ; key ; commit_cond ;
                  notifications ; _ } as a) ->
      let open Raql_select_field.DessserGen in
      Aggregate { a with
        aggregate_fields =
          List.map (fun sf ->
            { sf with expr = f sf.expr }
          ) aggregate_fields ;
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
  let start = N.field "start"
  and stop = N.field "stop"
  and duration = N.field "duration" in
  let time_fields = [ start ; stop ; duration ] in
  let filter_time_type mn name =
    if mn.DT.typ = DT.Unknown then
      invalid_arg "event_time_of_operation: untyped" ;
    let ok = not mn.nullable && DT.is_numeric mn.typ in
    if not ok && List.mem name time_fields then
      !logger.warning "The type of field %a does not suit event times"
        N.field_print name ;
    if ok then Some name else None in
  let event_time, fields =
    match op with
    | Aggregate { aggregate_event_time ; aggregate_fields ; _ } ->
        let open Raql_select_field.DessserGen in
        aggregate_event_time,
        List.filter_map (fun sf ->
          filter_time_type sf.expr.E.typ sf.alias
        ) aggregate_fields
    | ReadExternal { event_time ; format ; _ } ->
        event_time,
        fields_of_external_format format |>
        List.filter_map (fun ft ->
          filter_time_type ft.RamenTuple.typ ft.name)
    | ListenFor { proto ; _ } ->
        RamenProtocols.event_time_of_proto proto, []
  and event_time_from_fields fields =
    if List.mem start fields then
      Some RamenEventTime.(
        let open Event_time_field.DessserGen in
        (start, OutputField, 1.),
        if List.mem stop fields then
          StopField (stop, OutputField, 1.)
        else if List.mem duration fields then
          DurationField (duration, OutputField, 1.)
        else
          DurationConst 0.)
    else None
  in
  if event_time <> None then event_time else
  event_time_from_fields fields

let operation_with_event_time op event_time = match op with
  | Aggregate s -> Aggregate { s with aggregate_event_time = event_time }
  | ReadExternal s -> ReadExternal { s with event_time }
  | ListenFor _ -> op

let func_id_of_data_source = function
  | NamedOperation (site, rel_p_opt, f) ->
      site, Option.map N.rel_program rel_p_opt, f
  | SubQuery _ ->
      (* Should have been replaced by a hidden function
       * by the time this is called *)
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
  | ListenFor _ | ReadExternal _ ->
      []
  | Aggregate { from ; _ } ->
      List.map func_id_of_data_source from

let factors_of_operation = function
  | ReadExternal { readExternal_factors ; _ } -> readExternal_factors
  | Aggregate { aggregate_factors ; _ } -> aggregate_factors
  | ListenFor { factors ; proto ; _ } ->
      if factors <> [] then factors
      else RamenProtocols.factors_of_proto proto

let operation_with_factors op factors = match op with
  | ReadExternal s -> ReadExternal { s with readExternal_factors = factors }
  | Aggregate s -> Aggregate { s with aggregate_factors = factors }
  | ListenFor s -> ListenFor { s with factors }

(* Recursively filter out the private fields: *)
let filter_out_private typ =
  List.filter_map (fun ft ->
    if N.is_private ft.RamenTuple.name then None
    else Some { ft with typ = T.filter_out_private ft.typ }
  ) typ

(* Return the (likely) untyped output type, with (recursively) reordered record
 * fields as required to enable drawing fields from different record types.
 * There are a few places where reordering is not desired though:
 * - When decoding external data (CSV, CHB...) in which case we need to express
 *   exactly the type verbatim (for ReadExternal only)
 * - when typing, as for simplicity RamenTyping uses both the type computed
 *   by this function and Aggregate's list of fields. *)
let out_type_of_operation ?(reorder=true) ~with_priv op =
  let typ =
    match op with
    | Aggregate { aggregate_fields ; and_all_others ; _ } ->
        let open Raql_select_field.DessserGen in
        assert (and_all_others = None) ; (* Cleared after parsing of the program *)
        List.map (fun sf ->
          RamenTuple.{
            name = sf.alias ;
            doc = sf.doc ;
            aggr = sf.aggr ;
            typ = sf.expr.typ ;
            units = sf.expr.units }
        ) aggregate_fields
    | ReadExternal { format ; _ } ->
        fields_of_external_format format
    | ListenFor { proto ; _ } ->
        RamenProtocols.tuple_typ_of_proto proto in
  let typ =
    if with_priv then typ
    else filter_out_private typ in
  let typ =
    if reorder then RamenFieldOrder.order_tuple typ
    else typ in
  typ

(* Same as above, but return the output type as a Rec (the way it's
 * supposed to be!) *)
let out_record_of_operation ?reorder ~with_priv op =
  out_type_of_operation ?reorder ~with_priv op |>
  RamenTuple.to_record

let vars_of_operation tup_type op =
  fold_top_level_expr N.SetOfFields.empty (fun s _c e ->
    N.SetOfFields.union s (E.vars_of_expr tup_type e)
  ) op

let to_sorted_list s =
  N.SetOfFields.to_list s |> List.fast_sort N.compare

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
  | ListenFor _ | ReadExternal _ -> false
  | Aggregate { notifications ; _ } ->
      notifications <> []

(* Build the "early-filters" suitable to implement as much of the children's
 * where clause on the parent's side. The output is an array of scalar values
 * indexed by the scalar field index of the parent.
 * WARNING: This index has to match exactly the one computed when the
 * "scalar_extractors" vector is computed in the code generator, or filters
 * will silently never match anything!
 *
 * The idea is that the parent need no code generation to quickly compare
 * scalar values to some (set of) constant scalars. And given the scalar
 * extractors that are generated for all possible scalars in the parent output
 * type, the CodeGenLib_Skeleton code need not to know anything about the actual
 * type to perform that comparison (just comparing [T.value]s. *)
(* Call [f] for each scalar in mn with the path (a list of all types + index
 * from the root to the scalar): *)
let iter_scalars_with_path mn f =
  (* [i] is the current largest scalar index ; returns the new largest. *)
  let rec loop i path mn =
    match mn.DT.typ with
    | DT.Base _ | Usr _ ->
        f i (List.rev ((mn, Uint32.zero) :: path)) ;
        i + 1
    | Vec (d, mn') ->
        (* [j] iterate over the [d] items of the vector whereas [i] is as above
         * the scalar field index: *)
        let rec loop2 j i =
          if j >= d then i else
          loop2 (j + 1) (loop i ((mn, Uint32.of_int j) :: path) mn') in
        loop2 0 i
    | Arr _ | Set _ ->
        (* We cannot extract a value from a list (or a set) because they vary
         * in size; let's consider they have no scalars and move on. *)
        i
    | Tup mns ->
        (* [i] is as usual the largest scalar index while [j] count the tuple
         * fields: *)
        Array.fold_left (fun (i, j) mn' ->
          loop i ((mn, Uint32.of_int j) :: path) mn',
          j + 1
        ) (i, 0) mns |> fst
    | Rec mns ->
        (* [i] is as usual the largest scalar index while [j] count the record
         * fields: *)
        Array.fold_left (fun (i, j) (fn, mn') ->
          if N.is_private (N.field fn) then
            i, j + 1
          else (
            loop i ((mn, Uint32.of_int j) :: path) mn',
            j + 1)
        ) (i, 0) mns |> fst
    | Sum _ ->
        (* Sum types cannot be early-filtered because different alternatives
         * might have different number of scalars and those scalar types might
         * not be the same. Just pass. *)
        i
    | Ext _ ->
        (* For the purpose of early filtering those are absent *)
        i
    | _ ->
        assert false in
  loop 0 [] mn |> ignore

let scalar_filters_of_operation pop cop =
  let open Raql_path_comp.DessserGen in
  let rec convert_to_path = function
    | (DT.{ typ = Vec _ ; _ }, i) :: rest ->
        Idx i :: convert_to_path rest
    | (DT.{ typ = Tup _ ; _ }, i) :: rest ->
        Idx i :: convert_to_path rest
    | (DT.{ typ = Rec mns ; _ }, i) :: rest ->
        Name (N.field (fst mns.(Uint32.to_int i))) :: convert_to_path rest
    | _ :: [] ->
        []
    | p ->
        Printf.sprintf2 "convert_to_path: Don't know how to convert %a"
          (List.print (fun oc (mn, i) ->
            Printf.fprintf oc "(%a;%s)"
              DT.print_mn mn
              (Uint32.to_string i))) p |>
        failwith in
  (* The part of the where clause we can execute on the parent's side are
   * equality tests against constant values: *)
  match cop with
  | Aggregate { where ; _ } ->
      (* First, get all scalar tests that are decisive (ANDed): *)
      let p_out_mn =
        out_record_of_operation ~with_priv:false pop in
      let scalar_tests =
        E.as_nary E.And where |>
        List.filter_map E.get_scalar_test in
      !logger.debug "scalar_filters_of_operation: scalar tests = %a"
        (List.print (Tuple2.print E.print_path (Set.print T.print))) scalar_tests ;
      let filters = ref [] in
      iter_scalars_with_path p_out_mn (fun i path ->
        (* iter_scalars_with_path gives a path as a list of types+index, but
         * we want to compare with a path that's a list of path_comp: *)
        let path = convert_to_path path in
        (* Look for that path in the scalar_filters: *)
        match List.assoc path scalar_tests with
        | exception Not_found -> ()
        | vs ->
            !logger.debug "scalar_filters_of_operation: %a = %a"
              E.print_path path
              (Set.print T.print) vs ;
            filters := (i, Set.to_array vs) :: !filters
      ) ;
      Array.of_list !filters
  | _ ->
      (* Nothing that can be done on non-aggregate workers *)
      [||]

let resolve_unknown_variable resolver e =
  let open Raql_path_comp.DessserGen in
  E.map (fun stack e ->
    let resolver = function
      | [] | Idx _ :: _ as path -> (* Idx is TODO *)
          Printf.sprintf2 "Cannot resolve unknown path %a"
            E.print_path path |>
          failwith
      | Name n :: _ ->
          resolver stack n
    in
    match e.E.text with
    | Stateless (SL2 (
          Get, n, ({ text = Stateless (SL0 (Variable Unknown)) ; _ } as x))) ->
        let pref =
          match E.int_of_const n with
          | Some n -> resolver [ Idx (Uint32.of_int n) ]
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
          Stateless (SL2 (
              Get, n, { x with text = Stateless (SL0 (Variable pref)) })) }
    | _ -> e
  ) [] e

let field_in_globals globals n =
  List.exists (fun g -> g.Globals.name = n) globals

(* Also used by [RamenProgram] to check running condition *)
let prefix_def params globals def =
  resolve_unknown_variable (fun _stack n ->
    if RamenTuple.params_mem n params then Variable.Param else
    if field_in_globals globals n then Variable.GlobalVar else
    def)

(* Replace the expressions with [Unknown] with their likely tuple. *)
let resolve_unknown_variables params globals op =
  (* Unless it's a param (TODO: or an opened record), assume Unknow
   * belongs to def: *)
  match op with
  | Aggregate ({ aggregate_fields ; sort ; where ; key ; commit_cond ;
                 notifications ; every ; _ } as aggr) ->
      let open Raql_select_field.DessserGen in
      let is_selected_fields ?i name = (* Tells if a field is in _out_ *)
        list_existsi (fun i' sf ->
          sf.alias = name &&
          Option.map_default (fun i -> i' < i) true i
        ) aggregate_fields in
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
                Variable.Param
              else if field_in_globals globals n then
                Variable.GlobalVar
              (* Then into fields that have been defined before: *)
              else if allow_out && is_selected_fields ?i n then
                Variable.Out
              (* Then finally assume input: *)
              else Variable.In in
            !logger.debug "Field %a thought to belong to %s"
              N.field_print n
              (Variable.to_string pref) ;
            pref
          )
        )
      in
      let aggregate_fields =
        List.mapi (fun i sf ->
          { sf with expr = prefix_smart ~i sf.expr }
        ) aggregate_fields in
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
        aggregate_fields ; sort ; where ; key ; commit_cond ; notifications ;
        every }

  | ReadExternal _ ->
      (* Default to In if not a param, and then disallow In *)
      (* prefix_def will select Param if it is indeed in param, and only
       * if not will it assume it's in env; which makes sense as that's the
       * only two possible tuples here: *)
      map_top_level_expr (prefix_def params globals Env) op

  | op -> op

let get_variable e =
  match e.E.text with
  | Stateless (SL0 (Variable tuple |
                    Binding (RecordField (tuple, _)) |
                    Binding (RecordValue tuple))) ->
      tuple, None
  (* TO get a more helpful error message that mention the actual field: *)
  | Stateless (SL2 (Get, { text = Stateless (SL0 (Const (VString field))) ; _ },
                         { text = Stateless (SL0 (Variable tuple)) })) ->
      tuple, Some field
  | Stateless (SL0 EventStart) ->
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
      Out, Some "#start"
  | Stateless (SL0 EventStop) ->
      Out, Some "#stop"
  | _ ->
      raise Not_found

let all_used_variables =
  E.fold (fun _ lst e ->
    match get_variable e with
    | exception Not_found -> lst
    | v -> v :: lst) []

exception DependsOnInvalidVariable of (Variable.t * string)
let check_depends_only_on lst e =
  let check_can_use ?(field="") var =
    if not (List.mem var lst) then
      raise (DependsOnInvalidVariable (var, field))
  in
  all_used_variables e |>
  List.iter (fun (var, field) -> check_can_use ?field var)

let default_commit_cond = E.of_bool true

(* Check that the expression is valid, or return an error message.
 * Also perform some optimisation, numeric promotions, etc...
 * This is done after the parse rather than Rejecting the parsing
 * result for better error messages, and also because we need the
 * list of available parameters. *)
let checked ?(unit_tests=false) params globals op =
  (* Start by resolving Unknown variables into In/Out/Param etc: *)
  let op = resolve_unknown_variables params globals op in
  let check_pure clause =
    E.unpure_iter (fun _ _ ->
      failwith ("Stateful functions not allowed in "^ clause))
  and warn_no_group clause =
    E.unpure_iter (fun s e ->
      match e.E.text with
      | Stateful (LocalState, skip, stateful) ->
          !logger.warning
            "In %s: Locally stateful function without aggregation. \
             Do you mean %a%s?"
            clause
            (E.print_text ~max_depth:1 false)
              (Stateful (GlobalState, skip, stateful))
            (* TODO: Would be nicer if we were able to tell for sure but reaching
             * out to that info from here is complicated: *)
            (if s = [] (* top level expr, likely a field *) then
              ", or is "^ clause ^" aggregated using same"
            else "")
      | _ -> ())
  and check_fields_from lst where e =
    try check_depends_only_on lst e
    with DependsOnInvalidVariable (tuple, field) ->
      Printf.sprintf2 "Variable %s not allowed in %s (only %a)%s"
        (Variable.to_string tuple)
        where (pretty_list_print Variable.print) lst
        (if field = "" then ""
         else " (when accessing field "^ N.field_color (N.field field) ^")") |>
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
  let checked_event_time field_names (start_field, duration) =
    let open Event_time.DessserGen in
    let open Event_time_field.DessserGen in
    let checked_field (f, _src, scale as prev) =
      if RamenTuple.params_mem f params then (
        (* FIXME: check that the type is compatible with TFloat!
         *        And not nullable! *)
        (f, Parameter, scale)
      ) else (
        check_field_exists field_names f ;
        prev
      ) in
    let start_field = checked_field start_field
    and duration =
      match duration with
      | DurationConst _ -> duration
      | DurationField f -> DurationField (checked_field f)
      | StopField f -> StopField (checked_field f) in
    (start_field, duration)
  and check_factors field_names =
    List.iter (fun fn ->
      check_field_exists field_names fn ;
      check_field_is_public fn)
  in
  let op =
    match op with
    | Aggregate ({ aggregate_fields ; and_all_others ; sort ; where ; key ;
                   commit_cond ; aggregate_event_time ; notifications ; from ;
                   every ; aggregate_factors ; flush_how ; _ } as aggregate) ->
      let open Raql_select_field.DessserGen in
      (* STAR operator must have been dealt with by now normally, but
       * not for unit-testing:: *)
      assert (unit_tests || and_all_others = None) ;
      (* Check that we use the GroupState only for virtual fields: *)
      iter_expr (fun _ _ e ->
        match e.E.text with
        | Stateless (SL2 (
              Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                   { text = Stateless (SL0 (Variable (GroupState as var))) ;
                     _ })) ->
            let n = N.field n in
            if not (N.is_virtual n) then
              Printf.sprintf2 "Variable %s has only virtual fields (no %a)"
                (Variable.to_string var)
                N.field_print n |>
              failwith
        | _ -> ()) op ;
      (* Now check what tuple prefixes are used: *)
      let have_field alias =
        List.exists (fun sf' -> sf'.alias = alias) in
      let aggregate_fields =
        List.fold_left (fun prev_selected sf ->
          check_fields_from
            Variable.[
              Param; Env; GlobalVar; In; GroupState;
              Out (* FIXME: only if defined earlier *);
              OutPrevious ; Record ] "SELECT clause" sf.expr ;
          (* Check unicity of aliases *)
          if have_field sf.alias prev_selected then
            Printf.sprintf2 "Alias %a is not unique"
              N.field_print sf.alias |>
            failwith ;
          (* Check SAME aggr uses only out, and copy fields from in if needed: *)
          if sf.aggr = Some "same" then (
            let clause =
              Printf.sprintf2 "re-aggregated field %a" N.field_print sf.alias in
            check_fields_from
              Variable.[
                Param; Env; GlobalVar; In; Out; GroupState; OutPrevious;
                SortFirst; SortSmallest; SortGreatest; Record ]
              clause sf.expr ;
            let added =
              let have_field added alias =
                have_field alias prev_selected ||
                List.exists (fun sf -> sf.alias = alias) added in
              all_used_variables sf.expr |>
              List.fold_left (fun added -> function
                | Variable.In, Some alias
                  when not (have_field added (N.field alias)) ->
                    !logger.info "Re-Aggregate using field %s from input, \
                                  adding it to selection" alias ;
                    let text =
                      E.(Stateless (SL2 (
                        Get, E.of_string alias,
                             E.make (Stateless (SL0 (Variable In)))))) in
                    let expr = E.make text in
                    let alias = N.field alias in
                    let sf = { expr ; alias ; doc = "" ; aggr = None } in
                    sf :: added
                | _ ->
                    added
              ) [] in
            sf :: List.rev_append added prev_selected
          ) else
            sf :: prev_selected
        ) [] aggregate_fields |>
        List.rev in
      let field_names = List.map (fun sf -> sf.alias) aggregate_fields in
      let aggregate_event_time =
        Option.map (checked_event_time field_names) aggregate_event_time in
      check_factors field_names aggregate_factors ;
      check_fields_from
        Variable.[
          Param; Env; GlobalVar; In;
          GroupState; OutPrevious; Record ]
        "WHERE clause" where ;
      List.iter (fun k ->
        check_pure "GROUP-BY clause" k ;
        check_fields_from
          Variable.[
            Param; Env; GlobalVar; In ; Record ] "Group-By KEY" k
      ) key ;
      List.iter (fun name ->
        check_fields_from
          Variable.[ Param; Env; GlobalVar; In; Out; Record ]
          "notification" name
      ) notifications ;
      check_fields_from
        Variable.[
          Param; Env; GlobalVar; In;
          Out; OutPrevious;
          GroupState; Record ]
        "COMMIT WHEN clause" commit_cond ;
      Option.may (fun (_, until_opt, bys) ->
        Option.may (fun until ->
          check_fields_from
            Variable.[
              Param; Env; GlobalVar;
              SortFirst; SortSmallest; SortGreatest; Record ]
            "SORT-UNTIL clause" until
        ) until_opt ;
        List.iter (fun by ->
          check_fields_from
            Variable.[ Param; Env; GlobalVar; In; Record ]
            "SORT-BY clause" by
        ) bys
      ) sort ;
      Option.may
        (check_fields_from
          Variable.[ Param; Env; GlobalVar ] "EVERY clause") every ;
      if every <> None && from <> [] then
        failwith "Cannot have both EVERY and FROM" ;
      (* Check that we do not use any fields from out that is generated: *)
      let generators =
        List.filter_map (fun sf ->
          if E.is_generator sf.expr then Some sf.alias else None
        ) aggregate_fields in
      iter_expr (fun _ _ e ->
        match e.E.text with
        | Stateless (SL2 (
              Get, { text = Stateless (SL0 (Const (VString n))) ; _ },
                   { text = Stateless (SL0 (Variable OutPrevious)) ; _ })) ->
            let n = N.field n in
            if List.mem n generators then
              Printf.sprintf2 "Cannot use a generated output field %a"
                N.field_print n |>
              failwith
        | _ -> ()
      ) op ;
      (* Check that we do not use private fields from the input: *)
      (* FIXME: this is not enough as it only prevent selection of a direct
       * private input field, but what about private descendant? *)
      iter_expr (fun _ _ e ->
        match get_variable e with
        | exception Not_found -> ()
        | v, Some fn when Variable.has_type_input v &&
                          N.is_private (N.field fn) ->
            Printf.sprintf2 "Variable %a is private"
              N.field_print (N.field fn) |>
            failwith
        | _ -> ()
      ) op ;
      (* Check that if there is no aggregation then no LocalState is used
       * anywhere: *)
      if commit_cond == default_commit_cond &&
         flush_how = Reset &&
         key = [] then
        iter_top_level_expr warn_no_group op ;
      (* Check that the resulting list of fields is not longer than the
       * (arbitrary) maximum number, to produce a better error message than
       * the assertions in the worker executable: *)
      if List.length aggregate_fields > num_all_fields then
        Printf.sprintf2 "Too many fields (configured maximum is %d)"
          num_all_fields |>
        failwith ;
      Aggregate { aggregate with aggregate_fields ; aggregate_event_time }

    | ListenFor { proto ; factors ; _ } ->
      let tup_typ = RamenProtocols.tuple_typ_of_proto proto in
      let field_names = List.map (fun t -> t.RamenTuple.name) tup_typ in
      check_factors field_names factors ;
      op

    | ReadExternal ({ source ; format ; event_time ;
                      readExternal_factors ; _ } as readExternal) ->
      let field_names = fields_of_external_format format |>
                        List.map (fun t -> t.RamenTuple.name) in
      let event_time =
        Option.map (checked_event_time field_names) event_time in
      check_factors field_names readExternal_factors ;
      (* Unknown tuples has been defaulted to Param/Env already.
       * Let's now forbid explicit references to input: *)
      iter_top_level_expr (
        check_fields_from Variable.[ Param; Env; GlobalVar ]) op ;
      (* additionally, all expressions used for defining the source must be
       * stateless: *)
      iter_external_source (check_pure) source ;
      (* FIXME: check the CSV format is serializable *)
      ReadExternal { readExternal with event_time } in
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
    | Stateless (SL2 (Get, { text = Stateless (SL0 (Const (VString n))) ; _ }, _))
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
        { text = (Stateless (SL0 (Const p)) |
                 Vector [ { text = Stateless (SL0 (Const p)) ; _ } ]) ;
          _ }))
      when T.(is_round_integer (of_wire p)) ->
        Printf.sprintf2 "%s_%ath" (default_alias e) T.print (T.of_wire p)
    (* Some functions better leave no traces: *)
    | Stateless (SL1s (Print, es)) when es <> [] -> default_alias (List.last es)
    | Stateless (SL1 ((Cast _|UuidOfU128), e)) -> default_alias e
    | Stateful (_, _, SF1 (Group, e)) -> default_alias e
    | _ -> raise (Reject "must set alias")

  (* Either `expr` or `expr AS alias` or `expr AS alias "doc"`, or
   * `expr doc "doc"`: *)
  let selected_field m =
    let m = "selected field" :: m in
    (
      E.Parser.p ++ (
        (blanks -- strinG "doc" -- blanks -+ quoted_string >>:
         fun doc -> None, doc) |<|
        optional ~def:(None, "") (
          blanks -- strinG "as" -- blanks -+ some field_name ++
          optional ~def:"" (blanks -+ quoted_string))) ++
      optional ~def:None (
        blanks -+ some RamenTuple.Parser.default_aggr) >>:
      fun ((expr, (alias, doc)), aggr) ->
        let alias =
          Option.default_delayed (fun () ->
            N.field (default_alias expr)
          ) alias in
        Raql_select_field.DessserGen.{ expr ; alias ; doc ; aggr }
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
      strinG "event" -- blanks -- (strinG "starting" |<| strinG "starts") --
      blanks -- strinG "at" -- blanks -+ field_name ++ scale ++
      optional ~def:(DurationConst 0.) (
        (blanks -- optional ~def:() ((strinG "and" |<| strinG "with") -- blanks) --
         strinG "duration" -- blanks -+ (
           (field_name ++ scale >>: fun (n, s) ->
              DurationField (n, Field.OutputField, s)) |<|
           (duration >>: fun n -> DurationConst n)) |<|
         blanks -- strinG "and" -- blanks --
         (strinG "stops" |<| strinG "stopping" |<|
          strinG "ends" |<| strinG "ending") -- blanks --
         strinG "at" -- blanks -+
           (field_name ++ scale >>: fun (n, s) ->
              StopField (n, OutputField, s)))) >>:
      fun ((sta, sca), dur) ->
        (sta, Field.OutputField, sca), dur
    ) m

  let every_clause m =
    let m = "every clause" :: m in
    (
      strinG "every" -- blanks -+ E.Parser.p
    ) m

  let star_clause m =
    let m = "star selector" :: m in
    (
      star -+ optional ~def:[] (
        opt_blanks -+
        several_greedy ~sep:blanks (minus -- opt_blanks -+ field_name))
    ) m

  type selected_item = Field of selected_field | Star of N.field list

  let select_clause m =
    let m = "select clause" :: m in
    (
      (strinG "select" |<| strinG "yield") -- blanks -+
      several ~sep:list_sep (
        (star_clause >>: fun s -> Star s) |<|
        (selected_field >>: fun f -> Field f))
    ) m

  let event_time_start () =
    E.make (Stateless (SL2 (
      Get, E.of_string "start",
           E.make (Stateless (SL0 (Variable In))))))

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
        Uint32.of_int l, u, b
    ) m

  let where_clause m =
    let m = "where clause" :: m in
    ((strinG "where" |<| strinG "when") -- blanks -+ E.Parser.p) m

  let group_by m =
    let m = "group-by clause" :: m in
    (strinG "group" -- blanks -- strinG "by" -- blanks -+
     several ~sep:list_sep E.Parser.p) m

  type commit_spec =
    | NotifySpec of E.t
    | FlushSpec of flush_method
    | CommitSpec (* we would commit anyway, just a placeholder *)
    | CommitWhen of { before : bool ; cond : E.t }

  let notification_clause m =
    let m = "notification" :: m in
    (
      strinG "notify" -- blanks -+
      optional ~def:None (some E.Parser.p) >>: fun name ->
        NotifySpec (name |? E.of_string "Don't Panic!")
    ) m

  let flush m =
    let m = "flush clause" :: m in
    ((strinG "flush" >>: fun () -> Reset) |<|
     (strinG "keep" -- optional ~def:() (blanks -- strinG "all") >>:
       fun () -> Never) >>:
     fun s -> FlushSpec s) m

  let dummy_commit m =
    (strinG "commit" >>: fun () -> CommitSpec) m

  let commit_when m =
    (
      ((strinG "after" >>: fun _ -> false) |<|
       (strinG "before" >>: fun _ -> true)) +- blanks ++
       E.Parser.p >>: fun (before, cond) -> CommitWhen { before ; cond }
    ) m

  let default_commit =
    CommitWhen { before = false ; cond = default_commit_cond }

  let commit_clause m =
    let m = "commit clause" :: m in
    let on_commit =
      several ~sep:list_sep_and ~what:"commit clauses"
        (dummy_commit |<| notification_clause |<| flush) in
    (
      (commit_when +- blanks ++ on_commit >>:
        fun (c, cs) -> c :: cs) |<|
      (on_commit ++ optional ~def:default_commit (blanks -+ commit_when) >>:
        fun (cs, c) -> c :: cs)
    ) m

  let default_port_of_protocol =
    let open Raql_net_protocol.DessserGen in
    function
    | Collectd -> 25826
    | NetflowV5 -> 2055
    | Graphite -> 2003

  let net_protocol m =
    let m = "network protocol" :: m in
    let open Raql_net_protocol.DessserGen in
    (
      (strinG "collectd" >>: fun () -> Collectd) |<|
      ((strinG "netflow" |<| strinG "netflowv5") >>: fun () -> NetflowV5) |<|
      (strinG "graphite" >>: fun () -> Graphite)
    ) m

  let network_address =
    several ~sep:none (cond "inet address" (fun c ->
      (c >= '0' && c <= '9') ||
      (c >= 'a' && c <= 'f') ||
      (c >= 'A' && c <= 'A') ||
      c = '.' || c = ':') '0') >>:
    fun s ->
      let s = String.of_list s in
      try
        ignore (Unix.inet_addr_of_string s) ;
        s
      with Failure x ->
        raise (Reject x)

  let inet_addr m =
    let m = "network address" :: m in
    (
      that_string "*" |<|
      that_string "[*]" |<|
      network_address
    ) m

  let host_port m =
    let m = "host and port" :: m in
    (
      inet_addr ++
      optional ~def:None (
        char ':' -+
        some (decimal_integer_range ~min:0 ~max:65535 "port number" >>:
          Uint16.of_int))
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
          | None ->
              "*",
              Uint16.of_int (default_port_of_protocol proto)
          | Some (addr, None) ->
              addr,
              Uint16.of_int (default_port_of_protocol proto)
          | Some (addr, Some port) ->
              addr,
              port in
        net_addr, port, proto) m

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
       strinG "separator" -- opt_blanks -+ (
         quoted_char |<|
         (* For backward compatibility also accept single-char strings: *)
         (quoted_string >>: (fun s ->
           if String.length s <> 1 then raise (Reject "Invalid CSV separator")
           else s.[0]))
       ) +-
       opt_blanks) ++
     optional ~def:Default.csv_null (
       strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) ++
     optional ~def:true (
       optional ~def:true (strinG "no" -- blanks >>: fun () -> false) +-
       strinGs "quote" +- blanks) ++
     optional ~def:"" (
       strinG "escape" -- optional ~def:() (blanks -- strinG "with") --
       opt_blanks -+ quoted_string +- opt_blanks) ++
     optional ~def:false (
       strinG "vectors" -- blanks -- strinG "of" -- blanks -- strinG "chars" --
       blanks -- strinG "as" -- blanks -+
       (
         (strinG "string" >>: fun () -> true) |||
         (strinG "vector" >>: fun () -> false)
       ) +- opt_blanks) ++
     optional ~def:false (
       strinG "clickhouse" -- opt_blanks -- strinG "syntax" -- opt_blanks >>:
         fun () -> true) ++
     fields_schema >>:
     fun ((((((separator, null), may_quote), escape_seq),
            vectors_of_chars_as_string), clickhouse_syntax),
          fields) ->
       if String.of_char separator = null then
         raise (Reject "Invalid CSV separator") ;
       { separator ; null ; may_quote ; escape_seq ; fields ;
         vectors_of_chars_as_string ; clickhouse_syntax }) m

  let row_binary_specs =
    char '(' -- opt_blanks -+
    DT.Parser.clickhouse_names_and_types +-
    opt_blanks +- char ')' >>: function
      | Rec mns ->
          RowBinary (
            Array.enum mns /@
            (fun (n, mn) ->
              let name = N.field n
              and typ = mn in
              RamenTuple.{ name ; typ ; units = None ;
                                doc = "" ; aggr = None }) |>
            List.of_enum)
      | _ ->
          assert false

  let external_format m =
    let m = "external data format" :: m in
    (
      strinG "as" -- blanks -+ (
        (strinG "csv" -- blanks -+ csv_specs >>: fun s -> CSV s) |<|
        (strinG "rowbinary" -- blanks -+ row_binary_specs)
      )
    ) m

  let file_specs m =
    let m = "read file operation" :: m in
    (
      E.Parser.p ++
      optional ~def:None (
        blanks -+
        (strinG "preprocessed" |<| strinG "preprocess") -- blanks --
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
      optional ~def:None (
        strinGs "partition" -- blanks -+
        some (E.Parser.(vector p) |<| E.Parser.param) +- blanks) +-
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
        (strinGs "file" -- blanks -+ file_specs >>: fun s -> File s) |<|
        (strinG "kafka" -- blanks -+ kafka_specs >>: fun s -> Kafka s)
      )
    ) m

  let read_clause m =
    let m = "read" :: m in
    (
      strinG "read" -- blanks -+ (
        (
          external_source +- blanks ++ external_format
        ) |<| (
          external_format +- blanks ++ external_source >>: fun (a, b) -> b, a
        )
      )
    ) m

  let factor_clause m =
    let m = "factors" :: m
    and field = field_name in
    (strinGs "factor" -- blanks -+
     several ~sep:list_sep_and field) m

  (* BUMMER! we cannot define data_source independently because it's
   * recusring on raql_operation. We need an alias for this one! *)
  type data_source = ee20956156b3a0bf3ed4185051a85c84
  type external_source = v_8c0c938be0fcefc45cc5b9cf52c46f04
  type external_format = v_21e8c6eca31cc038e9faa45d5b86bfa4

  type select_clauses =
    | SelectClause of selected_item list
    | SortClause of (Uint32.t * E.t option (* until *) * E.t list (* by *))
    | WhereClause of E.t
    | EventTimeClause of RamenEventTime.t
    | FactorClause of N.field list
    | GroupByClause of E.t list
    | CommitClause of commit_spec list
    | FromClause of data_source list
    | EveryClause of E.t option
    | ListenClause of (string * Uint16.t * RamenProtocols.t)
    | InstrumentationClause of string
    | ReadClause of (external_source * external_format)
  (* A special from clause that accept globs, used to match workers in
   * instrumentation operations. *)
  let from_pattern m =
    let what = "pattern" in
    let m = what :: m in
    let first_char = letter |<| underscore |<| dot |<| slash |<| star in
    let any_char = first_char |<| decimal_digit in
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
    (unquoted |<| quoted) m

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
        ) |<| (
          func_identifier ++
          optional ~def:AllSites (
            blanks -- strinG "on" -- blanks -+ (
              (
                strinG "this" -- blanks -- strinG "site" >>:
                fun () -> ThisSite
              ) |<| (
                strinGs "site" -- blanks -+ site_identifier >>:
                fun s -> TheseSites s
              )
            )
          ) >>: fun ((p, f), s) -> NamedOperation (s, (p :> string option), f)
        )
      )
    ) m

  and p m =
    let m = "operation" :: m in
    let part =
      (select_clause >>: fun c -> SelectClause c) |<|
      (sort_clause >>: fun c -> SortClause c) |<|
      (where_clause >>: fun c -> WhereClause c) |<|
      (event_time_clause >>: fun c -> EventTimeClause c) |<|
      (group_by >>: fun c -> GroupByClause c) |<|
      (commit_clause >>: fun c -> CommitClause c) |<|
      (from_clause >>: fun c -> FromClause c) |<|
      (every_clause >>: fun c -> EveryClause (Some c)) |<|
      (listen_clause >>: fun c -> ListenClause c) |<|
      (read_clause >>: fun c -> ReadClause c) |<|
      (factor_clause >>: fun c -> FactorClause c) in
    (several ~sep:blanks part >>: fun clauses ->
      (* Used for its address: *)
      let default_select = []
      and default_sort = None
      and default_where = E.of_bool true
      and default_event_time = None
      and default_key = []
      and default_commit = [ default_commit ]
      and default_from = []
      and default_every = None
      and default_listen = None
      and default_instrumentation = ""
      and default_read_clause = None
      and default_factors = [] in
      let default_clauses =
        default_select, default_sort,
        default_where, default_event_time, default_key,
        default_commit, default_from, default_every,
        default_listen, default_instrumentation, default_read_clause,
        default_factors in
      let select, sort, where,
          event_time, key, commit, from, every, listen, instrumentation,
          read, factors =
        List.fold_left (
          fun (select, sort, where,
               event_time, key, commit, from, every, listen,
               instrumentation, read, factors) ->
            (* FIXME: in what follows, detect and signal cases when a new value
             * replaces an old one (but the default), such as when two WHERE
             * clauses are given. *)
            function
            | SelectClause fields_or_stars ->
              if select != default_select then
                raise (Reject "Cannot have several SELECT clauses") ;
              fields_or_stars, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | SortClause sort ->
              select, Some sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | WhereClause where ->
              select, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | EventTimeClause event_time ->
              select, sort, where,
              Some event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | GroupByClause key ->
              select, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | CommitClause commit' ->
              if commit != default_commit then
                raise (Reject "Cannot have several COMMIT clauses") ;
              select, sort, where,
              event_time, key, commit', from, every, listen,
              instrumentation, read, factors
            | FromClause from' ->
              select, sort, where,
              event_time, key, commit, (List.rev_append from' from),
              every, listen, instrumentation, read, factors
            | EveryClause every ->
              select, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
            | ListenClause l ->
              select, sort, where,
              event_time, key, commit, from, every, Some l,
              instrumentation, read, factors
            | InstrumentationClause c ->
              select, sort, where,
              event_time, key, commit, from, every, listen, c,
              read, factors
            | ReadClause c ->
              select, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, Some c, factors
            | FactorClause factors ->
              select, sort, where,
              event_time, key, commit, from, every, listen,
              instrumentation, read, factors
          ) default_clauses clauses in
      (* Try to catch when we write "commit when" instead of "commit
       * after/before": *)
      if commit = [ CommitSpec ] then
        raise (Reject "Lone COMMIT makes no sense. \
                       Do you mean COMMIT AFTER/BEFORE?") ;
      (* Distinguish between Aggregate, Read, ListenFor...: *)
      let not_aggregate =
        select == default_select && sort == default_sort &&
        where == default_where && key == default_key &&
        commit == default_commit
      and not_listen =
        listen = None || from != default_from || every != default_every
      and not_instrumentation = instrumentation = ""
      and not_read =
        read = None || from != default_from || every != default_every
      and not_event_time = event_time = default_event_time in
      if not_listen && not_read && not_instrumentation then
        let commit_before, commit_cond, flush_how, notifications =
          List.fold_left (fun (b, c, f, n as prev) -> function
            | CommitSpec -> prev
            | NotifySpec n' -> b, c, f, n'::n
            | FlushSpec f' ->
                if f = None then b, c, Some f', n
                else raise (Reject "Several flush clauses")
            | CommitWhen { before ; cond } ->
                if c == default_commit_cond then before, cond, f, n
                else raise (Reject "Several commit conditions")
          ) (false, default_commit_cond, None, []) commit in
        let flush_how = flush_how |? Reset in
        let fields, and_all_others =
          List.fold_left (fun (fields, and_all_others) -> function
            | Field f -> f :: fields, and_all_others
            | Star s ->
                if and_all_others <> None then
                  raise (Reject "Several '*' clauses") ;
                fields, Some s
          ) ([], None) select in
        let aggregate_fields = List.rev fields
        and aggregate_event_time = event_time
        and aggregate_factors = factors in
        Aggregate { aggregate_fields ; and_all_others ; sort ;
                    where ; aggregate_event_time ; notifications ; key ;
                    commit_before ; commit_cond ; flush_how ; from ;
                    every ; aggregate_factors }
      else if not_aggregate && not_read && not_event_time &&
              not_instrumentation && listen <> None then
        let net_addr, port, proto = Option.get listen in
        ListenFor { net_addr ; port ; proto ; factors }
      else if not_aggregate && not_listen &&
              not_instrumentation &&
              read <> None then
        let source, format = Option.get read
        and readExternal_factors = factors in
        ReadExternal { source ; format ; event_time ; readExternal_factors }
      else
        raise (Reject "Incompatible mix of clauses")
    ) m

  (*$inject
    let test_op s =
      (match test_p p s with
      | Ok (res, rem) ->
        let params =
          [ Program_parameter.DessserGen.{
              ptyp = { name = N.field "avg_window" ;
                       typ = DT.(required (Base I32)) ;
                       units = None ; doc = "" ; aggr = None } ;
              value = T.(to_wire (VI32 10l)) }] in
        BatPervasives.Ok (
          RamenOperation.checked ~unit_tests:true params [] res,
          rem)
      | x -> x) |>
      TestHelpers.test_printer (RamenOperation.print false)
  *)
  (*$= test_op & ~printer:BatPervasives.identity
    "FROM 'foo' SELECT in.'start', in.'stop', in.'itf_clt' AS 'itf_src', in.'itf_srv' AS 'itf_dst'" \
      (test_op "from foo select start, stop, itf_clt as itf_src, itf_srv as itf_dst")

    "FROM 'foo' WHERE (in.'packets') > (0)" \
      (test_op "from foo where packets > 0")

    "FROM 'foo' SELECT in.'t', in.'value' EVENT STARTING AT t*10. AND DURATION 60." \
      (test_op "from foo select t, value aggregates using max event starting at t*10 with duration 60s")

    "FROM 'foo' SELECT in.'t1', in.'t2', in.'value' EVENT STARTING AT t1*10. AND STOPPING AT t2*10." \
      (test_op "from foo select t1, t2, value event starting at t1*10. and stopping at t2*10.")

    "FROM 'foo' NOTIFY \"ouch\"" \
      (test_op "from foo NOTIFY \"ouch\"")

    "FROM 'foo' SELECT MIN LOCALLY skip nulls(in.'start') AS 'start', \\
       MAX LOCALLY skip nulls(in.'stop') AS 'max_stop', \\
       (SUM LOCALLY skip nulls(in.'packets')) / \\
         (param.'avg_window') AS 'packets_per_sec' \\
     GROUP BY (in.'start') / ((1000000) * (param.'avg_window')) \\
     COMMIT AFTER \\
       ((MAX LOCALLY skip nulls(in.'start')) + (3600)) > (out.'start')" \
        (test_op "select min start as start, \\
                           max stop as max_stop, \\
                           (sum packets)/avg_window as packets_per_sec \\
                   from foo \\
                   group by start / (1_000_000 * avg_window) \\
                   commit after out.start < (max in.start) + 3600")

    "FROM 'foo' SELECT 1 AS 'one' GROUP BY true COMMIT BEFORE (SUM LOCALLY skip nulls(1)) >= (5)" \
        (test_op "select 1 as one from foo commit before sum 1 >= 5 group by true")

    "FROM 'foo/bar' SELECT in.'n', LAG GLOBALLY skip nulls(2, out.'n') AS 'l'" \
        (test_op "SELECT n, lag globally(2, n) AS 'l' FROM foo/bar")

    "READ FROM FILES \"/tmp/toto.csv\" \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read file \"/tmp/toto.csv\" as csv (f1 bool?, f2 i32)")

    "READ FROM FILES \\
      CASE WHEN env.'glop' THEN \"glop.csv\" ELSE \"pas glop.csv\" END \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read from files (IF glop THEN \"glop.csv\" ELSE \"pas glop.csv\") as csv (f1 bool?, f2 i32)")

    "READ FROM FILES \"/tmp/toto.csv\" THEN DELETE \\
      AS CSV NO QUOTES (f1 BOOL?, f2 I32)" \
      (test_op "read from file \"/tmp/toto.csv\" then delete as csv no quote (f1 bool?, f2 i32)")

    "READ FROM FILES \"foo\" THEN DELETE IF env.'delete_flag' \\
      AS CSV NO QUOTES (x BOOL)" \
      (test_op "read from file \"foo\" then delete if delete_flag as csv no quote (x bool)")

    "READ FROM FILES \"/tmp/toto.csv\" \\
      AS CSV SEPARATOR #\\tab NULL \"<NULL>\" (f1 BOOL?, f2 I32)" \
      (test_op "read from file \"/tmp/toto.csv\" as csv \\
                      separator #\\tab null \"<NULL>\" \\
                      (f1 bool?, f2 i32)")

   "READ FROM KAFKA TOPIC \"foo\" WITH OPTIONS \\
      \"foo.bar\" = \"glop\" AND \"metadata.broker.list\" = \"localhost:9002\" \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read from kafka topic \"foo\" with options \\
        \"foo.bar\"=\"glop\", \"metadata.broker.list\" = \"localhost:9002\" \\
        as csv (f1 bool?, f2 i32)")

   "READ FROM KAFKA TOPIC \"foo\" PARTITIONS [0] WITH OPTIONS \\
      \"foo.bar\" = \"glop\" AND \"metadata.broker.list\" = \"localhost:9002\" \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read from kafka topic \"foo\" partitions [0] with options \\
        \"foo.bar\"=\"glop\", \"metadata.broker.list\" = \"localhost:9002\" \\
        as csv (f1 bool?, f2 i32)")

    "READ FROM KAFKA TOPIC \"foo\" PARTITIONS [0; 1] WITH OPTIONS \\
      \"foo.bar\" = \"glop\" AND \"metadata.broker.list\" = \"localhost:9002\" \\
      AS CSV (f1 BOOL?, f2 I32)" \
      (test_op "read from kafka topic \"foo\" partitions [ 0 ; 1 ] with options \\
        \"foo.bar\"=\"glop\", \"metadata.broker.list\" = \"localhost:9002\" \\
        as csv (f1 bool?, f2 i32)")

    "SELECT 1 AS 'one' EVERY 1{seconds}" \
        (test_op "YIELD 1 AS one EVERY 1 SECONDS")

    "LISTEN FOR NetflowV5 ON 1.2.3.4:1234" \
        (test_op "LISTEN FOR netflow ON 1.2.3.4:1234")

    "FROM 'foo' SELECT *" (test_op "select * from foo")
    "FROM 'foo' SELECT *" (test_op "from foo select *")

    "FROM 'foo' SELECT * -'pas_glop' -'pas_glop2', in.'glop'" \
      (test_op "from foo select * - pas_glop - pas_glop2, glop")
  *)

  (*$>*)
end
