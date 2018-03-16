open Batteries
open Lang
open Helpers
open RamenLog
open RamenSharedTypes
module Expr = RamenExpr

(*$inject
  open TestHelpers
  open Lang
*)

(* Direct field selection (not for group-bys) *)
type selected_field = { expr : Expr.t ; alias : string }

let print_selected_field fmt f =
  let need_alias =
    match f.expr with
    | Expr.Field (_, tuple, field)
      when !tuple = TupleIn && f.alias = field -> false
    | _ -> true in
  if need_alias then
    Printf.fprintf fmt "%a AS %s"
      (Expr.print false) f.expr
      f.alias
  else
    Expr.print false fmt f.expr

type flush_method = Reset | Slide of int
                  | KeepOnly of Expr.t | RemoveAll of Expr.t
                  | Never

let print_flush_method ?(prefix="") ?(suffix="") () oc = function
  | Reset ->
    Printf.fprintf oc "%sFLUSH%s" prefix suffix
  | Never ->
    Printf.fprintf oc "%sKEEP ALL%s" prefix suffix
  | Slide n ->
    Printf.fprintf oc "%sSLIDE %d%s" prefix n suffix
  | KeepOnly e ->
    Printf.fprintf oc "%sKEEP (%a)%s" prefix (Expr.print false) e suffix
  | RemoveAll e ->
    Printf.fprintf oc "%sREMOVE (%a)%s" prefix (Expr.print false) e suffix

type event_start = string * float
type event_duration = DurationConst of float (* seconds *)
                    | DurationField of (string * float)
                    | StopField of (string * float)
type event_time = (event_start * event_duration)

type file_spec = { fname : string ; unlink : bool }
type download_spec = { url : string ; accept : string }
type csv_specs =
  { separator : string ; null : string ; fields : RamenTuple.typ }

(* ReadFile has the func reading files directly on disc.
 * DownloadFile is (supposed to be) ramen downloading the content into
 * a temporary directory and spawning a worker that also perform a ReadFile.
 * ReceiveFile is similar: the file is actually received by ramen which
 * write it in a temporary directory for its ReadFile worker. Those files
 * are to be POSTed to $RAMEN_URL/upload/$url_suffix. *)
type where_specs = ReadFile of file_spec
                 | ReceiveFile
                 | DownloadFile of download_spec

type t =
  (* Generate values out of thin air. The difference with Select is that
   * Yield does not wait for some input. *)
  | Yield of {
      fields : selected_field list ;
      every : float ;
      event_time : event_time option ;
      force_export : bool }
  (* Aggregation of several tuples into one based on some key. Superficially looks like
   * a select but much more involved. *)
  | Aggregate of {
      fields : selected_field list ;
      (* Pass all fields not used to build an aggregated field *)
      and_all_others : bool ;
      sort : (int * Expr.t option (* until *) * Expr.t (* by *)) option ;
      (* Simple way to filter out incoming tuples: *)
      where : Expr.t ;
      event_time : event_time option ;
      force_export : bool ;
      (* If not empty, will notify this URL with a HTTP GET: *)
      notify_url : string ;
      key : Expr.t list ;
      top : (Expr.t (* N *) * Expr.t (* by *)) option ;
      commit_when : Expr.t ;
      commit_before : bool ; (* commit first and aggregate later *)
      (* How to flush: reset or slide values *)
      flush_how : flush_method ;
      (* List of funcs that are our parents *)
      from : string list }
  | ReadCSVFile of {
      where : where_specs ;
      what : csv_specs ;
      preprocessor : string ;
      event_time : event_time option ;
      force_export : bool }
  | ListenFor of {
      net_addr : Unix.inet_addr ;
      port : int ;
      proto : RamenProtocols.net_protocol ;
      event_time : event_time option ;
      force_export : bool }

let print_event_time fmt (start_field, duration) =
  let string_of_scale f = "*"^ string_of_float f in
  Printf.fprintf fmt "EVENT STARTING AT %s%s AND %s"
    (fst start_field)
    (string_of_scale (snd start_field))
    (match duration with
     | DurationConst f -> "DURATION "^ string_of_float f
     | DurationField (n, s) -> "DURATION "^ n ^ string_of_scale s
     | StopField (n, s) -> "STOPPING AT "^ n ^ string_of_scale s)

let print_csv_specs fmt specs =
  Printf.fprintf fmt "SEPARATOR %S NULL %S %a"
    specs.separator specs.null
    RamenTuple.print_typ specs.fields
let print_file_specs fmt specs =
  Printf.fprintf fmt "READ%s FILES %S"
    (if specs.unlink then " AND DELETE" else "") specs.fname
let print_download_specs fmt specs =
  Printf.fprintf fmt "DOWNLOAD FROM %S%s" specs.url
    (if specs.accept = "" then "" else
      Printf.sprintf " Accept: %S" specs.accept)
let print_upload_specs fmt =
  Printf.fprintf fmt "RECEIVE"
let print_where_specs fmt = function
  | ReadFile specs -> print_file_specs fmt specs
  | DownloadFile specs -> print_download_specs fmt specs
  | ReceiveFile -> print_upload_specs fmt

let print fmt =
  let sep = ", " in
  let print_single_quoted oc s = Printf.fprintf oc "'%s'" s in
  let print_export fmt event_time force_export =
    Option.may (fun e ->
      Printf.fprintf fmt " %a" print_event_time e
    ) event_time ;
    if force_export then Printf.fprintf fmt " EXPORT" in
  function
  | Yield { fields ; every ; event_time ; force_export } ->
    Printf.fprintf fmt "YIELD %a EVERY %g SECONDS"
      (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
      every ;
    print_export fmt event_time force_export ;
  | Aggregate { fields ; and_all_others ; where ; event_time ;
                force_export ; notify_url ; key ; top ; commit_when ;
                commit_before ; flush_how ; from } ->
    Printf.fprintf fmt "FROM %a SELECT %a%s%s"
      (List.print ~first:"" ~last:"" ~sep print_single_quoted) from
      (List.print ~first:"" ~last:"" ~sep print_selected_field) fields
      (if fields <> [] && and_all_others then sep else "")
      (if and_all_others then "*" else "") ;
    if not (Expr.is_true where) then
      Printf.fprintf fmt " WHERE %a"
        (Expr.print false) where ;
    print_export fmt event_time force_export ;
    if notify_url <> "" then
      Printf.fprintf fmt " NOTIFY %S" notify_url ;
    if key <> [] then
      Printf.fprintf fmt " GROUP BY %a"
        (List.print ~first:"" ~last:"" ~sep:", " (Expr.print false)) key ;
    Option.may (fun (n, by) ->
      Printf.fprintf fmt " TOP %a BY %a"
        (Expr.print false) n
        (Expr.print false) by) top ;
    if not (Expr.is_true commit_when) ||
       flush_how <> Reset then
      Printf.fprintf fmt " COMMIT %a%s %a"
        (print_flush_method ~prefix:"AND " ~suffix:" " ()) flush_how
        (if commit_before then "BEFORE" else "AFTER")
        (Expr.print false) commit_when
  | ReadCSVFile { where = where_specs ;
                  what = csv_specs ; preprocessor ; event_time ;
                  force_export } ->
    Printf.fprintf fmt "%a %s %a"
      print_where_specs where_specs
      (if preprocessor = "" then ""
        else Printf.sprintf "PREPROCESS WITH %S" preprocessor)
      print_csv_specs csv_specs ;
    print_export fmt event_time force_export ;
  | ListenFor { net_addr ; port ; proto ; event_time ; force_export } ->
    Printf.fprintf fmt "LISTEN FOR %s ON %s:%d"
      (RamenProtocols.string_of_proto proto)
      (Unix.string_of_inet_addr net_addr)
      port ;
    print_export fmt event_time force_export

let is_exporting = function
  | Aggregate { force_export ; _ }
  | Yield { force_export ; _ }
  | ListenFor { force_export ; _ }
  | ReadCSVFile { force_export ; _ } ->
    force_export (* FIXME: this info should come from the func *)

let run_in_tests = function
  | Aggregate _ | Yield _ -> true
  | ReadCSVFile _ | ListenFor _ -> false

let event_time_of_operation = function
  | Aggregate { event_time ; _ } -> event_time
  | ListenFor { proto = RamenProtocols.Collectd ; _ } ->
    Some (("time", 1.), DurationConst 0.)
  | ListenFor { proto = RamenProtocols.NetflowV5 ; _ } ->
    Some (("first", 1.), StopField ("last", 1.))
  | _ -> None

let parents_of_operation = function
  | ListenFor _ | ReadCSVFile _ | Yield _ -> []
  | Aggregate { from ; _ } -> from

let fold_expr init f = function
  | ListenFor _ | ReadCSVFile _ -> init
  | Yield { fields ; _ } ->
      List.fold_left (fun prev sf ->
          Expr.fold_by_depth f prev sf.expr
        ) init fields
  | Aggregate { fields ; sort ; where ; key ; top ; commit_when ;
                flush_how ; _ } ->
      let x =
        List.fold_left (fun prev sf ->
            Expr.fold_by_depth f prev sf.expr
          ) init fields in
      let x = Expr.fold_by_depth f x where in
      let x = List.fold_left (fun prev ke ->
            Expr.fold_by_depth f prev ke
          ) x key in
      let x = Option.map_default (fun (n, by) ->
          let x = Expr.fold_by_depth f x n in
          Expr.fold_by_depth f x by
        ) x top in
      let x = Expr.fold_by_depth f x commit_when in
      let x = match sort with
        | None -> x
        | Some (_, u_opt, b) ->
            let x = match u_opt with
              | None -> x
              | Some u -> Expr.fold_by_depth f x u in
            Expr.fold_by_depth f x b in
      match flush_how with
      | Slide _ | Never | Reset -> x
      | RemoveAll e | KeepOnly e ->
        Expr.fold_by_depth f x e

let iter_expr f op =
  fold_expr () (fun () e -> f e) op

(* Check that the expression is valid, or return an error message.
 * Also perform some optimisation, numeric promotions, etc...
 * This is done after the parse rather than Rejecting the parsing
 * result for better error messages. *)
let check =
  let pure_in clause = StatefulNotAllowed { clause }
  and no_group clause = GroupStateNotAllowed { clause }
  and fields_must_be_from tuple where allowed =
    TupleNotAllowed { tuple ; where ; allowed } in
  let pure_in_key = pure_in "GROUP-BY"
  and check_pure e =
    Expr.unpure_iter (fun _ -> raise (SyntaxError e))
  and check_no_group e =
    Expr.unpure_iter (function
      | StatefulFun (_, LocalState, _) -> raise (SyntaxError e)
      | _ -> ())
  and check_fields_from lst where =
    Expr.iter (function
      | Expr.Field (_, tuple, _) ->
        if not (List.mem !tuple lst) then (
          let m = fields_must_be_from !tuple where lst in
          raise (SyntaxError m)
        )
      | _ -> ())
  and check_event_time fields ((start_field, _), duration) =
    let check_field_exists f =
      if not (List.exists (fun sf -> sf.alias = f) fields) then
        let m =
          let print_alias oc sf = String.print oc sf.alias in
          let tuple_type = IO.to_string (List.print print_alias) fields in
          FieldNotInTuple { field = f ; tuple = TupleOut ; tuple_type } in
        raise (SyntaxError m)
    in
    check_field_exists start_field ;
    match duration with
    | DurationConst _ -> ()
    | DurationField (f, _)
    | StopField (f, _) -> check_field_exists f
  in
  function
  | Yield { fields ; _ } ->
    List.iter (fun sf ->
        let e = StatefulNotAllowed { clause = "YIELD" } in
        check_pure e sf.expr ;
        check_fields_from [TupleLastIn; TupleOut (* FIXME: only if defined earlier *)] "YIELD clause" sf.expr
      ) fields
    (* TODO: check unicity of aliases *)
  | Aggregate { fields ; and_all_others ; where ; key ; top ;
                commit_when ; flush_how ; event_time ;
                from ; _ } as op ->
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
    (* Resolve TupleUnknown into either TupleIn or TupleOut depending
     * on the presence of this alias in selected_fields (optionally,
     * only before position i) *)
    let prefix_smart ?i =
      Expr.iter (function
        | Field (_, ({ contents = TupleUnknown } as pref), alias) ->
            if Set.mem alias !fields_from_in then
              pref := TupleIn
            else if is_selected_fields ?i alias then
              pref := TupleOut
            else (
              pref := TupleIn ;
              fields_from_in := Set.add alias !fields_from_in) ;
            !logger.debug "Field %S thought to belong to %s"
              alias (string_of_prefix !pref)
        | _ -> ())
    and prefix_def def =
      Expr.iter (function
        | Field (_, ({ contents = TupleUnknown } as pref), _) ->
            pref := def
        | _ -> ()) in
    List.iteri (fun i sf -> prefix_smart ~i sf.expr) fields ;
    prefix_smart where ;
    List.iter (prefix_def TupleIn) key ;
    Option.may (fun (n, by) ->
      prefix_smart n ; prefix_def TupleIn by) top ;
    prefix_smart commit_when ;
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
        check_fields_from [TupleLastIn; TupleIn; TupleGroup; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroupFirst; TupleGroupLast; TupleOut (* FIXME: only if defined earlier *); TupleGroupPrevious; TupleOutPrevious] "SELECT clause" sf.expr ;
        (* Check unicity of aliases *)
        if List.mem sf.alias prev_aliases then
          raise (SyntaxError (AliasNotUnique sf.alias)) ;
        sf.alias :: prev_aliases
      ) [] fields |> ignore;
    if not and_all_others then Option.may (check_event_time fields) event_time ;
    (* Disallow group state in WHERE because it makes no sense: *)
    check_no_group (no_group "WHERE") where ;
    check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleGroup; TupleGroupFirst; TupleGroupLast; TupleOutPrevious] "WHERE clause" where ;
    List.iter (fun k ->
      check_pure pure_in_key k ;
      check_fields_from [TupleIn] "Group-By KEY" k) key ;
    Option.may (fun (n, by) ->
      (* TODO: Check also that it's an unsigned integer: *)
      Expr.check_const "TOP size" n ;
      check_fields_from [TupleIn] "TOP clause" by ;
      (* The only windowing mode supported is then `commit and flush`: *)
      if flush_how <> Reset then
        raise (SyntaxError OnlyTumblingWindowForTop)) top ;
    check_fields_from [TupleLastIn; TupleIn; TupleSelected; TupleLastSelected; TupleUnselected; TupleLastUnselected; TupleOut; TupleGroupPrevious; TupleOutPrevious; TupleGroupFirst; TupleGroupLast; TupleGroup; TupleSelected; TupleLastSelected] "COMMIT WHEN clause" commit_when ;
    (match flush_how with
    | Reset | Never | Slide _ -> ()
    | RemoveAll e | KeepOnly e ->
      let m = StatefulNotAllowed { clause = "KEEP/REMOVE" } in
      check_pure m e ;
      check_fields_from [TupleGroup] "REMOVE clause" e) ;
    if from = [] then
      raise (SyntaxError (MissingClause { clause = "FROM" })) ;
    (* Check that we do not use any fields from out that is generated: *)
    let generators = List.filter_map (fun sf ->
        if Expr.is_generator sf.expr then Some sf.alias else None
      ) fields in
    iter_expr (function
        | Field (_, tuple_ref, alias)
          when !tuple_ref = TupleOutPrevious ||
               !tuple_ref = TupleGroupPrevious ->
            if List.mem alias generators then
              let e = NoAccessToGeneratedFields { alias } in
              raise (SyntaxError e)
        | _ -> ()) op

    (* TODO: url_notify: check field names from text templates *)

  | ReadCSVFile _ | ListenFor _ -> ()

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let default_alias =
    let open Expr in
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
    | StatefulFun (_, _, AggrPercentile (Const (_, p), Field (_, _, field)))
      when RamenScalar.is_round_integer p ->
      Printf.sprintf "%s_%sth" (force_public field) (IO.to_string RamenScalar.print p)
    | _ -> raise (Reject "must set alias")

  let selected_field m =
    let m = "selected field" :: m in
    (Expr.Parser.p ++ optional ~def:None (
       blanks -- strinG "as" -- blanks -+ some non_keyword) >>:
     fun (expr, alias) ->
      let alias =
        Option.default_delayed (fun () -> default_alias expr) alias in
      { expr ; alias }) m

  let list_sep m =
    let m = "list separator" :: m in
    (opt_blanks -- char ',' -- opt_blanks) m

  let export_clause m =
    let m = "export clause" :: m in
    (strinG "export" >>: fun () -> true) m

  let event_time_clause m =
    let m = "event time clause" :: m in
    let scale m =
      let m = "scale event field" :: m in
      (optional ~def:1. (
        (optional ~def:() blanks -- char '*' --
         optional ~def:() blanks -+ number ))
      ) m
    in (
      strinG "event" -- blanks -- strinG "starting" -- blanks --
      strinG "at" -- blanks -+ non_keyword ++ scale ++
      optional ~def:(DurationConst 0.) (
        (blanks -- optional ~def:() ((strinG "and" ||| strinG "with") -- blanks) --
         strinG "duration" -- blanks -+ (
           (non_keyword ++ scale >>: fun n -> DurationField n) |||
           (number >>: fun n -> DurationConst n)) |||
         blanks -- strinG "and" -- blanks --
         (strinG "stopping" ||| strinG "ending") -- blanks --
         strinG "at" -- blanks -+
           (non_keyword ++ scale >>: fun n -> StopField n)))) m

  let notify_clause m =
    let m = "notify clause" :: m in
    (strinG "notify" -- blanks -+ quoted_string) m

  let yield_clause m =
    let m = "yield clause" :: m in
    (strinG "yield" -- blanks -+
     several ~sep:list_sep selected_field) m

  let yield_every_clause m =
    let m = "every clause" :: m in
    ((strinG "every" -- blanks -+ number +- blanks +-
      (strinG "seconds" ||| strinG "second")) >>:
      fun every ->
        if every < 0. then
          raise (Reject "sleep duration must be greater than 0") ;
        every) m

  let select_clause m =
    let m = "select clause" :: m in
    (strinG "select" -- blanks -+
     several ~sep:list_sep
             ((char '*' >>: fun _ -> None) |||
              some selected_field)) m

  let sort_clause m =
    let m = "sort clause" :: m in
    ((strinG "sort" -- blanks -- strinG "last" -- blanks -+
      pos_integer "Sort buffer size" ++
      optional ~def:None (
        blanks -- strinG "or" -- blanks -- strinG "until" -- blanks -+
        some Expr.Parser.p) +- blanks +-
      strinG "by" +- blanks ++ Expr.Parser.p) >>:
      fun ((l, u), b) -> l, u, b) m

  let where_clause m =
    let m = "where clause" :: m in
    ((strinG "where" ||| strinG "when") -- blanks -+ Expr.Parser.p) m

  let group_by m =
    let m = "group-by clause" :: m in
    (strinG "group" -- blanks -- strinG "by" -- blanks -+
     several ~sep:list_sep Expr.Parser.p) m

  let top_clause m =
    let m ="top-by clause" :: m in
    (strinG "top" -- blanks -+ Expr.Parser.p +- blanks +-
     strinG "by" +- blanks ++ Expr.Parser.p +- blanks +-
     strinG "when" +- blanks ++ Expr.Parser.p) m

  let flush m =
    let m = "flush clause" :: m in
    ((strinG "flush" >>: fun () -> Reset) |||
     (strinG "slide" -- blanks -+ (pos_integer "Sliding amount" >>:
        fun n -> Slide n)) |||
     (strinG "keep" -- blanks -- strinG "all" >>: fun () ->
       Never) |||
     (strinG "keep" -- blanks -+ Expr.Parser.p >>: fun e ->
       KeepOnly e) |||
     (strinG "remove" -- blanks -+ Expr.Parser.p >>: fun e ->
       RemoveAll e)) m

  let commit_when m =
    let m = "commit clause" :: m in
    (strinG "commit" -- blanks -+
     optional ~def:None
       (strinG "and" -- blanks -+ some flush +- blanks) ++
     ((strinG "after" >>: fun _ -> false) |||
      (strinG "when" >>: fun _ -> false) |||
      (strinG "before" >>: fun _ -> true)) +- blanks ++ Expr.Parser.p) m

  let from_clause m =
    let m = "from clause" :: m in
    (strinG "from" -- blanks -+
     several ~sep:list_sep (func_identifier ~program_allowed:true)) m

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
             optional ~def:None (char ':' -+ some unsigned_decimal_number))) >>:
     fun (proto, addr_opt) ->
        let net_addr, port =
          match addr_opt with
          | None -> Unix.inet_addr_any, default_port_of_protocol proto
          | Some (addr, None) -> addr, default_port_of_protocol proto
          | Some (addr, Some port) -> addr, Num.int_of_num port in
        net_addr, port, proto) m

  (* FIXME: It should be possible to enter separator, null, preprocessor in any order *)
  let read_file_specs m =
    let m = "read file operation" :: m in
    (strinG "read" -- blanks -+
     optional ~def:false (
       strinG "and" -- blanks -- strinG "delete" -- blanks >>:
         fun () -> true) +-
     (strinG "file" ||| strinG "files") +- blanks ++
     quoted_string >>: fun (unlink, fname) ->
       ReadFile { unlink ; fname }) m

  let download_file_specs m =
    let m = "download operation" :: m in
    (strinG "download" -- blanks --
     optional ~def:() (strinG "from" -- blanks) -+
     quoted_string ++
     repeat ~what:"download headers" ~sep:none (
       blanks -- strinG "accept" -- opt_blanks -- char ':' --
       opt_blanks -+ quoted_string) >>:
     function url, [accept] ->
       DownloadFile { url ; accept }
     | url, [] ->
       DownloadFile { url ; accept = "" }
     | _ ->
       raise (Reject "Only one header field can be set: Accept.")) m

  let upload_file_specs m =
    let m = "upload operation" :: m in
    (strinG "receive" >>: fun () -> ReceiveFile) m

  let external_data_clause =
    read_file_specs ||| download_file_specs ||| upload_file_specs

  let csv_specs  m =
    let m = "CSV format" :: m in
    let field =
      non_keyword +- blanks ++ RamenScalar.Parser.typ ++
      optional ~def:true (
        optional ~def:true (
          blanks -+ (strinG "not" >>: fun () -> false)) +-
        blanks +- strinG "null") >>:
      fun ((typ_name, typ), nullable) -> { typ_name ; typ ; nullable }
    in
    (optional ~def:"," (
       strinG "separator" -- opt_blanks -+ quoted_string +- opt_blanks) ++
     optional ~def:"" (
       strinG "null" -- opt_blanks -+ quoted_string +- opt_blanks) +-
     char '(' +- opt_blanks ++
     several ~sep:list_sep field +- opt_blanks +- char ')' >>:
     fun ((separator, null), fields) ->
       if separator = null || separator = "" then
         raise (Reject "Invalid CSV separator") ;
       { separator ; null ; fields }) m

  let preprocessor_clause m =
    let m = "file preprocessor" :: m in
    (strinG "preprocess" -- blanks -- strinG "with" -- opt_blanks -+
     quoted_string) m

  type select_clauses =
    | SelectClause of selected_field option list
    | SortClause of (int * Expr.t option (* until *) * Expr.t (* by *))
    | WhereClause of Expr.t
    | ExportClause of bool
    | EventTimeClause of event_time
    | NotifyClause of string
    | GroupByClause of Expr.t list
    | TopByClause of ((Expr.t (* N *) * Expr.t (* by *)) * Expr.t (* when *))
    | CommitClause of ((flush_method option * bool) * Expr.t)
    | FromClause of string list
    | YieldClause of selected_field list
    | EveryClause of float
    | ListenClause of (Unix.inet_addr * int * RamenProtocols.net_protocol)
    | ExternalDataClause of where_specs
    | PreprocessorClause of string
    | CsvSpecsClause of csv_specs

  let p m =
    let m = "operation" :: m in
    let part =
      (select_clause >>: fun c -> SelectClause c) |||
      (sort_clause >>: fun c -> SortClause c) |||
      (where_clause >>: fun c -> WhereClause c) |||
      (export_clause >>: fun c -> ExportClause c) |||
      (event_time_clause >>: fun c -> EventTimeClause c) |||
      (notify_clause >>: fun c -> NotifyClause c) |||
      (group_by >>: fun c -> GroupByClause c) |||
      (top_clause >>: fun c -> TopByClause c) |||
      (commit_when >>: fun c -> CommitClause c) |||
      (from_clause >>: fun c -> FromClause c) |||
      (yield_clause >>: fun c -> YieldClause c) |||
      (yield_every_clause >>: fun c -> EveryClause c) |||
      (listen_clause >>: fun c -> ListenClause c) |||
      (external_data_clause >>: fun c -> ExternalDataClause c) |||
      (preprocessor_clause >>: fun c -> PreprocessorClause c) |||
      (csv_specs >>: fun c -> CsvSpecsClause c) in
    (several ~sep:blanks part >>: fun clauses ->
      (* Used for its address: *)
      let default_select_fields = []
      and default_star = true
      and default_sort = None
      and default_where = Expr.expr_true
      and default_export = false
      and default_event_time = None
      and default_notify = ""
      and default_key = []
      and default_top = None
      and default_commit_before = false
      and default_commit_when = Expr.expr_true
      and default_flush_how = Reset
      and default_from = []
      and default_yield_fields = []
      and default_yield_every = 0.
      and default_listen = None
      and default_ext_data = None
      and default_preprocessor = ""
      and default_csv_specs = None in
      let default_clauses =
        default_select_fields, default_star, default_sort, default_where,
        default_export, default_event_time, default_notify, default_key,
        default_top, default_commit_before, default_commit_when,
        default_flush_how, default_from, default_yield_fields,
        default_yield_every, default_listen, default_ext_data,
        default_preprocessor, default_csv_specs in
      let select_fields, and_all_others, sort, where, force_export, event_time,
          notify_url, key, top, commit_before, commit_when, flush_how, from,
          yield_fields, yield_every, listen, ext_data, preprocessor,
          csv_specs =
        List.fold_left (
          fun (select_fields, and_all_others, sort, where, export, event_time,
               notify_url, key, top, commit_before, commit_when, flush_how,
               from, yield_fields, yield_every, listen, ext_data,
               preprocessor, csv_specs) ->
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
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | SortClause sort ->
              select_fields, and_all_others, Some sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | WhereClause where ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | ExportClause export ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | EventTimeClause event_time ->
              select_fields, and_all_others, sort, where, export, Some event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | NotifyClause notify_url ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | GroupByClause key ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | CommitClause ((Some flush_how, commit_before), commit_when) ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | CommitClause ((None, commit_before), commit_when) ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | TopByClause top ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, Some top, commit_before, commit_when,
              flush_how, from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | FromClause from' ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              (List.rev_append from' from), yield_fields, yield_every, listen,
              ext_data, preprocessor, csv_specs
            | YieldClause fields ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, fields, yield_every, listen, ext_data, preprocessor,
              csv_specs
            | EveryClause yield_every ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | ListenClause l ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, Some l, ext_data,
              preprocessor, csv_specs
            | ExternalDataClause c ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, Some c,
              preprocessor, csv_specs
            | PreprocessorClause preprocessor ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, csv_specs
            | CsvSpecsClause c ->
              select_fields, and_all_others, sort, where, export, event_time,
              notify_url, key, top, commit_before, commit_when, flush_how,
              from, yield_fields, yield_every, listen, ext_data,
              preprocessor, Some c
          ) default_clauses clauses in
      let commit_when, top =
        match top with
        | None -> commit_when, None
        | Some (top, top_when) ->
          if commit_when != default_commit_when ||
             flush_how != default_flush_how then
            raise (Reject "COMMIT and FLUSH clauses are incompatible \
                           with TOP") ;
          top_when, Some top in
      (* Distinguish between Aggregate, Yield ans ListenFor: *)
      let not_aggregate =
        select_fields == default_select_fields && sort == default_sort &&
        where == default_where && key == default_key && top == default_top &&
        commit_before == default_commit_before &&
        commit_when == default_commit_when &&
        flush_how == default_flush_how && from == default_from
      and not_yield =
        yield_fields == default_yield_fields &&
        yield_every == default_yield_every
      and not_listen = listen = None
      and not_csv =
        ext_data = None && preprocessor == default_preprocessor &&
        csv_specs = None in
      if not_yield && not_listen && not_csv then
        Aggregate { fields = select_fields ; and_all_others ; sort ; where ;
                    force_export ; event_time ; notify_url ; key ; top ;
                    commit_before ; commit_when ; flush_how ; from }
      else if not_aggregate && not_listen && not_csv &&
              yield_fields != default_yield_fields then
        Yield { fields = yield_fields ; every = yield_every ;
                force_export ; event_time }
      else if not_aggregate && not_yield && not_csv &&
              listen <> None then
        let net_addr, port, proto = Option.get listen in
        ListenFor { net_addr ; port ; proto ; force_export ; event_time }
      else if not_aggregate && not_yield && not_listen &&
              ext_data <> None && csv_specs <> None then
        ReadCSVFile { where = Option.get ext_data ;
                      what = Option.get csv_specs ;
                      preprocessor ;
                      force_export ; event_time }
      else
        raise (Reject "Incompatible mix of clauses")
    ) m

  (*$= p & ~printer:(test_printer print)
    (Ok (\
      Aggregate {\
        fields = [\
          { expr = Expr.(Field (typ, ref TupleIn, "start")) ;\
            alias = "start" } ;\
          { expr = Expr.(Field (typ, ref TupleIn, "stop")) ;\
            alias = "stop" } ;\
          { expr = Expr.(Field (typ, ref TupleIn, "itf_clt")) ;\
            alias = "itf_src" } ;\
          { expr = Expr.(Field (typ, ref TupleIn, "itf_srv")) ;\
            alias = "itf_dst" } ] ;\
        and_all_others = false ;\
        sort = None ;\
        where = Expr.Const (typ, VBool true) ;\
        notify_url = "" ;\
        key = [] ; top = None ;\
        commit_when = replace_typ Expr.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ;\
        force_export = false ; event_time = None ;\
        from = ["foo"] },\
      (67, [])))\
      (test_op p "from foo select start, stop, itf_clt as itf_src, itf_srv as itf_dst" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [] ;\
        and_all_others = true ;\
        sort = None ;\
        where = Expr.(\
          StatelessFun2 (typ, Gt, \
            Field (typ, ref TupleIn, "packets"),\
            Const (typ, VI32 (Int32.of_int 0)))) ;\
        force_export = false ; event_time = None ; notify_url = "" ;\
        key = [] ; top = None ;\
        commit_when = replace_typ Expr.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = ["foo"] },\
      (26, [])))\
      (test_op p "from foo where packets > 0" |> replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = Expr.(Field (typ, ref TupleIn, "t")) ;\
            alias = "t" } ;\
          { expr = Expr.(Field (typ, ref TupleIn, "value")) ;\
            alias = "value" } ] ;\
        and_all_others = false ;\
        sort = None ;\
        where = Expr.Const (typ, VBool true) ;\
        force_export = true ; event_time = Some (("t", 10.), DurationConst 60.) ;\
        notify_url = "" ;\
        key = [] ; top = None ;\
        commit_when = replace_typ Expr.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = ["foo"] },\
      (71, [])))\
      (test_op p "from foo select t, value export event starting at t*10 with duration 60" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = Expr.(Field (typ, ref TupleIn, "t1")) ;\
            alias = "t1" } ;\
          { expr = Expr.(Field (typ, ref TupleIn, "t2")) ;\
            alias = "t2" } ;\
          { expr = Expr.(Field (typ, ref TupleIn, "value")) ;\
            alias = "value" } ] ;\
        and_all_others = false ;\
        sort = None ;\
        where = Expr.Const (typ, VBool true) ;\
        force_export = true ; event_time = Some (("t1", 10.), StopField ("t2", 10.)) ;\
        notify_url = "" ; key = [] ; top = None ;\
        commit_when = replace_typ Expr.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = ["foo"] },\
      (82, [])))\
      (test_op p "from foo select t1, t2, value export event starting at t1*10 and stopping at t2*10" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [] ;\
        and_all_others = true ;\
        sort = None ;\
        where = Expr.Const (typ, VBool true) ;\
        force_export = false ; event_time = None ;\
        notify_url = "http://firebrigade.com/alert.php" ;\
        key = [] ; top = None ;\
        commit_when = replace_typ Expr.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = ["foo"] },\
      (50, [])))\
      (test_op p "from foo NOTIFY \"http://firebrigade.com/alert.php\"" |>\
       replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = Expr.(\
              StatefulFun (typ, LocalState, AggrMin (\
                Field (typ, ref TupleIn, "start")))) ;\
            alias = "start" } ;\
          { expr = Expr.(\
              StatefulFun (typ, LocalState, AggrMax (\
                Field (typ, ref TupleIn, "stop")))) ;\
            alias = "max_stop" } ;\
          { expr = Expr.(\
              StatelessFun2 (typ, Div, \
                StatefulFun (typ, LocalState, AggrSum (\
                  Field (typ, ref TupleIn, "packets"))),\
                Param (typ, "avg_window"))) ;\
            alias = "packets_per_sec" } ] ;\
        and_all_others = false ;\
        sort = None ;\
        where = Expr.Const (typ, VBool true) ;\
        force_export = false ; event_time = None ; \
        notify_url = "" ;\
        key = [ Expr.(\
          StatelessFun2 (typ, Div, \
            Field (typ, ref TupleIn, "start"),\
            StatelessFun2 (typ, Mul, \
              Const (typ, VI32 1_000_000l),\
              Param (typ, "avg_window")))) ] ;\
        top = None ;\
        commit_when = Expr.(\
          StatelessFun2 (typ, Gt, \
            StatelessFun2 (typ, Add, \
              StatefulFun (typ, LocalState, AggrMax (\
                Field (typ, ref TupleGroupFirst, "start"))),\
              Const (typ, VI32 (Int32.of_int 3600))),\
            Field (typ, ref TupleOut, "start"))) ; \
        commit_before = false ;\
        flush_how = Reset ;\
        from = ["foo"] },\
        (200, [])))\
        (test_op p "select min start as start, \\
                           max stop as max_stop, \\
                           (sum packets)/$avg_window as packets_per_sec \\
                   from foo \\
                   group by start / (1_000_000 * $avg_window) \\
                   commit when out.start < (max group.first.start) + 3600" |>\
         replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = Expr.Const (typ, VI32 (Int32.one)) ;\
            alias = "one" } ] ;\
        and_all_others = false ;\
        sort = None ;\
        where = Expr.Const (typ, VBool true) ;\
        force_export = false ; event_time = None ; \
        notify_url = "" ;\
        key = [] ; top = None ;\
        commit_when = Expr.(\
          StatelessFun2 (typ, Ge, \
            StatefulFun (typ, LocalState, AggrSum (\
              Const (typ, VI32 (Int32.one)))),\
            Const (typ, VI32 (Int32.of_int 5)))) ;\
        commit_before = true ;\
        flush_how = Reset ; from = ["foo"] },\
        (49, [])))\
        (test_op p "select 1 as one from foo commit before sum 1 >= 5" |>\
         replace_typ_in_op)

    (Ok (\
      Aggregate {\
        fields = [\
          { expr = Expr.Field (typ, ref TupleIn, "n") ; alias = "n" } ;\
          { expr = Expr.(\
              StatefulFun (typ, GlobalState, Expr.Lag (\
              Expr.Const (typ, VI32 (Int32.of_int 2)), \
              Expr.Field (typ, ref TupleIn, "n")))) ;\
            alias = "l" } ] ;\
        and_all_others = false ;\
        sort = None ;\
        where = Expr.Const (typ, VBool true) ;\
        force_export = false ; event_time = None ; \
        notify_url = "" ;\
        key = [] ; top = None ;\
        commit_when = replace_typ Expr.expr_true ;\
        commit_before = false ;\
        flush_how = Reset ; from = ["foo/bar"] },\
        (37, [])))\
        (test_op p "SELECT n, lag(2, n) AS l FROM foo/bar" |>\
         replace_typ_in_op)

    (Ok (\
      ReadCSVFile { where = ReadFile { fname = "/tmp/toto.csv" ; unlink = false } ; \
                    preprocessor = "" ; force_export = false ; event_time = None ; \
                    what = { \
                      separator = "," ; null = "" ; \
                      fields = [ \
                        { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                        { typ_name = "f2" ; nullable = false ; typ = TI32 } ] } },\
      (52, [])))\
      (test_op p "read file \"/tmp/toto.csv\" (f1 bool, f2 i32 not null)")

    (Ok (\
      ReadCSVFile { where = ReadFile { fname = "/tmp/toto.csv" ; unlink = true } ; \
                    preprocessor = "" ; force_export = false ; event_time = None ; \
                    what = { \
                      separator = "," ; null = "" ; \
                      fields = [ \
                        { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                        { typ_name = "f2" ; nullable = false ; typ = TI32 } ] } },\
      (63, [])))\
      (test_op p "read and delete file \"/tmp/toto.csv\" (f1 bool, f2 i32 not null)")

    (Ok (\
      ReadCSVFile { where = ReadFile { fname = "/tmp/toto.csv" ; unlink = false } ; \
                    preprocessor = "" ; force_export = false ; event_time = None ; \
                    what = { \
                      separator = "\t" ; null = "<NULL>" ; \
                      fields = [ \
                        { typ_name = "f1" ; nullable = true ; typ = TBool } ;\
                        { typ_name = "f2" ; nullable = false ; typ = TI32 } ] } },\
      (81, [])))\
      (test_op p "read file \"/tmp/toto.csv\" \\
                      separator \"\\t\" null \"<NULL>\" \\
                      (f1 bool, f2 i32 not null)")

    (Ok (\
      Yield {\
        fields = [ { expr = Expr.Const (typ, VI32 1l) ; alias = "one" } ] ;\
        every = 1. ; force_export = true ; event_time = None },\
        (36, [])))\
        (test_op p "YIELD 1 AS one EVERY 1 SECOND EXPORT" |>\
         replace_typ_in_op)
  *)

  (*$>*)
end
