(* This module deals with parsing programs.
 * It makes use of RamenOperation, which parses operations
 * (ie. function bodies).
 *)
open Batteries

open RamenLang
open RamenLog
open RamenHelpersNoLog
open RamenHelpers
module C = RamenConf
module DT = DessserTypes
module E = RamenExpr
module O = RamenOperation
module PParam = Program_parameter.DessserGen
module T = RamenTypes
module Variable = RamenVariable
module VSI = RamenSync.Value.SourceInfo
module Default = RamenConstsDefault
module Globals = RamenGlobalVariables
module Retention = RamenRetention

(*$inject
  open TestHelpers
  open RamenLang
  open Stdint
*)

(* A program is a set of parameters declaration and a set of functions.
 * Parameter declaration can be accompanied by a default value (by default,
 * a parameter default value will be NULL - and its type better be NULLable).
 * When running a program the user can override those defaults from the
 * command line.
 *
 * A running program is identified by its name + parameter values, so that it
 * is possible to run several time the same program with different parameters.
 *
 * To select from the function f from the program p running with default value,
 * select from p/f. To select from another instance of the same program running
 * with parameters p1=v1 and p2=v2, select from p{p1=v1,p2=v2}/f (order of
 * parameters does not actually matter, p{p2=v2,p1=v1}/f would identify the same
 * function).  *)

type func =
  { name : N.func option (* optional during parsing only *) ;
    doc : string ;
    operation : O.t ;
    retention : Retention.t option ;
    is_lazy : bool }

type t = RamenTuple.param list * func list

(* Anonymous functions (such as sub-queries) are given a boring name build
 * from a sequence: *)

let make_name =
  let seq = ref ~-1 in
  fun () ->
    incr seq ;
    N.func ("f"^ string_of_int !seq)

let make_func ?retention ?(is_lazy=false) ?name ?(doc="")
              operation =
  { name ; doc ; operation ; retention ; is_lazy }

(* Pretty-print a parsed program back to string: *)

let print_param oc p =
  Printf.fprintf oc "PARAMETER %a DEFAULTS TO %a;\n"
    RamenTuple.print_param_typ p
    T.print p.value

let print_global oc g =
  Printf.fprintf oc "DECLARE WITH %s SCOPE %a %a"
    (Globals.string_of_scope g.Globals.scope)
    N.field_print g.name
    DT.print_mn g.typ

let print_retention ?(with_types=false) oc r =
  Printf.fprintf oc
    "PERSIST FOR %a WHILE QUERYING EVERY %a"
    (E.print with_types) r.Retention.duration
    print_as_duration r.period

let print_func oc n =
  let with_types = false in (* TODO: a parameter *)
  match n.name with
  | None ->
      Printf.fprintf oc "%a;"
        (O.print with_types ) n.operation
  | Some name ->
      Printf.fprintf oc "DEFINE%s%a '%s' AS %a;"
        (if n.is_lazy then " LAZY" else "")
        (fun oc -> function
          | None -> ()
          | Some r ->
              Printf.fprintf oc " %a"
                (print_retention ~with_types) r) n.retention
        (name :> string)
        (O.print with_types) n.operation

let print oc (params, run_cond, globals, funcs, _warnings) =
  List.print ~first:"" ~last:"" ~sep:"" print_param oc params ;
  if not (E.is_true run_cond) then
    Printf.fprintf oc "RUN IF %a;\n" (E.print false) run_cond ;
  List.print ~first:"" ~last:"" ~sep:"" print_global oc globals ;
  List.print ~first:"" ~last:"" ~sep:"\n" print_func oc funcs

let has_star = function
  | O.Aggregate { and_all_others ; _ } -> and_all_others <> None
  | _ -> false

(* Check that a syntactically valid program is actually valid.
 * Returns a new programs with unknown variables replaced by actual ones
 * and unused params and globals removed. *)

let checked (params, run_cond, globals, funcs, warnings) =
  let warnings = ref warnings in
  let warn ?line ?column message =
    !logger.warning "%s" message ;
    warnings := RamenRaqlWarning.make ?line ?column message :: !warnings in
  let run_cond =
    (* No globals are accessible to the running condition *)
    O.prefix_def params [] Env run_cond in
  (* Check the running condition does not use any IO tuple: *)
  E.iter (fun _s e ->
    match e.E.text with
    | Stateless (SL0 (Variable tuple)) when
      Variable.has_type_input tuple ||
      Variable.has_type_output tuple ->
        Printf.sprintf "Running condition cannot use tuple %s"
          (Variable.to_string tuple) |>
        failwith
    | _ -> ()) run_cond ;
  let anonymous = N.func "<anonymous>" in
  let name_not_unique name =
    Printf.sprintf "Name %s is not unique" name |> failwith in
  (* Check parameters have unique names: *)
  List.fold_left (fun s p ->
    if Set.mem p.PParam.name s then
      name_not_unique (p.name :> string) ;
    Set.add p.name s
  ) Set.empty params |> ignore ;
  (* Check all functions in turn: *)
  let funcs, used_params, used_globals, _ =
    List.fold_left (fun (funcs, used_params, used_globals, names) n ->
      (* We should not have any STAR left at that point: *)
      assert (not (has_star n.operation)) ;
      (* Resolve unknown tuples in the operation: *)
      let op =
        (* Check the operation is OK: *)
        try O.checked params globals n.operation
        with Failure msg ->
          let open RamenTypingHelpers in
          Printf.sprintf "In function %s: %s"
            (N.func_color (n.name |? anonymous))
            msg |>
          failwith in
      (* Check that lazy functions do not emit notifications: *)
      if n.is_lazy && O.notifications_of_operation n.operation <> [] then
        Printf.sprintf2
          "Function %a defined as LAZY but emits notifications"
          N.func_print (n.name |? anonymous) |>
        warn ;
      (* Check that the name is valid and unique: *)
      let names =
        match n.name with
        | Some name ->
            let ns = (name :> string) in
            (* Names of defined functions cannot use '#' as we use it to delimit
             * special suffixes (stats, notifs): *)
            if String.contains ns '#' then
              Printf.sprintf "Invalid dash in function name %s"
                (N.func_color name) |> failwith ;
            (* Names must be unique: *)
            if Set.mem name names then name_not_unique ns ;
            Set.add name names
        | None -> names in
      (* Collect all parameters and global variables that are used: *)
      let collect_variable var used e =
        match e.E.text with
        | Stateless (SL2 (Get, n, { text = Stateless (SL0 (Variable v)) ;
                                    _ })) when v = var ->
            (match E.string_of_const n with
            | None ->
                Printf.sprintf2
                  "Cannot determine the name of variable in expression %a"
                  (E.print false) e |>
                warn ;
                used
            | Some name ->
                Set.add (N.field name) used)
        | _ -> used in
      let used_variables var used =
        O.fold_expr used (fun _ _ -> collect_variable var) op in
      let used_params =
        Retention.fold_expr used_params (collect_variable Param) n.retention in
      let used_params = used_variables Param used_params
      and used_globals = used_variables GlobalVar used_globals in
      { n with operation = op } :: funcs, used_params, used_globals, names
    ) ([], Set.empty, Set.empty, Set.empty) funcs in
  (* Remove unused parameters from params
   * See https://github.com/rixed/ramen/issues/731 *)
  let params =
    List.filter (fun p ->
      let is_used =
        Set.mem p.PParam.name used_params in
      if not is_used then
        Printf.sprintf2 "Parameter %a is unused" N.field_print p.name |>
        warn ;
      is_used
    ) params in
  (* Similarly, for the same reason, remove unused globals: *)
  let globals =
    List.filter (fun g ->
      let is_used = Set.mem g.Globals.name used_globals in
      if not is_used then
        Printf.sprintf2 "Global variable %a is unused" N.field_print g.name |>
        warn ;
      is_used
    ) globals in
  params, run_cond, globals, funcs, !warnings

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let params m =
    let m = "parameter" :: m in
    (
      strinGs "parameter" -- blanks -+
        several ~sep:list_sep_and (
          non_keyword ++
          optional ~def:None (
            blanks -+ some T.Parser.typ) ++
          optional ~def:None (
            blanks -+ some RamenUnits.Parser.p) ++
          optional ~def:Raql_value.VNull (
            blanks -- strinGs "default" -- blanks -- strinG "to" -- blanks -+
            ((duration >>: fun x -> Raql_value.VFloat x) |<|
             T.Parser.p_ ~min_int_width:0 |<|
             T.Parser.null)) ++
          optional ~def:"" quoted_string >>:
          fun ((((name, typ_decl), units), value), doc) ->
            let name = N.field name in
            let typ, value =
              let open T in
              match typ_decl with
              | None ->
                  if value = VNull then
                    let e =
                      Printf.sprintf2
                        "Declaration of parameter %a must either specify \
                         the type or a non-null default value"
                        N.field_print name in
                    raise (Reject e)
                  else
                    (* As usual, promote integers to 32 bits, preferably non
                     * signed, by default: *)
                    (try DT.u32,
                         enlarge_value TU32 value
                    with Invalid_argument _ ->
                      try DT.i32,
                          enlarge_value TI32 value
                      with Invalid_argument _ ->
                        DT.required (type_of_value value),
                        value)
              | Some typ ->
                  if value = VNull then
                    if typ.DT.nullable then
                      typ, value
                    else
                      let e =
                        Printf.sprintf2
                          "Parameter %a is not nullable, therefore it must have \
                           a default value"
                          N.field_print name in
                      raise (Reject e)
                  else
                    (* Scale the parsed type up to the declaration: *)
                    match enlarge_value typ.DT.typ value with
                    | exception Invalid_argument _ ->
                        let e =
                          Printf.sprintf2
                            "In declaration of parameter %a, type is \
                             incompatible with value %a"
                            N.field_print name
                            print value in
                        raise (Reject e)
                    | value -> typ, value
            in
            PParam.{
              name ; typ ; units ; doc ; value }
        )
    ) m

  let globals m =
    let scope m =
      let m = "variable scope" :: m in
      (
        blanks -- strinG "with" -- blanks -+
        (
          (strinG "PROGRAM" >>: fun () -> Globals.Program) |<|
          (strinG "SITE" >>: fun () -> Globals.Site) |<|
          (strinG "GLOBAL" >>: fun () -> Globals.Global)
        ) +-
        blanks +- strinG "scope"
      ) m in
    let m = "global variable declaration" :: m in
    (
      strinG "DECLARE" -+ optional ~def:Globals.Site scope +- blanks ++
      several ~sep:list_sep_and (non_keyword +- blanks ++ T.Parser.typ) >>:
      fun (scope, lst) ->
        List.map (fun (name, typ) ->
          let name = N.field name in
          Globals.{ scope ; name ; typ }
        ) lst
    ) m

  let run_cond m =
    let m = "running condition" :: m in
    (
      optional ~def:() (strinG "run" -- blanks) --
      strinG "if" -- blanks -+ E.Parser.p
    ) m

  let anonymous_func m =
    let m = "anonymous function" :: m in
    (O.Parser.p >>: make_func) m

  type func_flag =
    Name of N.func | Lazy | Persist of E.t | Querying of float | Ignore

  let named_func m =
    let m = "named function" :: m in
    (
      strinG "define" -- blanks -+
      repeat ~sep:none (
        (
          (
            strinG "lazy" >>: fun () -> Lazy
          ) |<| (
            strinG "persist" -- blanks -- strinG "for" -- blanks -+
            E.Parser.immediate_or_param >>: fun e -> Persist e
          ) |<| (
            (strinG "querying" |<| strinG "query") -- blanks --
            strinG "every" -- blanks -+ duration >>: fun d -> Querying d
          ) |<| (
            strinG "while" >>: fun () -> Ignore
          ) |<| (
            function_name >>: fun fname -> Name fname
          )
        ) +- blanks) ++
      optional ~def:"" (quoted_string +- blanks) +-
      strinG "as" +- blanks ++
      O.Parser.p >>:
      fun ((attrs, doc), op) ->
        let name =
          list_find_map_opt (function Name n -> Some n | _ -> None) attrs
        and duration =
          list_find_map_opt (function Persist r -> Some r | _ -> None) attrs
        and period =
          list_find_map_opt (function Querying d -> Some d | _ -> None) attrs
        and is_lazy = List.mem Lazy attrs in
        let name =
          match name with
          | None ->
              raise (Reject "cannot find function name")
          | Some n -> n in
        let retention =
          match duration, period with
          | Some duration, Some period ->
              Some Retention.{ duration ; period }
          | None, None ->
              None
          | Some duration, None ->
              Some VSI.{ duration ; period = Default.query_period }
          | None, Some _ ->
              raise (Reject "incomplete retention definition") in
        make_func ?retention ~is_lazy ~name ~doc op
    ) m

  let func m =
    (anonymous_func |<| named_func) m

  type definition =
    | DefFunc of func
    | DefParams of RamenTuple.param list
    | DefRunCond of E.t
    | DefGlobals of Globals.t list

  let p m =
    let m = "program" :: m in
    let default_run_cond = E.of_bool true in
    let sep = opt_blanks -- char ';' -- opt_blanks in
    (
      several ~sep ((func >>: fun f -> DefFunc f) |<|
                    (params >>: fun lst -> DefParams lst) |<|
                    (globals >>: fun lst -> DefGlobals lst) |<|
                    (run_cond >>: fun e -> DefRunCond e)) +-
      optional ~def:() (opt_blanks -- char ';') >>:
      fun defs ->
        let params, run_cond_opt, globals, funcs =
          List.fold_left (fun (params, run_cond_opt, globals, funcs) -> function
            | DefFunc func ->
                params, run_cond_opt, globals, func::funcs
            | DefParams lst ->
                List.rev_append lst params, run_cond_opt, globals, funcs
            | DefRunCond e ->
                if run_cond_opt <> None then
                  raise (Reject "Cannot have more than one global running \
                                 condition") ;
                params, Some e, globals, funcs
            | DefGlobals lst ->
                params, run_cond_opt, List.rev_append lst globals, funcs
          ) ([], None, [], []) defs in
        let run_cond = run_cond_opt |? default_run_cond in
        let warnings : Raql_warning.DessserGen.t list = [] in
        RamenTuple.params_sort params, run_cond, globals, funcs, warnings
    ) m

  (*$inject
   let test_prog str =
     (match test_p p str with
     | Ok (res, rem) ->
        BatPervasives.Ok (checked res, rem)
     | x -> x) |>
     TestHelpers.test_printer print
  *)
  (*$= test_prog & ~printer:BatPervasives.identity
   "DEFINE 'bar' AS FROM 'foo' SELECT 42 AS 'the_answer';" \
      (test_prog "DEFINE bar AS SELECT 42 AS the_answer FROM foo")

   "PARAMETER p1 U32 DEFAULTS TO 0;\n\\
    PARAMETER p2 U32 DEFAULTS TO 0;\n\\
    DEFINE 'add' AS SELECT (param.'p1') + (param.'p2') AS 'res';" \
      (test_prog "PARAMETERS p1 DEFAULTS TO 0 AND p2 DEFAULTS TO 0; \
                  DEFINE add AS YIELD p1 + p2 AS res")
  *)

  (*$>*)
end

let check_global g =
  (* For now we support only the most interesting case: *)
  if g.Globals.scope = Global then
    Printf.sprintf "Variable scope %s is not yet supported"
      (Globals.string_of_scope g.scope) |>
    failwith ;
  match g.typ.DT.typ with
  | DT.TMap ({ typ = TString ; _ }, { typ = TString ; _ }) ->
      ()
  | TMap _ ->
      Printf.sprintf2
        "Maps of type other than string[string] are not supported yet" |>
        failwith
  | _ ->
      Printf.sprintf2 "Variable type %a is not yet supported"
        DT.print_mn g.typ |>
      failwith

let check_globals params globals =
  List.iter check_global globals ;
  (* Check name uniqueness: *)
  let s =
    List.fold_left (fun s g ->
      Set.String.add (g.Globals.name : N.field :> string) s
    ) Set.String.empty globals in
  let s =
    List.fold_left (fun s p ->
      Set.String.add (p.PParam.name :> string) s
    ) s params in
  if Set.String.cardinal s <> List.length globals + List.length params then
    failwith "Global variable names must be unique"

(* For convenience, it is allowed to select from a sub-query instead of from a
 * named function. Here those sub-queries are turned into real functions
 * (aka reified). *)

let reify_subquery =
  let seqnum = ref 0 in
  fun op ->
    let name = N.func ("_"^ string_of_int !seqnum) in
    incr seqnum ;
    make_func ~name op

(* Returns a list of additional funcs and the list of parents that
 * contains only NamedOperations: *)
let expurgate from =
  List.fold_left (fun (new_funcs, from) -> function
    | O.SubQuery q ->
        let new_func = reify_subquery q in
        (new_func :: new_funcs),
        O.NamedOperation (O.ThisSite, None, Option.get new_func.name) :: from
    | O.NamedOperation _ as f -> new_funcs, f :: from
  ) ([], []) from

let reify_subqueries funcs =
  List.fold_left (fun fs func ->
    match func.operation with
    | Aggregate ({ from ; _ } as f) ->
        let funcs, from = expurgate from in
        { func with operation = Aggregate { f with from } } ::
          funcs @ fs
    | _ -> func :: fs
  ) [] funcs

let name_unnamed =
  List.map (fun func ->
    if func.name <> None then func else
    { func with name = Some (make_name ()) })

exception MissingParent of N.src_path
let () =
  Printexc.register_printer (function
    | MissingParent path ->
        Some (
          Printf.sprintf2 "Cannot find parent source %a"
            N.src_path_print path)
    | _ -> None)

(* For convenience, it is possible to 'SELECT *' rather than, or in addition
 * to, a set of named fields (see [and_all_others] in RamenOperation). For
 * simplicity, we resolve this STAR into the actual list of fields here right
 * after parsing so that the next stage of compilation do not have to bother
 * with that: *)

(* Exits when we met a parent which output type is not stable: *)
let common_fields_of_from get_program start_name funcs from =
  let unknown_parent fn parents =
    Printf.sprintf2
      "While expanding STAR, cannot find parent function %a (have %a)"
      N.func_print fn
      (pretty_list_print N.func_print) parents |>
    failwith in
  List.fold_left (fun common data_source ->
    let fields =
      match data_source with
      | O.SubQuery _ ->
          (* Sub-queries have been reified already *)
          assert false
      | O.NamedOperation (_, None, fn) ->
          (match List.find (fun f -> f.name = Some fn) funcs with
          | exception Not_found ->
              unknown_parent fn (List.filter_map (fun f -> f.name) funcs)
          | par ->
              if has_star par.operation then raise Exit ;
              O.out_type_of_operation ~with_priv:false par.operation |>
              List.map (fun ft -> ft.RamenTuple.name))
      | O.NamedOperation (_, Some rel_pn, fn) ->
          let pn = N.program_of_rel_program start_name (N.rel_program rel_pn) in
          (match get_program pn with
          | exception Not_found ->
              !logger.warning "Cannot get parent program %a"
                N.program_print pn ;
              raise (MissingParent (N.src_path_of_program pn))
          | par_rc ->
              (match List.find (fun f ->
                       f.VSI.name = fn
                     ) par_rc.VSI.funcs with
              | exception Not_found ->
                  unknown_parent fn (
                    List.map (fun f -> f.VSI.name) par_rc.VSI.funcs)
              | par_func ->
                  if has_star par_func.VSI.operation then raise Exit ;
                  O.out_type_of_operation
                    ~with_priv:false par_func.VSI.operation |>
                  List.map (fun ft -> ft.RamenTuple.name)))
    in
    let fields = Set.of_list fields in
    match common with
    | None -> Some fields
    | Some common_fields ->
        Some (Set.intersect common_fields fields)
  ) None from |? Set.empty

let reify_star_fields get_program program_name funcs =
  let input_field (alias : N.field) =
    let expr =
      let n = E.of_string (alias :> string) in
      E.make (Stateless (SL2 (
        Get, n, E.make (Stateless (SL0 (Variable In)))))) in
    Raql_select_field.DessserGen.{
      expr ; alias ;
      (* Those two will be inferred later, with non-star fields
       * (See RamenTypingHelpers): *)
      doc = "" ; aggr = None } in
  let new_funcs = ref funcs in
  let ok =
    (* If a function selects STAR from a parent that also selects STAR
     * then several passes will be needed: *)
    reach_fixed_point ~max_try:100 (fun () ->
      let changed, new_funcs' =
        List.fold_left (fun (changed, prev) func ->
          match func.operation with
          | Aggregate ({ aggregate_fields ; and_all_others = Some suppr_fields ;
                         from ; _ } as op) ->
              (* Exit when we met a parent which output type is not stable: *)
              (match common_fields_of_from get_program program_name
                                           !new_funcs from with
              | exception Exit ->
                  changed, func :: prev
              | common_fields ->
                  if Set.is_empty common_fields then
                    Printf.sprintf2
                      "Parent functions %a have no fields in common"
                      (pretty_list_print (O.print_data_source false)) from |>
                    failwith ;
                  (* Note that the fields are added in reverse alphabetical
                   * order at the beginning of the selected fields. That
                   * way, they can be used in the specified fields. Still it
                   * would be better to inject them where the "*" was. This
                   * requires to keep that star as a token and get rid of
                   * the "and_all_others" field of Aggregate. FIXME. *)
                  let aggregate_fields, some_added =
                    Set.fold (fun name (lst, some_added) ->
                      if (* already present: *)
                        List.exists (fun sf ->
                          sf.Raql_select_field.DessserGen.alias = name
                        ) aggregate_fields ||
                        (* or suppressed: *)
                        List.exists ((=) name) suppr_fields
                      then lst, some_added
                      else input_field name :: lst, true (* add this field *)
                    ) common_fields (aggregate_fields, false) in
                  if not some_added then
                    (* Fail if we suppressed all fields: *)
                    Printf.sprintf2
                      "Parent functions %a have no fields in common after %a
                       have been suppressed"
                      (pretty_list_print (O.print_data_source false)) from
                      (pretty_list_print N.field_print) suppr_fields |>
                    failwith ;
                  true,
                  { func with
                    operation = Aggregate {
                      op with aggregate_fields ; and_all_others = None }
                  } :: prev)
          | _ ->
              changed, func :: prev
        ) (false, []) !new_funcs in
      new_funcs := new_funcs' ;
      changed)
  in
  if not ok then
    failwith "Cannot expand STAR selections" ;
  !new_funcs

(*
 * Friendlier version of the parser.
 * Allows for extra spaces and reports errors.
 * Also substitute real functions for sub-queries and actual field names
 * for '*' in select clauses.
 *)

let parse =
  let p = RamenParsing.string_parser ~print Parser.p in
  fun get_program program_name program ->
    let params, run_cond, globals, funcs, warnings = p program in
    check_globals params globals ;
    let funcs = name_unnamed funcs in
    let funcs = reify_subqueries funcs in
    let funcs = reify_star_fields get_program program_name funcs in
    let t = params, run_cond, globals, funcs, warnings in
    checked t
