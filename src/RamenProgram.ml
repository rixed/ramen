(* This module deals with parsing programs.
 * It makes use of RamenOperation, which parses operations
 * (ie. function bodies).
 *)
open Batteries
open RamenLang
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

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
 * function).
 * *)

type func =
  { name : RamenName.func option (* optional during parsing only *) ;
    doc : string ;
    operation : RamenOperation.t ;
    (* Condition to run that function: *)
    condition : RamenExpr.t option }

type t = RamenTuple.param list * func list

(* Anonymous functions (such as sub-queries) are given a boring name build
 * from a sequence: *)

let make_name =
  let seq = ref ~-1 in
  fun () ->
    incr seq ;
    RamenName.func_of_string ("f"^ string_of_int !seq)

let make_func ?name ?condition ?(doc="") operation =
  { name ; doc ; operation ; condition }

(* Pretty-print a parsed program back to string: *)

let print_param oc p =
  Printf.fprintf oc "PARAMETER %a DEFAULTS TO %a;"
    RamenTuple.print_field_typ p.RamenTuple.ptyp
    RamenTypes.print p.value

let print_func oc n =
  match n.name with
  | None ->
      Printf.fprintf oc "%a;"
        RamenOperation.print n.operation
  | Some name ->
      Printf.fprintf oc "DEFINE%s '%s' AS %a;"
        (match n.condition with
        | None -> ""
        | Some e -> " IF "^ IO.to_string (RamenExpr.print false) e)
        (RamenName.string_of_func name)
        RamenOperation.print n.operation

let print oc (params, funcs) =
  List.print ~first:"" ~last:"" ~sep:"\n" print_param oc params ;
  List.print ~first:"" ~last:"" ~sep:"\n" print_func oc funcs

(* Check that a syntactically valid program is actually valid: *)

let check (params, funcs) =
  let anonymous = RamenName.func_of_string "<anonymous>" in
  let name_not_unique name =
    Printf.sprintf "Name %s not unique" name |> failwith in
  List.fold_left (fun s p ->
    if Set.mem p.RamenTuple.ptyp.typ_name s then
      name_not_unique p.ptyp.typ_name ;
    Set.add p.ptyp.typ_name s
  ) Set.empty params |> ignore ;
  List.fold_left (fun s n ->
    (* Check the operation is OK: *)
    (try RamenOperation.check params n.operation
    with Failure msg ->
      let open RamenTypingHelpers in
      Printf.sprintf "In function %s: %s"
        (RamenName.func_color (n.name |? anonymous))
        msg |>
      failwith) ;
    (* Check the running condition does not use any IO tuple: *)
    Option.may (
      RamenExpr.iter (function
        | Field (_, tuple, _) ->
            Printf.sprintf "Running condition cannot use tuple %s"
              (RamenLang.string_of_prefix !tuple) |>
            failwith
        | _ -> ())
    ) n.condition ;
    (* Finally, check that the name is valid and unique: *)
    match n.name with
    | Some name ->
        let ns = RamenName.string_of_func name in
        (* Names of defined functions cannot use '#' as we use it to delimit
         * special suffixes (stats, notifs): *)
        if String.contains ns '#' then
          Printf.sprintf "Invalid dash in function name %s"
            (RamenName.func_color name) |> failwith ;
        (* Names must be unique: *)
        if Set.mem name s then name_not_unique ns ;
        Set.add name s
    | None -> s ;
  ) Set.empty funcs |> ignore

module Parser =
struct
  (*$< Parser *)
  open RamenParsing

  let anonymous_func m =
    let m = "anonymous func" :: m in
    (RamenOperation.Parser.p >>: make_func) m

  let params m =
    let m = "parameter" :: m in
    (
      strinGs "parameter" -- blanks -+
        several ~sep:list_sep_and (
          non_keyword ++
          optional ~def:None (
            blanks -+ some RamenTypes.Parser.typ) ++
          optional ~def:None (
            blanks -+ some RamenUnits.Parser.p) ++
          optional ~def:RamenTypes.VNull (
            blanks -- strinGs "default" -- blanks -- strinG "to" -- blanks -+
            (RamenTypes.Parser.(p_ ~min_int_width:0 ||| null) |||
             (duration >>: fun x -> RamenTypes.VFloat x))) ++
          optional ~def:"" quoted_string ++
          optional ~def:None (some RamenTuple.Parser.default_aggr) >>:
          fun (((((typ_name, typ_decl), units), value), doc), aggr) ->
            let typ, value =
              let open RamenTypes in
              match typ_decl with
              | None ->
                  if value = VNull then
                    let e =
                      Printf.sprintf
                        "Declaration of parameter %S must either specify \
                         the type or a non-null default value" typ_name in
                    raise (Reject e)
                  else
                    (* As usual, promote integers to 32 bits, preferably non
                     * signed, by default: *)
                    (try { structure = TU32 ; nullable = false },
                        enlarge_value TU32 value
                    with Invalid_argument _ ->
                      try { structure = TI32 ; nullable = false },
                          enlarge_value TI32 value
                      with Invalid_argument _ ->
                        { structure = structure_of value ;
                          nullable = false },
                        value)
              | Some typ ->
                  if value = VNull then
                    if typ.nullable then
                      typ, value
                    else
                      let e =
                        Printf.sprintf2
                          "Parameter %S is not nullable, therefore it must have \
                           a default value"
                          typ_name in
                      raise (Reject e)
                  else
                    (* Scale the parsed type up to the declaration: *)
                    match enlarge_value typ.structure value with
                    | exception Invalid_argument _ ->
                        let e =
                          Printf.sprintf2
                            "In declaration of parameter %S, type is \
                             incompatible with value %a"
                            typ_name print value in
                        raise (Reject e)
                    | value -> typ, value
            in
            RamenTuple.{ ptyp = { typ_name ; typ ; units ; doc ; aggr } ; value }
        )
    ) m

  let named_func m =
    let m = "function" :: m in
    (
      strinG "define" -+
      optional ~def:None (
        blanks -- strinG "if" -+ blanks -+ some RamenExpr.Parser.p) +-
      blanks ++ function_name ++
      optional ~def:"" (blanks -+ quoted_string) +-
      blanks +- strinG "as" +- blanks ++
      RamenOperation.Parser.p >>:
      fun (((condition, name), doc), op) ->
        make_func ~name ~doc ?condition op
    ) m

  let func m =
    let m = "func" :: m in
    (anonymous_func ||| named_func) m

  type definition =
    | DefFunc of func
    | DefParams of RamenTuple.params

  let p m =
    let m = "program" :: m in
    let sep = opt_blanks -- char ';' -- opt_blanks in
    (
      several ~sep ((func >>: fun f -> DefFunc f) |||
                    (params >>: fun lst -> DefParams lst)) +-
      optional ~def:() (opt_blanks -- char ';') >>: fun defs ->
        let params, funcs =
          List.fold_left (fun (params, funcs) -> function
            | DefFunc func -> params, func::funcs
            | DefParams lst -> List.rev_append lst params, funcs
          ) ([], []) defs in
        RamenTuple.params_sort params, funcs
    ) m

  (*$= p & ~printer:(test_printer print)
   (Ok (([], [\
    { name = Some (RamenName.func_of_string "bar") ;\
      doc = "" ;\
      operation = \
        Aggregate {\
          fields = [\
            { expr = RamenExpr.Const (typ, VU32 (Uint32.of_int 42)) ;\
              alias = "the_answer" ; doc = "" ; aggr = None } ] ;\
          and_all_others = false ;\
          merge = { on = [] ; timeout = 0. ; last = 1 } ;\
          sort = None ;\
          where = RamenExpr.Const (typ, VBool true) ;\
          notifications = [] ;\
          key = [] ;\
          commit_cond = RamenExpr.Const (typ, VBool true) ;\
          commit_before = false ;\
          flush_how = Reset ;\
          event_time = None ;\
          from = [NamedOperation (None, RamenName.func_of_string "foo")] ; every = 0. ; factors = [] } } ]),\
      (46, [])))\
      (test_p p "DEFINE bar AS SELECT 42 AS the_answer FROM foo" |>\
       replace_typ_in_program)

   (Ok (([ RamenTuple.{ \
             ptyp = { typ_name = "p1" ; \
                      typ = { structure = TU32 ; nullable = false } ; \
                      units = None ; doc = "" ; aggr = None } ;\
             value = VU32 Uint32.zero } ;\
           RamenTuple.{ \
             ptyp = { typ_name = "p2" ; \
                      typ = { structure = TU32 ; nullable = false } ; \
                      units = None ; doc = "" ; aggr = None } ;\
             value = VU32 Uint32.zero } ], [\
    { name = Some (RamenName.func_of_string "add") ;\
      doc = "" ;\
      operation = \
        Aggregate {\
          fields = [\
            { expr = RamenExpr.(\
                StatelessFun2 (typ, Add,\
                  Field (typ, ref TupleParam, "p1"),\
                  Field (typ, ref TupleParam, "p2"))) ;\
              alias = "res" ; doc = "" ; aggr = None } ] ;\
          every = 0. ; event_time = None ;\
          and_all_others = false ; merge = { on = [] ; timeout = 0. ; last = 1 }; sort = None ;\
          where = RamenExpr.Const (typ, VBool true) ;\
          notifications = [] ; key = [] ;\
          commit_cond = RamenExpr.Const (typ, VBool true) ;\
          commit_before = false ; flush_how = Reset ; from = [] ;\
          factors = [] } } ]),\
      (84, [])))\
      (test_p p "PARAMETERS p1 DEFAULTS TO 0 AND p2 DEFAULTS TO 0; DEFINE add AS YIELD p1 + p2 AS res" |>\
       (function Ok ((ps, fs), _) as x -> check (ps, fs) ; x | x -> x) |>\
       replace_typ_in_program)
  *)

  (*$>*)
end

(* For convenience, it is allowed to select from a sub-query instead of from a
 * named function. Here those sub-queries are turned into real functions
 * (aka reified). *)

let reify_subquery =
  let seqnum = ref 0 in
  fun op ->
    let name = RamenName.func_of_string ("_"^ string_of_int !seqnum) in
    incr seqnum ;
    make_func ~name op

(* Returns a list of additional funcs and the list of parents that
 * contains only NamedOperations and GlobPattern: *)
let expurgate from =
  let open RamenOperation in
  List.fold_left (fun (new_funcs, from) -> function
    | SubQuery q ->
        let new_func = reify_subquery q in
        (new_func :: new_funcs),
        NamedOperation (None, Option.get new_func.name) :: from
    | (GlobPattern _ | NamedOperation _) as f -> new_funcs, f :: from
  ) ([], []) from

let reify_subqueries funcs =
  let open RamenOperation in
  List.fold_left (fun fs func ->
    match func.operation with
    | Aggregate ({ from ; _ } as f) ->
        let funcs, from = expurgate from in
        { func with operation = Aggregate { f with from } } ::
          funcs @ fs
    | Instrumentation ({ from ; _ }) ->
        let funcs, from = expurgate from in
        { func with operation = Instrumentation { from } } ::
          funcs @ fs
    | Notifications ({ from ; _ }) ->
        let funcs, from = expurgate from in
        { func with operation = Notifications { from } } ::
          funcs @ fs
    | _ -> func :: fs
  ) [] funcs

let name_unnamed =
  List.map (fun func ->
    if func.name <> None then func else
    { func with name = Some (make_name ()) })

(* For convenience, it is possible to 'SELECT *' rather than, or in addition
 * to, a set of named fields (see and_all_others flag in RamenOperation). For
 * simplicity, we resolve this STAR into the actual list of fields here right
 * after parsing so that the next stage of compilation do not have to bother
 * with that: *)

(* Exits when we met a parent which output type is not stable: *)
let common_fields_of_from get_parent start_name funcs from =
  let open RamenOperation in
  List.fold_left (fun common data_source ->
    let fields =
      match data_source with
      | SubQuery _ ->
          (* Sub-queries have been reified already *)
          assert false
      | GlobPattern _ ->
          List.map (fun f -> f.RamenTuple.typ_name) RamenBinocle.tuple_typ
      | NamedOperation (None, fn) ->
          (match List.find (fun f -> f.name = Some fn) funcs with
          | exception Not_found ->
              Printf.sprintf "While expanding STAR, cannot find parent %s"
                (RamenName.string_of_func fn) |>
              failwith
          | par ->
              (match par.operation with
              | Aggregate { fields ; and_all_others ; _ } ->
                  if and_all_others then raise Exit ;
                  List.map (fun sf -> sf.alias) fields
              | ReadCSVFile { what ; _ } ->
                  List.map (fun f -> f.RamenTuple.typ_name) what.fields
              | ListenFor { proto ; _ } ->
                  RamenProtocols.tuple_typ_of_proto proto |>
                  List.map (fun f -> f.RamenTuple.typ_name)
              | Instrumentation _ ->
                  RamenBinocle.tuple_typ |>
                  List.map (fun f -> f.RamenTuple.typ_name)
              | Notifications _ ->
                  RamenNotification.tuple_typ |>
                  List.map (fun f -> f.RamenTuple.typ_name)))
      | NamedOperation (Some rel_pn, fn) ->
          let pn = RamenName.program_of_rel_program start_name rel_pn in
          let par_rc = get_parent pn in
          let par_func =
            List.find (fun f -> f.F.name = fn) par_rc.P.funcs in
          List.map (fun ft ->
            ft.RamenTuple.typ_name
          ) (RingBufLib.ser_tuple_typ_of_tuple_typ par_func.F.out_type)
    in
    let fields = Set.String.of_list fields in
    match common with
    | None -> Some fields
    | Some common_fields ->
        Some (Set.String.inter common_fields fields)
  ) None from |? Set.String.empty

let reify_star_fields get_parent program_name funcs =
  let open RamenOperation in
  let input_field alias =
    let open RamenExpr in
    { expr = (Field (make_typ alias, ref TupleIn, alias)) ;
      alias ;
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
          | Aggregate ({ fields ; and_all_others = true ; from ; _ } as op) ->
              (* Exit when we met a parent which output type is not stable: *)
              (match common_fields_of_from get_parent program_name funcs from with
              | exception Exit -> changed, func :: prev
              | common_fields ->
                  let fields =
                    Set.String.fold (fun name lst ->
                      if is_private_field name ||
                         List.exists (fun sf -> sf.alias = name) fields
                      then lst
                      else input_field name :: lst
                    ) common_fields fields in
                  true, { func with
                    operation = Aggregate {
                      op with fields ; and_all_others = false } } :: prev)
          | _ -> changed, func :: prev
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

let parse get_parent program_name program =
  let p = RamenParsing.allow_surrounding_blanks Parser.p in
  let stream = RamenParsing.stream_of_string program in
  (* TODO: enable error correction *)
  match p ["program"] None Parsers.no_error_correction stream |>
        RamenParsing.to_result with
  | Bad e ->
    Printf.sprintf2 "Parse error: %a"
      (RamenParsing.print_bad_result print) e |>
    failwith
  | Ok ((params, funcs), _) ->
    let funcs = name_unnamed funcs in
    let funcs = reify_subqueries funcs in
    let funcs = reify_star_fields get_parent program_name funcs in
    let t = params, funcs in
    check t ; t
