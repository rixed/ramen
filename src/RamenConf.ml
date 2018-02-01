open Batteries
open RamenLog
open Helpers
open RamenSharedTypes
open RamenSharedTypesJS
open AlerterSharedTypesJS

(* Used to type the input/output of funcs. Of course a compiled/
 * running func must have finished_typing to true and all optional
 * values set, but we keep that type even for typed funcs so that
 * the typing code, which has to use both typed and untyped funcs,
 * has to deal with only one case. We will sometime Option.get those
 * values when we know the func is typed.
 * The other tuple type, Lang.Tuple.typ, is used to describe tuples
 * outside of this context (for instance, when describing a CSV or other
 * serialization format). *)
(* FIXME: rename this type *)
type temp_tup_typ =
  { mutable finished_typing : bool ;
    mutable fields : (string * Lang.Expr.typ) List.t }

let print_temp_tup_typ_fields fmt fs =
  List.print ~first:"{" ~last:"}" ~sep:", "
    (fun fmt (name, expr_typ) ->
      Printf.fprintf fmt "%s: %a"
        name
        Lang.Expr.print_typ expr_typ) fmt fs

let print_temp_tup_typ fmt t =
  Printf.fprintf fmt "%a (%s)"
    print_temp_tup_typ_fields t.fields
    (if t.finished_typing then "finished typing" else "to be typed")

let temp_tup_typ_copy t =
  { t with fields =
      List.map (fun (name, typ) -> name, Lang.Expr.copy_typ typ) t.fields }

type tuple_type = UntypedTuple of temp_tup_typ
                | TypedTuple of { user : field_typ list ;
                                  ser : field_typ list }

let tuple_is_typed = function
  | TypedTuple _ -> true
  | UntypedTuple _ -> false

let print_tuple_type fmt = function
  | UntypedTuple temp_tup_typ ->
      print_temp_tup_typ fmt temp_tup_typ
  | TypedTuple { user ; _ } ->
      Lang.Tuple.print_typ fmt user

exception BadTupleTypedness of string
let typed_tuple_type = function
  | TypedTuple { user ; ser } -> user, ser
  | UntypedTuple _ ->
      raise (BadTupleTypedness "Function should be typed by now!")

let untyped_tuple_type = function
  | TypedTuple _ ->
      raise (BadTupleTypedness "This func should not be typed!")
  | UntypedTuple temp_tup_typ -> temp_tup_typ

let tuple_ser_type = snd % typed_tuple_type
let tuple_user_type = fst % typed_tuple_type

let type_signature tuple_type =
  let ser = tuple_ser_type tuple_type in
  List.fold_left (fun s field ->
      (if s = "" then "" else s ^ "_") ^
      field.typ_name ^ ":" ^
      Lang.Scalar.string_of_typ field.typ ^
      if field.nullable then " null" else " notnull"
    ) "" ser

let type_signature_hash = md5 % type_signature

let make_temp_tup_typ () =
  { finished_typing = false ; fields = [] }

let finish_typing t =
  t.finished_typing <- true

let temp_tup_typ_of_tup_typ tup_typ =
  let t = make_temp_tup_typ () in
  List.iter (fun f ->
      let expr_typ =
        Lang.Expr.make_typ ~nullable:f.nullable
                           ~typ:f.typ f.typ_name in
      t.fields <- t.fields @ [f.typ_name, expr_typ]
    ) tup_typ ;
  finish_typing t ;
  t

let info_of_tuple_type = function
  | UntypedTuple ttt ->
      (* Is it really useful to send unfinished types? *)
      List.map (fun (name, typ) ->
          { name_info = name ;
            nullable_info = typ.Lang.Expr.nullable ;
            typ_info = typ.scalar_typ }) ttt.fields
  | TypedTuple { user ; _ } ->
      List.filter_map (fun f ->
          if is_private_field f.typ_name then None else Some (
            { name_info = f.typ_name ;
              nullable_info = Some f.nullable ;
              typ_info = Some f.typ })) user

let temp_tup_typ_of_tuple_type = function
  | UntypedTuple temp_tup_typ -> temp_tup_typ
  | TypedTuple { ser ; _ } -> temp_tup_typ_of_tup_typ ser

let tup_typ_of_temp_tup_type ttt =
  let open Lang in
  assert ttt.finished_typing ;
  List.map (fun (name, typ) ->
    { typ_name = name ;
      nullable = Option.get typ.Expr.nullable ;
      typ = Option.get typ.Expr.scalar_typ }) ttt.fields

let archive_file dir (block_start, block_stop) =
  dir ^"/"^ string_of_int block_start ^"-"^ string_of_int block_stop

module Func =
struct
  type t =
    { (* Funcs are added/removed to/from the graph in group called programs.
       * Programs can connect to funcs from any other programs and non existing
       * funcs, but there is still the notion of programs "depending" (or lying on)
       * others; we must prevent change of higher level programs to restart lower
       * level programs, while update of a low level programs can trigger the
       * recompilation of upper programs.  The idea is that there are some
       * ephemeral programs that answer specific queries on top of more fundamental
       * programs that compute generally useful data, a bit like functions calling
       * each others.
       * Of course to compile a program, all programs it depends on must have been
       * compiled. *)
      program : string ;
      (* within a program, funcs are identified by a name that can be optionally
       * provided automatically if its not meant to be referenced. *)
      name : string ;
      (* Parsed operation and its in/out types: *)
      operation : Lang.Operation.t ;
      mutable in_type : tuple_type ;
      mutable out_type : tuple_type ;
      (* If true then export tuples toward Ramen. Can be changed on demand. *)
      mutable exporting : bool ;
      (* The signature identifies the operation and therefore the binary.
       * It does not identifies a func! Only program name + func name identifies
       * a func. Indeed, it is frequent that different funcs in the graph have
       * the same signature; they perform the same operation, but with a
       * different internal state and different environment (ie. different
       * ringbufs and different parameters).
       * This field is computed as soon as the func is typed, and is otherwise
       * empty. *)
      mutable signature : string ;
      (* Parents are either in this program or a program _below_. *)
      parents : (string * string) list ;
      (* Worker info, only relevant if it is running: *)
      mutable pid : int option }

  let fq_name func = func.program ^"/"^ func.name

  let signature func =
    (* We'd like to be formatting independent so that operation text can be
     * reformatted without ramen recompiling it. For this it is not OK to
     * strip redundant white spaces as some of those might be part of literal
     * string values. So we print it, trusting the printer to be exhaustive.
     * This is not enough to print the expression with types, as those do not
     * contain relevant info such as field rank. We therefore print without
     * types and encode input/output types explicitly below: *)
    "OP="^ IO.to_string Lang.Operation.print func.operation ^
    "IN="^ type_signature func.in_type ^
    "OUT="^ type_signature func.out_type |>
    md5

  (* key is the FQN of the node *)
  let importing_threads : (string, unit Lwt.t) Hashtbl.t =
    Hashtbl.create 11
end

let exec_of_func persist_dir func =
  persist_dir ^"/workers/bin/"
              ^ RamenVersions.codegen
              ^"/ramen_worker_"^ func.Func.signature

let tmp_input_of_func persist_dir func =
  persist_dir ^"/workers/inputs/"^ Func.fq_name func ^"/"
              ^ type_signature func.Func.in_type

let upload_dir_of_func persist_dir func =
  tmp_input_of_func persist_dir func ^"/uploads"

exception InvalidCommand of string

module Program =
struct
  type t =
    { name : string ;
      mutable funcs : (string, Func.t) Hashtbl.t ;
      (* Also keep the string as defined by the client to preserve
       * formatting, comments, etc: *)
      mutable program : string ;
      (* How long can this program can stays without dependent funcs before
       * it's reclaimed. Set to 0 for no timeout. *)
      timeout : float ;
      mutable last_used : float ;
      mutable status : program_status ;
      mutable last_status_change : float ;
      mutable last_started : float option ;
      mutable last_stopped : float option }

  let print oc t =
    Printf.fprintf oc "status=%s"
      (Info.Program.string_of_status t.status)

  let set_status program status =
    !logger.info "Program %s status %s -> %s"
      program.name
      (Info.Program.string_of_status program.status)
      (Info.Program.string_of_status status) ;
    program.status <- status ;
    program.last_status_change <- Unix.gettimeofday () ;
    (* If we are not running, clean pid info *)
    if status <> Running then
      Hashtbl.iter (fun _ n -> n.Func.pid <- None) program.funcs ;
    (* If we are now in Edition _untype_ the funcs *)
    match status with Edition _ ->
      Hashtbl.iter (fun _ n ->
        let open Func in
        n.in_type <- UntypedTuple (make_temp_tup_typ ()) ;
        n.out_type <- UntypedTuple (make_temp_tup_typ ()) ;
        n.pid <- None) program.funcs
    | _ -> ()

  let is_typed program =
    match program.status with
    | Edition _ | Compiling -> false
    | Compiled | Running -> true

  (* Program edition: only when stopped *)
  let set_editable program reason =
    match program.status with
    | Edition _ ->
      (* Update the error message *)
      if reason <> "" then
        set_status program (Edition reason)
    | Compiling ->
      (* FIXME: rather discard the compilation, and change the compiler to
       * check the status in between compilations and before changing any value
       * (needs a mutex.) *)
      raise (InvalidCommand "Graph is compiling")
    | Compiled ->
      set_status program (Edition reason) ;
    | Running ->
      raise (InvalidCommand "Graph is running")

  let fold_dependencies program init f =
    (* One day we will have a lock on the configuration and we will be able to
     * mark visited funcs *)
    Hashtbl.fold (fun _func_name func (init, called) ->
      List.fold_left (fun (init, called as prev)
                          (parent_program, _parent_func) ->
        let dependency = parent_program in
        if Set.mem dependency called then prev else (
          f init dependency,
          Set.add dependency called)
      ) (init, called) func.Func.parents
    ) program.funcs (init, Set.singleton program.name) |>
    ignore

  (* Order programs according to dependency, depended upon first. *)
  let order programs =
    let rec loop ordered = function
      | [] -> List.rev ordered
      | programs ->
        let progress, ordered, later =
          List.fold_left (fun (progress, ordered, later) l ->
              try
                fold_dependencies l () (fun () dep ->
                  !logger.debug "Program %S depends on %S" l.name dep ;
                  let in_list = List.exists (fun o -> o.name = dep) in
                  if in_list programs && not (in_list ordered) then
                    raise Exit) ;
                true, l::ordered, later
              with Exit ->
                !logger.debug "Will do %S later" l.name ;
                progress, ordered, l::later
            ) (false, ordered, []) programs in
        if not progress then raise (InvalidCommand "Dependency loop") ;
        loop ordered later
    in
    loop [] programs
end

module Alerter =
struct
  (* The part of the internal state that we persist on disk: *)
  type t =
    { (* Ongoing incidents stays there until they are manually closed (with
        * comment, reason, etc...) *)
      ongoing_incidents : (int, Incident.t) Hashtbl.t ;

      (* Used to assign an id to any escalation so that we can ack them. *)
      mutable next_alert_id : int ;

      (* Same for incidents: *)
      mutable next_incident_id : int ;

      (* And inhibitions: *)
      mutable next_inhibition_id : int ;

      mutable static : StaticConf.t }

  let save_file persist_dir =
    persist_dir ^"/alerting/"^ RamenVersions.alerting_state ^"/state"

  (* TODO: write using Lwt *)
  let save_state persist_dir state =
    let fname = save_file persist_dir in
    mkdir_all ~is_file:true fname ;
    !logger.debug "Saving state in file %S." fname ;
    File.with_file_out ~mode:[`create; `trunc] fname (fun oc ->
      Marshal.output oc state)

  let get_state do_persist persist_dir =
    let get_new () =
      { ongoing_incidents = Hashtbl.create 5 ;
        next_alert_id = 0 ;
        next_incident_id = 0 ;
        next_inhibition_id = 0 ;
        static =
          { oncallers = [ OnCaller.{ name = "John Doe" ;
                                     contacts = [| Contact.Console |] } ] ;
            teams = [
              Team.{ name = "firefighters" ;
                     members = [ "John Doe" ] ;
                     escalations = [
                       { importance = 0 ;
                         steps = Escalation.[
                           { victims = [| 0 |] ; timeout = 350. } ;
                           { victims = [| 0; 1 |] ; timeout = 350. } ] } ] ;
                     inhibitions = [] } ] ;
            default_team = "firefighters" ;
            schedule =
              [ { rank = 0 ; from = 0. ; oncaller = "John Doe" } ] } }
    in
    if do_persist then (
      let fname = save_file persist_dir in
      mkdir_all ~is_file:true fname ;
      try
        File.with_file_in fname (fun ic -> Marshal.input ic)
      with Sys_error err ->
        !logger.debug "Cannot read state from file %S: %s. Starting anew."
            fname err ;
        get_new ()
      | BatInnerIO.No_more_input ->
        !logger.debug "Cannot read state from file %S: not enough input. Starting anew."
           fname ;
        get_new ()
    ) else get_new ()
end

type conf =
  { graph_lock : RWLock.t ; (* Protects the persisted programs (all at once) *)
    alerts : Alerter.t ;
    (* TODO: use the RWLock and forget about that dirty flag: *)
    alerts_lock : RWLock.t ; (* Protects the above alerts *)
    (* TODO: a file *)
    mutable archived_incidents : Incident.t list ;
    debug : bool ;
    ramen_url : string ;
    persist_dir : string ;
    do_persist : bool ; (* false for tests *)
    max_simult_compilations : int ref ;
    max_history_archives : int }

let parse_operation operation =
  let open RamenParsing in
  let p = Lang.(opt_blanks -+ Operation.Parser.p +- opt_blanks +- eof) in
  (* TODO: enable error correction *)
  let stream = stream_of_string operation in
  match p ["operation"] None Parsers.no_error_correction stream |>
        to_result with
  | Bad e ->
    let error =
      IO.to_string (print_bad_result Lang.Operation.print) e in
    let open Lang in
    raise (SyntaxError (ParseError { error ; text = operation }))
  | Ok (op, _) -> (* Since we force EOF, no need to keep what's left to parse *)
    Lang.Operation.check op ;
    op

let parse_program program =
  let open RamenParsing in
  let p = Lang.(opt_blanks -+ Program.Parser.p +- opt_blanks +- eof) in
  let stream = stream_of_string program in
  (* TODO: enable error correction *)
  match p ["program"] None Parsers.no_error_correction stream |>
        to_result with
  | Bad e ->
    let error =
      IO.to_string (print_bad_result Lang.Program.print) e in
    let open Lang in
    raise (SyntaxError (ParseError { error ; text = program }))
  | Ok (funcs, _) ->
    Lang.Program.check funcs ;
    funcs

let del_program programs program =
  let open Program in
  match Hashtbl.find programs program.name with
  | exception Not_found ->
      !logger.info "Program %s does not exist" program.name
  | program ->
      !logger.info "Deleting program %S" program.name ;
      if program.status = Running then
        raise (InvalidCommand "Program is running") ;
      Hashtbl.remove programs program.name

let fold_programs programs init f =
  Hashtbl.fold (fun _ program prev ->
    f prev program
  ) programs init

let fold_funcs programs init f =
  fold_programs programs init (fun prev program ->
    Hashtbl.fold (fun _ func prev ->
      f prev program func
    ) program.Program.funcs prev)

let program_func_of_user_string ?default_program s =
  let s = String.trim s in
  (* rsplit because we might want to have '/'s in the program name. *)
  try String.rsplit ~by:"/" s
  with Not_found ->
    match default_program with
    | Some l -> l, s
    | None ->
        !logger.error "Cannot find func %S" s ;
        raise Not_found

(* Create the func but not the links to parents (this is so we can have
 * loops) *)
(* FIXME: got bitten by the fact that func_name and program_name are 2 strings
 * so you can mix them up. Make specialized types for all those strings. *)
let make_func program_name func_name operation =
  !logger.debug "Creating func %s/%s" program_name func_name ;
  (* New lines have to be forbidden because of the out_ref ringbuf files.
   * slashes have to be forbidden because we rsplit to get program names. *)
  if func_name = "" ||
     String.fold_left (fun bad c ->
       bad || c = '\n' || c = '\r' || c = '/') false func_name then
    invalid_arg "func name" ;
  assert (func_name <> "") ;
  let parents =
    Lang.Operation.parents_of_operation operation |>
    List.map (fun p ->
      try program_func_of_user_string ~default_program:program_name p
      with Not_found ->
        raise (InvalidCommand ("Parent func "^ p ^" does not exist"))) in
  Func.{
    program = program_name ; name = func_name ;
    operation ; exporting = false ; signature = "" ; parents ;
    (* Set once the whole graph is known and reset each time the graph is
     * edited: *)
    in_type = UntypedTuple (make_temp_tup_typ ()) ;
    out_type = UntypedTuple (make_temp_tup_typ ()) ;
    pid = None }

let make_program ?(timeout=0.) programs name program =
  assert (String.length name > 0) ;
  let now = Unix.gettimeofday () in
  let p = Program.{
      name ; funcs = Hashtbl.create 17 ; program ;
      timeout ; last_used = now ;
      status = Edition "" ; last_status_change = now ;
      last_started = None ; last_stopped = None } in
  (* Since we have lost our funcs, rebuilt them: *)
  parse_program program |>
  List.iter (fun def ->
    let func_name = def.Lang.Program.name in
    if Hashtbl.mem p.funcs func_name then
      raise (InvalidCommand (
         "Function "^ func_name ^" already exists in program "^ name)) ;
    make_func name func_name def.Lang.Program.operation |>
    Hashtbl.add p.funcs def.name) ;
  Hashtbl.add programs name p ;
  p

(* What we save on disc for programs *)
type programs = (string, Program.t) Hashtbl.t

let save_file_of_programs persist_dir =
  (* Later we might have several files (so that we have partial locks) *)
  persist_dir ^"/configuration/"^ RamenVersions.graph_config ^"/conf"

let non_persisted_programs = ref (Hashtbl.create 11)

let load_programs conf : programs =
  let save_file = save_file_of_programs conf.persist_dir in
  if conf.do_persist then
    try
      !logger.debug "Loading programs from %S" save_file ;
      File.with_file_in save_file Marshal.input
    with
    | e ->
      !logger.error "Cannot read state from file %S: %s. Starting anew."
        save_file (Printexc.to_string e) ;
      Hashtbl.create 11
  else !non_persisted_programs

let save_programs conf (programs : programs) =
  if conf.do_persist then
    let save_file = save_file_of_programs conf.persist_dir in
    !logger.debug "Saving programs into %S: %a"
      save_file
      (Hashtbl.print String.print Program.print) programs ;
    mkdir_all ~is_file:true save_file ;
    File.with_file_out ~mode:[`create; `trunc] save_file (fun oc ->
      Marshal.output oc programs)

exception RetryLater of float

let with_rlock conf f =
  let open Lwt in
  let rec loop () =
    try%lwt
      RWLock.with_r_lock conf.graph_lock (fun () ->
        !logger.debug "Took graph lock (read)" ;
        let programs = load_programs conf in
        let%lwt x = f programs in
        !logger.debug "Release graph lock (read)" ;
        return x)
    with RetryLater s ->
      Lwt_unix.sleep s >>= loop
  in
  loop ()

let with_wlock conf f =
  let open Lwt in
  let rec loop () =
    try%lwt
      RWLock.with_w_lock conf.graph_lock (fun () ->
        !logger.debug "Took graph lock (write)" ;
        let programs = load_programs conf in
        let%lwt res = f programs in
        (* Save the config only if f did not fail: *)
        save_programs conf programs ;
        !logger.debug "Release graph lock (write)" ;
        return res)
    with RetryLater s ->
      Lwt_unix.sleep s >>= loop
  in
  loop ()

let find_func programs program name =
  let program = Hashtbl.find programs program in
  program, Hashtbl.find program.Program.funcs name

let make_conf do_persist ramen_url debug persist_dir
              max_simult_compilations max_history_archives =
  { graph_lock = RWLock.make () ; alerts_lock = RWLock.make () ;
    alerts = Alerter.get_state do_persist persist_dir ;
    archived_incidents = [] ;
    do_persist ; ramen_url ; debug ; persist_dir ;
    max_simult_compilations = ref max_simult_compilations ;
    max_history_archives }

(* AutoCompletion of func/field names *)

(* Autocompletion of *all* funcs; not only exporting ones since we might want
 * to graph some meta data. Also maybe we should export on demand? *)

let complete_func_name programs s only_exporting =
  let s = String.(lowercase (trim s)) in
  (* TODO: a better search structure for case-insensitive prefix search *)
  fold_funcs programs [] (fun lst _program func ->
      let lc_name = String.lowercase func.Func.name in
      let fq_name = Func.fq_name func in
      let lc_fq_name = String.lowercase fq_name in
      if String.(starts_with lc_fq_name s || starts_with lc_name s) &&
         (not only_exporting || func.Func.exporting)
      then fq_name :: lst
      else lst
    )

let complete_field_name programs name s =
  (* rsplit because we might want to have '/'s in the program name. *)
  match String.rsplit ~by:"/" (String.trim name) with
  | exception Not_found -> []
  | program_name, func_name ->
    match find_func programs program_name func_name with
    | exception Not_found -> []
    | _program, func ->
      (match func.Func.out_type with
      | UntypedTuple _ -> []
      (* Avoid private fields by looking only at ser: *)
      | TypedTuple { ser ; _ } ->
        let s = String.(lowercase (trim s)) in
        List.fold_left (fun lst field ->
            let n = field.typ_name in
            if String.starts_with (String.lowercase n) s then
              n :: lst
            else lst
          ) [] ser)
