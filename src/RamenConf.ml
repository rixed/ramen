open Batteries
open RamenLog
open Helpers
open RamenSharedTypes
open RamenSharedTypesJS
open AlerterSharedTypesJS
module Expr = RamenExpr

(* Used to type the input/output of funcs. Of course a compiled/
 * running func must have finished_typing to true and all optional
 * values set, but we keep that type even for typed funcs so that
 * the typing code, which has to use both typed and untyped funcs,
 * has to deal with only one case. We will sometime Option.get those
 * values when we know the func is typed.
 * The other tuple type, RamenTuple.typ, is used to describe tuples
 * outside of this context (for instance, when describing a CSV or other
 * serialization format). *)
(* FIXME: rename this type *)
type temp_tup_typ =
  { mutable finished_typing : bool ;
    mutable fields : (string * Expr.typ) list }

let print_temp_tup_typ_fields fmt fs =
  List.print ~first:"{" ~last:"}" ~sep:", "
    (fun fmt (name, expr_typ) ->
      Printf.fprintf fmt "%s: %a"
        name
        Expr.print_typ expr_typ) fmt fs

let print_temp_tup_typ fmt t =
  Printf.fprintf fmt "%a (%s)"
    print_temp_tup_typ_fields t.fields
    (if t.finished_typing then "finished typing" else "to be typed")

let temp_tup_typ_copy t =
  { t with fields =
      List.map (fun (name, typ) -> name, Expr.copy_typ typ) t.fields }

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
      RamenTuple.print_typ fmt user

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
      RamenScalar.string_of_typ field.typ ^
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
        Expr.make_typ ~nullable:f.nullable
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
            nullable_info = typ.Expr.nullable ;
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
      (* Within a program, funcs are identified by a name that can be optionally
       * provided automatically if its not meant to be referenced. *)
      name : string ;
      (* Parsed operation and its in/out types: *)
      operation : RamenOperation.t ;
      mutable in_type : tuple_type ;
      mutable out_type : tuple_type ;
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
      mutable pid : int option ;
      mutable last_exit : string }

  let fq_name func = func.program ^"/"^ func.name

  let signature func =
    (* We'd like to be formatting independent so that operation text can be
     * reformatted without ramen recompiling it. For this it is not OK to
     * strip redundant white spaces as some of those might be part of literal
     * string values. So we print it, trusting the printer to be exhaustive.
     * This is not enough to print the expression with types, as those do not
     * contain relevant info such as field rank. We therefore print without
     * types and encode input/output types explicitly below: *)
    "OP="^ IO.to_string RamenOperation.print func.operation ^
    "IN="^ type_signature func.in_type ^
    "OUT="^ type_signature func.out_type |>
    md5
end

let exec_of_func persist_dir func =
  persist_dir ^"/workers/bin/"
              ^ RamenVersions.codegen
              ^"/ramen_"^ func.Func.signature

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
      (* If this is for a test suite, which one: *)
      test_id : string ;
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
    (* If we are now in Edition _untype_ the funcs *)
    match status with Edition _ ->
      Hashtbl.iter (fun _ n ->
        let open Func in
        n.in_type <- UntypedTuple (make_temp_tup_typ ()) ;
        n.out_type <- UntypedTuple (make_temp_tup_typ ())
      ) program.funcs
    | _ -> ()

  let is_typed program =
    match program.status with
    | Edition _ | Compiling -> false
    | Compiled | Running | Stopping -> true

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
    | Running | Stopping ->
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

  let check_not_running ~persist_dir program =
    (* Demote the status to compiled since the workers can't be running
     * anymore. *)
    if program.status = Running then set_status program Compiled ;
    (* Further demote to edition if the binaries are not there anymore
     * (which will be the case if codegen version changed): *)
    if program.status = Compiled &&
       Hashtbl.values program.funcs |> Enum.exists (fun n ->
         not (file_exists ~has_perms:0o100 (exec_of_func persist_dir n)))
    then set_status program (Edition "") ;
    (* Also, we cannot be compiling anymore: *)
    if program.status = Compiling then set_status program (Edition "") ;
    (* Also clean the pid. Note: we must not do that in set_status as we
     * need the pid when supervising workers termination. *)
    Hashtbl.iter (fun _ f ->
      if f.Func.pid <> None then f.pid <- None
    ) program.funcs ;
    (* FIXME: also, as a precaution, delete any temporary program (maybe we
     * crashed because of it? *)
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
    persist_dir ^"/alerting/"
                ^ RamenVersions.alerting_state
                ^"/"^ Config.version ^"/state"

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
    alerts_lock : RWLock.t ; (* Protects the above alerts *)
    (* TODO: a file *)
    mutable archived_incidents : Incident.t list ;
    max_incidents_per_team : int ;
    debug : bool ;
    ramen_url : string ;
    persist_dir : string ;
    do_persist : bool ; (* false for tests *)
    max_simult_compilations : int ref ;
    max_history_archives : int ;
    use_embedded_compiler : bool ;
    bundle_dir : string }

let parse_operation operation =
  let open RamenParsing in
  let p = Lang.(opt_blanks -+ RamenOperation.Parser.p +- opt_blanks +- eof) in
  (* TODO: enable error correction *)
  let stream = stream_of_string operation in
  match p ["operation"] None Parsers.no_error_correction stream |>
        to_result with
  | Bad e ->
    let error =
      IO.to_string (print_bad_result RamenOperation.print) e in
    let open Lang in
    raise (SyntaxError (ParseError { error ; text = operation }))
  | Ok (op, _) -> (* Since we force EOF, no need to keep what's left to parse *)
    RamenOperation.check op ;
    op

let parse_program program =
  let open RamenParsing in
  let p = opt_blanks -+ RamenProgram.Parser.p +- opt_blanks +- eof in
  let stream = stream_of_string program in
  (* TODO: enable error correction *)
  match p ["program"] None Parsers.no_error_correction stream |>
        to_result with
  | Bad e ->
    let error =
      IO.to_string (print_bad_result RamenProgram.print) e in
    let open Lang in
    raise (SyntaxError (ParseError { error ; text = program }))
  | Ok (funcs, _) ->
    RamenProgram.check funcs ;
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

(* Tells if any function of [dependent] depends on any function of [dependee] *)
let depends_on dependent dependee =
  try
    Hashtbl.iter (fun _ func ->
      if List.exists (fun (parent_program, _) ->
        parent_program = dependee) func.Func.parents
      then raise Exit
    ) dependent.Program.funcs ;
    false
  with Exit ->
    true

let lwt_fold_programs programs init f =
  Hashtbl.values programs |> List.of_enum |>
  Lwt_list.fold_left_s f init

let lwt_fold_funcs programs init f =
  lwt_fold_programs programs init (fun prev program ->
    Hashtbl.values program.Program.funcs |> List.of_enum |>
    Lwt_list.fold_left_s (fun prev func -> f prev program func) prev)

let fold_programs programs init f =
  Hashtbl.fold (fun _ program prev ->
    f prev program
  ) programs init

let fold_funcs programs init f =
  fold_programs programs init (fun prev program ->
    Hashtbl.fold (fun _ func prev ->
      f prev program func
    ) program.Program.funcs prev)

let iter_funcs programs f =
  fold_funcs programs () (fun () prog func -> f prog func)

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
    RamenOperation.parents_of_operation operation |>
    List.map (fun p ->
      try program_func_of_user_string ~default_program:program_name p
      with Not_found ->
        raise (InvalidCommand ("Parent func "^ p ^" does not exist"))) in
  Func.{
    program = program_name ; name = func_name ;
    operation ; signature = "" ; parents ;
    (* Set once the whole graph is known and reset each time the graph is
     * edited: *)
    in_type = UntypedTuple (make_temp_tup_typ ()) ;
    out_type = UntypedTuple (make_temp_tup_typ ()) ;
    pid = None ; last_exit = "" }

(* Compile a program and add it to the configuration.
 * [timeout]: if not zero, the program will be destroyed automatically if
 * no client ask for its output for that long
 * [test_id]: if not nul, this program is just meant to be tested, so its
 * program must be renamed and rewritten, and then flagged as a test
 * program. *)
let make_program ?(test_id="") ?(timeout=0.) programs name program funcs_lst =
  assert (String.length name > 0) ;
  let now = Unix.gettimeofday () in
  let funcs = Hashtbl.create (List.length funcs_lst) in
  List.iter (fun def ->
    let func_name = def.RamenProgram.name in
    if Hashtbl.mem funcs func_name then
      raise (InvalidCommand (
         "Function "^ func_name ^" already exists in program "^ name)) ;
    make_func name func_name def.RamenProgram.operation |>
    Hashtbl.add funcs def.name
  ) funcs_lst ;
  let p = Program.{
      name ; funcs ; program ; timeout ; last_used = now ;
      status = Edition "" ; last_status_change = now ;
      last_started = None ; last_stopped = None ; test_id } in
  Hashtbl.add programs name p ;
  p

(* Cannot be in Helpers since it depends on PPP and CodeGen depends on
 * Helpers: *)
let ppp_of_file fname ppp =
  let openflags = [ Open_rdonly; Open_text ] in
  match Pervasives.open_in_gen openflags 0o644 fname with
  | exception e ->
      !logger.warning "Cannot open %S for reading: %s" fname (Printexc.to_string e) ;
      raise e
  | ic ->
      finally
        (fun () -> Pervasives.close_in ic)
        (PPP.of_in_channel_exc ppp) ic

let ppp_to_file fname ppp v =
  let openflags = [ Open_wronly; Open_creat; Open_trunc; Open_text ] in
  match Pervasives.open_out_gen openflags 0o644 fname with
  | exception e ->
      !logger.warning "Cannot open %S for writing: %s" fname (Printexc.to_string e) ;
      raise e
  | oc ->
      finally
        (fun () -> Pervasives.close_out oc)
        (PPP.to_out_channel ppp oc) v

let save_dir_of_programs persist_dir =
  (* Later we might have several files (so that we have partial locks) *)
  persist_dir ^"/configuration/"
              ^ RamenVersions.graph_config
              ^"/"^ Config.version ^"/programs"

let save_file_of_program persist_dir p =
  save_dir_of_programs persist_dir ^"/"^ p ^"/conf"

let non_persisted_programs = ref (Hashtbl.create 11)

let load_programs conf =
  let save_dir = save_dir_of_programs conf.persist_dir in
  if conf.do_persist then
    try
      let h = Hashtbl.create 11 in
      dir_subtree_iter ~on_dir:(fun p ->
        let save_file = save_file_of_program conf.persist_dir p in
        (* Not all subdirs are a program: *)
        if file_exists ~maybe_empty:false save_file then (
          !logger.debug "Loading program from %s" save_file ;
          let prog : Program.t =
            File.with_file_in save_file Marshal.input in
          Hashtbl.add h p prog
        )
      ) save_dir ;
      h
    with
    | e ->
      !logger.error "Cannot read state from directory %s: %s. Starting anew."
        save_dir (Printexc.to_string e) ;
      Hashtbl.create 11
  else !non_persisted_programs

let save_program conf p =
  let save_file = save_file_of_program conf.persist_dir p.Program.name in
  !logger.debug "Saving program %s" save_file ;
  mkdir_all ~is_file:true save_file ;
  File.with_file_out ~mode:[`create; `trunc] save_file (fun oc ->
    Marshal.output oc p)

let save_programs conf programs =
  if conf.do_persist then
    Hashtbl.iter (fun _name p ->
      save_program conf p
    ) programs

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
              max_simult_compilations max_history_archives
              use_embedded_compiler bundle_dir max_incidents_per_team =
  { graph_lock = RWLock.make () ; alerts_lock = RWLock.make () ;
    alerts = Alerter.get_state do_persist persist_dir ;
    archived_incidents = [ (* TODO *) ] ; max_incidents_per_team ;
    do_persist ; ramen_url ; debug ; persist_dir ;
    max_simult_compilations = ref max_simult_compilations ;
    max_history_archives ; use_embedded_compiler ; bundle_dir }

(* AutoCompletion of func/field names *)

let complete_func_name programs s =
  let s = String.(lowercase (trim s)) in
  (* TODO: a better search structure for case-insensitive prefix search *)
  fold_funcs programs [] (fun lst _program func ->
      let lc_name = String.lowercase func.Func.name in
      let fq_name = Func.fq_name func in
      let lc_fq_name = String.lowercase fq_name in
      if String.(starts_with lc_fq_name s || starts_with lc_name s)
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

(* Various directory names: *)

(* Compute input ringbuf and output ringbufs given the func identifier
 * and its input type, so that if we change the operation of a func we
 * don't risk reading old ringbuf with incompatible values. *)

let in_ringbuf_name conf func =
  let sign = type_signature_hash func.Func.in_type in
  conf.persist_dir ^"/workers/ringbufs/"
                   ^ RamenVersions.ringbuf ^"/"
                   ^ Func.fq_name func ^"/"^ sign ^"/in"

let temp_in_ringbuf_name conf identifier =
  conf.persist_dir ^"/tmp/ringbufs/"
                   ^ RamenVersions.ringbuf ^"/"
                   ^ identifier ^"/in"

let exp_ringbuf_name conf func =
  let sign = type_signature_hash func.Func.out_type in
  conf.persist_dir ^"/workers/ringbufs/"
                   ^ RamenVersions.ringbuf ^"/"
                   ^ Func.fq_name func ^"/"^ sign ^"/exp"

let out_ringbuf_names_ref conf func =
  conf.persist_dir ^"/workers/out_ref/"
                   ^ RamenVersions.out_ref ^"/"
                   ^ Func.fq_name func ^"/out_ref"

let report_ringbuf conf =
  conf.persist_dir ^"/instrumentation_ringbuf/"
                   ^ RamenVersions.instrumentation_tuple ^"_"
                   ^ RamenVersions.ringbuf
                   ^"/ringbuf"

let notify_ringbuf ?(test_id = "") conf =
  conf.persist_dir ^"/notify_ringbuf/"
                   ^ RamenVersions.notify_tuple ^"_"
                   ^ RamenVersions.ringbuf
                   ^ (if test_id = "" then "" else "/"^ test_id)
                   ^"/ringbuf"
