(* Now the actual implementation of the Ramen Sync Server and Client.
 * We need a client in OCaml to synchronise the configuration in between
 * several ramen processes, and one in C++ to synchronise with a GUI
 * tool.  *)
open Batteries
open RamenSyncIntf
open RamenHelpersNoLog
open RamenHelpers
open RamenLog
module Default = RamenConstsDefault
module N = RamenName
module O = RamenOperation
module E = RamenExpr
module T = RamenTypes
module Retention = RamenRetention
module TimeRange = RamenTimeRange
module Channel = RamenChannel
module Versions = RamenVersions
module Files = RamenFiles
module Globals = RamenGlobalVariables

module Key =
struct
  (*$< Key *)
  module User = RamenSyncUser

  type t =
    | DevNull (* Special, nobody should be allowed to read it *)
    | Time (* Approx unix timestamp on the confserver *)
    | Versions of string
    | Sources of (N.src_path * string (* extension ; FIXME: a type for file types *))
    | TargetConfig (* Where to store the desired configuration *)
    | PerSite of N.site * per_site_key
    | Storage of storage_key
    | Tails of N.site * N.fq * string * tail_key
    | Replays of Channel.t
    | Error of User.socket option
    (* A unique sink for all replay queries targeted at any worker, that only
     * the choreographer will read: *)
    | ReplayRequests
    | PerClient of User.socket * per_client_key
    | Dashboards of string * per_dash_key
    (* The following relate to alerting: *)
    | Notifications
    | Teams of N.team * per_team_key
    (* That string is a Uuidm.t but Uuidm needlessly adds/removes the dashes
     * with converting to strings *)
    | Incidents of string * per_incident_key

  and per_site_key =
    | IsMaster
    | PerService of N.service * per_service_key
    (* FIXME: keep program and func name distinct *)
    | PerWorker of N.fq * per_worker_key
    | PerProgram of string (* as in worker.info_signature *) * per_site_program_key

  and per_service_key =
    | Host
    | Port

  and per_worker_key =
    (* Set by the workers: *)
    | RuntimeStats
    (* Set by the archivist: *)
    | ArchivedTimes
    | NumArcFiles
    | NumArcBytes
    | AllocedArcBytes
    (* Set by the choreographer: *)
    | Worker
    (* Set by the supervisor: *)
    | PerInstance of string (* worker signature *) * per_instance_key
    | PerReplayer of int (* id used to count the end of retransmissions *)
    | OutputSpecs (* output specifications *)

  and per_site_program_key =
    | Executable

  and per_instance_key =
    (* All these are set by supervisor. First 3 are RamenValues. *)
    | StateFile  (* Local file where the worker snapshot its state *)
    | InputRingFile  (* Local ringbuf where worker reads its input from *)
    | Pid
    | LastKilled
    | LastExit
    | LastExitStatus
    | SuccessiveFailures
    | QuarantineUntil

  and tail_key =
    | Subscriber of string
    | LastTuple of int (* increasing sequence just for ordering *)

  and storage_key =
    | TotalSize
    | RecallCost
    | RetentionsOverride of Globs.t

  and per_client_key =
    | Response of string
    | Scratchpad of per_dash_key

  and per_dash_key =
    | Widgets of int

  and per_team_key =
    | Contacts of string
    | Inhibition of string

  and per_incident_key =
    (* The notification that started this incident: *)
    | FirstStartNotif
    (* The last notification with firing=1 (useful for timing out the
     * incident): *)
    | LastStartNotif
    (* If we received the firing=0 notification: *)
    | LastStopNotif
    (* The last notification that changed the state (firing or not) of
     * this incident. Gives the current nature of the incident
     * (firing/recovered): *)
    | LastStateChangeNotif
    (* The name of the team assigned to this incident: *)
    | Team
    | Dialogs of string (* contact name *) * per_dialog_key
    (* Log of everything that happened wrt. this incident: *)
    | Journal of float (* time *) * int (* random *)

  and per_dialog_key =
    (* Number of delivery attempts of the start or stop message. *)
    | NumDeliveryAttempts
    (* Timestamps of the first and last delivery attempt *)
    | FirstDeliveryAttempt
    | LastDeliveryAttempt
    (* Scheduling: *)
    | NextScheduled
    | NextSend
    | DeliveryStatus

  let print_per_service_key oc k =
    String.print oc (match k with
      | Host -> "host"
      | Port -> "port")

  let print_per_instance oc k =
    String.print oc (match k with
    | StateFile -> "state_file"
    | InputRingFile -> "input_ringbuf"
    | Pid -> "pid"
    | LastKilled -> "last_killed"
    | LastExit -> "last_exit"
    | LastExitStatus -> "last_exit_status"
    | SuccessiveFailures -> "successive_failures"
    | QuarantineUntil -> "quarantine_until")

  let print_per_worker_key oc k =
    String.print oc (match k with
      | RuntimeStats -> "stats/runtime"
      | ArchivedTimes -> "archives/times"
      | NumArcFiles -> "archives/num_files"
      | NumArcBytes -> "archives/current_size"
      | AllocedArcBytes -> "archives/alloc_size"
      | Worker -> "worker"
      | PerInstance (signature, per_instance_key) ->
          Printf.sprintf2 "instances/%s/%a"
            signature
            print_per_instance per_instance_key
      | PerReplayer id ->
          Printf.sprintf2 "replayers/%d" id
      | OutputSpecs -> "outputs")

  let print_per_program_key oc = function
    | Executable ->
        String.print oc "executable"

  let print_per_site_key oc = function
    | IsMaster ->
        String.print oc "is_master"
    | PerService (service, per_service_key) ->
        Printf.fprintf oc "services/%a/%a"
          N.service_print service
          print_per_service_key per_service_key
    | PerWorker (fq, per_worker_key) ->
        Printf.fprintf oc "workers/%a/%a"
          N.fq_print fq
          print_per_worker_key per_worker_key
    | PerProgram (info_sign, per_info_key) ->
        Printf.fprintf oc "programs/%s/%a"
          info_sign
          print_per_program_key per_info_key

  let print_storage_key oc = function
    | TotalSize ->
        String.print oc "total_size"
    | RecallCost ->
        String.print oc "recall_cost"
    | RetentionsOverride glob ->
        (* No need to quote the glob as it's in leaf position: *)
        Printf.fprintf oc "retention_override/%a"
          Globs.print glob

  let print_tail_key oc = function
    | Subscriber uid ->
        Printf.fprintf oc "users/%s" uid
    | LastTuple i ->
        Printf.fprintf oc "lasts/%d" i

  let print_per_dash_key oc = function
    | Widgets n ->
        Printf.fprintf oc "widgets/%d" n

  let print_per_client_key oc = function
    | Response id ->
        Printf.fprintf oc "response/%s" id
    | Scratchpad per_dash_key ->
        Printf.fprintf oc "scratchpad/%a"
          print_per_dash_key per_dash_key

  let print_per_dialog_key oc = function
    | NumDeliveryAttempts ->
        String.print oc "num_attempts"
    | FirstDeliveryAttempt ->
        String.print oc "first_attempt"
    | LastDeliveryAttempt ->
        String.print oc "last_attempt"
    | NextScheduled ->
        String.print oc "next_scheduled"
    | NextSend ->
        String.print oc "next_send"
    | DeliveryStatus ->
        String.print oc "delivery_status"

  let print_per_incident_key oc = function
    | FirstStartNotif ->
        String.print oc "first_start"
    | LastStartNotif ->
        String.print oc "last_start"
    | LastStopNotif ->
        String.print oc "last_stop"
    | LastStateChangeNotif ->
        String.print oc "last_change"
    | Team ->
        String.print oc "team"
    | Dialogs (d, per_dialog_key) ->
        Printf.fprintf oc "dialogs/%s/%a"
          d
          print_per_dialog_key per_dialog_key
    | Journal (t, d) ->
        Legacy.Printf.sprintf "journal/%h/%d" t d |>
        String.print oc

  let print_per_team_key oc = function
    | Contacts name ->
        Printf.fprintf oc "contacts/%s" name
    | Inhibition name ->
        Printf.fprintf oc "inhibitions/%s" name

  let print oc = function
    | DevNull ->
        String.print oc "devnull"
    | Time ->
        String.print oc "time"
    | Versions what ->
        Printf.fprintf oc "versions/%s"
          what
    | Sources (src_path, ext) ->
        Printf.fprintf oc "sources/%a/%s"
          N.src_path_print src_path
          ext
    | TargetConfig ->
        String.print oc "target_config"
    | PerSite (site, per_site_key) ->
        Printf.fprintf oc "sites/%a/%a"
          N.site_print site
          print_per_site_key per_site_key
    | Storage storage_key ->
        Printf.fprintf oc "storage/%a"
          print_storage_key storage_key
    | Tails (site, fq, instance, tail_key) ->
        Printf.fprintf oc "tails/%a/%a/%s/%a"
          N.site_print site
          N.fq_print fq
          instance
          print_tail_key tail_key
    | Replays chan ->
        Printf.fprintf oc "replays/%a"
          Channel.print chan
    | Error None ->
        Printf.fprintf oc "errors/global"
    | Error (Some s) ->
        Printf.fprintf oc "errors/sockets/%a" User.print_socket s
    | ReplayRequests ->
        String.print oc "replay_requests"
    | PerClient (s, per_client_key) ->
        Printf.fprintf oc "clients/%a/%a"
          User.print_socket s
          print_per_client_key per_client_key
    | Dashboards (s, per_dash_key) ->
        Printf.fprintf oc "dashboards/%s/%a"
          s
          print_per_dash_key per_dash_key
    | Notifications ->
        String.print oc "alerting/notifications"
    | Teams (n, per_team_key) ->
        Printf.fprintf oc "alerting/teams/%a/%a"
          N.team_print n
          print_per_team_key per_team_key
    | Incidents (uuid, per_incident_key) ->
        Printf.fprintf oc "alerting/incidents/%s/%a"
          uuid
          print_per_incident_key per_incident_key

  (* Special key for error reporting: *)
  let global_errs = Error None
  let user_errs user socket =
    match user with
    | User.Internal -> DevNull
    | User.Ramen _ | User.Auth _ -> Error (Some socket)
    | User.Anonymous -> DevNull

  let hash = Hashtbl.hash
  let equal = (=)

  let to_string = IO.to_string print

  let of_string =
    (* TODO: a string_split_by_char would come handy in many places. *)
    let cut s =
      try String.split ~by:"/" s
      with Not_found -> s, "" in
    let rec rcut ?(acc=[]) ?(n=2) s =
      if n <= 1 then s :: acc else
      let acc, s =
        match String.rsplit ~by:"/" s with
        | exception Not_found -> s :: acc, ""
        | a, b -> b :: acc, a in
      rcut ~acc ~n:(n - 1) s in
    fun s ->
      try
        match cut s with
        | "devnull", "" -> DevNull
        | "time", "" -> Time
        | "versions", what ->
            Versions what
        | "sources", s ->
            (match rcut s with
            | [ src_path ; ext ] ->
                Sources (N.src_path src_path, ext))
        | "target_config", "" -> TargetConfig
        | "sites", s ->
            let site, s = cut s in
            PerSite (N.site site,
              match cut s with
              | "is_master", "" ->
                  IsMaster
              | "services", s ->
                  (match cut s with
                  | service, s ->
                      PerService (N.service service,
                        match cut s with
                        | "host", "" -> Host
                        | "port", "" -> Port))
              | "workers", s ->
                  (match rcut s with
                  | [ fq ; "worker" ] ->
                      PerWorker (N.fq fq, Worker)
                  | [ fq ; "outputs" ] ->
                      PerWorker (N.fq fq, OutputSpecs)
                  | [ fq ; s ] ->
                      (match rcut fq, s with
                      | [ fq ; s1 ], s2 ->
                          try
                            PerWorker (N.fq fq,
                              match s1, s2 with
                              | "stats", "runtime" -> RuntimeStats
                              | "archives", "times" -> ArchivedTimes
                              | "archives", "num_files" -> NumArcFiles
                              | "archives", "current_size" -> NumArcBytes
                              | "archives", "alloc_size" -> AllocedArcBytes
                              | "replayers", id ->
                                  PerReplayer (int_of_string id))
                          with Match_failure _ ->
                            (match rcut fq, s1, s2 with
                            | [ fq ; "instances" ], sign, s ->
                                PerWorker (N.fq fq, PerInstance (sign,
                                  match s with
                                  | "state_file" -> StateFile
                                  | "input_ringbuf" -> InputRingFile
                                  | "pid" -> Pid
                                  | "last_killed" -> LastKilled
                                  | "last_exit" -> LastExit
                                  | "last_exit_status" -> LastExitStatus
                                  | "successive_failures" -> SuccessiveFailures
                                  | "quarantine_until" -> QuarantineUntil)))))
              | "programs", s ->
                  (match rcut s with
                  | [ info_sign ; "executable" ] ->
                      PerProgram (info_sign, Executable)))
        | "storage", s ->
            Storage (
              match cut s with
              | "total_size", "" -> TotalSize
              | "recall_cost", "" -> RecallCost
              | "retention_override", s ->
                  RetentionsOverride (Globs.compile s))
        | "tails", s ->
            (match cut s with
            | site, fq_s ->
                (match rcut ~n:4 fq_s with
                | [ fq ; instance ; "users" ; s ] ->
                    Tails (N.site site, N.fq fq, instance, Subscriber s)
                | [ fq ; instance ; "lasts" ; s ] ->
                    let i = int_of_string s in
                    Tails (N.site site, N.fq fq, instance, LastTuple i)))
        | "replays", s ->
            Replays (Channel.of_string s)
        | "errors", s ->
            Error (
              match cut s with
              | "global", "" -> None
              | "sockets", s -> Some (User.socket_of_string s))
        | "replay_requests", "" ->
            ReplayRequests
        | "clients", s ->
            (match cut s with
            | sock, resp_id ->
                (match cut resp_id with
                | "response", id ->
                    PerClient (User.socket_of_string sock, Response id)
                | "scratchpad", s ->
                    (match cut s with
                    | "widgets", n ->
                        let w = Widgets (int_of_string n) in
                        PerClient (User.socket_of_string sock, Scratchpad w))))
        | "dashboards", s ->
            (match rcut ~n:3 s with
            | [ name ; "widgets" ; n ] ->
                Dashboards (name, Widgets (int_of_string n)))
        | "alerting", s ->
            (match cut s with
            | "notifications", "" ->
                Notifications
            | "teams", s ->
                (match cut s with
                | name, s ->
                    (match cut s with
                    | "contacts", c -> Teams (N.team name, Contacts c)
                    | "inhibition", id -> Teams (N.team name, Inhibition id)))
            | "incidents", s ->
                (match cut s with
                | id, s ->
                    Incidents (id,
                      (match cut s with
                      | "first_start", "" -> FirstStartNotif
                      | "last_start", "" -> LastStartNotif
                      | "last_stop", "" -> LastStopNotif
                      | "last_change", "" -> LastStateChangeNotif
                      | "team", "" -> Team
                      | "dialogs", s ->
                          (match cut s with
                          | d, s ->
                              Dialogs (d,
                                (match s with
                                | "num_attempts" -> NumDeliveryAttempts
                                | "first_attempt" -> FirstDeliveryAttempt
                                | "last_attempt" -> LastDeliveryAttempt
                                | "next_scheduled" -> NextScheduled
                                | "next_send" -> NextSend
                                | "delivery_status" -> DeliveryStatus)))
                      | "journal", t_d ->
                          let t, d = String.split t_d ~by:"/" in
                          Journal (float_of_string t, int_of_string d)))))

    with Match_failure _ | Failure _ ->
      Printf.sprintf "Cannot parse key (%S)" s |>
      failwith
      [@@ocaml.warning "-8"]

  (*$= of_string & ~printer:Batteries.dump
    (PerSite (N.site "siteA", PerWorker (N.fq "prog/func", PerInstance ("123", StateFile)))) \
      (of_string "sites/siteA/workers/prog/func/instances/123/state_file")
    (PerSite (N.site "siteB", PerWorker (N.fq "prog/func", Worker))) \
      (of_string "sites/siteB/workers/prog/func/worker")
    (Dashboards ("test/glop", Widgets 42)) \
      (of_string "dashboards/test/glop/widgets/42")
    (Teams (N.team "test", Contacts "ctc")) \
      (of_string "alerting/teams/test/contacts/ctc")
  *)
  (*$= to_string & ~printer:Batteries.identity
    "versions/codegen" \
      (to_string (Versions "codegen"))
    "sources/glop/ramen" \
      (to_string (Sources (N.src_path  "glop", "ramen")))
    "alerting/teams/test/contacts/ctc" \
      (to_string (Teams (N.team "test", Contacts "ctc")))
   *)

  (* Returns if a user can read/write/del a key: *)
  let permissions =
    let only x = Set.singleton (User.Role.Specific x)in
    let user = Set.singleton (User.Role.User)
    and admin = Set.singleton (User.Role.Admin)
    and none = Set.empty
    and (+) = Set.union in
    fun u -> function
    (* Everyone can read/write/delete: *)
    | Sources _
    | ReplayRequests
    | Teams _ ->
        admin + user,
        admin + user,
        admin + user
    (* Nobody can delete: *)
    | DevNull
    | TargetConfig
    | Storage _ ->
        admin + user,
        admin + user,
        none
    (* Nobody can write nor delete: *)
    | Versions _ ->
        admin + user,
        none,
        none
    (* Default: reserve writes and dels to owner: *)
    | _ ->
        admin + user,
        admin + only u,
        admin + only u

  (*$>*)
end

(* For now we just use globs on the key names: *)
module Selector =
struct
  module Key = Key
  type t = Globs.t
  let print = Globs.print

  type id = string
  let print_id = String.print
  let to_id = Globs.decompile

  type prepared_key = string
  let prepare_key = Key.to_string
  let matches = Globs.matches
end

(* Unfortunately there is no association between the key and the type for
 * now. *)
module Value =
struct
  module Worker =
  struct
    type t =
      { (* From the rc_entry: *)
        enabled : bool ;
        debug : bool ;
        report_period : float ;
        cwd : N.path ; (* If not empty *)
        (* Mash together the function operation and types, program parameters
         * and some RC entries such as debug and report_period. Identifies a
         * running worker. Aka "instance". *)
        worker_signature : string ;
        (* Mash program operation including default parameters, identifies a
         * precompiled program. Notice however that the same info can be compiled
         * into different and incompatible binaries by two distinct versions of
         * the compiler: *)
        info_signature : string ;
        is_used : bool ;
        params : RamenParams.param list ;
        envvars : N.field list ; (* Actual values taken from the site host *)
        role : role ;
        parents : func_ref list ;
        children : func_ref list }

    and func_ref =
      { site : N.site ; program : N.program ; func : N.func }

    and role =
      | Whole
      (* Top half: only the filtering part of that function is run, once for
       * every local parent; output is forwarded to another site. *)
      | TopHalf of top_half_spec list
    (* FIXME: parent_num is not good enough because a parent num might change
     * when another parent is added/removed. *)

    and top_half_spec =
      (* FIXME: the workers should resolve themselves, once they become proper
       * confsync clients: *)
      { tunneld_host : N.host ; tunneld_port : int ; parent_num : int }

    let print_ref oc ref =
      Printf.fprintf oc "%a:%a/%a"
        N.site_print ref.site
        N.program_print ref.program
        N.func_print ref.func

    let print_role oc = function
      | Whole -> String.print oc "whole worker"
      | TopHalf _ -> String.print oc "top half"

    let print oc w =
      Printf.fprintf oc
        "%s%s%a with debug:%a, report_period:%a, cwd:%a, \
         worker_signature:%S, info_signature:%S, \
         parents:%a, children:%a, params:%a"
        (if w.enabled then "" else "DISABLED ")
        (if w.is_used then "" else "UNUSED ")
        print_role w.role
        Bool.print w.debug
        RamenParsing.print_duration w.report_period
        N.path_print w.cwd
        w.worker_signature
        w.info_signature
        (List.print print_ref) w.parents
        (List.print print_ref) w.children
        RamenParams.print_list w.params

    let is_top_half = function
      | TopHalf _ -> true
      | Whole -> false

    let fq_of_ref ref =
      N.fq_of_program ref.program ref.func

    let site_fq_of_ref ref =
      ref.site, fq_of_ref ref
  end

  module TargetConfig =
  struct
    type entry =
      { enabled : bool ;
        debug : bool ;
        report_period : float ;
        cwd : N.path ; (* If not empty *)
        params : RamenParams.param list ;
        on_site : string ; (* Globs as a string for simplicity *)
        automatic : bool }
      [@@ppp PPP_OCaml]

    type t = (N.program * entry) list
      [@@ppp PPP_OCaml]

    let print_entry oc rce =
      Printf.fprintf oc
        "{ enabled=%b; debug=%b; report_period=%f; cwd=%a; params={%a}; \
           on_site=%S; automatic=%b }"
        rce.enabled rce.debug rce.report_period
        N.path_print rce.cwd
        RamenParams.print_list rce.params
        rce.on_site
        rce.automatic

    let print oc rcs =
      Printf.fprintf oc "TargetConfig %a"
        (List.print (fun oc (pname, rce) ->
          Printf.fprintf oc "%a=>%a"
            N.program_print pname
            print_entry rce)) rcs
  end

  module SourceInfo =
  struct
    type t =
      (* Record the first source that was considered for building this: *)
      { src_ext : string ; md5s : string list ; detail : detail }

    and detail =
      | Compiled of compiled_program
      (* Maybe distinguish linking errors that can go away independently?*)
      | Failed of failed

    and compiled_program =
      { default_params : RamenTuple.params ;
        condition : E.t ; (* part of the program signature *)
        globals : Globals.t list ;
        funcs : compiled_func list }

    and failed =
      { err_msg : string ;
        (* If not null, try again when this other program is compiled: *)
        depends_on : N.src_path option }

    and compiled_func =
      { name : N.func ;
        retention : Retention.t option ;
        is_lazy : bool ;
        doc : string ;
        (* FIXME: mutable fields because of RamenCompiler finalize function *)
        mutable operation : O.t ;
        (* out type, factors...? store them in addition for the client, or use
         * the OCaml helper lib? Or have additional keys? Those keys are:
         * Retention, Doc, IsLazy, Factors, InType, OutType, Signature, MergeInputs.
         * Or replace the compiled info at reception by another object in RmAdmin?
         * For now just add the two that are important for RmAdmin: out_type and
         * factors. FIXME.
         * Note that fields are there ordered in user order, as expected. *)
        mutable out_record : T.t ;
        mutable factors : N.field list ;
        mutable signature : string ;
        (* Signature of the input type only (used to compute input ringbuf
         * name *)
        mutable in_signature : string }

    let compiled i =
      match i.detail with
      | Compiled _ -> true
      | _ -> false

    let compilation_error i =
      match i.detail with
      | Failed { err_msg ; _ } -> err_msg
      | _ -> invalid_arg "compilation_error"

    let print_failed oc i =
      Printf.fprintf oc "err:%S%s"
        i.err_msg
        (match i.depends_on with None -> ""
         | Some path -> " (depends_on: "^ (path :> string) ^")")

    let print_compiled_func oc i =
      N.func_print oc i.name

    let print_compiled oc i =
      Printf.fprintf oc "compiled functions %a"
        (pretty_list_print print_compiled_func) i.funcs

    let print_detail oc = function
      | Compiled i -> print_compiled oc i
      | Failed i -> print_failed oc i

    let print oc s =
      Printf.fprintf oc "SourceInfo { src_ext:%S, md5s:%a, %a }"
        s.src_ext
        (List.print String.print_quoted) s.md5s
        print_detail
        s.detail

    let signature_of_compiled info =
      Printf.sprintf2 "%s_%a_%a_%s"
        Versions.codegen
        (E.print false) info.condition
        (List.print (fun oc func ->
          String.print oc func.signature))
          info.funcs
        (RamenTuple.params_signature info.default_params) |>
      N.md5

    let fq_name prog_name f = N.fq_of_program prog_name f.name
    let fq_path prog_name f = N.path (fq_name prog_name f :> string)

    let signature = function
      | { detail = Compiled compiled ; _ } ->
          signature_of_compiled compiled
      | _ ->
          invalid_arg "SourceInfo.signature"

    let has_running_condition compiled =
      compiled.condition.E.text <> E.Const T.(VBool true)
  end

  module Alert =
  struct
    (* RamenApi.alert_info_v1 cannot be used as it depends on PPP and this
     * module must have as few dependencies as possible *)

    type t =
      | V1 of v1
      (* ... and so on *)

    and v1 =
      { table : N.fq ;
        column : N.field ;
        enabled : bool ;
        where : simple_filter list ;
        having : simple_filter list ;
        threshold : float ;
        recovery : float ;
        duration : float ;
        ratio : float ;
        time_step : float ;
        tops : N.field list ;
        carry : N.field list ;
        (* Unused, for the client purpose only *)
        id : string ;
        (* Desc to use when firing/recovering: *)
        desc_title : string ;
        desc_firing : string ;
        desc_recovery : string }

    and simple_filter =
      { lhs : N.field ;
        rhs : string ;
        op : string }

    let print_simple_filter oc f =
      Printf.fprintf oc "%a %s %s" N.field_print f.lhs f.op f.rhs

    let print_simple_filters oc fs =
      List.print ~sep:" AND " print_simple_filter oc fs

    let print_v1 oc a =
      Printf.fprintf oc "Alert { %a/%a %s %f where %a having %a }"
        N.fq_print a.table
        N.field_print a.column
        (if a.threshold > a.recovery then ">" else "<")
        a.threshold
        print_simple_filters a.where
        print_simple_filters a.having

    let print oc = function
      | V1 a -> print_v1 oc a

    let column_of_alert_source = function
      | V1 { table ; column ; _ } -> table, column
  end

  module RuntimeStats =
  struct
    open Stdint
    type t =
      { stats_time : float ;
        first_startup : float ;
        last_startup : float ;
        min_etime : float option ;
        max_etime : float option ;
        first_input : float option ;
        last_input : float option ;
        first_output : float option ;
        last_output : float option ;
        tot_in_tuples : Uint64.t ;
        tot_sel_tuples : Uint64.t ;
        tot_out_tuples : Uint64.t ;
        (* Those two measure the average size of all output fields: *)
        tot_full_bytes : Uint64.t ;
        tot_full_bytes_samples : Uint64.t ;
        cur_groups : Uint64.t ;
        tot_in_bytes : Uint64.t ;
        tot_out_bytes : Uint64.t ;
        tot_wait_in : float ;
        tot_wait_out : float ;
        tot_firing_notifs : Uint64.t ;
        tot_extinguished_notifs : Uint64.t ;
        tot_cpu : float ;
        cur_ram : Uint64.t ;
        max_ram : Uint64.t }

    let print oc s =
      Printf.fprintf oc "RuntimeStats{ time:%a, #in:%s, #out:%s, cpu:%f }"
        print_as_date s.stats_time
        (Uint64.to_string s.tot_in_tuples)
        (Uint64.to_string s.tot_out_tuples)
        s.tot_cpu
  end

  module Replay =
  struct
    type t =
      { channel : Channel.t ;
        target : N.site_fq ;
        target_fieldmask : RamenFieldMask.fieldmask ;
        since : float ;
        until : float ;
        recipient : recipient ;
        (* Sets turned into lists for easier deser in C++: *)
        sources : N.site_fq list ;
        (* We pave the whole way from all sources to the target for this
         * channel id, rather than letting the normal stream carry this
         * channel events, in order to avoid spamming unrelated nodes
         * (Cf. issue #640): *)
        links : (N.site_fq * N.site_fq) list ;
        timeout_date : float }

    and recipient =
      | RingBuf of N.path
      | SyncKey of string (* some id *)

    let print_recipient oc = function
      | RingBuf rb -> N.path_print oc rb
      | SyncKey id -> Printf.fprintf oc "resp#%s" id

    let print oc t =
      Printf.fprintf oc
        "Replay { channel=%a; target=%a; recipient=%a; sources=%a; ... }"
        Channel.print t.channel
        N.site_fq_print t.target
        print_recipient t.recipient
        (List.print N.site_fq_print) t.sources

    (* Simple replay requests can be written to the config tree and are turned
     * into actual replays by the choreographer. Result will always be written
     * into the config tree and will include all fields. *)
    type request =
      { target : N.site_fq ;
        since : float ;
        until : float ;
        (* Instead of actually starting a replay, just answer the client with
         * the computed replay in the designated key and then delete it: *)
        explain : bool ;
        (* TODO: Add the fieldmask! *)
        (* String representation of a key that should not exist yet: *)
        (* FIXME: For security, make it so that the client have to create the
         * key first, the publishing worker will just UpdateKey and then
         * DelKey. *)
        resp_key : string }

    let print_request oc r =
      Printf.fprintf oc
        "ReplayRequest { target=%a; since=%a; until=%a; resp_key=%s }"
        N.site_fq_print r.target
        print_as_date r.since
        print_as_date r.until
        r.resp_key
  end

  module Replayer =
  struct
    type t =
      { (* Aggregated from all replays. Won't change once the replayer is
         * spawned. *)
        time_range : TimeRange.t ;
        (* Actual process is spawned only a bit later: *)
        creation : float ;
        (* Set when the replayer has started and then always set.
         * Until then new channels can be added. *)
        pid : int option ;
        last_killed : float ;
        (* When the replayer actually stopped (remember pids stays set): *)
        exit_status : string option ;
        (* What running chanels are using this process.
         * The replayer can be killed/deleted when empty. *)
        channels : Channel.t Set.t }

    let print oc t =
      Printf.fprintf oc "Replayer { pid=%a; channels=%a }"
        (Option.print Int.print) t.pid
        (Set.print Channel.print) t.channels

    let make creation time_range channels =
      { time_range ; creation ; pid = None ; last_killed = 0. ;
        exit_status = None ; channels }
  end

  module OutputSpecs =
  struct
    type recipient =
      | DirectFile of N.path
      | IndirectFile of string
      | SyncKey of string (* Some identifier for the client request *)
      (* TODO: InputRingfileKey of string pointing at the children key storing
       * the input ringbuf name, that the parent could then monitor to know when
       * to stop the output.
       * Ideally we would have individual entries per output, specifying the
       * fieldmask, type of output, timeout etc *)

    let recipient_print oc = function
      | DirectFile p -> N.path_print oc p
      | IndirectFile k -> Printf.fprintf oc "File from %s" k
      | SyncKey k -> Printf.fprintf oc "Key %s" k

    type file_type =
      | RingBuf
      | Orc of { with_index : bool ; batch_size : int ; num_batches : int }

    type file_spec =
      { file_type : file_type ;
        fieldmask : RamenFieldMask.fieldmask ;
        (* per channel timeouts (0 = no timeout), number of sources (<0 for
         * endless channel), pid of the readers (or 0 if it does not depend on
         * a live reader or if the reader is not known yet) : *)
        mutable channels : (Channel.t, float * int * int) Hashtbl.t }

    let string_of_file_type = function
      | RingBuf -> "ring-buffer"
      | Orc _ -> "orc-file"

    let file_spec_print oc s =
      let chan_print oc (timeout, num_sources, pid) =
        Printf.fprintf oc "{ timeout=%a; %t; %t }"
          print_as_date timeout
          (fun oc ->
            if num_sources >= 0 then
              Printf.fprintf oc "#sources=%d" num_sources
            else
              Printf.fprintf oc "unlimited")
          (fun oc ->
            if pid = 0 then
              String.print oc "any readers"
            else
              Printf.fprintf oc "reader=%d" pid) in
      Printf.fprintf oc "{ file_type=%s; fieldmask=%a; channels=%a }"
        (string_of_file_type s.file_type)
        RamenFieldMask.print s.fieldmask
        (Hashtbl.print Channel.print chan_print) s.channels

    let print_out_specs oc =
      Hashtbl.print recipient_print file_spec_print oc

    let eq s1 s2 =
      s1.file_type = s2.file_type &&
      RamenFieldMask.eq s1.fieldmask s2.fieldmask &&
      hashtbl_eq (=) s1.channels s2.channels

    type t = (recipient, file_spec) Hashtbl.t
  end

  module DashboardWidget =
  struct
    type chart_type = Plot (* TODO *)

    type scale = Linear | Logarithmic

    type axis =
      { left : bool ;
        force_zero : bool ;
        scale : scale }

    (* Strictly speaking there is no need for Unused (unused fields could
     * merely be omitted, as they are identified by name not position), but
     * that's a way to save configuration for fields that are not supposed
     * to be visible yet.
     * Note that fields are grouped by axis, so that it is possible to have
     * several stacks on the same chart. The only downside is that to have
     * several stacks share the same y-range then the y-range of all the
     * corresponding axis must be fixed (if that ever become limiting then
     * an axis could be made to follow the range of another)
     * So, on a given axis, all stacked fields will be stacked together,
     * all stack-centered fields will be stack-centered together, and all
     * individual fields will be represented individually. *)
    type representation = Unused | Independent | Stacked | StackCentered

    type field =
      { opacity : float ;
        color : int ;
        representation : representation ;
        column : string ;
        factors : string array ;
        axis : int }

    type source =
      { name : N.site_fq ;
        visible : bool ;
        fields : field array }

    type t =
      | Text of string (* mostly a place holder *)
      | Chart of { title : string ;
                   type_ : chart_type ;
                   axis : axis array ;
                   sources : source array }

    let print oc = function
      | Text t ->
          Printf.fprintf oc "{ title=%S }" t
      | Chart { title ; axis ; sources } ->
          Printf.fprintf oc "{ chart %S, %d sources and %d axis }"
            title
            (Array.length sources)
            (Array.length axis)
  end

  (* Alerting give rise to those configuration objects:
   * - Teams, as "alerting/teams/$name/etc" with contacts and escalation
   *   definitions (as a specific value types); Note that a team
   *   name is really nothing but a prefix to match notification names
   * - Team inhibitions, as "alerting/teams/$name/inhibitions/$name" and the
   *   value is a value of a specific type inhibition.
   * - Incidents, as "alerting/incidents/$uuid/etc", with special types for
   *   escalation and notification.
   *   UUIDs are generated so that an incident can be externally referred to,
   *   for instance for acknowledgment or logging.
   * - Incident journal as "alerting/incidents/$uuid/journal/$num", the value
   *   being a log events *)
  module Alerting =
  struct
    module Contact =
    struct
      type t =
        { via : via ;
          (* After how long shall a new message be sent if no acknowledgment
           * is received? 0 means to not wait for an ack at all. *)
          timeout : float [@ppp_default 0. ] }
        [@@ppp PPP_OCaml]
        (* Notice: this annotation does not mandate linking with PPP as long
         * as one does not reference to the generated printer. For instance,
         * RamenConfClient makes use of this but RmAdmin does not. *)

      and via =
        | Ignore
        | Exec of string
        | SysLog of string
        | Sqlite of
            { file : string ;
              insert : string ;
              create : string }
        | Kafka of
            (* For now it's way simpler to have the connection configured
             * once and for all rather than dependent of the notification
             * options, as we can keep a single connection alive.
             * Customarily, options starting with kafka_topic_option_prefix
             * ("topic.") are topic options, while others are producer options.
             * Mandatory options:
             * - metadata.broker.list
             * Interesting options:
             * - topic.message.timeout.ms *)
            { options : (string * string) list ;
              topic : string ;
              partition : int ;
              text : string }
        [@@ppp PPP_OCaml]

      let compare = compare

      let print ?abbrev oc t =
        let abbrev s =
          match abbrev with
          | None -> s
          | Some l -> RamenHelpersNoLog.abbrev l s
        in
        (match t.via with
        | Ignore ->
            String.print oc "Ignore"
        | Exec pat ->
            Printf.fprintf oc "Exec %S" (abbrev pat)
        | SysLog pat ->
            Printf.fprintf oc "Syslog %S" (abbrev pat)
        | Sqlite { file ; insert ; create } ->
            Printf.fprintf oc "Sqlite { file = %S; insert = %S; create = %S }"
              (abbrev file) (abbrev insert) (abbrev create)
        | Kafka { options ; topic ; partition ; text } ->
            Printf.fprintf oc "Kafka { options = %a; topic = %S; \
                                       partition = %d; text = %S }"
              (List.print (fun oc (n, v) ->
                Printf.fprintf oc "%s:%S" n (abbrev v))) options
              (abbrev topic)
              partition
              (abbrev text)) ;
        if t.timeout > 0. then
          Printf.fprintf oc " (repeat after %a)"
            print_as_duration t.timeout
        else
          Printf.fprintf oc " (no ack. expected)"

      let print_short oc = print ~abbrev:10 oc
      let print oc = print ?abbrev:None oc
    end

    (* Incidents are started and ended by received notifications (NOTIFY
     * keyword).
     * An incident is first assigned to a team, and thus to an escalation.
     * During an incident any number of messages can be sent to the owning
     * team oncallers via their contacts, until it's acknowledged or until
     * it recovers.
     * The first step of an escalation should be to wait (ie. timeout > 0)
     * Also, incident logs in a journal everything they do so that an history
     * can be obtained.
     * Finally, incidents can be grouped into outages, but that does not
     * chanNge the escalation process. *)
    module Notification =
    struct
      type t =
        { site : N.site ;
          worker : N.fq ;
          test : bool [@ppp_default false] ;
          sent_time : float ;
          event_time : float option [@ppp_default None] ;
          name : string ;
          firing : bool [@ppp_default true] ;
          certainty : float [@ppp_default 1.] ;
          debounce : float [@ppp_default Default.debounce_delay] ;
          (* Duration after which the incident should be automatically closed
           * as if after a notification with firing=0 (for those cases when we
           * cannot tell from the data). Known from the notification itself,
           * using field named "timeout". None if <= 0 *)
          timeout : float [@ppp_default 0.] ;
          parameters : (string * string) list [@ppp_default []] }
        [@@ppp PPP_OCaml]

      let print oc t =
        Printf.fprintf oc "notification from %a:%a, name %s, %s"
          N.site_print t.site N.fq_print t.worker t.name
          (if t.firing then "firing" else "recovered")
    end

    module DeliveryStatus =
    struct
      type t =
        | StartToBeSent  (* firing notification that is yet to be sent *)
        | StartToBeSentThenStopped (* notification that stopped before being sent *)
        | StartSent      (* firing notification that is yet to be acked *)
        | StartAcked     (* firing notification that has been acked *)
        | StopToBeSent   (* non-firing notification that is yet to be sent *)
        | StopSent       (* we do not ack stop messages so this is all over *)

      let print oc = function
        | StartToBeSent -> String.print oc "StartToBeSent"
        | StartToBeSentThenStopped -> String.print oc "StartToBeSentThenStopped"
        | StartSent -> String.print oc "StartSent"
        | StartAcked -> String.print oc "StartAcked"
        | StopToBeSent -> String.print oc "StopToBeSent"
        | StopSent -> String.print oc "StopSent"
    end

    module Log =
    struct
      (* Incident also have an associated journal, one key per line, under
       * a "journal/$time/$random" subtree. *)
      type t =
        | NewNotification of notification_outcome
        | Outcry of string (* contact name *)
        (* TODO: we'd like to know the origin of this ack. *)
        | Ack of string (* contact name *)
        | Stop of stop_source
        | Cancel of string (* contact name *)

      and notification_outcome =
        | Duplicate | Inhibited | STFU | StartEscalation

      and stop_source =
        | Notification (* Stops all dialogs *)
        | Manual of string  (* name of user who stopped (all the dialogs) *)
        | Timeout of string (* contact name *)

      let to_string = function
        | NewNotification Duplicate -> "Received duplicate notification"
        | NewNotification Inhibited -> "Received inhibited notification"
        | NewNotification STFU -> "Received notification for silenced incident"
        | NewNotification StartEscalation -> "Notified"
        | Outcry contact -> "Sent message via "^ contact
        | Ack contact -> "Acknowledged "^ contact
        | Stop Notification -> "Notified to stop"
        | Stop (Manual reason) -> "Manual stop: "^ reason
        | Stop (Timeout contact) -> "Timed out "^ contact
        | Cancel contact -> "Cancelled "^ contact

      let print oc t =
        String.print oc (to_string t)
    end

    module Inhibition =
    struct
      type t =
        { mutable what : string ; (* any alerts starting with this prefix *)
          mutable start_date : float ; (* when occuring in this time range *)
          mutable stop_date : float ;
          (* Who created this inhibition. Not necessarily a user, can be a soft. *)
          who : string ;
          mutable why : string }

      let print oc t =
        Printf.fprintf oc
          "inhibit %S from %a to %a because %S (created by %S)"
          t.what
          print_as_date t.start_date
          print_as_date t.stop_date
          t.why t.who
    end
  end

  type t =
    (* report errors timestamp * seqnum * err_msg *)
    | Error of float * int * string
    (* Used for instance to reference parents of a worker: *)
    | Worker of Worker.t
    | Retention of Retention.t
    | TimeRange of TimeRange.t
    | Tuples of tuple array
    | RamenValue of T.value
    | TargetConfig of TargetConfig.t
    (* Holds all info from the compilation of a source ; what we used to have in the
     * executable binary itself. *)
    | SourceInfo of SourceInfo.t
    | RuntimeStats of RuntimeStats.t
    | Replay of Replay.t
    | Replayer of Replayer.t
    | Alert of Alert.t
    | ReplayRequest of Replay.request
    | OutputSpecs of OutputSpecs.t
    | DashboardWidget of DashboardWidget.t
    | AlertingContact of Alerting.Contact.t
    | Notification of Alerting.Notification.t
    | DeliveryStatus of Alerting.DeliveryStatus.t
    | IncidentLog of Alerting.Log.t
    | Inhibition of Alerting.Inhibition.t

  and tuple =
    { skipped : int (* How many tuples were skipped before this one *) ;
      values : bytes (* serialized, without header *) }

  let equal v1 v2 =
    match v1, v2 with
    (* For errors, avoid comparing timestamps as after Auth we would
     * otherwise sync it twice. *)
    | Error (_, i1, _), Error (_, i2, _) ->
        i1 = i2
    | OutputSpecs h1, OutputSpecs h2 ->
        hashtbl_eq OutputSpecs.eq h1 h2
    | v1, v2 ->
        v1 = v2

  let dummy = RamenValue T.VNull

  let rec print oc = function
    | Error (t, i, s) ->
        Printf.fprintf oc "%a:%d:%s"
          print_as_date t i s
    | Worker w ->
        Worker.print oc w
    | Retention r ->
        Retention.print oc r
    | TimeRange r ->
        TimeRange.print oc r
    | Tuples tuples ->
        Printf.fprintf oc "Batch of %d tuples" (Array.length tuples)
    | RamenValue v ->
        T.print oc v
    | TargetConfig rc ->
        TargetConfig.print oc rc
    | SourceInfo i ->
        SourceInfo.print oc i
    | RuntimeStats s ->
        RuntimeStats.print oc s
    | Replay r ->
        Replay.print oc r
    | ReplayRequest r ->
        Replay.print_request oc r
    | Replayer r ->
        Replayer.print oc r
    | Alert a ->
        Alert.print oc a
    | OutputSpecs h ->
        OutputSpecs.print_out_specs oc h
    | DashboardWidget c ->
        DashboardWidget.print oc c
    | AlertingContact c ->
        Alerting.Contact.print oc c
    | Notification n ->
        Alerting.Notification.print oc n
    | IncidentLog l ->
        Alerting.Log.print oc l
    | DeliveryStatus s ->
        Alerting.DeliveryStatus.print oc s
    | Inhibition i ->
        Alerting.Inhibition.print oc i

  let err_msg i s = Error (Unix.gettimeofday (), i, s)

  let of_int v = RamenValue T.(VI64 (Int64.of_int v))
  let of_int64 v = RamenValue T.(VI64 v)
  let of_float v = RamenValue T.(VFloat v)
  let of_string v = RamenValue T.(VString v)
  let of_bool v = RamenValue T.(VBool v)

  let to_int = function
    | RamenValue n -> T.int_of_scalar n
    | _ -> None
end

(*
 * Helpers
 *)

module Client = RamenSyncClient.Make (Value) (Selector)

let err_sync_type k v what =
  !logger.error "%a should be %s not %a"
    Key.print k
    what
    Value.print v

let invalid_sync_type k v what =
  Printf.sprintf2 "%a should be %s not %a"
    Key.print k
    what
    Value.print v |>
  failwith

let program_of_src_path clt src_path =
  let info_key = Key.Sources (src_path, "info") in
  match (Client.find clt info_key).value with
  | exception Not_found ->
      Printf.sprintf2
        "Cannot find source %a" Key.print info_key |>
      failwith
  | Value.SourceInfo { detail = Compiled prog ; _ } ->
      prog
  | v ->
      invalid_sync_type info_key v "a compiled SourceInfo"

let function_of_fq clt fq =
  let prog_name, func_name = N.fq_parse fq in
  let src_path = N.src_path_of_program prog_name in
  match program_of_src_path clt src_path with
  | exception e ->
      Printf.sprintf2
        "Cannot find program for worker %a: %s"
        N.fq_print fq
        (Printexc.to_string e) |>
      failwith
  | prog ->
      (try prog, prog_name, List.find (fun func ->
             func.Value.SourceInfo.name = func_name
           ) prog.funcs
      with Not_found ->
          Printf.sprintf2
            "No function named %a in program %a (only %a)"
            N.func_print func_name
            N.program_print prog_name
            (pretty_list_print (fun oc func ->
              N.func_print oc func.Value.SourceInfo.name
            )) prog.funcs |>
          failwith)
