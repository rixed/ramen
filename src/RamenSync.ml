(* Now the actual implementation of the Ramen Sync Server and Client.
 * We need a client in OCaml to synchronise the configuration in between
 * several ramen processes, and one in C++ to synchronise with a GUI
 * tool.  *)
open Batteries
open Stdint

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

(*$inject
  open Stdint *)

module Key =
struct
  (*$< Key *)
  module User = RamenSyncUser

  include Sync_key.DessserGen

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
          Printf.sprintf2 "replayers/"^ Uint32.to_string id
      | OutputSpecs -> "outputs")

  let print_per_program_key oc = function
    | Executable ->
        String.print oc "executable"

  let print_per_site_key oc = function
    | IsMaster ->
        String.print oc "is_master"
    | PerService (service, per_service_key) ->
        Printf.fprintf oc "services/%s/%a"
          service
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
        Printf.fprintf oc "retention_override/%s"
          glob

  let print_tail_key oc = function
    | Subscriber uid ->
        Printf.fprintf oc "users/%s" uid
    | LastTuple i ->
        Printf.fprintf oc "lasts/%s" (Uint32.to_string i)

  let print_per_dash_key oc = function
    | Widgets n ->
        Printf.fprintf oc "widgets/%s" (Uint32.to_string n)

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
    | Ack ->
        String.print oc "ack"

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
        Legacy.Printf.sprintf "journal/%h/%s" t (Uint32.to_string d) |>
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
        Printf.fprintf oc "alerting/teams/%s/%a"
          n
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
                      PerService (service,
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
                                  PerReplayer (Uint32.of_string id))
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
                  RetentionsOverride s)
        | "tails", s ->
            (match cut s with
            | site, fq_s ->
                (match rcut ~n:4 fq_s with
                | [ fq ; instance ; "users" ; s ] ->
                    Tails (N.site site, N.fq fq, instance, Subscriber s)
                | [ fq ; instance ; "lasts" ; s ] ->
                    let i = Uint32.of_string s in
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
                        let w = Widgets (Uint32.of_string n) in
                        PerClient (User.socket_of_string sock, Scratchpad w))))
        | "dashboards", s ->
            (match rcut ~n:3 s with
            | [ name ; "widgets" ; n ] ->
                Dashboards (name, Widgets (Uint32.of_string n)))
        | "alerting", s ->
            (match cut s with
            | "notifications", "" ->
                Notifications
            | "teams", s ->
                (match cut s with
                | name, s ->
                    (match cut s with
                    | "contacts", c -> Teams (name, Contacts c)
                    | "inhibition", id -> Teams (name, Inhibition id)))
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
                                | "delivery_status" -> DeliveryStatus
                                | "ack" -> Ack)))
                      | "journal", t_d ->
                          let t, d = String.split t_d ~by:"/" in
                          Journal (float_of_string t, Uint32.of_string d)))))

    with Match_failure _ | Failure _ ->
      Printf.sprintf "Cannot parse key (%S)" s |>
      failwith
      [@@ocaml.warning "-8"]

  (*$= of_string & ~printer:Batteries.dump
    (PerSite (N.site "siteA", PerWorker (N.fq "prog/func", PerInstance ("123", StateFile)))) \
      (of_string "sites/siteA/workers/prog/func/instances/123/state_file")
    (PerSite (N.site "siteB", PerWorker (N.fq "prog/func", Worker))) \
      (of_string "sites/siteB/workers/prog/func/worker")
    (Dashboards ("test/glop", Widgets (Uint32.of_int 42))) \
      (of_string "dashboards/test/glop/widgets/42")
    (Teams ("test", Contacts "ctc")) \
      (of_string "alerting/teams/test/contacts/ctc")
  *)
  (*$= to_string & ~printer:Batteries.identity
    "versions/codegen" \
      (to_string (Versions "codegen"))
    "sources/glop/ramen" \
      (to_string (Sources (N.src_path  "glop", "ramen")))
    "alerting/teams/test/contacts/ctc" \
      (to_string (Teams ("test", Contacts "ctc")))
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
    | Storage _
    | Notifications ->
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
  let of_id id = Globs.compile id

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
    include Worker.DessserGen

    let print_ref oc ref =
      Printf.fprintf oc "%a:%a/%a"
        N.site_print ref.Func_ref.DessserGen.site
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
        (Option.print (Array.print print_ref)) w.parents
        (Array.print print_ref) w.children
        (Array.print ~first:"" ~last:"" ~sep:";" (fun oc (n, v) ->
          Printf.fprintf oc "%a=%a" N.field_print n T.print v))
          w.params

    let is_top_half = function
      | TopHalf _ -> true
      | Whole -> false

    let fq_of_ref ref =
      N.fq_of_program ref.Func_ref.DessserGen.program ref.func

    let site_fq_of_ref ref =
      ref.Func_ref.DessserGen.site,
      fq_of_ref ref
  end

  module TargetConfig =
  struct
    include Target_config.DessserGen

    let print_run_param oc p =
      Printf.fprintf oc "%a=%a"
        N.field_print p.Program_run_parameter.DessserGen.name
        T.print p.value

    let print_run_params oc params =
      Array.print ~first:"" ~last:"" ~sep:";" print_run_param oc params

    let print_entry oc rce =
      Printf.fprintf oc
        "{ enabled=%b; debug=%b; report_period=%f; cwd=%a; params={%a}; \
           on_site=%S; automatic=%b }"
        rce.enabled rce.debug rce.report_period
        N.path_print rce.cwd
        print_run_params rce.params
        rce.on_site
        rce.automatic

    let print oc rcs =
      Printf.fprintf oc "TargetConfig %a"
        (Array.print (fun oc (pname, rce) ->
          Printf.fprintf oc "%a=>%a"
            N.program_print pname
            print_entry rce)) rcs
  end

  module SourceInfo =
  struct
    include Source_info.DessserGen

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
        (match i.depends_on with
        | None -> ""
        | Some s -> " (depends_on: "^ (s :> string) ^")")

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
      compiled.condition.E.text <> E.(Stateless (SL0 (Const T.(VBool true))))
  end

  module Alert =
  struct
    include Alert.DessserGen
    module SimpleFilter = Simple_filter.DessserGen
    type simple_filter = SimpleFilter.t

    let to_string a =
      dessser_to_string sersize_of_json to_json a

    let of_string s =
      dessser_of_string of_json s

    let print_simple_filter oc f =
      Printf.fprintf oc "%a %s %s"
        N.field_print f.SimpleFilter.lhs
        f.op f.rhs

    let print_simple_filters oc fs =
      List.print ~sep:" AND " print_simple_filter oc fs

    let print_distance oc = function
      | Absolute v -> Printf.fprintf oc "%f +" v
      | Relative v -> Printf.fprintf oc "%f%% of" v

    let print_threshold oc = function
      | Constant f ->
          Float.print oc f
      | Baseline { max_distance ; _ } ->
          Printf.fprintf oc "%a baseline" print_distance max_distance

    let print oc a =
      Printf.fprintf oc "{ %a/%a %s %a where %a having %a }"
        N.fq_print a.table
        N.field_print a.column
        (if a.hysteresis <= 0. then ">" else "<")
        print_threshold a.threshold
        print_simple_filters a.where
        print_simple_filters a.having
  end

  module RuntimeStats =
  struct
    open Stdint
    include Runtime_stats.DessserGen

    let print oc s =
      Printf.fprintf oc "RuntimeStats{ time:%a, #in:%s, #out:%s, cpu:%f }"
        print_as_date s.stats_time
        (Uint64.to_string s.tot_in_tuples)
        (Uint64.to_string s.tot_out_tuples)
        s.tot_cpu
  end

  let site_fq_print oc site_fq =
    let open Fq_function_name.DessserGen in
    Printf.fprintf oc "%a:%a/%a"
      N.site_print site_fq.site
      N.program_print site_fq.program
      N.func_print site_fq.function_

  module Replay =
  struct
    include Replay.DessserGen

    (* TODO: dessser should also (un)serialize from a user friendly format,
     * or maybe JSON: *)
    let print_recipient oc = function
      | RingBuf rb -> N.path_print oc rb
      | SyncKey id -> Printf.fprintf oc "resp#%s" id

    let print oc t =
      Printf.fprintf oc
        "Replay { channel=%a; target=%a; recipient=%a; sources=%a; ... }"
        Channel.print t.channel
        site_fq_print t.target
        print_recipient t.recipient
        (Array.print site_fq_print) t.sources

    (* Simple replay requests can be written to the config tree and are turned
     * into actual replays by the choreographer. Result will always be written
     * into the config tree and will include all fields. *)
    type request = Replay_request.DessserGen.t

    let print_request oc r =
      Printf.fprintf oc
        "ReplayRequest { target=%a; since=%a; until=%a; resp_key=%s }"
        site_fq_print r.Replay_request.DessserGen.target
        print_as_date r.since
        print_as_date r.until
        r.resp_key
  end

  module Replayer =
  struct
    include Replayer.DessserGen

    let print oc t =
      Printf.fprintf oc "Replayer { pid=%a; channels=%a }"
        (Option.print (fun oc n -> Uint32.to_string n |> String.print oc)) t.pid
        (Array.print Channel.print) t.channels

    let make creation time_range channels =
      { time_range ; creation ; pid = None ; last_killed = 0. ;
        exit_status = None ; channels }
  end

  module OutputSpecs =
  struct
    open Output_specs_wire.DessserGen
    include Output_specs.DessserGen

    type recipient = Output_specs_wire.DessserGen.recipient

    let recipient_print oc = function
      | DirectFile p -> N.path_print oc p
      | IndirectFile k -> Printf.fprintf oc "File from %s" k (* FIXME: obsolete? *)
      | SyncKey k -> Printf.fprintf oc "Key %s" k

    let print_filters oc filters =
      Array.print (fun oc (i, vals) ->
        Printf.fprintf oc "scalar#%s=>%a"
          (Uint16.to_string i)
          (Array.print T.(fun oc v -> print oc v)) vals
      ) oc filters

    let string_of_file_type = function
      | RingBuf -> "ring-buffer"
      | Orc _ -> "orc-file"

    let file_spec_print oc s =
      let chan_print oc (timeout, num_sources, pid) =
        Printf.fprintf oc "{ timeout=%a; %t; %t }"
          print_as_date timeout
          (fun oc ->
            let num_sources = Int16.to_int num_sources in
            if num_sources >= 0 then
              Printf.fprintf oc "#sources=%d" num_sources
            else
              Printf.fprintf oc "unlimited")
          (fun oc ->
            if pid = Uint32.zero then
              String.print oc "any readers"
            else
              Printf.fprintf oc "reader=%s" (Uint32.to_string pid)) in
      Printf.fprintf oc "{ file_type=%s; fieldmask=%a; filters=%a; channels=%a }"
        (string_of_file_type s.file_type)
        RamenFieldMask.print s.fieldmask
        print_filters s.filters
        (Hashtbl.print Channel.print chan_print) s.channels

    let print_out_specs oc =
      Hashtbl.print recipient_print file_spec_print oc

    let file_spec_eq f1 f2 =
      f1.file_type = f2.file_type &&
      DessserMasks.eq f1.fieldmask f2.fieldmask &&
      hashtbl_eq (=) f1.channels f2.channels

    let eq s1 s2 =
      hashtbl_eq file_spec_eq s1 s2
  end

  module DashboardWidget =
  struct
    include Dashboard_widget.DessserGen

    let print oc = function
      | Text t ->
          Printf.fprintf oc "{ title=%S }" t
      | Chart { title ; chart_axis ; sources } ->
          Printf.fprintf oc "{ chart %S, %d sources and %d axis }"
            title
            (Array.length sources)
            (Array.length chart_axis)
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
      include Alerting_contact.DessserGen

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
                                       partition = %s; text = %S }"
              (List.print (fun oc (n, v) ->
                Printf.fprintf oc "%s:%S" n (abbrev v))) options
              (abbrev topic)
              (Uint16.to_string partition)
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
      include Alerting_notification.DessserGen

      let print oc t =
        Printf.fprintf oc "notification from %a:%a, name %s, %s"
          N.site_print t.site
          N.fq_print t.worker
          t.name
          (if t.firing then "firing" else "recovered")
    end

    module DeliveryStatus =
    struct
      include Alerting_delivery_status.DessserGen

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
      include Alerting_log.DessserGen

      let to_string = function
        | NewNotification Duplicate -> "Received duplicate notification"
        | NewNotification Inhibited -> "Received inhibited notification"
        | NewNotification STFU -> "Received notification for silenced incident"
        | NewNotification StartEscalation -> "Notified"
        | Outcry (contact, attempts) ->
            let attempt = Uint32.to_int attempts + 1 in
            Printf.sprintf "Sent %d%s message via %s"
              attempt (ordinal_suffix attempt) contact
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
      include Alerting_inhibition.DessserGen

      let print oc t =
        Printf.fprintf oc
          "inhibit %S from %a to %a because %S (created by %S)"
          t.what
          print_as_date t.start_date
          print_as_date t.stop_date
          t.why t.who
    end
  end

  include Sync_value.DessserGen

  let equal v1 v2 =
    match v1, v2 with
    (* For errors, avoid comparing timestamps as after Auth we would
     * otherwise sync it twice. *)
    | Error (_, i1, _), Error (_, i2, _) ->
        i1 = i2
    | OutputSpecs s1, OutputSpecs s2 ->
        OutputSpecs.eq s1 s2
    | v1, v2 ->
        v1 = v2

  let dummy = RamenValue Raql_value.VNull

  let rec print oc = function
    | Error (t, i, s) ->
        Printf.fprintf oc "%a:%s:%s"
          print_as_date t (Uint32.to_string i) s
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

  let to_string = IO.to_string print

  let err_msg seq msg = Error (Unix.gettimeofday (), seq, msg)

  let of_int v = RamenValue T.(VI64 (Int64.of_int v))
  let of_u32 v = RamenValue T.(VU32 v)
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
