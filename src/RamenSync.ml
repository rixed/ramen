(* Now the actual implementation of the Ramen Sync Server and Client.
 * We need a client in OCaml to synchronise the configuration in between
 * several ramen processes, and one in C++ to synchronise with a GUI
 * tool.  *)
open Batteries
open RamenSyncIntf
open RamenHelpers
open RamenLog
module N = RamenName
module O = RamenOperation
module E = RamenExpr
module T = RamenTypes
module Retention = RamenRetention
module TimeRange = RamenTimeRange
module Channel = RamenChannel
module Versions = RamenVersions
module Files = RamenFiles

(* The configuration keys are either:
 * - The services directory
 * - The per site and per function stats
 * - The disk allocations (per site and per function)
 * - The user-conf for disk storage (tot disk size and per function
 *   override) ;
 * - Binocle saved stats (also per site)
 * - The RC file
 * - The global function graph
 * - The last logs of every processes (also per site)
 * - The current replays
 * - The workers possible values for factors, for all time
 * - The out_ref files (per site and per worker)
 *
 * Also, regarding alerting:
 * - The alerting configuration
 * - The current incidents
 * - For each incident, its history
 *
 * Also, we would like in there some timeseries:
 * - The tail of the last N entries of any leaf function;
 * - Per user:
 *   - A set of stored "tails" of any user specified function in a given
 *     time range (either since/until or last N), named;
 *   - A set of dashboards associating those tails to a layout of data
 *     visualisation widgets.
 *
 * That's... a lot. Let start with a few basic things that we would like
 * to see graphically soon, such as the per site stats and allocations.
 *)
module Key =
struct
  (*$< Key *)
  module User = RamenSyncUser

  type t =
    | DevNull (* Special, nobody should be allowed to read it *)
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

  and per_site_key =
    | IsMaster
    | PerService of N.service * per_service_key
    (* FIXME: keep program and func name distinct *)
    | PerWorker of N.fq * per_worker_key

  and per_service_key =
    | Host
    | Port

  and per_worker_key =
    | RuntimeStats
    | ArchivedTimes
    | NumArcFiles
    | NumArcBytes
    | AllocedArcBytes
    (* Set by the Choreographer: *)
    | Worker
    (* Set by the supervisor: *)
    | PerInstance of string (* worker signature *) * per_instance_key
    | PerReplayer of int (* id used to count the end of retransmissions *)

  and per_instance_key =
    (* All these are set by supervisor. First 3 are RamenValues. *)
    | StateFile  (* Local file where the worker snapshot its state *)
    | OutRefFile (* Local file where to write output specifications *)
    | InputRingFiles  (* Local ringbufs where worker reads its input from *)
    | ParentOutRefs  (* Local out_ref files of each parents *)
    | Pid
    | LastKilled
    | Unstopped (* whether this worker has been signaled to CONT *)
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

  let print_per_service_key oc k =
    String.print oc (match k with
      | Host -> "host"
      | Port -> "port")

  let print_per_instance oc k =
    String.print oc (match k with
    | StateFile -> "state_file"
    | OutRefFile -> "outref"
    | InputRingFiles -> "input_ringbufs"
    | ParentOutRefs -> "parent_outrefs"
    | Pid -> "pid"
    | LastKilled -> "last_killed"
    | Unstopped -> "unstopped"
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
          Printf.sprintf2 "replayers/%d" id)

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

  let print oc = function
    | DevNull ->
        String.print oc "devnull"
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
                                  | "outref" -> OutRefFile
                                  | "input_ringbufs" -> InputRingFiles
                                  | "parent_outrefs" -> ParentOutRefs
                                  | "pid" -> Pid
                                  | "last_killed" -> LastKilled
                                  | "unstopped" -> Unstopped
                                  | "last_exit" -> LastExit
                                  | "last_exit_status" -> LastExitStatus
                                  | "successive_failures" -> SuccessiveFailures
                                  | "quarantine_until" -> QuarantineUntil))))))
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
    with Match_failure _ | Failure _ ->
      Printf.sprintf "Cannot parse key (%S)" s |>
      failwith
      [@@ocaml.warning "-8"]

  (*$= of_string & ~printer:Batteries.dump
    (PerSite (N.site "siteA", PerWorker (N.fq "prog/func", PerInstance ("123", StateFile)))) \
      (of_string "sites/siteA/workers/prog/func/instances/123/state_file")
    (PerSite (N.site "siteB", PerWorker (N.fq "prog/func", Worker))) \
      (of_string "sites/siteB/workers/prog/func/worker")
  *)
  (*$= to_string & ~printer:Batteries.identity
    "versions/codegen" \
      (to_string (Versions "codegen"))
    "sources/glop/ramen" \
      (to_string (Sources (N.src_path  "glop", "ramen")))
   *)

  (*$>*)
end

(* For now we just use globs on the key names: *)
module Selector =
struct
  module Key = Key
  type t = Globs.t
  let print = Globs.print

  type set =
    { mutable lst : (t * int) list ;
      mutable next_id : int }

  let make_set () =
    { lst = [] ; next_id = 0 }

  type id = int

  let print_id = Int.print

  let add s t =
    try List.assoc t s.lst
    with Not_found ->
      let id = s.next_id in
      s.next_id <- id + 1 ;
      s.lst <- (t, id) :: s.lst ;
      id

  let matches k s =
    let k = IO.to_string Key.print k in
    List.enum s.lst //@
    fun (t, id) ->
      if Globs.matches t k then Some id else None
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
        (* Mash together the function operation and types, program parameters
         * and some RC entries such as debug and report_period. Identifies a
         * running worker: *)
        worker_signature : string ;
        (* Mash program operation including default parameters, identifies a
         * compiled binary: *)
        bin_signature : string ;
        is_used : bool ;
        params : RamenParams.param list ;
        envvars : N.field list ; (* Actual values taken from the site host *)
        role : role ;
        parents : ref list ;
        children : ref list }

    and ref =
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
        "%s%a with report_period:%a, \
         worker_signature:%S, bin_signature:%S, \
         parents:%a, children:%a, params:%a"
        (if w.enabled then "" else "DISABLED ")
        print_role w.role
        RamenParsing.print_duration w.report_period
        w.worker_signature
        w.bin_signature
        (List.print print_ref) w.parents
        (List.print print_ref) w.children
        RamenParams.print_list w.params

    let is_top_half = function
      | TopHalf _ -> true
      | Whole -> false
  end

  module TargetConfig =
  struct
    type t = (N.program * entry) list

    and entry =
      { enabled : bool ;
        debug : bool ;
        report_period : float ;
        params : RamenParams.param list ;
        on_site : string ; (* Globs as a string for simplicity *)
        automatic : bool }

  let print_entry oc rce =
    Printf.fprintf oc
      "{ enabled=%b; debug=%b; report_period=%f; params={%a}; \
         on_site=%S; automatic=%b }"
      rce.enabled rce.debug rce.report_period
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
      { src_ext : string ; md5 : string ; detail : detail }

    and detail =
      | Compiled of compiled
      (* Maybe distinguish linking errors that can go away independently?*)
      | Failed of failed

    and compiled = RamenConf.Program.Serialized.t

    and failed =
      { err_msg : string ;
        (* If not null, try again when this other program is compiled: *)
        depends_on : N.src_path option }

    and function_info = RamenConf.Func.Serialized.t

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

    let print_compiled oc _i =
      Printf.fprintf oc "compiled (TODO)"

    let print_detail oc = function
      | Compiled i -> print_compiled oc i
      | Failed i -> print_failed oc i

    let print oc s =
      Printf.fprintf oc "SourceInfo { src_ext:%S, md5:%S, %a }"
        s.src_ext s.md5 print_detail s.detail

    let signature_of_compiled info =
      Printf.sprintf2 "%s_%a_%a_%s"
        Versions.codegen
        (E.print false) info.RamenConf.Program.Serialized.condition
        (List.print (fun oc func ->
          String.print oc func.RamenConf.Func.Serialized.signature))
          info.funcs
        (RamenTuple.params_signature info.RamenConf.Program.Serialized.default_params) |>
      N.md5

    let signature = function
      | { detail = Compiled compiled ; _ } ->
          signature_of_compiled compiled
      | _ ->
          invalid_arg "SourceInfo.signature"
  end

  module Alert =
  struct
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

    let print_v1 oc a =
      Printf.fprintf oc "Alert { %a/%a %s %f }"
        N.fq_print a.table
        N.field_print a.column
        (if a.threshold > a.recovery then ">" else "<")
        a.threshold

    let print oc = function
      | V1 a -> print_v1 oc a
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

    let print oc _s =
      Printf.fprintf oc "RuntimeStats{ TODO }"
  end

  module Replay =
  struct
    type t = RamenConf.Replays.entry

    let print oc t =
      Printf.fprintf oc "Replay { channel=%a; target=%a; sources=%a; ... }"
        Channel.print t.RamenConf.Replays.channel
        N.site_fq_print t.target
        (List.print N.site_fq_print) t.sources

    (* Simple replay requests can be written to the config tree and are turned
     * into actual replays by the choreographer. Result will always be written
     * into the config tree and will include all fields. *)
    type request =
      { target : N.site_fq ; since : float ; until : float }

    let print_request oc r =
      Printf.fprintf oc "ReplayRequest { target=%a; since=%a; until=%a }"
        N.site_fq_print r.target
        print_as_date r.since
        print_as_date r.until
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

  type t =
    | Error of float * int * string
    (* Used for instance to reference parents of a worker: *)
    | Worker of Worker.t
    | Retention of Retention.t
    | TimeRange of TimeRange.t
    | Tuple of
        { skipped : int (* How many tuples were skipped before this one *) ;
          values : bytes (* serialized, without header *) }
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

  let equal v1 v2 =
    match v1, v2 with
    (* For errors, avoid comparing timestamps as after Auth we would
     * otherwise sync it twice. *)
    | Error (_, i1, _), Error (_, i2, _) -> i1 = i2
    | v1, v2 -> v1 = v2

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
    | Tuple { skipped ; values } ->
        Printf.fprintf oc "Tuple of %d bytes (after %d skipped)"
          (Bytes.length values) skipped
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

  let err_msg i s = Error (Unix.gettimeofday (), i, s)

  let of_int v = RamenValue T.(VI64 (Int64.of_int v))
  let of_int64 v = RamenValue T.(VI64 v)
  let of_float v = RamenValue T.(VFloat v)
  let of_string v = RamenValue T.(VString v)
  let of_bool v = RamenValue T.(VBool v)
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
      (try prog, List.find (fun func ->
             func.RamenConf.Func.Serialized.name = func_name
           ) prog.funcs
      with Not_found ->
          Printf.sprintf2
            "No function named %a in program %a"
            N.func_print func_name
            N.program_print prog_name |>
          failwith)
