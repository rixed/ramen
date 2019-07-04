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

(* The only capacity we need is:
 * - One per user for personal communications (err messages...)
 * - One for administrators, giving RW access to Ramen configuration and
 *   unprivileged data (whatever that means);
 * - One for normal users, giving read access to most of Ramen configuration
 *   and access to unprivileged data;
 * - Same as above, with no restriction on data.
 *)
module Capacity =
struct
  type t =
    | Nobody (* For DevNull *)
    | SingleUser of string (* Only this user *)
    (* Used by Ramen services (Note: different from internal user, which is
     * not authenticated) *)
    | Ramen
    | Admin (* Some human which job is to break things *)
    | UnrestrictedUser (* Users who can see whatever lambda users can not *)
    | Users (* Lambda users *)
    | Anybody (* No restriction whatsoever *)

  let print fmt = function
    | Nobody ->
        String.print fmt "nobody"
    | SingleUser name ->
        Printf.fprintf fmt "user:%s" name
    | Ramen -> (* Used by ramen itself *)
        String.print fmt "ramen"
    | Admin ->
        String.print fmt "admin"
    | UnrestrictedUser ->
        String.print fmt "unrestricted-users"
    | Users ->
        String.print fmt "users"
    | Anybody ->
        String.print fmt "anybody"

  let anybody = Anybody
  let nobody = Nobody

  let equal = (=)
end

module User =
struct
  module Capa = Capacity

  type socket = string (* ZMQ peer *)

  let print_socket oc s =
    String.print oc (Base64.str_encode s)

  let socket_of_string s =
    Base64.str_decode s

  type t =
    (* Internal implies no authn at all, only for when the messages do not go
     * through ZMQ: *)
    | Internal
    | Auth of { name : string ; capas : Capa.t Set.t }
    | Anonymous

  let equal = (=)

  let authenticated = function
    | Auth _ | Internal -> true
    | Anonymous -> false

  let internal = Internal

  module PubCredentials =
  struct
    (* TODO *)
    type t = string
    let print = String.print
  end

  (* FIXME: when to delete from these? *)
  let socket_to_user : (socket, t) Hashtbl.t =
    Hashtbl.create 90

  let authenticate u creds socket =
    match u with
    | Auth _ | Internal as u -> u (* ? *)
    | Anonymous ->
        let name, capas =
          match creds with
          | "internal" | "anonymous" ->
              failwith "Reserved usernames"
          | "admin" -> creds, Capa.[ Admin ]
          | c when String.starts_with c "_" -> creds, Capa.[ Ramen ]
          | "" -> failwith "Bad credentials"
          | _ -> creds, [] in
        let capas =
          Capa.Anybody :: Capa.SingleUser name :: capas |>
          Set.of_list in
        let u = Auth { name ; capas } in
        Hashtbl.replace socket_to_user socket u ;
        u

  let of_socket socket =
    try Hashtbl.find socket_to_user socket
    with Not_found -> Anonymous

  let print fmt = function
    | Internal -> String.print fmt "internal"
    | Auth { name ; _ } -> Printf.fprintf fmt "%s" name
    | Anonymous -> Printf.fprintf fmt "anonymous"

  type id = string

  let print_id = String.print

  (* Anonymous users could subscribe to some stuff... *)
  let id t = IO.to_string print t

  let has_capa c = function
    | Internal -> true
    | Auth { capas ; _ } -> Set.mem c capas
    | Anonymous -> c = Capa.anybody

  let only_me = function
    | Internal -> Capa.nobody
    | Auth { name ; _ } -> Capa.SingleUser name
    | Anonymous -> invalid_arg "only_me"
end

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
  module User = User

  type t =
    | DevNull (* Special, nobody should be allowed to read it *)
    | Sources of (N.path * string (* extension *))
    | TargetConfig (* Where to store the desired configuration *)
    | PerSite of N.site * per_site_key
    | PerProgram of (N.program * per_prog_key)
    | Storage of storage_key
    | Tails of N.site * N.fq * tail_key
    | Replays of Channel.t
    | Error of User.socket option
    (* TODO: alerting *)

  and per_site_key =
    | IsMaster
    | PerService of N.service * per_service_key
    (* FIXME: keep program and func name distinct *)
    | PerWorker of N.fq * per_worker_key

  and per_service_key =
    | Host
    | Port

  and per_prog_key =
    | Enabled (* Equivalent to MustRun *)
    | Debug
    | ReportPeriod
    | BinPath
    | SrcPath
    | Param of N.field
    | OnSite
    | Automatic
    | SourceFile
    | SourceModTime
    | RunCondition
    | PerFunction of N.func * per_func_key

  and per_worker_key =
    (* FIXME: create a single entry of type "stats" for the following: *)
    (* FIXME: The stats sum all various instances. Probably not what's wanted. *)
    | FirstStartupTime | LastStartupTime | MinETime | MaxETime
    | TotTuples | TotBytes | TotCpu | MaxRam
    | RuntimeStats
    | ArchivedTimes
    | NumArcFiles
    | NumArcBytes
    | AllocedArcBytes
    (* Set by the Choreographer: *)
    | Worker
    (* Set by the supervisor: *)
    | PerInstance of string (* func + params signature *) * per_instance_key
    | PerReplayer of int (* id used to count the end of retransmissions: *)

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

  and per_func_key =
    | Retention
    | Doc
    | IsLazy
    | Operation
    | Factors of int
    | InType
    | OutType
    | Signature
    | MergeInputs

  and tail_key =
    | Subscriber of string
    | LastTuple of int (* increasing sequence just for ordering *)

  and storage_key =
    | TotalSize
    | RecallCost
    | RetentionsOverride of Globs.t

  let print_per_service_key fmt k =
    String.print fmt (match k with
      | Host -> "host"
      | Port -> "port")

  let print_per_func_key fmt k =
    String.print fmt (match k with
      | Retention -> "retention"
      | Doc -> "doc"
      | IsLazy -> "is_lazy"
      | Operation -> "operation"
      | Factors i -> "factors/"^ string_of_int i
      | InType -> "type/in"
      | OutType -> "type/out"
      | Signature -> "signature"
      | MergeInputs -> "merge_inputs")

  let print_per_prog_key fmt k =
    String.print fmt (match k with
    | Enabled -> "enabled"
    | Debug -> "debug"
    | ReportPeriod -> "report_period"
    | BinPath -> "bin_path"
    | SrcPath -> "src_path"
    | Param s -> "param/"^ (s :> string)
    | OnSite -> "on_site"
    | Automatic -> "automatic"
    | SourceFile -> "source/file"
    | SourceModTime -> "source/mtime"
    | RunCondition -> "run_condition"
    | PerFunction (fname, per_func_key) ->
        Printf.sprintf2 "functions/%a/%a"
          N.func_print fname
          print_per_func_key per_func_key)

  let print_per_instance fmt k =
    String.print fmt (match k with
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

  let print_per_worker_key fmt k =
    String.print fmt (match k with
      | FirstStartupTime -> "startup_time/first"
      | LastStartupTime -> "startup_time/last"
      | MinETime -> "event_time/min"
      | MaxETime -> "event_time/max"
      | TotTuples -> "total/tuples"
      | TotBytes -> "total/bytes"
      | TotCpu -> "total/cpu"
      | MaxRam -> "max/ram"
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

  let print_per_site_key fmt = function
    | IsMaster ->
        String.print fmt "is_master"
    | PerService (service, per_service_key) ->
        Printf.fprintf fmt "services/%a/%a"
          N.service_print service
          print_per_service_key per_service_key
    | PerWorker (fq, per_worker_key) ->
        Printf.fprintf fmt "workers/%a/%a"
          N.fq_print fq
          print_per_worker_key per_worker_key

  let print_storage_key fmt = function
    | TotalSize ->
        String.print fmt "total_size"
    | RecallCost ->
        String.print fmt "recall_cost"
    | RetentionsOverride glob ->
        (* No need to quote the glob as it's in leaf position: *)
        Printf.fprintf fmt "retention_override/%a"
          Globs.print glob

  let print_tail_key fmt = function
    | Subscriber uid ->
        Printf.fprintf fmt "users/%s" uid
    | LastTuple i ->
        Printf.fprintf fmt "lasts/%d" i

  let print fmt = function
    | DevNull ->
        String.print fmt "devnull"
    | Sources (p, ext) ->
        Printf.fprintf fmt "sources/%a/%s"
          N.path_print p
          ext
    | TargetConfig ->
        String.print fmt "target_config"
    | PerSite (site, per_site_key) ->
        Printf.fprintf fmt "sites/%a/%a"
          N.site_print site
          print_per_site_key per_site_key
    | PerProgram (pname, per_prog_key) ->
        Printf.fprintf fmt "programs/%a/%a"
          N.program_print pname
          print_per_prog_key per_prog_key
    | Storage storage_key ->
        Printf.fprintf fmt "storage/%a"
          print_storage_key storage_key
    | Tails (site, fq, tail_key) ->
        Printf.fprintf fmt "tails/%a/%a/%a"
          N.site_print site
          N.fq_print fq
          print_tail_key tail_key
    | Replays chan ->
        Printf.fprintf fmt "replays/%a"
          Channel.print chan
    | Error None ->
        Printf.fprintf fmt "errors/global"
    | Error (Some s) ->
        Printf.fprintf fmt "errors/sockets/%a" User.print_socket s

  (* Special key for error reporting: *)
  let global_errs = Error None
  let user_errs user socket =
    match user with
    | User.Internal -> DevNull
    | User.Auth _ -> Error (Some socket)
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
        | "sources", s ->
            (match rcut s with
            | [ source ; ext ] ->
                Sources (N.path source, ext))
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
                  | [ fq ; s ] ->
                      (match rcut fq, s with
                      | [ fq ; s1 ], s2 ->
                          try
                            PerWorker (N.fq fq,
                              match s1, s2 with
                              | "startup_time", "first" -> FirstStartupTime
                              | "startup_time", "last" -> LastStartupTime
                              | "event_time", "min" -> MinETime
                              | "event_time", "max" -> MaxETime
                              | "total", "tuples" -> TotTuples
                              | "total", "bytes" -> TotBytes
                              | "total", "cpu" -> TotCpu
                              | "max", "ram" -> MaxRam
                              | "stats", "runtime" -> RuntimeStats
                              | "archives", "times" -> ArchivedTimes
                              | "archives", "num_files" -> NumArcFiles
                              | "archives", "current_size" -> NumArcBytes
                              | "archives", "alloc_size" -> AllocedArcBytes
                              | "worker", "" -> Worker)
                          with Match_failure _ ->
                            (match rcut fq, s1, s2 with
                            | [ fq ; "instance" ], sign, s ->
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
        | "programs", s ->
            (match cut s with
            | pname, s ->
              PerProgram (N.program pname,
                match cut s with
                | "enabled", "" -> Enabled
                | "debug", "" -> Debug
                | "report_period", "" -> ReportPeriod
                | "bin_path", "" -> BinPath
                | "src_path", "" -> SrcPath
                | "param", n -> Param (N.field n)
                | "on_site", "" -> OnSite
                | "automatic", "" -> Automatic
                | "source", "file" -> SourceFile
                | "source", "mtime" -> SourceModTime
                | "run_condition", "" -> RunCondition
                | "functions", s ->
                    (match cut s with
                    | fname, s ->
                      PerFunction (N.func fname,
                        match cut s with
                        | "retention", "" -> Retention
                        | "doc", "" -> Doc
                        | "is_lazy", "" -> IsLazy
                        | "operation", "" -> Operation
                        | "factors", i -> Factors (int_of_string i)
                        | "type", "in" -> InType
                        | "type", "out" -> OutType
                        | "signature", "" -> Signature
                        | "merge_inputs", "" -> MergeInputs))))
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
                (match rcut ~n:3 fq_s with
                | [ fq ; "users" ; s ] ->
                    Tails (N.site site, N.fq fq, Subscriber s)
                | [ fq ; "lasts" ; s ] ->
                    let i = int_of_string s in
                    Tails (N.site site, N.fq fq, LastTuple i)))
        | "replays", s ->
            Replays (Channel.of_string s)
        | "errors", s ->
            Error (
              match cut s with
              | "global", "" -> None
              | "sockets", s -> Some (User.socket_of_string s))
    with Match_failure _ | Failure _ ->
      Printf.sprintf "Cannot parse key (%S)" s |>
      failwith
      [@@ocaml.warning "-8"]

  (*$= of_string & ~printer:Batteries.dump
    (PerSite (N.site "siteA", PerWorker (N.fq "prog/func", TotBytes))) \
      (of_string "sites/siteA/workers/prog/func/total/bytes")
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
        src_path : N.path ; (* Without extension *)
        signature : string ; (* Mash both function and parameters *)
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
        "%a with source:%a, sign:%S, parents:%a, children:%a, params:%a"
        print_role w.role
        N.path_print w.src_path
        w.signature
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
        src_path : N.path ; (* With extension *)
        on_site : string ; (* Globs as a string for simplicity *)
        automatic : bool }

  let print_entry oc rce =
    Printf.fprintf oc
      "{ enabled=%b; debug=%b; report_period=%f; params={%a}; src_path=%a; \
         on_site=%S; automatic=%b }"
      rce.enabled rce.debug rce.report_period
      RamenParams.print_list rce.params
      N.path_print rce.src_path
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
      { md5 : string ;
        detail : detail }

    and detail =
      | Compiled of compiled
      (* Maybe distinguish linking errors that can go away independently?*)
      | Failed of failed

    and compiled = RamenConf.Program.Serialized.t

    and failed =
      { err_msg : string }

    and function_info = RamenConf.Func.Serialized.t

    let compiled i =
      match i.detail with
      | Compiled _ -> true
      | _ -> false

    let compilation_error i =
      match i.detail with
      | Failed { err_msg } -> err_msg
      | _ -> invalid_arg "compilation_error"

    let print_failed oc i =
      Printf.fprintf oc "err:%S" i.err_msg

    let print_compiled oc _i =
      Printf.fprintf oc "compiled (TODO)"

    let print_detail oc = function
      | Compiled i -> print_compiled oc i
      | Failed i -> print_failed oc i

    let print oc s =
      Printf.fprintf oc "SourceInfo { md5:%S, %a }"
        s.md5
        print_detail s.detail
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

    let site_fq_print oc (site, fq) =
      Printf.fprintf oc "%a:%a"
        N.site_print site
        N.fq_print fq

    let print oc t =
      Printf.fprintf oc "Replay { channel=%a; target=%a; sources=%a; ... }"
        Channel.print t.RamenConf.Replays.channel
        site_fq_print t.target
        (Set.print site_fq_print) t.sources
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
    | Bool of bool
    | Int of int64
    | Float of float
    | String of string
    | Error of float * int * string
    (* Used for instance to reference parents of a worker: *)
    | Worker of Worker.t
    | Retention of Retention.t
    | TimeRange of TimeRange.t
    | Tuple of
        { skipped : int (* How many tuples were skipped before this one *) ;
          values : bytes (* serialized, without header *) }
    | RamenType of T.t
    | RamenValue of T.value
    | TargetConfig of TargetConfig.t
    (* Holds all info from the compilation of a source ; what we used to have in the
     * executable binary itself. *)
    | SourceInfo of SourceInfo.t
    | RuntimeStats of RuntimeStats.t
    | Replay of Replay.t
    | Replayer of Replayer.t

  let equal v1 v2 =
    match v1, v2 with
    (* For errors, avoid comparing timestamps as after Auth we would
     * otherwise sync it twice. *)
    | Error (_, i1, _), Error (_, i2, _) -> i1 = i2
    | v1, v2 -> v1 = v2

  (* TODO: A void type? *)
  let dummy = String "undefined"

  let rec print oc = function
    | Bool b -> Bool.print oc b
    | Int i -> Int64.print oc i
    | Float f -> Float.print oc f
    | String s -> String.print oc s
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
    | RamenType t ->
        T.print_typ oc t
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
    | Replayer r ->
        Replayer.print oc r

  let err_msg i s = Error (Unix.gettimeofday (), i, s)

  let of_int v = Int (Int64.of_int v)
  let of_float v = Float v
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

let function_of_worker clt fq worker =
  match program_of_src_path clt worker.Value.Worker.src_path with
  | exception e ->
      Printf.sprintf2
        "Cannot find program for worker %a: %s"
        N.fq_print fq
        (Printexc.to_string e) |>
      failwith
  | prog ->
      let prog_name, func_name = N.fq_parse fq in
      (try prog, List.find (fun func ->
             func.RamenConf.Func.Serialized.name = func_name
           ) prog.funcs
      with Not_found ->
          Printf.sprintf2
            "No function named %a in program %a"
            N.func_print func_name
            N.program_print prog_name |>
          failwith)

let function_of_site_fq clt site fq =
  let worker_key = Key.(PerSite (site, PerWorker (fq, Worker))) in
  match (Client.find clt worker_key).value with
  | Value.Worker worker ->
      function_of_worker clt fq worker
  | v ->
      invalid_sync_type worker_key v "a Worker"
