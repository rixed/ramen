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
          | "admin" -> creds, Capa.[ Admin ]
          | c when String.starts_with c "worker " -> creds, Capa.[ Ramen ]
          | c when String.starts_with c "ramen " -> creds, Capa.[ Ramen ]
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
    | Auth { name ; _ } -> Printf.fprintf fmt "auth:%s" name
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
    | Tail of N.site * N.fq * tail_key
    | Error of string option (* the user name *)
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
    | IsUsed   (* Supervisor might not run lazy workers that are unused *)
    (* FIXME: create a single entry of type "stats" for the following: *)
    | StartupTime | MinETime | MaxETime
    | TotTuples | TotBytes | TotCpu | MaxRam
    | ArchivedTimes
    | NumArcFiles
    | NumArcBytes
    | AllocedArcBytes
    | Parents of int
    (* TODO: add children in the FuncGraph
    | Children of int *)
    | PerInstance of string (* func signature *) * per_instance_key

  and per_instance_key =
    (* These are contributed back by the supervisor: *)
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
    | Pid -> "pid"
    | LastKilled -> "last_killed"
    | Unstopped -> "unstopped"
    | LastExit -> "last_exit"
    | LastExitStatus -> "last_exit_status"
    | SuccessiveFailures -> "successive_failures"
    | QuarantineUntil -> "quarantine_until")

  let print_per_worker_key fmt k =
    String.print fmt (match k with
      | IsUsed -> "is_used"
      | StartupTime -> "startup_time"
      | MinETime -> "event_time/min"
      | MaxETime -> "event_time/max"
      | TotTuples -> "total/tuples"
      | TotBytes -> "total/bytes"
      | TotCpu -> "total/cpu"
      | MaxRam -> "max/ram"
      | Parents i -> "parents/"^ string_of_int i
      | ArchivedTimes -> "archives/times"
      | NumArcFiles -> "archives/num_files"
      | NumArcBytes -> "archives/current_size"
      | AllocedArcBytes -> "archives/alloc_size"
      | PerInstance (signature, per_instance_key) ->
          Printf.sprintf2 "instances/%s/%a"
            signature
            print_per_instance per_instance_key)

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
    | Tail (site, fq, tail_key) ->
        Printf.fprintf fmt "tail/%a/%a/%a"
          N.site_print site
          N.fq_print fq
          print_tail_key tail_key
    | Error None ->
        Printf.fprintf fmt "errors/global"
    | Error (Some s) ->
        Printf.fprintf fmt "errors/users/%s" s

  (* Special key for error reporting: *)
  let global_errs = Error None
  let user_errs = function
    | User.Internal -> DevNull
    | User.Auth { name ; _ } -> Error (Some name)
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
                      try
                        PerWorker (N.fq fq,
                          match s with
                          | "is_used" -> IsUsed
                          | "startup_time" -> StartupTime)
                      with Match_failure _ ->
                        (match rcut fq, s with
                        | [ fq ; s1 ], s2 ->
                            try
                              PerWorker (N.fq fq,
                                match s1, s2 with
                                | "event_time", "min" -> MinETime
                                | "event_time", "max" -> MaxETime
                                | "total", "tuples" -> TotTuples
                                | "total", "bytes" -> TotBytes
                                | "total", "cpu" -> TotCpu
                                | "max", "ram" -> MaxRam
                                | "archives", "times" -> ArchivedTimes
                                | "archives", "num_files" -> NumArcFiles
                                | "archives", "current_size" -> NumArcBytes
                                | "archives", "alloc_size" -> AllocedArcBytes
                                | "parents", i ->
                                    Parents (int_of_string i))
                            with Match_failure _ ->
                              (match rcut fq, s1, s2 with
                              | [ fq ; "instance" ], sign, s ->
                                  PerWorker (N.fq fq, PerInstance (sign,
                                    match s with
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
        | "tail", s ->
            (match cut s with
            | site, fq_s ->
                (match rcut ~n:3 fq_s with
                | [ fq ; "users" ; s ] ->
                    Tail (N.site site, N.fq fq, Subscriber s)
                | [ fq ; "lasts" ; s ] ->
                    let i = int_of_string s in
                    Tail (N.site site, N.fq fq, LastTuple i)))
        | "errors", s ->
            Error (
              match cut s with
              | "global", "" -> None
              | "users", s -> Some s)
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
  type t =
    | Bool of bool
    | Int of int64
    | Float of float
    | String of string
    | Error of float * int * string
    (* Used for instance to reference parents of a worker: *)
    | Worker of N.site * N.program * N.func
    | Retention of Retention.t
    | TimeRange of TimeRange.t
    | Tuple of
        { skipped : int (* How many tuples were skipped before this one *) ;
          values : bytes (* serialized *) }
    | RamenType of T.t
    | RamenValue of T.value
    | TargetConfig of (N.program * rc_entry) list
    (* Holds all info from the compilation of a source ; what we used to have in the
     * executable binary itself. *)
    | SourceInfo of source_info

  and worker_role =
    | Whole
    (* Top half: only the filtering part of that function is run, once for
     * every local parent; output is forwarded to another site. *)
    | TopHalf of top_half_spec list
  (* FIXME: parent_num is not good enough because a parent num might change
   * when another parent is added/removed. *)

  and top_half_spec =
    (* FIXME: the workers should resolve themselves, onces they become proper
     * confsync clients: *)
    { tunneld_host : N.host ; tunneld_port : int ; parent_num : int }

  and rc_entry =
    { enabled : bool ;
      debug : bool ;
      report_period : float ;
      params : RamenParams.param list ;
      src_file : N.path ;
      on_site : Globs.t ;
      automatic : bool }

  and source_info =
    { md5 : string ;
      detail : detail_source_info }

  and detail_source_info =
    | CompiledSourceInfo of compiled_source_info
    (* Maybe distinguish linking errors that can go away independently?*)
    | FailedSourceInfo of failed_source_info

  and compiled_source_info =
    { default_params : RamenTuple.param list ;
      condition : E.t option ;
      funcs : function_info list }

  and failed_source_info =
    { err_msg : string }

  and function_info =
    { name : N.func ;
      retention : Retention.t option ;
      is_lazy : bool ;
      doc : string ;
      operation : O.t ;
      signature : string }

  let source_compiled i =
    match i.detail with
    | CompiledSourceInfo _ -> true
    | _ -> false

  let source_compilation_error i =
    match i.detail with
    | FailedSourceInfo { err_msg } -> err_msg
    | _ -> invalid_arg "source_compilation_error"

  let print_failed_source_info oc i =
    Printf.fprintf oc "err:%S" i.err_msg

  let print_compiled_source_info oc _i =
    Printf.fprintf oc "compiled (TODO)"

  let print_detail_source_info oc = function
    | CompiledSourceInfo i -> print_compiled_source_info oc i
    | FailedSourceInfo i -> print_failed_source_info oc i

  let print_source_info oc s =
    Printf.fprintf oc "md5:%S, %a" s.md5 print_detail_source_info s.detail

  let print_worker_role oc = function
    | Whole -> String.print oc "whole worker"
    | TopHalf _ -> String.print oc "top half"

  let print_rc_entry oc rc =
    Printf.fprintf oc
      "{ enabled=%b; debug=%b; report_period=%f; params={%a}; src_file=%a; \
         on_site=%a; automatic=%b }"
      rc.enabled rc.debug rc.report_period
      RamenParams.print_list rc.params
      N.path_print rc.src_file
      Globs.print rc.on_site
      rc.automatic

  let equal v1 v2 =
    match v1, v2 with
    (* For errors, avoid comparing timestamps as after Auth we would
     * otherwise sync it twice. *)
    | Error (_, i1, _), Error (_, i2, _) -> i1 = i2
    | v1, v2 -> v1 = v2

  let dummy = String "undefined"

  let rec print fmt = function
    | Bool b -> Bool.print fmt b
    | Int i -> Int64.print fmt i
    | Float f -> Float.print fmt f
    | String s -> String.print fmt s
    | Error (t, i, s) ->
        Printf.fprintf fmt "%a:%d:%s"
          print_as_date t i s
    | Worker (s, p, f) ->
        Printf.fprintf fmt "%a/%a/%a"
          N.site_print s N.program_print p N.func_print f
    | Retention r ->
        Retention.print fmt r
    | TimeRange r ->
        TimeRange.print fmt r
    | Tuple { skipped ; values } ->
        Printf.fprintf fmt "Tuple of %d bytes (after %d skipped)"
          (Bytes.length values) skipped
    | RamenType t ->
        T.print_typ fmt t
    | RamenValue v ->
        T.print fmt v
    | TargetConfig rcs ->
        Printf.fprintf fmt "TargetConfig %a"
          (List.print (fun oc (pname, rce) ->
            Printf.fprintf oc "%a=>%a"
              N.program_print pname print_rc_entry rce)) rcs
    | SourceInfo i ->
        Printf.fprintf fmt "SourceInfo { %a }"
          print_source_info i

  let err_msg i s = Error (Unix.gettimeofday (), i, s)

end
