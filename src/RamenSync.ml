(* Now the actual implementation of the Ramen Sync Server and Client.
 * We need a client in OCaml to synchronise the configuration in between
 * several ramen processes, and one in C++ to synchronise with a GUI
 * tool.  *)
open Batteries
open RamenSyncIntf
open RamenHelpers
module N = RamenName
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

  type zmq_id = string
  type t =
    | Internal
    | Auth of { zmq_id : zmq_id ; name : string ; capas : Capa.t Set.t }
    | Anonymous of zmq_id

  let equal u1 u2 =
    match u1, u2 with
    | Internal, Internal -> true
    (* A user (identifier by name) could be connected several times, have
     * different zmq_id, and still be the same user: *)
    | Auth { name = n1 ; _ }, Auth { name = n2 ; _ } -> n1 = n2
    | Anonymous z1, Anonymous z2 -> z1 = z2
    | _ -> false

  let authenticated = function
    | Auth _ | Internal -> true
    | Anonymous _ -> false

  let internal = Internal

  module PubCredentials =
  struct
    (* TODO *)
    type t = string
    let print = String.print
  end

  (* FIXME: when to delete from these? *)
  let zmq_id_to_user = Hashtbl.create 30

  let authenticate u creds =
    match u with
    | Auth _ | Internal as u -> u (* ? *)
    | Anonymous zmq_id ->
        let name, capas =
          match creds with
          | "tintin" -> creds, []
          | "admin" -> creds, Capa.[ Admin ]
          | c when String.starts_with c "worker " -> creds, Capa.[ Ramen ]
          | _ -> failwith "Bad credentials" in
        let capas =
          Capa.Anybody :: Capa.SingleUser name :: capas |>
          Set.of_list in
        let u = Auth { zmq_id ; name ; capas } in
        Hashtbl.replace zmq_id_to_user zmq_id u ;
        u

  let of_zmq_id zmq_id =
    try Hashtbl.find zmq_id_to_user zmq_id
    with Not_found -> Anonymous zmq_id

  let zmq_id = function
    | Auth { zmq_id ; _ } | Anonymous zmq_id -> zmq_id
    | Internal ->
        invalid_arg "zmq_id"

  let print fmt = function
    | Internal -> String.print fmt "internal"
    | Auth { name ; _ } -> Printf.fprintf fmt "auth:%s" name
    | Anonymous zmq_id -> Printf.fprintf fmt "anonymous:%s" zmq_id

  type id = string

  let print_id = String.print

  (* Anonymous users could subscribe to some stuff... *)
  let id t = IO.to_string print t

  let has_capa c = function
    | Internal -> true
    | Auth { capas ; _ } -> Set.mem c capas
    | Anonymous _ -> c = Capa.anybody

  let only_me = function
    | Internal -> Capa.nobody
    | Auth { name ; _ } -> Capa.SingleUser name
    | Anonymous _ -> invalid_arg "only_me"
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
    | PerSite of N.site * per_site_key
    | PerProgram of (N.program * per_prog_key)
    | Storage of storage_key
    | Tail of N.site * N.fq * tail_key
    | Error of string option (* the user name *)
    (* TODO: alerting *)
  and per_site_key =
    | IsMaster
    | PerService of N.service * per_service_key
    | PerWorker of N.fq * per_site_fq_key
  and per_service_key =
    | Host
    | Port
  and per_prog_key =
    (* This is what the user ask for (via `ramen run` etc).
     * Not to be confused with the MustRun value, created by the confserver
     * out of those user set values. *)
    | MustRun
    | Debug
    | ReportPeriod
    | BinPath
    | SrcPath
    | Param of N.field
    | OnSite
    | Automatic
    | SourceText
    | SourceModTime
    | RunCondition
    | PerFunction of N.func * per_func_key
  and per_worker_key =
    | IsUsed
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
    | MustRun -> "must_run"
    | Debug -> "debug"
    | ReportPeriod -> "report_period"
    | BinPath -> "bin_path"
    | SrcPath -> "src_path"
    | Param s -> "param/"^ (s :> string)
    | OnSite -> "on_site"
    | Automatic -> "automatic"
    | SourceText -> "source/text"
    | SourceModTime -> "source/mtime"
    | RunCondition -> "run_condition"
    | PerFunction (fname, per_func_key) ->
        Printf.sprintf2 "functions/%a/%a"
          N.func_print fname
          print_per_func_key per_func_key)

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
      | AllocedArcBytes -> "archives/alloc_size")

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
    | User.Anonymous _ -> DevNull

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
                                  Parents (int_of_string i)))))
        | "programs", s ->
            (match cut s with
            | pname, s ->
              PerProgram (N.program pname,
                match cut s with
                | "must_run", "" -> MustRun
                | "debug", "" -> Debug
                | "report_period", "" -> ReportPeriod
                | "bin_path", "" -> BinPath
                | "src_path", "" -> SrcPath
                | "param", n -> Param (N.field n)
                | "on_site", "" -> OnSite
                | "automatic", "" -> Automatic
                | "source", "text" -> SourceText
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
    | Worker of N.site * N.program * N.func
    | Retention of Retention.t
    | TimeRange of TimeRange.t
    | Tuple of
        { skipped : int (* How many tuples were skipped before this one *) ;
          values : bytes (* serialized *) }
    | RamenType of T.t

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

  let err_msg i s = Error (Unix.gettimeofday (), i, s)

end
