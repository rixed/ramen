(* Now the actual implementation of the Ramen Sync Server and Client.
 * We need a client in OCaml to synchronise the configuration in between
 * several ramen processes, and one in C++ to synchronise with a GUI
 * tool.  *)
open Batteries
open RamenSyncIntf
open RamenHelpers
module N = RamenName
module T = RamenTypes
module C = RamenConf
module F = C.Func

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
    | Ramen (* Internal use by Ramen services *)
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
    let to_string s = s
    let of_string s = s
  end

  (* FIXME: when to delete from these? *)
  let zmq_id_to_user = Hashtbl.create 30

  let authenticate u creds =
    match u with
    | Auth _ | Internal as u -> u (* ? *)
    | Anonymous zmq_id ->
        let name, capas =
          match creds with
          | "tintin" -> "tintin", []
          | "admin" -> "admin", Capa.[ Admin ]
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
        invalid_arg "zqm_id"

  let print fmt = function
    | Internal -> String.print fmt "internal"
    | Auth { name ; _ } -> Printf.fprintf fmt "auth:%S" name
    | Anonymous zmq_id -> Printf.fprintf fmt "anonymous:%S" zmq_id

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
  module User = User

  type t =
    | DevNull (* Special, nobody should be allowed to read it *)
    | PerSite of N.site * per_site_key
    | Storage of storage_key
    | Error of string option (* the user name *)
  and per_site_key =
    | Name
    | IsMaster
    | PerService of N.service * per_service_key
    | PerFunction of N.fq * per_site_fq_key
  and per_service_key =
    | Host
    | Port
  and per_site_fq_key =
    | StartupTime | MinETime | MaxETime
    | TotTuples | TotBytes | TotCpu | MaxRam
    | Parents
    | ArchivedTimes
  and storage_key =
    | TotalSize
    | RecallCost
    | RetentionsOverride of Globs.t

  let print_per_service_key fmt k =
    String.print fmt (match k with
      | Host -> "host"
      | Port -> "port")

  let print_per_site_fq_key fmt k =
    String.print fmt (match k with
      | StartupTime -> "startup_time"
      | MinETime -> "event_time/min"
      | MaxETime -> "event_time/max"
      | TotTuples -> "total/tuples"
      | TotBytes -> "total/bytes"
      | TotCpu -> "total/cpu"
      | MaxRam -> "max/ram"
      | Parents -> "parents"
      | ArchivedTimes -> "archived_times")

  let print_per_site_key fmt = function
    | Name ->
        String.print fmt "name"
    | IsMaster ->
        String.print fmt "is_master"
    | PerService (service, per_service_key) ->
        Printf.fprintf fmt "services/%a/%a"
          N.service_print service
          print_per_service_key per_service_key
    | PerFunction (fq, per_site_fq_key) ->
        Printf.fprintf fmt "functions/%a/%a"
          N.fq_print fq
          print_per_site_fq_key per_site_fq_key

  let print_storage_key fmt = function
    | TotalSize ->
        String.print fmt "total_size"
    | RecallCost ->
        String.print fmt "recall_cost"
    | RetentionsOverride glob ->
        (* No need to quote the glob as it's in leaf position: *)
        Printf.fprintf fmt "retention_override/%a"
          Globs.print glob

  let print fmt = function
    | DevNull ->
        String.print fmt "devnull"
    | PerSite (site, per_site_key) ->
        Printf.fprintf fmt "sites/%a/%a"
          N.site_print site
          print_per_site_key per_site_key
    | Storage storage_key ->
        Printf.fprintf fmt "storage/%a"
          print_storage_key storage_key
    | Error None ->
        Printf.fprintf fmt "error/global"
    | Error (Some s) ->
        Printf.fprintf fmt "error/users/%s" s

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
    let rec cut s =
      try String.split ~by:"/" s
      with Not_found -> s, "" in
    fun s ->
      try
        match cut s with
        | "sites", s ->
            let site, s = cut s in
            PerSite (N.site site,
              match cut s with
              | "name", "" ->
                  Name
              | "is_master", "" ->
                  IsMaster
              | "services", s ->
                  (match cut s with
                  | service, s ->
                      PerService (N.service service,
                        match cut s with
                        | "host", "" -> Host
                        | "port", "" -> Port))
              | "functions", s ->
                  (match cut s with
                  | fq, s ->
                      PerFunction (N.fq fq,
                        match cut s with
                        | "startup_time", "" -> StartupTime
                        | "event_time/min", "" -> MinETime
                        | "event_time/max", "" -> MaxETime
                        | "total/tuples", "" -> TotTuples
                        | "total/bytes", "" -> TotBytes
                        | "total/cpu", "" -> TotCpu
                        | "max/ram", "" -> MaxRam
                        | "parents", "" -> Parents
                        | "archived_times", "" -> ArchivedTimes)))
        | "storage", s ->
            Storage (
              match cut s with
              | "total_size", "" -> TotalSize
              | "recall_cost", "" -> RecallCost
              | "retention_override", s ->
                  RetentionsOverride (Globs.compile s))
        | "error", s ->
            Error (
              match cut s with
              | "global", "" -> None
              | "users", s -> Some s)
    with Match_failure _ ->
      Printf.sprintf "Cannot parse key (%S)" s |>
      failwith
      [@@ocaml.warning "-8"]
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

  let to_string s = IO.to_string Globs.print s
  let of_string s = Globs.compile s
end

(* Unfortunately there is no association between the key and the type for
 * now. *)
module Value =
struct
  type t =
    | Bool of bool
    | Int of int64
    | Float of float
    | Time of float
    | String of string
    | Error of float * int * string
    | Retention of F.retention
    | RamenDataset of (T.value array * int (* index of the first *))

  let equal v1 v2 =
    match v1, v2 with
    (* For errors, avoid comparing timestamps as after Auth we would
     * otherwise sync it twice. *)
    | Error (_, i1, _), Error (_, i2, _) -> i1 = i2
    | v1, v2 -> v1 = v2

  let dummy = Bool true

  let print fmt = function
    | Bool b -> Bool.print fmt b
    | Int i -> Int64.print fmt i
    | Float f | Time f -> Float.print fmt f
    | String s -> String.print fmt s
    | Error (t, i, s) ->
        Printf.fprintf fmt "%a:%d:%s"
          print_as_date t i s
    | Retention r ->
        F.print_retention fmt r
    | RamenDataset (a, i) ->
        Printf.fprintf fmt "%a,%d"
          (Array.print T.print) a i

  let err_msg i s = Error (Unix.gettimeofday (), i, s)

  let to_string ?prev t =
    ignore prev ;
    Marshal.(to_string t [ No_sharing ])

  let of_string ?prev b =
    ignore prev ;
    Marshal.from_string b 0
end

module Client = RamenSyncClient.Make (Value) (Selector)

(* TODO:
 * - a stand alone ocaml program with this Client module above
 * - each time a key is created/modified/locked/unlocked/deleted,
 *   call a C++ function depending on the key, with proper C++
 *   values as arguments, that will, for now, just print something.
 * - make the client module above authenticate and also report to the
 *   C++ the answer to the sync.
 * - develop the Qt widgets that start by displaying a "waiting..."
 *   message and then, once the minimal required keys have been set
 *   (ie slot called) turn into an actual edition/visualisation widget
 *   adapted to the keys watched.
 * - start with the archive storage user config edition widget.
 *)
