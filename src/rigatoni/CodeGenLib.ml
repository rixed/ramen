(* Tools used by the generated OCaml code *)
open Batteries
open Stdint
open Log
open Lwt

(* Converters from string to values *)

let float_of_string = Pervasives.float_of_string
let string_of_string x = x
let bool_of_string = Pervasives.bool_of_string
let u8_of_string = Uint8.of_string
let u16_of_string = Uint16.of_string
let u32_of_string = Uint32.of_string
let u64_of_string = Uint64.of_string
let u128_of_string = Uint128.of_string
let i8_of_string = Int8.of_string
let i16_of_string = Int16.of_string
let i32_of_string = Int32.of_string
let i64_of_string = Int64.of_string
let i128_of_string = Int128.of_string
let null_of_string = ()
let eth_of_string s = Uint48.of_string ("0x"^ String.nreplace s ":" "")
let ip4_of_string = Ipv4.of_string
let ip6_of_string = Ipv6.of_string
let cidr4_of_string = Ipv4.Cidr.of_string
let cidr6_of_string = Ipv6.Cidr.of_string

(* Functions *)

(* We are not allowed to have any state specific to this function.
 * Consequently we must compute the sequence number from the start
 * and increment and the global tuple count. *)
let sequence start inc =
  Int128.(start + (of_uint64 !CodeGenLib_IO.tuple_count) * inc)

let now = Unix.gettimeofday

let age_float x = x -. now ()
let age_u8 = Uint8.of_float % age_float
let age_u16 = Uint16.of_float % age_float
let age_u32 = Uint32.of_float % age_float
let age_u64 = Uint64.of_float % age_float
let age_u128 = Uint128.of_float % age_float
let age_i8 = Int8.of_float % age_float
let age_i16 = Int16.of_float % age_float
let age_i32 = Int32.of_float % age_float
let age_i64 = Int64.of_float % age_float
let age_i128 = Int128.of_float % age_float
(* FIXME: typecheck age_eth, age_ipv4 etc out of existence *)

let percentile prev _pct x = x::prev
let percentile_finalize pct lst =
  let arr = Array.of_list lst in
  Array.fast_sort Pervasives.compare arr ;
  assert (pct >= 0.0 && pct <= 100.0) ;
  let pct = pct *. 0.01 in
  let idx = Helpers.round_to_int (pct *. float_of_int (Array.length arr - 1)) in
  arr.(idx)

let getenv ?def n =
  try Sys.getenv n
  with Not_found ->
    match def with
    | Some d -> d
    | None ->
      Printf.sprintf "Cannot find envvar %s" n |>
      failwith

let identity x = x

let begin_of_range_cidr4 (n, l) = Ipv4.Cidr.and_to_len l n
let end_of_range_cidr4 (n, l) = Ipv4.Cidr.or_to_len l n
let begin_of_range_cidr6 (n, l) = Ipv6.Cidr.and_to_len l n
let end_of_range_cidr6 (n, l) = Ipv6.Cidr.or_to_len l n

(* Health and Stats
 *
 * Each node has to periodically report to ramen http server its health and some stats.
 * Could have been the other way around, and that would have made the
 * connection establishment easier possibly (since we already must be able to
 * ssh to other machines in order to start a node) but we already have an http
 * server on Ramen and probably want to avoid opening too many ports everywhere, and forcing
 * generated nodes to implement too many things.
 *
 * Stats must include:
 *
 * - total number of tuples input and output
 * - total CPU time consumed
 * - current RAM used
 * and others depending on the operation.
 *)

open Binocle

let stats_in_tuple_count =
  IntCounter.make Consts.in_tuple_count_metric
    "Number of received tuples that have been processed since the \
     node started."

let make_stats_selected_tuple_count () =
  IntCounter.make Consts.selected_tuple_count_metric
    "Number of tuples that have passed the WHERE filter, since the \
     node started."

let stats_out_tuple_count =
  IntCounter.make Consts.out_tuple_count_metric
    "Number of emitted tuples to each child of this node since it started."

let stats_cpu =
  FloatCounter.make Consts.cpu_time_metric
    "Total CPU time, in seconds, spent in this node (this process and any \
     subprocesses."

let stats_ram =
  IntGauge.make Consts.ram_usage_metric
    "Total RAM size used by the GC, in bytes (does not take into account \
     other heap allocations nor fragmentation."

let tot_cpu_time () =
  let open Unix in
  let pt = times () in
  pt.tms_utime +. pt.tms_stime +. pt.tms_cutime +. pt.tms_cstime

let tot_ram_usage =
  let word_size = Sys.word_size / 8 in
  fun () ->
    let stat = Gc.quick_stat () in
    stat.Gc.heap_words * word_size

let send_stats url =
  let open Cohttp in
  let open Cohttp_lwt_unix in
  let metrics = Hashtbl.fold (fun _name exporter lst ->
    List.rev_append (exporter ()) lst) Binocle.all_measures [] in
  let body = `String Marshal.(to_string metrics [No_sharing]) in
  let headers = Header.init_with "Content-Type" Consts.ocaml_marshal_type in
  (* TODO: but also fix the server never timeouting! *)
  let headers = Header.add headers "Connection" "close" in
  !logger.debug "Send stats to %S" url ;
  let%lwt resp, body = Client.put ~headers ~body (Uri.of_string url) in
  let code = resp |> Response.status |> Code.code_of_status in
  if code <> 200 then (
    let%lwt body = Cohttp_lwt_body.to_string body in
    !logger.error "Received code %d, body %S, when reporting stats to %S"
      code body url ;
    return_unit
  ) else
    return_unit

let update_stats_th report_url () =
  while%lwt true do
    FloatCounter.set stats_cpu (tot_cpu_time ()) ;
    IntGauge.set stats_ram (tot_ram_usage ()) ;
    let%lwt () = send_stats report_url in
    Lwt_unix.sleep 10.
  done

(* Helpers *)

let output rb sersize_of_tuple serialize_tuple tuple =
  let open RingBuf in
  let sersize = sersize_of_tuple tuple in
  let tx = enqueue_alloc rb sersize in
  let offs = serialize_tuple tx tuple in
  enqueue_commit tx ;
  assert (offs = sersize)

let outputer_of rb_outs sersize_of_tuple serialize_tuple =
  let outputers_with_retry = List.map (fun rb_out ->
        let once = output rb_out sersize_of_tuple serialize_tuple in
        RingBufLib.retry_for_ringbuf once
      ) rb_outs in
  fun tuple ->
    IntCounter.add stats_out_tuple_count 1 ;
    List.map (fun out -> out tuple) outputers_with_retry |>
    join

(* Each node can write in several ringbuffers (one per children) which
 * names are given by the output_ringbuf envvar followed by the child number
 * as an extension. *)
let load_out_ringbufs () =
  let rb_out_fnames = getenv ~def:"/tmp/ringbuf_out" "output_ringbufs" |> String.split_on_char ','
  in
  !logger.info "Will output into %a" (List.print String.print) rb_out_fnames ;
  List.map (fun fname -> RingBuf.load fname) rb_out_fnames

let node_start node_name =
  !logger.info "Starting %s process..." node_name ;
  let report_url =
    (* The real one will have an process identifier instead of "anonymous" *)
    getenv ~def:"http://localhost:29380/report/anonymous" "report_url" in
  async (update_stats_th report_url) (* TODO: catch exceptions in async_exception_hook *)

exception InvalidCSVQuoting

let quote_at_start s =
  String.length s > 0 && s.[0] = '"'

let quote_at_end s =
  String.length s > 0 && s.[String.length s - 1] = '"'

let read_csv_file filename do_unlink separator sersize_of_tuple
                  serialize_tuple tuple_of_strings preprocessor =
  node_start "READ CSV FILE" ;
  (* For tests, allow to overwrite what's specified in the operation: *)
  let filename = getenv ~def:filename "csv_filename"
  and separator = getenv ~def:separator "csv_separator"
  in
  !logger.debug "Will read CSV file %S using separator %S"
                filename separator ;
  let of_string line =
    let strings = String.nsplit line separator in
    (* Handle quoting in CSV values. TODO: enable/disable based on operation flag *)
    let strings', rem_s, has_quote =
      List.fold_left (fun (lst, prev_s, has_quote) s ->
        if prev_s = "" then (
          if quote_at_start s then lst, s, true
          else (s :: lst), "", has_quote
        ) else (
          if quote_at_end s then (String.(lchop prev_s ^ rchop s) :: lst, "", true)
          else lst, prev_s ^ s, true
        )) ([], "", false) strings in
    if rem_s <> "" then raise InvalidCSVQuoting ;
    let strings = if has_quote then List.rev strings' else strings in
    tuple_of_strings (Array.of_list strings)
  in
  let rb_outs = load_out_ringbufs () in
  let outputer =
    outputer_of rb_outs sersize_of_tuple serialize_tuple in
  CodeGenLib_IO.read_glob_lines ~do_unlink filename preprocessor (fun line ->
    match of_string line with
    | exception e ->
      !logger.error "Cannot parse line %S: %s"
        line (Printexc.to_string e) ;
      return_unit ;
    | tuple -> outputer tuple)

(* Operations that nodes may run: *)

let notify url field_of_tuple tuple =
  let expand_fields =
    let open Str in
    let re = regexp "\\${\\(in\\.\\)?\\([_a-zA-Z0-9]+\\)}" in
    fun text tuple ->
      global_substitute re (fun s ->
          let field_name = matched_group 2 s in
          try field_of_tuple tuple field_name
          with Not_found ->
            !logger.error "Field %S used in text substitution is not \
                           present in the input!" field_name ;
            "??"^ field_name ^"??"
        ) text
  in
  let url = expand_fields url tuple in
  !logger.info "Notifying url %S" url ;
  async (fun () -> CodeGenLib_IO.http_notify url)

let select read_tuple field_of_tuple sersize_of_tuple serialize_tuple where select notify_url =
  node_start "SELECT" ;
  let rb_in_fname = getenv ~def:"/tmp/ringbuf_in" "input_ringbuf"
  in
  !logger.debug "Will read ringbuffer %S" rb_in_fname ;
  let rb_outs = load_out_ringbufs () in
  let%lwt rb_in =
    Helpers.retry ~on:(fun _ -> true) ~min_delay:1.0 RingBuf.load rb_in_fname in
  let outputer =
    outputer_of rb_outs sersize_of_tuple serialize_tuple
  and stats_selected_tuple_count = make_stats_selected_tuple_count ()
  and in_ = ref None
  and selected_tuple = ref None
  and selected_count = ref Uint64.zero
  and selected_successive = ref Uint64.zero
  and unselected_tuple = ref None
  and unselected_count = ref Uint64.zero
  and unselected_successive = ref Uint64.zero in
  CodeGenLib_IO.read_ringbuf rb_in (fun tx ->
    let in_tuple = read_tuple tx in
    RingBuf.dequeue_commit tx ;
    IntCounter.add stats_in_tuple_count 1 ;
    let last_in = Option.default in_tuple !in_
    and last_selected = Option.default in_tuple !selected_tuple
    and last_unselected = Option.default in_tuple !unselected_tuple in
    in_ := Some in_tuple ;
    let in_count = Uint64.succ !CodeGenLib_IO.tuple_count in
    if where
         in_count in_tuple last_in
         !selected_count !selected_successive last_selected
         !unselected_count !unselected_successive last_unselected
    then (
      if notify_url <> "" then notify notify_url field_of_tuple in_tuple ;
      IntCounter.add stats_selected_tuple_count 1 ;
      unselected_successive := Uint64.zero ;
      selected_tuple := Some in_tuple ;
      selected_count := Uint64.succ !selected_count ;
      selected_successive := Uint64.succ !selected_successive ;
      let out_tuple =
        select
          in_count in_tuple last_in
          !selected_count !selected_successive last_selected
          !unselected_count !unselected_successive last_unselected
      in
      outputer out_tuple
    ) else (
      selected_successive := Uint64.zero ;
      unselected_tuple := Some in_tuple ;
      unselected_count := Uint64.succ !unselected_count ;
      unselected_successive := Uint64.succ !unselected_successive ;
      return_unit
    ))

let yield sersize_of_tuple serialize_tuple select =
  node_start "YIELD" ;
  let rb_outs = load_out_ringbufs () in
  let outputer =
    outputer_of rb_outs sersize_of_tuple serialize_tuple in
  let rec loop () =
    let%lwt () = outputer (select Uint64.zero () ()) in
    CodeGenLib_IO.on_each_input () ;
    loop () in
  loop ()

type ('aggr, 'tuple_in, 'tuple_out) aggr_value =
  { (* used to compute the actual selected field when outputing the
     * aggregate: *)
    mutable first_in : 'tuple_in ; (* first in-tuple of this aggregate *)
    mutable last_in : 'tuple_in ; (* last in-tuple of this aggregate *)
    mutable out_tuple : 'tuple_out ; (* The current one *)
    mutable previous_out : 'tuple_out ; (* previously computed temp out tuple, if any *)
    mutable nb_entries : int ;
    mutable nb_successive : int ;
    mutable last_ev_count : int ; (* used for others.successive (TODO) *)
    mutable to_resubmit : 'tuple_in list ; (* in_tuples to resubmit at flush *)
    mutable fields : 'aggr (* the record of aggregation values *) }

let flush_aggr aggr_init update_aggr should_resubmit h k aggr =
  if aggr.to_resubmit = [] then
    Hashtbl.remove h k
  else (
    let to_resubmit = List.rev aggr.to_resubmit in
    aggr.nb_entries <- 1 ;
    aggr.to_resubmit <- [] ;
    (* Warning: should_resubmit might need realistic nb_entries, last_in etc *)
    let in_tuple = List.hd to_resubmit in
    aggr.first_in <- in_tuple ;
    aggr.last_in <- in_tuple ;
    aggr.fields <- aggr_init in_tuple ;
    if should_resubmit aggr in_tuple then
      aggr.to_resubmit <- [ in_tuple ] ;
    List.iter (fun in_tuple ->
        update_aggr aggr.fields in_tuple ;
        aggr.nb_entries <- aggr.nb_entries + 1 ;
        aggr.last_in <- in_tuple ;
        if should_resubmit aggr in_tuple then
          aggr.to_resubmit <- in_tuple :: aggr.to_resubmit
      ) (List.tl to_resubmit)
  )

type when_to_check_group = ForAll | ForAllSelected | ForAllInGroup

let aggregate (read_tuple : RingBuf.tx -> 'tuple_in)
              (sersize_of_tuple : 'tuple_out -> int)
              (serialize_tuple : RingBuf.tx -> 'tuple_out -> int)
              (tuple_of_aggr :
                Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
                Uint64.t -> (* out.#count *)
                Uint64.t -> Uint64.t -> 'aggr -> (* group.#count, #successive, aggr *)
                'tuple_in -> 'tuple_in -> (* first, last *)
                'tuple_out)
              (* Where_fast/slow: premature optimisation: if the where filter
               * uses the aggregate then we need where_slow (checked after
               * the aggregate look up) but if it uses only the incoming
               * tuple then we can use only where_fast. *)
              (where_fast :
                Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
                bool)
              (where_slow :
                Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
                Uint64.t -> Uint64.t -> 'aggr -> (* group.#count, #successive, aggr *)
                'tuple_in -> 'tuple_in -> (* first, last *)
                bool)
              (key_of_input : 'tuple_in -> 'key)
              (commit_when :
                Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
                Uint64.t -> 'tuple_out -> (* out.#count, previous *)
                Uint64.t -> Uint64.t -> 'aggr -> (* group.#count, #successive, aggr *)
                'tuple_in -> 'tuple_in -> 'tuple_out -> (* first, last, current out *)
                bool)
              (when_to_check_for_commit : when_to_check_group)
              (flush_when :
                Uint64.t -> 'tuple_in -> 'tuple_in -> (* in.#count, current and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* selected.#count, #successive and last *)
                Uint64.t -> Uint64.t -> 'tuple_in -> (* unselected.#count, #successive and last *)
                Uint64.t -> 'tuple_out -> (* out.#count, previous *)
                Uint64.t -> Uint64.t -> 'aggr -> (* group.#count, #successive, aggr *)
                'tuple_in -> 'tuple_in -> 'tuple_out -> (* first, last, current out *)
                bool)
              (when_to_check_for_flush : when_to_check_group)
              (should_resubmit : ('aggr, 'tuple_in, 'tuple_out) aggr_value -> 'tuple_in -> bool)
              (aggr_init : 'tuple_in -> 'aggr)
              (update_aggr : 'aggr -> 'tuple_in -> unit) =
  node_start "GROUP BY" ;
  let rb_in_fname = getenv ~def:"/tmp/ringbuf_in" "input_ringbuf"
  in
  !logger.debug "Will read ringbuffer %S" rb_in_fname ;
  let rb_outs = load_out_ringbufs () in
  let%lwt rb_in =
    Helpers.retry ~on:(fun _ -> true) ~min_delay:1.0
                  RingBuf.load rb_in_fname in
  let h = Hashtbl.create 701
  and stats_selected_tuple_count = make_stats_selected_tuple_count ()
  and event_count = ref 0 (* used to fake others.count etc *)
  and last_key = ref None (* used for successive count *)
  and in_ = ref None (* last incoming tuple *)
  and selected_tuple = ref None (* last incoming tuple that passed the where filter *)
  and selected_count = ref Uint64.zero
  and selected_successive = ref Uint64.zero
  and unselected_tuple = ref None
  and unselected_count = ref Uint64.zero
  and unselected_successive = ref Uint64.zero
  and out_count = ref Uint64.zero
  and stats_group_count =
    IntGauge.make Consts.group_count_metric "Number of groups currently maintained."
  in
  IntGauge.set stats_group_count 0 ;
  let commit tuple =
    out_count := Uint64.succ !out_count ;
    outputer_of rb_outs sersize_of_tuple serialize_tuple tuple
  in
  CodeGenLib_IO.read_ringbuf rb_in (fun tx ->
    let in_tuple = read_tuple tx in
    RingBuf.dequeue_commit tx ;
    IntCounter.add stats_in_tuple_count 1 ;
    let last_in = Option.default in_tuple !in_
    and last_selected = Option.default in_tuple !selected_tuple
    and last_unselected = Option.default in_tuple !unselected_tuple in
    in_ := Some in_tuple ;
    let in_count = Uint64.succ !CodeGenLib_IO.tuple_count in
    (* TODO: pass selected_successive *)
    let must f aggr =
      f in_count in_tuple last_in
        !selected_count !selected_successive last_selected
        !unselected_count !unselected_successive last_unselected
        !out_count aggr.previous_out
        (Uint64.of_int aggr.nb_entries) (Uint64.of_int aggr.nb_successive) aggr.fields
        aggr.first_in aggr.last_in
        aggr.out_tuple
    in
    let commit_and_flush_list to_commit to_flush =
      (* We must commit first and then flush *)
      let%lwt () =
        Lwt_list.iter_s (fun (_k, a) -> commit a.out_tuple) to_commit in
      List.iter (fun (k, a) ->
          flush_aggr aggr_init update_aggr should_resubmit h k a
        ) to_flush ;
      return_unit
    in
    let commit_and_flush_all_if check_when =
      let to_commit =
        if when_to_check_for_commit <> check_when then [] else
          Hashtbl.fold (fun k a l ->
            if must commit_when a then (k, a)::l else l) h [] in
      let to_flush =
        if flush_when == commit_when then to_commit else
        if when_to_check_for_commit <> check_when then [] else
        Hashtbl.fold (fun k a l ->
          if must flush_when a then (k, a)::l else l) h [] in
      commit_and_flush_list to_commit to_flush
    in
    (if where_fast
         in_count in_tuple last_in
         !selected_count !selected_successive last_selected
         !unselected_count !unselected_successive last_unselected
    then (
      IntGauge.set stats_group_count (Hashtbl.length h) ;
      let k = key_of_input in_tuple in
      let prev_last_key = !last_key in
      last_key := Some k ;
      (* Update/create the group *)
      let aggr_opt =
        match Hashtbl.find h k with
        | exception Not_found ->
          let fields = aggr_init in_tuple
          and one = Uint64.one in
          if where_slow
               in_count in_tuple last_in
               !selected_count !selected_successive last_selected
               !unselected_count !unselected_successive last_unselected
               one one fields
               in_tuple in_tuple
          then (
            IntCounter.add stats_selected_tuple_count 1 ;
            (* TODO: pass selected_successive *)
            let out_tuple =
              tuple_of_aggr
                in_count in_tuple last_in
                !selected_count !selected_successive last_selected
                !unselected_count !unselected_successive last_unselected
                !out_count
                one one fields
                in_tuple in_tuple in
            let aggr = {
              first_in = in_tuple ;
              last_in = in_tuple ;
              out_tuple = out_tuple ;
              previous_out = out_tuple ; (* Not correct for the very first check *)
              nb_entries = 1 ;
              nb_successive = 1 ;
              last_ev_count = !event_count ;
              to_resubmit = [] ;
              fields } in
            Hashtbl.add h k aggr ;
            if should_resubmit aggr in_tuple then
              aggr.to_resubmit <- [ in_tuple ] ;
            Some aggr
          ) else None
        | aggr ->
          if where_slow
               in_count in_tuple last_in
               !selected_count !selected_successive last_selected
               !unselected_count !unselected_successive last_unselected
               (Uint64.of_int aggr.nb_entries) (Uint64.of_int aggr.nb_successive) aggr.fields
               aggr.first_in aggr.last_in
          then (
            IntCounter.add stats_selected_tuple_count 1 ;
            update_aggr aggr.fields in_tuple ;
            aggr.last_ev_count <- !event_count ;
            aggr.nb_entries <- aggr.nb_entries + 1 ;
            if should_resubmit aggr in_tuple then
              aggr.to_resubmit <- in_tuple :: aggr.to_resubmit ;
            if prev_last_key = Some k then
              aggr.nb_successive <- aggr.nb_successive + 1 ;
            (* TODO: pass selected_successive *)
            let out_tuple =
              tuple_of_aggr
                in_count in_tuple last_in
                !selected_count !selected_successive last_selected
                !unselected_count !unselected_successive last_unselected
                !out_count
                (Uint64.of_int aggr.nb_entries) (Uint64.of_int aggr.nb_successive) aggr.fields
                aggr.first_in aggr.last_in in
            aggr.out_tuple <- out_tuple ;
            aggr.last_in <- in_tuple ;
            Some aggr
          ) else None in
      (match aggr_opt with
      | None ->
        selected_successive := Uint64.zero ;
        unselected_tuple := Some in_tuple ;
        unselected_count := Uint64.succ !unselected_count ;
        unselected_successive := Uint64.succ !unselected_successive ;
        return_unit
      | Some aggr ->
        (* Here we passed the where filter and the selected_tuple (and
         * selected_count) must be updated. *)
        unselected_successive := Uint64.zero ;
        selected_tuple := Some in_tuple ;
        selected_count := Uint64.succ !selected_count ;
        selected_successive := Uint64.succ !selected_successive ;
        (* Committing / Flushing *)
        let to_commit =
          if when_to_check_for_commit = ForAllInGroup && must commit_when aggr
          then [ k, aggr ] else [] in
        let to_flush =
          if flush_when == commit_when then to_commit else
          if when_to_check_for_flush = ForAllInGroup && must flush_when aggr
          then [ k, aggr ] else [] in
        let%lwt () = commit_and_flush_list to_commit to_flush in
        (* Maybe any other groups. Notice that there is no risk to commit/flush
         * this aggr twice since when_to_check_for_commit force either one or the
         * other (or none at all) of these chunks of code to be run. *)
        let%lwt () = commit_and_flush_all_if ForAllSelected in
        aggr.previous_out <- aggr.out_tuple ;
        return_unit)
    ) else return_unit) >>= fun () ->
    (* Now there is also the possibility that we need to commit / flush for
     * every single input tuple :-< *)
    commit_and_flush_all_if ForAll
  )

let () =
  Lwt.async_exception_hook := (fun exn ->
    !logger.error "Received exception %s from:\n%s"
      (Printexc.to_string exn)
      (Printexc.get_backtrace()))
