(* This implementation of the configuration actually does the thing, if
 * only in a straightforward way. *)
open Batteries
open Model
open Lwt
open Option.Infix

let alerter = ref None

let init ?(alerter_conf_db="/tmp/alerter_conf.db")
         ?(alerter_tmp_state="/tmp/alerter_state.raw") () =
  alerter := Some (Alerter.get_state alerter_tmp_state alerter_conf_db)

module Impl : Engine.S with type ('e, 'k) result = 'e -> unit Lwt.t =
struct
  type ('e, 'k) result = 'e -> unit Lwt.t

  let output ks e =
    Lwt_list.iter_p (fun k -> k e) ks

  (* keep as many values with their timestamp and return a time series. Best
   * used after an aggregate on time. *)
  let series =
    let export_ns = ["series"] in
    fun ?name ?id ?ppp ~nb_values ks ->
      ignore name ; ignore id ; ignore ppp ;
      let values = Array.make nb_values None
      and next_idx = ref 0 in
      let output = output ks in
      fun e ->
        let i = !next_idx in
        values.(i) <- Some e ;
        let i = if i < Array.length values - 1 then i+1 else 0 in
        next_idx := i ;
        output (values, i)

  let discard ?name ?id ?ppp () =
    ignore name ; ignore id ; ignore ppp ;
    fun _ -> return_unit

  let replicate ?name ?id ?ppp ks =
    ignore name ; ignore id ; ignore ppp ;
    let output = output ks in
    fun e -> output e

  let convert ?name ?id ?ppp ~f ks =
    ignore name ; ignore id ; ignore ppp ;
    let output = output ks in
    fun e ->
      Lwt_list.iter_s output (f e)

  let filter ?name ?id ?ppp ~by ks =
    ignore name ; ignore id ; ignore ppp ;
    let output = output ks in
    fun e ->
      if by e then output e else return_unit

  let on_change ?name ?id ?ppp ks =
    ignore name ; ignore id ; ignore ppp ;
    let output = output ks in
    let prev = ref None in
    fun e ->
      match !prev with
      | Some e' when e' = e -> return_unit
      | _ -> prev := Some e ; output e

  type 'a aggr_value =
    { mutable last_touched : float ; mutable last_ev_count : int ; aggr : 'a }
  let aggregate ?name ?id ?ppp ~key_of_event ~make_aggregate
                ~aggregate ?is_complete ?timeout_sec ?timeout_events ks =
    ignore name ; ignore id ; ignore ppp ;
    let output = output ks in
    let h = Hashtbl.create 701 in
    let event_count = ref 0 in
    let max_key = ref None in
    fun e ->
      incr event_count ;
      let k = key_of_event e in
      let max_k =
        match !max_key with None -> k | Some mk -> max mk k in
      max_key := Some max_k ;
      let to_output = ref [] in
      (match Hashtbl.find h k with
      | exception Not_found ->
        Hashtbl.add h k {
            last_touched = !Alarm.now ;
            last_ev_count = !event_count ;
            aggr = make_aggregate e }
        | av ->
          av.last_touched <- !Alarm.now ;
          av.last_ev_count <- !event_count ;
          aggregate av.aggr e) ;
      (* haha lol: TODO a heap of timeouts *)
      Hashtbl.filteri_inplace (fun k av ->
          if Option.map_default (fun ts -> !Alarm.now -. av.last_touched >= ts) false timeout_sec ||
             Option.map_default (fun tc -> !event_count - av.last_ev_count >= tc) false timeout_events ||
             Option.map_default (fun ic -> ic k av.aggr max_k) false is_complete
          then (
            to_output := av.aggr :: !to_output ;
            false
          ) else true
        ) h ;
      Lwt_list.iter_p output !to_output

  let sliding_window ?name ?id ?ppp ~cmp ~is_complete ks =
    ignore name ; ignore id ; ignore ppp ;
    let output = output ks in
    let past_events = ref [] in
    fun e ->
      past_events := List.fast_sort cmp (e :: !past_events) ;
      if is_complete !past_events then (
        let%lwt () = output !past_events in
        past_events := List.tl !past_events ;
        return_unit
      ) else return_unit

  let all ?name ?id ?ppp ~cond ks =
    ignore name ; ignore id ; ignore ppp ;
    let output = output ks in
    fun es ->
      output (List.for_all cond es)

  let alert ?(name="unnamed alert") ?id ?(ppp=PPP_OCaml.bool) ?(importance=0) ~team ~title ~text () =
    ignore id ; ignore ppp ;
    fun firing ->
      let text = text 42 in
      Lwt.wrap (fun () ->
        Alerter.alert (Option.get !alerter) ~name ~team ~importance
                      ~title ~text ~firing)

  let save ?name ?id ?ppp ~retention () =
    ignore name ; ignore id ; ignore ppp ;
    fun e ->
      ignore retention ; ignore e (* TODO *) ;
      return_unit
end

module AddSettings (Conf : sig val graph : Graph.t end) (M : Engine.S with type ('e, 'k) result = 'e -> unit Lwt.t) :
  Engine.S with type ('e, 'k) result = 'e -> unit Lwt.t =
struct
  type ('e, 'k) result = 'e -> unit Lwt.t

  let connect ?id ?ppp op =
    let id = Option.get id in (* If this fail it's because you forgot AddId *)
    let node = Graph.lookup_node Conf.graph (Graph.Id id) in

    (* This is where we wrap output functions into whatever, or start another
     * reader for events that also call our ops. So for instance we will have
     * to read an 'e. Marshal or Obj.magic. Or...?  Of course, since the
     * objective is to be able to send a string as a value to any node, we need
     * to tell the configuration that this string is to be unserialized by this
     * unserializer, which will produce some type, that we have to assume is
     * ours. Or, we also take a PPP as a parameter or the function, so that we
     * can at least check at runtime that the values can be unserialized This
     * PPP could be an optional ?ppp BTW, so only those nodes with a defined
     * ppp could be written to. Not ideal though.  TODO: Also, ideally a single
     * PPP would be able to handle all supported syntax (json, ocaml...) and
     * pick one according to the node settings.  *)

    Setting.wrap_op ?ppp node (fun e ->
      if Node.is_alive node then op e
      else return_unit)

  let series ?name ?id ?ppp ~nb_values ks =
    connect ?id ?ppp (M.series ?name ?id ?ppp ~nb_values ks)

  let discard ?name ?id ?ppp () =
    connect ?id ?ppp (M.discard ?name ?id ?ppp ())

  let replicate ?name ?id ?ppp ks =
    connect ?id ?ppp (M.replicate ?name ?id ?ppp ks)

  let convert ?name ?id ?ppp ~f ks =
    connect ?id ?ppp (M.convert ?name ?id ?ppp ~f ks)

  let filter ?name ?id ?ppp ~by ks =
    connect ?id ?ppp (M.filter ?name ?id ?ppp ~by ks)

  let on_change ?name ?id ?ppp ks =
    connect ?id ?ppp (M.on_change ?name ?id ?ppp ks)

  let aggregate ?name ?id ?ppp ~key_of_event ~make_aggregate
                ~aggregate ?is_complete ?timeout_sec ?timeout_events ks =
    connect ?id ?ppp (M.aggregate ?name ?id ?ppp ~key_of_event ~make_aggregate ~aggregate ?is_complete ?timeout_sec ?timeout_events ks)

  let sliding_window ?name ?id ?ppp ~cmp ~is_complete ks =
    connect ?id ?ppp (M.sliding_window ?name ?id ?ppp ~cmp ~is_complete ks)

  let all ?name ?id ?ppp ~cond ks =
    connect ?id ?ppp (M.all ?name ?id ?ppp ~cond ks)

  let alert ?name ?id ?ppp ?importance ~team ~title ~text () =
    connect ?id ?ppp (M.alert ?name ?id ?ppp ?importance ~team ~title ~text ())

  let save ?name ?id ?ppp ~retention () =
    connect ?id ?ppp (M.save ?name ?id ?ppp ~retention ())
end

module AddTracing (M : Engine.S with type ('e, 'k) result = 'e -> unit Lwt.t) :
  Engine.S with type ('e, 'k) result = 'e -> unit Lwt.t =
struct
  type ('i, 'k) result = ('i, 'k) M.result

  let trace opname ?name ?ppp op =
    let name = match name with Some n -> opname ^"("^ n ^")" | None -> opname
    in
    match ppp with
    | Some ppp ->
      (fun e ->
        Printf.eprintf "%s %s\n%!" name (PPP.to_string ppp e) ;
        op e)
    | None ->
      (fun e ->
        Printf.eprintf "%s ???\n%!" name ;
        op e)

  let series ?name ?id ?ppp ~nb_values ks =
    trace "series" ?name ?ppp (M.series ?name ?id ?ppp ~nb_values ks)

  let discard ?name ?id ?ppp () =
    trace "discard" ?name ?ppp (M.discard ?name ?id ?ppp ())

  let replicate ?name ?id ?ppp ks =
    trace "replicate" ?name ?ppp (M.replicate ?name ?id ?ppp ks)

  let convert ?name ?id ?ppp ~f ks =
    trace "convert" ?name ?ppp (M.convert ?name ?id ?ppp ~f ks)

  let filter ?name ?id ?ppp ~by ks =
    trace "filter" ?name ?ppp (M.filter ?name ?id ?ppp ~by ks)

  let on_change ?name ?id ?ppp ks =
    trace "on_change" ?name ?ppp (M.on_change ?name ?id ?ppp ks)

  let aggregate ?name ?id ?ppp ~key_of_event ~make_aggregate ~aggregate
                ?is_complete ?timeout_sec ?timeout_events ks =
    trace "aggregate" ?name ?ppp (
      M.aggregate ?name ?id ?ppp ~key_of_event ~make_aggregate ~aggregate ?is_complete
                  ?timeout_sec ?timeout_events ks)

  let sliding_window ?name ?id ?ppp ~cmp ~is_complete ks =
    trace "sliding_window" ?name ?ppp (
      M.sliding_window ?name ?id ?ppp ~cmp ~is_complete ks)

  let all ?name ?id ?ppp ~cond ks =
    trace "all" ?name ?ppp (M.all ?name ?id ?ppp ~cond ks)

  let alert ?name ?id ?(ppp=PPP_OCaml.bool) ?importance ~team ~title ~text () =
    trace "alert" ?name ~ppp (
      M.alert ?name ?id ~ppp ?importance ~team ~title ~text ())

  let save ?name ?id ?ppp ~retention () =
    trace "save" ?name ?ppp (M.save ?name ?id ?ppp ~retention ())
end
