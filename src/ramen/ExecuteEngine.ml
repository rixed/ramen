(* This implementation of the configuration actually does the thing, if
 * only in a straightforward way. *)
open Batteries
open Model
open Lwt

let alerter = ref None

let init ?(alerter_conf_db="/tmp/alerter_conf.db")
         ?(alerter_tmp_state="/tmp/alerter_state.raw") () =
  alerter := Some (Alerter.get_state alerter_tmp_state alerter_conf_db)

module Impl (Conf : sig val graph : Graph.t end) :
  Engine.S with type ('e, 'k) result = 'e -> unit Lwt.t =
struct
  type ('e, 'k) result = 'e -> unit Lwt.t

  let connect ?id ?ppp ks op =
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

    (* Our output function: *)
    let output e = Lwt_list.iter_p (fun k -> k e) ks in
    Setting.wrap_op ?ppp node (fun e ->
      if Node.is_alive node then op output e
      else return_unit)

  let discard ?name ?id ?ppp () =
    ignore name ;
    connect ?id ?ppp [] (fun _ _ -> return_unit)

  let replicate ?name ?id ?ppp ks =
    ignore name ;
    connect ?id ?ppp ks (fun output e -> output e)

  let convert ?name ?id ?ppp ~f ks =
    ignore name ;
    connect ?id ?ppp ks (fun output e ->
      Lwt_list.iter_s output (f e))

  let filter ?name ?id ?ppp ~by ks =
    ignore name ;
    connect ?id ?ppp ks (fun output e ->
      if by e then output e else return_unit)

  let on_change ?name ?id ?ppp ks =
    ignore name ;
    let prev = ref None in
    connect ?id ?ppp ks (fun output e ->
      match !prev with
      | Some e' when e' = e -> return_unit
      | _ -> prev := Some e ; output e)

  let aggregate ?name ?id ?ppp ~key_of_event ~make_aggregate
                ~aggregate ?is_complete ?timeout_sec ?timeout_events ks =
    ignore name ;
    let h = Hashtbl.create 701 in
    let event_count = ref 0 in
    connect ?id ?ppp ks (fun output e ->
      incr event_count ;
      let k = key_of_event e in
      let to_output = ref [] in
      (match Hashtbl.find h k with
      | exception Not_found ->
        Hashtbl.add h k (!Alarm.now, !event_count, make_aggregate e)
      | _, _, a ->
        aggregate a e ;
        Option.may (fun ic -> if ic a then to_output := a :: !to_output) is_complete) ;
      (* haha lol: TODO a heap of timeouts *)
      Hashtbl.filter_inplace (fun (t, c, a) ->
          if Option.map_default (fun ts -> !Alarm.now -. t >= ts) false timeout_sec ||
             Option.map_default (fun tc -> !event_count - c >= tc) false timeout_events
          then (
            to_output := a :: !to_output ;
            false
          ) else true
        ) h ;
      Lwt_list.iter_p output !to_output)

  let sliding_window ?name ?id ?ppp ~cmp ~is_complete ks =
    ignore name ;
    let past_events = ref [] in
    connect ?id ?ppp ks (fun output e ->
      past_events := List.fast_sort cmp (e :: !past_events) ;
      if is_complete !past_events then (
        let%lwt () = output !past_events in
        past_events := List.tl !past_events ;
        return_unit
      ) else return_unit)

  let all ?name ?id ?ppp ~cond ks =
    ignore name ;
    connect ?id ?ppp ks (fun output es ->
      output (List.for_all cond es))

  let alert ?(name="unnamed alert") ?id ?ppp ?(importance=0) ~team ~title ~text () =
    connect ?id ?ppp [] (fun _output firing ->
      let text = if firing then text 42 else "Back to Normal" in
      Lwt.wrap (fun () ->
        Alerter.alert (Option.get !alerter) ~name ~team ~importance
                      ~title ~text))

  let save ?name ?id ?ppp ~retention () =
    ignore name ;
    connect ?id ?ppp [] (fun _output e ->
      ignore retention ; ignore e (* TODO *) ;
      return_unit)
end
