(* This implementation of the configuration actually does the thing, if
 * only in a straightforward way. *)
open Batteries
open Model

module Impl (Conf : sig val graph : Graph.t end) :
  Engine.S with type ('i, 'k) result = 'i -> unit =
struct
  type ('i, 'k) result = 'i -> unit

  let connect ?id ks op =
    let id = Option.get id in (* If this fail it's because you forgot AddId *)
    let node = Graph.lookup_node Conf.graph id in

    (* This is where we wrap output functions into whatever, or start another
     * reader for events that also call our ops. So for instance we will have
     * to read an 'i. Marshal or Obj.magic. Or...?  Of course, since the
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
    let output e = List.iter (fun k -> k e) ks in
    Setting.wrap_op node (op output)

  let replicate ?name ?id ks =
    ignore name ;
    connect ?id ks (fun output e -> output e)

  let filter ?name ?id ~by ks =
    ignore name ;
    connect ?id ks (fun output e -> if by e then output e)

  let on_change ?name ?id ks =
    ignore name ;
    let prev = ref None in
    connect ?id ks (fun output e ->
      match !prev with
      | Some e' when e' = e -> ()
      | _ -> prev := Some e ; output e)

  let aggregate ?name ?id ~key_of_event ~make_aggregate
                ~aggregate ?is_complete ?timeout_sec ?timeout_events ks =
    ignore name ;
    let h = Hashtbl.create 701 in
    let event_count = ref 0 in
    connect ?id ks (fun output e ->
      incr event_count ;
      let k = key_of_event e in
      (match Hashtbl.find h k with
      | exception Not_found ->
        Hashtbl.add h k (!Alarm.now, !event_count, make_aggregate e)
      | _, _, a ->
        aggregate a e ;
        BatOption.may (fun ic -> if ic a then output a) is_complete
        ) ;
      (* haha lol: TODO a heap of timeouts *)
      Hashtbl.filter_inplace (fun (t, c, a) ->
          if BatOption.map_default (fun ts -> !Alarm.now -. t >= ts) false timeout_sec ||
             BatOption.map_default (fun tc -> !event_count - c >= tc) false timeout_events
          then (
            output a ;
            false
          ) else true
        ) h)

  let sliding_window ?name ?id ~cmp ~is_complete ks =
    ignore name ;
    let past_events = ref [] in
    connect ?id ks (fun output e ->
      past_events := List.fast_sort cmp (e :: !past_events) ;
      if is_complete !past_events then (
        output !past_events ;
        past_events := List.tl !past_events
      ))

  let all ?name ?id ~cond ks =
    ignore name ;
    connect ?id ks (fun output es ->
      output (List.for_all cond es))

  let alert ?name ?id ~team ~title ~text () =
    ignore name ;
    connect ?id [] (fun _output -> function
      | true ->
        Printf.printf "To: %s\nSubject: %s\n\n%s\n%!" team title (text 42)
      | false ->
        Printf.printf "To: %s\nSubject: %s\n\nBack to Normal\n%!" team title)

  let save ?name ?id ~retention () =
    ignore name ;
    connect ?id [] (fun _output e ->
      ignore retention ; ignore e (* TODO *))
end
