(* The primitive operators a configuration is build from *)

module type S =
sig
  (* All the interesting functions are returning an abstract type which depends
   * on the implementation at hand (can be an event processor, a pretty printer,
   * a choreographer...) For generality this type is parameterized over the
   * input event type 'e (*and output event type 'o*) and whatever output the
   * continuations: 'k. *)
  type ('e, 'k) result
  (* in general that type will be: some_input -> some_output.
   * For instance, for the actual implementation that will be: 'e -> unit *)

  (* Discard the event. *)
  val discard:
    ?name:string -> ?id:int -> ?ppp:('e PPP.t) ->
    unit ->
    ('e, 'k) result
  
  (* Duplicate the event as is. Useful for root. *)
  val replicate:
    ?name:string -> ?id:int -> ?ppp:('e PPP.t) ->
    ('e, 'k) result list ->
    ('e, 'k) result

  (* Convert the event into 0, 1 or more other events. Useful for map/filter *)
  val convert:
    ?name:string -> ?id:int -> ?ppp:('e PPP.t) ->
    f:('e -> 'f list) ->
    ('f, 'k) result list -> (* continuations take 'f and output 'k *)
    ('e, 'k) result

  (* Drop all incoming events but the ones which pass the condition. *)
  val filter:
    ?name:string -> ?id:int -> ?ppp:('e PPP.t) ->
    by:('e -> bool) -> (* the condition to be in *)
    ('e, 'k) result list ->
    ('e, 'k) result

  (* Filter all values that are the same as previously received. *)
  val on_change:
    ?name:string -> ?id:int -> ?ppp:('e PPP.t) ->
    ('e, 'k) result list ->
    ('e, 'k) result

  (* Group events according to some key and for each group build a single value
   * by aggregating all events of this group. Issue the aggregates as they are
   * completed (or expired) *)
  val aggregate:
    ?name:string -> ?id:int -> ?ppp:('e PPP.t) ->
    key_of_event:('e -> 'key) -> (* group events by that key into aggregates *)
    make_aggregate:('e -> 'a) -> (* turn a single event into an aggregate *)
    (* add this event into the aggregate (in-place). *)
    aggregate:('a -> 'e -> unit) ->
    (* If true this aggregate is outputted. The second key is the max key that
     * has ever been received. Useful for time-outs. *)
    ?is_complete:('key -> 'a -> 'key -> bool) ->
    (* if set, timeout an aggregate after it's untouched for that long *)
    ?timeout_sec:float ->
    (* if set, timeout an aggregate after it's untouched for that many event
     * processed in this operator *)
    ?timeout_events:int ->
    ('a, 'k) result list ->
    ('e, 'k) result

  (* Buffers all events, order them according to the given comparator, and
   * keep only the last ones. Once the completion condition is met, output the
   * result (as a list of ordered events from newest to oldest) and drop the
   * oldest event. May fire again immediately if the condition still holds
   * true. *)
  val sliding_window:
    ?name:string -> ?id:int -> ?ppp:('e PPP.t) ->
    (* comparator used to order incoming events *)
    cmp:('e -> 'e -> int) ->
    is_complete:('e list -> bool) ->
    ('e list, 'k) result list ->
    ('e, 'k) result

  (* Boolean operator that takes lists of events and fires true if all pass
   * the given condition, or false otherwise. *)
  val all:
    ?name:string -> ?id:int -> ?ppp:('e list PPP.t) ->
    (* the condition each constituent of the list should pass *)
    cond:('e -> bool) ->
    (bool, 'k) result list ->
    ('e list, 'k) result

  (* This function is special as it does not fire any event but terminates a
   * stream with an alert. *)
  val alert:
    ?name:string -> ?id:int -> ?ppp:(bool PPP.t) ->
    ?importance:int -> (* 0 = most important *)
    team:string -> (* identify the team this alert is for *)
    title:string -> (* informative title. Must sound scary but not hopeless *)
    (* receive this alert identifier and output the body of the alert
     * message *)
    text:(int -> string) ->
    unit ->
    (bool, unit) result

  (* Another end point: save data to disk. *)
  val save:
    ?name:string -> ?id:int -> ?ppp:('e PPP.t) ->
    retention:int -> unit ->
    ('e, unit) result
end

(* The following functor automatically numbers every node.
 *
 * We need a way to identify any operation in the configuration once it's
 * been turned into machine instruction by the compiler. We use an optional
 * parameter [?id] that will count function instantiation.
 * Notice that those functions typically returns other functions. We do not
 * count how often those are called but how often they are created.
 *
 * So my processing the configuration with this functor we make sure that all
 * functions are numbered unequivocally. We also rely on the fact that the
 * evaluation of the configuration arguments will happen in the same order
 * regardless of the implementation module that it applies to, which seems to
 * be the case in practice if not in theory. *)

module AddId (M : S) : S with type ('i, 'k) result = ('i, 'k) M.result =
struct
  type ('i, 'k) result = ('i, 'k) M.result

  let get_id =
    (* This is important that this sequence is restarted for each functor
     * application so keep it here: *)
    let seq = ref ~-1 in
    function
      | None -> incr seq ; !seq
      (* must not happen unless nodes have been enumerated already: *)
      | Some _id -> assert false

  let discard ?name ?id ?ppp () =
    let id = get_id id in
    M.discard ?name ~id ?ppp ()

  let replicate ?name ?id ?ppp ks =
    let id = get_id id in
    M.replicate ?name ~id ?ppp ks

  let convert ?name ?id ?ppp ~f ks =
    let id = get_id id in
    M.convert ?name ~id ?ppp ~f ks

  let filter ?name ?id ?ppp ~by ks =
    let id = get_id id in
    M.filter ?name ~id ?ppp ~by ks

  let on_change ?name ?id ?ppp ks =
    let id = get_id id in
    M.on_change ?name ~id ?ppp ks

  let aggregate ?name ?id ?ppp ~key_of_event ~make_aggregate ~aggregate
                ?is_complete ?timeout_sec ?timeout_events ks =
    let id = get_id id in
    M.aggregate ?name ~id ?ppp ~key_of_event ~make_aggregate ~aggregate ?is_complete
                ?timeout_sec ?timeout_events ks

  let sliding_window ?name ?id ?ppp ~cmp ~is_complete ks =
    let id = get_id id in
    M.sliding_window ?name ~id ?ppp ~cmp ~is_complete ks

  let all ?name ?id ?ppp ~cond ks =
    let id = get_id id in
    M.all ?name ~id ?ppp ~cond ks

  let alert ?name ?id ?ppp ?importance ~team ~title ~text () =
    let id = get_id id in
    M.alert ?name ~id ?ppp ?importance ~team ~title ~text ()

  let save ?name ?id ?ppp ~retention () =
    let id = get_id id in
    M.save ?name ~id ?ppp ~retention ()
end
