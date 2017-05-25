(* Configuration (as OCaml code) for a simple yet realistic usage.
 *
 * == Configuration is code ==
 *
 * Before reviewing this decision dreadful consequences, let's review the
 * reasons why we want the configuration to be actual code:
 *
 * 1. The configuration is not exactly a tree: sometime we might want a
 * descendent node to send some output to one of its parent, or a common child
 * to collect the output of several parents. Therefore we need to be able to
 * define once such a function and refer to it several times by name. Using
 * actual code gives this for free.
 *
 * 2. We want the configuration to be type-checked before runtime! As the
 * message types evolve along the operations we want to make sure that the
 * output of an operation matches the input of the next operation. Many event
 * processor would use a single map from string to string for all events (if
 * not for everything!) which is what we want to improve upon, both for
 * reliability and performance.  It would be possible to do type-checking
 * manually on the configuration but that would in all likelihood results in
 * very poor type system, while again OCaml does this for free.
 *
 * 3. We want to be able to assemble higher level functions from basic
 * primitives, to build libraries of such functions, and so on. Again, using a
 * real programming language provides all of this for free.
 *
 * This design decision has far reaching consequences. Since this is code
 * that's going to be computed in a single function (or the top-level input)
 * the compiler will hand us nothing but a pile of machine instructions. The
 * structure of the configuration is lost. Therefore we cannot that easily,
 * given a configuration, inspect it and decide for instance to run only half
 * of it and connect via a socket to another instance running the other half.
 * And should we manage to take that decision, it seems harder to actually
 * interfere with the internal working of the operations to swap direct
 * function calls for their remote counterparts.
 *
 * Here we would greatly benefit from a language that'd be both homoiconic
 * (code = data) and compile-time typed, which is a scarcely populated
 * intersection to say the least. The only language with an actual compiler
 * that I'm aware of in this narrow intersection is mercury. A declarative
 * language such as this Prolog variant is indeed an appealing option for a
 * configuration but doesn't match my original conception of an event processor
 * so I haven't considered it until now.  On another hand, this declarative
 * nature makes it harder to analyse the data flow, which is exactly why we
 * need homoiconicity for. Metaocaml also comes to mind but it's more geared
 * toward code generation than code inspection.
 *
 * The trick that will be used to solve this issue is OCaml module system,
 * which allows us to "run" the configuration with various implementations of
 * the base operations. One implementation will actually perform the operations
 * (the ExecuteEngine) while another one will merely build at runtime the graph
 * structure representing these operations (the ReifyEngine). The operations of
 * the execution-engine must be able to find their configuration in that graph,
 * and therefore be passed some kind of identifier, which raised unforeseen
 * difficulties and required some functorial shenanigans that will be
 * highlighted in due time.
 *
 * = Alternative 1: allow any code for ops but provide an opaque pipe to
 * connect them =
 *
 * We could allow any code rather than just code based a fixed set of
 * operations. Instead, to still get a chance to learn about the graph we could
 * provide a "pipe" abstraction that functions must use to call each others
 * (not unlike Riemann), and therefore we could change the implementation of
 * that pipe to change direct calls into remote functions if needed. Pros: no
 * need to rely solely on a fixed set of primitive functions, no need for the
 * multiple implementations of those primitive ops (one for the ExecuteEngine,
 * one for the ReifyEngine, one for AddId...). Cons: It is not that easy to get
 * the graph from the calls only (unless we force the user functions to take
 * not only event but also meta messages that must be forwarded to all possible
 * continuations regardless of the logic, but then it's not that simpler any
 * more.
 *
 * = Alternative 2: allow any code, give up the graph =
 *
 * We could give up this idea of building that graph of operations and merely
 * runs the stream processor blindly in a single program. Pros: much simpler,
 * also more similar to Riemann. Cons: Harder to distribute, harder to
 * transition from prototype to production bit by bit.
 *
 * == Distributed execution ==
 *
 * We want to be able to run a single configuration in a distributed fashion,
 * some part of it in some process and some other part in another one, possibly
 * on another machine. This partition of the configuration is called a
 * choreography. When passed a configuration the event processor (ep) can emit
 * a choreography for it. Conversely, when given a choreography and a name it
 * can run that part assigned to that name.  Details of that choreography can
 * also be overwritten via ep command line. As a consequence, we can begin with
 * a default choreography consisting on running everything in a single place
 * and rely solely on manual control at first.
 *
 * We do not want this distributed processing just for the crack but also to
 * help porting some operations into more efficient languages, smoothing the
 * transition between prototype and production.
 *
 * It goes without saying that no amount of compile-time type checks can
 * guarantee that events received from the network will have the expected type.
 * The unserializer is then in charge of building a valid event from a
 * string of bytes (or rejecting invalid ones). But we want to be able to
 * manually control when a node will perform a remote call rather than a direct
 * function call. Therefore, any primitive operation must be prepared to
 * unserialize events. But an event is an opaque data structure for most of the
 * primitive operations (a universal type; in other words primitive operations
 * accept any type of event).  Therefore we need either a universal
 * (un)serializer or we need to provide a specific one to each primitive
 * operation call. OCaml standard library comes with a universal serializer but
 * it is very specific to OCaml internal data representation and therefore not
 * a good fit for network communication with another instance implemented in
 * another language.  On another hand, passing a serializer to each operation
 * would be cumbersome.  A tradeof could be: make this extra parameter optional
 * (by default: use the universal one) and only allow offloading to another
 * implementation language the nodes provided with a custom serializer.
 *
 *
 * The test configuration below reads some netflow stream and aggregates it
 * with various time steps.  It also computes the traffic in between two
 * subnets of interest, stores it, and monitor that the traffic in between
 * those never falls below some minimum. It uses mainly primitive operations
 * for this for exposition but it is likely that a real world sample would make
 * use of mostly higher level functions.
 *
 * This configuration is compiled into a module that will be loaded at runtime
 * (so that the same binary can process different configurations). This
 * compiled module does *not* embed any implementation of the operations,
 * though. It must therefore be a functor of some implementation provided by
 * the event processor itself. Think of it as a shared object (.so file) with
 * unresolved dependencies for all the base operations.
 *
 * When given such a configuration for execution, the event processor binary
 * (ep) can:
 *
 * - Emmit a choreography for it (aka an execution plan), given a set of
 * constraints (number of machines, cpus and target memory to control the
 * tradeoff between RAM and disk, and alternate binary for some operations, to
 * help using several programming languages in the same configuration).  This
 * choreography partition the configuration into machines and further partition
 * each machine into instances.
 *
 * - Run a single instance of it according to some choreography. Opened sockets
 * will be printed on stdout.
 *
 * - Run all instances of a given machine. Execs all the necessary instances
 * and collect their opened sockets. This main process serves as a controller
 * and just listen to some standard port to answer queries about the EP, such
 * as the above socket information.
 *
 *
 * How to port into another programming language?
 *
 * 1. First, make sure the communication channel from one instance to another
 * is not bound to any language.
 *
 * 2. Then, in another language (say rust or C++), implement a library with the
 * implementation for that external channel plus some of the primitive
 * operations (say on-disk aggregation).
 *
 * 3. Then hardcode in this language an implementation of a subpart of the
 * engine (can be a single primitive operation, so that's reusable).
 *
 * 4. In the configuration provide custom serializers for the offloaded
 * operations
 *
 * 5. In the choreography force this subset of the configuration to be
 * implemented by an alternate binary.
 *
 * So we can start with rewriting what benefits the most from a more efficient
 * language for a specific part of the configuration.  And we can port the rest
 * later or rethink the strategy.
 *)
open Batteries
open EventTypes

(* Basically, a configuration must register a functor from some implementation
 * into a configuration.S, which is little more than a variable called
 * configurations and being a list of functions from event to unit, that will
 * be called repeatedly for all events.
 *
 * The implementation provide operations (filter, aggregate...) which all take
 * a optional name parameter (only used to give a less generic name to this
 * node in the graph) and then other named parameter depending on the operation,
 * many of which are functions taking the incoming event.
 *
 * Although events will be processed in the same order they are received by the
 * EP, care must generally be taken not to rely on strict event ordering. It is
 * worth noting though that the event processor internal notion of time, used
 * for timeouts, does not necessarily corresponds to event time. As far as the
 * EP is concerned, events are totally opaque data structures.
 *)

let db =
  let config_db = Sys.getenv "CONFIG_DB" in
  Conf_of_sqlite.make config_db

module Make : Configuration.MAKER = functor (Impl : Engine.S) ->
struct
  open Impl

  (* Alert if all events met the given condition: *)
  let alert_when_all_bytes ~cond ~name ~title ~text =
    all ~name ~cond
      [on_change
        ~name:("changed "^ name)
        [alert
           ~name:("alert on "^name)
           ~team:"network firefighters"
           ~title ~text ()]]

  let string_of_zone = function
    | Some z -> Printf.sprintf "%d" z
    | None -> Printf.sprintf "Any"

  (* FIXME: instead of checking for equality we should look for inclusion (which
   * requires us to have the whole zone tree *)
  let is_from_to_zone z1 z2 e =
    let open TCP_v29.UniDir in
    (* FIXME: should be zone _in_ z1 etc but will do for now *)
    Option.map_default ((=) e.zone_src) true z1 &&
    Option.map_default ((=) e.zone_dst) true z2

  (* Here we define a simple helper to define an alert on low traffic between
   * any two zones: *)
  let alert_on_volume ?min_bytes ?max_bytes ~duration z1 z2 =
     filter
       ~name:(Printf.sprintf "from zone %s to %s"
                (string_of_zone z1) (string_of_zone z2))
       ~by:(is_from_to_zone z1 z2)
       [aggregate
          (* In the aggregate we reuse clt for source and srv for dest.
           * TODO: define a more explicit and less error prone struct for
           * this aggregate? *)
          ~name:"minutely"
          ~key_of_event:(fun e -> TCP_v29.(to_minutes e.UniDir.start))
          ~make_aggregate:identity (* event = aggregate *)
          ~aggregate:
            (fun a (* the aggregate *) e (* the event *) ->
               let open TCP_v29.UniDir in
               a.start <- min a.start e.start ;
               a.stop <- max a.stop e.stop ;
               a.packets <- a.packets + e.packets ;
               a.bytes <- a.bytes + e.bytes)
          ~timeout_sec:30. ~timeout_events:1000
          [save
             ~name:"TODO: save"
             ~retention:(3600*24*30) () ;
           sliding_window
             ~name:(Printf.sprintf "%d mins sliding window" (to_minutes duration))
             ~cmp:(fun a1 a2 -> compare a1.TCP_v29.UniDir.start a2.TCP_v29.UniDir.start)
             ~is_complete:
               (fun lst ->
                  try (
                    (* Notice any missing segment will count as compliant *)
                    let oldest = (List.first lst).TCP_v29.UniDir.start
                    and newest = (List.last lst).TCP_v29.UniDir.stop in
                    newest >= oldest + duration
                  ) with Failure _ | Invalid_argument _ -> false)
             ((match min_bytes with
              | None -> []
              | Some min_bytes ->
                  let name = Printf.sprintf "all samples bellow %d" min_bytes
                  and title = Printf.sprintf "Too little traffic from zone %s to %s"
                                (string_of_zone z1) (string_of_zone z2)
                  and text id = Printf.sprintf
                                  "The traffic from zone %s to %s has sunk below \
                                   the configured minimum of %d \
                                   for the last %d minutes.\n\n\
                                   See https://event_proc.home.lan/show_alert?id=%d\n"
                                   (string_of_zone z1) (string_of_zone z2)
                                   min_bytes (to_minutes duration) id
                  and cond a = a.TCP_v29.UniDir.bytes < min_bytes
                  in
                  [alert_when_all_bytes ~cond ~name ~title ~text]) @
              (match max_bytes with
              | None -> []
              | Some max_bytes ->
                  let name = Printf.sprintf "all samples above %d" max_bytes
                  and title = Printf.sprintf "Too much traffic from zone %s to %s"
                                (string_of_zone z1) (string_of_zone z2)
                  and text id = Printf.sprintf
                                  "The traffic from zones %s to %s has raised above \
                                   the configured maximum of %d \
                                   for the last %d minutes.\n\n\
                                   See https://event_proc.home.lan/show_alert?id=%d\n"
                                   (string_of_zone z1) (string_of_zone z2)
                                   max_bytes (to_minutes duration) id
                  and cond a = a.TCP_v29.UniDir.bytes > max_bytes
                  in
                  [alert_when_all_bytes ~cond ~name ~title ~text]))]]

  type input = TCP_v29.t

  (* To make it easier to update the configuration at runtime, this is actually
   * a function.  The event processor will call this function to get the new
   * configuration and update (hopefully in a smart way) the configuration
   * that's currently running, if any.
   *
   * This is reloaded every time must_reload returns true. This function ough
   * to be fast since it's called very frequently.
   *)

(*
  let configuration () = replicate ~ppp:TCP_v29.of_csv_ppp [
    alert_on_volume ~min_bytes:50_000_000 ~duration:(of_minutes 10) 50 72 ;
    alert_on_volume ~min_bytes:50_000_000 ~duration:(of_minutes 10) 0 30
  ]

  let must_reload =
    let loaded = ref false in
    fun () ->
      if !loaded then false else (
        loaded := true ;
        true
      )
*)

  let configuration () =
    let open Conf_of_sqlite in
    let alert_of_conf conf =
      alert_on_volume ?min_bytes:conf.min_bytes ?max_bytes:conf.max_bytes
                      ~duration:(of_seconds_f conf.obs_window)
                      conf.source conf.dest
    in
    convert ~name:"to unidir volumes"
            ~ppp:TCP_v29.of_csv_ppp
            ~f:TCP_v29.UniDir.of_event
            (build_config alert_of_conf db)

  let must_reload () = Conf_of_sqlite.must_reload db
end

(* Programs cannot access dynamically loaded modules but they can access the
 * loading program, therefore plug-ins have to register somehow: *)

let () =
  Configuration.register "alerting (test)"
                         (module Make : Configuration.MAKER)
