(* Representation of the configuration as a graph *)
module Node =
struct
  type setting = Debug
               | SayHello (* for testing *)
               | ReadFile of string [@@ppp PPP_OCaml] (* TODO: ReadDir *)

  type t =
    { name : string ;
      id : int ;
      ppp_descr : string option ;
      mutable to_root : t list ; [@ppp_ignore []]
      mutable settings : setting list } [@@ppp PPP_OCaml]

  let make ?ppp name id =
    { name ; id ;
      ppp_descr = BatOption.map (fun p -> p.PPP.descr) ppp ;
      to_root = [] ; settings = [] }

  let long_name t =
    "/"^ List.fold_left (fun s n -> n.name ^(if s = "" then "" else "/"^ s)) "" (t::t.to_root)
end

module Pipe =
struct
  type t =
    (* TODO: a [@ppp_force expr to force the ppp to be used, so we can
     * defined a Node.t_ppp_terse and use it for the nodes here *)
    { from_node : Node.t ;
      to_node : Node.t } [@@ppp PPP_OCaml]

  let make from_node to_node =
    { from_node ; to_node }
end

(* Must be acyclic *)
module Graph =
struct
  type t =
    { name : string ;
      nodes: Node.t list ;
      pipes: Pipe.t list ;
      roots: Node.t list } [@@ppp PPP_OCaml]

  let empty name =
    { name ; nodes = [] ; pipes = [] ; roots = [] }

  let lookup_pipe t from_node to_node =
    List.find (fun p ->
        p.Pipe.from_node = from_node && p.Pipe.to_node = to_node
      ) t.pipes

  let lookup_node t id =
    List.find (fun n -> n.Node.id = id) t.nodes

  let add_node g ?from_node node =
    match from_node with
    | Some n ->
      node.Node.to_root <- n :: n.Node.to_root ;
      { g with nodes = node :: g.nodes ;
               pipes = Pipe.make n node :: g.pipes }
    | None -> { g with nodes = node :: g.nodes ;
                       roots = node :: g.roots }
end

module Setting =
struct
  type t = Node.setting
  let t_ppp = Node.setting_ppp

  (* Wrappers for node operation (op is the input of a node, a function from
   * event to unit) *)

  let debug_op_wrapper node op =
    (* as a demo, count the events *)
    let count = ref 0 in
    let last_count = ref 0 in
    let last_time = ref !Alarm.now in
    Alarm.every 0.3 (fun () ->
      let now = !Alarm.now in
      let rate = float_of_int (!count - !last_count) /. (now -. !last_time) in
      Printf.printf "Node %S (#%d), %d events, +%g events/sec\n%!"
        node.Node.name node.Node.id !count rate ;
      last_time := now ;
      last_count := !count) ;
    fun e ->
      incr count ;
      op e

  let say_hello_op_wrapper node op =
    Printf.printf "Hello from node %S!\n" node.Node.name ;
    op

  (* We could implement both read and write as the same behavior like this:
   * - If the node receives some event then it would be written in that file,
   *   and the chain of operations stops.
   * - In addition to this, spawn a thread that waits for that file to appear
   *   and then read from it.
   * Assuming the instance that's intended to read the file is never sent any
   * event and the file is not already visible from the node intended to write
   * then it would work. This is a bit too fragile though and I'd rather have
   * two distinct behaviors for that.
   * Here only the reading part is implemented because that's the one we need
   * ATM. Leaving the write counterpart for later.
   *
   * Notice that we could start reading right now because our callees have been
   * initialized already (operation tree is build bottom-up). But we'd rather
   * have all IO starts at once so that we do not have plenty of useless Lwt.t
   * to carry around until the IO party gets started. *)
  let read_file_op_wrapper file ppp node op =
    IO.(register_file_reader file ppp op) ;
    (* We still need a function that accept events, but it should not be
     * called: *)
    fun _e ->
      Printf.eprintf "Op for node#%d called from above while it has been set \
                      to read file %S. WTF?\n%!" node.Node.id file ;
      Lwt.return_unit


  exception MissingSerializer of Node.t
  let () = Printexc.register_printer (function
    | MissingSerializer node ->
      Some (Printf.sprintf "Serializer must be specified for node %d"
                           node.Node.id)
    | _ -> None)

  let required_ppp node = function
    | Some x -> x
    | None -> raise (MissingSerializer node)

  let wrap_op ?ppp node op =
    ignore ppp (* for now *) ;
    let open Node in
    List.fold_left (fun wrapper -> function
        | Debug -> debug_op_wrapper node wrapper
        | SayHello -> say_hello_op_wrapper node wrapper
        | ReadFile file ->
          let ppp = required_ppp node ppp in
          read_file_op_wrapper file ppp node wrapper
      ) op node.settings
end
