(* Representation of the configuration as a graph *)

open Batteries

module Node =
struct
  type setting = Debug | SayHello (* for testing *) [@@ppp PPP_OCaml]

  (* More interesting behavioral changes would be:
   *
   * - Instead of the normal operation, forward all events to some socket or to
   * some on-disc MQ, with or without sharding.
   *
   * - The operation never returns the function from event to unit but instead
   * read events from an external source (socket or disc MQ.
   *)

  type t =
    { name : string ;
      id : int ;
      mutable to_root : t list ;
      mutable settings : setting list (* from command line *) } (*[@@ppp PPP_Ocaml]*)

  let t_ppp = PPP_OCaml.(record (
    field "id" int <->
    field "name" string <->
    field "settings" (list setting_ppp)) >>:
    ((fun n -> Some (Some n.id, Some n.name), Some n.settings),
     (fun _ -> failwith "You cannot scan a node into existence")))

  let make name id =
    { name ; to_root = [] ; id ; settings = [] }

  let long_name t =
    "/"^ List.fold_left (fun s n -> n.name ^(if s = "" then "" else "/"^ s)) "" (t::t.to_root)
end

module Pipe =
struct
  type t =
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
    Alarm.every 10 (fun () ->
      Printf.printf "Node %S (#%d) received %d events\n"
        node.Node.name node.Node.id !count) ;
    fun e ->
      incr count ;
      op e

  let say_hello_op_wrapper node op =
    Printf.printf "Hello from node %S!\n" node.Node.name ;
    op

  let wrap_op node op =
    let open Node in
    List.fold_left (fun wrapper -> function
        | Debug -> debug_op_wrapper node wrapper
        | SayHello -> say_hello_op_wrapper node wrapper
      ) op node.settings
end
