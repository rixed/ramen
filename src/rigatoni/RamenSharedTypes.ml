(* TNum is not an actual type used by any value, but it's used as a default
 * type for numeric operands that can be "promoted" to any other numerical
 * type *)
type scalar = TNull | TFloat | TString | TBool | TNum
            | TU8 | TU16 | TU32 | TU64 | TU128
            | TI8 | TI16 | TI32 | TI64 | TI128 [@@ppp PPP_JSON]

type expr_type_info =
  { name : string ;
    nullable : bool option ;
    typ : scalar option } [@@ppp PPP_JSON]

type graph_status = Edition | Compiled | Running [@@ppp PPP_JSON]

type make_node =
  { (* The input type of this node is any tuple source with at least all the
     * field mentioned in the "in" tuple of its operation. *)
    operation : string ; (* description of what this node does in the DSL defined in Lang.ml *)
    (* Fine tuning info about the size of in/out ring buffers etc. *)
    input_ring_size : int option [@ppp_default None] ;
    output_ring_size : int option [@ppp_default None] } [@@ppp PPP_JSON]

(*$= make_node_ppp & ~printer:(PPP.to_string make_node_ppp)
  { operation = "test" ;\
    input_ring_size = None ;\
    output_ring_size = None }\
    (PPP.of_string_exc make_node_ppp "{\"operation\":\"test\"}")

  { operation = "op" ;\
    input_ring_size = Some 42 ;\
    output_ring_size = None }\
    (PPP.of_string_exc make_node_ppp "{\"operation\":\"op\", \"input_ring_size\":42}")
*)

type node_info =
  (* I'd like to offer the AST but PPP still fails on recursive types :-( *)
  { mutable name : string ;
    mutable operation : string ;
    mutable parents : string list ;
    mutable children : string list ;
    type_of_operation : string option ;
    command : string option ;
    pid : int option ;
    input_type : (int option * expr_type_info) list ;
    output_type : (int option * expr_type_info) list } [@@ppp PPP_JSON]

type node_links =
  (* I'd like to offer the AST but PPP still fails on recursive types :-( *)
  { parents : string list ;
    children : string list } [@@ppp PPP_JSON]

type graph_info =
  { nodes : node_info list ;
    links : (string * string) list ;
    status : graph_status } [@@ppp PPP_JSON]
