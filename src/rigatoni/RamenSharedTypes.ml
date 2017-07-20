(* TNum is not an actual type used by any value, but it's used as a default
 * type for numeric operands that can be "promoted" to any other numerical
 * type *)
type scalar_typ =
  | TNull | TFloat | TString | TBool | TNum
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128 [@@ppp PPP_JSON]

type field_typ =
  { typ_name : string ; nullable : bool ; typ : scalar_typ } [@@ppp PPP_JSON]

type field_typ_arr = field_typ array [@@ppp PPP_JSON]

type expr_type_info =
  { name_info : string ;
    nullable_info : bool option ;
    typ_info : scalar_typ option } [@@ppp PPP_JSON]

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

module Node =
struct
  type info =
    (* I'd like to offer the AST but PPP still fails on recursive types :-( *)
    { mutable name : string ;
      mutable operation : string ;
      mutable parents : string list ;
      mutable children : string list ;
      type_of_operation : string option ;
      input_type : (int option * expr_type_info) list ;
      output_type : (int option * expr_type_info) list ;
      (* Info about the running process (if any) *)
      command : string option ;
      pid : int option ;
      in_tuple_count : int ;
      selected_tuple_count : int ;
      out_tuple_count : int ;
      group_count : int option ;
      cpu_time : float ;
      ram_usage : int } [@@ppp PPP_JSON]

  let empty =
    { name = "" ; operation = "" ; parents = [] ; children = [] ;
      type_of_operation = None ; input_type = [] ; output_type = [] ;
      command = None ; pid = None ;
      in_tuple_count = 0 ; selected_tuple_count = 0 ; out_tuple_count = 0 ;
      group_count = None ; cpu_time = 0. ; ram_usage = 0 }
end

type node_links =
  (* I'd like to offer the AST but PPP still fails on recursive types :-( *)
  { parents : string list ;
    children : string list } [@@ppp PPP_JSON]

type graph_info =
  { nodes : Node.info list ;
    status : graph_status [@ppp_default Edition] ;
    last_started : float option ;
    last_stopped : float option } [@@ppp PPP_JSON]

(* Commands/Answers related to export *)

type export_req =
  { since : int option ;
    max_results : int option } [@@ppp PPP_JSON]

let empty_export_req = { since = None ; max_results = None }

(* The answer is handcrafted since we want to map all numerical types into the
 * unique JS number type and want to avoid costly variant before each value.
 * But if it existed it would be something like this:

  type export_resp =
    { fields : field_typ array ;
      values : any_js_type array list }

 *)
