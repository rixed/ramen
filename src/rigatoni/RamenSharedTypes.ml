open Stdint

(* TNum is not an actual type used by any value, but it's used as a default
 * type for numeric operands that can be "promoted" to any other numerical
 * type *)
type scalar_typ =
  | TNull | TFloat | TString | TBool | TNum
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128 [@@ppp PPP_JSON]

(* stdint types are implemented as custom blocks, therefore are slower than
 * ints.  But we do not care as we merely represents code here, we do not run
 * the operators. *)
type scalar_value =
  | VFloat of float | VString of string | VBool of bool
  | VU8 of uint8 | VU16 of uint16 | VU32 of uint32
  | VU64 of uint64 | VU128 of uint128
  | VI8 of int8 | VI16 of int16 | VI32 of int32
  | VI64 of int64 | VI128 of int128 | VNull [@@ppp PPP_JSON]

let scalar_type_of = function
  | VFloat _ -> TFloat | VString _ -> TString | VBool _ -> TBool
  | VU8 _ -> TU8 | VU16 _ -> TU16 | VU32 _ -> TU32 | VU64 _ -> TU64
  | VU128 _ -> TU128 | VI8 _ -> TI8 | VI16 _ -> TI16 | VI32 _ -> TI32
  | VI64 _ -> TI64 | VI128 _ -> TI128 | VNull -> TNull

(* A "columnar" type, to help store/send large number of values *)
type scalar_column =
  | AFloat of float array | AString of string array
  | ABool of bool array | AU8 of uint8 array
  | AU16 of uint16 array | AU32 of uint32 array
  | AU64 of uint64 array | AU128 of uint128 array
  | AI8 of int8 array | AI16 of int16 array
  | AI32 of int32 array | AI64 of int64 array
  | AI128 of int128 array | ANull of int (* length of the array! *)
  [@@ppp PPP_JSON]

let scalar_type_of_column = function
  | AFloat _ -> TFloat | AString _ -> TString | ABool _ -> TBool
  | AU8 _ -> TU8 | AU16 _ -> TU16 | AU32 _ -> TU32 | AU64 _ -> TU64
  | AU128 _ -> TU128 | AI8 _ -> TI8 | AI16 _ -> TI16 | AI32 _ -> TI32
  | AI64 _ -> TI64 | AI128 _ -> TI128 | ANull _ -> TNull

let scalar_value_at n = function
  | AFloat a -> VFloat (Array.get a n) | AString a -> VString (Array.get a n)
  | ABool a -> VBool (Array.get a n) | AU8 a -> VU8 (Array.get a n)
  | AU16 a -> VU16 (Array.get a n) | AU32 a -> VU32 (Array.get a n)
  | AU64 a -> VU64 (Array.get a n) | AU128 a -> VU128 (Array.get a n)
  | AI8 a -> VI8 (Array.get a n) | AI16 a -> VI16 (Array.get a n)
  | AI32 a -> VI32 (Array.get a n) | AI64 a -> VI64 (Array.get a n)
  | AI128 a -> VI128 (Array.get a n) | ANull _ -> VNull

type column_mapper =
  { f : 'a. 'a array -> 'a array ;
    null : int -> int }

let scalar_column_map m = function
  | AFloat a -> AFloat (m.f a) | AString a -> AString (m.f a)
  | ABool a -> ABool (m.f a) | AU8 a -> AU8 (m.f a) | AU16 a -> AU16 (m.f a)
  | AU32 a -> AU32 (m.f a) | AU64 a -> AU64 (m.f a) | AU128 a -> AU128 (m.f a)
  | AI8 a -> AI8 (m.f a) | AI16 a -> AI16 (m.f a) | AI32 a -> AI32 (m.f a)
  | AI64 a -> AI64 (m.f a) | AI128 a -> AI128 (m.f a) | ANull l -> ANull (m.null l)

let scalar_column_length =
  let al = Array.length in function
  | AFloat a -> al a | AString a -> al a | ABool a -> al a | AU8 a -> al a
  | AU16 a -> al a | AU32 a -> al a | AU64 a -> al a | AU128 a -> al a
  | AI8 a -> al a | AI16 a -> al a | AI32 a -> al a | AI64 a -> al a
  | AI128 a -> al a | ANull l -> l

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

let empty_make_node =
  { operation = "" ; input_ring_size = None ; output_ring_size = None }

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
    max_results : int option ;
    (* If there are no results at all currently available, wait up to
     * that many seconds: *)
    wait_up_to : float option } [@@ppp PPP_JSON]

let empty_export_req =
  { since = None ; max_results = None ; wait_up_to = None }

(* We send values column by column to limit sending a type variant
 * for each and every value *)

type export_resp =
  { first: int ;
    columns : (string * bool * scalar_column) list } [@@ppp PPP_JSON]
