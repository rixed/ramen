(* For types used by RPCs, so that clients that wish to use them don't have
 * to link with HttpSrv. *)
open Stdint

(* Scalar types *)

(* TNum is not an actual type used by any value, but it's used as a default
 * type for numeric operands that can be "promoted" to any other numerical
 * type. TAny is means to be replaced by an actual type during compilation:
 * all TAny types in an expression will be changed to a specific type that's
 * large enought to accommodate all the values at hand. *)
type scalar_typ =
  | TNull | TFloat | TString | TBool | TNum | TAny
  | TU8 | TU16 | TU32 | TU64 | TU128
  | TI8 | TI16 | TI32 | TI64 | TI128
  | TEth (* 48bits unsigned integers with funny notation *)
  | TIpv4 | TIpv6 | TCidrv4 | TCidrv6 [@@ppp PPP_JSON]

(* stdint types are implemented as custom blocks, therefore are slower than
 * ints.  But we do not care as we merely represents code here, we do not run
 * the operators. *)
type scalar_value =
  | VFloat of float | VString of string | VBool of bool
  | VU8 of uint8 | VU16 of uint16 | VU32 of uint32
  | VU64 of uint64 | VU128 of uint128
  | VI8 of int8 | VI16 of int16 | VI32 of int32
  | VI64 of int64 | VI128 of int128 | VNull
  | VEth of uint48
  | VIpv4 of uint32 | VIpv6 of uint128
  | VCidrv4 of (uint32 * int) | VCidrv6 of (uint128 * int) [@@ppp PPP_JSON]

let scalar_type_of = function
  | VFloat _ -> TFloat | VString _ -> TString | VBool _ -> TBool
  | VU8 _ -> TU8 | VU16 _ -> TU16 | VU32 _ -> TU32 | VU64 _ -> TU64
  | VU128 _ -> TU128 | VI8 _ -> TI8 | VI16 _ -> TI16 | VI32 _ -> TI32
  | VI64 _ -> TI64 | VI128 _ -> TI128 | VNull -> TNull
  | VEth _ -> TEth | VIpv4 _ -> TIpv4 | VIpv6 _ -> TIpv6
  | VCidrv4 _ -> TCidrv4 | VCidrv6 _ -> TCidrv6

(* A "columnar" type, to help store/send large number of values *)

type column =
  | AFloat of float array | AString of string array
  | ABool of bool array | AU8 of uint8 array
  | AU16 of uint16 array | AU32 of uint32 array
  | AU64 of uint64 array | AU128 of uint128 array
  | AI8 of int8 array | AI16 of int16 array
  | AI32 of int32 array | AI64 of int64 array
  | AI128 of int128 array | ANull of int (* length of the array! *)
  | AEth of uint48 array
  | AIpv4 of uint32 array | AIpv6 of uint128 array
  | ACidrv4 of (uint32 * int) array | ACidrv6 of (uint128 * int) array
  [@@ppp PPP_JSON]

let type_of_column = function
  | AFloat _ -> TFloat | AString _ -> TString | ABool _ -> TBool
  | AU8 _ -> TU8 | AU16 _ -> TU16 | AU32 _ -> TU32 | AU64 _ -> TU64
  | AU128 _ -> TU128 | AI8 _ -> TI8 | AI16 _ -> TI16 | AI32 _ -> TI32
  | AI64 _ -> TI64 | AI128 _ -> TI128 | ANull _ -> TNull
  | AEth _ -> TEth | AIpv4 _ -> TIpv4 | AIpv6 _ -> TIpv6
  | ACidrv4 _ -> TCidrv4 | ACidrv6 _ -> TCidrv6

let column_value_at n =
  let g a = Array.get a n in
  function
  | AFloat a -> VFloat (g a) | AString a -> VString (g a)
  | ABool a -> VBool (g a) | AU8 a -> VU8 (g a)
  | AU16 a -> VU16 (g a) | AU32 a -> VU32 (g a)
  | AU64 a -> VU64 (g a) | AU128 a -> VU128 (g a)
  | AI8 a -> VI8 (g a) | AI16 a -> VI16 (g a)
  | AI32 a -> VI32 (g a) | AI64 a -> VI64 (g a)
  | AI128 a -> VI128 (g a) | ANull _ -> VNull
  | AEth a -> VEth (g a)
  | AIpv4 a -> VIpv4 (g a) | AIpv6 a -> VIpv6 (g a)
  | ACidrv4 a -> VCidrv4 (g a) | ACidrv6 a -> VCidrv6 (g a)

type column_mapper =
  { f : 'a. 'a array -> 'a array ;
    null : int -> int }

let column_map m = function
  | AFloat a -> AFloat (m.f a) | AString a -> AString (m.f a)
  | ABool a -> ABool (m.f a) | AU8 a -> AU8 (m.f a) | AU16 a -> AU16 (m.f a)
  | AU32 a -> AU32 (m.f a) | AU64 a -> AU64 (m.f a) | AU128 a -> AU128 (m.f a)
  | AI8 a -> AI8 (m.f a) | AI16 a -> AI16 (m.f a) | AI32 a -> AI32 (m.f a)
  | AI64 a -> AI64 (m.f a) | AI128 a -> AI128 (m.f a)
  | ANull l -> ANull (m.null l) | AEth a -> AEth (m.f a)
  | AIpv4 a -> AIpv4 (m.f a) | AIpv6 a -> AIpv6 (m.f a)
  | ACidrv4 a -> ACidrv4 (m.f a) | ACidrv6 a -> ACidrv6 (m.f a)

let column_length =
  let al = Array.length in function
  | AFloat a -> al a | AString a -> al a | ABool a -> al a | AU8 a -> al a
  | AU16 a -> al a | AU32 a -> al a | AU64 a -> al a | AU128 a -> al a
  | AI8 a -> al a | AI16 a -> al a | AI32 a -> al a | AI64 a -> al a
  | AI128 a -> al a | ANull l -> l | AEth a -> al a | AIpv4 a -> al a
  | AIpv6 a -> al a | ACidrv4 a -> al a | ACidrv6 a -> al a

(* Tuple types *)

type field_typ =
  { typ_name : string ; nullable : bool ; typ : scalar_typ } [@@ppp PPP_JSON]

type field_typ_arr = field_typ array [@@ppp PPP_JSON]

type expr_type_info =
  { name_info : string ;
    nullable_info : bool option ;
    typ_info : scalar_typ option } [@@ppp PPP_JSON]

(* Nodes  / Layers / Graphs *)

module Node =
struct
  type definition =
    { name : string ; operation : string } [@@ppp PPP_JSON]

  type info =
    (* I'd like to offer the AST but PPP still fails on recursive types :-( *)
    { definition : definition ;
      type_of_operation : string option ;
      exporting : bool ;
      input_type : expr_type_info list ;
      output_type : expr_type_info list ;
      (* fq names of parents/children *)
      parents : string list ;
      children : string list ;
      (* Info about the running process (if any) *)
      signature : string option ;
      pid : int option ;
      in_tuple_count : int ;
      selected_tuple_count : int ;
      out_tuple_count : int ;
      group_count : int option ;
      cpu_time : float ;
      ram_usage : int } [@@ppp PPP_JSON]
end

module Layer =
struct
  type status = RamenSharedTypesJS.layer_status [@@ppp PPP_JSON]

  type info =
    { name : string ;
      nodes : Node.info list ;
      status : status [@ppp_default RamenSharedTypesJS.Edition] ;
      last_started : float option ;
      last_stopped : float option } [@@ppp PPP_JSON]
end

type get_graph_resp = Layer.info list [@@ppp PPP_JSON]

type put_layer_req =
  { name : string ;
    nodes : Node.definition list } [@@ppp PPP_JSON]

(* Commands/Answers related to export *)

type export_req =
  { since : int option [@ppp_default None] ;
    max_results : int option ;
    (* If there are no results at all currently available, wait up to
     * that many seconds: *)
    wait_up_to : float [@ppp_default 0.] } [@@ppp PPP_JSON]

let empty_export_req =
  { since = None ; max_results = None ; wait_up_to = 0. }

(* We send values column by column to limit sending a type variant
 * for each and every value *)

type export_resp =
  { first: int ;
    columns : (string * bool array option * column) list } [@@ppp PPP_JSON]

(* Autocompletion of names: *)

(* TODO: exporting : bool option ; temporary : bool option *)
type complete_node_req =
  { node_prefix : string ; only_exporting : bool } [@@ppp PPP_JSON] [@@ppp_extensible]

type complete_field_req =
  { node : string ; field_prefix : string } [@@ppp PPP_JSON] [@@ppp_extensible]

type complete_resp = string list [@@ppp PPP_JSON]

(* Time series retrieval: *)

type timeserie_spec = Predefined of { node : string ; data_field : string }
                    (* If select_x is not given we will reuse the parent event
                     * configuration *)
                    | NewTempNode of { select_x : string [@ppp_default ""] ;
                                       select_y : string ;
                                       from : string [@ppp_default ""] ;
                                       where : string [@ppp_default ""] }
                    [@@ppp PPP_JSON]

type timeserie_req =
  { id : string ;
    consolidation : string [@ppp_default "avg"] ;
    spec : timeserie_spec } [@@ppp PPP_JSON] [@@ppp_extensible]

type timeseries_req =
  { from : float ; (* from and to_ are in seconds *)
    to_ : float [@ppp_rename "to"] ;
    max_data_points : int ; (* FIXME: should be optional *)
    timeseries : timeserie_req list } [@@ppp PPP_JSON] [@@ppp_extensible]

type timeserie_resp =
  { id : string ;
    times : float array ; (* in seconds *)
    values : float option array } [@@ppp PPP_JSON]

type timeseries_resp = timeserie_resp list [@@ppp PPP_JSON]
