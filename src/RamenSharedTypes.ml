(* For types used by RPCs, so that clients that wish to use them don't have
 * to link with HttpSrv. *)
open RamenSharedTypesJS
open Stdint

(* Scalar types *)

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

(* A "columnar" type, to help send large number of values to clients.
 * Exotic types not available to JSON are converting to string by the
 * server to help clients. *)
type export_column =
  (* TODO: round those to a given decimal length to save bandwidth? *)
  | AFloat of float array
  | AString of string array
  | ABool of bool array | AU8 of uint8 array
  | AU16 of uint16 array | AU32 of uint32 array
  | AU64 of uint64 array | AU128 of uint128 array
  | AI8 of int8 array | AI16 of int16 array
  | AI32 of int32 array | AI64 of int64 array
  | AI128 of int128 array | ANull of int (* length of the array! *)
  | AEth of string array
  | AIpv4 of string array | AIpv6 of string array
  | ACidrv4 of string array | ACidrv6 of string array
  [@@ppp PPP_JSON]

let type_of_column = function
  | AFloat _ -> TFloat | AString _ -> TString | ABool _ -> TBool
  | AU8 _ -> TU8 | AU16 _ -> TU16 | AU32 _ -> TU32 | AU64 _ -> TU64
  | AU128 _ -> TU128 | AI8 _ -> TI8 | AI16 _ -> TI16 | AI32 _ -> TI32
  | AI64 _ -> TI64 | AI128 _ -> TI128 | ANull _ -> TNull
  | AEth _ -> TEth | AIpv4 _ -> TIpv4 | AIpv6 _ -> TIpv6
  | ACidrv4 _ -> TCidrv4 | ACidrv6 _ -> TCidrv6

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

(* Funcs  / Programs / Graphs *)

module Info =
struct
  module Func =
  struct
    type definition =
      { name : string ; operation : string } [@@ppp PPP_JSON]

    type worker_stats =
      { time : float ;
        in_tuple_count : int option ;
        selected_tuple_count : int option ;
        out_tuple_count : int option ;
        group_count : int option ;
        cpu_time : float ;
        ram_usage : int ;
        in_sleep : float option ;
        out_sleep : float option ;
        in_bytes : int option ;
        out_bytes : int option } [@@ppp PPP_JSON]

    type info =
      (* I'd like to offer the AST but PPP still fails on recursive types :-( *)
      { definition : definition ;
        exporting : bool ;
        input_type : expr_type_info list ;
        output_type : expr_type_info list ;
        (* fq names of parents/children *)
        parents : string list ;
        children : string list ;
        (* Info about the running process (if any) *)
        signature : string option ;
        pid : int option ;
        stats : worker_stats } [@@ppp PPP_JSON]
  end

  module Program =
  struct
    type status = RamenSharedTypesJS.program_status [@@ppp PPP_JSON]

    let string_of_status = function
      | Edition reason -> "Edition ("^ reason ^")"
      | Compiling -> "Compiling"
      | Compiled -> "Compiled"
      | Running -> "Running"

    type info =
      { name : string ;
        program : string ;
        operations : Func.info list ;
        status : status [@ppp_default (RamenSharedTypesJS.Edition "")] ;
        last_started : float option ;
        last_stopped : float option } [@@ppp PPP_JSON]
  end
end

type get_graph_resp = Info.Program.info list [@@ppp PPP_JSON]

type put_program_req =
  { (* Name of the program. If this program already exists then it is
     * replaced. *)
    name : string ;
    (* If this program is already running stop it then restart it: *)
    ok_if_running : bool [@ppp_default false] ;
    program : string } [@@ppp PPP_JSON]

(* Commands/Answers related to export *)

type export_req =
  { since : int option [@ppp_default None] ;
    (* Negative values for the last N *)
    max_results : int option ;
    (* If there are no results at all currently available, wait up to
     * that many seconds: *)
    wait_up_to : float [@ppp_default 0.] } [@@ppp PPP_JSON]

let empty_export_req =
  { since = None ; max_results = None ; wait_up_to = 0. }

(* Send values column by column in order to avoid sending a type variant for
 * each and every value. *)

type export_resp =
  { first: int ;
    columns : (string * bool array option * export_column) list }
    [@@ppp PPP_JSON]

(* Autocompletion of names: *)

(* TODO: exporting : bool option ; temporary : bool option *)
type complete_func_req =
  { prefix : string ; only_exporting : bool } [@@ppp PPP_JSON] [@@ppp_extensible]

type complete_field_req =
  { operation : string ; prefix : string } [@@ppp PPP_JSON] [@@ppp_extensible]

type complete_resp = string list [@@ppp PPP_JSON]

(* Time series retrieval: *)

type timeserie_spec = Predefined of { operation : string ; data_field : string }
                    (* If select_x is not given we will reuse the parent event
                     * configuration *)
                    | NewTempFunc of { select_x : string [@ppp_default ""] ;
                                       select_y : string ;
                                       from : string [@ppp_default ""] ;
                                       where : string [@ppp_default ""] }
                    [@@ppp PPP_JSON]

type timeserie_req =
  { id : string ;
    consolidation : string [@ppp_default "avg"] ;
    spec : timeserie_spec } [@@ppp PPP_JSON] [@@ppp_extensible]

type timeseries_req =
  { since : float ; (* [since] and [until] are in seconds *)
    until : float ;
    max_data_points : int ; (* FIXME: should be optional *)
    timeseries : timeserie_req list } [@@ppp PPP_JSON] [@@ppp_extensible]

type timeserie_resp =
  { id : string ;
    times : float array ; (* in seconds *)
    values : float option array } [@@ppp PPP_JSON]

type timeseries_resp = timeserie_resp list [@@ppp PPP_JSON]

type time_range_resp = RamenSharedTypesJS.time_range [@@ppp PPP_JSON]
