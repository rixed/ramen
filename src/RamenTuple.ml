(* This module keeps types and functions related to tuples.
 *
 * Tuples are what operations consume and produce.
 * Notice that we have a type to describe the type of a tuple (the type
 * and names of its fields) but no type to describe an actual tuple value.
 * That's because all tuple values appear only in generated code.
 *)
open Batteries
open RamenHelpersNoLog
module T = RamenTypes
module DT = DessserTypes
module N = RamenName

open Program_parameter.DessserGen
include Field_type.DessserGen

type field_typ = t

let eq_field_typ t1 t2 =
  t1.name = t2.name && t1.typ = t2.typ

(* Some "well known" type that we might need on the fly: *)
let seq_typ =
  { name = N.field "Seq" ;
    typ = DT.required TU64 ;
    units = Some RamenUnits.dimensionless ;
    doc = "Sequence number" ;
    aggr = None }

let start_typ =
  { name = N.field "Event start" ;
    typ = DT.optional TFloat ;
    units = Some RamenUnits.seconds_since_epoch ;
    doc = "Event start" ;
    aggr = Some "min" }

let stop_typ =
  { name = N.field "Event stop" ;
    typ = DT.optional TFloat ;
    units = Some RamenUnits.seconds_since_epoch ;
    doc = "Event stop" ;
    aggr = Some "max" }

(* TODO: have an array instead? *)
type typ = field_typ list

let rec eq_types t1s t2s =
  match t1s, t2s with
  | [], [] -> true
  | t1::t1s, t2::t2s ->
      eq_field_typ t1 t2 && eq_types t1s t2s
  | _ -> false

let print_field_typ oc field =
  Printf.fprintf oc "%a %a"
    N.field_print field.name
    DT.print_mn field.typ ;
  Option.may (RamenUnits.print oc) field.units

let print_typ oc =
  (List.print ~first:"(" ~last:")" ~sep:", "
    (fun oc t -> print_field_typ oc t)) oc

let print_typ_names oc =
  pretty_list_print (fun oc t ->
    String.print oc (N.field_color t.name)) oc

(* Params form a special tuple with fixed values: *)

type param = Program_parameter.DessserGen.t

let hash_of_params params =
  List.enum params /@
  (fun p -> p.ptyp.name, p.value) |>
  Hashtbl.of_enum

let print_param oc p =
  Printf.fprintf oc "%a=%a"
    N.field_print p.ptyp.name
    T.print p.value

let print_params oc =
  List.print (fun oc p -> print_param oc p) oc

let params_sort params =
  let param_compare p1 p2 =
    N.compare p1.ptyp.name p2.ptyp.name in
  List.fast_sort param_compare params

let params_find n = List.find (fun p -> p.ptyp.name = n)
let params_mem n = List.exists (fun p -> p.ptyp.name = n)

let print_params_names oc =
  print_typ_names oc % List.map (fun p -> p.ptyp) % params_sort

(* Same signature for different instances of the same program but changes
 * whenever the type of parameters change: *)

let type_signature =
  List.fold_left (fun s ft ->
    (if s = "" then "" else s ^ "_") ^
    (ft.name :> string) ^ ":" ^
    DT.mn_to_string ft.typ
  ) ""

let params_type_signature =
  type_signature % List.map (fun p -> p.ptyp) % params_sort

let params_signature params =
  params_sort params |>
  List.fold_left (fun s param ->
    (if s = "" then "" else s ^ "_") ^
    (param.ptyp.name :> string) ^ ":" ^
    DT.mn_to_string param.ptyp.typ ^ ":" ^
    (T.to_string param.value)
  ) ""

(* Override ps1 with values from ps2, ignoring the values of ps2 that are
 * not in ps1. Enlarge the values of ps2 as necessary: *)
let overwrite_params ps1 ps2 =
  List.map (fun p1 ->
    match assoc_array_find p1.ptyp.name ps2 with
    | exception Not_found -> p1
    | p2_val ->
        if p2_val = Raql_value.VNull then
          if p1.ptyp.typ.DT.nullable then
            { p1 with value = VNull }
          else
            Printf.sprintf2 "Parameter %a is not nullable so cannot \
                             be set to NULL"
              N.field_print p1.ptyp.name |>
            failwith
        else match T.enlarge_value p1.ptyp.typ.DT.typ p2_val with
          | exception Invalid_argument msg ->
              Printf.sprintf2 "Parameter %a of type %a can not be \
                               promoted into a %a: %s"
                N.field_print p1.ptyp.name
                DT.print (T.type_of_value p2_val)
                DT.print_mn p1.ptyp.typ
                msg |>
              failwith
          | value -> { p1 with value }
  ) ps1

module Parser =
struct
  open RamenParsing

  let default_aggr m =
    let m = "default aggregate" :: m in
    (
      strinGs "aggregate" -- blanks -- strinG "using" -- blanks -+
      identifier >>: fun aggr ->
        match String.lowercase aggr with
        (* Same list as in RamenTimeseries, + bitwise ops + "same": *)
        | "avg" | "sum" | "min" | "max"
        | "bitand" | "bitor" | "bitxor"
        | "same" as x -> x
        | x -> raise (Reject ("Unknown aggregation function "^ x))
    ) m

  let field m =
    let m = "field declaration" :: m in
    (
      non_keyword +- blanks ++ T.Parser.typ ++
      optional ~def:None (opt_blanks -+ some RamenUnits.Parser.p) ++
      optional ~def:"" (opt_blanks -+ quoted_string) ++
      optional ~def:None (opt_blanks -+ some default_aggr) >>:
      fun ((((name, typ), units), doc), aggr) ->
        let name = N.field name in
        { name ; typ ; units ; doc ; aggr }
    ) m
end

(* Turn an old-school RamenTuple.field_typ list into a record: *)
(* FIXME: obscoleted by O.ser_fields_to_record *)
let to_record t =
  DT.required
    (DT.TRec (
      List.map (fun ft -> (ft.name :> string), ft.typ) t |>
      Array.of_list))
