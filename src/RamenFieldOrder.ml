open Batteries

module DT = DessserTypes
module N = RamenName

let rec_field_cmp (n1, _) (n2, _) =
  String.compare n1 n2

let rec order_rec_fields mn =
  let rec order_value_type = function
    | DT.Rec mns ->
        let mns = Array.copy mns in
        Array.fast_sort rec_field_cmp mns ;
        DT.Rec (Array.map (fun (name, mn) -> name, order_rec_fields mn) mns)
    | Tup mns ->
        DT.Tup (Array.map order_rec_fields mns)
    | Vec (dim, mn) ->
        DT.Vec (dim, order_rec_fields mn)
    | Lst mn ->
        DT.Lst (order_rec_fields mn)
    | Sum mns ->
        DT.Sum (Array.map (fun (name, mn) -> name, order_rec_fields mn) mns)
    | Usr ut ->
        DT.Usr { ut with def = order_value_type ut.def }
    | mn -> mn in
  { mn with vtyp = order_value_type mn.vtyp }

let rec are_rec_fields_ordered mn =
  let rec aux = function
    | DT.Rec mns ->
        DessserTools.array_for_alli (fun i (_, mn) ->
          are_rec_fields_ordered mn &&
          (i = 0 || rec_field_cmp mns.(i-1) mns.(i)< 0)
        ) mns
    | Tup mns ->
        Array.for_all are_rec_fields_ordered mns
    | Vec (_, mn) | Lst mn ->
        are_rec_fields_ordered mn
    | Sum mns ->
        Array.for_all (fun (_, mn) -> are_rec_fields_ordered mn) mns
    | Usr ut ->
        aux ut.def
    | _ ->
        true in
  aux mn.DT.vtyp

let check_rec_fields_ordered mn =
  if not (are_rec_fields_ordered mn) then
    Printf.sprintf2
      "RingBuffer can only serialize/deserialize records which \
       fields are sorted (had: %a)"
      DT.print_maybe_nullable mn |>
    failwith

let order_tuple tup =
  let cmp ft1 ft2 =
    rec_field_cmp ((ft1.RamenTuple.name :> string), ())
                  ((ft2.RamenTuple.name :> string), ()) in
  List.map (fun ft ->
    RamenTuple.{ ft with typ = order_rec_fields ft.typ }
  ) tup |>
  List.fast_sort cmp
