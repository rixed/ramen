open Batteries

module DT = DessserTypes
module N = RamenName

let rec_field_cmp (n1, _) (n2, _) =
  String.compare n1 n2

let rec order_rec_fields mn =
  let rec order_value_type = function
    | DT.TRec mns ->
        let mns = Array.copy mns in
        Array.fast_sort rec_field_cmp mns ;
        DT.TRec (Array.map (fun (name, mn) -> name, order_rec_fields mn) mns)
    | TTup mns ->
        DT.TTup (Array.map order_rec_fields mns)
    | TVec (dim, mn) ->
        DT.TVec (dim, order_rec_fields mn)
    | TArr mn ->
        DT.TArr (order_rec_fields mn)
    | TSum mns ->
        DT.TSum (Array.map (fun (name, mn) -> name, order_rec_fields mn) mns)
    | TUsr ut ->
        DT.TUsr { ut with def = order_value_type ut.def }
    | mn -> mn in
  { mn with typ = order_value_type mn.typ }

let rec are_rec_fields_ordered mn =
  let rec aux = function
    | DT.TRec mns ->
        DessserTools.array_for_alli (fun i (_, mn) ->
          are_rec_fields_ordered mn &&
          (i = 0 || rec_field_cmp mns.(i-1) mns.(i)< 0)
        ) mns
    | TTup mns ->
        Array.for_all are_rec_fields_ordered mns
    | TVec (_, mn) | TArr mn ->
        are_rec_fields_ordered mn
    | TSum mns ->
        Array.for_all (fun (_, mn) -> are_rec_fields_ordered mn) mns
    | TUsr ut ->
        aux ut.def
    | _ ->
        true in
  aux mn.DT.typ

let check_rec_fields_ordered mn =
  if not (are_rec_fields_ordered mn) then
    Printf.sprintf2
      "RingBuffer can only serialize/deserialize records which \
       fields are sorted (had: %a)"
      DT.print_mn mn |>
    failwith

let order_tuple tup =
  let cmp ft1 ft2 =
    rec_field_cmp ((ft1.RamenTuple.name :> string), ())
                  ((ft2.RamenTuple.name :> string), ()) in
  List.map (fun ft ->
    RamenTuple.{ ft with typ = order_rec_fields ft.typ }
  ) tup |>
  List.fast_sort cmp
