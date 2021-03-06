open Batteries

module N = RamenName

include Event_time.DessserGen
module Field = Event_time_field.DessserGen

let string_of_field ((n : N.field), _, s) =
  let string_of_scale f = if f = 1. then "" else "*"^ string_of_float f in
  (n :> string) ^ string_of_scale s

let print oc (start_field, duration) =
  Printf.fprintf oc "EVENT STARTING AT %s AND %s"
    (string_of_field start_field)
    (match duration with
     (* FIXME: uses RamenExpr.print_duration: *)
     | DurationConst f -> "DURATION "^ string_of_float f
     | DurationField f -> "DURATION "^ string_of_field f
     | StopField f -> "STOPPING AT "^ string_of_field f)

(* Return the set of field names used for event time: *)
let required_fields (start_field, duration) =
  let outfields_of_field (field, source, _) =
    if source = Field.OutputField then N.SetOfFields.singleton field
                                  else N.SetOfFields.empty in
  let s = outfields_of_field start_field in
  match duration with
  | DurationConst _ -> s
  | DurationField f | StopField f-> N.SetOfFields.union s (outfields_of_field f)
