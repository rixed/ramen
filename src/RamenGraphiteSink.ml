(* Collector for graphite plain text protocol (as described in
 * https://graphite.readthedocs.io/en/latest/feeding-carbon.html).
 *
 * Graphite metrics are organized as a hierarchy (with added tags on top).
 * We make no attempt to map that hierarchy into several functions; instead,
 * like for collectd we merely queue all received metrics into a single
 * stream representing the type of a generic graphite metric and let
 * interested functions select what they want from it. This is both simpler
 * and more versatile. Also, it is somewhat required by the fact that all
 * metrics have to be received by a single listener.
 *)
open Batteries
open RamenLog
open RamenHelpers
open RamenTuple
open RamenNullable
open RamenConsts
module T = RamenTypes
module U = RamenUnits

type graphite_metric =
  RamenIp.t nullable (* sender *) *
  float (* recept time *) *
  float (* start *) *
  string (* metric *) *
  (string * string) array (* tags *) *
  float (* value *)

(* TODO: have pre-made common types such as
 * RamenTypes.string = { structure = TString ; nullable = false } ... *)
let typ =
  T.(make (TRecord [|
    "sender",
      { structure = TIp ; nullable = true ; units = None ;
        doc = "Where we received this metric from." ; aggr = None } ;
    "receipt_time",
      { structure = TFloat ; nullable = false ;
        units = Some U.seconds_since_epoch ;
        doc = "When this metric has been received." ; aggr = None } ;
    "start",
      { structure = TFloat ; nullable = false ;
        units = Some U.seconds_since_epoch ;
        doc = "Event time." ; aggr = None } ;
    "metric",
      { structure = TString ; nullable = false ; units = None ;
        doc = "The graphite metric path." ; aggr = None } ;
    "tags",
      { structure = TList {
          structure =
            (let s = T.{ structure = TString ; nullable = false ;
                         units = None ; doc = "" ; aggr = None } in
            TTuple [| s ; s |]) ;
          nullable = false ; units = None ; doc = "" ; aggr = None } ;
        nullable = false ; units = None ; doc = "Accompanying tags." ;
        aggr = None } ;
    "value",
      { structure = TFloat ; nullable = false ; units = None ;
        doc = "The metric value." ; aggr = None } |]))

let event_time =
  let open RamenEventTime in
  Some ((RamenName.field_of_string "start", ref OutputField, 1.),
        DurationConst 0.)

let factors =
  [ RamenName.field_of_string "sender" ;
    RamenName.field_of_string "metric" ]

let print oc (_sender, _recept_time, start, metric, tags, value) =
  Printf.fprintf oc "%s%s%a %s %s"
    metric
    (if tags <> [||] then ";" else "")
    (Array.print ~first:"" ~last:"" ~sep:";" (fun oc (n,v) ->
      Printf.fprintf oc "%s=%s" n v)) tags
    (nice_string_of_float value)
    (nice_string_of_float start)

let to_string m =
  Printf.sprintf2 "%a" print m

(*$inject
   open Batteries
   open RamenNullable *)

(*$= to_string & ~printer:identity
  "foo.bar 42 123.12" \
    (to_string (Null, 0., 123.12, "foo.bar", [||], 42.))
  "foo;tag1=val1;tag2=val2 0.1 123.12" \
    (to_string (Null, 0., 123.12, "foo", \
                [| "tag1", "val1"; "tag2", "val2" |], 0.1))
*)

let parse ?sender ~recept_time line =
  let parse_err () =
    Printf.sprintf "Cannot parse %S as graphite" line |>
    invalid_arg in
  let s2f f =
    try float_of_string f with _ -> parse_err () in
  let metric, value, start =
    match String.split_on_char ' ' line with
    | [ metric ; value ; start ] -> metric, s2f value, s2f start
    | [ metric ; value ] -> metric, s2f value, recept_time
    | _ -> parse_err () in
  let metric, tags =
    match String.split_on_char ';' metric with
    | [] -> parse_err ()
    | metric :: tags ->
        metric,
        List.enum tags /@
        (fun t ->
          try String.split ~by:"=" t
          with Not_found -> parse_err ()) |>
        Array.of_enum in
  nullable_of_option sender, recept_time, start, metric, tags, value

(*$= parse & ~printer:to_string
  (Null, 1., 123.12, "foo.bar", [||], 42.) \
    (parse ~recept_time:1. "foo.bar 42 123.12")
  (Null, 1., 123.12, "foo", [| "tag1","val1"; "tag2", "val2" |], 0.1) \
    (parse ~recept_time:1. "foo;tag1=val1;tag2=val2 0.1 123.12")
  (Null, 1.23, 1.23, "foo.bar", [||], 42.) \
    (parse ~recept_time:1.23 "foo.bar 42")
*)

let collector ~inet_addr ~port ?while_ k =
  let lines_of_string s =
    (String.split_on_char '\n' s |> List.enum) // ((<>) "")
  in
  let serve ?sender buffer recv_len =
    let sender = Option.map RamenIp.of_unix_addr sender in
    !logger.debug "Received a graphite UDP datagram from %a"
      (Option.print RamenIp.print) sender ;
    let recept_time = Unix.gettimeofday () in
    match
      (Bytes.sub_string buffer 0 recv_len |>
       lines_of_string) /@
      String.trim /@
      parse ?sender ~recept_time with
    | exception e ->
        print_exception ~what:"Converting graphite plain text into tuple" e
    | tuples ->
        Enum.iter k tuples
  in
  udp_server ~buffer_size:60000 ~inet_addr ~port ?while_ serve
