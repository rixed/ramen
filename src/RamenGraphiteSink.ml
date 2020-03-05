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
open RamenHelpersNoLog
open RamenHelpers
open RamenTypes
open RamenTuple
open RamenNullable
open RamenConsts
module N = RamenName

type graphite_metric =
  RamenIp.t nullable (* sender *) *
  float (* recept time *) *
  float (* start *) *
  string (* metric *) *
  (string * string) array (* tags *) *
  float (* value *)

(* TODO: have pre-made common types such as
 * RamenTypes.string = { structure = TString ; nullable = false } ... *)
let tuple_typ =
  [ { name = N.field "sender" ; typ = { structure = TIp ; nullable = true } ; units = None ; doc = "Where we received this metric from." ; aggr = None } ;
    { name = N.field "receipt_time" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ; doc = "When this metric has been received." ; aggr = None } ;
    { name = N.field "start" ; typ = { structure = TFloat ; nullable = false } ; units = Some RamenUnits.seconds_since_epoch ; doc = "Event time." ; aggr = None } ;
    { name = N.field "metric" ; typ = { structure = TString ; nullable = false } ; units = None ; doc = "The graphite metric path." ; aggr = None } ;
    { name = N.field "tags" ; typ = { structure = TList { structure = TTuple [| { structure = TString ; nullable = false } ; { structure = TString ; nullable = false } |] ; nullable = false } ; nullable = false } ; units = None ; doc = "Accompanying tags." ; aggr = None } ;
    { name = N.field "value" ; typ = { structure = TFloat ; nullable = false } ; units = None ; doc = "The metric value." ; aggr = None } ]

let event_time =
  let open RamenEventTime in
  Some ((N.field "start", ref OutputField, 1.),
        DurationConst 0.)

let factors =
  [ N.field "sender" ;
    N.field "metric" ]

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
    match string_split_on_char ' ' line with
    | [ metric ; value ; start ] -> metric, s2f value, s2f start
    | [ metric ; value ] -> metric, s2f value, recept_time
    | _ -> parse_err () in
  let metric, tags =
    match string_split_on_char ';' metric with
    | [] | [""] -> parse_err ()
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
    (string_split_on_char '\n' s |> List.enum) // ((<>) "")
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
  udp_server
    ~what:"graphite sink" ~buffer_size:60000 ~inet_addr ~port ?while_ serve
