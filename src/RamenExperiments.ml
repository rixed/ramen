open Batteries
open RamenHelpers
open RamenHelpersNoLog
open RamenLog
module Files = RamenFiles
module N = RamenName
module Versions = RamenVersions

module Variant =
struct
  type t =
    { name : string ; descr : string ;
      mutable share : float }

  let make ?share name descr =
    Option.may (fun s ->
      if s < 0. || s > 1. then invalid_arg "variant share"
    ) share ;
    { name ; descr ; share = share |? -1. }
end

type t =
  { mutable variant : int ;
    mutable forced : bool ;
    variants : Variant.t array }

module Serialized =
struct
  type var =
    { descr : string ;
      share : float [@ppp_default -1.] }
    [@@ppp PPP_OCaml]
  type vars = (string, var) Hashtbl.t [@@ppp PPP_OCaml]
  type exps = (string, vars) Hashtbl.t [@@ppp PPP_OCaml]
end

let make variants =
  (* Adjust the shares: *)
  let sum, num_unset =
    Array.fold_left (fun (s, n) v ->
      match v.Variant.share with
      | -1. -> s, n + 1
      | share -> s +. share, n
    ) (0., 0) variants in
  if sum > 1. then invalid_arg "variant shares sum > 1" ;
  let _rem =
    Array.fold_left (fun (rem_s, rem_n as rem) v ->
      match v.Variant.share with
      | -1. ->
          let s = rem_s /. float_of_int rem_n in
          v.share <- s ;
          rem_s -. s, rem_n - 1
      | _ -> rem
    ) (1. -. sum, num_unset) variants in
  { variant = -1 ; forced = false ; variants }

(*
 * Now the actual experiments:
 *)

(* Whether we run in dummy mode (null) or not.
 * Not other experiment can run alongside this one. *)

let the_big_one =
  make [|
    Variant.make ~share:0. "off"
      "Run as little as possible from ramen:\n\
       - no workers are launched;\n\
       - httpd, alerter and other daemons do nothing.\n" ;
    Variant.make "on" "Run ramen normally." |]

let archive_in_orc =
  make [|
    Variant.make "off"
      "All archives are written as non-wrapping ringbuffers as usual.\n" ;
    Variant.make ~share:0. "on"
      "All archives are written in ORC format. Non ORC non-wrapping \
       ringbufs are still possible but will not be archived.\n" |]

let parse_error_correction =
  make [|
    Variant.make "off" "No attempt at error correction\n" ;
    Variant.make ~share:0. "on" "Look for likely typo on parse errors\n" |]

let all_internal_experiments =
  [ "TheBigOne", the_big_one ;
    "ArchiveInORC", archive_in_orc ;
    "ParseErrorCorrection", parse_error_correction ]

(*
 * Initialization
 *)

(* Find out the experimenter id
 * It must never change. That's why we save it in a file and we try to
 * reproduce the same value should that file disappear. *)
let get_experimenter_id persist_dir =
  let fname = N.path_cat [ persist_dir ; N.path ".experimenter_id" ] in
  let compute () =
    match Unix.run_and_read "hostname" with
    | exception e ->
        !logger.error "Cannot execute hostname: %s"
          (Printexc.to_string e) ;
        0
    | WEXITED 0, hostname ->
        let id = Hashtbl.hash hostname in
        !logger.debug "Experimenter id: %d (from hostname %S)"
          id hostname ;
        id
    | st, _ ->
        !logger.error "Cannot execute hostname: %s"
          (string_of_process_status st) ;
        0 in
  try Files.save ~compute ~serialize:string_of_int
                 ~deserialize:int_of_string fname
  with _ -> 0

(* A file where to store additional experiments (usable from ramen programs)
 * Cannot be moved into RamenPaths because of dependencies. *)
let local_experiments_file persist_dir =
  N.path_cat
    [ persist_dir ; N.path "experiments" ;
      N.path Versions.experiment ; N.path "config" ]

(* All internal and external (in fname) experiments.
 * External experiments are loaded only once so that they can be mutated to set
 * the variant and the decision remembered. *)
(* FIXME: this is called before logging is enabled and therefore cannot log *)
let all_experiments =
  let ext_exps = ref None in
  let ppp_of_file =
    Files.ppp_of_file Serialized.exps_ppp_ocaml in
  fun persist_dir ->
    match !ext_exps with
    | Some lst -> lst
    | None ->
        !logger.debug"Looking for all experiments" ;
        let fname = local_experiments_file persist_dir in
        !logger.debug "Looking for additional experiment definitions in %a"
          N.path_print fname ;
        let lst =
          if not (N.is_empty persist_dir) && Files.exists fname then
            let exps =
              ppp_of_file fname |>
              Hashtbl.to_list |>
              List.map (fun (name, vars) ->
                !logger.debug "found definition for experiment %S" name ;
                name,
                make (
                  Hashtbl.to_list vars |>
                  List.map (fun (name, var) ->
                    let share =
                      if var.Serialized.share < 0. then None
                      else Some var.share in
                    Variant.make name ?share var.descr) |>
                  Array.of_list)) in
            List.rev_append all_internal_experiments exps
          else all_internal_experiments in
        ext_exps := Some lst ;
        lst

let initialized = ref false

(* Must be called before specialize is called: *)
let set_variants persist_dir forced_variants =
  let eid = get_experimenter_id persist_dir in
  let all_exps = all_experiments persist_dir in
  (*
   * Set all the forced variants:
   *)
  List.iter (fun variant_name ->
    match String.split ~by:"=" variant_name with
    | exception Not_found ->
        invalid_arg "Variant must be `experiment=variant`"
    | en, vn ->
        (match List.assoc en all_exps with
        | exception Not_found ->
            Printf.sprintf2
              "Unknown experiment %S, only possible experiments are: %a"
              en
              (pretty_list_print (fun oc (name, _) -> String.print oc name))
                all_exps |>
            invalid_arg
        | e ->
            (match Array.findi (fun v -> v.Variant.name = vn) e.variants with
            | exception Not_found ->
                Printf.sprintf2
                  "Unknown variant %S, only possible variants of \
                   experiment %S are: %a"
                  vn en
                  (pretty_array_print (fun oc v ->
                    String.print oc v.Variant.name)) e.variants |>
                invalid_arg
            | i ->
                e.variant <- i ; e.forced <- true))
  ) forced_variants ;
  (*
   * Then set the variants that are still unset:
   *)
  List.iter (fun (name, e) ->
    let forced = e.variant >= 0 in
    if not forced then (
      (* Look for the share we end up in: *)
      let rec find_variant i r =
        if i = Array.length e.variants - 1 then i else
        if r < e.variants.(i).share then i else
        find_variant (i + 1) (r -. e.variants.(i).share) in
      let m = 1000 in
      let r = abs (eid + Hashtbl.hash name) mod m in
      let r = float_of_int r /. float_of_int m in
      e.variant <- find_variant 0 r) ;
    !logger.debug "Experiment %s: variant %s%s"
      name
      e.variants.(e.variant).name
      (if forced then " (forced)" else "")
  ) all_exps ;
  initialized := true

(*
 * Helpers
 *)

let specialize e branches =
  if not !initialized then failwith "Experiment system is not initialized" ;
  assert (Array.length branches = Array.length e.variants) ;
  branches.(e.variant) ()
