open Batteries
open RamenHelpers
open RamenLog

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
  { name : string ;
    mutable variant : int ;
    variants : Variant.t array }

let make name variants =
  (* Adjust the shares: *)
  let sum, nb_unset =
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
    ) (sum, nb_unset) variants in
  { name ; variant = -1 ; variants }

(*
 * Now the actual experiments:
 *)

(* Whether we run in dummy mode (null) or not.
 * Not other experiment can run alongside this one. *)

let the_big_one =
  make "TheBigOne" [|
    Variant.make ~share:0.9 "off"
      "Run as little as possible from ramen:\n\
       - no workers ;\n\
       - ...?\n" ;
    Variant.make "on" "Run ramen normally." |]

let all_experiments = [ the_big_one ]

(*
 * Helpers
 *)

let experimenter_id =
  memoize (fun persist_dir ->
    (* The host id must never change. That's why we save it in a file and we
     * try to reproduce the same value should that file disappear: *)
    let compute () =
      try
        let id = run ~timeout:2. [| "hostname" |] |> Hashtbl.hash in
        !logger.debug "Experimenter id: %d" id ;
        id
      with _ -> 0 in
    let fname = persist_dir ^"/.experimenter_id" in
    try save_in_file ~compute ~serialize:string_of_int
                     ~deserialize:int_of_string fname
    with _ -> 0)

let specialize persist_dir e branches =
  assert (Array.length branches = Array.length e.variants) ;
  branches.(e.variant) ()

let set_variants persist_dir forced_variants =
  let eid = experimenter_id persist_dir in
  (* First, set the forced variants *)
  List.iter (fun variant_name ->
    match String.split ~by:"=" variant_name with
    | exception Not_found ->
        invalid_arg "variant must be `experiment=variant`"
    | en, vn ->
        (match List.find (fun e -> e.name = en) all_experiments with
        | exception Not_found ->
            invalid_arg ("unknown experiment: "^ en) (* TODO: list them *)
        | e ->
            (match Array.findi (fun v -> v.Variant.name = vn) e.variants with
            | exception Not_found ->
                invalid_arg ("unknown variant: "^ vn) (* TODO: list them *)
            | i ->
                e.variant <- i))
  ) forced_variants ;
  (* Then set the variants that are still unset: *)
  List.iter (fun e ->
    let forced = e.variant >= 0 in
    if not forced then (
      (* Look for the share we end up in: *)
      let rec find_variant i r =
        if i = Array.length e.variants - 1 then i else
        if r < e.variants.(i).share then i else
        find_variant (i + 1) (r -. e.variants.(i).share) in
      let m = 1000 in
      let r = abs (eid + Hashtbl.hash e.name) mod m in
      let r = float_of_int r /. float_of_int m in
      e.variant <- find_variant 0 r) ;
    !logger.debug "Experiment %s: variant %s%s"
      e.name
      e.variants.(e.variant).name
      (if forced then " (forced)" else "")
  ) all_experiments
