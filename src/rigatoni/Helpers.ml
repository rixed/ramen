open Batteries

(* Small helper to return the ith entry of an array, capped to the last one.
 * Useful when we reach the last defined attempt while escalating an alert. *)
let get_cap a i =
  let len = Array.length a in
  assert (len > 0) ;
  a.(min i (len - 1))

let round_to_int f =
  int_of_float (Float.round f)

let retry ~on ?(first_delay=1.0) ?(min_delay=0.000001) ?(max_delay=10.0) ?(delay_adjust_ok=0.2) ?(delay_adjust_nok=1.1) f =
  let open Lwt in
  let open Log in
  let next_delay = ref first_delay in
  let rec loop x =
    (match f x with
    | exception e ->
      if on e then (
        let delay = !next_delay in
        let delay = min delay max_delay in
        let delay = max delay min_delay in
        next_delay := !next_delay *. delay_adjust_nok ;
        if delay > 1. then
          !logger.debug "Retryable error: %s, pausing %gs"
            (Printexc.to_string e) delay ;
        let%lwt () = Lwt_unix.sleep delay in
        loop x
      ) else (
        !logger.error "Non-retryable error: %s"
          (Printexc.to_string e) ;
        fail e
      )
    | r ->
      next_delay := !next_delay *. delay_adjust_ok ;
      return r)
  in
  loop
