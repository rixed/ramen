(* A small program that benchmark the HeavyHitters module, either for
 * correctness or speed.
 * It receives the top parameters on the command line and then generate as
 * many entries as needed with a configurable distribution.
 * Every time a value is added the top is asked to classify the point (either
 * in the top or not). In parallel, the test tracks the values that are
 * supposed to be the actual heavy hitters and estimates the correctness of
 * the answer.
 *
 * We can thus learn about the trade off between max-top-size and resolution,
 * for different flatness of distributions. *)
open Batteries
module HH = HeavyHitters

(* Generate a random integer according to a given distribution *)
let uniform () =
  Random.float 1.

let rec exp_cdf l =
  let x = uniform () in
  if x = 0. then exp_cdf l else
  ~-. (log x /. l)

let plot_distribution seen =
  let fname = "/tmp/heavyhitters_test.plg" in
  File.with_file_out ~mode:[`create;`trunc] fname (fun oc ->
    Array.iter (Printf.fprintf oc "%d\n") seen) ;
  let max_height = Array.fold_left max 0 seen in
  let cmd =
    Printf.sprintf
      "gnuplot -p -e 'set terminal dumb size 120,%d ansi256; \
                      set title \"value distribution\"; \
                      plot \"%s\" notitle with points pointtype 0'"
      (min (max_height + 4) 25)
      fname in
  ignore (Unix.system cmd) ;
  Unix.unlink fname

let () =
  let top_size = ref 100
  and top_max_size = ref 50_000
  and top_decay = ref 0.
  and top_sigmas = ref 0.
  and num_inserts = ref 100_000
  and init_inserts = ref 50_000
  and pop_size = ref 1_000_000
  and num_tracked = ref 50_000
  and lambda = ref 0.0002
  (* Test the rank function rather than is_in_top: *)
  and test_rank = ref false
  and plot_distrib = ref false
  and skip_tests = ref false
  in
  Arg.(parse
    [ "-top-size", Set_int top_size, "top size (def 20)" ;
      "-top-max-size", Set_int top_max_size,
          "max number of allowed tracked values in the top. Lower values \
           improve performance but decrease correctness (def 50k)" ;
      "-decay", Set_float top_decay, "decay coefficient (def 0)" ;
      "-sigmas", Set_float top_sigmas, "drop hitters which weight do not \
           deviate more than that number of sigmas." ;
      "-inserts", Set_int num_inserts,
          "number of values to insert and test (def 100k)" ;
      "-init-inserts", Set_int init_inserts,
          "entries added before starting the test (def 50k)" ;
      "-pop-size", Set_int pop_size,
          "size of the hitter population, the largest the more challenging \
           (def 1M)" ;
      "-tracked", Set_int num_tracked,
          "how many actual heavy hitters to track (50k)" ;
      "-lambda", Set_float lambda,
          "distribution is λ*exp(-λ*x) so smaller values flatten the distribution \
           and increase the challenge (def 0.0001)" ;
      "-test-rank", Set test_rank, "test rank function rather than is-in-top" ;
      "-plot", Set plot_distrib, "plot actual distribution over the population" ;
      "-skip-tests", Set skip_tests, "skip actual tests, useful with -plot" ]
    (fun s -> raise (Bad ("unknown "^ s)))
    "HeavyHitters benchmark") ;
  if !top_size > !num_tracked then (
    Printf.printf "tracked must not be smaller than top-size.\n" ;
    exit 1
  ) ;
  Random.self_init () ;
  (* Build a top for integers (TODO: string of size n) *)
  let top =
    HH.make ~max_size:!top_max_size ~decay:!top_decay ~sigmas:!top_sigmas in
  let time = ref 0. in
  (* Return a random hitter *)
  let rec rand () =
    (int_of_float (exp_cdf !lambda)) mod !pop_size in
  let value_for_top x =
    (* Just in case it could help the top algorithm that the heavier hitter
     * are the smaller values, scramble the values (need to be reproductible
     * obviously, but not reversible: *)
    (x * 48271) mod 0x7fffffff in
  let add x =
    HH.add top !time 1. (value_for_top x) ;
    time := !time +. 1. in
  let is_in_top x =
    HH.is_in_top !top_size (value_for_top x) top in
  let seen = Array.make !num_tracked 0 in
  let take () =
    let x = rand () in
    if x < Array.length seen then seen.(x) <- seen.(x) + 1 ;
    add x ;
    x in
  let is_really_in_top n x =
    if x < Array.length seen then (
      (* We know many times we have seen x, how many times we have seen
       * individually non x other heavy hitters. At worse, the rest was
       * evenly spread between as few other individuals as necessary to make
       * x fall out of the top. Would x still be in the top then? *)
      let sum, min_rank, max_rank =
        Array.fold_left (fun (sum, min_rank, max_rank) count ->
          (* Assume other values with same seen count would go before x: *)
          sum + count,
          (if count > seen.(x) then min_rank + 1 else min_rank),
          (if count >= seen.(x) then max_rank + 1 else max_rank)
        ) (0, 0, -1) seen in (* We are going to overestimate max_rank *)
      (* Remember we track more values than !top_size, so it may be that we
       * have tracked enough values to already know that x is below !top_size: *)
      if min_rank >= !top_size then
        Some false
      else if max_rank >= !top_size then
        None
      else
        (* Then try to down x just below the top size: *)
        let num_others = !top_size - max_rank in
        let untracked = n - sum in
        let max_untracked_count = untracked / num_others in
        (* So if num_others untracked values had max_untracked_count score,
         * would they rank before x? If not then we can still be certain that
         * x was is the top, otherwise we can't be certain: *)
        if max_untracked_count < seen.(x) then Some true
        else None
    ) else (
      None
    ) in
  Printf.printf "Initial round of %d insertions...\n%!" !init_inserts ;
  for _ = 1 to !init_inserts do ignore (take ()) done ;
  if !skip_tests then (
    if !plot_distrib then plot_distribution seen
  ) else (
    Printf.printf "Start benchmarking...\n%!" ;
    let successes_true = ref 0 (* Success when true *)
    and successes_false = ref 0 (* Success when false *)
    and failures_true = ref 0
    and failures_false = ref 0
    and unknowns = ref 0 in
    for n = 1 to !num_inserts do
      let x = take () in
      match is_in_top x,
            is_really_in_top (n + !init_inserts) x with
      | true, Some true ->
          incr successes_true
      | false, Some false ->
          incr successes_false
      | false, Some true ->
          incr failures_true
      | true, Some false->
          incr failures_false
      | _ ->
          incr unknowns
    done ;
    if !plot_distrib then plot_distribution seen ;
    let tracked_ratio =
      float_of_int (Array.fold_left (+) 0 seen) /.
      float_of_int (!num_inserts + !init_inserts) in
    Printf.printf "Tracked %d/%d events (% 5.2f%%)\n"
      (Array.fold_left (+) 0 seen) (!num_inserts + !init_inserts)
      (100. *. tracked_ratio) ;
    (* Find the most top_size value in seen, making use of an array of indices
     * into seen and sorting it according to tracked sums: *)
    let ordered_seen = Array.init (Array.length seen) identity in
    Array.fast_sort (fun i1 i2 -> compare seen.(i2) seen.(i1))
                    ordered_seen ;
    Printf.printf "Heavy hitters were: %a...\n"
      (Enum.print ~first:"" ~last:"" ~sep:", " (fun oc i ->
        Printf.fprintf oc "%d:%d" i seen.(i)))
        (Enum.take (!top_size+1) (Array.enum ordered_seen)) ;
    let hh_mask = String.init !top_size (fun i ->
      let x = ordered_seen.(i) in
      if is_in_top x then 'H' else '.') in
    let hh_mask_real =
      String.init !top_size (fun i ->
        let x = ordered_seen.(i) in
        begin
        end ;
        match is_really_in_top (!num_inserts + !init_inserts) x with
        | None -> '?'
        | Some true -> 'H'
        | Some false -> '.') in
    Printf.printf "Last HH mask (expected): %s\n" hh_mask_real ;
    Printf.printf "Last HH mask (estimate): %s\n" hh_mask ;

    let successes = !successes_true + !successes_false
    and failures = !failures_true + !failures_false in
    assert (successes + failures + !unknowns = !num_inserts) ;
    Printf.printf "Total:      % 6d successes, % 6d failures, % 6d unknowns\n"
      successes failures !unknowns ;
    let hi =
      100. *. float_of_int !successes_true /.
      float_of_int (!successes_true + !failures_true)
    and lo =
      100. *. float_of_int !successes_true /.
      float_of_int (!successes_true + !failures_true + !unknowns) in
    let resolution_true = 100. -. (hi -. lo) in
    Printf.printf "When true:  % 6d successes, % 6d failures \
      -> % 5.2f … %.2f%% (resolution: % 5.2f%%)\n"
      !successes_true !failures_true hi lo resolution_true ;
    let hi =
      100. *. float_of_int !successes_false /.
      float_of_int (!successes_false + !failures_false)
    and lo =
      100. *. float_of_int !successes_false /.
      float_of_int (!successes_false + !failures_false + !unknowns) in
    let resolution_false = 100. -. (hi -. lo) in
    Printf.printf "When false: % 6d successes, % 6d failures \
      -> % 5.2f … %.2f%% (resolution: % 5.2f%%)\n"
      !successes_false !failures_false hi lo resolution_false ;

    if !successes_true + !failures_true = 0 then
       Printf.printf "Result does not tell anything about top hitters.\n" ;
    if !successes_false + !failures_false = 0 then
       Printf.printf "Result does not tell anything about non-top hitters.\n" ;

    if resolution_true < 90. || resolution_false < 90. then (
      if tracked_ratio > 0.9 then
        Printf.printf "Resolution is low despite good tracking, caused by rank equalities.\n"
      else
        Printf.printf "Resolution is low as a result of bad tracking, consider increasing -tracked.\n"
    )
  )
