open Batteries
open Cmdliner

type options = { debug : bool }

let options debug = { debug }

let common_opts =
  let debug =
    Arg.(value (flag (info ~doc:"increase verbosity" ["d"; "debug"])))
  in
  Term.(const options $ debug)

(* Dequeue command *)

let dequeue _opts file n () =
  let open RingBuf in
  let rb = load file in
  let rec dequeue_loop n =
    if n > 0 then (
      (* TODO: same automatic retry-er as in CodeGenLib_IO *)
      let bytes = dequeue rb in
      Printf.printf "dequeued %d bytes\n%!" (Bytes.length bytes) ;
      dequeue_loop (n - 1)
    )
  in
  dequeue_loop n

(* TODO: check that this is actually the name of a ringbuffer file *)
let rb_file_opt =
  let i = Arg.info ~docv:"FILE" ~doc:"File with the ring buffer" [] in
  Arg.(required (pos 0 (some string) None i))

let nb_tuples_opt =
  let i = Arg.info ~doc:"How many entries to dequeue"
                   ["n"; "nb-entries"] in
  Arg.(value (opt int 1 i))

let dequeue_cmd =
  Term.(
    (const dequeue
      $ common_opts
      $ rb_file_opt
      $ nb_tuples_opt),
    info "dequeue")

(*
 * Main
 *)

let default_cmd =
  let doc = "Ring Buffer Swiss-army knife" in
  Term.((ret (app (const (fun _ -> `Help (`Pager, None))) common_opts)),
        info "ringbuf_ctl" ~doc)

let () =
  match Term.eval_choice default_cmd [
    dequeue_cmd
  ] with `Error _ -> exit 1
       | `Version | `Help -> exit 42
       | `Ok f -> f ()
