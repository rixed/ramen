open Batteries
open RamenLog
open RamenHelpers
module C = RamenConf
module F = C.Func
module P = C.Program

(* Dequeue command *)

let dequeue conf file n () =
  init_logger conf.C.log_level ;
  if file = "" then invalid_arg "dequeue" ;
  let open RingBuf in
  let rb = load file in
  let rec dequeue_loop n =
    if n > 0 then (
      (* TODO: same automatic retry-er as in CodeGenLib_IO *)
      match dequeue rb with
      | exception Empty ->
          failwith "ring buffer is empty."
      | bytes ->
          Printf.printf "dequeued %d bytes:" (Bytes.length bytes) ;
          hex_print stdout bytes ;
          dequeue_loop (n - 1)
    )
  in
  dequeue_loop n

(* Summary command *)

let summary conf max_bytes files () =
  let open RingBuf in
  let max_words = round_up_to_rb_word max_bytes in
  init_logger conf.C.log_level ;
  List.iter (fun file ->
    let rb = load file in
    let s = stats rb in
    (* The file header: *)
    Printf.printf "%s:\n\
                   Flags:%s\n\
                   seq range: %d..%d (%d)\n\
                   time range: %f..%f (%.1fs)\n\
                   %d/%d words used (%3.1f%%)\n\
                   mmapped bytes: %d\n\
                   producers range: %d..%d\n\
                   consumers range: %d..%d\n"
      file
      (if s.wrap then " Wrap" else "")
      s.first_seq (s.first_seq + s.alloc_count - 1) s.alloc_count
      s.t_min s.t_max (s.t_max -. s.t_min)
      s.alloced_words s.capacity
      (float_of_int s.alloced_words *. 100. /. (float_of_int s.capacity))
      s.mem_size s.prod_tail s.prod_head s.cons_tail s.cons_head ;
    (* Also dump the content from consumer begin to producer begin, aka
     * the available tuples. *)
    let dump o sz =
      let bytes = RingBuf.read_raw rb o sz in
      hex_print ~from_rb:true stdout bytes
    in
    if s.prod_tail <> s.cons_head then ( (* not empty *)
      Printf.printf "\nAvailable bytes:" ;
      if s.cons_head < s.prod_tail then ( (* no wraparound *)
        (* Reminder: we manipulate only word indices here: *)
        let sz = s.prod_tail - s.cons_tail in
        dump s.cons_tail (min max_words sz) ;
        if sz > max_words then Printf.printf "...\n"
      ) else ( (* wrap around *)
        let sz = s.capacity - s.cons_tail in
        dump s.cons_tail (min max_words sz) ;
        if sz < max_words then (
          Printf.printf "***** WRAP AROUND *****\n" ;
          let max_words_ = max_words - sz in
          dump 0 (min max_words_ s.prod_tail) ;
          if s.prod_tail > max_words_ then Printf.printf "...\n"))) ;
    unload rb
  ) files

(* Repair Command *)

let repair conf files () =
  init_logger conf.C.log_level ;
  List.iter (fun file ->
    let open RingBuf in
    let rb = load file in
    if repair rb then
      !logger.warning "Ringbuf was damaged" ;
    unload rb
  ) files

(* List all ringbuffers in use with some stats: *)

type func_status =
  | Running of F.t
  | NotRunning of RamenName.program * RamenName.func
  | ProgramError of RamenName.program * string

let links conf no_abbrev show_all as_tree pretty with_header sort_col top
          pattern () =
  init_logger conf.C.log_level ;
  if as_tree &&
     (pretty || with_header > 0 || sort_col <> "1" || top <> None || show_all)
  then
    failwith "Option --as-tree is not compatible with --pretty, --header, \
              --sort, --top or --show-all" ;
  let pattern = Globs.compile pattern in
  (* Same to get the ringbuffer stats, but we never reread the stats (not
   * needed, and mtime wouldn't really work on those mmapped files *)
  let get_rb_stats = cached "links" (fun fname ->
    match RingBuf.load fname with
    | exception Failure _ -> None
    | rb ->
        let s = RingBuf.stats rb in
        RingBuf.unload rb ;
        Some s) ignore in
  let fq_name = function
    | NotRunning (pn, fn) ->
        red ((pn :> string) ^"/"^ (fn :> string))
    | ProgramError (pn, e) ->
        red ((pn :> string) ^": "^ e)
    | Running func -> (F.fq_name func :> string) in
  let line_of_link i p c =
    let parent = fq_name p and child = fq_name c in
    let ringbuf, fill_ratio, next_seqs, max_etime, is_err1 =
      match c with
      | NotRunning _ | ProgramError _ -> "", "0.", "", None, false
      | Running c ->
          let ringbuf =
            if c.F.merge_inputs then
              C.in_ringbuf_name_merging conf c i
            else
              C.in_ringbuf_name_single conf c in
          let fill_ratio, next_seqs, max_etime =
            match get_rb_stats ringbuf with
            | None -> 0., "", None
            | Some s ->
                float_of_int s.alloced_words /. float_of_int s.capacity,
                string_of_int s.cons_tail ^".."^ string_of_int s.cons_head,
                if s.t_max = 0. then None else Some s.t_max in
          let fr = string_of_float fill_ratio in
          let fr, is_err =
            if fill_ratio < 0.33 then green fr, false else
            (* The ringbuffer will be full before the word fill ratio is 100%: *)
            if fill_ratio >= 0.9 then red fr, true else
            if fill_ratio > 0.75 then yellow fr, false else
            fr, false in
          ringbuf, fr, next_seqs, max_etime, is_err
    in
    let out_ref, spec, is_err2 =
      match p with
      | NotRunning _ ->
          "", red "NOT RUNNING", true
      | ProgramError (_, e) ->
          "", red e, true
      | Running p ->
          let out_ref = C.out_ringbuf_names_ref conf p in
          let outs = RamenOutRef.read out_ref in
          let spec, is_err =
            if Hashtbl.mem outs ringbuf then ringbuf, false
            else red "MISSING", true in
          out_ref, spec, is_err
    in
    let is_err = is_err1 || is_err2 in
    let ap s = if no_abbrev then s else abbrev_path s in
    let parent_disp = ap parent and child_disp = ap child in
    let ap s = if no_abbrev then s else
                 abbrev_path ~known_prefix:conf.persist_dir s in
    let out_ref = ap out_ref and ringbuf = ap ringbuf in
    is_err, parent, parent_disp, child,
    TermTable.[|
      Some (ValStr parent_disp) ;
      Some (ValStr child_disp) ;
      Some (ValStr out_ref) ;
      Some (ValStr spec) ;
      Some (ValStr ringbuf) ;
      Some (ValStr fill_ratio) ;
      Some (ValStr next_seqs) ;
      date_or_na max_etime |]
  in
  (* We first collect all links supposed to exist according to
   * parent/children relationships: *)
  let links =
    C.with_rlock conf (fun programs ->
      Hashtbl.fold (fun _prog_name (mre, get_rc) links ->
        if mre.C.status <> MustRun then links else
        match get_rc () with
        | exception _ ->
            links (* Errors have been logged already *)
        | prog ->
            List.fold_left (fun links func ->
              let links =
                List.fold_lefti (fun links i
                                    (par_rel_prog, par_func) ->
                  (* i is the index in the list of parents for a given
                   * child *)
                  let par_prog =
                    F.program_of_parent_prog func.F.program_name
                                             par_rel_prog in
                  let parent =
                    match Hashtbl.find programs par_prog with
                    | exception Not_found ->
                        NotRunning (par_prog, par_func)
                    | _mre, get_rc ->
                      (match get_rc () with
                      | exception e ->
                          ProgramError (par_prog, Printexc.to_string e)
                      | pprog ->
                          (match List.find (fun f ->
                                   f.F.name = par_func
                                 ) pprog.P.funcs with
                          | exception Not_found ->
                              NotRunning (par_prog, par_func)
                          | pfunc -> Running pfunc)) in
                  line_of_link i parent (Running func) :: links
                ) links func.parents in
              links
            ) links prog.P.funcs
      ) programs []) in
  let head =
    [| "parent" ; "child" ; "out_ref" ; "spec" ; "ringbuf" ;
       "fill ratio" ; "next seqs" ; "max event time" |] in
  if as_tree then (
    let roots =
      List.filter_map (fun (_is_err, parent, parent_disp, _child, _link) ->
        if Globs.matches pattern parent then Some parent_disp
        else None
      ) links
    (* FIXME: roots should be ordered parents first. *)
    and links = List.map (fun (_, _, _, _, link) -> link) links in
    TermTable.print_tree ~parent:0 ~child:1 head links roots
  ) else (
    let sort_col = RamenCliCmd.sort_col_of_string head sort_col in
    let print =
      TermTable.print_table ~pretty ~sort_col ~with_header ?top head in
    List.iter (fun (is_err, parent, _parent_disp, child, link) ->
      if (Globs.matches pattern parent || Globs.matches pattern child) &&
         (is_err || show_all)
      then print link
    ) links ;
    print [||]
  )
