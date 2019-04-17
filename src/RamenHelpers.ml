open Batteries
open RamenLog
open RamenConsts
module Atomic = RamenAtomic

(*$inject open Batteries *)

let max_int_for_random = 0x3FFFFFFF

(* Small helper to return the ith entry of an array, capped to the last one.
 * Useful when we reach the last defined attempt while escalating an alert. *)
let get_cap a i =
  let len = Array.length a in
  assert (len > 0) ;
  a.(min i (len - 1))

let align_float ?(round=floor) step v =
  round (v /. step) *. step

let round_to_int f =
  int_of_float (Float.round f)

let ceil_to_int f =
  int_of_float (Float.ceil f)

(* The original Float.to_string adds a useless dot at the end of
 * round numbers, and likes to end with lots of zeroes: *)
let nice_string_of_float v =
  let s = Float.to_string v in
  assert (String.length s > 0) ;
  match String.index s '.' with
  | exception Not_found -> s
  | i ->
      let last_non_zero =
        let rec loop j =
          assert (j >= i) ;
          if s.[j] <> '0' then j else loop (j - 1) in
        loop (String.length s - 1) in
      let has_trailling_dot = s.[last_non_zero] = '.' in
      let n =
        (String.length s - last_non_zero) - 1 +
        (if has_trailling_dot then 1 else 0) in
      String.rchop ~n s

(*$= nice_string_of_float & ~printer:(fun x -> x)
  "1.234"    (nice_string_of_float 1.234)
  "1.001"    (nice_string_of_float 1.001)
  "1"        (nice_string_of_float 1.)
  "31536000" (nice_string_of_float 31536000.)
*)

let print_nice_float oc f =
  String.print oc (nice_string_of_float f)

exception Timeout

(* Avoid to create a new while_ at each call: *)
let always _ = true

let retry
    ~on ?(first_delay=1.0) ?(min_delay=0.0001) ?(max_delay=10.0)
    ?(delay_adjust_ok=0.2) ?(delay_adjust_nok=1.5) ?delay_rec
    ?max_retry ?max_retry_time ?(while_=always) f =
  let next_delay = ref first_delay in
  let started = Unix.gettimeofday () in
  let can_wait_longer () =
    match max_retry_time with
    | None -> true
    | Some d -> Unix.gettimeofday () -. started < d in
  let rec loop num_try x =
    if not (while_ ()) then raise Exit
    else if not (can_wait_longer ()) then raise Timeout
    else match f x with
      | exception e ->
        let retry_on_this = on e in
        let should_retry =
          Option.map_default (fun max -> num_try < max) true max_retry &&
          retry_on_this in
        if should_retry then (
          if not (while_ ()) then raise Exit ; (* Before sleeping *)
          let delay = !next_delay in
          let delay = min delay max_delay in
          let delay = max delay min_delay in
          next_delay := !next_delay *. delay_adjust_nok ;
          Option.may (fun f -> f delay) delay_rec ;
          Unix.sleepf delay ;
          (loop [@tailcall]) (num_try + 1) x
        ) else (
          !logger.debug "Non-retryable error: %s after %d attempt%s"
            (Printexc.to_string e) num_try (if num_try > 1 then "s" else "") ;
          raise e)
      | r ->
        next_delay := !next_delay *. delay_adjust_ok ;
        r
  in
  loop 1

let shell_quote s =
  "'"^ String.nreplace s "'" "'\\''" ^"'"

let sql_quote s =
  "'"^ String.nreplace s "'" "''" ^"'"

let ramen_quote = sql_quote

(* TODO: add to batteries *)
let set_iteri f s =
  Set.fold (fun e i -> f i e ; i + 1) s 0 |>
  ignore

(* FIXME: could be faster :) *)
let set_is_singleton s =
  Set.cardinal s = 1

let array_rfindi f a =
  let res = ref (-1) in
  try
    for i = Array.length a - 1 downto 0 do
      if f a.(i) then (
        res := i ; raise Exit
      )
    done ;
    raise Not_found
  with Exit ->
    !res

let array_rfind f a =
  let i = array_rfindi f a in
  a.(i)

let array_filter_mapi f a =
  Array.enum a |> Enum.mapi f |> Enum.filter_map identity |> Array.of_enum

let list_filter_mapi f a =
  List.enum a |> Enum.mapi f |> Enum.filter_map identity |> List.of_enum

let list_rfind_map f l =
  List.rev l |> List.find_map f

let list_rassoc k l =
  List.rev l |> List.assoc k

let list_existsi f l =
  match List.findi (fun i v -> f i v) l with
  | exception Not_found -> false
  | _ -> true

let list_iter_first_last f lst =
  let rec loop is_first = function
  | [] -> ()
  | [x] -> f is_first true x
  | x::lst ->
      f is_first false x ;
      loop false lst in
  loop true lst

(* Same as BatList.fold_left2, but do not choke on lists of different
 * length but instead go as far as possible. Handy when iterating over
 * infinite lists such as all-true out-ref file specs. *)
let rec list_fold_left2 f init l1 l2 =
  match l1, l2 with
  | h1::r1, h2::r2 -> list_fold_left2 f (f init h1 h2) r1 r2
  | _ -> init

(* Remove the dups (according to [cmp]) without altering the order of
 * elements: *)
(* FIXME: a RamenSet that takes a [cmp] function, since that's not in
 * Batteries. *)
let remove_dups _cmp =
  let s = ref Set.empty in
  Enum.filter (fun x ->
    if Set.mem x !s then false else (
      s := Set.add x !s ;
      true))

let list_remove_dups cmp lst =
  List.enum lst |> remove_dups cmp |> List.of_enum

(*$= list_remove_dups & ~printer:(IO.to_string (List.print Int.print))
  [1;2;3] (list_remove_dups Int.compare [1;1;2;3;1;3;2])
*)

let list_revmap_3 f l1 l2 l3 =
  let rec loop res l1 l2 l3 =
    match l1, l2, l3 with
    | [], [], [] -> res
    | a::l1, b::l2, c::l3 ->
        loop (f a b c :: res) l1 l2 l3
    | _ ->
        invalid_arg "list_revmap_3" in
  loop [] l1 l2 l3

let hashtbl_find_first f h =
  let res = ref None in
  try
    Hashtbl.iter (fun k v ->
      if f k v then (
        res := Some (k, v) ;
        raise Exit)
    ) h ;
    raise Not_found
  with Exit -> Option.get !res

let hashtbl_find_all f h =
  let res = ref [] in
  Hashtbl.iter (fun k v ->
    if f k v then res := (k, v) :: !res
  ) h ;
  !res

let hashtbl_find_option_delayed def h k =
  try
    Hashtbl.find h k
  with Not_found ->
    let v = def () in
    Hashtbl.add h k v ;
    v

let print_exception ?(what="Exception") e =
  !logger.error "%s: %s\n%s" what
    (Printexc.to_string e)
    (Printexc.get_backtrace ())

let result_print p_ok p_err oc = function
  | Result.Ok x -> Printf.fprintf oc "Ok(%a)" p_ok x
  | Result.Bad x -> Printf.fprintf oc "Bad(%a)" p_err x

let on_error k f =
  try f ()
  with e ->
    let bt = Printexc.get_raw_backtrace () in
    k () ;
    Printexc.raise_with_backtrace e bt

let log_exceptions ?what f =
  try f ()
  with e ->
    let backtrace = Printexc.get_raw_backtrace () in
    if e <> Exit then print_exception ?what e ;
    Printexc.raise_with_backtrace e backtrace

let log_and_ignore_exceptions ?what f x =
  try f x
  with Exit -> ()
     | e -> print_exception ?what e

let default_on_exception def ?what f x =
  try f x
  with e -> print_exception ?what e ; def

let print_dump oc x = dump x |> String.print oc

let string_sub_eq ?(case_sensitive=true) s1 o1 s2 o2 len =
  let rec loop o1 o2 len =
    len <= 0 ||
    (
      (
        let c1 = s1.[o1] and c2 = s2.[o2] in
        c1 = c2 ||
        not case_sensitive && Char.(lowercase_ascii c1 = lowercase_ascii c2)
      ) &&
      loop (o1 + 1) (o2 + 1) (len - 1)
    ) in
  (o1 + len <= String.length s1) &&
  (o2 + len <= String.length s2) &&
  loop o1 o2 len

(*$= string_sub_eq & ~printer:string_of_bool
  true (string_sub_eq "glop glop" 0 "glop" 0 4)
  true (string_sub_eq "glop glop" 0 "XglopX" 1 4)
  false (string_sub_eq "glop glop" 0 "glup" 0 4)
  false (string_sub_eq "glop glop" 0 "pas glop pas glop" 0 17)
*)

let string_is_term fins s o =
  o >= String.length s ||
  (let c = s.[o] in List.exists ((=) c) fins)

(*$= string_is_term & ~printer:string_of_bool
  true (string_is_term [] "" 0)
  true (string_is_term [] "xx" 2)
  true (string_is_term [';';','] "," 0)
  true (string_is_term [';';','] "x;x" 1)
  true (string_is_term [';';','] "x,x" 1)
  false (string_is_term [] "xx" 1)
  false (string_is_term [';';','] "xx" 1)
  false (string_is_term [';';','] "x;x" 0)
  false (string_is_term [';';','] "x,x" 2)
*)

let abbrev len s =
  if String.length s <= len then s else
  String.sub s 0 (len-3) ^"..."

let string_skip_blanks_until c s o =
  let rec loop o =
    if o >= String.length s then raise Not_found ;
    if s.[o] = c then o else
    if Char.is_whitespace s.[o] then loop (o + 1) else
    Printf.sprintf "Unexpected %C while looking for %C at offset %d"
      s.[o] c o |>
    failwith in
  loop o

let rec string_skip_blanks s o =
  if o < String.length s && Char.is_whitespace s.[o] then
    string_skip_blanks s (o + 1)
  else o

let check_parse_all s (x, o) =
  let l = String.length s in
  let o = string_skip_blanks s o in
  if o = l then x else
    Printf.sprintf "Junk at end of string (offset %d/%d): %S"
      o l (String.lchop ~n:o s |> abbrev 10) |>
    failwith

let looks_like_true s =
  s = "1" || (
    String.length s > 1 &&
    let lc = Char.lowercase s.[0] in lc = 'y' || lc = 't')

(* When we do have to convert a null value into a string: *)
let string_of_null = "null"

let looks_like_null ?(offs=0) s =
  string_sub_eq ~case_sensitive:false s offs string_of_null 0
                (String.length string_of_null)

(*$= looks_like_null & ~printer:string_of_bool
  true (looks_like_null "null")
  true (looks_like_null "NULL")
  true (looks_like_null "nuLL")
  false (looks_like_null "")
*)

let is_alphanum c =
  Char.(is_letter c || is_digit c)

(* Helper to build the indentation in front of printed lines. We just use 2
 * spaces like normal people: *)
let indent_of i = String.make (i*2) ' '

(* Helper to emit code at a given level: *)
let emit oc indent fmt =
  Printf.fprintf oc ("%s" ^^ fmt ^^ "\n") (indent_of indent)

let with_time f k =
  let start = Unix.gettimeofday () in
  let res = f () in
  let dt = Unix.gettimeofday () -. start in
  k dt ;
  res

let time what f =
  let start = Unix.gettimeofday () in
  let res = f () in
  let dt = Unix.gettimeofday () -. start in
  !logger.info "%s in %gs." what dt ;
  res

(*
 * Some file utilities using mere strings that RamenName.path is using
 *)

let rec restart_on_eintr ?(while_=always) f x =
  let open Unix in
  try f x
  with Unix_error (EINTR, _, _) ->
    if while_ () then restart_on_eintr ~while_ f x
    else raise Exit

(*
 * Some Unix utilities
 *)

let name_of_signal s =
  let open Sys in
  if s = sigabrt then "ABORT"
  else if s = sigalrm then "ALRM"
  else if s = sigfpe then "FPE"
  else if s = sighup then "HUP"
  else if s = sigill then "ILL"
  else if s = sigint then "INT"
  else if s = sigkill then "KILL"
  else if s = sigpipe then "PIPE"
  else if s = sigquit then "QUIT"
  else if s = sigsegv then "SEGV"
  else if s = sigterm then "TERM"
  else if s = sigusr1 then "USR1"
  else if s = sigusr2 then "USR2"
  else if s = sigchld then "CHLD"
  else if s = sigcont then "CONT"
  else if s = sigstop then "STOP"
  else if s = sigtstp then "TSTP"
  else if s = sigttin then "TTIN"
  else if s = sigttou then "TTOU"
  else if s = sigvtalrm then "VTALRM"
  else if s = sigprof then "PROF"
  else if s = sigbus then "BUS"
  else if s = sigpoll then "POLL"
  else if s = sigsys then "SYS"
  else if s = sigtrap then "TRAP"
  else if s = sigurg then "URG"
  else if s = sigxcpu then "XCPU"
  else if s = sigxfsz then "XFSZ"
  else "Unknown OCaml signal number "^ string_of_int s

let set_signals sigs behavior =
  List.iter (fun s ->
    Sys.set_signal s behavior
  ) sigs

let string_of_process_status = function
  | Unix.WEXITED 126 ->
      "couldn't execve after fork"
  | Unix.WEXITED 127 ->
      "couldn't be executed"
  | Unix.WEXITED code ->
      Printf.sprintf "terminated with code %d (%s)"
        code (ExitCodes.string_of_code code)
  | Unix.WSIGNALED sign ->
      Printf.sprintf "killed by signal %s" (name_of_signal sign)
  | Unix.WSTOPPED sign ->
      Printf.sprintf "stopped by signal %s" (name_of_signal sign)

exception RunFailure of Unix.process_status
let () = Printexc.register_printer (function
  | RunFailure st -> Some ("RunFailure "^ string_of_process_status st)
  | _ -> None)

(* Trick from LWT: how to exit without executing the at_exit hooks: *)
external sys_exit : int -> 'a = "caml_sys_exit"

(* FIXME: is_singleton and cardinal_greater in batteries *)
let set_int_is_singleton s = Set.Int.cardinal s = 1

let rec waitall_once ?expected_status ~what pids =
  let open Unix in
  Set.Int.filter (fun pid ->
    let complain is_err status =
      (if is_err then !logger.error else !logger.debug)
        "%s: %s" what (string_of_process_status status) in
    let flags = [ WUNTRACED ] in
    let flags =
      if set_int_is_singleton pids then flags
      else WNOHANG :: flags in
    match restart_on_EINTR (waitpid flags) pid with
    | 0, _ -> true
    | _, (WEXITED c as status) ->
        complain
          (expected_status <> Some c && expected_status <> None)
          status ;
        false
    | _, status ->
        complain true status ;
        false
  ) pids

let waitall ?(while_=always) ?expected_status ~what pids =
  let rec loop pids =
    if while_ () && not (Set.Int.is_empty pids) then (
      let pids = waitall_once ?expected_status ~what pids in
      if not (Set.Int.is_empty pids) then (
        Unix.sleep 1 ;
        loop pids))
  in
  loop pids

let waitpid_log ?expected_status ~what pid =
  waitall ?expected_status ~what (Set.Int.singleton pid)

let quote_at_start s =
  String.length s > 0 && s.[0] = '"'

let quote_at_end s =
  String.length s > 0 && s.[String.length s - 1] = '"'

exception InvalidCSVQuoting

let strings_of_csv separator line =
  (* If line is the empty string, String.nsplit returns an empty list
   * instead of a list with a single empty value. *)
  let strings =
    if line = "" then [ "" ]
    else String.nsplit line separator in
  (* Handle quoting in CSV values. TODO: enable/disable based on operation flag *)
  let strings', rem_s, has_quote =
    List.fold_left (fun (lst, prev_s, has_quote) s ->
      if prev_s = "" then (
        if quote_at_start s then (
          if quote_at_end s then (
            let len = String.length s in
            if len > 1 then String.sub s 1 (len - 2) :: lst, "", true
            else s :: lst, "", has_quote
          ) else lst, s, true
        ) else s :: lst, "", has_quote
      ) else (
        if quote_at_end s then (String.(lchop prev_s ^ rchop s) :: lst, "", true)
        else lst, prev_s ^ s, true
      )) ([], "", false) strings in
  if rem_s <> "" then raise InvalidCSVQuoting ;
  if has_quote then List.rev strings' else strings

(*$= strings_of_csv & ~printer:(IO.to_string (List.print String.print))
  [ "glop" ; "glop" ] (strings_of_csv " " "glop glop")
  [ "John" ; "+500" ] (strings_of_csv "," "\"John\",+500")
 *)

let getenv ?def n =
  try Sys.getenv n
  with Not_found ->
    match def with
    | Some d -> d
    | None ->
      Printf.sprintf "Cannot find envvar %s" n |>
      failwith

let do_daemonize () =
  let open Unix in
  flush_all () ;
  if fork () > 0 then sys_exit 0 ;
  setsid () |> ignore ;
  (* Close in/out, ignoring errors in case they have been closed already: *)
  let null = openfile "/dev/null" [O_RDONLY] 0 in
  dup2 null stdin ;
  close null ;
  let null = openfile "/dev/null" [O_WRONLY; O_APPEND] 0 in
  dup2 null stdout ;
  dup2 null stderr ;
  close null

let random_string =
  let chars = "0123456789abcdefghijklmnopqrstuvwxyz" in
  let random_char _ =
    let i = Random.int (String.length chars) in chars.[i]
  in
  fun len ->
    Bytes.init len random_char |>
    Bytes.to_string

let max_simult ~what ~max_count f =
  let rec loop () =
    if Atomic.Counter.get max_count <= 0 then (
      !logger.debug "Too many %s pending, waiting..." what ;
      Unix.sleepf (0.2 +. Random.float 0.2) ;
      loop ()
    ) else (
      Atomic.Counter.decr max_count ;
      finally (fun () -> Atomic.Counter.incr max_count)
        f ()
    ) in
  loop ()

let read_lines fd =
  let open Legacy.Unix in
  let last_chunk = ref Bytes.empty in
  let buf = Buffer.create 1000 in
  let eof = ref false in
  (* Tells if we also had a newline after buf: *)
  let flush ends_with_nl =
    if Buffer.length buf = 0 && not ends_with_nl then
      raise Enum.No_more_elements ;
    let s = Buffer.contents buf in
    Buffer.clear buf ;
    s
  in
  Enum.from (fun () ->
    let rec loop () =
      if !eof then raise Enum.No_more_elements ;
      let chunk =
        if Bytes.length !last_chunk > 0 then (
          (* If we have some bytes left from previous run, use that: *)
          !last_chunk
        ) else (
          (* Get new bytes: *)
          let chunk = Bytes.create 1000 in
          let r = read fd chunk 0 (Bytes.length chunk) in
          Bytes.sub chunk 0 r
        ) in
      if Bytes.length chunk = 0 then (
        eof := true ;
        flush false
      ) else match Bytes.index chunk '\n' with
        | exception Not_found ->
            Buffer.add_bytes buf chunk ;
            last_chunk := Bytes.empty ;
            loop ()
        | l ->
            Buffer.add_bytes buf (Bytes.sub chunk 0 l) ;
            last_chunk :=
              (let l = l+1 in
              Bytes.sub chunk l (Bytes.length chunk - l)) ;
            flush true in
    loop ())

(* Run given command, logging its output in our log-file *)
let run_coprocess ~max_count ?(to_stdin="") cmd_name cmd =
  !logger.debug "Executing: %s" cmd ;
  max_simult ~what:cmd_name ~max_count (fun () ->
    let open Legacy.Unix in
    let (pstdout, pstdin, pstderr as chans) = open_process_full cmd [||] in
    let pstdout = descr_of_in_channel pstdout
    and pstdin = descr_of_out_channel pstdin
    and pstderr = descr_of_in_channel pstderr in
    let status = ref None in
    let write_stdin () =
      try
        let len = String.length to_stdin in
        let w = write_substring pstdin to_stdin 0 len in
        if w < len then
          !logger.error "Can only write %d/%d bytes" w len
      with Unix_error (EPIPE, _, _) -> ()
    and read_out c =
      try
        read_lines c |>
        Enum.iter (fun line ->
          !logger.info "%s: %s" cmd_name line)
      with exn ->
        let msg = Printexc.to_string exn in
        !logger.error "%s: Cannot read output: %s" cmd_name msg
    in
    finally (fun () -> status := Some (close_process_full chans))
      (List.iter Thread.join)
        [ Thread.create write_stdin () ;
          Thread.create read_out pstdout ;
          Thread.create read_out pstderr ] ;
    !status)

let string_of_time ts =
  let open Unix in
  match localtime ts with
  | exception Unix_error (EINVAL, _, _) ->
      Printf.sprintf "Invalid date %f" ts
  | tm ->
      Printf.sprintf "%04d-%02d-%02dT%02d:%02d:%02d"
        (tm.tm_year + 1900) (tm.tm_mon + 1) tm.tm_mday
        tm.tm_hour tm.tm_min tm.tm_sec

let string_of_duration d =
  let aux s d k u =
    if d >= k then
      let x = Float.floor (d /. k) in
      s ^ nice_string_of_float x ^ u, d -. x *. k
    else
      s, d in
  let s, d = aux "" d 3600. "h" in
  if d = 0. then s else
  let s, d = aux s d 60. "m" in
  if d = 0. then s else
  s ^ nice_string_of_float d ^ "s"

let udp_server ?(buffer_size=2000) ~inet_addr ~port ?(while_=always) k =
  let open Unix in
  (* FIXME: it seems that binding that socket makes cohttp leak descriptors
   * when sending reports to ramen. Oh boy! *)
  let sock_of_domain domain =
    let sock = socket domain SOCK_DGRAM 0 in
    bind sock (ADDR_INET (inet_addr, port)) ;
    sock in
  let sock =
    try sock_of_domain PF_INET6
    with _ -> sock_of_domain PF_INET in
  !logger.info "Listening for datagrams on %s:%d"
    (Unix.string_of_inet_addr inet_addr) port ;
  let buffer = Bytes.create buffer_size in
  let rec forever () =
    if while_ () then
      let recv_len, sockaddr =
        restart_on_eintr ~while_ (fun () ->
          recvfrom sock buffer 0 (Bytes.length buffer) []) () in
      !logger.debug "Received %d bytes on UDP port %d" recv_len port ;
      let sender =
        match sockaddr with
        | ADDR_INET (addr, _port) -> Some addr
        | _ -> None in
      k ?sender buffer recv_len ;
      (forever [@tailcall]) ()
  in
  forever ()

let hex_of =
  let zero = Char.code '0'
  and ten = Char.code 'a' - 10 in
  fun n ->
    if n < 10 then Char.chr (zero + n)
    else Char.chr (ten + n)

(* Returns the int (0..255) into a 2 char hex representation: *)
let hex_byte_of i =
  assert (i >= 0 && i <= 255) ;
  String.init 2 (function
    | 0 -> i lsr 4 |> hex_of
    | _ -> i land 15 |> hex_of)

(*$= hex_byte_of & ~printer:identity
  "00" (hex_byte_of 0)
  "01" (hex_byte_of 1)
  "0a" (hex_byte_of 10)
  "42" (hex_byte_of 66)
*)

let is_printable c =
  let open Char in
  is_letter c || is_digit c || is_symbol c

let hex_print ?(from_rb=false) ?(num_cols=16) oc bytes =
  let disp_char_of c =
    if is_printable c then c else '.'
  in
  (* [b0] was the offset at the beginning of the line while [b] is the
   * current offset.
   * [c] is the current column.
   * [l] is the length of the current record (length included) in bytes if
   * [from_rb], while [bl] is the offset into that record. [bl0] was
   * that offset at the beginning of the line. *)
  let rec aux b0 bl0 l c b bl =
    (* Sep from column c-1: *)
    let sep c =
      if c >= num_cols then ""
      else if c = 0 then "    "
      else if c land 7 = 0 then " - "
      else " " in
    (* Display the ascii section + new line: *)
    let eol () =
      if c > 0 then (
        (* Fill up to ascii section: *)
        for i = c to num_cols do
          Printf.fprintf oc "%s  " (sep i)
        done ;
        (* Ascii section: *)
        Printf.fprintf oc "  " ;
        for i = 0 to c - 1 do
          Char.print oc
            (disp_char_of (Bytes.get bytes (b0 + i)))
        done ;
        String.print oc "\n"
      )
    in
    (* Actually add an hex byte: *)
    if b >= Bytes.length bytes then (
      eol ()
    ) else (
      if c >= num_cols then (
        eol () ;
        aux b bl l 0 b bl
      ) else (
        let l, bl =
          if from_rb && bl >= l then (
            (* Read the length, and highlight it.
             * Remember that the length is the number of words, excluding
             * the length itself: *)
            let rec loop l (* in words *) bl (* in bytes *) =
              if bl > 3 || b + bl >= Bytes.length bytes then
                (* We've read the length: *) (l + 1) * 4, 0
              else
                (* Assume little endian: *)
                loop (l + Char.code (Bytes.get bytes (b + bl)) lsl (8 * bl))
                     (bl + 1) in
            loop 0 0
          ) else l, bl in
        let str = hex_byte_of (Char.code (Bytes.get bytes b)) in
        let str =
          if from_rb && bl < 4 then blue str else
          if from_rb && bl = l - 1 then yellow str else str in
        Printf.fprintf oc "%s%s" (sep c) str ;
        aux b0 bl0 l (c + 1) (b + 1) (bl + 1)))
  in
  Printf.fprintf oc "\n" ;
  aux 0 0 0 0 0 0

let fail_for_good = ref false
let rec restart_on_failure ?(while_=always) what f x =
  if !fail_for_good then
    f x
  else
    try f x
    with e ->
      print_exception e ;
      if while_ () then (
        !logger.error "Will restart %s..." what ;
        Unix.sleepf (0.5 +. Random.float 0.5) ;
        (restart_on_failure ~while_ [@tailcall]) what f x)

(* Cohttp does not enforce any scheme but we want to be friendlier with
 * user entered urls so we add one if it's missing, assuming http: *)
let sure_is_http str =
  if match String.find str "://" with
     | exception Not_found -> true
     | n -> n > 10
  then "http://" ^ str
  else str
(*$= sure_is_http & ~printer:identity
  "http://blabla.com" (sure_is_http "http://blabla.com")
  "http://blabla.com" (sure_is_http "blabla.com")
  "https://blabla.com" (sure_is_http "https://blabla.com")
 *)

let packed_string_of_int n =
  let buf = Buffer.create 8 in
  let rec loop n =
    if n = 0 then Buffer.contents buf else (
      Buffer.add_char buf (Char.chr (n land 255)) ;
      loop (n / 256) (* Beware that n is signed *))
  in
  loop n
(*$= packed_string_of_int & ~printer:identity
  "abc" (packed_string_of_int 0x636261)
  "" (packed_string_of_int 0)
 *)

let age t = Unix.gettimeofday () -. t

let option_get what = function
  | Some x -> x
  | None ->
      !logger.error "Forced the None value of %s" what ;
      invalid_arg "option_get"

let memoize f =
  let cached = ref None in
  fun () ->
    match !cached with
    | Some r -> r
    | None ->
        let r = f () in
        cached := Some r ;
        r

let cache_clean_after = 1200.
let cached2 cache_name reread time =
  (* Cache is a hash from some key to last access time, last data time,
   * and data. *)
  !logger.debug "Create a new cache for %s" cache_name ;
  let cache = Hashtbl.create 31 in
  let next_clean = ref (Unix.time () +. Random.float cache_clean_after) in
  fun k u ->
    let ret = ref None in
    let now = Unix.time () in
    Hashtbl.modify_opt k (function
      | None ->
          let t = time k u
          and v = reread k u in
          ret := Some v ;
          Some (ref now, t, v)
      | Some (a, t, v) as prev ->
          let t' = time k u in
          if t' <= t then (
            ret := Some v ;
            a := now ;
            prev
          ) else (
            let v = reread k u in
            ret := Some v ;
            Some (ref now, t', v)
          )
    ) cache ;
    (* Clean the cache every now and then *)
    if now > !next_clean then (
      Hashtbl.filter_inplace (fun (a, _, _) ->
        now -. !a  < cache_clean_after
      ) cache ;
      next_clean := now +. Random.float cache_clean_after ;
      !logger.debug "Cache size is now %d" (Hashtbl.length cache)
    ) ;
    Option.get !ret

(* Same as above without the additional user parameter: *)
let cached cache_name reread time =
  let c = cached2 cache_name (fun k () -> reread k) (fun k () -> time k) in
  fun k -> c k ()

(* Addition capped to min_int/max_int *)
let cap_add a b =
  if a > 0 && b > 0 then
    if max_int - b >= a then a + b else max_int
  else if a < 0 && b < 0 then
    if min_int - b <= a then a + b else min_int
  else a + b

(*$= cap_add & ~printer:string_of_int
  42 (cap_add 31 11)
  42 (cap_add 57 ~-15)
  42 (cap_add ~-17 59)
  max_int (cap_add (max_int - 3) 3)
  max_int (cap_add (max_int - 3) 4)
  max_int (cap_add (max_int - 3) 9)
  min_int (cap_add (min_int + 3) ~-3)
  min_int (cap_add (min_int + 3) ~-4)
  min_int (cap_add (min_int + 3) ~-9)
 *)

(* min_int cannot be negated without overflow *)
let cap_neg a = if a = min_int then max_int else ~-a

let uniquify () =
  let past = ref Set.empty in
  fun x ->
    if Set.mem x !past then false
    else (
      past := Set.add x !past ;
      true
    )
(*$= uniquify & ~printer:(IO.to_string (List.print Int.print))
  [1;2;3] (List.filter (uniquify ()) [1;1;2;3;3;2;1])
 *)

let jitter ?(amplitude=0.25) v =
  let r = (Random.float amplitude) -. amplitude /. 2. in
  v +. (v *. r)

let todo msg = failwith ("not implemented: "^ msg)

let ordinal_suffix n =
  let tens = n mod 100 in
  if tens >= 10 && tens < 20 then "th" else
  match n mod 10 with
  | 1 -> "st"
  | 2 -> "nd"
  | 3 -> "rd"
  | _ -> "th"

(* Given an array of floats, display an UTF-8 sparkline: *)
let sparkline vec =
  let stairs = [| "▁"; "▂"; "▃"; "▄"; "▅"; "▆"; "▇"; "█" |] in
  let mi, ma =
    Array.fold_left (fun (mi, ma) v ->
      min v mi, max v ma
    ) (infinity, neg_infinity) vec in
  let ratio =
    if ma > mi then
      float_of_int (Array.length stairs - 1) /. (ma -. mi)
    else 0. in
  let res = Buffer.create (Array.length vec * 4) in
  Array.iter (fun v ->
    let c = int_of_float ((v -. mi) *. ratio) in
    Buffer.add_string res stairs.(c)
  ) vec ;
  Buffer.contents res

(* All the time conversion functions below are taken from (my understanding of)
 * http://graphite-api.readthedocs.io/en/latest/api.html#from-until *)

let time_of_reltime s =
  let scale d s =
    try
      Some (
        Unix.gettimeofday () +. d *.
          (match s with
          | "s" -> 1.
          | "m" | "min" -> 60.
          | "h" -> 3600.
          | "d" -> 86400.
          | "w" -> 7. *. 86400.
          | "mon" -> 30. *. 86400.
          | "y" -> 365. *. 86400.
          | _ -> raise Exit))
    with Exit ->
      None
  in
  try Scanf.sscanf s "%f%s%!" scale
  with _ -> None

(* String interpreted in the local time zone: *)
let time_of_abstime s =
  let s = String.lowercase s in
  let scan c recv =
    try Some (Scanf.sscanf s c recv)
    with Scanf.Scan_failure _ | End_of_file | Failure _ -> None
  and eq str recv =
    if s = str then Some (recv ()) else None
  and (|||) o1 o2 =
    if o1 <> None then o1 else o2 in
  let open Unix in
  let is_past h m tm =
    h < tm.tm_hour || h = tm.tm_hour && m < tm.tm_min in
  let time_of_hh_mm h m am_pm =
    let h = match String.lowercase am_pm with
      | "am" | "" -> h
      | "pm" -> h + 12
      | _ -> raise (Scanf.Scan_failure ("Invalid AM/PM: "^ am_pm)) in
    let now = time () in
    let tm = localtime now in
    (* "If that time is already past, the next day is assumed" *)
    if is_past h m tm then now +. 86400. else now in
  let time_of_yyyy_mm_dd_h_m_s y mo d h mi s =
    let tm =
      { tm_sec = round_to_int s ; tm_min = mi ; tm_hour = h ;
        tm_mday = d ; tm_mon = mo - 1 ; tm_year = y - 1900 ;
        (* ignored: *) tm_wday = 0 ; tm_yday = 0 ; tm_isdst = false } in
    mktime tm |> fst in
  let time_of_dd_mm_yyyy d m y =
    let y = if y < 100 then y + 2000 (* ? *) else y in
    time_of_yyyy_mm_dd_h_m_s y m d 0 0 0.
  in
  (* Extracts are from `man 1 at`:
   *
   * "It accepts times of the form HHMM or HH:MM to run a job at a specific
   * time of day.  (If that time is already past, the next day is assumed.)
   * (...) and time-of-day may be suffixed with AM or PM for running in the
   * morning or the evening." *)
  (scan "%2d:%2d%s%!" time_of_hh_mm) |||
  (scan "%2d:%2d%s%!" time_of_hh_mm) |||
  (* "As an alternative, the following keywords may be specified: midnight,
   * noon, or teatime (4pm) (...)." *)
  (eq "midnight" (fun () -> time_of_hh_mm 0 0 "")) |||
  (eq "noon" (fun () -> time_of_hh_mm 12 00 "")) |||
  (eq "teatime" (* fuck you! *) (fun () -> time_of_hh_mm 16 00 "")) |||
  (* Not specified but that's actually the first Grafana will send: *)
  (eq "now" time) |||
  (* Also not specified but mere unix timestamps are actually frequent: *)
  (scan "%f%!" identity) |||
  (* "The day on which the job is to be run may also be specified by giving a
   * date in the form month-name day with an optional year," *)
  (* TODO *)
  (* "or giving a date of the forms DD.MM.YYYY, DD.MM.YY, MM/DD/YYYY, MM/DD/YY,
   * MMDDYYYY, or MMDDYY." *)
  (scan "%2d.%2d.%4d%!" time_of_dd_mm_yyyy) |||
  (scan "%2d/%2d/%4d%!" (fun m d y -> time_of_dd_mm_yyyy d m y)) |||
  (scan "%2d%2d%4d%!" (fun m d y -> time_of_dd_mm_yyyy d m y)) |||
  (* "The specification of a date must follow the specification of the time of
   * day.  Time can also be specified as: [now] + count time-units, where the
   * time-units can be minutes, hours, days, weeks, months or years and at may
   * be told to run the job today by suffixing the time with today and to run
   * the job tomorrow by suffixing the time with tomorrow.  The shortcut next
   * can be used instead of + 1." *)
  (* TODO *)
  (* And now for the only sane formats: *)
  (scan "%4d-%2d-%2d%!" (fun y m d -> time_of_yyyy_mm_dd_h_m_s y m d 0 0 0.)) |||
  (scan "%4d-%2d-%2d%[ tT]%d:%d%!" (fun y mo d _ h mi -> time_of_yyyy_mm_dd_h_m_s y mo d h mi 0.)) |||
  (scan "%4d-%2d-%2d%[ tT]%d:%d:%f%!" (fun y mo d _ h mi s -> time_of_yyyy_mm_dd_h_m_s y mo d h mi s)) |||
  None

(* mktime tm struct "is interpreted in the local time zone". Work around this
 * by dividing by 24h. *)
(*$= time_of_abstime & ~printer:(function None -> "None" | Some f -> string_of_float f)
 (Some 2218.) (BatOption.map (fun ts -> ceil (ts /. 86400.)) (time_of_abstime "28.01.1976"))
 (time_of_abstime "28.01.1976") (time_of_abstime "01/28/1976")
 (time_of_abstime "28.01.1976") (time_of_abstime "1976-01-28")
 (BatOption.map ((+.) (12.*.3600.)) (time_of_abstime "28.01.1976")) \
    (time_of_abstime "1976-01-28 12:00")
 (time_of_abstime "1976-01-28 12:00") (time_of_abstime "1976-01-28T12:00:00")
 (time_of_abstime "1976-01-28 12:00") (time_of_abstime "1976-01-28T12:00:00.1")
 (time_of_abstime "1976-01-28 12:00:01") (time_of_abstime "1976-01-28 12:00:00.9")
 (Some 1523052000.) (time_of_abstime "1523052000")
 (Some 10.) (time_of_abstime "10")
 *)

let time_of_graphite_time s =
  let s = String.trim s in
  let len = String.length s in
  if len = 0 then None
  else if s.[0] = '-' then time_of_reltime s
  else time_of_abstime s

(* Replace ${tuple.field} by the actual value the passed string: *)
let subst_tuple_fields =
  let open Str in
  let re =
    regexp "\\${\\(\\([_a-zA-Z0-9.]+\\)\\.\\)?\\([_a-zA-Z0-9]+\\)}" in
  fun tuples text ->
    global_substitute re (fun s ->
      let tuple_name = try matched_group 2 s with Not_found -> "" in
      let field_name = matched_group 3 s in
      let tot_name =
        tuple_name ^ (if tuple_name <> "" then "." else "") ^ field_name in
      let search_all () =
        try
          List.find_map (fun (_, finder) ->
            try Some (finder field_name)
            with Not_found -> None
          ) tuples
        with Not_found ->
          !logger.error "Field %S used in text substitution is not \
                         present in any tuple." field_name ;
          "??"^ field_name ^"??"
      in
      match List.find (fun (names, _finder) ->
              List.mem tuple_name names
            ) tuples with
      | exception Not_found ->
        if tuple_name = "" then search_all () else (
          !logger.error "Unknown tuple %S used in text substitution!"
            tuple_name ;
          "??"^ tot_name ^"??")
      | _, finder ->
        (* We may provide "" explicitely to force order of search but
         * then if the field is not there we still want to look for it
         * elsewhere: *)
        try finder field_name
        with Not_found ->
          if tuple_name = "" then search_all () else (
            !logger.error "Field %S used in text substitution is not \
                           present in that tuple!" tot_name ;
            "??"^ tot_name ^"??")
    ) text

(* Similarly, but for simpler identifiers without tuple prefix: a function
 * to replace a map of keys by their values in a string.
 * Keys are delimited in the string with "${" "}".
 * Used to replace notification parameters by their values. *)
let subst_dict =
  let open Str in
  let re =
    regexp "\\${\\([_a-zA-Z][-_a-zA-Z0-9]*\\)}" in
  fun dict ?(quote=identity) ?null text ->
    global_substitute re (fun s ->
      let var_name = matched_group 1 s in
      (try List.assoc var_name dict
      with Not_found ->
        !logger.debug "Unknown parameter %S" var_name ;
        null |? "??"^ var_name ^"??") |>
      quote
    ) text

(*$= subst_dict & ~printer:(fun x -> x)
  "glop 'pas' glop" \
      (subst_dict ~quote:shell_quote ["glop", "pas"] "glop ${glop} glop")
  "pas"           (subst_dict ["glop", "pas"] "${glop}")
  "??"            (subst_dict ~null:"??" ["glop", "pas"] "${gloup}")
 *)

let reindent indent s =
  indent ^ String.nreplace (String.trim s) "\n" ("\n"^indent)

(* FIXME: this won't work but for the simplest types: *)
let split_string ~sep ~opn ~cls s =
  let open String in
  let s = trim s in
  if s.[0] <> opn || s.[length s - 1] <> cls then
    failwith (Printf.sprintf "Value must be delimited with %c and %c"
                opn cls) ;
  let s = sub s 1 (length s - 2) in
  split_on_char sep s |> List.map trim |> Array.of_list

(*$= split_string & ~printer:(IO.to_string (Array.print String.print))
  [| "glop" |] (split_string ~sep:';' ~opn:'(' ~cls:')' "(glop)")
  [| "glop" |] (split_string ~sep:';' ~opn:'(' ~cls:')' "  ( glop  )  ")
  [| "pas"; "glop" |] \
    (split_string ~sep:';' ~opn:'(' ~cls:')' "(pas;glop)")
  [| "pas"; "glop" |] \
    (split_string ~sep:';' ~opn:'(' ~cls:')' "(  pas ;  glop)  ")
*)

(* Helper functions: return a positive int from a string: *)
let unsigned_of_string s o =
  let rec loop n o =
    if o >= String.length s then n, o else
    let d = Char.code s.[o] - Char.code '0' in
    if d < 0 || d > 9 then n, o else
      loop (n * 10 + d) (o + 1) in
  loop 0 o

(*$= unsigned_of_string & ~printer:(IO.to_string (Tuple2.print Int.print Int.print))
  (4, 1)   (unsigned_of_string "4" 0)
  (4, 2)   (unsigned_of_string "x4" 1)
  (4, 2)   (unsigned_of_string "x4y" 1)
  (417, 3) (unsigned_of_string "417" 0)
  (417, 4) (unsigned_of_string "x417" 1)
  (417, 4) (unsigned_of_string "x417y" 1)
*)

let unsigned_of_hexstring s o =
  let rec loop n o =
    if o >= String.length s then n, o else
    let d = Char.code s.[o] in
    if d >= Char.code '0' && d <= Char.code '9' then
      loop (n * 16 + d - Char.code '0') (o + 1) else
    if d >= Char.code 'a' && d <= Char.code 'f' then
      loop (n * 16 + 10 + d - Char.code 'a') (o + 1) else
    if d >= Char.code 'A' && d <= Char.code 'F' then
      loop (n * 16 + 10 + d - Char.code 'A') (o + 1) else
    n, o in
  loop 0 o

(*$= unsigned_of_hexstring & ~printer:(BatIO.to_string (BatTuple.Tuple2.print BatInt.print BatInt.print))
  (4, 1)      (unsigned_of_hexstring "4" 0)
  (0xC, 1)    (unsigned_of_hexstring "c" 0)
  (0xC, 1)    (unsigned_of_hexstring "C" 0)
  (4, 2)      (unsigned_of_hexstring "x4" 1)
  (0xC, 2)    (unsigned_of_hexstring "xC" 1)
  (4, 2)      (unsigned_of_hexstring "x4y" 1)
  (0xC, 2)    (unsigned_of_hexstring "xCy" 1)
  (0x4F7, 3)  (unsigned_of_hexstring "4F7" 0)
  (0x4F7, 4)  (unsigned_of_hexstring "x4F7" 1)
  (0x4F7, 4)  (unsigned_of_hexstring "x4F7y" 1)
  (0x8329, 4) (unsigned_of_hexstring "8329" 0)
*)

let fail_with_context ctx f =
  try f () with e ->
    Printf.sprintf "While %s: %s"
      ctx (Printexc.to_string e) |>
    failwith

let rec reach_fixed_point ?max_try f =
  match max_try with Some n when n <= 0 -> false
  | _ ->
    if f () then (
      !logger.debug "Looping to reach fixed point" ;
      reach_fixed_point ?max_try:(Option.map pred max_try) f
    ) else true

(* How many ways to choose n undistinguishable things in a set of m *)
let comb n m =
  assert (n <= m) ;
  let rec loop num den i =
    if i > n then num /. den else
    loop
      (num *. float_of_int (m + 1 - i))
      (den *. float_of_int i)
      (i + 1) in
  loop 1. 1. 1
(*$= comb & ~printer:string_of_float
  1. (comb 0 10)
  1. (comb 10 10)
  2_598_960. (comb 5 52)
 *)

(* TODO: should go in batteries *)
let option_map2 f o1 o2 =
  match o1, o2 with
  | None, _ | _, None -> None
  | Some a, Some b -> Some (f a b)

(* To circumvent short-cuts *)
let (|||) = (||)

let pretty_enum_print p oc e =
  let rec loop first x =
    match Enum.get e with
    | None ->
        Printf.fprintf oc "%s%a" (if first then "" else " and ") p x
    | Some next ->
        Printf.fprintf oc "%s%a" (if first then "" else ", ") p x ;
        loop false next in
  match Enum.get e with
  | None -> String.print oc "<empty>"
  | Some x -> loop true x

let pretty_list_print p oc =
  pretty_enum_print p oc % List.enum

let pretty_array_print p oc =
  pretty_enum_print p oc % Array.enum

let pretty_set_print p oc =
  pretty_enum_print p oc % Set.enum

(* Return the distance (as a float) between two values of the same type: *)
module Distance = struct
  let float a b = abs_float (a -. b)

  let string a b =
    (* TODO *)
    String.length a - String.length b |> float_of_int
end

let string_of_sockaddr addr =
  let open Unix in
  match addr with
  | ADDR_UNIX file ->
      "UNIX:"^ file
  | ADDR_INET (addr, port) ->
      string_of_inet_addr addr ^":"^ string_of_int port

(* We need an accept that stops waiting whenever the while_ condition become
 * false. It is not enough to check while_ on EINTR since the actual signal handler
 * will be resumed and the OCaml signal handler might not have run yet when
 * the accept is interrupted by EINTR. So we have to summon select.
 * raise Exit when while_ says so. *)
let rec my_accept ~while_ sock =
  let open Legacy.Unix in
  match restart_on_eintr ~while_ (select [sock] [] []) 0.5 with
  | [], _, _ ->
      if while_ () then my_accept ~while_ sock
      else raise Exit
  | _ ->
      restart_on_eintr ~while_ (accept ~cloexec:true) sock

(* This is a version of [Unix.establish_server] that pass the file
 * descriptor instead of buffered channels. Also, we want a way to stop
 * the server: *)
let forking_server ~while_ ~service_name sockaddr server_fun =
  let open Legacy.Unix in
  let sock =
    socket ~cloexec:true (domain_of_sockaddr sockaddr) SOCK_STREAM 0 in
  (* Keep an eye on my sons pids: *)
  let sons = Atomic.Set.make () in
  let killer_thread =
    Thread.create (fun () ->
      !logger.debug "Starting killer thread..." ;
      let stop_since = ref 0. in
      while true do
        log_and_ignore_exceptions ~what:"Killer thread" (fun () ->
          let now = gettimeofday () in
          let continue = while_ () in
          (* If we want to quit, kill the sons: *)
          if not continue then (
            if !stop_since = 0. then stop_since := now ;
            if not (Atomic.Set.is_empty sons) then (
              !logger.debug "Killing %d %s servers..."
                (Atomic.Set.cardinal sons) service_name ;
              Atomic.Set.iter sons (fun (pid, _) ->
                !logger.info "Killing %d" pid ;
                let what =
                  Printf.sprintf "stopping %s servers" service_name in
                let signal =
                  let open Sys in
                  if now -. !stop_since > 3. then sigkill else sigterm in
                log_and_ignore_exceptions ~what
                  (kill pid) signal)
            ) else (
              !logger.debug "Quit killer thread" ;
              Thread.exit ()
            )
          ) ;
          (* Collect the sons statuses: *)
          Atomic.Set.filter sons
            (fun (pid, start) ->
              match BatUnix.restart_on_EINTR (waitpid [ WNOHANG ]) pid with
              | 0, _ ->
                  true
              | _, status ->
                  let dt = now -. start in
                  (if status = WEXITED 0 then !logger.info else !logger.error)
                    "%s server %d %s after %fs"
                      service_name pid (string_of_process_status status) dt ;
                  false) ;
          sleep 1) ()
      done
    ) () in
  (* Now fork a new server for each new connection: *)
  finally
    (fun () -> close sock)
    (fun () ->
      setsockopt sock SO_REUSEADDR true ;
      bind sock sockaddr ;
      listen sock 5 ;
      while while_ () do
        match my_accept ~while_ sock with
        | exception Exit ->
            !logger.debug "%s stops accepting connections." service_name
        | s, _caller ->
            (* Before forking, advance the PRNG so that all children do not
             * re-init their own PRNG with the same number: *)
            let prng_init = Random.bits () in
            flush_all () ;
            (match fork () with
            | 0 ->
                let what = "forked "^ service_name ^" server" in
                (try
                  (* Server process must not escape this scope or it would
                   * try to use sock again! *)
                  Random.init prng_init ;
                  close sock ;
                  (try
                    server_fun s ;
                    exit 0
                  with End_of_file ->
                      !logger.info "%s: client disconnected, exiting" what ;
                      exit 0
                    | Exit ->
                      !logger.info "%s: time to quit" what ;
                      exit 0)
                with e ->
                  print_exception ~what e ;
                  exit ExitCodes.forking_server_uncaught_exception)
            | pid ->
                close s ;
                !logger.info "Forked server with pid %d" pid ;
                Atomic.Set.add sons (pid, Unix.gettimeofday ()))
      done
    ) () ;
  !logger.info "Waiting for killer thread to finish..." ;
  Thread.join killer_thread ;
  !logger.debug "Killed thread finished!"

let cap ?min ?max f =
  let f = Option.map_default (Pervasives.max f) f min in
  Option.map_default (Pervasives.min f) f max

(*$= cap & ~printer:string_of_int
  2 (cap ~min:1 ~max:3 2)
  1 (cap ~min:1 ~max:3 0)
  0 (cap ~max:3 0)
  3 (cap ~min:1 ~max:3 5)
  5 (cap ~min:1 5)
*)

let strip_control_chars =
  let open Str in
  let res =
    [ regexp "[\n\r] *", " " ;
      regexp "\t", "    " ;
      regexp "\027\\[[0-9];[0-9]+m", "" ;
      regexp "\027\\[0m", "" ] in
  fun msg ->
    List.fold_left (fun s (re, repl) ->
      Str.global_replace re repl s
    ) msg res

(* Return whether we are _below_ the rate limit *)
let rate_limit max_events duration =
  let last_period = ref 0
  and count = ref 0 in
  fun () ->
    let now = Unix.time () in
    let period = int_of_float (now /. duration) in
    if period = !last_period && !count >= max_events then false else (
      if period = !last_period then (
        incr count
      ) else (
        last_period := period ;
        count := 1
      ) ;
      true)

let string_same_pref l a b =
  if l > String.length a || l > String.length b then false
  else
    try
      for i = 0 to l - 1 do
        if a.[i] <> b.[i] then raise Exit
      done ;
      true
    with Exit -> false

let as_date ?rel ?(right_justified=true) t =
  let full = string_of_time t in
  match rel with
  | None -> full
  | Some rel ->
      let possible_cuts = [| 11; 14; 17 |] in
      let rec loop i =
        if i < 0 then
          full
        else (
          let pref_len = possible_cuts.(i) in
          if string_same_pref pref_len rel full then
            (if right_justified then String.make pref_len ' ' else "")^
            String.lchop ~n:pref_len full
          else
            loop (i - 1)
        ) in
      loop (Array.length possible_cuts - 1)

(*$= as_date & ~printer:(fun x -> x)
  "2018-11-14T22:13:20" (as_date ~rel:"" 1542230000.)
  "2018-11-14T22:13:20" (as_date ~rel:"1983-11-14T22:13:20" 1542230000.)
  "           22:13:20" (as_date ~rel:"2018-11-14T08:12:32" 1542230000.)
  "              13:20" (as_date ~rel:"2018-11-14T22:12:20" 1542230000.)
*)

(* A pretty printer for timestamps, with the peculiarity that it tries to not
 * repeat the date components that have already been written, saved in [rel]. *)
let print_as_date ?rel ?right_justified oc t =
  let s = as_date ?rel:(Option.map (!) rel) ?right_justified t in
  Option.may (fun rel -> rel := s) rel ;
  String.print oc s

let print_as_duration oc d =
  String.print oc (string_of_duration d)

(*
 * Some graph utilities
 *)

(* Given a set of edges, return the path (reverted) between any two vertices
 * or raise Not_found.
 * It is assumed that, as is the case with the running config or the graph
 * of builders, the graph is mostly a tree.
 * Edges are given as a folder passing each vertex and its "descendants"
 * as the vertex identifier and vertex content. *)
type ('id, 'vtx) fold_t =
  { fold : 'usr. 'id -> ('usr -> 'id -> 'vtx -> 'usr) -> 'usr -> 'usr }

let path_in_graph fold ?(max_len=50) ~src ~dst =
  (* Complete the given path towards [dst], return both the path (reverted)
   * and its length (which is not larger than max_len): *)
  let rec loop max_len prev prev_len id =
    !logger.debug "Looking for a path from %S to %S of max length %d"
      (dump id) (dump dst) max_len ;
    if prev_len > max_len then failwith "Path too long"
    else if id = dst then prev, prev_len
    else (
      (* Try each edge: *)
      let best_path_opt =
        fold.fold id (fun prev_best_opt id' v ->
          match loop (max_len - 1) (v :: prev) (prev_len + 1) id' with
          | exception _ -> prev_best_opt
          | _, path_len as res ->
              if match prev_best_opt with
                 | None -> true
                 | Some (_, best_len) -> path_len < best_len
              then Some res
              else prev_best_opt
        ) None
      in
      match best_path_opt with
      | None ->
          Printf.sprintf "No path from %s to %s" (dump src) (dump dst) |>
          failwith
      | Some (path, path_len) ->
          path, path_len)
  in
  let path_rev, _ = loop max_len [] 0 src in
  path_rev

(* Used to abbreviate file paths as well as program names: *)
let abbrev_path ?(max_length=20) ?(known_prefix="") path =
  let known_prefix =
    if String.length known_prefix > 0 &&
       known_prefix.[String.length known_prefix - 1] <> '/'
    then known_prefix ^"/"
    else known_prefix in
  let path =
    if String.starts_with path known_prefix then
      String.lchop ~n:(String.length known_prefix) path
    else path in
  let rec loop abb rest =
    if String.length rest < 1 || rest.[0] = '.' ||
       String.length abb + String.length rest <= max_length
    then
      abb ^ rest
    else
      if rest.[0] = '/' then loop (abb ^"/") (String.lchop rest)
      else
        match String.index rest '/' with
        | exception Not_found ->
            abb ^ rest
        | n ->
            loop (abb ^ String.of_char rest.[0]) (String.lchop ~n rest)
  in loop "" path
(*$= abbrev_path & ~printer:(fun x -> x)
  "/a/b/c/glop" (abbrev_path "/a very long name/before another very long one/could be reduced to/glop")
  "/a/b/c/glop" (abbrev_path ~known_prefix:"/tmp" "/a very long name/before another very long one/could be reduced to/glop")
  "a/b/c/glop" (abbrev_path ~known_prefix:"/tmp" "/tmp/a very long name/before another very long one/could be reduced to/glop")
  "a/b/c/glop" (abbrev_path ~known_prefix:"/tmp/" "/tmp/a very long name/before another very long one/could be reduced to/glop")
  "a/b/c/glop" (abbrev_path "a very long name/before another very long one/could be reduced to/glop")
 *)


(* TODO: in batteries? *)
let hashtbl_merge h1 h2 f =
  let res = Hashtbl.create (Hashtbl.length h1) in
  let may_add_res k v1 v2 =
    match f k v1 v2 with
    | None -> ()
    | Some v -> Hashtbl.add res k v in
  Hashtbl.iter (fun k v1 ->
    match Hashtbl.find h2 k with
    | exception Not_found ->
        may_add_res k (Some v1) None
    | v2 ->
        may_add_res k (Some v1) (Some v2)
  ) h1 ;
  Hashtbl.iter (fun k v2 ->
    match Hashtbl.find h1 k with
    | exception Not_found ->
        may_add_res k None (Some v2)
    | _ -> () (* done above *)
  ) h2 ;
  res

let invalid_byte_for what x =
  !logger.error "Invalid byte 0x%0xd for %s" x what ;
  assert false

let int_of_bool b = if b then 1 else 0

let array_print_i ?first ?last ?sep p oc a =
  let i = ref 0 in
  Array.print ?first ?last ?sep (fun oc x ->
    p !i oc x ; incr i) oc a

let finally handler f x =
  let r =
    try f x
    with e ->
      let bt = Printexc.get_raw_backtrace () in
      handler () ;
      Printexc.raise_with_backtrace e bt in
  handler () ;
  r

let char_print_quoted oc = Printf.fprintf oc "%C"
