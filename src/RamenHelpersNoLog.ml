open Batteries
open Stdint
module Atomic = RamenAtomic

(*$inject open Batteries *)

(* Overwrite Batteries' with better reraise and logs: *)
let finally handler f x =
  let r =
    try f x
    with e ->
      let bt = Printexc.get_raw_backtrace () in
      handler () ;
      Printexc.raise_with_backtrace e bt in
  handler () ;
  r

(* Avoid to create a new while_ at each call: *)
let always _ = true

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

let reldiff a b =
  let diff = abs_float (a -. b)
  and scale = max (abs_float a) (abs_float b) in
  if scale = 0. then 0. else diff /. scale

(*$= reldiff & ~printer:string_of_float
  1. (reldiff 0. 5.)
  1. (reldiff 5. 0.)
  0. (reldiff 5. 5.)
  0. (reldiff 0. 0.)
  0.5 (reldiff 1. 2.)
*)
(*$Q reldiff
  (Q.pair Q.float Q.float) (fun (a, b) -> reldiff a b = reldiff b a)
 *)

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
  "0"        (nice_string_of_float 0.)
*)

let print_nice_float oc f =
  String.print oc (nice_string_of_float f)

let shell_quote s =
  "'"^ String.nreplace s "'" "'\\''" ^"'"

let sql_quote s =
  "'"^ String.nreplace s "'" "''" ^"'"

let ramen_quote = sql_quote

let json_quote s =
  let q sub by str = String.nreplace ~str ~sub ~by in
  "\""^ (s |> q "\\" "\\\\" |> q "\"" "\\\"" |> q "/" "\\/" |> q "\b" "\\b" |>
              q "\n" "\\n" |> q "\r" "\\r" |> q "\t" "\\t" |>
              q "\x0c" "\\f") ^"\""

(*$= json_quote & ~printer:identity
 "\"foo\\\"bar\"" (json_quote "foo\"bar")
 "\"foo\\\\bar\"" (json_quote "foo\\bar")
 "\"foo\\nbar\"" (json_quote "foo\nbar")
*)

(* TODO: add to batteries *)
let set_iteri f s =
  Set.fold (fun e i -> f i e ; i + 1) s 0 |>
  ignore

(* FIXME: could be faster :) *)
let set_is_singleton s =
  Set.cardinal s = 1

(* FIXME: is_singleton and cardinal_greater in batteries *)
let set_int_is_singleton s =
  Set.Int.cardinal s = 1

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

let list_find_map_opt f l =
  try Some (List.find_map f l) with Not_found -> None

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

let rec list_longer_than n lst =
  if n < 0 then true else
  match lst with
  | [] -> false
  | _ :: lst -> list_longer_than (n - 1) lst

let rec list_shorter_than n lst =
  not (list_longer_than (n -  1) lst)

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

let result_print p_ok p_err oc = function
  | Result.Ok x -> Printf.fprintf oc "Ok(%a)" p_ok x
  | Result.Bad x -> Printf.fprintf oc "Bad(%a)" p_err x

let on_error k f =
  try f ()
  with e ->
    let bt = Printexc.get_raw_backtrace () in
    k () ;
    Printexc.raise_with_backtrace e bt

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
      (loop [@tailcall]) (o1 + 1) (o2 + 1) (len - 1)
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

let string_is_numeric s =
  try
    ignore (int_of_string s) ;
    true
  with Failure _ ->
    false

(* Former versions of Batteries used to return [] when splitting the empty
 * string, while newer return [""]. Let's pretend we are using a recent
 * version: *)
let string_nsplit str sep =
  let l = String.nsplit str sep in
  if l = [] then [""] else l
let string_split_on_char c str =
  let l = String.split_on_char c str in
  if l = [] then [""] else l

let rec string_skip_blanks s o =
  if o < String.length s && Char.is_whitespace s.[o] then
    string_skip_blanks s (o + 1)
  else o

(* Similar to string_sub_eq but for bytes: *)
let bytes_sub_eq s1 o1 s2 o2 len =
  let rec loop o1 o2 len =
    len <= 0 ||
    (
      Bytes.get s1 o1 = Bytes.get s2 o2 &&
      (loop [@tailcall]) (o1 + 1) (o2 + 1) (len - 1)
    ) in
  (o1 + len <= Bytes.length s1) &&
  (o2 + len <= Bytes.length s2) &&
  loop o1 o2 len

let bytes_of_enum = Bytes.of_string % String.of_enum
let bytes_to_enum = String.enum % Bytes.to_string
let bytes_of_list = bytes_of_enum % List.enum
let bytes_to_list = List.of_enum % bytes_to_enum

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

(*
 * Some Unix utilities
 *)

let rec restart_on_eintr ?(while_=always) f x =
  let open Unix in
  try f x
  with Unix_error (EINTR, _, _) ->
    if while_ () then restart_on_eintr ~while_ f x
    else raise Exit

let set_signals sigs behavior =
  List.iter (fun s ->
    Sys.set_signal s behavior
  ) sigs

(* Trick from LWT: how to exit without executing the at_exit hooks: *)
external sys_exit : int -> 'a = "caml_sys_exit"

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

let random_port () =
  let min_port = 1024 in
  min_port + Random.int (65535 - min_port)

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
  let s, d = aux "" d 86400. "d" in
  if d = 0. && s <> "" then s else
  let s, d = aux s d 3600. "h" in
  if d = 0. && s <> "" then s else
  let s, d = aux s d 60. "m" in
  if d = 0. && s <> "" then s else
  s ^ nice_string_of_float d ^ "s"

(*$= string_of_duration & ~printer:identity
  "1d10m" (string_of_duration 87000.)
  "10s" (string_of_duration 10.)
  "0s" (string_of_duration 0.)
*)

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

(* TODO: add those to BatChar.is_symbol? *)
let is_missing_symbol = function
  | '(' | ')' | '[' | ']' | '{' | '}' | ';'
  | '\'' | '"' | ',' | '.' | '_' |  ' ' ->
      true
  | _ ->
      false

let is_printable c =
  let open Char in
  is_letter c || is_digit c || is_symbol c || is_missing_symbol c

let with_colors = ref true

let colored ansi s =
  if !with_colors then
    Printf.sprintf "\027[%sm%s\027[0m" ansi s
  else
    Printf.sprintf "%s" s

let red = colored "1;31"
let green = colored "1;32"
let yellow = colored "1;33"
let blue = colored "1;34"
let magenta = colored "1;35"
let cyan = colored "1;36"
let white = colored "1;37"
let gray = colored "2;37"

let hex_print ?(from_rb=false) ?(num_cols=16) bytes oc =
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

let memoize f =
  let cached = ref None in
  fun () ->
    match !cached with
    | Some r -> r
    | None ->
        let r = f () in
        cached := Some r ;
        r

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
  string_split_on_char sep s |> List.map trim |> Array.of_list

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

let pretty_enum_print ?(uppercase=false) p oc e =
  let and_ = if uppercase then " AND " else " and " in
  let rec loop first x =
    match Enum.get e with
    | None ->
        Printf.fprintf oc "%s%a" (if first then "" else and_) p x
    | Some next ->
        Printf.fprintf oc "%s%a" (if first then "" else ", ") p x ;
        loop false next in
  match Enum.get e with
  | None -> String.print oc "<empty>"
  | Some x -> loop true x

let pretty_list_print ?uppercase p oc =
  pretty_enum_print ?uppercase p oc % List.enum

let pretty_array_print ?uppercase p oc =
  pretty_enum_print ?uppercase p oc % Array.enum

let pretty_set_print ?uppercase p oc =
  pretty_enum_print ?uppercase p oc % Set.enum

(* Return the distance (as a float) between two values of the same type: *)
module Distance = struct
  let float a b = abs_float (a -. b)

  let string a b =
    if a = b then 0.0 else
    (* FIXME: better string distance. Meanwhile, must always return a
     * value > 1e-7 *)
    1 + abs(String.length a - String.length b) |> float_of_int

  let some_int sub to_float a b =
    (* Beware that abs won't work on unsigned types *)
    let d = if a > b then sub a b else sub b a in
    to_float d

  let int64 = Int64.(some_int sub to_float)
  let uint64 = Uint64.(some_int sub to_float)
  let int128 = Int128.(some_int sub to_float)
  let uint128 = Uint128.(some_int sub to_float)

  let char c1 c2 =
    let foc = float_of_int % Char.code in
    float (foc c1) (foc c2)
end

let string_of_sockaddr addr =
  let open Unix in
  match addr with
  | ADDR_UNIX file ->
      "UNIX:"^ file
  | ADDR_INET (addr, port) ->
      string_of_inet_addr addr ^":"^ string_of_int port

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
let rate_limiter max_events duration =
  let last_period = ref 0
  and count = ref 0 in
  fun ?now () ->
    let now = Option.default_delayed Unix.time now in
    let period = int_of_float (now /. duration) in
    if period = !last_period && !count >= max_events then false else (
      if period = !last_period then (
        incr count
      ) else (
        last_period := period ;
        count := 1
      ) ;
      true)

let rate_limit max_rate =
  let last_sec = ref 0 and count = ref 0 in
  fun now ->
    let sec = int_of_float now in
    if sec = !last_sec then (
      incr count ;
      !count > max_rate
    ) else (
      last_sec := sec ;
      count := 0 ;
      false
    )

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
let print_as_date_rel ?rel ?right_justified oc t =
  let s = as_date ?rel:(Option.map (!) rel) ?right_justified t in
  Option.may (fun rel -> rel := s) rel ;
  String.print oc s

let print_as_date oc t =
  print_as_date_rel ?rel:None ?right_justified:None oc t

let print_as_duration oc d =
  String.print oc (string_of_duration d)

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

(* TODO: in batteries? *)
let hashtbl_take_option h k =
  let ret = ref None in
  Hashtbl.modify_opt k (function
    | None -> None
    | v -> ret := v ; None
  ) h ;
  !ret

let alist_of_hashtbl h =
  Hashtbl.enum h |> List.of_enum

let hashtbl_of_alist l =
  List.enum l |> Hashtbl.of_enum

let hashtbl_to_alist h =
  Hashtbl.enum h |> List.of_enum

(* Does not support multivalued hashtbls *)
let hashtbl_eq eqv h1 h2 =
  if Hashtbl.length h1 <> Hashtbl.length h2 then false else
  try
    Hashtbl.iter (fun k1 v1 ->
      let v2 = Hashtbl.find h2 k1 in
      if not (eqv v1 v2) then raise Exit
    ) h1 ;
    true
  with Exit | Not_found ->
    false

let int_of_bool b = if b then 1 else 0

let array_print_i ?first ?last ?sep p oc a =
  let i = ref 0 in
  Array.print ?first ?last ?sep (fun oc x ->
    p !i oc x ; incr i) oc a

let char_print_quoted oc = Printf.fprintf oc "%C"

let ignore1 = ignore
let ignore2 _ _ = ()
let ignore3 _ _ _ = ()
let ignore4 _ _ _ _ = ()
let ignore5 _ _ _ _ _ = ()
let ignore6 _ _ _ _ _ _ = ()
let ignore7 _ _ _ _ _ _ _ = ()
let ignore8 _ _ _ _ _ _ _ _ = ()
let ignore9 _ _ _ _ _ _ _ _ _ = ()

let sq x = x *. x

(* Fails if the passed float is NaN/infinite: *)
let check_finite_float what v =
  if Float.is_special v then
    Printf.sprintf2 "Invalid value (%a) in %s" Float.print v what |>
    failwith

let check_not_nan what v =
  if Float.is_nan v then
    Printf.sprintf "NaN in %s" what |>
    failwith

let with_lock m f =
  Mutex.lock m ;
  finally (fun () -> Mutex.unlock m)
    f ()

let wait_condition cond lock f =
  with_lock lock (fun () ->
    let rec loop () =
      Condition.wait cond lock ;
      if not (f ()) then loop () in
    loop ())
