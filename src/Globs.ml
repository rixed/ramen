open Batteries
open String

type chunk =
  | String of string
  | AnyString of int (* min length *)
  | AnyChar

let print_chunk oc = function
  | String s -> Printf.fprintf oc "String %S" s
  | AnyString n -> Printf.fprintf oc "AnyString %d" n
  | AnyChar -> String.print oc "AnyChar"

type pattern = chunk list

let compile ?(star='*') ?(placeholder='_') ?(escape='\\') =
  (* It matters that Str is opened *after* String so quote is Str.quote: *)
  let open Str in
  let unescape =
    (* Replaces \* and \_ with * and _, once their interpretation as globs
     * is over: *)
    let re c = regexp (quote (of_char escape) ^ quote (of_char c)) in
    let re1 = re star and re2 = re placeholder in
    fun str ->
      global_replace re1 (of_char star) str |>
      global_replace re2 (of_char placeholder)
  (* The regexp below reads as: either at beginning of string or not
   * after a backslash, then either (some) * or (one) _: *)
  and split_re =
    let re_s =
      "\\(^\\|[^"^ quote (of_char escape) ^"]\\)"^
      "\\("^ quote (of_char star) ^"+\\|"^ quote (of_char placeholder) ^"\\)" in
    regexp re_s
  in
  (* We cannot use Str.split because it would consider the non-\ character
   * before the star as part of the delimiter.
   * Note: split return the list of pieces _reverted_: *)
  let rec split prev s =
    if s = "" then prev else (
      match search_forward split_re s 0 with
      | exception Not_found ->
        String (unescape s) :: prev
      | o ->
        (* Skip the non-\ char before the special char: *)
        let o = if s.[o] <> star && s.[o] <> placeholder then
          o + 1 else o in
        let spec =
          if s.[o] = star then AnyString 0 else
          (assert (s.[o] = placeholder) ; AnyChar) in
        (* Have to call match_end before unescape! *)
        let n = match_end () in
        let prev' =
          spec :: String (unescape (sub s 0 o)) :: prev in
        split prev' (lchop ~n s)) in
  fun s ->
    let rec aux prev rest =
      match rest with
      | [] ->
          prev
      | String "" :: rest ->
          aux prev rest
      | x :: String "" :: rest ->
          aux prev (x :: rest)
      | AnyString m1 :: AnyString m2 :: rest ->
          aux prev (AnyString (m1 + m2) :: rest)
      | AnyString m :: AnyChar :: rest
      | AnyChar :: AnyString m :: rest ->
          aux prev (AnyString (m + 1) :: rest)
      | x :: rest ->
          aux (x :: prev) rest in
    aux [] (split [] s)

(*$= compile & ~printer:(BatIO.to_string (BatList.print print_chunk))
  [ String "glop" ; AnyString 0 ] (compile "glop*")
  [ String "pas" ; AnyString 0 ; String "glop" ] (compile "pas*glop")
  [ String "zzz" ; AnyString 0 ] (compile "zzz**")
  [ String "glop" ; AnyString 0 ; String "glop" ] (compile "glop**glop")
  [] (compile "")
  [ AnyChar ] (compile "_")
  [ AnyChar ; String "lop" ] (compile "_lop")
  [ String "glo" ; AnyChar ] (compile "glo_")
  [ String "gl" ; AnyChar ; String "p" ] (compile "gl_p")
  [ String "gl" ; AnyString 1 ; String "p" ] (compile "gl_*p")
*)

(* Make the given string a glob that matches only itself,
 * by replacing any literal stars by escaped stars: *)
let escape ?(star='*') ?(escape='\\') =
  let open Str in
  let re = regexp (quote (of_char star)) in
  fun str ->
    global_replace re (of_char escape ^ of_char star) str

let string_ends_with s e =
  let off = length s - length e in
  if off < 0 then false else
  let rec loop i =
    if i >= length e then true else
    if s.[off+i] <> e.[i] then false else
    loop (i+1) in
  loop 0

(* Tells if c.[i..] matches s: *)
let substring_match c i s =
  if length s > length c - i then false
  else
    try
      for j = 0 to length s - 1 do
        if s.[j] <> c.[i + j] then raise Exit
      done ;
      true
    with Exit -> false

let rec match_chunks ?(from=0) c = function
  | [] ->
      from >= length c
  | [ AnyString m ] ->
      length c - from >= m
  | AnyChar :: rest ->
      from < length c &&
      match_chunks ~from:(from + 1) c rest
  | AnyString m :: String s :: rest as chunks ->
      assert (length s > 0) ;
      (match Str.(search_forward (regexp_string s) c (from + m)) with
      | exception Not_found -> false
      | i ->
        (* Either the substring at i is the one we wanted to match: *)
        match_chunks ~from:(i + length s) c rest ||
        (* or it is still part of the star and we should look further away: *)
        match_chunks ~from:(i + 1) c chunks)
  | String s :: rest ->
      assert (length s > 0) ;
      substring_match c from s &&
      match_chunks ~from:(from + length s) c rest
  | AnyString _ :: AnyString _ :: _
  | AnyString _ :: AnyChar :: _ ->
      assert false

let matches p c = match_chunks c p

(*$= matches & ~printer:string_of_bool
  true  (matches (compile "") "")
  false (matches (compile "") "glop glop")
  true  (matches (compile "*") "glop glop")
  false (matches (compile "\\*") "glop glop")
  true  (matches (compile "glop") "glop")
  false (matches (compile "glop") "pas glop")
  false (matches (compile "glop") "glo")
  false (matches (compile "glop") "lop")
  false (matches (compile "glop") "")
  true  (matches (compile "glop*") "glop")
  true  (matches (compile "glop*") "glop glop")
  false (matches (compile "glop\\*") "glop glop")
  false (matches (compile "glop*") "pas glop")
  false (matches (compile "*glop") "")
  true  (matches (compile "*glop") "glop")
  false (matches (compile "\\*glop") "glop")
  false (matches (compile "*glop") "glop foo")
  true  (matches (compile "*glop") "pas glop")
  false (matches (compile "\\*glop") "pas glop")
  false (matches (compile "*glop*") "")
  false (matches (compile "*glop*") "foo")
  true  (matches (compile "*glop*") "glop")
  true  (matches (compile "*glop*") "pas glop")
  true  (matches (compile "*glop*") "glop foo")
  true  (matches (compile "*glop*") "pas glop foo")
  true  (matches (compile "pas*glop*") "pas foo glop")
  false (matches (compile "pas\\*glop*") "pas foo glop")
  true  (matches (compile "pas*glop*") "pas glop")
  true  (matches (compile "pas*glop*") "pas glop foo")
  false (matches (compile "pas*glop*") "foo pas glop")
  true  (matches (compile "\\*") "*")
  false (matches (compile "\\*") "+")
  true  (matches (compile "glop\\*") "glop*")
  false (matches (compile "glop\\*") "glop")
  true  (matches (compile "\\*glop") "*glop")
  true  (matches (compile "pas\\*glop") "pas*glop")
  false (matches (compile "glopz*") "glop glop")
  true  (matches (compile "glop**") "glop glop")
  true  (matches (compile "*glop") "glop glop")
  true  (matches (compile "*glop") "pas glop glop")
 *)

let has_wildcard = function
  | [] | [ String _ ] -> false
  | _ -> true

(*$= has_wildcard & ~printer:string_of_bool
  false (has_wildcard (compile ""))
  false (has_wildcard (compile "glop"))
  true  (has_wildcard (compile "pas*glop"))
  true  (has_wildcard (compile "glop*"))
  true  (has_wildcard (compile "*glop"))
  true  (has_wildcard (compile "*glop*"))
  true  (has_wildcard (compile "*"))
  true  (has_wildcard (compile "****"))
*)

let match_fold h s i f =
  let p = compile s in
  if has_wildcard p then (
    Hashtbl.fold (fun k v i ->
        if matches p k then f k v i else i) h i
  ) else (
    match Hashtbl.find h s with
    | exception Not_found -> i
    | v -> f s v i)
