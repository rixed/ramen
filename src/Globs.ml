open Batteries
open String

type t =
  { chunks : chunk list ;
    star : char ; placeholder : char ; escape : char }

and chunk =
  | String of string
  | AnyString of int (* min length *)
  | AnyChar

let all =
  { chunks = [ AnyString 0 ] ;
    star = '*' ; placeholder = '?' ; escape = '\\' }

(* Print chunks as OCaml values: *)
let print_chunk_ocaml oc = function
  | String s -> Printf.fprintf oc "String %S" s
  | AnyString n -> Printf.fprintf oc "AnyString %d" n
  | AnyChar -> String.print oc "AnyChar"

let print_pattern_ocaml oc p =
  Printf.fprintf oc
    "{ chunks = %a ; star = %C ; placeholder = %C ; escape = %C }"
    (List.print print_chunk_ocaml) p.chunks
    p.star p.placeholder p.escape

(* Print chunks back to a pattern: *)
let rec print_chunk_pattern ~star ~placeholder ~escape oc = function
  | AnyString 0 -> Char.print oc star
  | AnyString n ->
      Char.print oc placeholder ;
      print_chunk_pattern ~star ~placeholder ~escape oc (AnyString (n-1))
  | AnyChar -> Char.print oc placeholder
  | String s ->
      let open String in
      let esc c = String.init 2 (function 0 -> escape | _ -> c) in
      let s = nreplace ~str:s ~sub:(of_char star) ~by:(esc star) in
      let s = nreplace ~str:s ~sub:(of_char placeholder)
                       ~by:(esc placeholder) in
      String.print oc s

let print oc p =
  List.print ~first:"" ~last:"" ~sep:""
    (print_chunk_pattern ~star:p.star ~placeholder:p.placeholder
                         ~escape:p.escape) oc p.chunks

let decompile p =
  IO.to_string print p

let compile ?(star='*') ?(placeholder='?') ?(escape='\\') =
  (* It matters that Str is opened *after* String so quote is Str.quote: *)
  let open Str in
  let unescape =
    (* Replaces \* and \? with * and ?, once their interpretation as globs
     * is over: *)
    let re c = regexp (quote (of_char escape) ^ quote (of_char c)) in
    let re1 = re star and re2 = re placeholder in
    fun str ->
      global_replace re1 (of_char star) str |>
      global_replace re2 (of_char placeholder)
  (* The regexp below reads as: either at beginning of string or not
   * after a backslash, then either (some) * or (one) ?: *)
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
      match Str.search_forward split_re s 0 with
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
    let rec aux prev = function
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
    let chunks = aux [] (split [] s) in
    { chunks ; star ; placeholder ; escape }

(*$= compile & ~printer:(BatIO.to_string (BatList.print print_chunk_ocaml))
  [ String "glop" ; AnyString 0 ] (compile "glop*").chunks
  [ String "glop*" ] (compile "glop\\*").chunks
  [ String "pas" ; AnyString 0 ; String "glop" ] (compile "pas*glop").chunks
  [ String "zzz" ; AnyString 0 ] (compile "zzz**").chunks
  [ String "glop" ; AnyString 0 ; String "glop" ] (compile "glop**glop").chunks
  [] (compile "").chunks
  [ AnyChar ] (compile "?").chunks
  [ AnyChar ; String "lop" ] (compile "?lop").chunks
  [ String "glo" ; AnyChar ] (compile "glo?").chunks
  [ String "gl" ; AnyChar ; String "p" ] (compile "gl?p").chunks
  [ String "gl" ; AnyString 1 ; String "p" ] (compile "gl?*p").chunks
*)

(* Make the given string a glob that matches only itself,
 * by replacing any literal stars by escaped stars: *)
let escape ?(star='*') ?placeholder ?(escape='\\') =
  let open Str in
  let re = regexp (quote (of_char star)) in
  fun str ->
    global_replace re (of_char escape ^ of_char star) str |>
    compile ~star ?placeholder ~escape

let string_ends_with s e =
  let off = length s - length e in
  if off < 0 then false else
  let rec loop i =
    if i >= length e then true else
    if s.[off+i] <> e.[i] then false else
    loop (i+1) in
  loop 0

let char_eq cs c1 c2 =
  let to_lower = Char.lowercase_ascii in
  if cs then c1 = c2 else to_lower c1 = to_lower c2

(* Tells if c.[i..] matches s: *)
let substring_match cs c i s =
  if length s > length c - i then false
  else
    try
      for j = 0 to length s - 1 do
        if not (char_eq cs s.[j] c.[i + j]) then raise Exit
      done ;
      true
    with Exit ->
      false

let rec match_chunks cs c from len = function
  | [] ->
      from >= len
  | [ AnyString m ] ->
      len - from >= m
  | AnyChar :: rest ->
      from < len &&
      match_chunks cs c (from + 1) len rest
  | AnyString m :: String s :: rest as chunks ->
      assert (length s > 0) ;
      (* TODO: precompute that regexp in the String constructor *)
      let re = Str.(if cs then regexp_string else regexp_string_case_fold) s in
      (match Str.(search_forward re c (from + m)) with
      | exception Not_found -> false
      | i ->
        (* Either the substring at i is the one we wanted to match: *)
        match_chunks cs c (i + length s) len rest ||
        (* or it is still part of the star and we should look further away: *)
        match_chunks cs c (i + 1) len chunks)
  | String s :: rest ->
      assert (length s > 0) ;
      substring_match cs c from s &&
      match_chunks cs c (from + length s) len rest
  | AnyString _ :: AnyString _ :: _
  | AnyString _ :: AnyChar :: _ ->
      assert false

let matches ?(case_sensitive=false) p c =
  match_chunks case_sensitive c 0 (length c) p.chunks

(*$= matches & ~printer:string_of_bool
  true  (matches (compile "") "")
  false (matches (compile "") "glop glop")
  true  (matches (compile "*") "glop glop")
  false (matches (compile "\\*") "glop glop")
  true  (matches (compile "glop") "glop")
  true  (matches (compile "gLOp") "glop")
  false (matches ~case_sensitive:true (compile "gLOp") "glop")
  false (matches (compile "glop") "pas glop")
  false (matches (compile "glop") "glo")
  false (matches (compile "glop") "lop")
  false (matches (compile "glop") "")
  true  (matches (compile "glop*") "glop")
  true  (matches (compile "glop*") "glop glop")
  true  (matches (compile "GLOP*") "glop glop")
  false (matches ~case_sensitive:true (compile "GLOP*") "glop glop")
  false (matches (compile "glop\\*") "glop glop")
  false (matches (compile "glop*") "pas glop")
  false (matches (compile "*glop") "")
  true  (matches (compile "*glop") "glop")
  true  (matches (compile "*gLOp") "glop")
  false (matches ~case_sensitive:true (compile "*gLOp") "glop")
  false (matches (compile "\\*glop") "glop")
  false (matches (compile "*glop") "glop foo")
  true  (matches (compile "*glop") "pas glop")
  false (matches (compile "\\*glop") "pas glop")
  false (matches (compile "*glop*") "")
  false (matches (compile "*glop*") "foo")
  true  (matches (compile "*glop*") "glop")
  true  (matches (compile "*glop*") "pas glop")
  true  (matches (compile "*gLOp*") "pas glop")
  false (matches ~case_sensitive:true (compile "*gLOp*") "pas glop")
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
  false (matches (compile "foo\\*") "foobar")
 *)

let matches_substring ?(case_sensitive=false) p c o l =
  match_chunks case_sensitive c o l p.chunks

(* Tells if the pattern has any kind of wildcard (`*` or `?`) *)
let has_wildcard p =
  match p.chunks with
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

let concat t1 t2 =
  if t1.star <> t2.star ||
     t1.placeholder <> t2.placeholder ||
     t1.escape <> t2.escape
  then
    invalid_arg "Glob.concat" ;
  match t1.chunks, t2.chunks with
  | [], _ -> t2
  | _, [] -> t1
  | c1, (h2::rest as c2) ->
      let chunks =
        match List.split_at (List.length c1 - 1) c1, h2 with
        | (c1, [ String s1 ]), String s2 ->
            c1 @ (String (s1 ^ s2) :: rest)
        | (c1, [ AnyString l1]), AnyString l2 ->
            c1 @ (AnyString (l1 + l2) :: rest)
        | (c1, [ AnyChar ]), AnyString l2 ->
            c1 @ (AnyString (1 + l2) :: rest)
        | (c1, [ AnyString l1 ]), AnyChar ->
            c1 @ (AnyString (l1 + 1) :: rest)
        | _ ->
            c1 @ c2 in
      { t1 with chunks }

(*$= concat & ~printer:string_of_bool
  true  (matches (concat (compile "") (compile "")) "")
  false (matches (concat (compile "") (compile "")) "glop glop")
  true  (matches (concat (compile "*") (compile "")) "glop glop")
  true  (matches (concat (compile "") (compile "*")) "glop glop")
  false (matches (concat (compile "") (compile "\\*")) "glop glop")
  true  (matches (concat (compile "") (compile "glop")) "glop")
  true  (matches (concat (compile "glop") (compile "")) "glop")
  true  (matches (concat (compile "gl") (compile "op")) "glop")
  false (matches (concat (compile "gl") (compile "op")) "pas glop")
  false (matches (concat (compile "gl") (compile "op")) "glo")
*)

let extract_prefix p =
  match p.chunks with
  | String s :: chunks -> s, { p with chunks }
  | _ -> "", p
