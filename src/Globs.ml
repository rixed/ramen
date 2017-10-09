open BatString
(* TODO: implement '?' for replacing a single char. (Like wants '_') *)

type pattern = { anchored_start : bool ;
                 anchored_end : bool ;
                 chunks : string list }

let compile ?(star='*') ?(escape='\\') =
  let open Str in
  let unescape =
    (* Replaces \* with *, once their interpretation as globs is over: *)
    let re = regexp (quote (of_char escape) ^ quote (of_char star)) in
    fun str -> global_replace re (of_char star) str
  (* The regexp below reads as: either at beginning of string or not
   * after a backslash, then a star. *)
  and star_re = regexp ("\\(^\\|[^"^ of_char escape ^"]\\)"^ quote (of_char star) ^"+") in
  (* We cannot use Str.split because it would consider the non-\ character
   * before the star as part of the delimiter. *)
  let my_split re s =
    let add_chunk lst s =
      if s <> "" then s::lst else lst in
    let rec loop prev o =
      match search_forward re s o with
      | exception Not_found ->
        add_chunk prev (sub s o (length s - o)) |> List.rev
      | o' ->
        assert (o' >= o) ;
        let o' = if s.[o'] <> '*' then o'+1 else o' in
        loop (add_chunk prev (sub s o (o'-o))) (match_end ()) in
    loop [] 0 in
  fun s ->
    let l = length s in
    let anchored_start = l = 0 || s.[0] <> star in
    let anchored_end =
      l = 0 ||
      s.[l-1] <> star ||
      (l >= 2 && s.[l-2] = escape) in
    { anchored_start ; anchored_end ;
      chunks = my_split star_re s |> List.map unescape }

(*$inject
  let string_of_pattern p =
    (if p.anchored_start then "^" else "") ^
    (String.concat "*" p.chunks) ^
    (if p.anchored_end then "$" else "")
*)

(*$= compile & ~printer:string_of_pattern
  { anchored_start = true ; anchored_end = false ; chunks = [ "glop" ] } \
    (compile "glop*")
  { anchored_start = true ; anchored_end = true ; chunks = [ "pas"; "glop" ] } \
    (compile "pas*glop")
  { anchored_start = true ; anchored_end = false ; chunks = [ "zzz" ] } \
    (compile "zzz**")
  { anchored_start = true ; anchored_end = true ; chunks = [ "glop"; "glop" ] } \
    (compile "glop**glop")
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

let rec match_chunks ~at_start ~at_end ?(from=0) c = function
  | [] ->
    (not at_start || from = 0) &&
    (not at_end || from = length c)
  | s :: rest ->
    (match Str.(search_forward (regexp_string s) c from) with
     | exception Not_found -> false
     | i ->
       (not at_start || i = 0) &&
       (rest <> [] || not at_end || string_ends_with c s) &&
       match_chunks ~at_start:false ~at_end ~from:(i + length s) c rest)

let matches p c =
  match_chunks ~at_start:p.anchored_start ~at_end:p.anchored_end c p.chunks

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
 *)

let has_wildcard = function
  | { chunks = [ _ ] | [] ;
      anchored_start = true ;
      anchored_end = true } -> false
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
