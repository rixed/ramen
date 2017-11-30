(* Same HTML node definitions that are used in the back-end (for
 * server rendered charts for instance) than in the front-end (for virtual
 * DOM tree diffing). *)

(* First some helping functions: *)

let rec string_times n s =
  if n = 0 then "" else s ^ string_times (n - 1) s

let abbrev len s =
  if String.length s <= len then s else
  String.sub s 0 (len-3) ^"..."

let rec short_string_of_float f =
  if f = 0. then "0" else  (* take good care of ~-.0. *)
  if f < 0. then "-"^ short_string_of_float (~-.f) else
  (* SVG don't like digits ending with a dot *)
  let s = Printf.sprintf "%.5f" f in (* limit number of significant digits to reduce page size *)
  (* chop trailing zeros and trailing dot *)
  let rec chop last l =
    let c = s.[l] in
    if last || l < 1 || c <> '0' && c <> '.' then (
      if l = String.length s - 1 then s else
      String.sub s 0 (l + 1)
    ) else
      chop (c = '.') (l - 1) in
  chop false (String.length s - 1)

let log_base base n = log n /. log base

let grid_interval ?(base=10.) n start stop =
  let dv = stop -. start in
  (* find the round value closest to dv/n (by round we mean 1, 5, 10...) *)
  let l = dv /. float_of_int n in (* l = length if we split dv in n equal parts *)
  let f = base ** floor (log_base base l) in (* f closest power of 10 below l *)
  let i = floor (l /. f) in (* i >= 1, how much f is smaller than l *)
  if i < 2.5 || 5. *. f >= dv then f else (* if it's less than 2.5 times smaller, use it *)
  if i < 7.5 || 10. *. f >= dv then 5. *. f else (* if it's around 5 times smaller, use 5*f *)
  10. *. f

(*$Q grid_interval
  (Q.triple Q.small_int Q.float Q.pos_float) (fun (n, start, width) -> \
    n = 0 || width = 0. || \
    let interval = grid_interval n start (start +. width) in \
    interval >= 0. && interval <= width)
 *)

(* Given a range of values [start:stop], returns an enum of approximatively [n]
 * round intermediate values *)

let grid ?base n start stop =
  let interval = grid_interval ?base n start stop in
  let lo = interval *. floor (start /. interval) in
  let lo = if lo >= start then lo else lo +. interval in
  let rec loop prev i =
    if stop >= i then
      loop (i :: prev) (i +. interval)
    else List.rev prev in
  loop [] lo

(*$Q grid
  (Q.triple Q.small_int Q.float Q.pos_float) (fun (n, start, width) -> \
    n = 0 || width = 0. || \
    let stop = start +. width in \
    grid n start stop |> List.for_all (fun x -> x >= start && x <= stop))
 *)

(* Then HTML as a tree: *)

type vnode =
  | Attribute of string * string
  | Text of string
  | Element of { tag : string ; svg : bool ; action : (string -> unit) option ; subs : vnode list }
  (* Name of the parameter and function that, given this param, generate the vnode *)
  | Fun of { param : string ; f : unit -> vnode ;
             last : (int * vnode) ref }
  (* No HTML produced, just for grouping: *)
  | Group of { subs : vnode list }
  | InView (* No production, put parent in view when created *)

let rec string_of_vnode = function
  | Attribute (n, v) -> "attr("^ n ^", "^ abbrev 10 v ^")"
  | Text s -> "text(\""^ abbrev 15 s ^"\")"
  | Element { tag ; subs ; _ } -> tag ^"("^ string_of_tree subs ^")"
  | Group { subs } -> "group("^ string_of_tree subs ^")"
  | Fun { param ; _ } -> "fun("^ param ^")"
  | InView -> "in-view"
and string_of_tree subs =
  List.fold_left (fun s tree ->
    s ^ (if s = "" then "" else ";") ^ string_of_vnode tree) "" subs

let rec string_of_html ?(in_svg=false) = function
  | Attribute (n, v) -> n ^"='"^ v ^"'"
  | Text s -> s
  | Element { tag ; subs ; svg ; _ } ->
    let attrs, others = List.fold_left (fun (a, o) -> function
        | Attribute _ as n -> n::a, o
        | _ as n -> a, n::o) ([], []) subs in
    "<"^ tag ^ (if svg && not in_svg then
                  " xmlns=\"http://www.w3.org/2000/svg\" \
                    xmlns:xlink=\"http://www.w3.org/1999/xlink\""
                else "") ^
      List.fold_left (fun s a ->
      s ^" "^ string_of_html ~in_svg:svg a) "" attrs ^">"^
    string_of_htmls ~in_svg:svg others ^
    "</"^ tag ^">"
  | Group { subs } -> string_of_htmls ~in_svg subs
  | Fun { last = { contents = _, html } ; _ } ->
    string_of_html ~in_svg html
  | _ -> ""
and string_of_htmls ?in_svg h =
  List.fold_left (fun s e -> s ^ string_of_html ?in_svg e) "" h

let rec flat_length = function
  | Group { subs } ->
    List.fold_left (fun s e -> s + flat_length e) 0 subs
  | Element _ | Text _ -> 1
  | Attribute _ | InView -> 0
  | Fun { last ; _ } ->
    flat_length (snd !last)

let elmt tag ?(svg=false) ?action subs =
  Element { tag ; svg ; action ; subs }

let group subs = Group { subs }

let text s = Text s

let attr n v = Attribute (n, v)

let in_view = InView

(* We like em so much: *)
let div = elmt "div"
let clss = attr "class"
let attri n i = attr n (string_of_int i)
let attrsf n f = attr n (short_string_of_float f)
let attr_opt n =
  function None -> group [] | Some v -> attr n v
let attrsf_opt n =
  function None -> group [] | Some v -> attrsf n v
let id = attr "id"
let title = attr "title"
let span = elmt "span"
let table = elmt "table"
let thead = elmt "thead"
let tbody = elmt "tbody"
let tfoot = elmt "tfoot"
let tr = elmt "tr"
let td = elmt "td"
let th = elmt "th"
let p = elmt "p"
let button = elmt "button"
let input = elmt "input"
let textarea = elmt "textarea"
let br = elmt "br" []
let h1 t = elmt "h1" [ text t ]
let h2 t = elmt "h2" [ text t ]
let h3 t = elmt "h3" [ text t ]
let hr = elmt "hr"
let em = elmt "em"
let ul = elmt "ul"
let ol = elmt "ol"
let li = elmt "li"

(* Some more for SVG *)

let svg width height subs =
  let subs =
    let to_s f = string_of_int (int_of_float f) in
    (* Specific with and height attributes helps when displaying the
     * image as loaded form a file., for some reason *)
    attr "width" (to_s width) ::
    attr "height" (to_s height) ::
    attr "style" ("width:"^ to_s width ^
                  "; height:"^ to_s height ^
                  "; min-height:"^ to_s height ^";") ::
    subs in
  elmt ~svg:true "svg" subs

let g = elmt ~svg:true "g"

let rect
    ?(attrs=[]) ?fill ?stroke ?stroke_opacity ?stroke_dasharray ?fill_opacity
    ?stroke_width x y width height =
  let attrs = List.rev_append attrs
    [ attrsf "x" x ;
      attrsf "y" y ;
      attrsf "width" width ;
      attrsf "height" height ;
      attr_opt "fill" fill ;
      attrsf_opt "stroke-opacity" stroke_opacity ;
      attr_opt "stroke-dasharray" stroke_dasharray ;
      attrsf_opt "fill-opacity" fill_opacity ;
      attr_opt "stroke" stroke ;
      attrsf_opt "stroke-width" stroke_width ] in
  elmt ~svg:true "rect" attrs

let circle
    ?(attrs=[]) ?cx ?cy ?fill ?stroke ?stroke_opacity ?stroke_dasharray
    ?fill_opacity ?stroke_width r =
  let attrs = List.rev_append attrs
    [ attrsf "r" r ;
      attrsf_opt "cx" cx ;
      attrsf_opt "cy" cy ;
      attr_opt "fill" fill ;
      attrsf_opt "stroke-opacity" stroke_opacity ;
      attr_opt "stroke-dasharray" stroke_dasharray ;
      attrsf_opt "fill-opacity" fill_opacity ;
      attr_opt "stroke" stroke ;
      attrsf_opt "stroke-width" stroke_width ] in
  elmt ~svg:true "circle" attrs

let path
    ?(attrs=[]) ?style ?transform ?fill ?stroke ?stroke_width
    ?stroke_opacity ?stroke_dasharray ?fill_opacity d =
  let attrs = List.rev_append attrs
    [ attr "d" d ;
      attr_opt "style" style ;
      attr_opt "transform" transform ;
      attr_opt "fill" fill ;
      attrsf_opt "stroke-opacity" stroke_opacity ;
      attr_opt "stroke-dasharray" stroke_dasharray ;
      attrsf_opt "fill-opacity" fill_opacity ;
      attr_opt "stroke" stroke ;
      attrsf_opt "stroke-width" stroke_width ] in
  elmt ~svg:true "path" attrs

let moveto (x, y) =
  "M "^ short_string_of_float x ^" "^ short_string_of_float y ^" "
let lineto (x, y) =
  "L "^ short_string_of_float x ^" "^ short_string_of_float y ^" "
let curveto (x1, y1) (x2, y2) (x, y) =
  "C "^ short_string_of_float x1 ^" "^ short_string_of_float y1 ^" "
      ^ short_string_of_float x2 ^" "^ short_string_of_float y2 ^" "
      ^ short_string_of_float x  ^" "^ short_string_of_float y  ^" "
let smoothto (x2, y2) (x, y) =
  "S "^ short_string_of_float x2 ^" "^ short_string_of_float y2 ^" "
      ^ short_string_of_float x  ^" "^ short_string_of_float y  ^" "
let closepath = "Z"

let line
    ?(attrs=[]) ?style ?stroke ?stroke_width ?stroke_opacity
    ?stroke_dasharray (x1, y1) (x2, y2) =
  let attrs = List.rev_append attrs
    [ attrsf "x1" x1 ;
      attrsf "y1" y1 ;
      attrsf "x2" x2 ;
      attrsf "y2" y2 ;
      attr_opt "style" style ;
      attrsf_opt "stroke-opacity" stroke_opacity ;
      attr_opt "stroke-dasharray" stroke_dasharray ;
      attr_opt "stroke" stroke ;
      attrsf_opt "stroke-width" stroke_width ] in
  elmt ~svg:true "line" attrs

let svgtext
    ?(attrs=[]) ?x ?y ?dx ?dy ?style ?rotate ?text_length ?length_adjust
    ?font_family ?font_size ?fill ?stroke ?stroke_width ?stroke_opacity
    ?stroke_dasharray ?fill_opacity txt =
  let attrs = List.rev_append attrs
    [ attrsf_opt "x" x ;
      attrsf_opt "y" y ;
      attrsf_opt "dx" dx ;
      attrsf_opt "dy" dy ;
      attr_opt "style" style ;
      attrsf_opt "rotate" rotate ;
      attrsf_opt "textLength" text_length ;
      attrsf_opt "lengthAdjust" length_adjust ;
      attr_opt "font-family" font_family ;
      attrsf_opt "font-size" font_size ;
      attr_opt "fill" fill ;
      attrsf_opt "stroke-opacity" stroke_opacity ;
      attr_opt "stroke-dasharray" stroke_dasharray ;
      attrsf_opt "fill-opacity" fill_opacity ;
      attr_opt "stroke" stroke ;
      attrsf_opt "stroke-width" stroke_width ] in
  elmt ~svg:true "text" (text txt :: attrs)

(* Takes a list of (string * font_size) *)
let svgtexts
    ?attrs ?dx ?dy ?style ?rotate ?text_length ?length_adjust ?font_family
    ?fill ?stroke ?stroke_width ?stroke_opacity ?fill_opacity x y txts =
  let rec aux res y = function
    | [] -> res
    | (str, sz)::txts' ->
        aux ((svgtext
                ?attrs ~x ~y ~font_size:sz ?dx ?dy ?style ?rotate
                ?text_length ?length_adjust ?font_family ?fill ?stroke
                ?stroke_width ?stroke_opacity ?fill_opacity str) :: res)
            (y +. sz *. 1.05) txts' in
  List.rev (aux [] y txts)
