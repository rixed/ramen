open Js_of_ocaml
module Html = Dom_html
let doc = Html.window##.document

let with_debug = false

let print a = if with_debug then Firebug.console##log a
let print_2 a b = if with_debug then Firebug.console##log_2 a b
let print_3 a b c = if with_debug then Firebug.console##log_3 a b c
let print_4 a b c d = if with_debug then Firebug.console##log_4 a b c d
let fail () =
  Firebug.console##assert_ Js._false ;
  assert false

(* Stdlib complement: *)

let identity x = x

let option_may f = function
  | None -> ()
  | Some x -> f x

let option_map f = function
  | None -> None
  | Some x -> Some (f x)

let option_def x = function None -> x | Some v -> v
let (|?) a b = option_def b a

let optdef_get x = Js.Optdef.get x fail

let list_init n f =
  let rec loop prev i =
    if i >= n then List.rev prev else
    loop (f i :: prev) (i + 1) in
  loop [] 0

let replace_assoc n v l = (n, v) :: List.remove_assoc n l

let opt_get x = Js.Opt.get x fail
let to_int x = Js.float_of_number x |> int_of_float

let string_starts_with p s =
  let open String in
  length s >= length p &&
  sub s 0 (length p) = p

let rec string_times n s =
  if n = 0 then "" else s ^ string_times (n - 1) s

let abbrev len s =
  Firebug.console##assert_ (Js.bool (len >= 3)) ;
  if String.length s <= len then s else
  String.sub s 0 (len-3) ^"..."

(* Printing *)

let string_of_list ?(first="[") ?(last="]") ?(sep=", ") p l =
  first ^ List.fold_left (fun s i ->
            s ^ (if s = "" then "" else sep) ^ p i) "" l ^ last

let quote s = "\""^ s ^"\""
let string_of_string s = quote s
let string_of_option p = function None -> "null" | Some v -> p v
let string_of_field (n, v) = quote n ^": "^ v
let string_of_record = string_of_list ~first:"{ " ~last:" }" string_of_field
(* For a smaller JS: *)
let string_of_float x =
  (Js.number_of_float x)##toString |> Js.to_string
let string_of_int x = string_of_float (float_of_int x)

let clock =
  let seq = ref 0 in
  fun () ->
    incr seq ;
    !seq

type param_desc = { name : string ; mutable last_changed : int }

type vnode =
  | Attribute of string * string
  | Text of string
  | Element of { tag : string ; svg : bool ; action : (string -> unit) option ; subs : vnode list }
  (* Name of the parameter and function that, given this param, generate the vnode *)
  | Fun of { param : param_desc ; f : unit -> vnode ;
             last : (int * vnode) ref }
  (* No HTML produced, just for grouping: *)
  | Group of { subs : vnode list }

let rec string_of_vnode = function
  | Attribute (n, v) -> "attr("^ n ^", "^ abbrev 10 v ^")"
  | Text s -> "text(\""^ abbrev 15 s ^"\")"
  | Element { tag ; subs ; _ } -> tag ^"("^ string_of_tree subs ^")"
  | Group { subs ; _ } -> "group("^ string_of_tree subs ^")"
  | Fun { param ; _ } -> "fun("^ param.name ^")"
and string_of_tree subs =
  List.fold_left (fun s tree ->
    s ^ (if s = "" then "" else ";") ^ string_of_vnode tree) "" subs

let rec is_worthy = function
  | Element { subs ; _ } | Group { subs ; _ } ->
    List.exists is_worthy subs
  | Fun { last ; param ; _ } ->
    param.last_changed > fst !last || is_worthy (snd !last)
  | _ -> false

(* A named ref cell *)
type 'a param = { desc : param_desc ; mutable value : 'a }

let elmt tag ?(svg=false) ?action subs =
  Element { tag ; svg ; action ; subs }
let with_value p f =
  let last = clock (), f p.value in
  Fun { param = p.desc ; f = (fun () -> f p.value) ; last = ref last }
let group subs =
  Group { subs }
let text s = Text s
let attr n v = Attribute (n, v)

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

(* Some more for SVG *)

let svg = elmt ~svg:true "svg"

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

(* Parameters *)

let change p =
  p.desc.last_changed <- clock ()

let set p v =
  p.value <- v ;
  change p

(* Current DOM, starts empty *)

let vdom = ref (Group { subs = [] })

(* Rendering *)

let coercion_motherfucker_can_you_do_it o = Js.Opt.get o fail

let rec remove (parent : Html.element Js.t) child_idx n =
  if n > 0 then (
    Js.Opt.iter (parent##.childNodes##item child_idx) (fun child ->
      print_4 (Js.string ("Removing child_idx="^ string_of_int child_idx ^
                          " of")) parent
              (Js.string ("and "^ string_of_int (n-1) ^" more:")) child ;
      Dom.removeChild parent child) ;
    remove parent child_idx (n - 1)
  )

let rec add_listeners tag (elmt : Html.element Js.t) action =
  match tag with
  | "input" ->
    let elmt = Html.CoerceTo.input elmt |>
               coercion_motherfucker_can_you_do_it in
    elmt##.oninput := Html.handler (fun _e ->
      action (Js.to_string elmt##.value) ;
      Js._false)
  | "textarea" ->
    let elmt = Html.CoerceTo.textarea elmt |>
               coercion_motherfucker_can_you_do_it in
    elmt##.oninput := Html.handler (fun _e ->
      action (Js.to_string elmt##.value) ;
      Js._false)
  | "button" ->
    let elmt = Html.CoerceTo.button elmt |>
               coercion_motherfucker_can_you_do_it in
    elmt##.onclick := Html.handler (fun _ ->
      action (Js.to_string elmt##.value) ;
      resync () ;
      Js._false)
  | _ ->
    print (Js.string ("No idea how to add an event listener to a "^ tag ^
                      " but I can try")) ;
    elmt##.onclick := Html.handler (fun _ ->
      action "click :)" ;
      resync () ;
      Js._false)

and insert (parent : Html.element Js.t) child_idx vnode =
  print_2 (Js.string ("Appending "^ string_of_vnode vnode ^
                      " as child "^ string_of_int child_idx ^" of"))
          parent ;
  match vnode with
  | Attribute (n, v) ->
    parent##setAttribute (Js.string n) (Js.string v) ;
    0
  | Text t ->
    let data = doc##createTextNode (Js.string t) in
    let next = parent##.childNodes##item child_idx in
    Dom.insertBefore parent data next ;
    1
  | Element { tag ; svg ; action ; subs ; _ } ->
		let elmt =
      if svg then
        doc##createElementNS Dom_svg.xmlns (Js.string tag)
      else
        doc##createElement (Js.string tag) in
    option_may (fun action ->
      add_listeners tag elmt action) action ;
    List.fold_left (fun i sub ->
        i + insert elmt i sub
      ) 0 subs |> ignore ;
    let next = parent##.childNodes##item child_idx in
    Dom.insertBefore parent elmt next ;
    1
  | Fun { param ; f ; last } ->
    if param.last_changed > fst !last then last := clock (), f () ;
    insert parent child_idx (snd !last)
  | Group { subs ; _ } ->
    List.fold_left (fun i sub ->
        i + insert parent (child_idx + i) sub
      ) 0 subs

(* Only the Fun can produce a different result. Leads_to tells us where to go
 * to have Funs. *)
and sync (parent : Html.element Js.t) child_idx vdom =
  let rec flat_length = function
    | Group { subs ; _ } ->
      List.fold_left (fun s e -> s + flat_length e) 0 subs
    | Element _ | Text _ -> 1
    | Attribute _ -> 0
    | Fun { last ; _ } ->
      flat_length (snd !last) in
  let ( += ) a b = a := !a + b in
  let worthy = is_worthy vdom in
  print (Js.string ("sync vnode="^ string_of_vnode vdom ^
                    if worthy then " (worthy)" else "")) ;
  (* We might not already have an element there if the DOM
   * is initially empty, in which case we create it. *)
  while parent##.childNodes##.length <= child_idx do
    print (Js.string "Appending missing element in DOM") ;
    insert parent child_idx vdom |> ignore
  done ;
  match vdom with
  | Element { subs ; _ } ->
    if worthy then (
      (* Follow this path. Child_idx count the children so far. *)
      let parent' = parent##.childNodes##item child_idx |>
                    coercion_motherfucker_can_you_do_it |>
                    Html.CoerceTo.element |>
                    coercion_motherfucker_can_you_do_it in
      let child_idx = ref 0 in
      List.iter (fun sub ->
          child_idx += sync parent' !child_idx sub
        ) subs) ;
    1
  | Text _ -> 1
  | Attribute _ -> 0
  | Group { subs ; _ } ->
    if worthy then (
      let i = ref 0 in
      List.iter (fun sub ->
          i += sync parent (child_idx + !i) sub
        ) subs) ;
    flat_length vdom
  | Fun { param ; last ; _ } ->
    if param.last_changed > fst !last then (
      (* Bingo! For now we merely discard the whole sub-element.
       * Later: perform an actual diff. *)
      remove parent child_idx (flat_length (snd !last)) ;
      (* Insert will refresh last *)
      insert parent child_idx vdom
    ) else if worthy then (
      sync parent child_idx (snd !last)
    ) else (
      flat_length (snd !last)
    )

and resync () =
  print (Js.string "Syncing") ;
  let div =
    Html.getElementById "application" in
  sync div 0 !vdom |> ignore

let start nd =
  print (Js.string "starting...") ;
  vdom := nd ;
	Html.window##.onload := Html.handler (fun _ -> resync () ; Js._false)

(* Ajax *)

let enc s = Js.(to_string (encodeURIComponent (string s)))

let ajax action path content cb =
  let req = XmlHttpRequest.create () in
  req##.onreadystatechange := Js.wrap_callback (fun () ->
    if req##.readyState = XmlHttpRequest.DONE then (
      print (Js.string "AJAX query DONE!") ;
      if req##.status <> 200 then
        print (Js.string "AJAX query failed")
      else (
        let js = Js._JSON##parse req##.responseText in
        print js ;
        cb js ;
        resync ()
      )
    )) ;
  req##_open (Js.string action)
             (Js.string path)
             (Js.bool true) ;
  let ct = Js.string Consts.json_content_type in
  req##setRequestHeader (Js.string "Accept") ct ;
  let content =
    if content = "" then Js.null
    else (
      req##setRequestHeader (Js.string "Content-type") ct ;
      Js.some (Js.string content)
    ) in
  req##send content

let http_get path cb = ajax "GET" path "" cb
let http_post path content cb = ajax "POST" path content cb
let http_put path content cb = ajax "PUT" path content cb
let http_del path cb = ajax "DELETE" path "" cb
