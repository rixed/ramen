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

let clock =
  let seq = ref 0 in
  fun () ->
    incr seq ;
    !seq

type param_desc = { name : string ; mutable last_changed : int }

type vnode =
  | Attribute of string * string
  | Text of string
  | Element of { tag : string ; action : (string -> unit) option ; subs : vnode list }
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

let elmt tag ?action subs =
  Element { tag ; action ; subs }
let with_value p f =
  let last = clock (), f p.value in
  Fun { param = p.desc ; f = (fun () -> f p.value) ; last = ref last }
let group subs =
  Group { subs }
let text s = Text s
let attr n v = Attribute (n, v)

(* We like em so much: *)
let div = elmt "div"
let clss = attr "class"
let attri n i = attr n (string_of_int i)
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

(* Note: When we resync from an event handler, we run the risk of
 * overwriting a DOM element that is next in line with a triggered
 * event; which would cancel that event before we handle it. That's
 * why we do not resync for onchange events. A better solution might
 * be to just set a resync_needed flag, and a short setTimeout to
 * resync once after any handler. *)
let rec add_listeners tag (elmt : Html.element Js.t) action =
  match tag with
  | "input" ->
    let elmt = Html.CoerceTo.input elmt |>
               coercion_motherfucker_can_you_do_it in
    elmt##.onchange := Html.handler (fun _e ->
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
  | Element { tag ; action ; subs ; _ } ->
    let elmt = doc##createElement (Js.string tag) in
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
    Html.getElementById_exn "application" in
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
