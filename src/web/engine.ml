open Js_of_ocaml
module Html = Dom_html
let doc = Html.window##.document

module SSet = struct
  module S = Set.Make (struct type t = string let compare = compare end)
  type t = All | Set of S.t
  let empty = Set S.empty
  let all = All
  let is_empty = function
    | All -> false
    | Set s -> S.is_empty s
  let inter s1 s2 = match s1, s2 with
    | All, s2 -> s2
    | s1, All -> s1
    | Set s1, Set s2 -> Set (S.inter s1 s2)
  let union s1 s2 = match s1, s2 with
    | All, _ | _, All -> All
    | Set s1, Set s2 -> Set (S.union s1 s2)
  let add p = function
    | All -> All
    | Set s -> Set (S.add p s)
  let mem p = function
    | All -> true
    | Set s -> S.mem p s
  let to_string = function
    | All -> "all"
    | Set s ->
      S.fold (fun x prev ->
        prev ^ (if prev = "" then "" else ",") ^ x) s ""
  let intersect s1 s2 = match s1, s2 with
    | All, _ | _, All -> true
    | Set s1, Set s2 ->
      S.inter s1 s2 |> S.is_empty |> not
end

(* Stdlib complement: *)

let option_may f = function
  | None -> ()
  | Some x -> f x

let option_map f = function
  | None -> None
  | Some x -> Some (f x)

let option_def x = function None -> x | Some v -> v
let (|?) a b = option_def b a

let optdef_get x = Js.Optdef.get x (fun () -> assert false)

let list_init n f =
  let rec loop prev i =
    if i >= n then List.rev prev else
    loop (f i :: prev) (i + 1) in
  loop [] 0

let opt_get x = Js.Opt.get x (fun () -> assert false)
let to_int x = Js.float_of_number x |> int_of_float

let string_starts_with p s =
  let open String in
  length s >= length p &&
  sub s 0 (length p) = p

let rec string_times n s =
  if n = 0 then "" else s ^ string_times (n - 1) s

let abbrev len s =
  assert (len >= 3) ;
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

type vnode =
  | Attribute of string * string
  | Text of string
  | Element of { tag : string ; action : (string -> unit) option ; subs : vnode list }
  (* Name of the parameter and function that, given this param, generate the vnode *)
  | Fun of { param : string ; f : unit -> vnode ; last : vnode ref }
  (* No HTML produced, just for grouping: *)
  | Group of { subs : vnode list }

let rec string_of_vnode = function
  | Attribute (n, v) -> "attr("^ n ^", "^ abbrev 10 v ^")"
  | Text s -> "text(\""^ abbrev 15 s ^"\")"
  | Element { tag ; subs ; _ } -> tag ^"("^ string_of_tree subs ^")"
  | Group { subs ; _ } -> "group("^ string_of_tree subs ^")"
  | Fun { param ; _ } -> "fun("^ param ^")"
and string_of_tree subs =
  List.fold_left (fun s tree ->
    s ^ (if s = "" then "" else ";") ^ string_of_vnode tree) "" subs

let rec leads_to what = function
  | Element { subs ; _ } | Group { subs ; _ } ->
    List.exists (leads_to what) subs
  | Fun { last ; param ; _ } ->
    SSet.mem param what || leads_to what !last
  | _ -> false

(* A named ref cell *)
type 'a param = { name : string ; mutable value : 'a }

let elmt tag ?action subs =
  Element { tag ; action ; subs }
let with_value p f =
  let last = f p.value in
  Fun { param = p.name ; f = (fun () -> f p.value) ; last = ref last }
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

let changes = ref SSet.all (* Set of names of changed variables *)

let string_of_changes () =
  SSet.to_string !changes

let change p =
  changes := SSet.add p.name !changes

let set p v =
  p.value <- v ;
  change p

(* Current DOM, starts empty *)

let vdom = ref (Group { subs = [] })

(* Rendering *)

let coercion_motherfucker_can_you_do_it o =
  Js.Opt.get o (fun () ->
    Firebug.console##log (Js.string "Assertion failed") ;
    assert false)

let rec remove (parent : Html.element Js.t) child_idx n =
  if n > 0 then (
    Js.Opt.iter (parent##.childNodes##item child_idx) (fun child ->
      Firebug.console##log_4 (Js.string ("Removing child_idx="^ string_of_int child_idx^" of")) parent
        (Js.string ("and "^ string_of_int (n-1) ^" more:")) child ;
      Dom.removeChild parent child) ;
    remove parent child_idx (n - 1)
  )

let rec add_listeners tag (elmt : Html.element Js.t) action =
  match tag with
  | "input" ->
    let elmt = Html.CoerceTo.input elmt |>
               coercion_motherfucker_can_you_do_it in
    elmt##.onchange := Html.handler (fun _e ->
      action (Js.to_string elmt##.value) ;
      resync () ;
      Js._false)
  | "button" ->
    let elmt = Html.CoerceTo.button elmt |>
               coercion_motherfucker_can_you_do_it in
    elmt##.onclick := Html.handler (fun _ ->
      action (Js.to_string elmt##.value) ;
      resync () ;
      Js._false)
  | _ ->
    Firebug.console##log (Js.string ("No idea how to add an event listener to a "^ tag ^" but I can try")) ;
    elmt##.onclick := Html.handler (fun _ ->
      action "click :)" ;
      resync () ;
      Js._false)

and insert changed (parent : Html.element Js.t) child_idx vnode =
  Firebug.console##log_2
    (Js.string ("Appending "^ string_of_vnode vnode ^
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
        i + insert changed elmt i sub
      ) 0 subs |> ignore ;
    let next = parent##.childNodes##item child_idx in
    Dom.insertBefore parent elmt next ;
    1
  | Fun { param ; f ; last } ->
    if SSet.mem param changed then last := f () ;
    insert changed parent child_idx !last
  | Group { subs ; _ } ->
    List.fold_left (fun i sub ->
        i + insert changed parent (child_idx + i) sub
      ) 0 subs

(* Only the Fun can produce a different result. Leads_to tells us where to go
 * to have Funs. *)
and sync changed (parent : Html.element Js.t) child_idx vdom =
  let rec flat_length = function
    | Group { subs ; _ } ->
      List.fold_left (fun s e -> s + flat_length e) 0 subs
    | Element _ | Text _ -> 1
    | Attribute _ -> 0
    | Fun { last ; _ } ->
      flat_length !last in
  let ( += ) a b = a := !a + b in
  let worthy = leads_to changed vdom in
  Firebug.console##log
    (Js.string ("sync vnode="^ string_of_vnode vdom ^
                if worthy then " (worthy)" else "")) ;
  (* We might not already have an element there if the DOM
   * is initially empty, in which case we create it. *)
  while parent##.childNodes##.length <= child_idx do
    Firebug.console##log (Js.string "Appending missing element in DOM") ;
    insert changed parent child_idx vdom |> ignore
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
          child_idx += sync changed parent' !child_idx sub
        ) subs) ;
    1
  | Text _ -> 1
  | Attribute _ -> 0
  | Group { subs ; _ } ->
    if worthy then (
      let i = ref 0 in
      List.iter (fun sub ->
          i += sync changed parent (child_idx + !i) sub
        ) subs) ;
    flat_length vdom
  | Fun { param ; last ; f ; _ } ->
    if SSet.mem param changed then (
      (* Bingo! For now we merely discard the whole sub-element *)
      remove parent child_idx (flat_length !last) ;
      last := f () ;
      insert changed parent child_idx !last
    ) else if worthy then (
      sync changed parent child_idx !last
    ) else (
      flat_length !last
    )

and resync () =
  Firebug.console##log_2 (Js.string "Syncing with changes:")
                         (Js.string (string_of_changes ())) ;
  let div =
    Html.getElementById_exn "application" in
  sync !changes div 0 !vdom |> ignore ;
  changes := SSet.empty

let start nd =
  Firebug.console##log (Js.string "starting...") ;
  vdom := nd ;
	Html.window##.onload := Html.handler (fun _ -> resync () ; Js._false)

(* Ajax *)

let enc s = Js.(to_string (encodeURIComponent (string s)))

let ajax action path content cb =
  let req = XmlHttpRequest.create () in
  req##.onreadystatechange := Js.wrap_callback (fun () ->
    if req##.readyState = XmlHttpRequest.DONE then (
      Firebug.console##log (Js.string "AJAX query DONE!") ;
      if req##.status <> 200 then
        Firebug.console##log (Js.string "AJAX query failed")
      else (
        let js = Js._JSON##parse req##.responseText in
        Firebug.console##log js ;
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
