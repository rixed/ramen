open Js_of_ocaml
module Html = Dom_html
let doc = Html.window##.document

module SSet = Set.Make (struct type t = string let compare = compare end)

(* Stdlib complement: *)

let intersect s1 s2 =
  SSet.inter s1 s2 |> SSet.is_empty |> not

let option_may f = function
  | None -> ()
  | Some x -> f x

let option_map f = function
  | None -> None
  | Some x -> Some (f x)

let option_def x = function None -> x | Some v -> v

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


(* Those are the only we care about: *)
(* TODO: put worthy into Element? *)
type dyn_node_spec =
  | Attribute of string * string
  | Text of string
  | Element of string * (string -> unit) option * dyn_node_tree list
  (* No HTML produced, just for grouping: *)
  | Group of dyn_node_tree list
and dyn_node_tree =
  { spec : dyn_node_spec ;
    mutable dirty : SSet.t ; worthy : SSet.t }

(* Remove groups before diffing for simplicity: *)
let rec flatten_tree t =
  match t with
  | { spec = (Attribute _ | Text _) ; _ } -> [ t ]
  | { spec = Element (tag, action, subs) ; _ } ->
    [ { t with spec = Element (tag, action, flatten_trees subs) } ]
  (* worthy has been propagated to the root already but dirty is never
   * propagated to the leaves. It is indeed useless since the whole subtree is
   * going to be redrawn anyway. But if the node is a group that is going to
   * be simplified out, then we do want to propagate this now! Note that it's
   * enough to propagate it one level after we've flattened out sub: *)
  | { spec = Group lst ; _ } ->
    flatten_trees lst |>
    List.map (fun e -> { e with dirty = SSet.union e.dirty t.dirty })
and flatten_trees ts =
  List.fold_left (fun ts' t ->
      List.rev_append (flatten_tree t) ts'
    ) [] ts |>
  List.rev

let rec string_of_spec = function
  | Attribute (n, v) -> "attr("^ n ^", "^ v ^")"
  | Text s -> "text(\""^ s ^"\")"
  | Element (tag, _, subs) ->
    tag ^"("^ string_of_tree subs ^")"
  | Group subs -> "group("^ string_of_tree subs ^")"
and string_of_tree subs =
  List.fold_left (fun s tree ->
    s ^ (if s = "" then "" else ";") ^ string_of_spec tree.spec) "" subs

let elmt tag ?action subs =
  let worthy = List.fold_left (fun set sub ->
    SSet.union set sub.dirty |>
    SSet.union sub.worthy) SSet.empty subs in
  { dirty = SSet.empty ; worthy ; spec = Element (tag, action, subs) }
let text s =
  { dirty = SSet.empty ; worthy = SSet.empty ; spec = Text s }
let attr n v =
  { dirty = SSet.empty ; worthy = SSet.empty ; spec = Attribute (n, v) }
let group lst =
  { dirty = SSet.empty ; worthy = SSet.empty ; spec = Group lst }

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

(* A named ref cell *)
type 'a param = { name : string ; mutable value : 'a }

(* FIXME:
 * We should cache the construction of the virtual DOM.
 * So that in with_value we would reuse the cache if the value is not
 * in changed.
 * Also, we could have a "const" for when we do not depend on any value,
 * that would be cached always.
 * Then once we have this, we could add a seqnum to each virtual dom element
 * (thanks to [elmt]) and use this to identify where nodes have moved
 * in the next_dom compared to curr_dom (instead of supposing that next 3rd
 * child is the same as curr 3rd child) *)
(* NOTE: dependency of a node to a parameter might change according to
 * circumstances and must not be considered constant! *)
let with_value p f =
  let sub = f p.value in
  (* if this value change, redraw this: *)
  sub.dirty <- SSet.add p.name sub.dirty ;
  sub

let changes = ref SSet.empty (* Set of names of changed variables *)

let string_of_changes () =
  SSet.fold (fun s prev ->
    prev ^ (if prev = "" then "" else ",") ^ s) !changes ""

let change p =
  changes := SSet.add p.name !changes

let set p v =
  p.value <- v ;
  change p

(* Current DOM *)

let curr_dom = ref []

(* Compute next DOM *)

let next_dom = ref (fun () -> [])

(* Rendering *)

let coercion_motherfucker_can_you_do_it o =
  Js.Opt.get o (fun () ->
    Firebug.console##log (Js.string "Assertion failed") ;
    assert false)

let get (parent : Html.element Js.t) n =
  parent##.childNodes##item n |> coercion_motherfucker_can_you_do_it |>
  Html.CoerceTo.element |> coercion_motherfucker_can_you_do_it

let remove_element (parent : Html.element Js.t) n =
  Firebug.console##log (Js.string "Removing element") ;
  Js.Opt.iter (parent##.childNodes##item n) (fun child ->
    Dom.removeChild parent child)

let remove_attribute (parent : Html.element Js.t) n =
  Firebug.console##log (Js.string "Removing attribute") ;
  parent##removeAttribute (Js.string n)

let remove parent ni = function
  | Element _ | Text _ -> remove_element parent ni
  | Attribute (n, _) -> remove_attribute parent n
  | Group _ -> assert false

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

and replace (parent : Html.element Js.t) n spec changed =
  (* If n is < 0 this is an append *)
  let what = if n < 0 then "Appending" else "Replacing" in
  match spec with
  | Attribute (n, v) ->
    Firebug.console##log (Js.string (what ^" attribute...")) ;
    parent##setAttribute (Js.string n) (Js.string v)
  | Text t ->
    Firebug.console##log (Js.string (what ^" text")) ;
    let data = doc##createTextNode (Js.string t) in
    if n < 0 then
      Dom.appendChild parent data
    else
      let prev = parent##.childNodes##item n |>
                 coercion_motherfucker_can_you_do_it in
      Dom.replaceChild parent data prev
  | Element (tag, action, subs) ->
    Firebug.console##log (Js.string (what ^" element "^ tag)) ;
    let elmt = doc##createElement (Js.string tag) in
    option_may (fun action ->
      add_listeners tag elmt action) action ;
    Firebug.console##log (Js.string "recurse sync...") ;
    sync elmt [] subs changed ;
    if n < 0 then
      Dom.appendChild parent elmt
    else
      let prev = parent##.childNodes##item n |>
                 coercion_motherfucker_can_you_do_it in
      Dom.replaceChild parent elmt prev
  | Group _ -> assert false

and sync parent prev next changed =
  Firebug.console##log (Js.string "sync parent...") ;
  Firebug.console##log parent ;
  (* [ni] count the elements/texts, in the current
   * parent. Used to index the DOM children. *)
  let rec loop ni ps ns =
    let ni' =
      match ns with
      | { spec = (Element _ | Text _) ; _ } :: _ -> ni + 1
      | { spec = Group _ ; _ } :: _ -> assert false
      | _ -> ni in
    match ps, ns with
    | [], [] -> ()
    | [], n::ns' ->
      replace parent ~-1 n.spec changed ;
      loop ni' ps ns'
    | p::ps', [] ->
      remove parent ni p.spec ;
      loop ni' ps' ns
    | p::ps', n::ns' ->
      if intersect changed p.dirty then (
        replace parent ni n.spec changed ;
        loop ni' ps' ns'
      ) else if intersect changed p.worthy then (
        Firebug.console##log(Js.string ("Child #"^ string_of_int ni ^"/"^ string_of_int parent##.childNodes##.length ^" is worthy of a visit")) ;
        let parent' = get parent ni in
        (match p.spec, n.spec with
        | Element (_, _, prevs'), Element (_, _, next') ->
          sync parent' prevs' next' changed
        | _ -> assert false) ;
        loop ni' ps' ns'
      ) else (
        loop ni' ps' ns'
      )
  in
  loop 0 prev next

and resync () =
  Firebug.console##log (Js.string "Syncing with changes:") ;
  Firebug.console##log (Js.string (string_of_changes ())) ;
  let div =
    Html.getElementById_exn "application" in
  let n = !next_dom () in
  let n = flatten_trees n in
  sync div !curr_dom n !changes ;
  curr_dom := n ;
  changes := SSet.empty

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

let start nd =
  Firebug.console##log (Js.string "starting...") ;
  next_dom := nd ;
	Html.window##.onload := Html.handler (fun _ -> resync () ; Js._false)
