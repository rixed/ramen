open Vdom
open RamenSharedTypes

(* misc *)
let log str =
  Js_browser.(Console.log console (Ojs.string_to_js str))

let assert_fail str =
  log str ; assert false

let option_get = function
  | Some x -> x
  | None -> assert_fail "invalid_arg: option_get with None"

(* State *)

(* We convert JS object into OCaml objects, believing that this conversion
 * that happens once and for all is not slower than all the runtime checks
 * we'd have to add should we manipulate JS objects instead, while being
 * more robust. *)
(*
type node =
  { mutable name : string ;
    mutable operation : string ;
    mutable parents : string list ;
    mutable children : string list ;
    command : string option ;
    pid : int option ;
    creation : bool } (* true if this is a new node that hasn't been saved yet *)
*)
let creation_node () =
  { name = "" ; operation = "" ;
    parents = [] ; children = [] ;
    type_of_operation = None ;
    command = None ; pid = None ;
    input_type = [] ; output_type = [] }

let node_of_ojs ojs =
  let open Ojs in
  { name = get ojs "name" |> string_of_js ;
    operation = get ojs "operation" |> string_of_js ;
    parents = get ojs "parents" |> list_of_js string_of_js ;
    children = get ojs "children" |> list_of_js string_of_js ;
    type_of_operation = get ojs "type_of_operation" |> option_of_js string_of_js ;
    command = get ojs "command" |> option_of_js string_of_js ;
    pid = get ojs "pid" |> option_of_js int_of_js ;
    input_type = [] ; output_type = [] }

(*
type graph =
  { nodes : node_info list ;
    links : (string * string) array ;
    status : bool }
*)
let link_of_pair pair =
  let open Ojs in
  array_get pair 0 |> string_of_js,
  array_get pair 1 |> string_of_js

let graph_of_ojs ojs =
  let open Ojs in
  let status_of_js js =
    if has_property js "Edition" then Edition else
    if has_property js "Compiled" then Compiled else
    if has_property js "Running" then Running else
    assert false in
  { nodes = get ojs "nodes" |> list_of_js node_of_ojs ;
    links = get ojs "links" |> list_of_js link_of_pair ;
    status = get ojs "status" |> status_of_js }

(*type links =
  { parents : string list ;
    children : string list } *)

(* TODO: deriving json *)
module JZON =
struct
  let quote s = let q = "\"" in q^ String.escaped s ^q
  let label l = quote l ^":"
  let value_str l s = label l ^ quote s
  let value_int l i = label l ^ string_of_int i
  let value_bool l b = label l ^ (if b then "true" else "false")
  let obj_of_list lst = "{"^ String.concat "," lst ^"}"
  let arr_of_list lst = "["^ String.concat "," lst ^"]"
  let value_lst l lst = label l ^ arr_of_list lst

  (* The only used function, when sending a node to Ramen.
   * Alternative: ? *)
  let of_node node =
    obj_of_list [ value_str "operation" node.operation ]
  let of_links links =
    obj_of_list [ value_lst "parents" (List.map quote links.parents) ;
                  value_lst "children" (List.map quote links.children) ]
end

type api_response = Ok of Ojs.t | Error of string
type spinner = Nope | Spinning of string | Done of api_response
type edited_node = FromGraph of string | NewNode of node_info | NoNode
type state =
  { graph : graph_info option ;
    edited_node : edited_node ;  (* The node currently undergoing edition *)
    mutable saving : spinner }

let get_node graph name =
  List.find (fun node -> node.name = name) graph.nodes

let get_edited_node st =
  match st.graph, st.edited_node with
  | None, _ -> None
  | Some graph, FromGraph node_name ->
    (match get_node graph node_name with
    | exception Not_found -> None
    | node -> Some (node, false))
  | Some _, NewNode node -> Some (node, true)
  | Some _, NoNode -> None

let edit_node st f =
  match get_edited_node st with
  | Some (node, _) -> f node
  | None -> ()

let initialState =
  { graph = None ; edited_node = NoNode ; saving = Nope }

(* View *)

let label ?key ?a l = elt "label" ?key ?a l
let br ?key ?a () = elt "br" ?key ?a []
let cols = int_attr "cols"
let rows = int_attr "rows"
let id_ = attr "id"
let tech_text str = txt_span ~a:[class_ "tech"] str
let textarea ?key ?a l = elt "textarea" ?key ?a l

(*  vdom does not propagate multi-values properly
let select ?key ?(a=[]) name options selection =
  let a = (str_prop "name" name) :: a in
  elt "select" ?key ~a (
    Array.map (fun opt ->
        let selected = Array.mem opt selection in
        if name = "parents" && selected then log ("Option "^ opt ^" selected") ;
        elt "option" ~a:[value opt; str_prop "label" opt; bool_prop "selected" selected] []
      ) options |>
    Array.to_list)

let multiselect ?key ?(a=[]) name options selection =
  let a = bool_prop "multiple" true :: a in
  select ?key ~a name options selection
*)

let checkboxes ~action ?key ?a options selection =
  div ?key ?a
    (List.map (fun opt ->
        let selected = List.mem opt selection in
        label [
          (let a = [type_ "checkbox"; value opt; onchange action] in
           let a = if selected then attr "checked" ""::a else a in
           input ~a []) ;
          text opt ]
      ) options)

let unorderd_list ?key ?a lst =
  if lst = [] then text "None" else
    elt "ul" ?key ?a (List.map (fun n -> elt "li" [ text n ]) lst)

let togle_list s lst =
  let rec loop prev = function
  | [] -> s :: prev
  | x :: rest ->
    if x = s then List.rev_append prev rest
    else loop (x::prev) rest
  in
  loop [] lst

let prog_info node =
  match node.command, node.pid  with
  | Some cmd, Some pid when cmd <> "" && pid > 0 ->
    [ text "Program running as pid " ;
      tech_text (string_of_int pid) ;
      text ", command " ;
      tech_text cmd ]
  | _ -> []

let view st =
    match st.graph with
    | None ->
      div [ text "Select a graph" ]
    | Some graph ->
      let is_editable = graph.status = Edition || graph.status = Compiled in
      div [
        (let a = [type_ "button"; onclick `CreateNode; value "New Node"] in
         let a = if is_editable then a else attr "disabled" "" :: a in
         input [] ~a) ;
        (let a = [type_ "button"; onclick `CompileGraph; value "Compile"] in
         let a = if graph.status = Edition then a else attr "disabled" "" :: a in
         input [] ~a) ;
        (let a = [type_ "button"; onclick `StartGraph; value "Run"] in
         let a = if graph.status = Compiled then a else attr "disabled" "" :: a in
         input [] ~a) ;
        (let a = [type_ "button"; onclick `StopGraph; value "Stop"] in
         let a = if graph.status = Running then a else attr "disabled" "" :: a in
         input [] ~a) ;
        br () ;
        (match get_edited_node st with
        | None ->
          text "Select a node"
        | Some (node, is_creation) ->
          let no_html = div ~a:[class_ "invisible"] []
          and other_nodes =
            List.fold_left (fun lst n ->
              if n != node then n.name::lst else lst) [] graph.nodes in
          div ([
            label [
              text "Node Name:" ;
              if is_creation then (
                let a = [class_ "node-name"; type_ "text"; value node.name; oninput (fun s -> `NameChange s)] in
                input [] ~a
              ) else text node.name ] ;
            br () ;
            label [
              text "Parents:" ;
              if is_editable then
                checkboxes ~action:(fun s -> `ParentsChange s) other_nodes node.parents
              else
                unorderd_list node.parents
            ] ;
            br () ;
            label [
              text "Children:" ;
              if is_editable then
                checkboxes ~action:(fun s -> `ChildrenChange s) other_nodes node.children
              else
                unorderd_list node.children
            ] ;
            br () ;
            label [
              text "Operation:" ;
              (if is_editable then
                textarea ~a:[cols 40; rows 15; class_ "node-op"; oninput (fun s -> `OpChange s)] [ text node.operation ]
              else
                elt "pre" [
                  elt "code" ~a:[class_ "node-op"] [ text node.operation ] ]) ] ;
              (*elt "pre" [
                elt "code" ~a:[id_ "code-editor"; (*attr "contenteditable" "true";*) class_ "node-op"; onkeydown (fun s -> `OpChange s)] [
                  text node.operation ] ] ] ;*)
            br () ;
            (match st.saving with
            | Spinning txt ->
              text txt
            | _ ->
              if is_editable then div [
                input [] ~a:[class_ "node-update"; onclick `UpdateNode; type_ "button";
                             value (if is_creation then "Create" else "Update")] ;
                if is_creation then no_html
                else
                  input [] ~a:[class_ "node-delete"; onclick `DeleteNode; type_ "button";
                               value "Delete"]
              ] else no_html) ;
            br () ] @
            (match st.saving with
            | Done (Ok _) ->
              [ txt_span ~a:[class_ "success"] "Saved" ; br () ]
            | Done (Error err) ->
              [ txt_span ~a:[class_ "failure"] err ; br () ]
            | _ -> []) @
            prog_info node)) ]

(* Update *)

open Js_browser

(* Custom commands *)
type 'msg Vdom.Cmd.t +=
  | Http_get of { url: string; callback: api_response -> 'msg }
  | Http_put of { url: string; payload: string; callback: api_response -> 'msg }
  | Http_del of { url: string; callback: api_response -> 'msg }
  | Redisplay_graph of Ojs.t * (Ojs.t -> 'msg)

(* TODO: reload one in particular *)
let reload_graph =
  Http_get { url = "graph" ; callback = fun r -> `GotGraph r }

let display_graph_js_fun = Ojs.variable "display_graph"

let update st msg =
  match msg with
  | `GetGraph ->
    return ~c:[reload_graph] st
  | `GotGraph (Ok ojs) ->
    let cmd = Redisplay_graph (ojs, fun js ->
      `SetNode Ojs.(option_of_js string_of_js js)) in
    let g = graph_of_ojs ojs in
    return ~c:[cmd] { st with graph = Some g }
  | `GotGraph (Error e) ->
    log ("Couldn't receive graph: "^ e) ;
    return st
  | `SetNode new_node_name_opt ->
    let edited_node =
      match new_node_name_opt with
      | Some name -> FromGraph name
      | None -> NoNode in
    return { st with edited_node }
  | `CompileGraph ->
    let cmd = Http_get {
      url = "compile" ; callback = fun r -> `CompiledGraph r } in
    return ~c:[cmd] { st with saving = Spinning "Compiling" }
  | `CompiledGraph r ->
    return ~c:[reload_graph] { st with saving = Done r }
  | `StartGraph ->
    let cmd = Http_get {
      url = "run" ; callback = fun r -> `StartedGraph r } in
    return ~c:[cmd] { st with saving = Spinning "Starting Graph" }
  | `StartedGraph r ->
    log "Graph has started" ;
    return ~c:[reload_graph] { st with saving = Done r }
  | `StopGraph ->
    let cmd = Http_get {
      url = "stop" ;
      callback = fun r -> `StoppedGraph r } in
    return ~c:[cmd] st
  | `StoppedGraph _r ->
    log "Graph has stopped" ;
    return ~c:[reload_graph] st
  | `CreateNode ->
    return { st with edited_node = NewNode (creation_node ()) }
  | `NameChange new_text ->
    edit_node st (fun node -> node.name <- new_text) ;
    return st
  | `OpChange new_value ->
    edit_node st (fun node -> node.operation <- new_value) ;
    return st
  | `ParentsChange s ->
    edit_node st (fun node -> node.parents <- togle_list s node.parents) ;
    return st
  | `ChildrenChange s ->
    edit_node st (fun node -> node.children <- togle_list s node.children) ;
    return st
  | `UpdateNode ->
    let c = ref [] in
    let edited_node = ref st.edited_node in
    edit_node st (fun node ->
      edited_node := FromGraph node.name ; (* In case we add a NewNode *)
      let url = "node/"^ node.name in
      let payload = JZON.of_node node in
      let txt = "Saving node "^node.name in
      st.saving <- Spinning txt ;
      c := [ Http_put { url ; payload ; callback = fun r -> `UpdatedNode (r, node) } ]) ;
    return ~c:!c { st with edited_node = !edited_node }
  | `UpdatedNode (Ok _ojs, node) ->
    (* Now that we updated the node let's update the links *)
    let url = "links/"^ node.name in
    let payload = JZON.of_links { parents = node.parents ;
                                  children = node.children } in
    let c = [ Http_put { url ; payload ; callback = fun r -> `UpdatedLinks r } ] in
    return ~c { st with saving = Spinning "Saving links" }
  | `UpdatedNode (r, _node) ->
    return { st with saving = Done r }
  | `UpdatedLinks r ->
    return ~c:[reload_graph] { st with saving = Done r }
  | `DeleteNode ->
    let c = ref [] in
    edit_node st (fun node ->
      let url = "node/"^ node.name in
      let txt = "Deleting node "^node.name in
      st.saving <- Spinning txt ;
      c := [ Http_del { url ; callback = fun r -> `DeletedNode r } ]) ;
    return ~c:!c st
  | `DeletedNode r ->
    return ~c:[reload_graph] { st with saving = Done r ; edited_node = NoNode }

let run_http ?(command="GET") ?(content_type="text/plain")
             ~url ?(payload="") ~callback () =
  let open XHR in
  let r = create () in
  open_ r command url;
  set_onreadystatechange r
    (fun () ->
       match ready_state r with
       | Done ->
          let res = response_text r and code = status r in
          let resp =
            if code = 200 then Ok (Js_browser.JSON.parse res)
            else Error res in
          callback resp
       | _ -> ()) ;
  set_request_header r "Content-Type" content_type ;
  send r payload

let run_http_get ~url ~callback () =
  run_http ~url ~callback ()

let run_http_put ~url ~payload ~callback () =
  run_http ~command:"PUT" ~content_type:"application/json"
           ~url ~callback ~payload ()

let run_http_del ~url ~callback () =
  run_http ~command:"DELETE" ~url ~callback ()

let cmd_handler ctx = function
  | Http_put { url; payload; callback } ->
    run_http_put ~url ~payload ~callback:(fun s -> Vdom_blit.Cmd.send_msg ctx (callback s)) () ;
    true
  | Http_del { url; callback } ->
    run_http_del ~url ~callback:(fun s -> Vdom_blit.Cmd.send_msg ctx (callback s)) () ;
    true
  | Http_get { url; callback } ->
    run_http_get ~url ~callback:(fun s -> Vdom_blit.Cmd.send_msg ctx (callback s)) () ;
    true
  | Redisplay_graph (ojs, callback) ->
    let ret = Ojs.(apply display_graph_js_fun [|
      ojs ;
      fun_to_js 1 (fun js ->
        Vdom_blit.Cmd.send_msg ctx (callback js))
    |]) in
    ignore ret ;
    true
  | _ -> false

let run () =
  let vdom_app = ref None in
  let inject msg =
    Vdom_blit.process (option_get !vdom_app) msg
  in
  let where =
    match Document.get_element_by_id document "node_panel" with
    | Some e -> e
    | None -> assert false in
  let app =
    app ~init:(return initialState) ~view ~update () in
  let a = Vdom_blit.run app in
  vdom_app := Some a ;
  Vdom_blit.dom a |>
  Element.append_child where ;
  inject `GetGraph

let () =
  Vdom_blit.(register (cmd {Cmd.f = cmd_handler})) ;
  Window.set_onload window run
