open Vdom
open RamenSharedTypes_JS

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

let make_empty_node () = Node.{ empty with name = "" }

let node_of_ojs ojs =
  let open Ojs in
  Node.{
    name = get ojs "name" |> string_of_js ;
    operation = get ojs "operation" |> string_of_js ;
    parents = get ojs "parents" |> list_of_js string_of_js ;
    children = get ojs "children" |> list_of_js string_of_js ;
    type_of_operation = get ojs "type_of_operation" |> option_of_js string_of_js ;
    input_type = [] ; output_type = [] (* TODO *) ;
    command = get ojs "command" |> option_of_js string_of_js ;
    pid = get ojs "pid" |> option_of_js int_of_js ;
    in_tuple_count = get ojs "in_tuple_count" |> int_of_js ;
    selected_tuple_count = get ojs "selected_tuple_count" |> int_of_js ;
    out_tuple_count = get ojs "out_tuple_count" |> int_of_js ;
    group_count = get ojs "group_count" |> option_of_js int_of_js ;
    cpu_time = get ojs "cpu_time" |> float_of_js ;
    ram_usage = get ojs "ram_usage" |> int_of_js }

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
    status = get ojs "status" |> status_of_js ;
    last_started = get ojs "last_started" |> option_of_js float_of_js ;
    last_stopped = get ojs "last_stopped" |> option_of_js float_of_js }

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

  let of_node node =
    obj_of_list [ value_str "operation" node.Node.operation ]
  let of_links links =
    obj_of_list [ value_lst "parents" (List.map quote links.parents) ;
                  value_lst "children" (List.map quote links.children) ]
end

type api_response = Ok of Ojs.t | Error of string
type spinner = Nope | Spinning of string | Done of api_response
type edited_node = FromGraph of string | NewNode | NoNode
type state =
  { graph : graph_info option ;
    mutable new_node : Node.info ; (* To save the current edition *)
    mutable edited_node : edited_node ;  (* The node currently undergoing edition *)
    mutable saving : spinner }

let get_node graph name =
  List.find (fun node -> node.Node.name = name) graph.nodes

let get_edited_node st =
  match st.graph, st.edited_node with
  | None, _ -> None
  | Some graph, FromGraph node_name ->
    (match get_node graph node_name with
    | exception Not_found -> None
    | node -> Some (node, false))
  | Some _, NewNode -> Some (st.new_node, true)
  | Some _, NoNode -> None

let edit_node st f =
  match get_edited_node st with
  | Some (node, _) -> f node
  | None -> ()

let initialState =
  { graph = None ; new_node = make_empty_node () ; edited_node = NoNode ; saving = Nope }

(* Demo content *)

let list_unfold f =
  let rec aux lst i =
    let lst' = f i lst in
    if lst' == lst then lst else aux lst' (i+1)
  in
  aux [] 0

type demo_cmd = { demo_url : string ; demo_payload : string }
let demo_cmds =
  let nb_zones = 7 in
  { demo_url = "node/TCPv29" ;
    demo_payload = "{\n\
    \"operation\":\n\
      \"read csv file \\\"100k.csv\\\" separator \\\"\\\\t\\\" null \\\"<NULL>\\\" (\n\
        poller string not null,\n\
        capture_begin u64 not null,\n\
        capture_end u64 not null,\n\
        device_client u8 null,\n\
        device_server u8 null,\n\
        vlan_client u32 null,\n\
        vlan_server u32 null,\n\
        mac_client u64 null,\n\
        mac_server u64 null,\n\
        zone_client u32 not null,\n\
        zone_server u32 not null,\n\
        ip4_client u32 null,\n\
        ip6_client i128 null,\n\
        ip4_server u32 null,\n\
        ip6_server i128 null,\n\
        ip4_external u32 null,\n\
        ip6_external i128 null,\n\
        port_client u16 not null,\n\
        port_server u16 not null,\n\
        diffserv_client u8 not null,\n\
        diffserv_server u8 not null,\n\
        os_client u8 null,\n\
        os_server u8 null,\n\
        mtu_client u16 null,\n\
        mtu_server u16 null,\n\
        captured_pcap string null,\n\
        application u32 not null,\n\
        protostack string null,\n\
        uuid string null,\n\
        traffic_bytes_client u64 not null,\n\
        traffic_bytes_server u64 not null,\n\
        traffic_packets_client u64 not null,\n\
        traffic_packets_server u64 not null,\n\
        payload_bytes_client u64 not null,\n\
        payload_bytes_server u64 not null,\n\
        payload_packets_client u64 not null,\n\
        payload_packets_server u64 not null,\n\
        retrans_traffic_bytes_client u64 null,\n\
        retrans_traffic_bytes_server u64 null,\n\
        retrans_payload_bytes_client u64 null,\n\
        retrans_payload_bytes_server u64 null,\n\
        syn_count_client u64 null,\n\
        fin_count_client u64 null,\n\
        fin_count_server u64 null,\n\
        rst_count_client u64 null,\n\
        rst_count_server u64 null,\n\
        timeout_count u64 not null,\n\
        close_count u64 null,\n\
        dupack_count_client u64 null,\n\
        dupack_count_server u64 null,\n\
        zero_window_count_client u64 null,\n\
        zero_window_count_server u64 null,\n\
        ct_count u64 null,\n\
        ct_sum u64 not null,\n\
        ct_square_sum u64 not null,\n\
        rt_count_server u64 null,\n\
        rt_sum_server u64 not null,\n\
        rt_square_sum_server u64 not null,\n\
        rtt_count_client u64 null,\n\
        rtt_sum_client u64 not null,\n\
        rtt_square_sum_client u64 not null,\n\
        rtt_count_server u64 null,\n\
        rtt_sum_server u64 not null,\n\
        rtt_square_sum_server u64 not null,\n\
        rd_count_client u64 null,\n\
        rd_sum_client u64 not null,\n\
        rd_square_sum_client u64 not null,\n\
        rd_count_server u64 null,\n\
        rd_sum_server u64 not null,\n\
        rd_square_sum_server u64 not null,\n\
        dtt_count_client u64 null,\n\
        dtt_sum_client u64 not null,\n\
        dtt_square_sum_client u64 not null,\n\
        dtt_count_server u64 null,\n\
        dtt_sum_server u64 not null,\n\
        dtt_square_sum_server u64 not null,\n\
        dcerpc_uuid string null\n\
      )\"\n\
    }" } ::
  { demo_url = "node/LowTrafficAlert" ;
    demo_payload = "{\n\
    \"operation\":\n\
      \"NOTIFY \\\"http://firebrigade.com/alert.php?msg=Low%20traffic%20to%20${ip4_server}\\\"\"\n\
    }" } ::
  (list_unfold (fun i lst ->
    if i >= nb_zones then lst else (
      let node_name1 = "PerServerTrafficZone"^ string_of_int i
      and node_name2 = "LowTrafficServersZone"^ string_of_int i in
      { demo_url = "node/"^ node_name1 ;
        demo_payload = "{\n\
        \"operation\":\n\
          \"SELECT MIN capture_begin, MAX capture_end, ip4_server,\n\
                   SUM(traffic_bytes_client + traffic_bytes_server) AS bytes,\n\
                   SUM(traffic_packets_client + traffic_packets_server) AS packets\n\
           WHERE ip4_client IS NOT NULL AND ip4_server IS NOT NULL AND\n\
                 zone_server = "^ string_of_int i ^"\n\
           GROUP BY ip4_server, capture_begin//60_000_000\n\
           COMMIT AND FLUSH WHEN MAX(capture_begin) > min_capture_begin + 10_000_000\"\n\
        }" } ::
      { demo_url = "link/TCPv29/"^ node_name1 ; demo_payload = "" } ::
      { demo_url = "node/"^ node_name2 ;
        demo_payload = "{\n\
        \"operation\":\n\
          \"SELECT * WHERE bytes / (min_capture_begin - max_capture_end) < 1_000_000_000\"\n\
        }" } ::
      { demo_url = "link/"^ node_name1 ^"/"^ node_name2 ; demo_payload = "" } ::
      ({ demo_url = "link/"^ node_name2 ^"/LowTrafficAlert" ; demo_payload = "" }) :: lst)
    ))


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
  match node.Node.command, node.Node.pid  with
  | Some cmd, Some pid when cmd <> "" && pid > 0 ->
    (text "Program running as pid " :: tech_text (string_of_int pid) ::
     text ", command " :: tech_text cmd ::
     br () ::
     text "Tuples (in/selected/out): " ::
       tech_text (string_of_int node.Node.in_tuple_count) :: text "/" ::
       tech_text (string_of_int node.Node.selected_tuple_count) :: text "/" ::
       tech_text (string_of_int node.Node.out_tuple_count) ::
     br () ::
     text "CPU time: " :: tech_text (string_of_float node.Node.cpu_time) :: text "seconds" ::
     br () ::
     text "RAM: " :: tech_text (string_of_int node.Node.ram_usage) :: text "bytes" ::
     br () ::
     (match node.Node.group_count with
      | None -> []
      | Some c -> [ text "Groups: " ; tech_text (string_of_int c) ; br () ]))
  | _ -> []

let view st =
  let spinner st =
    match st.saving with
    | Done (Ok _) ->
      [ txt_span ~a:[class_ "success"] "Saved" ]
    | Done (Error err) ->
      [ txt_span ~a:[class_ "failure"] err ]
    | _ -> [] in
  let node_editor st =
    match st.graph with
    | None ->
      [ text "Select a graph" ]
    | Some graph ->
      let is_editable = graph.status = Edition || graph.status = Compiled in
      [
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
        (* Add a special magic button to build a whole configuration if the
         * graph is empty *)
        (if is_editable && graph.nodes = [] then
           input [] ~a:[type_ "button"; onclick (`DemoCmd (Ok Ojs.null, demo_cmds)); value "DEMO!"]
         else
           div []) ;
        br () ;
        (match get_edited_node st with
        | None ->
          text "Select a node"
        | Some (node, is_creation) ->
          let no_html = div ~a:[class_ "invisible"] []
          and other_nodes =
            List.fold_left (fun lst n ->
              if n != node then n.Node.name::lst else lst) [] graph.nodes in
          div ([
            label [
              text "Node Name:" ;
              if is_creation then (
                let a = [class_ "node-name"; type_ "text"; value node.Node.name; oninput (fun s -> `NameChange s)] in
                input [] ~a
              ) else text node.Node.name ] ;
            br () ;
            label [
              text "Parents:" ;
              if is_editable then
                checkboxes ~action:(fun s -> `ParentsChange s) other_nodes node.Node.parents
              else
                unorderd_list node.Node.parents
            ] ;
            br () ;
            label [
              text "Children:" ;
              if is_editable then
                checkboxes ~action:(fun s -> `ChildrenChange s) other_nodes node.Node.children
              else
                unorderd_list node.Node.children
            ] ;
            br () ;
            label [
              text "Operation:" ;
              (if is_editable then
                textarea ~a:[cols 40; rows 15; class_ "node-op"; oninput (fun s -> `OpChange s)] [ text node.Node.operation ]
              else
                elt "pre" [
                  elt "code" ~a:[class_ "node-op"] [ text node.Node.operation ] ]) ] ;
              (*elt "pre" [
                elt "code" ~a:[id_ "code-editor"; (*attr "contenteditable" "true";*) class_ "node-op"; onkeydown (fun s -> `OpChange s)] [
                  text node.Node.operation ] ] ] ;*)
            br () ;
            (match st.saving with
            | Spinning txt ->
              text txt
            | _ ->
              if is_editable then div [
                (let a = [class_ "node-update"; onclick `UpdateNode; type_ "button";
                          value (if is_creation then "Create" else "Update")] in
                 let a = if node.Node.operation <> "" && node.Node.name <> "" then a
                         else attr "disabled" "" :: a in
                 input [] ~a) ;
                (if is_creation then no_html
                 else
                   input [] ~a:[class_ "node-delete"; onclick `DeleteNode; type_ "button";
                                value "Delete"])
              ] else no_html) ;
            br () ] @
            prog_info node))
      ]
    in
    div [
      div ~a:[class_ "spinner"] (spinner st) ;
      div ~a:[class_ "node-editor"] (node_editor st) ]

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
      url = "start" ; callback = fun r -> `StartedGraph r } in
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
  | `DemoCmd (Ok _ as r, dc::rest) ->
    let c =
      [ Http_put {
        url = dc.demo_url ;
        payload = dc.demo_payload ;
        callback = fun r -> `DemoCmd (r, rest) } ] in
    return ~c { st with saving = Done r }
  | `DemoCmd (r, _) ->
    return ~c:[reload_graph] { st with saving = Done r }
  | `CreateNode ->
    (* TODO: somehow disable selection in the d3 graph *)
    return { st with edited_node = NewNode }
  | `NameChange new_text ->
    edit_node st (fun node -> node.Node.name <- new_text) ;
    return st
  | `OpChange new_value ->
    edit_node st (fun node -> node.Node.operation <- new_value) ;
    return st
  | `ParentsChange s ->
    edit_node st (fun node -> node.Node.parents <- togle_list s node.Node.parents) ;
    return st
  | `ChildrenChange s ->
    edit_node st (fun node -> node.Node.children <- togle_list s node.Node.children) ;
    return st
  | `UpdateNode ->
    let c = ref [] in
    edit_node st (fun node ->
      if st.edited_node = NewNode then ( (* TODO: maybe wait for the update to succeed? *)
        assert (node == st.new_node) ;
        st.edited_node <- FromGraph node.Node.name ;
        st.new_node <- make_empty_node ()
      ) ;
      let url = "node/"^ node.Node.name in
      let payload = JZON.of_node node in
      let txt = "Saving node "^ node.Node.name in
      st.saving <- Spinning txt ;
      c := [ Http_put { url ; payload ; callback = fun r -> `UpdatedNode (r, node) } ]) ;
    return ~c:!c st
  | `UpdatedNode (Ok _ojs, node) ->
    (* Now that we updated the node let's update the links *)
    let url = "links/"^ node.Node.name in
    let payload = JZON.of_links { parents = node.Node.parents ;
                                  children = node.Node.children } in
    let c = [ Http_put { url ; payload ; callback = fun r -> `UpdatedLinks r } ] in
    return ~c { st with saving = Spinning "Saving links" }
  | `UpdatedNode (r, _node) ->
    return { st with saving = Done r }
  | `UpdatedLinks r ->
    return ~c:[reload_graph] { st with saving = Done r }
  | `DeleteNode ->
    let c = ref [] in
    edit_node st (fun node ->
      let url = "node/"^ node.Node.name in
      let txt = "Deleting node "^ node.Node.name in
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
