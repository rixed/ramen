(* Start an HTTP(S) daemon to allow setting up the configuration graph. *)
open Batteries
open BatOption.Infix
open Cohttp
open Cohttp_lwt_unix
open Lwt
open RamenLog
open RamenSharedTypes
open Helpers
module C = RamenConf
module N = RamenConf.Node
module L = RamenConf.Layer
module SL = RamenSharedTypes.Layer
module SN = RamenSharedTypes.Node

let not_implemented msg = fail (HttpError (501, msg))
let bad_request msg = fail (HttpError (400, msg))

let get_content_type headers =
  Header.get headers "Content-Type" |? Consts.json_content_type |> String.lowercase

let get_accept headers =
  let h =
    Header.get headers "Accept" |? Consts.json_content_type |>
    String.lowercase in
  let h =
    try String.split ~by:";" h |> fst
    with Not_found -> h in
  String.split_on_char ',' h

let is_accepting_anything = List.mem "*/*"

let is_accepting content_type accept =
  is_accepting_anything accept || List.mem content_type accept

(* When the client cannot accept the response *)
let cant_accept accept =
  let msg =
    Printf.sprintf "{\"error\": \"Can't produce any of %s\"}\n"
      (IO.to_string (List.print ~first:"" ~last:"" ~sep:", " String.print)
                    accept) in
  fail (HttpError (406, msg))

let check_accept headers content_type =
  let accept = get_accept headers in
  if not (is_accepting content_type accept) then
    cant_accept accept
  else return_unit

let switch_accepted headers al =
  let accept = get_accept headers in
  match List.find (fun (ct, _) -> is_accepting ct accept) al with
  | exception Not_found -> cant_accept accept
  | _, k -> k ()

(* Helper to deserialize an incoming json *)
let of_json headers what ppp body =
  if get_content_type headers <> Consts.json_content_type then
    bad_request "Bad content type"
  else (
    try PPP.of_string_exc ppp body |> return
    with e ->
      !logger.info "%s: Cannot parse received body: %S, Exception %s"
        what body (Printexc.to_string e) ;
      fail e
  )

let ok_body = "{\"success\": true}"
let respond_ok ?(body=ok_body) ?(ct=Consts.json_content_type) () =
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" ct in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let body = body ^"\n" in
  Server.respond_string ~status ~headers ~body ()

let type_of_operation =
  let open Lang.Operation in
  function
  | Yield _ -> "YIELD"
  | Aggregate _ -> "GROUP BY"
  | ReadCSVFile _ -> "READ CSV"
  | ListenOn _ -> "LISTEN"

let layer_node_of_user_string conf ?default_layer s =
  let s = String.trim s in
  (* rsplit because we might want to have '/'s in the layer name. *)
  try String.rsplit ~by:"/" s |> return
  with Not_found ->
    match default_layer with
    | Some l -> return (l, s)
    | None ->
      (* Last resort: look for the first node with that name: *)
      match C.fold_nodes conf None (fun res node ->
              if res = None && node.N.name = s then
                Some (node.N.layer, node.N.name)
              else res) with
      | Some res -> return res
      | None -> fail_with ("node "^ s ^" does not exist")

(*
    Returns the graph (as JSON, dot or mermaid representation)
*)

let rec find_int_opt_metric metrics name =
  let open Binocle in
  match metrics with
  | [] -> None
  | { measure = MInt n ; _ } as m ::_ when m.name = name -> Some n
  | _::rest -> find_int_opt_metric rest name

let find_int_metric metrics name = find_int_opt_metric metrics name |? 0

let rec find_float_metric metrics name =
  let open Binocle in
  match metrics with
  | [] -> 0.
  | { measure = MFloat n ; _ } as m ::_ when m.name = name -> n
  | _::rest -> find_float_metric rest name

let node_info_of_node node =
  let to_expr_type_info lst =
    List.map (fun (rank, typ) -> rank, Lang.Expr.to_expr_type_info typ) lst
  in
  Node.{
    definition = {
      name = node.N.name ;
      operation = node.N.op_text ;
      parents = List.map (fun n -> n.N.name) node.N.parents } ;
      type_of_operation = Some (type_of_operation node.N.operation) ;
      signature = if node.N.signature = "" then None else Some node.N.signature ;
      pid = node.N.pid ;
      input_type = C.list_of_temp_tup_type node.N.in_type |> to_expr_type_info ;
      output_type = C.list_of_temp_tup_type node.N.out_type |> to_expr_type_info ;
      in_tuple_count = find_int_metric node.N.last_report Consts.in_tuple_count_metric ;
      selected_tuple_count = find_int_metric node.N.last_report Consts.selected_tuple_count_metric ;
      out_tuple_count = find_int_metric node.N.last_report Consts.out_tuple_count_metric ;
      group_count = find_int_opt_metric node.N.last_report Consts.group_count_metric ;
      cpu_time = find_float_metric node.N.last_report Consts.cpu_time_metric ;
      ram_usage = find_int_metric node.N.last_report Consts.ram_usage_metric }

let layer_info_of_layer layer =
  SL.{
    name = layer.L.name ;
    nodes = Hashtbl.values layer.L.persist.L.nodes /@
            node_info_of_node |>
            List.of_enum ;
    status = layer.L.persist.L.status ;
    last_started = layer.L.persist.L.last_started ;
    last_stopped = layer.L.persist.L.last_stopped }

let graph_layers conf = function
  | None ->
    Hashtbl.values conf.C.graph.C.layers |>
    List.of_enum |>
    return
  | Some l ->
    try Hashtbl.find conf.C.graph.C.layers l |>
        List.singleton |>
        return
    with Not_found -> bad_request ("Unknown layer "^l)

let get_graph_json _headers layers =
  let body = List.map layer_info_of_layer layers |>
             PPP.to_string get_graph_resp_ppp in
  respond_ok ~body ()

let dot_of_graph layers =
  let dot = IO.output_string () in
  Printf.fprintf dot "digraph g {\n" ;
  List.iter (fun layer ->
    Hashtbl.iter (fun _ node ->
        Printf.fprintf dot "\t%S\n" (N.fq_name node)
      ) layer.L.persist.L.nodes
    ) layers ;
  Printf.fprintf dot "\n" ;
  List.iter (fun layer ->
    Hashtbl.iter (fun _ node ->
        List.iter (fun p ->
            Printf.fprintf dot "\t%S -> %S\n" (N.fq_name p) (N.fq_name node)
          ) node.N.parents
      ) layer.L.persist.L.nodes
    ) layers ;
  Printf.fprintf dot "}\n" ;
  IO.close_out dot

let get_graph_dot _headers layers =
  let body = dot_of_graph layers in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.dot_content_type in
  Server.respond_string ~headers ~status ~body ()

let mermaid_of_graph layers =
  (* Build unique identifier that are valid for mermaid: *)
  let is_alphanum c =
    Char.(is_letter c || is_digit c) in
  let mermaid_id n =
    "id_" ^ (* To make it start with an alpha *)
    String.replace_chars (fun c ->
      if is_alphanum c then String.of_char c
      else string_of_int (Char.code c)) n
  (* And valid labels *)
  and mermaid_label n =
    "\""^ String.nreplace n "\"" "#quot;" ^"\""
  in
  let txt = IO.output_string () in
  Printf.fprintf txt "graph LR\n" ;
  List.iter (fun layer ->
    Hashtbl.iter (fun _ node ->
        Printf.fprintf txt "%s(%s)\n"
          (mermaid_id (N.fq_name node))
          (mermaid_label node.N.name)
      ) layer.L.persist.L.nodes
    ) layers ;
  Printf.fprintf txt "\n" ;
  List.iter (fun layer ->
    Hashtbl.iter (fun _ node ->
        List.iter (fun p ->
            Printf.fprintf txt "\t%s-->%s\n"
              (mermaid_id (N.fq_name p))
              (mermaid_id (N.fq_name node))
          ) node.N.parents
      ) layer.L.persist.L.nodes
    ) layers ;
  IO.close_out txt

let get_graph_mermaid _headers layers =
  let body = mermaid_of_graph layers in
  let status = `Code 200 in
  let headers = Header.init_with "Content-Type" Consts.mermaid_content_type in
  let headers = Header.add headers "Access-Control-Allow-Origin" "*" in
  let headers = Header.add headers "Access-Control-Allow-Methods" "POST" in
  let headers = Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
  Server.respond_string ~headers ~status ~body ()

let get_graph conf headers layer_opt =
  let accept = get_accept headers in
  let%lwt layers = graph_layers conf layer_opt in
  if is_accepting Consts.json_content_type accept then
    get_graph_json headers layers
  else if is_accepting Consts.dot_content_type accept then
    get_graph_dot headers layers
  else if is_accepting Consts.mermaid_content_type accept then
    get_graph_mermaid headers layers
  else
    cant_accept accept

(*
    Add/Remove layers

    Layers and nodes within a layer are referred to via name that can be
    anything as long as they are unique.  So the clients decide on the name.
    The server ensure uniqueness by forbidding creation of a new layers by the
    same name as one that exists already.

*)

let find_node_or_fail conf layer_name node_name =
  try C.find_node conf layer_name node_name |> return
  with Not_found ->
    fail_with ("Node "^ layer_name ^"/"^ node_name ^" does not exist")

let node_of_name conf layer_name node_name =
  if node_name = "" then fail_with "Empty string is not a valid node name"
  else find_node_or_fail conf layer_name node_name

let put_layer conf headers body =
  let%lwt msg = of_json headers "Uploading layer" put_layer_req_ppp body in

  (* Disallow anonymous layers for simplicity: *)
  if msg.name = "" then
    bad_request "Layers must have non-empty names"
  (* Check that this layer is new *)
  else if Hashtbl.mem conf.C.graph.C.layers msg.name then
    bad_request ("Layer "^ msg.name ^" already present")
  else (
    (* TODO: Check that this layer node names are unique within the layer *)
    (* Create all the nodes *)
    let%lwt () = Lwt_list.iter_s (fun def ->
        let name =
          if def.Node.name <> "" then def.Node.name
          else N.make_name () in
        wrap (fun () -> C.add_node conf name msg.name def.Node.operation)
      ) msg.nodes in
    (* Then all the links *)
    let%lwt () = Lwt_list.iter_s (fun def ->
        let%lwt _layer, dst = node_of_name conf msg.name def.Node.name in
        Lwt_list.iter_s (fun p ->
            let%lwt parent_layer, parent_name =
              layer_node_of_user_string conf ~default_layer:msg.name p in
            let%lwt _layer, src = node_of_name conf parent_layer parent_name in
            wrap (fun () -> C.add_link conf src dst)
          ) def.SN.parents
      ) msg.nodes in
    respond_ok ())
  (* TODO: why wait before compiling this layer? *)

(*
    Top query: display a page with details of operation
*)

let hostname =
  let cached = ref "" in
  fun () ->
    if !cached <> "" then return !cached else
    let%lwt c =
      catch
        (fun () -> run ~timeout:2. [| "hostname" |])
        (function _ -> return "unknown host") in
    cached := c ;
    return c

let top conf headers params =
  let%lwt () = check_accept headers Consts.html_content_type in
  let enc = Uri.pct_encode in
  let style = {|
  <style media="screen" type="text/css">
    body { margin:0; height: 100%; background-color: #eee; }
    #global, #top, #details, .subitem {
      display: flex;
      flex-direction: row;
      flex-wrap: nowrap;
      overflow: auto; }
    body, #layers, #nodes, #input, #operation, #tail, .subtree {
      display: flex;
      flex-direction: column;
      flex-wrap: nowrap;
      overflow: auto; }

    .subtree { }
    .subitem {
      padding-left: 20px;
      background-image:
        url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='20' height='32'><line x1='6' y1='10' x2='18' y2='10' stroke-width='2' stroke='black'/><circle cx='6' cy='10' r='5' fill='white' stroke='black' stroke-width='2'/></svg>"),
        url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='20' height='32'><line x1='6' y1='0' x2='6' y2='32' stroke-width='2' stroke='black'/></svg>");
      background-repeat: no-repeat, repeat-y; }
    .subitem:last-child {
        background:
          url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='20' height='32'><line x1='6' y1='0' x2='6' y2='10' stroke-width='2' stroke='black'/><line x1='6' y1='10' x2='18' y2='10' stroke-width='2' stroke='black'/><circle cx='6' cy='10' r='5' fill='white' stroke='black' stroke-width='2'/></svg>")
        no-repeat; }

    table { border-collapse: collapse; border-spacing: 10; }
    td, th { padding: 0 1em 0 1em; }
    tfoot td, th { font-weight: bold; text-align: center; }
    span.null { font-style: italic; color: #888; font-size: 80%; }
    a, a:visited { color: #22c; text-decoration: none; }
    #global { height: 3em; width: 100%; }
    #top { max-height: 80%; width: 100%; flex-grow: 1; }
    #top .selected { background-color: #ddd; }
      #layers { padding-right: 1em; max-width:50%; }
        #layers div { display: flex; flex-direction: column; }
        #layers div p.name { display: flex; flex-direction: row; justify-content: space-between; }
        #layers .name, #layers .info, #layers .info p { margin: 0px; }
      #nodes { flex-grow: 1; }
        #nodes tbody td:first-child a { display: flex; flex-direction: row; justify-content: space-between; }
      #nodes a { display: block; width: 100%; color: black; }
      #nodes tbody tr:hover { background-color: #fff; }
    #top tbody td hr { margin-left: 0; margin-top: 0; margin-bottom: 0; border: 1px solid #aaa; }
    #details { max-height: 80%; width: 100%; flex-grow: 0.2; }
      #input { max-width: 50%; }
        #input p { margin: 0; padding: 0 }
      #operation { flex-grow: 1; }
    #tail { flex-grow: 0.2; width: 100%; }

    span.label { margin-right: 0.2em; font-size: 80%; color: #222; font-weight: 700; }
    span.value { margin-right: 1em; font-size: 85%; color: #003; }
    h1 { position: relative; top:0; left:0; background-color: #fff; color: #000010;
         text-align: left; margin: 0; padding: 0.5em 1em 0.5em 1em;
         font-weight: 700; font-size: 100%; }
    th p { margin: 0px; }
    th p.type { font-size: 60%; font-style: italic; }
    td.number, td.FLOAT, td.U8, td.U16, td.U32, td.U64, td.U128, td.I8,
    td.I16, td.I32, td.I64, td.I128 { text-align: right; font-family: mono; }

    .icon { }
  </style>
|} in
  let snode =
    try Some (Hashtbl.find params "snode")
    with Not_found -> None in
  let scol = Hashtbl.find_default params "scol" "1" in
  let href ?(path="/top") ?(snode=snode) ?(scol=scol) () =
    path ^"?"^ (match snode with None -> "" | Some s -> "&snode="^ enc s)
         ^"&scol="^ enc scol in
  let%lwt sel_node =
    match snode with
    | None -> return None
    | Some node_name ->
      let%lwt sel_layer_name, sel_node_name =
        layer_node_of_user_string conf node_name in
      let%lwt _sel_layer, sel_node =
        node_of_name conf sel_layer_name sel_node_name in
      return (Some sel_node) in
  let tagged tag ?(attr=[]) x =
    let attr =
      IO.to_string (
        List.print ~first:"" ~last:"" ~sep:" "
          (fun oc (t,v) -> Printf.fprintf oc "%s=%S" t v)) attr in
    let attr = if attr = "" then "" else " "^attr in
    "<"^ tag ^ attr ^">"^ x ^"</"^ tag ^">" in
  let td = tagged "td" and th = tagged "th" in
  let str_of_float f =
    let s = Printf.sprintf "%.3f" f in
    let i = ref (String.length s - 1) in
    while !i > 0 && s.[!i] = '0' do s.[!i] <- '_' ; decr i done ;
    if !i > 0 && s.[!i] = '.' then s.[!i] <- '_' ;
    String.nreplace ~str:s ~sub:"_" ~by:"&nbsp;" in
  let icon_of_layer layer =
    let icon, path, alt =
      match layer.L.persist.L.status with
      | Layer.Edition -> "&#x270E;", "/compile/"^ enc layer.L.name, "compile"
      | Layer.Compiling -> "&#x2610;", "", "reload"
      | Layer.Compiled -> "&#x2611;", "/start/"^ enc layer.L.name, "start"
      | Layer.Running -> "&#9881;", "/stop/"^ enc layer.L.name, "stop"  in
    tagged "a" ~attr:["class","icon"; "href",href ~path (); "title",alt] icon in
  let dispname_of_type nullable scalar_typ =
    Lang.Scalar.string_of_typ scalar_typ ^
    (if nullable then " (or null)" else "") in
  let pretty_th title subtitle =
    th (
      tagged "p" title ^
      (if subtitle = "" then "" else tagged "p" ~attr:["class","type"] subtitle)) in
  let reload =
    (* To refresh the top we reload (toward top not whatever last command was
     * called!) every 10s, which is also the interval at which ramen receive
     * reports from the workers: *)
    Printf.sprintf {|<meta http-equiv="refresh" content="10;url=%s" />|}
      (href ()) in
  let%lwt hostname = hostname () in
  let header_panel =
    tagged "p" ("Ramen v0.1 running on "^ tagged "em" hostname ^".") in
  let labbeled_value l v =
    tagged "p" (
      tagged "span" ~attr:["class","label"] (l^":") ^
      tagged "span" ~attr:["class","value"] v) in
  let short_month = function
    | 0 -> "Jan" | 1 -> "Fev" | 2 -> "Mar" | 3 -> "Apr" | 4 -> "May"
    | 5 -> "Jun" | 6 -> "Jul" | 7 -> "Aug" | 8 -> "Sep" | 9 -> "Oct"
    | 10 -> "Nov" | 11 -> "Dec" | _ -> "?!?" in
  let date_of_ts = function
    | Some ts ->
      let open Unix in
      let tm = localtime ts in
      Printf.sprintf "%d&nbsp;%s at %02dh%02d"
        tm.tm_mday (short_month tm.tm_mon) tm.tm_hour tm.tm_min
    | None -> "never" in
  let print_scalar_value oc = function
    | VFloat f ->
      String.print oc (str_of_float f)
    | VNull ->
      String.print oc (tagged "span" ~attr:["class","null"] "NULL")
    | x -> Lang.Scalar.print oc x in
  let layers_panel =
    IO.to_string
      (Enum.print ~first:"" ~last:"" ~sep:""
        (fun oc (layer_name, layer) ->
          let attr =
            match sel_node with
            | Some n when n.N.layer = layer_name -> ["class","selected"]
            | _ -> [] in
          String.print oc (
            tagged "div" ~attr (
              tagged "p" ~attr:["class","name"] (
                layer_name ^ icon_of_layer layer) ^
              tagged "div" ~attr:["class","info"] (
                labbeled_value "#nodes" (string_of_int (Hashtbl.length layer.L.persist.L.nodes)) ^
                labbeled_value "started" (date_of_ts layer.L.persist.L.last_started) ^
                labbeled_value "stopped" (date_of_ts layer.L.persist.L.last_stopped))))))
      (Hashtbl.enum conf.C.graph.C.layers) in
  let top_sorter col n1 n2 =
    (* Numbers are sorted greater to smaller while strings are sorted
     * in ascending order: *)
    match col with
    | "2" -> String.compare (type_of_operation n1.N.operation)
                            (type_of_operation n2.N.operation)
    | "3" -> Int.compare (find_int_metric n2.N.last_report Consts.in_tuple_count_metric)
                         (find_int_metric n1.N.last_report Consts.in_tuple_count_metric)
    | "4" -> Int.compare (find_int_metric n2.N.last_report Consts.selected_tuple_count_metric)
                         (find_int_metric n1.N.last_report Consts.selected_tuple_count_metric)
    | "5" -> Int.compare (find_int_metric n2.N.last_report Consts.out_tuple_count_metric)
                         (find_int_metric n1.N.last_report Consts.out_tuple_count_metric)
    | "9" -> Float.compare (find_float_metric n2.N.last_report Consts.cpu_time_metric)
                           (find_float_metric n1.N.last_report Consts.cpu_time_metric)
    | "10" -> Int.compare (find_int_metric n2.N.last_report Consts.ram_usage_metric)
                          (find_int_metric n1.N.last_report Consts.ram_usage_metric)
    | _ ->
      match String.icompare n1.N.layer n2.N.layer with
      | 0 -> String.icompare n1.N.name n2.N.name
      | x -> x in
  let nodes =
    C.fold_nodes conf [] (fun prev node -> node :: prev) |>
    List.sort (top_sorter scol) in
  (* Start by computing the total per columns so we can display an histogram
   * in the tbody: *)
  let layers = ref Set.empty and tot_nodes = ref 0 and tot_ins = ref 0
  and tot_sels = ref 0 and tot_outs = ref 0 and tot_groups = ref 0
  and tot_cpu = ref 0. and tot_ram = ref 0 and max_ins = ref 0
  and max_sels = ref 0 and max_outs = ref 0 and max_groups = ref 0
  and max_cpu = ref 0. and max_ram = ref 0 in
  List.iter (fun node ->
      let ins = find_int_metric node.N.last_report Consts.in_tuple_count_metric
      and sels = find_int_metric node.N.last_report Consts.selected_tuple_count_metric
      and outs = find_int_metric node.N.last_report Consts.out_tuple_count_metric
      and cpu = find_float_metric node.N.last_report Consts.cpu_time_metric
      and ram = find_int_metric node.N.last_report Consts.ram_usage_metric in
      layers := Set.add node.N.layer !layers ;
      incr tot_nodes ;
      tot_ins := !tot_ins + ins ;
      tot_sels := !tot_sels + sels ;
      tot_outs := !tot_outs + outs ;
      tot_cpu := !tot_cpu +. cpu ;
      tot_ram := !tot_ram + ram ;
      max_ins := max !max_ins ins ;
      max_sels := max !max_sels sels ;
      max_outs := max !max_outs outs ;
      max_cpu := max !max_cpu cpu ;
      max_ram := max !max_ram ram ;
      Option.may (fun g ->
          tot_groups := !tot_groups + g ;
          max_groups := max !max_groups g)
        (find_int_opt_metric node.N.last_report Consts.group_count_metric)
    ) nodes ;
  let node_columns =
    [ "layer", true, "" ; "name", true, "" ; "op", true, "" ;
      "#in", true, "tuples" ; "#selected", true, "tuples" ;
      "#out", true, "tuples" ; "#groups", false, "" ; "parents", false, "" ;
      "children", false, "" ; "CPU", true, "seconds" ; "RAM", true, "bytes" ;
      "PID", false, "" ; "signature", false, "" ; "export", false, "" ] in
  let nodes_head =
    tagged "thead" (tagged "tr" (String.concat "" (
      List.mapi (fun i (col, sortable, subtitle) ->
        if sortable then
          let attr = ["href", href ~scol:(string_of_int i) ()] in
          pretty_th (tagged "a" ~attr col) (tagged "a" ~attr subtitle)
        else
          pretty_th col subtitle) node_columns))) in
  let nodes_body =
    let tr_of_node node =
      let ta x =
        (* Makes the box clickable even if empty (chrome): *)
        let x = if x <> "" then x else "&nbsp;" in
        tagged "a" ~attr:["href", href ~snode:(Some (N.fq_name node)) ()] x in
      let tdi = td ~attr:["class", "number"] % ta % string_of_int
      and tdf = td ~attr:["class", "number"] % ta % str_of_float in
      let tdih tot x =
        if tot = 0 then tdi x else
        let w = float_of_int (100 * x) /. float_of_int tot in
        td ~attr:["class", "number"] (
          ta (string_of_int x) ^ Printf.sprintf "<hr width=\"%f%%\"/>" w)
      and tdfh tot x =
        if tot = 0. then tdf x else
        let w = 100. *. x /. tot in
        td ~attr:["class", "number"] (
          ta (str_of_float x) ^ Printf.sprintf "<hr width=\"%f%%\"/>" w) in
      let td = td % ta in
      let groups =
        match find_int_opt_metric node.N.last_report Consts.group_count_metric with
        | None -> td "n.a" | Some x -> tdih !max_groups x in
      let pid =
        match node.N.pid with None -> td "n.a" | Some p -> tdi p in
      (* FIXME: really, save this into node! *)
      let ins = find_int_metric node.N.last_report Consts.in_tuple_count_metric
      and sels = find_int_metric node.N.last_report Consts.selected_tuple_count_metric
      and outs = find_int_metric node.N.last_report Consts.out_tuple_count_metric
      and cpu = find_float_metric node.N.last_report Consts.cpu_time_metric
      and ram = find_int_metric node.N.last_report Consts.ram_usage_metric in
      let short_node_list ?(max_len=20) lst =
        abbrev max_len (List.fold_left (fun s n ->
            if String.length s > max_len then s else
            s ^ (if s <> "" then ", " else "")
              ^ (if n.N.layer = node.N.layer then n.N.name else N.fq_name n)
          ) "" lst) in
      let attr =
        match sel_node with Some n when n == node -> ["class","selected"]
                          | _ -> [] in
      tagged "tr" ~attr (
        td node.N.layer ^ td node.N.name ^
        td (type_of_operation node.N.operation) ^
        tdih !max_ins ins ^ tdih !max_sels sels ^ tdih !max_outs outs ^ groups ^
        td (short_node_list node.N.parents) ^
        td (short_node_list node.N.children) ^
        tdfh !max_cpu cpu ^ tdih !max_ram ram ^ pid ^ td node.N.signature ^
        td (if Lang.Operation.is_exporting node.N.operation then "&#x2713;" else "&nbsp;")) in
    tagged "tbody" (String.concat "" (List.map tr_of_node nodes)) in
  let nodes_foot =
    let tdi = td ~attr:["class", "number"] % string_of_int
    and tdf = td ~attr:["class", "number"] % str_of_float in
    tagged "tfoot" (tagged "tr" (
      tdi (Set.cardinal !layers) ^ tdi !tot_nodes ^ td "" ^ tdi !tot_ins ^
      tdi !tot_sels ^ tdi !tot_outs ^ tdi !tot_groups ^ td "" ^ td "" ^
      tdf !tot_cpu ^ tdi !tot_ram ^ td "" ^ td "" ^ td "")) in
  let nodes_panel = tagged "table" (nodes_head ^ nodes_body ^ nodes_foot) in
  let input_panel, op_panel, tail_panel =
    match sel_node with
    | None ->
      tagged "p" "",
      tagged "p" "Select a node to see the operation it performs",
      tagged "p" "Select a node to see its output"
    | Some node ->
      (if node.N.in_type.C.finished_typing then
        C.tup_typ_of_temp_tup_type node.N.in_type |>
        List.fold_left (fun s ft ->
            s ^ labbeled_value ft.typ_name (dispname_of_type ft.nullable ft.typ)
          ) ""
      else tagged "p" (tagged "em" "not compiled")),
      tagged "pre" node.N.op_text,
      (if node.N.out_type.C.finished_typing then (
        let out_tuple_type = C.tup_typ_of_temp_tup_type node.N.out_type in
        let class_name_of_value v = scalar_type_of v |> Lang.Scalar.string_of_typ in
        tagged "table" (
          tagged "thead" (
            IO.to_string
              (List.print ~first:"<tr>" ~last:"</tr>" ~sep:""
                (fun oc ft ->
                  String.print oc
                    (pretty_th ft.typ_name (dispname_of_type ft.nullable ft.typ))))
              out_tuple_type) ^
          tagged "tbody" (
            if Lang.Operation.is_exporting node.N.operation then (
              let _, values =
                let max_res = 8 in
                let since = RamenExport.since_of_last_tuples max_res node in
                RamenExport.fold_tuples ~max_res ~since node [] (fun _ tup prev ->
                  List.cons tup prev) in
              IO.to_string (
                List.print ~first:"" ~last:"" ~sep:"\n"
                  (Array.print ~first:"<tr>" ~last:"</tr>" ~sep:""
                     (fun oc v ->
                        Printf.fprintf oc "<td class=%S>%a</td>"
                          (class_name_of_value v)
                          print_scalar_value v))) values)
            else tagged "tr" (
              tagged ~attr:["colspan", string_of_int (List.length out_tuple_type)] "td" (
                tagged "p" ("node "^ N.fq_name node ^" does not export data")))))
      ) else tagged "p" (tagged "em" "not compiled")) in
  let page =
    tagged "html" (
      tagged "head" (reload ^ style) ^
      tagged "body" ({|
        <div id="global">|} ^ header_panel ^ {|</div>
        <div id="top">
          <div id="layers"><h1>Layers</h1>|} ^ layers_panel ^ {|</div>
          <div id="nodes"><h1>Nodes</h1>|} ^ nodes_panel ^ {|</div>
        </div>
        <div id="details">
          <div id="input"><h1>Input</h1>|} ^ input_panel ^ {|</div>
          <div id="operation"><h1>Operation</h1>|} ^ op_panel ^ {|</div>
        </div>
        <div id="tail"><h1>Output</h1>|} ^ tail_panel ^ {|</div>|})) in
  respond_ok ~body:page ~ct:Consts.html_content_type ()

(*
    Whole graph operations: compile/run/stop
*)

let compile conf headers layer_opt params =
  let%lwt layers = graph_layers conf layer_opt in
  catch
    (fun () ->
      let rec loop left_try layers =
        !logger.debug "%d layers left to compile..." (List.length layers) ;
        if layers = [] then return_unit else
        if left_try < 0 then bad_request "Unsolvable dependency loop" else
        Lwt_list.fold_left_s (fun failed layer ->
            let open Compiler in
            catch
              (fun () -> let%lwt () = compile conf layer in
                         return failed)
              (function AlreadyCompiled -> return failed
                      | MissingDependency n ->
                        !logger.debug "We miss node %s" (N.fq_name n) ;
                        return (layer::failed)
                      | e -> fail e)
          ) [] layers >>=
        loop (left_try-1)
      in
      let%lwt () = loop (List.length layers) layers in
      switch_accepted headers [
        Consts.json_content_type, (fun () -> respond_ok ()) ;
        Consts.html_content_type, (fun () -> top conf headers params) ])
    (function Lang.SyntaxError e | C.InvalidCommand e -> bad_request e
            | e -> fail e)

let run conf headers layer_opt params =
  let%lwt layers = graph_layers conf layer_opt in
  let layers = L.order layers in
  try
    List.iter (fun layer ->
        let open RamenProcesses in
        try run conf layer with AlreadyRunning -> ()
      ) layers ;
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ;
      Consts.html_content_type, (fun () -> top conf headers params) ]
  with (Lang.SyntaxError e | C.InvalidCommand e) ->
    bad_request e

let stop conf headers layer_opt params =
  let%lwt layers = graph_layers conf layer_opt in
  try
    List.iter (fun layer ->
        let open RamenProcesses in
        try stop conf layer with NotRunning -> ()
      ) layers ;
    switch_accepted headers [
      Consts.json_content_type, (fun () -> respond_ok ()) ;
      Consts.html_content_type, (fun () -> top conf headers params) ]
  with C.InvalidCommand e -> bad_request e

(*
    Exporting tuples

    Clients can request to be sent the tuples from an exporting node.
*)

let export conf headers layer_name node_name body =
  let%lwt () = check_accept headers Consts.json_content_type in
  let%lwt req =
    if body = "" then return empty_export_req else
    of_json headers ("Exporting from "^ node_name) export_req_ppp body in
  (* Check that the node exists and exports *)
  let%lwt layer, node = find_node_or_fail conf layer_name node_name in
  if not (L.is_typed layer) then
    bad_request ("node "^ node_name ^" is not typed (yet)")
  else if not (Lang.Operation.is_exporting node.N.operation) then
    bad_request ("node "^ node_name ^" does not export data")
  else (
    let start = Unix.gettimeofday () in
    let tuple_type = C.tup_typ_of_temp_tup_type node.N.out_type in
    let rec loop () =
      let first, values =
        RamenExport.fold_tuples ?since:req.since ?max_res:req.max_results
                                node [] (fun _ tup prev -> List.cons tup prev) in
      let dt = Unix.gettimeofday () -. start in
      if values = [] && dt < (req.wait_up_to |? 0.) then (
        (* TODO: sleep for dt, queue the wakener on this history,
         * and wake all the sleeps when a tuple is received *)
        Lwt_unix.sleep 0.1 >>= loop
      ) else (
        (* Store it in column to save variant types: *)
        let resp =
          { first ;
            columns = RamenExport.columns_of_tuples tuple_type values |>
                      List.map (fun (typ, nullmask, column) ->
                        typ, Option.map RamenBitmask.to_bools nullmask, column) } in
        let body = PPP.to_string export_resp_ppp resp in
        respond_ok ~body ()
      ) in
    loop ())

(*
    API for Workers: health and report
*)

let report conf _headers layer name body =
  (* TODO: check application-type is marshaled.ocaml *)
  let last_report = Marshal.from_string body 0 in
  let%lwt _layer, node = find_node_or_fail conf layer name in
  node.N.last_report <- last_report ;
  respond_ok ()

(*
    Grafana Datasource: autocompletion of node/field names
*)

let complete_nodes conf headers body =
  let%lwt msg = of_json headers "Complete tables" complete_node_req_ppp body in
  let body =
    C.complete_node_name conf msg.node_prefix |>
    PPP.to_string complete_resp_ppp
  in
  respond_ok ~body ()

let complete_fields conf headers body =
  let%lwt msg = of_json headers "Complete fields" complete_field_req_ppp body in
  let body =
    C.complete_field_name conf msg.node msg.field_prefix |>
    PPP.to_string complete_resp_ppp
  in
  respond_ok ~body ()

(*
    Grafana Datasource: data queries
*)

let timeseries conf headers body =
  let open Lang.Operation in
  let%lwt msg = of_json headers "time series query" timeseries_req_ppp body in
  let ts_of_node_field req layer node data_field =
    let%lwt _layer, node = find_node_or_fail conf layer node in
    if not (is_exporting node.N.operation) then
      fail_with ("node "^ node.N.name ^" does not export data")
    else match export_event_info node.N.operation with
    | None ->
      fail_with ("node "^ node.N.name ^" does not specify event time info")
    | Some ((start_field, start_scale), duration_info) ->
      let open RamenExport in
      let consolidation =
        match String.lowercase req.consolidation with
        | "min" -> bucket_min | "max" -> bucket_max | _ -> bucket_avg in
      wrap (fun () ->
        build_timeseries
          node start_field start_scale data_field duration_info
          msg.max_data_points msg.from msg.to_ consolidation)
  and create_temporary_node select_x select_y from where =
    (* First, we need to find out the name for this operation, and create it if
     * it does not exist yet. Name must be given by the operation and parent, so
     * that we do not create new nodes when not required (avoiding a costly
     * compilation and losing export history). This is not equivalent to the
     * signature: the signature identifies operation and types (aka the binary)
     * but not the data (since the same worker can be placed at several places
     * in the graph); while here we want to identify the data, that depends on
     * everything the user sent (aka operation text and parent name) but for the
     * formatting. We thus start by parsing and pretty-printing the operation: *)
    let%lwt parent_layer, parent_name =
      layer_node_of_user_string conf from in
    let%lwt _layer, parent = node_of_name conf parent_layer parent_name in
    let%lwt op_text =
      if select_x = "" then (
        let open Lang.Operation in
        match parent.N.operation with
        | Aggregate { export = Some (Some ((start, scale), DurationConst dur)) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s AS data \
             EXPORT EVENT STARTING AT %s * %g WITH DURATION %g"
            start select_y
            start scale dur |> return
        | Aggregate { export = Some (Some ((start, scale), DurationField (dur, scale2))) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s AS data \
             EXPORT EVENT STARTING AT %s * %g WITH DURATION %s * %g"
            start select_y
            start scale dur scale2 |> return
        | Aggregate { export = Some (Some ((start, scale), StopField (stop, scale2))) ; _ } ->
          Printf.sprintf
            "SELECT %s, %s, %s AS data \
             EXPORT EVENT STARTING AT %s * %g AND STOPPING AT %s * %g"
            start stop select_y
            start scale stop scale2 |> return
        | _ ->
          fail_with "This parent does not provide time information"
      ) else return (
        "SELECT "^ select_x ^" AS time, "
                 ^ select_y ^" AS data \
         EXPORT EVENT STARTING AT time") in
    let op_text =
      if where = "" then op_text else op_text ^" WHERE "^ where in
    let%lwt operation = wrap (fun () -> C.parse_operation op_text) in
    let reformatted_op = IO.to_string Lang.Operation.print operation in
    let layer_name =
      "temp/from_"^ from ^"_"^ Cryptohash_md4.(string reformatted_op |> to_hex)
    and node_name = "operation" in
    (* So far so good. In all likelihood this layer exists already: *)
    (if Hashtbl.mem conf.C.graph.C.layers layer_name then (
      !logger.debug "Layer %S already there" layer_name ;
      return_unit
    ) else (
      (* Add this layer to the running configuration: *)
      let layer =
        C.add_parsed_node ~timeout:300.
          conf node_name layer_name op_text operation in
      let node = Hashtbl.find layer.L.persist.L.nodes node_name in
      let%lwt () = wrap (fun () -> C.add_link conf parent node) in
      let%lwt () = Compiler.compile conf layer in
      wrap (fun () -> RamenProcesses.run conf layer)
    )) >>= fun () ->
    return (layer_name, node_name, "data")
  in
  catch
    (fun () ->
      let%lwt resp = Lwt_list.map_s (fun req ->
          let%lwt layer_name, node_name, data_field =
            match req.spec with
            | Predefined { node ; data_field } ->
              let%lwt layer, node = layer_node_of_user_string conf node in
              return (layer, node, data_field)
            | NewTempNode { select_x ; select_y ; from ; where } ->
              create_temporary_node select_x select_y from where in
          let%lwt _layer, node = find_node_or_fail conf layer_name node_name in
          RamenProcesses.use_layer conf (Unix.gettimeofday ()) node.N.layer ;
          let%lwt times, values =
            ts_of_node_field req layer_name node_name data_field in
          return { id = req.id ; times ; values }
        ) msg.timeseries in
      let body = PPP.to_string timeseries_resp_ppp resp in
      respond_ok ~body ())
    (function Failure err -> bad_request err | e -> fail e)

(* A thread that hunt for unused layers *)
let rec timeout_layers conf =
  let%lwt () = wrap (fun () -> RamenProcesses.timeout_layers conf) in
  let%lwt () = Lwt_unix.sleep 7.1 in
  timeout_layers conf

let start do_persist debug to_stderr ramen_url version_tag persist_dir port
          cert_opt key_opt () =
  let logdir = if to_stderr then None else Some (persist_dir ^"/log") in
  Option.may mkdir_all logdir ;
  logger := make_logger ?logdir debug ;
  let conf = C.make_conf do_persist ramen_url debug version_tag persist_dir in
  (* When there is nothing to do, listen to collectd!
   * (TODO: make this an option) *)
  if Hashtbl.is_empty conf.C.graph.C.layers then (
    !logger.info "Adding default nodes since we have nothing to do..." ;
    C.add_node conf "collectd" "demo" "LISTEN FOR COLLECTD") ;
  async (fun () -> timeout_layers conf) ;
  let router meth path params headers body =
    (* The function called for each HTTP request: *)
      match meth, path with
      (* API *)
      | `GET, ["graph"] -> get_graph conf headers None
      | `GET, ["graph" ; layer] -> get_graph conf headers (Some layer)
      | `PUT, ["graph"] -> put_layer conf headers body
(*      | `DELETE, ["graph" ; layer] -> del_layer conf headers *)
      | `GET, ["compile"] -> compile conf headers None params
      | `GET, ["compile" ; layer] -> compile conf headers (Some layer) params
      | `GET, ["run" | "start"] -> run conf headers None params
      | `GET, ["run" | "start" ; layer] -> run conf headers (Some layer) params
      | `GET, ["stop"] -> stop conf headers None params
      | `GET, ["stop" ; layer] -> stop conf headers (Some layer) params
      | (`GET|`POST), ["export" ; layer ; node] ->
        (* TODO: a variant where we do not have to specify layer *)
        (* We must allow both POST and GET for that one since we have an optional
         * body (and some client won't send a body with a GET) *)
        export conf headers layer node body
      (* API for children *)
      | `PUT, ["report" ; layer ; node] -> report conf headers layer node body
      (* Grafana datasource plugin *)
      | `GET, ["grafana"] -> respond_ok ()
      | `POST, ["complete"; "nodes"] ->
        complete_nodes conf headers body
      | `POST, ["complete"; "fields"] ->
        complete_fields conf headers body
      | `POST, ["timeseries"] ->
        timeseries conf headers body
      | `OPTIONS, _ ->
        let headers = Header.init_with "Access-Control-Allow-Origin" "*" in
        let headers = Header.add headers "Access-Control-Allow-Methods" "POST" in
        let headers = Header.add headers "Access-Control-Allow-Headers" "Content-Type" in
        Server.respond_string ~status:(`Code 200) ~headers ~body:"" ()
      (* Top *)
      | `GET, ([]|["top"|"index.html"]) -> top conf headers params
      (* Errors *)
      | `PUT, _ | `GET, _ | `DELETE, _ ->
        fail (HttpError (404, "No such resource"))
      | _ ->
        fail (HttpError (405, "Method not implemented"))
  in
  Lwt_main.run (http_service port cert_opt key_opt router)
