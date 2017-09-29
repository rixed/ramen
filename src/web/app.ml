open Js_of_ocaml
open Engine

(* Printers *)

let dec_num = 3

let str_of_float_str' s =
  let i = ref (String.length s - 1) in
  while !i > 0 && s.[!i] = '0' do decr i done ;
  if !i > 0 && s.[!i] = '.' then decr i ;
  (* Replace all chars after i with non-breakable spaces *)
  let nb_spcs = String.length s - !i - 1 in
  String.sub s 0 (!i + 1) ^ string_times nb_spcs " "

let str_of_float_str s =
  match String.index s '.' with
  | exception Not_found -> s
  | i ->
    (* FIXME: round instead of truncate *)
    str_of_float_str' (String.sub (s ^ "00000") 0 (i + dec_num + 1))

let str_of_float f =
  let s = Printf.sprintf "%.*f" dec_num f in
  str_of_float_str' s

(* The types we will use to deserialize JSon. Cannot be the same as
 * RamenSharedTypes because this json unparser is not compatible
 * with PPP (not because of some deficiencies in those implementations
 * but because it is ambiguous how to map rich ML types into poor JSON
 * types - if only for the various integer types but also for the
 * algebraic types). Therefore we have to unserialize by hand. *)

(* State variables *)

type layer_status = Edition | Compiling | Compiled | Running

module Layer =
struct
  let status_of_string = function
    "Edition" -> Edition
  | "Compiling" -> Compiling
  | "Compiled" -> Compiled
  | "Running" -> Running
  | _ -> assert false

  type t =
    { name : string ;
      status : layer_status ;
      status_str : string ;
      nb_nodes : int ;
      last_started : float option ;
      last_stopped : float option }
  let to_string l =
    string_of_record [
      "name", string_of_string l.name ;
      "status", string_of_string l.status_str ;
      "nb_nodes", string_of_int l.nb_nodes ;
      "last_started", string_of_option string_of_float l.last_started ;
      "last_stopped", string_of_option string_of_float l.last_stopped ]
end

module Field =
struct
  type t = { name : string ; nullable : bool ; typ : string }
  let to_string t =
    string_of_record [
      "name", string_of_string t.name ;
      "nullable", string_of_bool t.nullable ;
      "type", string_of_string t.typ ]
end

module Node =
struct
  type t =
    { layer : string ;
      name : string ;
      id : string ;
      type_of_operation : string ;
      exporting : bool ;
      operation : string ;
      input_type : Field.t list ;
      output_type : Field.t list ;

      parents : string list ;
      children : string list ;

      in_tuple_count : int ;
      out_tuple_count : int ;
      sel_tuple_count : int ;
      group_count : int option ;

      cpu_time : float ;
      ram_usage : int ;
      pid : int option ;
      signature : string option }
  let to_string n =
    string_of_record [
      "layer", string_of_string n.layer ;
      "name", string_of_string n.name ;
      "type_of_operation", string_of_string n.type_of_operation ;
      "exporting", string_of_bool n.exporting ;
      "operation", string_of_string n.operation ;
      "input_type", string_of_list Field.to_string n.input_type ;
      "output_type", string_of_list Field.to_string n.output_type ;
      "parents", string_of_list string_of_string n.parents ;
      "children", string_of_list string_of_string n.children ;
      "in_tuple_count", string_of_int n.in_tuple_count ;
      "out_tuple_count", string_of_int n.out_tuple_count ;
      "sel_tuple_count", string_of_int n.sel_tuple_count ;
      "group_count", string_of_option string_of_int n.group_count ;
      "cpu_time", string_of_float n.cpu_time ;
      "ram_usage", string_of_int n.ram_usage ;
      "pid", string_of_option string_of_int n.pid ;
      "signature", string_of_option string_of_string n.signature ]
end

(* Each layer and node is its own state variable.
 * But the layers hash has to be a state variable as well, and we
 * want to call [with_value layers] just before calling [with_value]
 * on any individual layer in order for the rendering algorithm to
 * discover new/deleted entries because no worthy path lead to them
 * (so we instead "touch" [layers] to direct the rendering over there.
 *
 * In practical terms, we want [with_value layers] to always immediately
 * precede [with_value some_layer]. *)
(* Alternatively, for simplicity we could have a single value for the whole
 * table but then very long list of nodes would be slow. *)
let layers = { name = "layers" ; value = Hashtbl.create 5 }
let update_layer layer =
  let p =
    try Hashtbl.find layers.value layer.Layer.name
    with Not_found ->
      Firebug.console##log (Js.string ("Creating layer "^ Layer.to_string layer)) ;
      change layers ;
      { name = "layer "^ layer.name ; value = layer } in
  set p layer ;
  Hashtbl.replace layers.value layer.name p

(* Value is a hash from node id to node *)
let nodes = { name = "nodes" ; value = Hashtbl.create 5 }
(* Fake value used to track when to redraw the totals: *)
let nodes_sum = { name = "nodes sum" ; value = () }
let update_node node =
  let p =
    try Hashtbl.find nodes.value node.Node.id
    with Not_found ->
      Firebug.console##log (Js.string ("Creating node "^ Node.to_string node)) ;
      change nodes ;
      { name = "node "^ node.name ; value = node } in
  set p node ;
  Hashtbl.replace nodes.value node.id p ;
  change nodes_sum

(* We have only one variable for all the lines because they always change
 * all together when we refresh. Value is a list of fields and an array
 * of rows, made of optional strings *)
let tail_rows = { name = "tail rows" ; value = [||] }

(* Use node.id as a value *)
let sel_node = { name = "selected node" ; value = "" }

let rec set_sel_node v =
  set sel_node v ;
  reload_tail ()

and reload_tail () =
  match Hashtbl.find nodes.value sel_node.value with
  | exception Not_found -> ()
  | node ->
    let node = node.value in
    let content = "{\"max_results\":8}"
    and path = "/export/"^ enc node.layer ^"/"^ enc node.name in
    http_post path content (fun r ->
      update_tail r ;
      resync ())

and update_tail resp =
  let columns = Js.Unsafe.get resp "columns" in
  let rows = ref [||] in
  (* Returns the nulls and the values of a column *)
  let col_of_js o =
    (* keep the JS bool array: *)
    let nulls = Js.array_get o 1 |> optdef_get
    and values =
      (* This final opt_def is needed, despite we know we cannot have
       * null instead of the array of value, because for OCaml a and
       * null, coming both from [Js.array_get o], have to have the same
       * type. *)
      let a = Js.array_get o 2 |> optdef_get |> opt_get in
      (* takes the only variant *)
      let typ =
        Js.(array_get (object_keys a) 0 |> optdef_get |> to_string) in
      Js.Unsafe.get a typ (* Keep the JS array of values *)
    in
    nulls, values
  in
  let nb_cols = columns##.length in
  for ci = 0 to nb_cols - 1 do
    let nulls, vals = col_of_js Js.(array_get columns ci |> optdef_get) in
    if ci = 0 then (
      let nb_rows =
        Js.Opt.case nulls (fun () -> vals##.length)
                          (fun n -> n##.length) in
      rows := Array.(init nb_rows (fun _ -> make nb_cols None))
    ) ;
    (* vi index vals and ri rows - vi will be < ri in presence of nulls *)
    let rec loop vi ri =
      if ri < Array.length !rows then (
        match Js.Opt.to_option nulls with
          Some n when not Js.(array_get n ri |> optdef_get |> to_bool) ->
            !rows.(ri).(ci) <- None ;
            loop vi (ri+1)
        | _ ->
            !rows.(ri).(ci) <-
              Some (Js.(array_get vals vi |> optdef_get)##toString |>
                        Js.to_string |> str_of_float_str) ;
            loop (vi+1) (ri+1))
    in
    loop 0 0
  done ;
  set tail_rows !rows


(* TODO: add a health indicator (based on how old is the last report) *)
let node_columns =
  [| "layer", true, "" ; "name", true, "" ; "op", true, "" ;
     "#in", true, "tuples" ; "#selected", true, "tuples" ;
     "#out", true, "tuples" ; "#groups", true, "" ;
     "export", false, "" ;
     "CPU", true, "seconds" ; "RAM", true, "bytes" ;
     "parents", false, "" ; "children", false, "" ;
     "PID", false, "" ; "signature", false, "" |]

let sel_column = { name = "selected column" ; value = "layer" (* title in node_columns *) }

(* Tells if the GUI is in the layer edition mode where only the layer
 * panel is displayed alongside the large editor panel. *)
let editor_mode = { name = "layer edition mode" ; value = None }

let get_variant js =
  let open Js in
  let a = object_keys js in
  array_get a 0 |> optdef_get |> to_string

let type_spec_of_js r =
  list_init r##.length (fun i ->
    let t = Js.array_get r i in
    let name = Js.(Unsafe.get t "name_info" |> to_string)
    and nullable = Js.(Unsafe.get t "nullable_info" |> to_bool)
    and typ = Js.(Unsafe.get t "typ_info" |> get_variant) in
    let typ =
      if String.length typ > 0 &&
         typ.[0] = 'T' then
        String.sub typ 1 (String.length typ-1)
      else typ in
    Field.{ name ; nullable ; typ })

let node_list_of_js r =
  list_init r##.length (fun i ->
    Js.array_get r i |> optdef_get |> Js.to_string)

let update_graph total g =
  (* g is a JS array of layers *)
  Firebug.console##log g##.length ;
  (* Keep track of the layers we had to clean the extra ones at the end: *)
  let had_layers = ref [] in
  for i = 0 to g##.length - 1 do
    let l = Js.array_get g i in
    let name = Js.(Unsafe.get l "name" |> to_string) in
    let status_str = Js.(Unsafe.get l "status" |> get_variant) in
    let status = Layer.status_of_string status_str in
    had_layers := name :: !had_layers ;
    let nodes = Js.Unsafe.get l "nodes" in
    let layer = Layer.{
      name ; status_str ; status ;
      last_started = Js.(Unsafe.get l "last_started" |> Opt.to_option |>
                         option_map float_of_number) ;
      last_stopped = Js.(Unsafe.get l "last_started" |> Opt.to_option |>
                         option_map float_of_number) ;
      nb_nodes = nodes##.length } in
    update_layer layer ;
    for j = 0 to nodes##.length - 1 do
      let n = Js.array_get nodes j in
      let definition = Js.Unsafe.get n "definition" in
      let name = Js.(Unsafe.get definition "name" |> to_string) in
      let node = Node.{
        layer = layer.Layer.name ;
        name ;
        id = layer.Layer.name ^"/"^ name ;
        type_of_operation = Js.(Unsafe.get n "type_of_operation" |>
                                to_string) ;
        exporting = Js.(Unsafe.get n "exporting" |> to_bool) ;
        operation = Js.(Unsafe.get definition "operation" |> to_string) ;
        input_type = type_spec_of_js Js.(Unsafe.get n "input_type") ;
        output_type = type_spec_of_js Js.(Unsafe.get n "output_type") ;
        parents = node_list_of_js Js.(Unsafe.get n "parents") ;
        children = node_list_of_js Js.(Unsafe.get n "children") ;
        in_tuple_count = Js.(Unsafe.get n "in_tuple_count" |> to_int) ;
        out_tuple_count = Js.(Unsafe.get n "out_tuple_count" |> to_int) ;
        sel_tuple_count = Js.(Unsafe.get n "selected_tuple_count" |>
                              to_int) ; 
        group_count = Js.(Unsafe.get n "group_count" |> Opt.to_option |>
                          option_map to_int) ; 
        cpu_time = Js.(Unsafe.get n "cpu_time" |> float_of_number) ; 
        ram_usage = Js.(Unsafe.get n "ram_usage" |> to_int) ; 
        pid = Js.(Unsafe.get n "pid" |> Opt.to_option |>
                  option_map to_int) ; 
        signature = Js.(Unsafe.get n "signature" |> Opt.to_option |>
                        option_map to_string) } in
      update_node node
    done
  done ;
  if total then
    Hashtbl.filter_map_inplace (fun name layer ->
      if List.mem name !had_layers then (
        change layers ;
        Some layer
      ) else (
        Firebug.console##log(Js.string ("Deleting layer "^ name)) ;
        None
      )) layers.value

let reload_graph () =
  http_get "/graph" (fun g ->
    update_graph true g ;
    resync ())

(* DOM *)

let header_panel () =
  [ p
    [ text "Ramen v0.1 running on " ;
      elmt "em" [ text "$HOSTNAME$." ] ] ]

let labeled_value l v =
  p [
    span [
      clss "label" ;
      text (l ^ ":") ] ;
    span [
      clss "value" ;
      text v ] ]

let date_of_ts = function
  | Some ts ->
    let d = new%js Js.date_fromTimeValue (1000. *. ts) in
    Js.to_string d##toLocaleString
  | None -> "never"

let with_node node_id f =
  with_value nodes (fun h ->
    match Hashtbl.find h node_id with
    | exception Not_found -> text ("Can't find node "^ node_id)
    | node -> f node.value)

let icon_of_layer layer =
  let icon, path, alt =
    match layer.Layer.status with
    | Edition -> "✎", "/compile/"^ enc layer.Layer.name, "compile"
    | Compiling -> "☐", "", "reload"
    | Compiled -> "☑", "/start/"^ enc layer.Layer.name, "start"
    | Running -> "⚙", "/stop/"^ enc layer.Layer.name, "stop"
  in
  button ~action:(fun _ ->
      http_get path (fun status ->
        if Js.(Unsafe.get status "success" |> to_bool) then
        http_get ("/graph/" ^ enc layer.Layer.name) (fun g ->
          update_graph false g ;
          resync ()))) [
    clss "icon" ;
    attr "title" alt ;
    text icon ]

let layer_panel layer =
  (* TODO: We will frequently change the order of those blocks but the 
   * rendering algo assumes that nodes in same position as before are the
   * same. Since we have both curr_dom and next_dom we could use some
   * identifier in the dyn_node_tree (a mere seqnum would do) to help
   * identify if the desired new child could be found elsewhere (not
   * necessarily in this node, but we could keep a cache of seqnum to
   * dom_tree (to which we want to add a reference to the actual DOM
   * object so we could revive it faster). *)
  let e = [
    p [
      clss "name" ;
      text layer.Layer.name ;
      (if layer.status <> Running then
        button ~action:(fun _ -> set editor_mode (Some layer))
          [ text "edit" ]
      else group []) ;
      icon_of_layer layer ] ;
    div [
      clss "info" ;
      labeled_value "#nodes" (string_of_int layer.nb_nodes) ;
      labeled_value "started" (date_of_ts layer.last_started) ;
      labeled_value "stopped" (date_of_ts layer.last_stopped) ] ]
  in
  with_value sel_node (fun sel ->
    if sel = "" then div e else
    with_node sel (fun node ->
      let e =
        if node.Node.layer = layer.Layer.name then clss "selected" :: e
        else e in
      div e))

let layers_panel () =
  with_value layers (fun h ->
    Hashtbl.fold (fun _ p lst ->
      with_value p layer_panel :: lst) h [] |>
    List.rev |>
    div)

let pretty_th ?action c title subtitle =
  elmt ?action "th" (
    clss c ::
    p [ text title ] ::
    if subtitle = "" then [] else
      [ p [ clss "type" ; text subtitle ] ])

let node_thead_col (title, sortable, subtitle) =
  with_value sel_column (fun col ->
    let c = if col = title then "ordered" else "" in
    let action =
      if sortable && col <> title then Some (fun _ ->
        set sel_column title)
      else None in
    pretty_th ?action c title subtitle)

let tds v = td [ text v ]
let tdo = function None -> tds "n.a." | Some v -> tds v
let tdi v = td [ clss "number" ; text (string_of_int v) ]
let tdf v = td [ clss "number" ; text (str_of_float v) ]

let short_node_list ?(max_len=20) layer lst =
  let pref = layer ^"/" in
  let len = String.length in
  abbrev max_len (List.fold_left (fun s n ->
     if len s > max_len then s else
     s ^ (if s <> "" then ", " else "")
       ^ (if string_starts_with pref n then
            String.sub n (len pref) (len n - len pref)
          else n)
    ) "" lst)

let node_tbody_row (_tot_nodes, tot_ins, tot_sels, tot_outs,
                    tot_grps, tot_cpu, tot_ram) node =
  let tdh w xs =
    td [ clss "number" ; text xs ;
         elmt "hr" [ attr "width" (string_of_float w) ] ] in
  let tdih tot x =
    if tot = 0 then tdi x else
    let w = float_of_int (100 * x) /. float_of_int tot in
    tdh w (string_of_int x)
  and tdfh tot x =
    if tot = 0. then tdf x else
    let w = 100. *. x /. tot in
    tdh w (str_of_float x) in
  let tdoi = function None -> tds "n.a." | Some v -> tdi v
  and tdoih tot = function None -> tds "n.a." | Some v -> tdih tot v
  in
  let cols =
    [ tds node.Node.layer ;
      tds node.name ;
      tds node.type_of_operation ;
      tdih tot_ins node.in_tuple_count ;
      tdih tot_sels node.sel_tuple_count ;
      tdih tot_outs node.out_tuple_count ;
      tdoih tot_grps node.group_count ;
      tds (if node.exporting then "✓" else " ") ;
      tdfh tot_cpu node.cpu_time ;
      tdih tot_ram node.ram_usage ;
      tds (short_node_list node.layer node.parents) ;
      tds (short_node_list node.layer node.children) ;
      tdoi node.pid ;
      tdo node.signature ] in
  (* FIXME: So all the lines vary every time sel_node changes. Ie we are going to
   * redraw the whole table, while in theory only two lines must be redrawn.
   * Instead, we could have one individual boolean state variable per line and this would
   * depend only on this. *)
  with_value sel_node (fun sel ->
    if sel = node.Node.id then
      elmt ~action:(fun _ -> set_sel_node "") "tr" (clss "selected" :: cols)
    else
      elmt ~action:(fun _ -> set_sel_node node.id) "tr" cols)

let node_sorter col n1 n2 =
  (* Numbers are sorted greater to smaller while strings are sorted
   * in ascending order: *)
  let n1 = n1.value and n2 = n2.value in
  let open Node in
  match col with
  | "op" -> compare n1.type_of_operation n2.type_of_operation
  | "#in" -> compare n2.in_tuple_count n1.in_tuple_count
  | "#selected" -> compare n2.sel_tuple_count n1.sel_tuple_count
  | "#out" -> compare n2.out_tuple_count n1.out_tuple_count
  | "#groups" ->
    (match n2.group_count, n1.group_count with
    | None, None -> 0
    | Some _, None -> 1
    | None, Some _ -> -1
    | Some i2, Some i1 -> compare i2 i1)
  | "CPU" -> compare n2.cpu_time n1.cpu_time
  | "RAM" -> compare n2.ram_usage n1.ram_usage
  | _ ->
    match compare n1.layer n2.layer with
    | 0 -> compare n1.name n2.name
    | x -> x

let nodes_panel () =
  (* If a single line changes, then the whole table change (due to totals
   * and histograms. So let's grab all values at once: *)
  (* Start by computing the tots so we can display histograms in the table. *)
  let tots = ref (0, 0, 0, 0, 0, 0., 0) in
  let foot =
    with_value nodes_sum (fun () ->
      tfoot [
        with_value nodes (fun h ->
          Hashtbl.iter (fun _ p ->
              let n = p.value in
              let tot_nodes, tot_ins, tot_sels, tot_outs,
                  tot_grps, tot_cpu, tot_ram = !tots in
              tots :=
                tot_nodes + 1, tot_ins + n.Node.in_tuple_count,
                tot_sels + n.sel_tuple_count,
                tot_outs + n.out_tuple_count,
                tot_grps + (option_def 0 n.group_count),
                tot_cpu +. n.cpu_time, tot_ram + n.ram_usage
            ) h ;
          let tot_nodes, tot_ins, tot_sels, tot_outs,
              tot_grps, tot_cpu, tot_ram = !tots in
          elmt "tr" [
            tds "" ;
            tdi tot_nodes ;
            tds "" ;
            tdi tot_ins ;
            tdi tot_sels ;
            tdi tot_outs ;
            tdi tot_grps ;
            tds "" ;
            tdf tot_cpu ;
            tdi tot_ram ;
            tds "" ; tds "" ; tds "" ;
            tds "" ]) ]) in
  table [
    thead [
      Array.fold_left (fun lst col ->
        node_thead_col col :: lst) [] node_columns |>
      List.rev |> elmt "tr" ] ;
    (* Table body *)
    with_value nodes (fun h ->
      with_value sel_column (fun sel_col ->
        (* Build a list of params sorted according to sel_column: *)
        let rows =
          Hashtbl.fold (fun _ p lst -> p :: lst) h [] |>
          List.fast_sort (node_sorter sel_col) in
        List.map (fun p ->
          with_value p (node_tbody_row !tots)) rows |>
        tbody)) ;
    foot ]

let dispname_of_type nullable typ =
  String.lowercase typ ^ (if nullable then " (or null)" else "")

let field_panel f =
  labeled_value f.Field.name (dispname_of_type f.nullable f.typ)

let input_panel () =
  with_value sel_node (fun sel ->
    if sel = "" then elmt "span" []
    else with_node sel (fun node ->
      div (List.map field_panel node.input_type)))

let op_panel () =
  with_value sel_node (fun sel ->
    if sel = "" then
      p [ text "Select a node to see the operation it performs" ]
    else with_node sel (fun node ->
      elmt "pre" [ text node.operation ]))

let th_field f =
  pretty_th "" f.Field.name (dispname_of_type f.nullable f.typ)

let tail_panel () =
  let row fs r =
    let rec loop tds ci = function
      [] -> tr (List.rev tds)
    | field::fs ->
      let tds =
        td [ clss field.Field.typ ;
             match r.(ci) with
               None -> span [ clss "null" ; text "NULL" ]
             | Some v -> text v ] :: tds in
      loop tds (ci + 1) fs in
    loop [] 0 fs
  in
  with_value sel_node (fun sel ->
    if sel = "" then
      p [ text "Select a node to see its output" ]
    else with_node sel (fun node ->
      let lame_excuse t =
        tbody [ tr [ td
          [ attri "colspan" (List.length node.output_type) ;
            p [ text t ] ] ] ] in
      table
        [ thead [ tr (List.map th_field node.output_type) ] ;
          (if not node.exporting then
            lame_excuse ("node "^ node.id ^" does not export data")
          else
            with_value layers (fun h ->
              match Hashtbl.find h node.layer with
              | exception Not_found ->
                text ("Cannot find layer "^ node.layer)
              | layer_p ->
                with_value layer_p (fun layer ->
                  if layer.status <> Running then
                    lame_excuse ("Layer "^ layer.Layer.name ^" is not running")
                  else
                    with_value tail_rows (fun rows ->
                      Array.fold_left (fun l r -> row node.output_type r :: l) [] rows |>
                      List.rev |> tbody))))]))

let editor_panel () =
  div
    [ p [ text "TODO" ] ;
      button ~action:(fun _ -> set editor_mode None)
        [ text "Cancel" ] ]

let h1 t = elmt "h1" [ text t ]

let next_dom () =
  [ div (id "global" :: header_panel ()) ;
    with_value editor_mode (function
      Some layer ->
        div [ id "editor" ;
              h1 ("Edition of "^ layer.Layer.name) ;
              editor_panel () ]
    | None ->
      group
        [ div
          [ id "top" ;
            div [ id "layers" ; h1 "Layers" ; layers_panel () ] ;
            div [ id "nodes" ; h1 "Nodes" ; nodes_panel () ] ] ;
          div
            [ id "details" ;
              div [ id "input" ; h1 "Input" ; input_panel () ] ;
              div [ id "operation" ; h1 "Operation" ; op_panel () ] ] ;
          div [ id "tail" ; h1 "Output" ; tail_panel () ] ]) ]

let () =
  let every_10s () =
    Firebug.console##log (Js.string "Reloading...") ;
    reload_graph () ;
    reload_tail () in
  ignore (Dom_html.window##setInterval (Js.wrap_callback every_10s) 10_000.) ;
  start next_dom ;
  reload_graph () ;
  reload_tail ()
