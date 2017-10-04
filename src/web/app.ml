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
  match String.index s 'e' with
  | exception Not_found ->
    (match String.index s '.' with
    | exception Not_found ->
      s ^ string_times (dec_num + 1) " "
    | i ->
      (* FIXME: round instead of truncate *)
      str_of_float_str' (String.sub (s ^ "00000") 0 (i + dec_num + 1)))
  | _ -> s

let str_of_float f =
  let s = string_of_float f in
  str_of_float_str s

(* The types we will use to deserialize JSON. Cannot be the same as
 * RamenSharedTypes because this JSON unparser is not compatible
 * with PPP (not because of some deficiencies in those implementations
 * but because it is ambiguous how to map rich ML types into poor JSON
 * types - if only for the various integer types but also for the
 * algebraic types). Therefore we have to unserialize by hand. *)

(* State variables *)

open RamenSharedTypesJS_noPPP

module Layer =
struct
  let status_of_string = function
    "Edition" -> Edition
  | "Compiling" -> Compiling
  | "Compiled" -> Compiled
  | "Running" -> Running
  | _ -> fail ()

  type t =
    { name : string ;
      status : layer_status ;
      status_str : string ;
      nb_nodes : int ;
      last_started : float option ;
      last_stopped : float option }

  let make () =
    { name = "unnamed layer" ; nb_nodes = 0 ;
      status = Edition ; status_str = "Edition" ;
      last_started = None ; last_stopped = None }

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
      in_sleep : float ;
      out_sleep : float ;
      in_bytes : int ;
      out_bytes : int ;
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
      "in_sleep", string_of_float n.in_sleep ;
      "out_sleep", string_of_float n.out_sleep ;
      "in_bytes", string_of_int n.in_bytes ;
      "out_bytes", string_of_int n.out_bytes ;
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
(* Value is an association list from layer name to layer *)
let layers = { desc = { name = "layers" ; last_changed = clock () } ;
               value = [] }
let update_layer layer =
  let p =
    try List.assoc layer.Layer.name layers.value
    with Not_found ->
      print (Js.string ("Creating layer "^ Layer.to_string layer)) ;
      change layers ;
      { desc = { name = "layer "^ layer.name ; last_changed = clock () } ;
        value = layer } in
  set p layer ;
  layers.value <- replace_assoc layer.name p layers.value

(* Value is an association list from node id to node.
 * Note: in theory node names are optional, and would be supplied by the
 * server if missing, but we do not make use of this behavior here. *)
let nodes = { desc = { name = "nodes" ; last_changed = clock () } ;
              value = [] }

(* We use floats for every counter since JS integers are only 32bits *)
let zero_sums = 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.
let nodes_sum = { desc = { name = "nodes sum" ; last_changed = clock () } ;
                  value = zero_sums }

let update_node node =
  let p =
    try List.assoc node.Node.id nodes.value
    with Not_found ->
      print (Js.string ("Creating node "^ Node.to_string node)) ;
      change nodes ;
      { desc = { name = "node "^ node.name ; last_changed = clock () } ;
        value = node } in
  set p node ;
  nodes.value <- replace_assoc node.id p nodes.value

(* Uses node.id as a value *)
let sel_node =
  { desc = { name = "selected node" ;last_changed = clock () } ;
    value = "" }

(* We have only one variable for all the lines because they always change
 * all together when we refresh. Value is a list of fields and an array
 * of rows, made of optional strings *)
let tail_rows =
  { desc = { name = "tail rows" ; last_changed = clock () } ;
    value = [||] }

let update_tail resp =
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

let reload_tail () =
  match List.assoc sel_node.value nodes.value with
  | exception Not_found -> ()
  | node ->
    let node = node.value in
    let content = "{\"max_results\":-8}"
    and path = "/export/"^ enc node.layer ^"/"^ enc node.name in
    http_post path content (fun r ->
      update_tail r ;
      resync ())

let chart_points =
  { desc = { name = "chart points" ; last_changed = clock () } ;
    value = [||] (* An array of time * value *) }

let sel_output_col =
  { desc = { name = "selected output column" ; last_changed = clock () } ;
    value = None (* number of column *) }

let update_chart resp =
  (* As we asked for only one timeseries, consider only the first result: *)
  let resp = Js.(array_get resp 0 |> optdef_get) in
  let times = Js.Unsafe.get resp "times"
  and values = Js.Unsafe.get resp "values" in
  let nb_points = times##.length in
  let points = Array.init nb_points (fun i ->
    let t = Js.(array_get times i |> optdef_get |> float_of_number)
    and v = Js.(array_get values i |> Optdef.to_option |>
            option_map float_of_number) in
    t, v) in
  set chart_points points

let reload_chart () =
  match List.assoc sel_node.value nodes.value,
        sel_output_col.value with
  | exception Not_found -> ()
  | _, None -> ()
  | node, Some col ->
    let node = node.value in
    let field_name = (List.nth node.output_type col).Field.name in
    (* Last 2 hours for now *)
    let now = (new%js Js.date_now)##valueOf in
    let content = string_of_record
      [ "from", string_of_float (now -. 3600. *. 2.) ;
        "to", string_of_float now ;
        "max_data_points", "800" ;
        "timeseries", string_of_list string_of_record
          [ [ "id",  string_of_string "test" ;
              "consolidation", string_of_string "avg" ;
              "spec", string_of_record
                [ "Predefined", string_of_record
                  [ "node", string_of_string node.id ;
                    "data_field", string_of_string
                      field_name ] ] ] ] ]
    and path = "/timeseries" in
    http_post path content (fun r ->
      update_chart r ;
      resync ())

let set_sel_node id =
  set sel_node id ;
  reload_tail ()

(* TODO: add a health indicator (based on how old is the last report) *)
let node_columns =
  [| "layer", true, "" ; "name", true, "" ; "op", true, "" ;
     "#in", true, "tuples" ; "#selected", true, "tuples" ;
     "#out", true, "tuples" ; "#groups", true, "" ;
     "export", true, "" ;
     "CPU", true, "seconds" ;
     "wait in", true, "seconds" ;
     "wait out", true, "seconds" ;
     "RAM", true, "bytes" ;
     "volume in", true, "bytes" ;
     "volume out", true, "bytes" ;
     "parents", false, "" ; "children", false, "" ;
     "PID", false, "" ; "signature", false, "" |]

let sel_column =
  { desc = { name = "selected column" ; last_changed = clock () } ;
    value = "layer" (* title in node_columns *) }

(* Tells if the GUI is in the layer edition mode where only the layer
 * panel is displayed alongside the large editor panel. *)
type edited_layer =
  { layer_name : string ;
    new_layer_name : string ref ;
    mutable edited_nodes : (string ref * string ref) list }

let the_new_layer = { desc = { name = "the new layer" ; last_changed = clock () } ;
                      value = Layer.make () }

let editor_mode =
  { desc = { name = "layer edition mode" ; last_changed = clock () } ;
             value = false }

let new_edited_node edited_nodes =
  let rec loop i =
    let name = "new node "^ string_of_int i in
    if List.exists (fun (n, _) -> !n = name) edited_nodes then
      loop (i + 1)
    else name
  in
  let name = loop 1 in
  ref name, ref ""

let edited_nodes_of_layer l =
  let edited_nodes =
    List.fold_left (fun ns (_, n) ->
        let n = n.value in
        if n.Node.layer <> l.Layer.name then ns
        else (ref n.Node.name, ref n.operation) :: ns
      ) [] nodes.value in
  edited_nodes @ [ new_edited_node edited_nodes ]

let edited_layer_of_layer l =
  { layer_name = l.Layer.name ;
    new_layer_name = ref l.Layer.name ;
    edited_nodes = edited_nodes_of_layer l }

let edited_layer =
  { desc = { name = "edited nodes" ; last_changed = clock () } ;
    value = edited_layer_of_layer the_new_layer.value }

let set_editor_mode = function
  | None ->
    set editor_mode false
  | Some l ->
    set editor_mode true ;
    if l.Layer.name <> edited_layer.value.layer_name then
      set edited_layer (edited_layer_of_layer l)

let add_edited_node () =
  let edl = edited_layer.value in
  edl.edited_nodes <-
    edl.edited_nodes @ [ new_edited_node edl.edited_nodes ] ;
  change edited_layer

let raw_output_mode =
  { desc = { name = "output mode" ; last_changed = clock () } ;
    value = true }

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

(* Recompute the sums from the nodes *)
let update_nodes_sum () =
  let sum =
    List.fold_left (fun (tot_nodes, tot_ins, tot_sels, tot_outs,
                         tot_grps, tot_cpu, tot_ram, tot_in_sleep,
                         tot_out_sleep, tot_in_bytes, tot_out_bytes)
                        (_, n) ->
        let n = n.value in
        tot_nodes +. 1., tot_ins +. float_of_int n.Node.in_tuple_count,
        tot_sels +. float_of_int n.sel_tuple_count,
        tot_outs +. float_of_int n.out_tuple_count,
        tot_grps +. float_of_int (option_def 0 n.group_count),
        tot_cpu +. n.cpu_time,
        tot_ram +. float_of_int n.ram_usage,
        tot_in_sleep +. n.in_sleep,
        tot_out_sleep +. n.out_sleep,
        tot_in_bytes +. float_of_int n.in_bytes,
        tot_out_bytes +. float_of_int n.out_bytes
      ) zero_sums nodes.value in
  set nodes_sum sum

let update_graph total g =
  (* g is a JS array of layers *)
  (* Keep track of the layers we had to clean the extra ones at the end: *)
  let had_layers = ref [] in
  let had_nodes = ref [] in
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
      let id = layer.Layer.name ^"/"^ name in
      had_nodes := id :: !had_nodes ;
      let node = Node.{
        layer = layer.Layer.name ;
        name ; id ;
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
        in_sleep = Js.(Unsafe.get n "in_sleep" |> float_of_number) ;
        out_sleep = Js.(Unsafe.get n "out_sleep" |> float_of_number) ;
        in_bytes = Js.(Unsafe.get n "in_bytes" |> to_int) ;
        out_bytes = Js.(Unsafe.get n "out_bytes" |> to_int) ;
        pid = Js.(Unsafe.get n "pid" |> Opt.to_option |>
                  option_map to_int) ; 
        signature = Js.(Unsafe.get n "signature" |> Opt.to_option |>
                        option_map to_string) } in
      update_node node
    done
  done ;
  update_nodes_sum () ;
  if total then (
    layers.value <- List.filter (fun (name, _) ->
      if List.mem name !had_layers then (
        change layers ; true
      ) else (
        print (Js.string ("Deleting layer "^ name)) ;
        false
      )) layers.value ;
    nodes.value <- List.filter (fun (id, _) ->
      if List.mem id !had_nodes then (
        change nodes ; true
      ) else (
        Firebug.console##log (Js.string ("Deleting node "^ id)) ;
        false
      )) nodes.value
  ) else (
    (* Still, this is total for the nodes of these layers. But so far
     * when we ask for a partial graph we do not modify the composition
     * of those layers (but their status). The background periodic reload
     * of the graph will be good enough to fetch the modifications of
     * the graph that are performed independently of this app. So no
     * worries. *)
  )

let reload_graph () =
  http_get "/graph" (fun g ->
    update_graph true g ;
    resync ())

(* DOM *)

let spacer = p [ clss "spacer" ]

let autoreload =
  { desc = { name = "autoreload" ; last_changed = clock () } ;
    value = true }

let header_panel =
  [ p
    [ text "Ramen v0.1 running on " ;
      elmt "em" [ text "$HOSTNAME$." ] ] ;
    spacer ;
    with_value autoreload (fun ar ->
      button ~action:(fun _ ->
          set autoreload (not autoreload.value))
        [ clss ("icon actionable" ^ if ar then " selected" else "") ;
          attr "title" ((if ar then "dis" else "en")^ "able auto-reload") ;
          text "⟳" ]) ]

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
  with_value nodes (fun nodes ->
    match List.assoc node_id nodes with
    | exception Not_found -> text ("Can't find node "^ node_id)
    | node -> f node.value)

let icon_of_layer layer =
  let icon, path, alt =
    match layer.Layer.status with
    | Edition -> "✎", "/compile/"^ enc layer.Layer.name, "compile"
    | Compiling -> "☐", "/graph/"^ enc layer.Layer.name, "reload"
    | Compiled -> "☑", "/start/"^ enc layer.Layer.name, "start"
    | Running -> "⚙", "/stop/"^ enc layer.Layer.name, "stop"
  in
  button ~action:(fun _ ->
      http_get path (fun status ->
        if Js.(Unsafe.get status "success" |> to_bool) then
        http_get ("/graph/" ^ enc layer.Layer.name) (fun g ->
          update_graph false g ;
          resync ()))) [
    clss "icon actionable" ;
    attr "title" alt ;
    text icon ]

let layer_panel layer =
  let e = [
    p [
      clss "name" ;
      text layer.Layer.name ;
      (if layer.status <> Running then
        button ~action:(fun _ -> set_editor_mode (Some layer))
          [ clss "actionable" ; text "edit" ]
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

let layers_panel =
  div [
    with_value layers (fun layers ->
      List.fold_left (fun lst (_, p) ->
        with_value p layer_panel :: lst) [] layers |>
      List.rev |>
      group) ;
    button ~action:(fun _ -> set_editor_mode (Some the_new_layer.value))
      [ clss "actionable" ; text "new" ] ]

let pretty_th ?action c title subtitle =
  th ?action (
    (if c <> "" then (List.cons (clss c)) else identity)
      (p [ text title ] ::
       if subtitle = "" then [] else
         [ p [ clss "type" ; text subtitle ] ]))

let node_thead_col (title, sortable, subtitle) =
  with_value sel_column (fun col ->
    let action, c =
      if sortable && col <> title then
        Some (fun _ -> set sel_column title), "actionable"
      else None, if col = title then "selected" else "" in
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

let node_tbody_row node =
  let tdh w xs =
    td [ clss "number" ; text xs ;
         elmt "hr" [ attr "width" (string_of_float w) ] ] in
  let tdih tot x =
    if tot = 0. then tdi x else
    let w = 100. *. float_of_int x /. tot in
    tdh w (string_of_int x)
  and tdfh tot x =
    if tot = 0. then tdf x else
    let w = 100. *. x /. tot in
    tdh w (str_of_float x) in
  let tdoi = function None -> tds "n.a." | Some v -> tdi v
  and tdoih tot = function None -> tds "n.a." | Some v -> tdih tot v
  in
  with_value nodes_sum (fun (_tot_nodes, tot_ins, tot_sels, tot_outs,
                             tot_grps, tot_cpu, tot_ram, tot_in_sleep,
                             tot_out_sleep, tot_in_bytes, tot_out_bytes) ->
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
        tdfh tot_in_sleep node.in_sleep ;
        tdfh tot_out_sleep node.out_sleep ;
        tdih tot_ram node.ram_usage ;
        tdih tot_in_bytes node.in_bytes ;
        tdih tot_out_bytes node.out_bytes ;
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
        tr ~action:(fun _ -> set_sel_node "")
          (clss "selected actionable" :: cols)
      else
        tr ~action:(fun _ -> set_sel_node node.id)
          (clss "actionable" :: cols)))

let node_sorter col =
  (* Numbers are sorted greater to smaller while strings are sorted
   * in ascending order: *)
  let make f (_, a) (_, b) =
    let a = a.value and b = b.value in
    f a b in
  let open Node in
  match col with
  | "op" ->
    make (fun a b -> compare a.type_of_operation b.type_of_operation)
  | "#in" ->
    make (fun a b -> compare b.in_tuple_count a.in_tuple_count)
  | "#selected" ->
    make (fun a b -> compare b.sel_tuple_count a.sel_tuple_count)
  | "#out" ->
    make (fun a b -> compare b.out_tuple_count a.out_tuple_count)
  | "#groups" ->
    make (fun a b -> match b.group_count, a.group_count with
         | None, None -> 0
         | Some _, None -> 1
         | None, Some _ -> -1
         | Some i2, Some i1 -> compare i2 i1)
  | "export" -> make (fun a b -> compare b.exporting a.exporting)
  | "CPU" -> make (fun a b -> compare b.cpu_time a.cpu_time)
  | "wait in" -> make (fun a b -> compare b.in_sleep a.in_sleep)
  | "wait out" -> make (fun a b -> compare b.out_sleep a.out_sleep)
  | "bytes in" -> make (fun a b -> compare b.in_bytes a.in_bytes)
  | "bytes out" -> make (fun a b -> compare b.out_bytes a.out_bytes)
  | "RAM" -> make (fun a b -> compare b.ram_usage a.ram_usage)
  | _ ->
    make (fun a b -> match compare a.layer b.layer with
         | 0 -> compare a.name b.name
         | x -> x)

let nodes_panel =
  table [
    thead [
      Array.fold_left (fun lst col ->
        node_thead_col col :: lst) [] node_columns |>
      List.rev |> tr ] ;
    (* Table body *)
    with_value nodes (fun nodes ->
      with_value sel_column (fun sel_col ->
        (* Build a list of params sorted according to sel_column: *)
        let rows =
          List.fold_left (fun lst p -> p :: lst) [] nodes |>
          List.fast_sort (node_sorter sel_col) in
        List.map (fun (_, p) ->
          with_value p node_tbody_row) rows |>
        tbody)) ;
    with_value nodes_sum (fun (tot_nodes, tot_ins, tot_sels, tot_outs,
                               tot_grps, tot_cpu, tot_ram, tot_in_sleep,
                               tot_out_sleep, tot_in_bytes, tot_out_bytes) ->
      tfoot [
        tr [
          tds "" ; tdf tot_nodes ; tds "" ; tdf tot_ins ;
          tdf tot_sels ; tdf tot_outs ; tdf tot_grps ;
          tds "" ; tdf tot_cpu ; tdf tot_in_sleep ; tdf tot_out_sleep ;
          tdf tot_ram ; tdf tot_in_bytes ; tdf tot_out_bytes ;
          tds "" ; tds "" ; tds "" ; tds "" ] ]) ]

let dispname_of_type nullable typ =
  String.lowercase typ ^ (if nullable then " (or null)" else "")

let field_panel f =
  labeled_value f.Field.name (dispname_of_type f.nullable f.typ)

let input_panel =
  with_value sel_node (fun sel ->
    if sel = "" then elmt "span" []
    else with_node sel (fun node ->
      div (List.map field_panel node.input_type)))

let op_panel =
  with_value sel_node (fun sel ->
    if sel = "" then
      p [ text "Select a node to see the operation it performs" ]
    else with_node sel (fun node ->
      elmt "pre" [ text node.operation ]))

let tail_panel =
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
  and th_field ci f =
    with_value sel_output_col (fun col ->
      let action, c =
        (* TODO: only if the type is a number *)
        if col <> Some ci then
          Some (fun _ -> set sel_output_col (Some ci)), "actionable"
        else None, "selected" in
      pretty_th ?action c f.Field.name
        (dispname_of_type f.nullable f.typ))
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
        [ thead [ tr (List.mapi th_field node.output_type) ] ;
          (if not node.exporting then
            lame_excuse ("node "^ node.id ^" does not export data")
          else
            with_value tail_rows (fun rows ->
              Array.fold_left (fun l r -> row node.output_type r :: l) [] rows |>
              List.rev |> tbody))]))

let timechart_panel =
  with_value sel_output_col (function
    | None ->
      p [ text "Select a column in the raw output panel to plot it." ]
    | Some _col ->
      with_value chart_points (fun pts ->
        let nb_pts = Array.length pts in
        if nb_pts = 0 then
          p [ text "No data" ]
        else
          let vx_start = fst pts.(0) and vx_stop = fst pts.(nb_pts-1) in
          let vx_step =
            if nb_pts < 2 then 0.
            else (vx_stop -. vx_start) /. (float_of_int (nb_pts - 1)) in
          let pen =
            Chart.{ label = "value" ; draw_line = true ; draw_points = true ;
                    color = "#55e" ; stroke_width = 1.5 ; opacity = 1. ;
                    dasharray = None ; filled = true ; fill_opacity = 0.5 } in
          let fold = { Chart.fold = fun f init ->
            (* As for now we have only one dataset the fold over datasets
             * is trivial: *)
            f init pen true (fun i -> snd pts.(i) |? 0.) } in
          let svg_width = 800. and svg_height = 400. in
          let attrs =
            [ clss "chart" ;
              attr "style" ("width:"^ string_of_float svg_width ^
                            "; height:"^ string_of_float svg_height ^
                            "; min-height:"^ string_of_float svg_height ^";") ] in
          Chart.xy_plot ~attrs ~svg_width ~svg_height
              ~string_of_x:short_string_of_float
              "time" "value"
              vx_start vx_step nb_pts fold))

let form_input label value =
  elmt "label"
    [ text label ;
      input ~action:(fun v -> value := v ; change edited_layer)
        [ attr "type" "text" ;
          attr "value" !value ] ]

let get_text_size s =
  let rec loop_line nb_l longest from =
    match String.index_from s from '\n' with
    | exception Not_found ->
      max longest (String.length s - from), nb_l + 1
    | eol ->
      loop_line (nb_l + 1) (max longest (eol - from)) (eol + 1)
  in
  loop_line 0 0 0

let form_input_large label value =
  let cols, rows = get_text_size !value in
  let cols = max 20 (cols + 5)
  and rows = max 5 (rows + 4) in
  elmt "label"
    [ text label ;
      br ;
      textarea ~action:(fun v -> value := v ; change edited_layer)
        [ attr "rows" (string_of_int rows) ;
          attr "cols" (string_of_int cols) ;
          attr "spellcheck" "false" ;
          text !value ] ]

let node_editor_panel (name, operation) =
  div
    [ clss "node-edition" ;
      div [ form_input "Name" name ] ;
      div [ form_input_large "Operation" operation ] ]

let done_edit_cb what status =
  if Js.(Unsafe.get status "success" |> to_bool) then (
    Firebug.console##log (Js.string ("DONE "^ what)) ;
    set_editor_mode None ;
    reload_graph ()
  ) else (
    Firebug.console##error_2 (Js.string ("Cannot "^ what ^" layer")) status
  )

let save_layer _ =
  let string_of_node (name, operation) =
    string_of_record [ "name", string_of_string !name ;
                       "operation", string_of_string !operation ]
  and edl = edited_layer.value in
  let nodes =
    List.filter (fun (name, operation) ->
      !name <> "" && !operation <> "") edl.edited_nodes in
  let content =
    string_of_record
      [ "name", string_of_string !(edl.new_layer_name) ;
        "nodes", string_of_list string_of_node nodes ]
  and path = "/graph" in
  http_put path content (done_edit_cb "save")

let del_layer layer_name =
  let path = "/graph/"^ enc layer_name in
  http_del path (done_edit_cb "delete")

let layer_editor_panel =
  with_value edited_layer (fun edl ->
    div
      [ h2 "Layer" ;
        form_input "Name" edl.new_layer_name ;
        h2 "Nodes" ;
        group (List.map node_editor_panel edl.edited_nodes) ;
        br ;
        button ~action:(fun _ -> add_edited_node ())
          [ clss "actionable" ; text "+" ] ;
        button ~action:(fun _ -> set_editor_mode None)
          [ clss "actionable" ; text "Cancel" ] ;
        button ~action:(fun _ -> del_layer edl.layer_name)
          [ clss "actionable" ; text "Delete" ] ;
        button ~action:save_layer
          [ clss "actionable" ; text "Save" ] ])

let output_panel =
  with_value raw_output_mode (function
    true ->
    group
      [ div
        [ clss "tab" ;
          div [ clss "selected" ; text "Raw Output" ] ;
          div ~action:(fun _ ->
              set raw_output_mode false ;
              reload_chart ())
            [ clss "unselected" ; text "Time Chart" ] ] ;
        tail_panel ]
  | false ->
    group
      [ div
        [ clss "tab" ;
          div ~action:(fun _ ->
              set raw_output_mode true ;
              reload_tail ())
            [ clss "unselected" ; text "Raw Output" ] ;
          div [ clss "selected" ; text "Time Chart" ] ] ;
        timechart_panel ])

let dom =
  group
    [ div (id "global" :: header_panel) ;
      with_value editor_mode (function
        true ->
          div [ id "editor" ; layer_editor_panel ]
      | false ->
        group
          [ div
            [ id "top" ;
              div [ id "layers" ; h1 "Layers" ; layers_panel ] ;
              div [ id "nodes" ; h1 "Nodes" ; nodes_panel ] ] ;
            div
              [ id "details" ;
                div [ id "input" ; h1 "Input" ; input_panel ] ;
                div [ id "operation" ; h1 "Operation" ; op_panel ] ] ;
            div
              [ id "output" ; output_panel ] ]) ]

let () =
  let periodically () =
    if autoreload.value && not editor_mode.value then (
      reload_graph () ;
      (* Tail could benefit from a higher refresh frequency *)
      if sel_node.value <> "" then
        if raw_output_mode.value then reload_tail ()
        else reload_chart ()) in
  Dom_html.window##setInterval (Js.wrap_callback periodically) 3_500. |>
  ignore ;
  start dom ;
  reload_graph ()
