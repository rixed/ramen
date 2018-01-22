open Js_of_ocaml
module Html = Dom_html
open Engine
open WebHelpers
open JsHelpers
open RamenHtml
open Style

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

(* Main views of the application: *)

type page = PageLive | PageHistory | PageEventProcessor | PageTeam
let current_page = make_param "tab" PageLive

open RamenSharedTypesJS_noPPP

(* FIXME: actually get an ocaml js object *)
let time_range_of_js js =
  let open Js in
  let a = object_keys js in
  let variant_str = array_get a 0 |> optdef_get |> to_string in
  match variant_str with
    "NoData" -> NoData
  | "TimeRange" ->
      let a = Js.(Unsafe.get js variant_str) in
      let since = Js.(array_get a 0 |> optdef_get |> to_float)
      and until = Js.(array_get a 1 |> optdef_get |> to_float) in
      TimeRange (since, until)
  | x -> fail ("Unknown time range "^x)

module Layer =
struct
  let status_of_js js =
    let open Js in
    let a = object_keys js in
    let status_str = array_get a 0 |> optdef_get |> to_string in
    (match status_str with
      "Edition" ->
        (* In this case we have another array as the value instead of null,
         * with a lone string in it: *)
        let msg = Js.(Unsafe.get js status_str |> to_string) in
        Edition msg
    | "Compiling" -> Compiling
    | "Compiled" -> Compiled
    | "Running" -> Running
    | x -> fail ("Unknown layer status "^x)),
    status_str

  type t =
    { name : string ;
      order : int ; (* depended upon before depending on *)
      status : layer_status ;
      status_str : string ;
      nb_nodes : int ;
      last_started : float option ;
      last_stopped : float option }
end

module Field =
struct
  let type_of_string = function
  | "TNull" -> TNull | "TFloat" -> TFloat | "TString" -> TString
  | "TBool" -> TBool
  | "TU8" -> TU8 | "TU16" -> TU16 | "TU32" -> TU32
  | "TU64" -> TU64 | "TU128" -> TU128
  | "TI8" -> TI8 | "TI16" -> TI16 | "TI32" -> TI32
  | "TI64" -> TI64 | "TI128" -> TI128
  | "TEth" -> TEth | "TIpv4" -> TIpv4 | "TIpv6" -> TIpv6
  | "TCidrv4" -> TCidrv4 | "TCidrv6" -> TCidrv6
  | "TNum" -> TNum
  | x -> fail ("Unknown type "^x)

  type t =
    { name : string ; nullable : bool option ; typ : scalar_typ option ;
      typ_str : string ; typ_disp : string }
end

module Node =
struct
  type worker_stats =
    { time : float ;
      in_tuple_count : float option ;
      out_tuple_count : float option ;
      sel_tuple_count : float option ;
      group_count : float option ;
      cpu_time : float ;
      ram_usage : float ;
      in_sleep : float option ;
      out_sleep : float option ;
      in_bytes : float option ;
      out_bytes : float option }

  type t =
    { layer : string ;
      name : string ;
      id : string ;
      exporting : bool ;
      operation : string ;
      input_type : Field.t list ;
      output_type : Field.t list ;

      parents : string list ;
      children : string list ;

      mutable last_stats : worker_stats ;
      stats : worker_stats ;

      pid : int option ;
      signature : string option }

  let name_of_id id =
    let i = String.rindex id '/' in
    String.sub id (i + 1) (String.length id - i - 1)
end

(* Value is an association list from node id to node.
 * Note: in theory node names are optional, and would be supplied by the
 * server if missing, but we do not make use of this behavior here. *)
let nodes = make_param "nodes" []

(* Use layer name as a value, same reasons as for set_node. *)
type selected_layer = NoLayer | ExistingLayer of string | NewLayer
let sel_layer = make_param "selected layer" NoLayer

(* Each layer and node is its own state variable.
 * But the layers hash has to be a state variable as well, and we
 * want to call [with_param layers] just before calling [with_param]
 * on any individual layer in order for the rendering algorithm to
 * discover new/deleted entries because no worthy path lead to them
 * (so we instead "touch" [layers] to direct the rendering over there.
 *
 * In practical terms, we want [with_param layers] to always immediately
 * precede [with_param some_layer]. *)
(* Alternatively, for simplicity we could have a single value for the whole
 * table but then very long list of nodes would be slow. *)
(* Value is an association list from layer name to layer *)
let layers = make_param "layers" []

(* The edition form state, with as many additional nodes as requested,
 * and the ongoing modifications: *)
type edited_layer =
  { mutable is_new : bool ;
    (* Name of the new layer.
     * Note that once created a layer cannot be renamed. *)
    layer_name : string ref ;
    mutable edited_nodes : (string ref * string ref) list }

(* name and operation of a new node in the edition form: *)
let new_edited_node edited_nodes =
  let rec loop i =
    let name = "new node "^ string_of_int i in
    if List.exists (fun (n, _) -> !n = name) edited_nodes then
      loop (i + 1)
    else name
  in
  let name = loop 1 in
  ref name, ref ""

(* List of all names and operations of nodes in the edition form, including
 * new nodes: *)
let edited_nodes_of_layer l =
  let edited_nodes =
    List.fold_left (fun ns (_, n) ->
        if n.value.Node.layer <> l then ns
        else (ref n.value.Node.name, ref n.value.operation) :: ns
      ) [] nodes.value in
  edited_nodes @ [ new_edited_node edited_nodes ]

let edited_layer_of_layer = function
  ExistingLayer l ->
  { is_new = false ;
    layer_name = ref l ;
    edited_nodes = edited_nodes_of_layer l }
| NewLayer ->
  (* edited_layer record for a new layer: *)
  { is_new = true ;
    layer_name = ref "new layer name" ;
    edited_nodes = [ new_edited_node [] ] }
| NoLayer -> fail "invalid edited layer NoLayer"

let edited_layer =
  make_param "edited nodes" (edited_layer_of_layer NewLayer)


let update_layer layer =
  let p =
    try List.assoc layer.Layer.name layers.value
    with Not_found ->
      print (Js.string ("Creating layer "^ layer.Layer.name)) ;
      change layers ;
      make_param ("layer "^ layer.name) layer in
  set p layer ;
  layers.value <- replace_assoc layer.name p layers.value ;
  (* Also, if we were editing this layer, change its is_new status: *)
  if !(edited_layer.value.layer_name) = layer.name &&
     edited_layer.value.is_new then (
    edited_layer.value.is_new <- false ;
    change edited_layer)

(* We use floats for every counter since JS integers are only 32bits *)
let zero_sums = 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.
(* FIXME: should be in the single param with nodes info (which should be a
 * record type). Also we should compute both the sum of cumulative and
 * current values. *)
let nodes_sum = make_param "nodes sum" zero_sums

(* FIXME: have a single param for all the nodes *)
let update_node node =
  let p =
    try
      let p = List.assoc node.Node.id nodes.value in
      if node.stats.time > p.value.stats.time then
        node.last_stats <- p.value.stats
      else
        node.last_stats <- p.value.last_stats ;
      set p node ;
      p
    with Not_found ->
      print (Js.string ("Creating node "^ node.Node.name)) ;
      change nodes ;
      make_param ("node "^ node.id) node in
  nodes.value <- replace_assoc node.id p nodes.value

(* Uses node.id as a value. Avoid pointing toward a node that may be
 * superseded by the next graph reload. *)
let sel_node = make_param "selected node" ""

(* We have only one variable for all the lines because they always change
 * all together when we refresh. Value is a list of fields and an array
 * of rows, made of optional strings *)
let tail_rows = make_param "tail rows" [||]

let update_tail resp =
  let columns = Js.Unsafe.get resp "columns" in
  let rows = ref [||] in
  (* Returns the nulls and the values of a column *)
  let col_of_js o =
    (* [o] is an array which first item is the field name, the second
     * item is the null bitmask, and the third and last one is the
     * values. *)
    (* keep the JS bool array: *)
    let nulls = Js.array_get o 1 |> optdef_get
    (* This final opt_get is needed, despite we know we cannot have
     * null instead of the array of value, because for OCaml [a] and
     * [nulls], coming both from [Js.array_get o], have to have the same
     * type. *)
    and a = Js.array_get o 2 |> optdef_get |> opt_get in
    (* takes the only variant *)
    let typ =
      Js.(array_get (object_keys a) 0 |> optdef_get |> to_string) in
    let values =
      Js.Unsafe.get a typ (* Keep the JS array of values *)
    in
    typ, nulls, values
  in
  let nb_cols = columns##.length in
  for ci = 0 to nb_cols - 1 do
    let typ, nulls, vals =
      col_of_js Js.(array_get columns ci |> optdef_get) in
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
            (*!rows.(ri).(ci) <- None ; (* leave it to null *) *)
            loop vi (ri+1)
        | _ ->
            !rows.(ri).(ci) <- (
              (* Remember from PPP_JSON that nan/inf are both turned into
               * null since there is no representation for them in JSON.
               * Yay for expressive types! *)
              let v = Js.array_get vals vi |> optdef_get in
              let s = Js.Opt.case v
                        (fun () -> "nan/inf")
                        (fun v ->
                          let s = v##toString |> Js.to_string in
                          if typ = "AFloat" then str_of_float_str s
                          else s) in
              Some s) ;
            loop (vi+1) (ri+1))
    in
    loop 0 0
  done ;
  set tail_rows !rows

(* For when we use it from an action handler: *)
let can_export node =
  match List.assoc node.Node.layer layers.value with
    exception Not_found -> false
  | layer ->
    let status = layer.value.status in
    node.exporting &&
    (status = Compiled || status = Running)

(* For when we use it to draw the DOM: *)
let can_export_with_layer node cb =
  match List.assoc node.Node.layer layers.value with
    exception Not_found -> cb false
  | layer ->
    if not node.exporting then cb false
    else
      with_param layer (fun layer ->
        let status = layer.status in
        cb (status = Compiled || status = Running))

let reload_tail =
  let reloading = ref None in
  fun ?(single=false) () ->
    match List.assoc sel_node.value nodes.value with
    | exception Not_found -> ()
    | node_p ->
      let node = node_p.value in
      if can_export node then
        let content = object%js val max_results_ = -8 end
        and path = "/export/"^ enc node.layer ^"/"^ enc node.name in
        if not single || !reloading <> Some path then (
          reloading := Some path ;
          http_post path content ~on_done:(fun () ->
              reloading := None)
            (fun r ->
              update_tail r ;
              resync ()))

(* A list of field_name * points *)
let chart_points = make_param "chart points" []

let chart_type = make_param "chart type" RamenChart.NotStacked

let show_zero = make_param "show zero" false

let sel_output_cols = make_param "selected output columns" []

let chart_duration = make_param "chart duration" (3. *. 3600.)

let chart_relto = make_param "chart rel.to" true

let update_chart field_names resp =
  List.mapi (fun i field_name ->
    let resp = Js.(array_get resp i |> optdef_get) in
    let times = Js.Unsafe.get resp "times"
    and values = Js.Unsafe.get resp "values" in
    let nb_points = times##.length in
    let points = Array.init nb_points (fun i ->
      let t = Js.(array_get times i |> optdef_get |> float_of_number)
      and v = Js.(array_get values i |> optdef_get |> Opt.to_option |>
              option_map float_of_number) in
      t, v) in
    field_name, points) field_names |>
  set chart_points

let reload_chart =
  let reloading = ref None in
  fun ?(single=false) () ->
    match List.assoc sel_node.value nodes.value,
          sel_output_cols.value with
    | exception Not_found -> ()
    | _, [] -> ()
    | node, cols ->
      let node = node.value in
      let get_timeseries_until until =
        (* Request the timeseries *)
        let field_names =
          List.map (fun col ->
            (List.nth node.output_type col).Field.name) cols in
        let content =
          object%js
            val since = js_of_float (until -. chart_duration.value)
            val until = js_of_float until
            val max_data_points_ = 800
            val timeseries =
              List.map (fun field_name ->
                object%js
                  val id = Js.string field_name
                  val consolidation = Js.string "avg"
                  val spec =
                    object%js
                      val _Predefined =
                        object%js
                          val node = Js.string node.id
                          val data_field_ = Js.string field_name
                        end
                    end
                end) field_names |>
              js_of_list identity
          end
        and path = "/timeseries" in
        if not single || !reloading <> Some content then
          http_post path content ~on_done:(fun () ->
              reloading := None)
            (fun r ->
              update_chart field_names r ;
              resync ())
      in
      if chart_relto.value then
        (* Get the available time range *)
        let path = "/timerange/"^ enc node.layer ^"/"^ enc node.name in
        http_get path (fun r ->
          match time_range_of_js r with
          | NoData -> ()
          | TimeRange (_, until) ->
            get_timeseries_until until)
      else get_timeseries_until (now ())

let sel_top_column = make_param "selected top column" "layer"

let reset_for_node_change () =
  set sel_output_cols [] ;
  set chart_type RamenChart.NotStacked ;
  set show_zero false ;
  set tail_rows [||] ;
  reload_tail ()

let set_sel_layer l =
  if sel_layer.value <> l then (
    set sel_layer l ;
    if l <> NoLayer then
      (* Try to keep edited content as long as possible: *)
      let el = edited_layer_of_layer l in
      if !(edited_layer.value.layer_name) <> !(el.layer_name) then
        set edited_layer el)

let set_sel_node = function
  Some node ->
  if node.Node.id <> sel_node.value then (
    set sel_node node.id ;
    let l = ExistingLayer node.layer in
    set_sel_layer l ;
    reset_for_node_change ())
| None ->
  if sel_node.value <> "" then (
    set sel_node "" ;
    reset_for_node_change ())

let get_variant js =
  let open Js in
  let a = object_keys js in
  array_get a 0 |> optdef_get |> to_string

let type_spec_of_js r =
  list_init r##.length (fun i ->
    let t = Js.array_get r i in
    let name = Js.(Unsafe.get t "name_info" |> to_string)
    and nullable = Js.(Opt.map (Unsafe.get t "nullable_info" |> some) to_bool |> Opt.to_option)
    and typ_str = Js.(Opt.map (Unsafe.get t "typ_info" |> some) get_variant |> Opt.to_option) in
    let typ, typ_str, typ_disp = match typ_str with
      | None -> None, "unknown type", "unknown type"
      | Some ts ->
        let typ_str =
          if String.length ts > 0 && ts.[0] = 'T' then
            String.sub ts 1 (String.length ts - 1)
          else ts in
        Some (Field.type_of_string ts),
        typ_str,
        String.lowercase typ_str ^ (match nullable with
          | Some true -> " (or null)"
          | Some false -> ""
          | None -> " (unknown nullability)") in
    Field.{ name ; nullable ; typ ; typ_str ; typ_disp })

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
        tot_nodes +. 1.,
        tot_ins +. (n.Node.stats.in_tuple_count |? 0.),
        tot_sels +. (n.stats.sel_tuple_count |? 0.),
        tot_outs +. (n.stats.out_tuple_count |? 0.),
        tot_grps +. (n.stats.group_count |? 0.),
        tot_cpu +. n.stats.cpu_time,
        tot_ram +. n.stats.ram_usage,
        tot_in_sleep +. (n.stats.in_sleep |? 0.),
        tot_out_sleep +. (n.stats.out_sleep |? 0.),
        tot_in_bytes +. (n.stats.in_bytes |? 0.),
        tot_out_bytes +. (n.stats.out_bytes |? 0.)
      ) zero_sums nodes.value in
  set nodes_sum sum

(* Jstable of spinning layers *)
let spinners = make_param "spinners" (Jstable.create ())
let spinner_icon = "❍" (*"°"*)

let definition_of_js js =
  of_field js "name" Js.to_string,
  of_field js "operation" Js.to_string

let worker_stats_of_js js =
  Node.{
    time = of_field js "time" Js.to_float ;
    in_tuple_count = of_opt_field js "in_tuple_count" Js.to_float ;
    out_tuple_count = of_opt_field js "out_tuple_count" Js.to_float ;
    sel_tuple_count = of_opt_field js "selected_tuple_count" Js.to_float ;
    group_count = of_opt_field js "group_count" Js.to_float ;
    cpu_time = of_field js "cpu_time" Js.to_float ;
    ram_usage = of_field js "ram_usage" Js.to_float ;
    in_sleep = of_opt_field js "in_sleep" Js.to_float ;
    out_sleep = of_opt_field js "out_sleep" Js.to_float ;
    in_bytes = of_opt_field js "in_bytes" Js.to_float ;
    out_bytes = of_opt_field js "out_bytes" Js.to_float }

let node_of_js layer js =
  let name, operation = of_field js "definition" definition_of_js in
  let id = layer.Layer.name ^"/"^ name in
  let stats = of_field js "stats" worker_stats_of_js in
  Node.{
    layer = layer.Layer.name ;
    name ; id ; operation ;
    exporting = of_field js "exporting" Js.to_bool ;
    input_type = of_field js "input_type" type_spec_of_js ;
    output_type = of_field js "output_type" type_spec_of_js ;
    parents = of_field js "parents" node_list_of_js ;
    children = of_field js "children" node_list_of_js ;
    stats ;
    last_stats = stats ; (* set later by update_node *)
    pid = of_opt_field js "pid" to_int ;
    signature = of_opt_field js "signature" Js.to_string }

let update_graph total g =
  (* g is a JS array of layers *)
  (* Keep track of the layers we had to clean the extra ones at the end: *)
  let had_layers = ref [] in
  let had_nodes = ref [] in
  for i = 0 to g##.length - 1 do
    let l = Js.array_get g i in
    let name = Js.(Unsafe.get l "name" |> to_string) in
    let status_js = Js.Unsafe.get l "status" in
    let status, status_str = Layer.status_of_js status_js in
    had_layers := name :: !had_layers ;
    let nodes = Js.Unsafe.get l "nodes" in
    let layer = Layer.{
      name ; status_str ; status ; order = i ;
      last_started = Js.(Unsafe.get l "last_started" |> Opt.to_option |>
                         option_map float_of_number) ;
      last_stopped = Js.(Unsafe.get l "last_stopped" |> Opt.to_option |>
                         option_map float_of_number) ;
      nb_nodes = nodes##.length } in
    update_layer layer ;
    for j = 0 to nodes##.length - 1 do
      let n = Js.array_get nodes j in
      let node = node_of_js layer n in
      had_nodes := node.Node.id :: !had_nodes ;
      update_node node
    done
  done ;
  (* Order the layers according to dependencies*)
  layers.value <-
    List.fast_sort (fun (_, a) (_, b) ->
      compare a.value.Layer.order b.value.Layer.order) layers.value ;
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
        print (Js.string ("Deleting node "^ id)) ;
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

let reload_graph =
  let reloading = ref false in
  fun ?redirect_to_layer ?(single=false) () ->
    if not single || not !reloading then (
      reloading := true ;
      http_get "/graph" ~on_done:(fun () ->
          if single then reloading := false)
        (fun g ->
          update_graph true g ;
          option_may set_sel_layer redirect_to_layer ;
          resync ()))

(* Panel pending deletion, if any. There is only one, so selecting another
 * layer for deletion cancel the delete status of the current one. *)
let layer_to_delete = make_param "layer pending deletion" ""

let autoreload = make_param "autoreload" true
let display_temp = make_param "display temp layers" false
let is_temp layer_name = string_starts_with "temp/" layer_name
let cumul_tot_ins = make_param "cumul tot_ins" true
let cumul_tot_sels = make_param "cumul tot_sels" true
let cumul_tot_outs = make_param "cumul tot_outs" true
let cumul_tot_grps = make_param "cumul tot_grps" true
let cumul_tot_cpu = make_param "cumul tot_cpu" true
let cumul_tot_in_sleep = make_param "cumul tot_in_sleep" true
let cumul_tot_out_sleep = make_param "cumul tot_out_sleep" true
let cumul_tot_ram = make_param "cumul tot_ram" true
let cumul_tot_in_bytes = make_param "cumul tot_in_bytes" true
let cumul_tot_out_bytes = make_param "cumul tot_out_bytes" true

(* Loading / Saving data *)

let reload_for_current_page init =
  if init || autoreload.value then (
    match current_page.value with
    | PageEventProcessor ->
      (match sel_layer.value with
      | NoLayer | ExistingLayer _ -> reload_graph ~single:true ()
      | _ -> ()) ;
      if sel_node.value <> "" then (
        reload_tail ~single:true () ;
        reload_chart ~single:true ())
    | PageLive -> Alerter_app.reload_ongoing ()
    | PageHistory -> Alerter_app.reload_history ()
    | PageTeam -> if init then Alerter_app.reload_teams () else ())

(* DOM *)

let spacer = div [ clss "spacer" ] []

let tab label page =
  with_param current_page (fun cp ->
    div ~action:(fun _ ->
        set current_page page ;
        reload_for_current_page true)
      [ if cp = page then clss "tab selected"
                     else clss "tab actionable" ]
      [ p [] [ text label ] ])

let nav_bar =
  div
    [ clss "tabs" ]
    [ tab "Live" PageLive ;
      tab "History" PageHistory ;
      with_param Alerter_app.sel_team (function
        | AllTeams -> tab "Teams" PageTeam
        | SingleTeam t -> tab ("Team "^ t) PageTeam) ;
      tab "Event Processor" PageEventProcessor ]

let header_panel =
  group
    [ div
      [ id "global" ]
      [ div
          [ clss "title" ]
          [ div [] [ text "Ramen $VERSION$" ] ;
            div [] [ text "running on " ;
                     em [ text "$HOSTNAME$." ] ] ] ;
        nav_bar ;
        spacer ;
        with_param display_temp (fun disp ->
          let help = (if disp then "do not " else "")^
                     "display temporary layers" in
          button ~action:(fun _ -> toggle display_temp)
            (icon_class ~help disp)
            [ text "temp." ]) ;
        with_param autoreload (fun ar ->
          let help = (if ar then "dis" else "en")^ "able auto-reload" in
          button ~action:(fun _ -> toggle autoreload)
            (icon_class ~help ar)
            [ text "⟳" ]) ] ;
      with_param last_errors (fun lst ->
        let ps =
          List.map (fun e ->
            p [ clss (if e.is_error then "error" else "ok") ]
              [ text e.message ;
                if e.times > 1 then
                  span [ clss "err-times" ]
                       [ text (" × "^ string_of_int e.times) ]
                else group [] ]) lst in
        if ps = [] then group [] else div [ id "messages" ] ps) ]

let labeled_value l v =
  p [] [
      span [ clss "label" ] [ text (l ^ ":") ] ;
      span [ clss "value" ] [ text v ] ]

let with_node node_id f =
  with_param nodes (fun nodes ->
    match List.assoc node_id nodes with
    | exception Not_found -> text ("Can't find node "^ node_id)
    | node -> f node.value)

let icon_of_layer ?(suppress_action=false) layer =
  let icon, path, alt, what, while_ =
    match layer.Layer.status with
    | Edition _ ->
      "⚙", "/compile/"^ enc layer.Layer.name,
      "compile", Some ("Compiled "^ layer.Layer.name),
      "compiling..."
    | Compiling ->
      "☐", "/graph/"^ enc layer.Layer.name,
      "reload", None, "compiling..."
    | Compiled ->
      "▷", "/start/"^ enc layer.Layer.name,
      "start", Some ("Started "^ layer.Layer.name),
      "starting..."
    | Running ->
      "||", "/stop/"^ enc layer.Layer.name,
      "stop", Some ("Stopped "^ layer.Layer.name),
      "stopping..."
  in
  let js_name = Js.string layer.name in
  let action =
    if suppress_action then None
    else (
      Some (fun _ ->
        Jstable.add spinners.value js_name while_ ;
        change spinners ;
        http_get path ?what ~on_done:(fun () ->
          Jstable.remove spinners.value js_name ;
          change spinners)
          (fun status ->
            (* FIXME: graph won't return a status so the following will
             * fail for Compiling. Make all these return proper JSON RPC *)
            if Js.(Unsafe.get status "success" |> to_bool) then
              http_get ("/graph/" ^ enc layer.Layer.name) (fun g ->
                update_graph false g ;
                resync ())))) in
  button ?action
    [ clss "icon actionable" ; title alt ] [ text icon ]

let editor_spinning = make_param "editor spinning" false

let done_edit_layer_cb ?redirect_to_layer what status =
  if Js.(Unsafe.get status "success" |> to_bool) then (
    Firebug.console##log (Js.string ("DONE "^ what)) ;
    reload_graph ?redirect_to_layer ()
  ) else (
    Firebug.console##error_2 (Js.string ("Cannot "^ what ^" layer")) status
  )

let del_layer layer_name =
  let js_name = Js.string layer_name in
  Jstable.add spinners.value js_name "deleting..." ;
  let path = "/graph/"^ enc layer_name
  and what = "Deleted "^ layer_name in
  http_del path ~what ~on_done:(fun () ->
    Jstable.remove spinners.value js_name ;
    change spinners)
    (done_edit_layer_cb ~redirect_to_layer:NoLayer "delete")

let layer_panel to_del layer =
  let is_to_del = to_del = layer.Layer.name in
  let date_or_never = function
    | Some ts -> date_of_ts ts
    | None -> "never" in
  (* In order not to change the size of the panel we paint the
   * confirmation dialog on top of the normal tile. *)
  let e = [
    div
      [ clss "title" ]
      [ p [ clss "name" ] [ text layer.Layer.name ] ;
        with_param spinners (fun spins ->
          Js.Optdef.case (Jstable.find spins (Js.string layer.name))
            (fun () ->
              group [
                if layer.status <> Running then (
                  let action =
                     if is_to_del then None
                     else Some (fun _ -> set layer_to_delete layer.name) in
                  button ?action
                    [ clss "actionable icon" ; title "delete" ] [ text "⌫" ]
                ) else group [] ;
                icon_of_layer ~suppress_action:is_to_del layer ])
            (fun while_ ->
              button [ clss "icon spinning" ; title while_ ]
                     [ text spinner_icon ])) ] ;
    div
      [ clss "info" ]
      (labeled_value "#nodes" (string_of_int layer.nb_nodes) ::
       labeled_value "started" (date_or_never layer.last_started) ::
       labeled_value "stopped" (date_or_never layer.last_stopped) ::
       (match layer.status with
       | Edition err when err <> "" ->
         [ p [ clss "error" ; title err ] [ text (abbrev 25 err) ] ]
       | _ -> [])) ]
  in
  with_param sel_layer (fun slayer ->
    if is_to_del then
      div
        [ clss "warning layer" ]
        ( div
            [ clss "overwrite1" ]
            [ div
                [ clss "overwrite2" ]
                [ p []
                    [ text "Delete layer " ;
                      em [ text layer.Layer.name ] ;
                      text "?" ] ;
                  p [ clss "yes-or-no" ]
                    [ span ~action:(fun _ -> del_layer layer.name)
                        [ clss "yes" ] [ text "yes" ] ;
                      text "/" ;
                      span ~action:(fun _ -> set layer_to_delete "")
                        [ clss "no" ] [ text "NO" ] ] ] ] :: e )
    else if slayer = ExistingLayer layer.name then
      div ~action:(fun _ -> set_sel_layer NoLayer ; set_sel_node None)
        [ clss "selected-actionable layer" ] e
    else
      div ~action:(fun _ ->
          let l = ExistingLayer layer.name in
          set_sel_layer l ;
          set_sel_node None ;
          set layer_to_delete "")
        [ clss "actionable layer" ] e)

let layers_panel =
  div [] [
    with_param display_temp (fun disp_temp ->
      with_param layers (fun layers ->
        with_param layer_to_delete (fun to_del ->
          List.fold_left (fun lst (_, p) ->
            with_param p (fun layer ->
              if disp_temp || not (is_temp layer.Layer.name) then
                layer_panel to_del layer
              else group []) :: lst) [] layers |>
          List.rev |>
          group))) ;
    with_param sel_layer (fun sl ->
      let c, action =
        if sl = NewLayer then "selected new-layer", (fun _ ->
          set_sel_layer NoLayer)
        else "actionable new-layer", (fun _ ->
          set_sel_layer NewLayer ;
          set_sel_node None ;
          set layer_to_delete "") in
      button ~action [ clss c ] [ text "new layer" ]) ]

(* filter_param is a string param with a search string *)
let node_thead_col ?filter_param (title, subtitle, sortable, cumul_opt) =
  with_param sel_top_column (fun col ->
    let sort_me =
      if sortable then
        let help = "Sort the table according to this column" in
        button ~action:(fun _ ->
            if col = title then
              set sel_top_column "layer" (* the default *)
            else
              set sel_top_column title)
          (icon_class ~help (col = title))
          [ text "sort" ]
      else group [] in
    let cumul_me =
      match cumul_opt with
      | None -> group []
      | Some p ->
        with_param p (fun cumul ->
          let help =
            "display "^(if cumul then "instant" else "cumulative")^" stats" in
          button ~action:(fun _ -> toggle p)
            (icon_class ~help (not cumul))
            [ text "/s" ]) in
    let search_box = match filter_param with
      | None -> group []
      | Some filter ->
          with_param filter (fun flt ->
            elmt "label"
              [ clss "searchbox" ]
              [ text "🔍" ;
                input ~action:(fun v -> set filter v)
                  [ attr "type" "text" ;
                    attr "size" "12" ;
                    attr "placeholder" "search..." ;
                    attr "value" flt ] ]) in
    th []
       [ p []
           [ text title ; cumul_me ; sort_me ; search_box ] ;
         if subtitle = "" then group [] else
         p [ clss "type" ] [ text subtitle ] ])

let tds v = td [] [ text v ]
let tdo = function None -> tds "n/a" | Some v -> tds v
let tdi v = td [ clss "number" ] [ text (string_of_int v) ]
let tdf v = td [ clss "number" ] [ text (str_of_float v) ]
(* Sometime we use floats to get bigger integers.
 * Do not add nbsp to those: *)
let tdfi v = td [ clss "number" ] [ text (string_of_float v) ]

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
  let dt = node.Node.stats.time -. node.Node.last_stats.time in
  let na = td [ clss "number" ] [ text "n/a" ] in
  let unk = td [ clss "number" ] [ text "unknown" ] in
  let tdh ~to_str tot x =
    td [ clss "number" ]
       [ text (to_str x) ;
         if tot = 0. then group [] else
         let w = 100. *. x /. tot in
         hr [ attr "width" (string_of_float w) ] ] in
  let tdhcs ~to_str cumul_p tot x last_x =
    with_param cumul_p (function
      | true -> tdh ~to_str tot x
      | false ->
          match last_x with
          | None -> unk
          | Some last_x ->
            if dt = 0. then unk else
            let x = (x -. last_x) /. dt in
            (* In non-cumulative mode we don't have round numbers anymore
             * so we force str_of_float for conversions: *)
            tdh ~to_str:str_of_float tot x) in
  let tdfh cumul_p tot x last_x =
    tdhcs ~to_str:str_of_float cumul_p tot x last_x
  and tdofih cumul_p tot x last_x =
    match x with
    | None -> na
    | Some x ->
      tdhcs ~to_str:string_of_float cumul_p tot x last_x in
  let tdofh cumul_p tot x last_x =
    match x with
    | None -> na
    | Some x -> tdfh cumul_p tot x last_x in
  let tdoi = function None -> na | Some v -> tdi v in
  with_param nodes_sum (fun (_tot_nodes, tot_ins, tot_sels, tot_outs,
                             tot_grps, tot_cpu, tot_ram, tot_in_sleep,
                             tot_out_sleep, tot_in_bytes, tot_out_bytes) ->
    let cols =
      [ tds node.layer ;
        tds node.name ;
        tdofih cumul_tot_ins tot_ins node.stats.in_tuple_count node.last_stats.in_tuple_count ;
        tdofih cumul_tot_sels tot_sels node.stats.sel_tuple_count node.last_stats.sel_tuple_count ;
        tdofih cumul_tot_outs tot_outs node.stats.out_tuple_count node.last_stats.out_tuple_count ;
        tdofih cumul_tot_grps tot_grps node.stats.group_count node.last_stats.group_count ;
        td [ clss "export" ]
           [ text (if node.exporting then "✓" else " ") ] ;
        tdfh cumul_tot_cpu tot_cpu node.stats.cpu_time (Some node.last_stats.cpu_time) ;
        tdofh cumul_tot_in_sleep tot_in_sleep node.stats.in_sleep node.last_stats.in_sleep ;
        tdofh cumul_tot_out_sleep tot_out_sleep node.stats.out_sleep node.last_stats.out_sleep ;
        tdfh cumul_tot_ram tot_ram node.stats.ram_usage (Some node.last_stats.ram_usage) ;
        tdofih cumul_tot_in_bytes tot_in_bytes node.stats.in_bytes node.last_stats.in_bytes ;
        tdofih cumul_tot_out_bytes tot_out_bytes node.stats.out_bytes node.last_stats.out_bytes ;
        tds (short_node_list node.layer node.parents) ;
        tds (short_node_list node.layer node.children) ;
        tdoi node.pid ;
        tdo node.signature ] in
    (* FIXME: So all the lines vary every time sel_node changes. Ie we are going to
     * redraw the whole table, while in theory only two lines must be redrawn.
     * Instead, we could have one individual boolean state variable per line and this would
     * depend only on this. *)
    with_param sel_node (fun sel ->
      if sel = node.Node.id then
        tr ~action:(fun _ -> set_sel_node None)
          [ clss "selected-actionable" ] cols
      else
        tr ~action:(fun _ -> set_sel_node (Some node))
          [ clss "actionable" ] cols))

let node_sorter col =
  (* Numbers are sorted greater to smaller while strings are sorted
   * in ascending order: *)
  let make f (_, a) (_, b) =
    (* TODO: when !cumul_stats, f should take both a and last_a *)
    f a.value b.value in
  let open Node in
  match col with
  | "#in" ->
    make (fun a b -> compare b.stats.in_tuple_count a.stats.in_tuple_count)
  | "#selected" ->
    make (fun a b -> compare b.stats.sel_tuple_count a.stats.sel_tuple_count)
  | "#out" ->
    make (fun a b -> compare b.stats.out_tuple_count a.stats.out_tuple_count)
  | "#groups" ->
    make (fun a b -> match b.stats.group_count, a.stats.group_count with
         | None, None -> 0
         | Some _, None -> 1
         | None, Some _ -> -1
         | Some i2, Some i1 -> compare i2 i1)
  | "export" -> make (fun a b -> compare b.exporting a.exporting)
  | "CPU" -> make (fun a b -> compare b.stats.cpu_time a.stats.cpu_time)
  | "wait in" -> make (fun a b -> compare b.stats.in_sleep a.stats.in_sleep)
  | "wait out" -> make (fun a b -> compare b.stats.out_sleep a.stats.out_sleep)
  | "bytes in" -> make (fun a b -> compare b.stats.in_bytes a.stats.in_bytes)
  | "bytes out" -> make (fun a b -> compare b.stats.out_bytes a.stats.out_bytes)
  | "heap" -> make (fun a b -> compare b.stats.ram_usage a.stats.ram_usage)
  | "volume in" -> make (fun a b -> compare b.stats.in_bytes a.stats.in_bytes)
  | "volume out" -> make (fun a b -> compare b.stats.out_bytes a.stats.out_bytes)
  | _ ->
    make (fun a b -> match compare a.layer b.layer with
         | 0 -> compare a.name b.name
         | x -> x)

(* TODO: add a health indicator (based on how old is the last report) *)
let node_columns =
  [| "layer", "", true, None ;
     "name", "", true, None ;
     "#in", "tuples", true, Some cumul_tot_ins ;
     "#selected", "tuples", true, Some cumul_tot_sels ;
     "#out", "tuples", true, Some cumul_tot_outs ;
     "#groups", "", true, Some cumul_tot_grps ;
     "export", "", true, None ;
     "CPU", "seconds", true, Some cumul_tot_cpu ;
     "wait in", "seconds", true, Some cumul_tot_in_sleep ;
     "wait out", "seconds", true, Some cumul_tot_out_sleep ;
     "heap", "bytes", true, Some cumul_tot_ram ;
     "volume in", "bytes", true, Some cumul_tot_in_bytes ;
     "volume out", "bytes", true, Some cumul_tot_out_bytes ;
     "parents", "", false, None ;
     "children", "", false, None ;
     "PID", "", false, None ;
     "signature", "", false, None |]

let wide_table lst =
  div
    [ clss "wide" ]
    [ table [] lst ]

let node_filter = make_param "node filter" ""

let nodes_panel =
  wide_table [
    thead [] [
      Array.fold_left (fun lst (col_name, _, _, _ as col) ->
        let filter_param =
          if col_name = "name" then Some node_filter
          else None in
        node_thead_col ?filter_param col :: lst) [] node_columns |>
      List.rev |> tr [] ] ;
    (* Table body *)
    with_param display_temp (fun disp_temp ->
      with_param nodes (fun nodes ->
        with_param sel_top_column (fun sel_col ->
          with_param sel_layer (fun sel_lay ->
            with_param node_filter (fun node_flt ->
              (* Build a list sorted according to sel_top_column: *)
              let rows =
                nodes |>
                List.filter (fun (_, p) ->
                  let n = p.value in
                  (sel_lay = ExistingLayer n.Node.layer ||
                   (sel_lay = NoLayer &&
                    (disp_temp || not (is_temp n.Node.layer)))) &&
                  string_starts_with node_flt n.Node.name) |>
                List.fold_left (fun lst p -> p :: lst) [] |>
                List.fast_sort (node_sorter sel_col) in
              List.map (fun (_, p) ->
                with_param p node_tbody_row) rows |>
              tbody []))))) ;
    with_param nodes_sum (fun (tot_nodes, tot_ins, tot_sels, tot_outs,
                               tot_grps, tot_cpu, tot_ram, tot_in_sleep,
                               tot_out_sleep, tot_in_bytes,
                               tot_out_bytes) ->
      tfoot [] [
        tr [] [
          tds "Total:" ; tdfi tot_nodes ; tdfi tot_ins ;
          tdfi tot_sels ; tdfi tot_outs ; tdfi tot_grps ;
          tds "" ; tdf tot_cpu ; tdf tot_in_sleep ; tdf tot_out_sleep ;
          tdfi tot_ram ; tdfi tot_in_bytes ; tdfi tot_out_bytes ;
          tds "" ; tds "" ; tds "" ; tds "" ] ]) ]

let field_panel f =
  labeled_value f.Field.name f.typ_disp

let input_output_panel =
  with_param sel_node (fun sel ->
    if sel = "" then
      p [ clss "nodata" ]
        [ text "Select a node to see its input/output" ]
    else with_node sel (fun node ->
      group [
        h1 [] [ text "Input fields" ] ;
        ol [] (List.map (fun f ->
          li [] [ field_panel f ]) node.input_type) ;
        h1 [] [ text "Output fields" ] ;
        ol [] (List.map (fun f ->
          li [] [ field_panel f]) node.output_type) ]))

let can_plot_type = function
    Some (TFloat | TU8 | TU16 | TU32 | TU64 | TU128 |
          TI8 | TI16 | TI32 | TI64 | TI128) -> true
  | _ -> false

let op_panel =
  with_param sel_node (fun sel ->
    if sel = "" then
      p [ clss "nodata" ]
        [ text "Select a node to see the operation it performs" ]
    else with_node sel (fun node ->
      div
        [ clss "operation" ]
        [ elmt "pre" [] [ text node.operation ] ]))

let tail_panel =
  let pretty_th ?action c title subtitle =
    th ?action
      (if c <> "" then [ clss c ] else [])
      (p [] title ::
       if subtitle = "" then []
       else [ p [ clss "type" ] [ text subtitle ] ]) in
  let row fs r =
    let rec loop tds ci = function
      [] -> tr [] (List.rev tds)
    | field::fs ->
      let tds =
        (td [ clss field.Field.typ_str ]
            [ match r.(ci) with
                None -> span [ clss "null" ] [ text "NULL" ]
              | Some v -> text v ]) :: tds in
      loop tds (ci + 1) fs in
    loop [] 0 fs
  and th_field ci f =
    with_param sel_output_cols (fun cols ->
      let c, action =
        if can_plot_type f.Field.typ then
          let is_selected = List.mem ci cols in
          let action _ =
            let toggled =
              if is_selected then
                List.filter ((<>) ci) cols
              else ci :: cols in
            set sel_output_cols toggled ;
            reload_chart () in
          let c = if is_selected then "selected actionable"
                                 else "actionable" in
          c, Some action
        else "", None in
      pretty_th ?action c [ text f.Field.name ] f.typ_disp)
  in
  with_param sel_node (fun sel ->
    if sel = "" then
      p [ clss "nodata" ]
        [ text "Select a node to see its output" ]
    else with_node sel (fun node ->
      let lame_excuse t =
        tbody [] [ tr [] [ td
          [ attri "colspan" (List.length node.output_type) ]
          [ p [] [ text t ] ] ] ] in
      wide_table
        [ thead [] [ tr [] (List.mapi th_field node.output_type) ] ;
          (if not node.exporting then
            lame_excuse ("node "^ node.id ^" does not export data")
          else
            with_param tail_rows (fun rows ->
              Array.fold_left (fun l r ->
                row node.output_type r :: l) [] rows |>
              List.rev |> tbody []))]))

let chart_type_selector =
  with_param chart_type (fun cur_ct ->
    let sel label ct =
      if ct = cur_ct then
        button [ clss "selected" ] [ text label ]
      else
        button ~action:(fun _ -> set chart_type ct)
          [ clss "actionable" ] [ text label ] in
    div
      [ clss "chart-buttons" ]
      [ sel "normal" RamenChart.NotStacked ;
        sel "stacked" RamenChart.Stacked ;
        sel "stacked+centered" RamenChart.StackedCentered ])

let show_zero_selector =
  with_param show_zero (fun fz ->
    div
      [ clss "chart-buttons" ]
      [ button ~action:(fun _ -> toggle show_zero)
          [ clss (if fz then "selected-actionable" else "actionable") ]
          [ text "force zero" ] ])

let timechart_panel =
  with_param sel_output_cols (function
    | [] ->
      p [ clss "nodata" ]
        [ text "Select one or several columns to plot them." ]
    | _ ->
      with_param chart_points (fun field_pts ->
        if field_pts = [] || Array.length (snd (List.hd field_pts)) = 0
        then p [ clss "nodata" ]
               [ text "No data received yet" ]
        else
          (* We consider times are the same for all fields *)
          let fst_field_name, fst_pts = List.hd field_pts in
          let nb_pts = Array.length fst_pts in
          let single_field = match field_pts with [_] -> true | _ -> false in
          (* Notice that we have time values but we may still not have data at
           * those times. *)
          let vx_start =
            fst fst_pts.(0) and vx_stop = fst fst_pts.(nb_pts-1) in
          let fold = { RamenChart.fold = fun f init ->
            List.fold_left (fun f_val (field_name, pts) ->
                let pen =
                  RamenChart.{
                    label = field_name ; draw_line = true ; draw_points = true ;
                    color = RamenColor.random_of_string field_name ;
                    stroke_width = 1.5 ; opacity = 1. ;
                    dasharray = None ; filled = true ; fill_opacity = 0.3 } in
                f f_val pen true (fun i -> snd pts.(i) |? 0. (* TODO: handle None *))
              ) init field_pts } in
          let svg_width = 800. and svg_height = 400. in
          let attrs = [ clss "chart" ] in
          div []
            [ time_selector ~action:reload_chart chart_duration chart_relto ;
              chart_type_selector ;
              show_zero_selector ;
              with_param chart_type (fun stacked_y1 ->
                with_param show_zero (fun force_show_0 ->
                  RamenChart.xy_plot ~attrs ~svg_width ~svg_height
                    ~string_of_x:RamenFormats.((timestamp string_of_timestamp).to_label)
                    ~string_of_y:RamenFormats.(numeric.to_label)
                    ~stacked_y1 ~draw_legend:RamenChart.UpperRight
                    ~force_show_0 "time"
                    (if single_field then fst_field_name else "")
                    vx_start vx_stop nb_pts shash fold)) ]))

let form_input label value placeholder =
  let size = String.length !value + 10 in
  div
    [ clss "input" ]
    [ elmt "label" []
        [ text label ;
          input ~action:(fun v -> value := v ; change edited_layer)
            [ attr "type" "text" ;
              attr "size" (string_of_int size) ;
              attr "placeholder" placeholder ;
              attr "value" !value ] ] ]

let get_text_size s =
  let rec loop_line nb_l longest from =
    match String.index_from s from '\n' with
    | exception Not_found ->
      max longest (String.length s - from), nb_l + 1
    | eol ->
      loop_line (nb_l + 1) (max longest (eol - from)) (eol + 1)
  in
  loop_line 0 0 0

let form_input_large label value placeholder =
  let cols, rows = get_text_size !value in
  let cols = max 20 (cols + 5)
  and rows = max 5 (rows + 4) in
  div
    [ clss "input" ]
    [ elmt "label" []
        [ text label ;
          br ;
          textarea ~action:(fun v -> value := v ; change edited_layer)
            [ attr "rows" (string_of_int rows) ;
              attr "cols" (string_of_int cols) ;
              attr "placeholder" placeholder ;
              attr "spellcheck" "false" ]
            [ text !value ] ] ]

let node_editor_panel (name, operation) =
  div
    [ clss "node-edition" ]
    [ form_input "Name" name "enter a node name" ;
      form_input_large "Operation" operation "enter operation here" ;
      hr [] ]

let save_layer _ =
  let js_of_node (name, operation) =
    object%js
      val name = Js.string !name
      val operation = Js.string !operation
    end
  and edl = edited_layer.value in
  let nodes =
    List.filter (fun (name, operation) ->
      !name <> "" && !operation <> "") edl.edited_nodes in
  let content =
    object%js
      val name = Js.string !(edl.layer_name)
      val nodes = js_of_list js_of_node nodes
    end
  and path = "/graph"
  and what = "Saved "^ !(edl.layer_name) in
  set editor_spinning true ;
  let redirect_to_layer = ExistingLayer !(edl.layer_name) in
  http_put path content ~what
    ~on_done:(fun () -> set editor_spinning false)
    (done_edit_layer_cb ~redirect_to_layer "save")

let add_edited_node () =
  let edl = edited_layer.value in
  edl.edited_nodes <-
    edl.edited_nodes @ [ new_edited_node edl.edited_nodes ] ;
  change edited_layer

let layer_editor_panel =
  with_param edited_layer (fun edl ->
    let title =
      if edl.is_new then "New layer configuration"
      else "Configuration for "^ !(edl.layer_name) in
    div
      [ id "editor" ]
      [ h1 [] [ text title ] ;
        (if edl.is_new then
          form_input "Name" edl.layer_name "enter a node name"
        else group []) ;
        h2 [] [ text "Nodes" ] ;
        group (List.map node_editor_panel edl.edited_nodes) ;
        br ;
        with_param editor_spinning (fun spinning ->
          group
            [ if spinning then
                button [] [ text "+" ]
              else
                button ~action:(fun _ -> add_edited_node ())
                  [ clss "actionable" ] [ text "+" ] ;
              if spinning then
                button [] [ text "Cancel" ]
              else
                button ~action:(fun _ -> set_sel_layer NoLayer)
                  [ clss "actionable" ] [ text "Cancel" ] ;
              if spinning then
                button [] [ text "Save" ]
              else
                button ~action:save_layer
                  [ clss "actionable" ] [ text "Save" ] ;
              if spinning then
                button [ clss "spinning" ] [ text spinner_icon ]
              else group [] ]) ])

let output_panel =
  div
    [ id "output" ]
    [ h1 [] [ text "Raw Output" ] ;
      tail_panel ;
      div [ id "timechart" ] [ timechart_panel ] ]

let top_layers =
  div [ id "layers" ] [ h1 [] [ text "Layers" ] ; layers_panel ]

let top_nodes =
  div [ id "nodes" ] [ h1 [] [ text "Nodes" ] ; nodes_panel ]

let event_processor_page =
  with_param sel_node (function
    "" ->
    with_param sel_layer (function
      NoLayer ->
      div [ id "top" ] [ top_layers ; top_nodes ]
    | ExistingLayer slayer ->
      with_param layers (fun layers ->
        match List.assoc slayer layers with
        | exception Not_found -> group []
        | layer ->
          with_param layer (fun layer ->
            div
              [ id "top" ]
              [ top_layers ; top_nodes ;
                if layer.status <> Running then
                  layer_editor_panel
                else
                  p [ clss "nodata" ]
                    [ text "Running layer cannot be edited" ] ]))
    | NewLayer ->
      div [ id "top" ] [ top_layers ; layer_editor_panel ])
  | snode ->
    match List.assoc snode nodes.value with
    | exception Not_found -> group []
    | node ->
      let node = node.value in
      group
        [ div [ id "top" ] [ top_layers ; top_nodes ] ;
          div
            [ id "details" ]
            [ div [ id "in-out" ]
                  [ input_output_panel ] ;
              div [ id "operation" ]
                  [ h1 [] [ text "Operation" ] ; op_panel ] ] ;
          (* TODO: instead of true/false this should return
           * ExportNothing/ExportOut/ExportWithTime so that we know if we
           * can offer to plot the chart as well as display the tail. *)
          can_export_with_layer node (function
            true -> output_panel
          | false -> p [ clss "nodata" ]
                       [ text "This node exports no data" ]) ])


let dom =
  group
    [ header_panel ;
      div [ id "page" ]
          [ with_param current_page (function
            | PageEventProcessor ->
              event_processor_page
            | PageLive -> Alerter_app.page_live
            | PageHistory -> Alerter_app.page_history
            | PageTeam -> Alerter_app.page_teams) ] ]

let () =
  reload_for_current_page true ;
  Html.window##setInterval (Js.wrap_callback
      (fun () -> reload_for_current_page false)
    ) 3_137. |>
  ignore ;
  start dom
