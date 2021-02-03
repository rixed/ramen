open Batteries

module E = RamenExpr
module Globals = RamenGlobalVariables
module N = RamenName
module O = RamenOperation

(* Return the environment corresponding to the used envvars: *)
let env_of_envvars envvars =
  List.map (fun f ->
    (* To be backend independent, values must be symbolic *)
    let v =
      Printf.sprintf2 "(Sys.getenv_opt %S |> Nullable.of_option)"
        (f : N.field :> string) in
    (* FIXME: RecordField should take a tuple and a _path_ not a field
     * name *)
    E.RecordField (Env, f), v
  ) envvars

let env_of_params params =
  List.map (fun param ->
    let f = param.RamenTuple.ptyp.name in
    let v = CodeGen_OCaml.id_of_field_name ~tuple:Param f in
    (* FIXME: RecordField should take a tuple and a _path_ not a field
     * name *)
    E.RecordField (Param, f), v
  ) params

let env_of_globals globals_mod_name globals =
  List.map (fun g ->
    let v =
      assert (globals_mod_name <> "") ;
      globals_mod_name ^"."^ CodeGen_OCaml.id_of_global g in
    E.RecordField (Global, g.Globals.name), v
  ) globals

(* Returns all the bindings for accessing the env and param 'tuples': *)
let static_environments
    globals_mod_name params envvars globals =
  let init_env = E.RecordValue Env, "envs_"
  and init_param = E.RecordValue Param, "params_"
  and init_global = E.RecordValue Global, "globals_" in
  let env_env = init_env :: env_of_envvars envvars
  and param_env = init_param :: env_of_params params
  and global_state_env =
    init_global :: env_of_globals globals_mod_name globals in
  env_env, param_env, global_state_env

(* Returns all the bindings in global and group states: *)
let initial_environments op =
  let glob_env, loc_env =
    O.fold_expr ([], []) (fun _c _s (glo, loc as prev) e ->
      match e.E.text with
      | Stateful (g, _, _) ->
          let n = CodeGen_OCaml.name_of_state e in
          (match g with
          | E.GlobalState ->
              let v = CodeGen_OCaml.id_of_state GlobalState ^"."^ n in
              (E.State e.uniq_num, v) :: glo, loc
          | E.LocalState ->
              let v = CodeGen_OCaml.id_of_state LocalState ^"."^ n in
              glo, (E.State e.uniq_num, v) :: loc)
      | _ -> prev
    ) op in
  glob_env, loc_env
