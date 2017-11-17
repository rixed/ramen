open Batteries
open Sqlite3
open RamenLog

let to_int64 x =
  let open Data in
  match x with
  | NONE | NULL | TEXT "" -> None
  | INT i -> Some i
  | FLOAT f -> Some (Int64.of_float f)
  | TEXT s | BLOB s -> Some (Int64.of_string s)

let to_int x =
  to_int64 x |> Option.map Int64.to_int

let to_float x =
  let open Data in
  match x with
  | NONE | NULL | TEXT "" -> None
  | INT i -> Some (Int64.to_float i)
  | FLOAT f -> Some f
  | TEXT s | BLOB s -> Some (float_of_string s)

let to_string x =
  let open Data in
  match x with
  | NONE | NULL | TEXT "" -> None
  | INT i -> Some (Int64.to_string i)
  | FLOAT f -> Some (string_of_float f)
  | TEXT s | BLOB s -> Some s

let of_int i = Data.INT (Int64.of_int i)
let of_int_or_null = function
  | Some i -> of_int i
  | None -> Data.NULL
let of_float f = Data.FLOAT f
let of_float_or_null = function
  | Some f -> of_float f
  | None -> Data.NULL

let required = function
  | None -> failwith "Missing required value"
  | Some x -> x

let default x = function
  | None -> x
  | Some y -> y

let must_be to_string expected actual =
  if expected <> actual then
    failwith ("Bad return code: expected "^ to_string expected ^
              " but got "^ to_string actual)

let must_be_ok actual =
  let open Rc in must_be to_string OK actual

let must_be_done actual =
  let open Rc in must_be to_string DONE actual

let rec step_all_fold stmt init f =
  let open Sqlite3 in
  match step stmt with
  | Rc.DONE -> init
  | Rc.ROW -> step_all_fold stmt (f init) f
  | rc -> failwith ("Unexpected Sqlite3 return code: "^ Rc.to_string rc)

let rec close ~max_tries db =
  let open Sqlite3 in
  let open Lwt in
  !logger.debug "Closing sqlite DB" ;
  if db_close db then return_unit else (
    if max_tries > 1 then (
      !logger.info "Cannot close sqlite3 DB, retrying" ;
      let%lwt () = Lwt_unix.sleep 0.5 in
      close ~max_tries:(max_tries - 1) db
    ) else (
      !logger.error "Cannot close sqlite3 DB, giving up!" ;
      return_unit (* so be it *)
    )
  )
