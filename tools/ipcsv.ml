open Sys

let is_comment c = (List.hd c).[0] = '#'

let print_ip ip off =
  let i = Ipaddr.V6.of_string_exn ip in
  let a, b, _c, _d = Ipaddr.V6.to_int32 i in
  Printf.printf "((uint128_t)%#08lx%08lxULL << 64U) | %s" a b off

let f1 r columns =
  if not (is_comment columns) then (
  match columns with
    | [ips; country;_;_] ->
      if country <> "" && Str.string_match r ips 0 then (
        let start_ip = Str.matched_group 1 ips in
        let end_ip = Str.matched_group 2 ips in
        Printf.printf "  { ";
        print_ip start_ip "0x0000000000000000ULL";
        Printf.printf ", " ;
        print_ip end_ip "0xffffffffffffffffULL";
        Printf.printf ", %S },\n" country;
      )
    | c -> failwith "Wrong csv format")

let () =
  let file = open_in Sys.argv.(1) in
  let r = Str.regexp "^ *\\([0-9a-fA-F:]+\\)-\\([0-9a-fA-F:]+\\) *" in
  let csv = Csv.of_channel ~separator:',' ~strip:true ~backslash_escape:false file in
  let f = f1 r in
  Printf.printf "#include \"db.h\"\n" ;
  Printf.printf "struct db_entry_v6 db_v6_v2[] = {\n" ;
  Csv.iter ~f csv ;
  Printf.printf "};"
