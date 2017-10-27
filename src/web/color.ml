let hexdigit_of n =
  let hd = "0123456789abcdef" in
  let n = n mod 256 in
  String.init 2 (function 0 -> hd.[(n lsr 4) land 15] 
                        | _ -> hd.[n land 15])

let nb_random_colors = 64

let random_colors =
  let rec loop n prev r g b =
    if n >= nb_random_colors then Array.of_list prev else
    let prev' =
      ("#"^ hexdigit_of r ^ hexdigit_of g ^ hexdigit_of b) :: prev in
    loop (n + 1) prev' (r + 133) (g + 39) (b + 247) in
  loop 0 [] 102 311 67

let hash s =
  let rec loop h i =
    if i >= String.length s then h
    else loop (h + (i+1) * int_of_char s.[i]) (i + 1) in
  loop 42 0

let random_of_string str =
  let i = hash str in
  random_colors.(i mod nb_random_colors)
