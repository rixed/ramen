{ stdenv, fetchFromGitHub, ocaml, findlib, batteries, stdint, syslog, lacaml,
  cmdliner, ocaml_sqlite3, qtest, inotify, binocle, net_codecs,
  parsercombinator, pretty-printers-parsers, z3 }:

stdenv.mkDerivation rec {
  pname = "ramen";
  version = "3.3.0";

  src = fetchFromGitHub {
    owner = "rixed";
    repo = "ramen";
    rev = "v${version}";
    sha256 = "17mx2fqsp8xlhx118rm2607fncyf91n27szw648akhlxr2il6n9z";
  };

  buildInputs = [
    ocaml findlib batteries stdint syslog lacaml cmdliner ocaml_sqlite3
    qtest binocle net_codecs parsercombinator pretty-printers-parsers
  ] ++ stdenv.lib.optionals (!stdenv.isDarwin) [ inotify ];

  createFindlibDestdir = true;

  meta = with stdenv.lib; {
    homepage = https://github.com/rixed/ramen;
    description = "Event processor tailored for human-scale monitoring";
    platforms = ocaml.meta.platforms or [];
    maintainers = [ maintainers.rixed ];
  };
}
