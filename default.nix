{ stdenv, darwin, ocaml, findlib, batteries, stdint, syslog, lacaml,
  cmdliner, ocaml_sqlite3, qtest, inotify, binocle, net_codecs,
  parsercombinator, pretty-printers-parsers, z3,
  liborc, cyrus_sasl, openssl, blas, liblapack }:

stdenv.mkDerivation rec {
  pname = "ramen";
  version = "3.3.0";

  src = builtins.fetchGit {
    url = "/home/rixed/share/src/ramen";
    #"https://github.com/rixed/ramen.git";
    ref = "nobundle";
  };

  buildInputs = [
    ocaml findlib batteries syslog stdint lacaml cmdliner
    ocaml_sqlite3 qtest binocle net_codecs parsercombinator
    pretty-printers-parsers liborc
  ] ++ stdenv.lib.optionals (!stdenv.isDarwin) [ inotify ];

  configureFlags = [
    "--with-liborc=${liborc.out}/lib"
    ("--with-runtime-libdirs=${cyrus_sasl.out}/lib,${openssl.out}/lib,${blas.out}/lib,${liblapack.out}/lib" +
     (if stdenv.isDarwin then ",${darwin.apple_sdk.frameworks.Accelerate}" else ""))
    "--with-runtime-incdirs=${liborc.out}/include"
  ];

  createFindlibDestdir = true;

  meta = with stdenv.lib; {
    homepage = https://github.com/rixed/ramen;
    description = "Event processor tailored for small-scale monitoring";
    platforms = ocaml.meta.platforms or [];
    maintainers = [ maintainers.rixed ];
  };
}
