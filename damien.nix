{pkgs ? import <nixpkgs> {}}:

let
  ocamlPackages = pkgs.ocamlPackages;
  batteries =
    pkgs.stdenv.mkDerivation {
      name = "batteries-master";
      src = pkgs.fetchFromGitHub {
        owner = "ocaml-batteries-team";
        repo = "batteries-included";
        rev = "43068b3d3eea4354f8bcef3d7a042e7138e15edc";
        sha256 = "1a868zmkdncqyq18a9x9cwha6w57hwbhwbh8i73w31a3p7f9zznm";
      };
      buildInputs = with pkgs; [
        ocamlPackages.ocaml
        ocamlPackages.findlib
        ocamlPackages.ocamlbuild
        ncurses
      ];
      propagatedBuildInputs = [ ocamlPackages.num ];
      createFindlibDestdir = true;
      meta = {
        description = "OCaml Batteries Included (master branch)";
      };
    };
  pfds = with pkgs; with ocamlPackages; stdenv.mkDerivation {
    name = "ocaml-pfds";

    src = builtins.fetchGit {
      url = "https://github.com/rixed/ocaml-pfds";
      ref = "refs/tags/v0.4";
    };

    buildInputs = [ ocaml findlib benchmark ];

    createFindlibDestdir = true;
  };
  portia = with pkgs; with ocamlPackages; stdenv.mkDerivation {
    name = "portia";

    src = builtins.fetchGit {
      url = "https://github.com/rixed/portia";
      ref = "refs/tags/v1.3";
    };

    buildInputs = [ocaml findlib batteries];

    createFindlibDestdir = true;
  };
  parsercombinator = with pkgs; with ocamlPackages; stdenv.mkDerivation {
    name = "ocaml-parsercombinator";

    src = builtins.fetchGit {
      url = "https://github.com/rixed/ocaml-parsercombinator";
      ref = "refs/tags/v1.0";
    };

    buildInputs = [ ocaml findlib batteries lwt_ppx portia num ];

    createFindlibDestdir = true;
    };
  ppp = with pkgs; with ocamlPackages; stdenv.mkDerivation {
    name = "ocaml-pretty-printers-parsers";

    src = builtins.fetchGit {
      url = "https://github.com/rixed/ocaml-pretty-printers-parsers";
      ref = "v2.8.2";
    };

    buildInputs = [ocaml findlib ppx_tools stdint];

    createFindlibDestdir = true;
  };
  binocle = with pkgs; with ocamlPackages; stdenv.mkDerivation {
    name = "ocaml-binocle";

    src = builtins.fetchGit {
      url = "https://github.com/rixed/binocle";
      ref = "v0.11.0";
    };

    buildInputs = [ocaml findlib batteries stdint ppp];

    createFindlibDestdir = true;
  };
  net-codecs = with pkgs; with ocamlPackages; stdenv.mkDerivation {
    name = "ocaml-net_codecs";

    src = builtins.fetchGit {
      url = "https://github.com/rixed/ocalme-net-codecs";
      ref = "v1.1";
    };

    buildInputs = [ ocaml findlib batteries parsercombinator ];

    createFindlibDestdir = true;
  };
  ocaml_lmdb = with pkgs; with ocamlPackages; buildDunePackage rec {
    pname = "lmdb";
    version = "0.3";

    src = builtins.fetchGit {
      url = "https://github.com/Drup/ocaml-lmdb";
      ref = "75bc99d29e9fac5fbb96d70969aa1cdf1ea593fc";
    };

    buildInputs = [ pkgconfig lmdb ocamlPackages.bigstringaf ];
  };
  qcheck = with pkgs; with ocamlPackages; stdenv.mkDerivation {
    name = "ocaml${ocaml.version}-qcheck-0.13";
    pname = "qcheck";
    version = "0.13";

    src = builtins.fetchGit {
      url = "https://github.com/c-cube/qcheck";
      ref = "refs/tags/0.13";
    };

    buildInputs = [ ocaml findlib ocamlbuild ounit dune alcotest opaline];

    configurePhase = ''
    '';

    buildPhase= ''
      make
    '';

    installPhase = ''
      ${opaline}/bin/opaline -prefix $out -libdir $OCAMLFIND_DESTDIR
    '';

    createFindlibDestdir = true;

  };
  dessser = with pkgs; with ocamlPackages; stdenv.mkDerivation {
    name = "ocaml-dessser";

    src = builtins.fetchGit {
      url = "https://github.com/rixed/dessser/";
      ref = "refs/tags/v0.1.2";
    };

    buildInputs = [ ocaml findlib batteries stdint parsercombinator qcheck ounit cmdliner ];

    createFindlibDestdir = true;
  };
  orc = import ./liborc.nix {
    inherit pkgs;
  };
  liblapack = pkgs.liblapack.override {shared=true;};
  lacaml = ocamlPackages.lacaml.override {liblapack=liblapack;};
in

with pkgs; stdenv.mkDerivation {
  name = "ramen";

  src = builtins.fetchGit {
    url = ./.;
  };

  hostname = "nix-ramen";

  buildPhase = ''
    make bundle
  '';

  configurePhase = ''
    LIBORCPATH=${orc} ./configure --prefix=$out
  '';

  buildInputs = with ocamlPackages; [
    ocaml batteries stdint syslog lacaml cmdliner ocaml_sqlite3 zmq czmq cyrus_sasl openssl lmdb gfortran /* needed to run ramen */
    qtest findlib z3 parsercombinator ppp binocle net-codecs pfds ocaml_lmdb dessser sodium kafka bigstringaf nettools orc
  ] ++ stdenv.lib.optionals (!stdenv.isDarwin) [ inotify ];


  createFindlibDestdir = true;

  meta = with stdenv.lib; {
    homepage = https://github.com/rixed/ramen;
    description = "Event processor tailored for human-scale monitoring";
    platforms = platforms.all;
    maintainers = [ maintainers.rixed ];
  };
}
