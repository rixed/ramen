{pkgs ? import <nixpkgs> {}}:

with pkgs;
let 
  protobuf = fetchurl {
    url = "https://github.com/google/protobuf/archive/v3.5.1.tar.gz";
    sha256 = "03vlb84djjkdks3g86hlvb7r0wgaqg2igf8p29rr0fg45qc2ar42";
  };
  gtest = fetchurl {
    url = "https://github.com/google/googletest/archive/release-1.8.0.tar.gz";
    sha256 = "1n5p1m2m3fjrjdj752lf92f9wq3pl5cbsfrb49jqbg52ghkz99jq";
  };
  lz4 = fetchurl {
    url = "https://github.com/lz4/lz4/archive/v1.7.5.tar.gz";
    sha256 = "0zkykqqjfa1q3ji0qmb1ml3l9063qqfh99agyj3cnb02cg6wm401";
  };
  zstd = fetchurl {
    url = "https://github.com/facebook/zstd/archive/v1.3.5.tar.gz";
    sha256 = "1sifbq18p0hc978g0pq8fymrlpzz1fcxqkbxfqk44z6v9jg5bqfn";
  };
  snappy = fetchurl {
    url = "https://github.com/google/snappy/archive/1.1.7.tar.gz";
    sha256 = "1m7rcdqzkys5lspj8jcsaah8w33zh28s771bw0ga2lgzfgl05yix";
  };
  zlib = fetchurl {
    url = "http://zlib.net/fossils/zlib-1.2.11.tar.gz";
    sha256 = "18dighcs333gsvajvvgqp8l4cx7h1x7yx9gd5xacnk80spyykrf3";
  };
in

stdenv.mkDerivation {
  name = "orc";

  src = fetchFromGitHub {
    owner = "rixed";
    repo = "orc";
    rev = "ee8c731718f628b3f0be5c30c505fb0fc8b2080c";
    sha256 = "0509bzdarzrdl9kv087hs62yyb0f22ljv8vd51hwl6mwl1x64693";
  };

  nativeBuildInputs = [cmake];
  buildInputs = [openssl cyrus_sasl];
  patches = [./protobuf.patch];

  buildPhase = ''
    cp ${protobuf} /protobuf.tar.gz
    cp ${gtest} /release.tar.gz
    cp ${lz4} /lz4.tar.gz
    cp ${zstd} /zstd.tar.gz
    cp ${snappy} /snappy.tar.gz
    cp ${zlib} /zlib.tar.gz
    CPPFLAGS='$(CFLAGS) $(CPPFLAGS)' cmake .. -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON -DBUILD_JAVA=OFF -DCMAKE_BUILD_TYPE=DEBUG -DCMAKE_INSTALL_PREFIX:PATH=$out
    make install
  '';

  postInstall = ''
    export LIBORCPATH=$out
  '';
}
