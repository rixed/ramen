# How to build?

First step is to configure:

```
./configure
```

From there, if you are using [opam](https://opam.ocaml.org) (which you probably
should), installing OCaml dependencies (displayed as missing in configure's
output) should be achieved with:

```
opam repo add --set-default ocalme https://github.com/rixed/ocalme-opam-repository.git
opam repo priority ocalme 1
opam install --deps-only ./opam
```

And then, to actually build:

```
make dep && make
```
