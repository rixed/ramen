FROM debian:stable-slim

# Do not install recommends:
RUN echo 'APT::Install-Recommends "0";' > \
      /etc/apt/apt.conf.d/no_recommends

# Install a few things
RUN apt-get --yes update && \
    apt-get --yes install \
      aspcud \
      ca-certificates \
      collectd \
      git \
      m4 \
      ocaml-nox \
      opam \
      rsync

# Comfigure opam
RUN opam init --no-setup && \
    opam repo add --priority=1 ocalme \
      git://github.com/rixed/ocalme-opam-repository.git

# Install as fast a compiler we can get
RUN opam switch install ramen --alias-of 4.05.0+flambda

# Install ramen
RUN opam depext --noninteractive \
      conf-gsl conf-openblas conf-pkg-config \
      plplot sqlite3 && \
    opam install ramen

# Minify the image (??)

# Environment
ENV RAMEN_PERSIST_DIR=/ramen \
    CAML_LD_LIBRARY_PATH=/root/.opam/ramen/lib/stublibs \
    PATH=/root/.opam/ramen/bin:/usr/sbin:/usr/bin:/sbin:/bin

# For owl not to bring down ramen workers:
ENV HOME=/tmp

# cohttp/conduit requires /etc/protocols and /etc/services,
# which debian-slim does not provide:
COPY protocols services /etc/
# TODO: also collectd configuration

# Command to be run:
WORKDIR /ramen
ENTRYPOINT ["/root/.opam/ramen/bin/ramen", "start"]

# Default arguments if not provided on `docker run` command line:
CMD ["--save-conf"]

# TODO: where to store stuff
# CMD ["--directories etc"]

# TODO: better healthcheck
HEALTHCHECK CMD wget -O /dev/null http://localhost:29380/graph

# HTTP API:
EXPOSE 29380/TCP
# Collectd:
EXPOSE 25826/UDP

LABEL maintainer="rixed-docker@happyleptic.org"
