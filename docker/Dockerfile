# vim: ft=dockerfile expandtab
FROM debian:11.1-slim

# Do not install recommends and remove unused deps automatically,
# then install some packages
RUN echo 'APT::Install-Recommends "0";' > \
      /etc/apt/apt.conf.d/no_recommends && \
    echo 'APT::Get::AutomaticRemove "1";' >> \
      /etc/apt/apt.conf.d/no_recommends && \
    DEBIAN_FRONTEND=noninteractive \
      apt-get --yes update && \
    DEBIAN_FRONTEND=noninteractive \
      apt-get --yes upgrade && \
    DEBIAN_FRONTEND=noninteractive \
      apt-get --yes install \
        aspcud \
        binutils \
        bzip2 \
        ca-certificates \
        cmake \
        coreutils \
        cucumber \
        curl \
        fakeroot \
        fuse \
        g++ \
        gcc \
        git \
        iproute2 \
        iputils-ping \
        imagemagick \
        less \
        libblas-dev \
        libffi-dev \
        liblapack-dev \
        liblmdb-dev \
        librdkafka-dev \
        libsasl2-dev \
        libsodium-dev \
        libsqlite3-0 \
        libssl-dev \
        libz-dev \
        m4 \
        make \
        net-tools \
        netbase \
        ocaml-nox \
        patch \
        php-cli \
        procps \
        rsync \
        ruby-rspec \
        socat \
        sqlite3 \
        ssh-client \
        unzip \
        wget

COPY install-opam.sh /tmp
RUN echo | /tmp/install-opam.sh

RUN /usr/local/bin/opam init --no-setup --disable-sandboxing && \
    /usr/local/bin/opam update --yes && \
    /usr/local/bin/opam switch install --yes ramen 4.07.1+flambda

# Environment
ENV OPAM_SWITCH_PREFIX=/root/.opam/ramen
ENV CAML_LD_LIBRARY_PATH=/root/.opam/ramen/lib/stublibs:/root/.opam/ramen/lib/ocaml/stublibs:/root/.opam/ramen/lib/ocaml
ENV OCAML_TOPLEVEL_PATH=/root/.opam/ramen/lib/toplevel
ENV MANPATH=:/root/.opam/ramen/man
ENV PATH=/root/.opam/ramen/bin:/root/.gem/ruby/2.5.0/bin/:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV RAMEN_LIBS=/root/ramen/bundle

RUN opam install --yes depext && \
    opam depext --noninteractive \
      conf-gsl conf-blas conf-lapack conf-pkg-config sqlite3 && \
    opam repo add --set-default ocalme \
      git://github.com/rixed/ocalme-opam-repository.git && \
    opam repo priority ocalme 1 && \
    opam install --yes inotify qtest ipaddr csv

# Now install z3 from source (as we need both executable and libs):
RUN cd /root && \
    git clone https://github.com/Z3Prover/z3.git && \
    cd z3 && \
    git checkout z3-4.8.10 && \
    DEBIAN_FRONTEND=noninteractive apt-get install --yes python && \
    python scripts/mk_make.py && \
    cd build && \
    make -j3 && \
    make install && \
    DEBIAN_FRONTEND=noninteractive apt-get purge --yes python

# Install all ramen dependencies using opam
RUN cd /root && \
    git clone https://github.com/rixed/ramen.git && \
    cd ramen && \
    git checkout v6.12.14 && \
    ./configure && \
    opam install --yes --deps-only .

# Perform a first build to get the submodule compiled already which save a lot
# of time later.
RUN cd /root/ramen && make dep && make -j3 && git clean -fdx

# Buidl and install ramen (via de deb package)
RUN cd /root/ramen && \
    ./configure && \
    make dep && \
    make deb && \
    dpkg -i ramen.*.deb

# Uninstall everything we can
RUN DEBIAN_FRONTEND=noninteractive \
    SUDO_FORCE_REMOVE=yes \
    apt-get purge --yes \
      bzip2 \
      ca-certificates \
      curl \
      fakeroot \
      fontconfig-config \
      gringo \
      install-info \
      iproute2 \
      iputils-ping \
      imagemagick \
      less \
      m4 \
      make \
      manpages \
      mount \
      net-tools \
      netbase \
      ocaml-nox \
      openssh-client \
      patch \
      perl \
      php-cli \
      procps \
      psmisc \
      rsync \
      ruby \
      shared-mime-info \
      socat \
      sqlite3 \
      strace \
      sudo \
      unzip \
      valgrind \
      vim-nox \
      wget \
      xauth && \
  DEBIAN_FRONTEND=noninteractive \
    apt-get clean

RUN strip /usr/bin/z3 && \
    rm -rf /root/ocaml-lmdb /root/ramen /root/z3 \
           /root/.cache /root/.opam \
           /var/cache/debconf /var/lib/apt/lists /var/lib/dpkg/info \
           /usr/local/bin /usr/share/doc

# Environment
ENV RAMEN_SMT_SOLVER="/usr/bin/z3 -t:90000 -smt2 %s"
ENV RAMEN_DIR=/ramen
ENV RAMEN_LIBS=/var/lib/ramen/bundle
# Override for multi-site deployment:
ENV RAMEN_CONFSERVER=127.0.0.1:29340

# Command to be run:
WORKDIR /ramen
COPY start /
ENTRYPOINT ["/start"]

# Collectd:
EXPOSE 25826/UDP
# Fprobe:
EXPOSE 2055/UDP
# Confserver (insecure, only local and VPN)
EXPOSE 29340/TCP
# Confserver (secure)
EXPOSE 29341/TCP
# Tunneld
EXPOSE 29329/TCP
# Httpd
EXPOSE 29380/TCP
# Graphite sink
EXPOSE 2003/TCP

LABEL maintainer="rixed@happyleptic.org"
