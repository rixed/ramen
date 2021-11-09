FROM rixed/ramen-dev:latest

# Uninstall qt to *not* build RmAdmin
RUN DEBIAN_FRONTEND=noninteractive \
      apt-get purge --yes \
        qt5-qmake-bin \
        qt5-image-formats-plugins \
        qtbase5-dev

# Buidl and install ramen (via de deb package)
RUN ./configure && \
    make dep && \
    make deb

RUN dpkg -i ramen.*.deb

# Uninstall everything we can
RUN DEBIAN_FRONTEND=noninteractive \
    SUDO_FORCE_REMOVE=yes \
      apt-get purge --yes \
        bzip2 \
        ca-certificates \
        curl \
        emacs-nox \
        fakeroot \
        fontconfig-config \
        gringo \
        install-info \
        iproute2 \
        iputils-ping \
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
        xauth

RUN DEBIAN_FRONTEND=noninteractive \
      apt-get clean

RUN rm -rf /root/ocaml-lmdb /root/ramen /root/z3 \
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
