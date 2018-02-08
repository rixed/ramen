# We cannot start from rixed/ramen:dev since it would prevent squashing
FROM debian:stable-slim

# Do not install recommends:
RUN echo 'APT::Install-Recommends "0";' > \
      /etc/apt/apt.conf.d/no_recommends

# Install a few things
RUN apt-get --yes update && \
    DEBIAN_FRONTEND=noninteractive \
      apt-get --yes install \
        gcc \
				openssl \
				libblas-dev \
				liblapack-dev \
        libsqlite3-0 \
        collectd \
        fprobe

# Do not install recommends:
RUN echo 'APT::Install-Recommends "0";' > \
      /etc/apt/apt.conf.d/no_recommends

# Rebuild a new image from here
RUN echo "version 1.2.0~0"

# Install ramen
COPY ramen.1.2.0.deb /tmp
RUN dpkg -i /tmp/*.deb && rm /tmp/*.deb

# Environment
ENV RAMEN_PERSIST_DIR=/ramen

# cohttp/conduit requires /etc/protocols and /etc/services,
# which debian-slim does not provide:
COPY protocols services /etc/
COPY start /

# Command to be run:
WORKDIR /ramen
ENTRYPOINT ["/start"]

# Default arguments if not provided on `docker run` command line:
CMD ["--use-embedded-compiler --to-stderr"]

# TODO: where to store stuff
# CMD ["--directories etc"]

# TODO: better healthcheck
HEALTHCHECK CMD wget -O /dev/null http://localhost:29380/graph

# HTTP API:
EXPOSE 29380/TCP
# Collectd:
EXPOSE 25826/UDP
# Fprobe:
EXPOSE 2055/UDP

LABEL maintainer="rixed-docker@happyleptic.org"
