FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y curl build-essential software-properties-common ca-certificates gnupg && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    NODE_MAJOR=20 && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y python3 python3-venv python3-dev \
    libpq-dev supervisor \
    git nodejs postgresql-client vim nano screen htop \
    libcurl4-gnutls-dev libgnutls28-dev

# we install nginx with brotli support from ppa:ondrej/nginx-mainline
RUN add-apt-repository ppa:ondrej/nginx-mainline -y && \
	apt update -y && \
	apt install -y nginx libnginx-mod-http-brotli-static libnginx-mod-http-brotli-filter

RUN python3 -m venv /skyportal_env && \
    \
    bash -c "source /skyportal_env/bin/activate && \
    pip install --upgrade pip==22.2.2 wheel numpy"

# install snid
RUN git clone https://github.com/Theodlz/snid-install-ubuntu.git && \
    cd snid-install-ubuntu && chmod +x install.sh && bash ./install.sh

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ADD . /skyportal
WORKDIR /skyportal

RUN bash -c "\
    cp docker.yaml config.yaml && \
    \
    source /skyportal_env/bin/activate && \
    export NPM_CONFIG_LEGACY_PEER_DEPS=true && \
    make system_setup"
