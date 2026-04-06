FROM ubuntu:22.04

# Устанавливаем зависимости для сборки
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    libreadline-dev \
    curl \
    unzip \
    git \
    libssl-dev \
    libz-dev \
    wget

# Устанавливаем Lua 5.3
RUN wget https://www.lua.org/ftp/lua-5.3.5.tar.gz && \
    tar -zxf lua-5.3.5.tar.gz && \
    cd lua-5.3.5 && \
    make linux test && \
    make install

# Клонируем и собираем wrk2 (ARM64-совместимая версия)
RUN git clone https://github.com/AmpereTravis/wrk2-aarch64.git wrk2 && \
    cd wrk2 && \
    make && \
    cp wrk /usr/local/bin/

WORKDIR /wrk2
ENTRYPOINT ["wrk"]
CMD ["--version"]
