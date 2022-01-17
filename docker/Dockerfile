FROM ubuntu:20.04

WORKDIR /work
RUN chmod 777 /work

ENV TZ=GMT
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update && apt-get install -y bash 
#zstd awscli liblz4-tool postgresql-client

COPY extract/bin  /usr/local/bin
COPY extract/lib  /usr/local/lib
COPY extract/lib64 /usr/local/lib64
COPY extract/runDeps .

RUN runDeps=$(cat runDeps) && \
    echo $runDeps && \
    apt-get update && apt-get install -y $runDeps


     

