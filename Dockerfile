FROM ubuntu:22.04
RUN apt update -y && apt upgrade -y 
RUN apt install -y curl gcc
RUN apt install -y protobuf-compiler libprotobuf-dev
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"
RUN cargo install cargo-watch
RUN mkdir /app
COPY ./Cargo.toml /app/Cargo.toml
COPY ./Cargo.lock /app/Cargo.lock
COPY ./build.rs /app/build.rs
COPY ./src /app/src
COPY ./proto /app/proto
WORKDIR /app
#RUN cargo build
ENTRYPOINT ["cargo", "watch", "-x", "run --bin server"]
