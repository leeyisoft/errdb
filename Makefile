
all: deps
    ./rebar3 compile

deps:
    ./rebar3 get-deps

dist:
    rm -rf _build/prod
    ./rebar3 as prod tar

clean:
    ./rebar3 clean

