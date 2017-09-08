%%%----------------------------------------------------------------------
%%% File    : errdb_sup.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Errdb supervisor
%%% Created : 03 Jun. 2011
%%% License : http://www.opengoss.com/license
%%%
%%% Copyright (C) 2012, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb_sup).

-author('<ery.lee@gmail.com>').

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %rrdb cluster
    Pool = case application:get_env(pool_size) of
        {ok, schedulers} -> erlang:system_info(schedulers);
        {ok, Val} when is_integer(Val) -> Val;
        undefined -> erlang:system_info(schedulers)
    end,
    Errdbs = [worker(Id) || Id <- lists:seq(1, Pool)],

    %% Httpd config
    {ok, HttpdConf} = application:get_env(httpd),
    %% Httpd
    Httpd = {errdb_httpd, {errdb_httpd, start, [HttpdConf]},
           permanent, 10, worker, [errdb_httpd]},

    %% Socket config
    {ok, SocketConf} = application:get_env(socket),
    %% Socket
    Socket = {errdb_socket, {errdb_socket, start, [SocketConf]},
           permanent, 10, worker, [errdb_socket]},

    {ok, {{one_for_one, 10, 1000}, Errdbs ++ [Httpd, Socket]}}.

worker(Id) ->
    {errdb:name(Id), {errdb, start_link, [Id]},
       permanent, 5000, worker, [errdb]}.

