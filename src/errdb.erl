%%%----------------------------------------------------------------------
%%% File    : errdb.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose :
%%% Created : 03 Apr. 2010
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb).

-author('ery.lee@gmail.com').

-include_lib("elog/include/elog.hrl").

-import(proplists, [get_value/2, get_value/3]).

-export([name/1,
        info/0,
        last/1,
        last/2,
        fetch/4,
        insert/3]).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1,
        handle_call/3,
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        priorities_info/2,
        terminate/2,
        code_change/3]).

-record(state, {dbtab, journal, store, cache, threshold = 1}).

-record(errdb, {key, first=0, last=0, rows=[]}). %fields = [],

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Id) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id],
        [{spawn_opt, [{min_heap_size, 4096}]}]).

name(Id) ->
    list_to_atom("errdb_" ++ integer_to_list(Id)).

info() ->
    Pids = chash_pg:get_pids(errdb),
    [gen_server2:call(Pid, info) || Pid <- Pids].

last(Key) when is_list(Key) ->
    Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:call(Pid, {last, Key}).

last(Key, Fields) when is_list(Key)
    and is_list(Fields) ->
    Pid = chash_pg:get_pid(?MODULE, Key),
    gen_server2:call(Pid, {last, Key, Fields}).

fetch(Key, Fields, Begin, End) when
    is_list(Key), is_list(Fields),
    is_integer(Begin), is_integer(End),
    Begin =< End ->
    Pid = chash_pg:get_pid(?MODULE, Key),
    case gen_server2:call(Pid, {fetch, Key}) of
    {ok, DataInMem, StorePid} ->
        case rpc:call(node(Pid), errdb_store, read, [StorePid, Key, Begin, End]) of
        {ok, DataInFile} ->
            Rows = [Row || {T, _} = Row <- DataInMem++DataInFile,
                                    T >= Begin, T =< End],
            {ok, lists:sort([{Time, values(Fields, Record)} || {Time, Record} <- Rows])};
        {error, Reason} ->
            {error, Reason}
        end;
    {error, Reason1} ->
        {error, Reason1}
    end.

%metrics: [{k, v}, {k, v}...]
insert(Key, Time, Metrics) when is_list(Key)
    and is_integer(Time) and is_list(Metrics) ->
    gen_server2:cast(chash_pg:get_pid(?MODULE, Key),
        {insert, Key, Time, Metrics}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Id]) ->
    put(fetch, 0),
    put(insert, 0),
    random:seed(now()),
    %process_flag(trap_exit, true),
    {ok, Opts} = application:get_env(rrdb),

    DbTab = ets:new(dbtab(Id), [set, protected,
                    named_table, {keypos, 2}]),

    %start store process
    {ok, Store} = errdb_store:start_link(Id),

    %start journal process
    {ok, Journal} = errdb_journal:start_link(Id),

    VNodes = get_value(vnodes, Opts, 40),
    chash_pg:create(errdb),
    chash_pg:join(errdb, self(), name(Id), VNodes),

    CacheSize = get_value(cache, Opts, 12),
    ?INFO("~p is started.~n ", [name(Id)]),

    erlang:send_after(1000, self(), cron),

    {ok, #state{dbtab = DbTab,
                store = Store,
                journal = Journal,
                cache = CacheSize}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(info, _From, #state{store=Store, journal=Journal} = State) ->
    Reply = [errdb_misc:pinfo(self()),
            errdb_misc:pinfo(Store),
            errdb_misc:pinfo(Journal)],
    {reply, Reply, State};

handle_call({last, Key}, _From, #state{dbtab = DbTab} = State) ->
    Reply =
    case ets:lookup(DbTab, Key) of
    [#errdb{rows=[{Time, Metrics}|_]}] ->
        {Fields, Values} = lists:unzip(Metrics),
        {ok, Time, Fields, Values};
    [] ->
        {error, notfound}
    end,
    {reply, Reply, State};

handle_call({last, Key, Fields}, _From, #state{dbtab=DbTab} = State) ->
    Reply =
    case ets:lookup(DbTab, Key) of
    [#errdb{rows=[{Time, Metrics}|_]}] ->
        {ok, Time, Fields, values(Fields, Metrics)};
    [] ->
        {error, notfound}
    end,
    {reply, Reply, State};

handle_call({fetch, Key}, _From, #state{store=Store, dbtab=DbTab} = State) ->
    errdb_misc:incr(fetch, 1),
    case ets:lookup(DbTab, Key) of
    [#errdb{rows=Rows}] ->
        {reply, {ok, Rows, Store}, State};
    [] ->
        {reply, {error, notfound}, State}
    end;

handle_call(Req, _From, State) ->
    ?ERROR("badreq: ~p", [Req]),
    {stop, {error, {bagreq, Req}}, State}.

priorities_call(info, _From, _State) ->
    10;
priorities_call({last, _}, _From, _State) ->
    10;
priorities_call({last, _, _}, _From, _State) ->
    10;
priorities_call({fetch, _}, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.

check_time(Last, Time) ->
    Time > Last.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({insert, Key, Time, Metrics}, #state{dbtab = DbTab,
    journal = Journal, store = Store, cache = CacheSize,
    threshold = Threshold} = State) ->
    errdb_misc:incr(insert, 1),
    Result =
    case ets:lookup(DbTab, Key) of
    [#errdb{last=Last, rows=Rows} = OldRecord] ->
        case check_time(Last, Time) of
        true ->
            case length(Rows) >= (CacheSize+Threshold) of
            true ->
                errdb_store:write(Store, Key, lists:reverse(Rows)),
                {ok, OldRecord#errdb{first = Time, last = Time,
                    rows = [{Time, Metrics}]}};
            false ->
                {ok, OldRecord#errdb{last = Time, rows = [{Time, Metrics}|Rows]}}
            end;
        false ->
            ?WARNING("key: ~p, badtime: time=~p =< last=~p", [Key, Time, Last]),
            {error, badtime}
        end;
    [] ->
        {ok, #errdb{key=Key, first=Time, last=Time, rows=[{Time, Metrics}]}}
    end,
    case Result of
    {ok, NewRecord} ->
        ets:insert(DbTab, NewRecord),
        errdb_journal:write(Journal, Key, Time, Metrics);
    {error, _Reason} ->
        ignore %here
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    ?ERROR("badmsg: ~p", [Msg]),
    {noreply, State}.

handle_info(cron, #state{cache = CacheSize} = State) ->
    emit_metrics(),
    Threshold = random:uniform(CacheSize),
    erlang:send_after(1000, self(), cron),
    {noreply, State#state{threshold = Threshold}};

handle_info(Info, State) ->
    ?ERROR("badinfo: ~p", [Info]),
    {noreply, State}.

priorities_info(cron, _State) ->
    11;
priorities_info(_, _) ->
    1.

emit_metrics() ->
    folsom_metrics:notify({'errdb.insert', {inc, get(insert)}}),
    put(insert, 0),
    folsom_metrics:notify({'errdb.fetch', {inc, get(fetch)}}),
    put(fetch, 0).

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

values(Fields, Metrics) ->
    [get_value(F, Metrics) || F <- Fields].

dbtab(Id) ->
    list_to_atom("errdb_" ++ integer_to_list(Id)).
