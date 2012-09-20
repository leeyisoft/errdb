%%%----------------------------------------------------------------------
%%% File    : errdb_store.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : File Storage
%%% Created : 03 Apr. 2012
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.opengoss.com
%%%----------------------------------------------------------------------
-module(errdb_store).

-author('ery.lee@gmail.com').

-compile(export_all).

-include_lib("elog/include/elog.hrl").

-import(extbif, [zeropad/1]).

-import(lists, [concat/1, reverse/1]).

-import(proplists, [get_value/3]).

-behavior(gen_server).

-export([start_link/1,
		name/1,
        read/4, 
        write/3]).

-export([init/1, 
        handle_call/3, 
        priorities_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-define(DAY, 3600). %86400).

-define(RRDB_VER, <<"RRDB0003">>).

-define(OPEN_MODES, [binary, raw, {read_ahead, 1024}]).

%index: {ref, file}
%data: {fd, file}
-record(db, {name, index, data}).

-record(state, {id, name, buffer, dir, today, days = 3, db, hdbs=[], queue=[]}).

%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Id) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Id],
		[{spawn_opt, [{min_heap_size, 204800}]}]).

name(Id) ->
    list_to_atom("errdb_store_" ++ integer_to_list(Id)).

read(Pid, Key, Begin, End) ->
    {Time, Result} = timer:tc(fun do_read/4, [Pid, Key, Begin, End]),
    folsom_metrics:notify({'store.read_count', {inc, 1}}),
    folsom_metrics:notify({'store.read_time', {inc, Time div 1000}}),
    Result.

do_read(Pid, Key, Begin, End) ->
    case gen_server:call(Pid, {read_idx, Key, Begin, End}) of
    {ok, IdxList} ->
        ?INFO("do read: ~s", [Key]),
        ?INFO("~p", [IdxList]),
        DataList = 
        lists:map(fun({DataFile, Indices}) -> 
            case file:open(DataFile, [read | ?OPEN_MODES]) of
            {ok, Fd} ->
                case file:pread(Fd, Indices) of
                {ok, DataL} ->
                    Result=
                    lists:map(fun(Data) -> 
                        try binary_to_term(Data) of
                        Rows -> Rows
                        catch
                        _:Err ->
                            ?ERROR("~p: ~s ", [Err, DataFile]),
                            ?ERROR("~p", [Indices]),
                            []
                        end
                    end, DataL),
                    {ok, Result};
                    %{ok, [binary_to_term(Data) || Data <- DataL]};
                eof -> 
                    {ok, []};
                {error, Reason} -> 
                    ?ERROR("pread ~p error: ~p", [DataFile, Reason]),
                    {error, Reason}
                end;
            {error, eof} ->
                {ok, []};
            {error, Reason1} ->
                ?ERROR("open ~p error: ~p", [DataFile, Reason1]),
                {error, Reason1}
            end
        end, IdxList),
        {ok, lists:flatten([Rows || {ok, Rows} <- DataList])};
    {error, Reason} ->
        {error, Reason}
    end.

write(Pid, Key, Rows) when length(Rows) > 0 ->
    gen_server2:cast(Pid, {write, Key, Rows}).

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------

init([Id]) ->
	random:seed(now()),
    put(write_time, 0),
    put(write_count, 0),
    {ok, Opts} = application:get_env(store),
    Days = get_value(days, Opts, 3),
    Buffer = get_value(buffer, Opts, 20),
    Dir = get_value(dir, Opts, "var/data"),
	DbDir = lists:concat([Dir, "/", zeropad(Id)]),
    Now = extbif:timestamp(),
    Today = (Now div ?DAY),
    DB = open(db, DbDir, Now, Id),
    HDBS = [open(hdb, DbDir, ago(Now, I), Id)
                || I <- lists:seq(1, Days-1)],

    %schedule daily rotation
    sched_daily_rotate(),

    %flush queue to disk
    erlang:send_after(Id*3000, self(), flush_queue),
    
    ?INFO("~p is started.", [name(Id)]),
    {ok, #state{id = Id, name = name(Id), dir = DbDir, 
                buffer = Buffer+random:uniform(Buffer), 
                today = Today, days = Days,
                db = DB, hdbs = HDBS}}.

ago(Ts, I) ->
    Ts - (I * ?DAY).

open(Type, Dir, Ts, Id) ->
    Name = dbname(Id, Ts),
    IdxFile = idxfile(Dir, Name),
    DataFile = datafile(Dir, Name),
    case {Type, filelib:is_file(DataFile)} of
    {db, true} ->
        opendb(Name, IdxFile, DataFile);
    {db, false} ->
        filelib:ensure_dir(DataFile),
        opendb(Name, IdxFile, DataFile);
    {hdb, true}->
        opendb(Name, IdxFile, DataFile);
    {hdb, false} ->
        undefined
    end.

opendb(Name, IdxFile, DataFile) ->
    ?INFO("opendb ~s ", [Name]),
    IdxRef = list_to_atom("index_"++Name),
    {ok, IdxRef} = dets:open_file(IdxRef, [{file, IdxFile}, {type, bag}]),
    {ok, DataFd} = file:open(DataFile, [read, write, append | ?OPEN_MODES]),
	case file:read(DataFd, 8) of
    {ok, ?RRDB_VER} -> ok;
    eof -> file:write(DataFd, ?RRDB_VER)
    end,
    #db{name = list_to_atom(Name),
        index = {IdxRef, IdxFile},
        data = {DataFd, DataFile}}.

dbname(Id, Ts) ->
    zeropad(Id) ++ integer_to_list(Ts div ?DAY).

idxfile(Dir, Name) ->
    lists:concat([Dir, "/", Name, ".idx"]).

datafile(Dir, Name) ->
    lists:concat([Dir, "/", Name, ".data"]).

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({read_idx, Key, Begin, End}, _From, #state{db = DB, hdbs = HDBS} = State) ->
    %beginIdx is bigger than endidx
    {BeginIdx, EndIdx} = dayidx(Begin, End, State),
    ?INFO("read_idx: ~s ~p", [Key, {BeginIdx, EndIdx}]),
    DbInRange = lists:sublist([DB|HDBS], EndIdx, (BeginIdx-EndIdx+1)), 
    IdxList = [{DataFile, [Idx || {_K, Idx} <- dets:lookup(IdxRef, Key)]} 
                    || #db{index={IdxRef, _}, data={_, DataFile}} <- DbInRange],
    Reply = {ok, [ E || {_, Indices} = E <- IdxList, Indices =/= []]},
    {reply, Reply, State};

handle_call(Req, _From, State) ->
    {stop, {error, {badreq, Req}}, State}.

priorities_call({read_idx, _Key, _Begin, _End}, _From, _State) ->
    10;
priorities_call(_, _From, _State) ->
    0.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({write, Key, Rows}, #state{db=DB, buffer=Buffer, queue=Queue} = State) ->
    case length(Queue) >= Buffer of
    true ->
        flush_queue(DB, Queue),
        {noreply, State#state{queue=[]}};
    false ->
        NewQueue = [{Key, Rows}|Queue],
        {noreply, State#state{queue=NewQueue}}
    end;

handle_cast(Msg, State) ->
    {stop, {error, {badcast, Msg}}, State}.

handle_info(flush_queue, #state{db=DB, queue=Queue} = State) ->
    flush_queue(DB, Queue),
    emit_metrics(),
    erlang:send_after(1000, self(), flush_queue),
    {noreply, State#state{queue=[]}};

handle_info(rotate, #state{id = Id, dir = DbDir, db=OldDB, hdbs = HDBS, queue=Queue} = State) ->
    ?INFO("rotate at ~p", [{date(), time()}]),
    flush_queue(OldDB, Queue),
    %create new db
    Now = extbif:timestamp(),
    Today = (Now div ?DAY),
    NewDB = open(db, DbDir, Now, Id),
    %close oldest db
    close(lists:last(HDBS), deleted),
    NewHDBS = [OldDB | lists:sublist(HDBS, 1, length(HDBS)-1)],
    %rotation 
    sched_daily_rotate(),
    {noreply, State#state{today = Today, db = NewDB, hdbs = NewHDBS, queue=[]}};

handle_info(Info, State) ->
    {stop, {error, {badinfo, Info}}, State}.

terminate(_Reason, #state{db = DB, hdbs = HDBS }) ->
    [close(R, normal) || R <- [ DB | HDBS ]],
    ok.

%[d, d-1, d-2,...,d-n]
%EndIdx....BeginIdx
dayidx(Begin, End, #state{today=Today, days=Days}) ->
    BeginDay = Begin div ?DAY,
    EndDay = End div ?DAY,

    EndDelta = Today - EndDay,
    EndIdx=
    if
    EndDelta =< 0 -> 1;
    true -> EndDelta+1
    end,

    BeginDelta = Today - BeginDay,
    BeginIdx = 
    if
    BeginDelta =< 0 -> 1;
    BeginDelta > Days -> Days; %only two days
    true -> BeginDelta+1
    end,
    {BeginIdx, EndIdx}.

close(undefined, _) -> ok;
close(#db{index={IdxRef, IdxFile}, data={DataFd, DataFile}}, Type) ->
    dets:close(IdxRef),
    file:close(DataFd),
    case Type of
    deleted ->
        spawn(fun() -> 
            [begin 
                Res = file:delete(File),
                ?INFO("delete ~s: ~p", [File, Res]) 
             end || File <- [DataFile, IdxFile]]
        end);
    _ ->
        ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

flush_to_disk(DB, Queue) ->
    #db{index={IdxRef, _}, data={DataFd, _}} = DB,
    {ok, Pos} = file:position(DataFd, eof),
    {_LastPos, Indices, DataList} = 
    lists:foldl(fun({Key, Rows}, {PosAcc, IdxAcc, DataAcc})  -> 
        Data = term_to_binary(Rows, [compressed]),        
        Size = size(Data),
        Idx = {Key, {PosAcc, Size}},
        {PosAcc+Size, [Idx|IdxAcc], [Data|DataAcc]}
    end, {Pos, [], []}, Queue),
    Data = list_to_binary(lists:reverse(DataList)),
    %?INFO("Pos: ~p, LastPos: ~p, Size: ~p", [Pos, LastPos, size(Data)]),
    case file:write(DataFd, Data) of
    ok ->
        %?INFO("write indices: ~p", [Indices]),
        dets:insert(IdxRef, Indices);
    {error, Reason} ->
        ?ERROR("~p", [Reason])
    end.

sched_daily_rotate() ->
    Now = extbif:timestamp(),
    NextDay = (Now div ?DAY + 1) * ?DAY,
    %?INFO("will rotate at: ~p", [extbif:datetime(NextDay)]),
    Delta = (NextDay + 1 - Now) * 1000,
    erlang:send_after(Delta, self(), rotate).

flush_queue(_DB, []) ->
    ignore;
flush_queue(DB, Queue) ->
    {Time, _} = timer:tc(fun flush_to_disk/2, [DB, lists:reverse(Queue)]),
    errdb_misc:incr(write_count, 1),
    errdb_misc:incr(write_time, Time).

emit_metrics() ->
    Time = get(write_time) div 1000,
    Count = get(write_count),
    if
    Time > 0 ->
        folsom_metrics:notify({'store.write_time', {inc, Time}}),
        put(write_time, 0);
    true ->
        ignore
    end,
    if
    Count > 0 ->
        folsom_metrics:notify({'store.write_count', {inc, Count}}),
        put(write_count, 0);
    true ->
        ingore
    end.

