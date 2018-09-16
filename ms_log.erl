-module(ms_log).
-export([start_link/1,stop/0,init/1,log/1,terminate/2,handle_cast/2,handle_call/3,get/2]).
-record(loopData, {logref}).
-behaviour(gen_server).

start_link(LogName) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [{logName, LogName}], []).


init(Args) ->
  [{logName, LogName}] = Args,
  LogTableName = string:concat(lists:nth(1, string:tokens(atom_to_list(node()), "@")), LogName),
  {ok, Log_Ref} = dets:open_file(LogTableName, [{type, bag}, {file, LogTableName}]),
  process_flag(trap_exit, true),
  LoopData = #loopData{ logref = Log_Ref},
  {ok, LoopData}.

stop() ->
  gen_server:cast(?MODULE, stop).

terminate(_Reason, LoopData) ->
  dets:close(LoopData#loopData.logref),
  ok.

log(Log) ->
  gen_server:cast(?MODULE, {insert, Log}).


get(Node, Key) ->
  gen_server:call({?MODULE, Node}, {get, Key}).

handle_call({get, Key}, _From, LoopData) ->
  Result = dets:lookup(LoopData#loopData.logref, Key),
  {reply, Result, LoopData}.

handle_cast({insert, Log}, LoopData) ->
  ok = dets:insert(LoopData#loopData.logref, Log),
  {noreply,LoopData};

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.





