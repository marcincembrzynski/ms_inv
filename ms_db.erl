-module(ms_db).
-behaviour(gen_server).
-export([write/2,read/1]).
-export([start_link/1,init/1,handle_call/3,handle_cast/2,stop/0,terminate/2]).
-record(loopData, {nodes, dbname, groupname, dbref}).

start_link(GroupName) ->
  [DBName|_] = string:split(atom_to_list(node()),"@"),
  {ok,[Nodes]} = file:consult(nodes),
  LoopData = #loopData{ nodes = Nodes, dbname = DBName, groupname = GroupName},
  gen_server:start_link({local, ?MODULE}, ?MODULE, LoopData, []).

init(LoopData) ->
  PingNode = fun(N) -> net_adm:ping(N) end,
  lists:foreach(PingNode, LoopData#loopData.nodes),
  {ok, DB} = dets:open_file(LoopData#loopData.dbname, [{type, set}, {file, LoopData#loopData.dbname}]),
  NewLoopData = LoopData#loopData{dbref = DB},
  process_flag(trap_exit, true),
  pg2:create(NewLoopData#loopData.groupname),
  io:format("# joining: ~p~n", [NewLoopData#loopData.groupname]),
  pg2:join(NewLoopData#loopData.groupname, self()),
  {ok, NewLoopData}.

stop() ->
  gen_server:cast(?MODULE, stop).

terminate(_Reason, LoopData) ->
  pg2:leave(LoopData#loopData.groupname, self()),
  dets:close(LoopData#loopData.dbref),
  ok.

call(Msg) -> gen_server:call(?MODULE, Msg).

write(Key, Value) ->
  call({write, {Key, Value}}).

read(Key) -> call({read, Key}).

handle_call({write, {Key, Value}}, _From, LoopData) ->
  {reply, write_to_all(Key, Value, LoopData), LoopData};

handle_call({read, Key}, _From, LoopData) ->
  {reply, read(Key,LoopData), LoopData}.

handle_cast(stop, LoopData) -> {stop, normal, LoopData};

handle_cast({write_cast, {Key, Value, Version}}, LoopData) ->
  ok = dets:insert(db_ref(LoopData), {Key, Value, Version}),
  {noreply,LoopData}.

read(Key, LoopData) ->
  Result = dets:lookup(db_ref(LoopData), Key),
  case Result of
    [] ->
      {error, not_found};
    Found ->
      [Latest|[]] = Found,
      {ok, Latest}
  end.

write_to_all(Key, Value, LoopData) ->

  Response = read(Key, LoopData),

  case Response of
    {error, _Error} ->
      NewVersion = 1;
    {ok, {Key, _, Version}} ->
      NewVersion = Version + 1
  end,

  ok = dets:insert(db_ref(LoopData), {Key, Value, NewVersion}),
  gen_server:abcast(db_nodes(LoopData), ms_db, {write_cast, {Key, Value, NewVersion}}),
  {ok, {Key, Value, NewVersion}}.

db_nodes(LoopData) ->
  Nodes = lists:map(fun(E) -> node(E) end, pg2:get_members(LoopData#loopData.groupname)),
  lists:filter(fun(Node) -> Node /= node() end, Nodes).

db_ref(LoopData) -> LoopData#loopData.dbref.
