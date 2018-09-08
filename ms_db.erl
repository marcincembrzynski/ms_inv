-module(ms_db).
-behaviour(gen_server).
-export([write/2,read/1]).
-export([start_link/1,init/1,handle_call/3,handle_cast/2,stop/0,terminate/2]).
-export([read_from_remote/2,write_cast/4]).
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

call(Node, Msg) ->
  gen_server:call({?MODULE,Node}, Msg).

cast(Node, Msg) ->
  gen_server:cast({?MODULE, Node}, Msg).

write(Key, Value) ->
  call({write, {Key, Value}}).

read(Key) -> call({read, Key}).

read_from_remote(Node, Key) ->
  call(Node, {read_from_remote, Key}).

write_cast(Node, Key, Value, Version) ->
  cast(Node, {write_to_local, {Key, Value, Version}}).

handle_call({write, {Key, Value}}, _From, LoopData) ->
  {reply, write_to_all(Key, Value, LoopData), LoopData};

handle_call({write_to_local, {Key, Value, Version}}, _From, LoopData) ->
  {reply, write_to_local(Key, Value, Version, LoopData), LoopData};

handle_call({read, Key}, _From, LoopData) ->
  {reply, read(Key,LoopData), LoopData};

handle_call({read_from_local, Key}, _From, LoopData) ->
  {reply, read(Key, LoopData), LoopData}.


handle_cast(stop, LoopData) -> {stop, normal, LoopData};

handle_cast({write_to_local, {Key, Value, Version}}, LoopData) ->
  ok = dets:insert(db_ref(LoopData), {Key, Value, Version}),
  {noreply,LoopData}.


read(Key, LoopData) ->
  Result = dets:lookup(db_ref(LoopData), Key),
  %%io:format("### read, ~p~n", [Result]),
  case Result of
    [] ->
      {error, not_found};
    Found ->
      [Latest|[]] = Found,
      {ok, Latest}
  end.



update_node_fun_node(Key, Value, Version, LoopData) ->
  fun(Node) ->
    case (node() == Node) of
      true ->
        write_to_local(Key, Value, Version, LoopData);
      false ->
        write_cast(Node, Key, Value, Version)
    end
  end.

write_to_all(Key, Value, LoopData) ->

  Response = read(Key, LoopData),

  case Response of
    {error, _Error} ->
      NewVersion = 1;
    {ok, {Key, _, Version}} ->
      NewVersion = Version + 1
  end,

  UpdateNode = update_node_fun_node(Key, Value, NewVersion, LoopData),
  lists:foreach(UpdateNode,db_nodes(LoopData)),
  {ok, {Key, Value, NewVersion}}.


write_to_local(Key, Value, Version, LoopData) ->
  ok = dets:insert(db_ref(LoopData), {Key, Value, Version}),
  {ok, {Key, Value, Version}}.

db_nodes(LoopData) ->
  lists:map(fun(E) -> node(E) end, pg2:get_members(LoopData#loopData.groupname)).

db_ref(LoopData) -> LoopData#loopData.dbref.
