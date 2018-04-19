-module(ms_db).
-behaviour(gen_server).
-export([write/2,read/1]).
-export([start_link/0,init/1,handle_call/3,handle_cast/2,stop/0,terminate/2]).
-export([read_from_remote/2,write_to_local/3,write_cast/4]).

start_link() ->
  [DBName|_] = string:split(atom_to_list(node()),"@"),
  {ok,[Nodes]} = file:consult(nodes),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [{{nodes, Nodes},{dbname, DBName}}], []).

stop() ->
  gen_server:cast(?MODULE, stop).

init([{{nodes, Nodes},{dbname, DBName}}]) ->
  PingNode = fun(N) -> net_adm:ping(N) end,
  lists:foreach(PingNode, Nodes),
  {ok, DB} = dets:open_file(DBName, [{type, set}, {file, DBName}]),
  process_flag(trap_exit, true),
  pg2:create(ms_db),
  pg2:join(ms_db, self()),
  {ok, [{dbname, DB}]}.

terminate(_Reason, DB) ->
  pg2:leave(ms_db, self()),
  dets:close(DB), ok.

call(Msg) ->
  gen_server:call(?MODULE, Msg).

call(Node, Msg) ->
  gen_server:call({?MODULE,Node}, Msg).

cast(Node, Msg) ->
  gen_server:cast({?MODULE, Node}, Msg).

write(Key, Value) ->
  call({write, {Key, Value}}).

read(Key) -> call({read, Key}).

read_from_remote(Node, Key) ->
  call(Node, {read_from_remote, Key}).

write_to_local(Key, Value, Version) ->
  call({write_to_local, {Key, Value, Version}}).

write_cast(Node, Key, Value, Version) ->
  cast(Node, {write_to_local, {Key, Value, Version}}).

handle_call({write, {Key, Value}}, _From, LoopData) ->
  {reply, write_to_all(Key, Value, LoopData), LoopData};

handle_call({write_to_local, {Key, Value, Version}}, _From, LoopData) ->
  {reply, write_to_local(Key, Value, Version, LoopData), LoopData};

handle_call({read, Key}, _From, LoopData) ->
  {reply, read_from_all(Key,LoopData), LoopData};

handle_call({read_from_local, Key}, _From, LoopData) ->
  {reply, read_from_local(Key, LoopData), LoopData};

handle_call({read_from_remote, Key}, _From, LoopData) ->
  {reply, read_from_local(Key, LoopData), LoopData}.

handle_cast(stop, LoopData) -> {stop, normal, LoopData};

handle_cast({write_to_local, {Key, Value, Version}}, LoopData) ->
  ok = dets:insert(db_ref(LoopData), {Key, Value, Version}),
  {noreply,LoopData}.

read_from_all(Key,LoopData) ->

  GetFromNode = fun(Node, List) ->
    case Node == node() of
      true ->
        [{node(), read_from_local(Key,LoopData)}] ++ List;
      false ->
        try ms_db:read_from_remote(Node, Key) of
          Response -> [{Node, Response}] ++ List
        catch
          _:_ -> List
        end
    end
  end,

  Responses = lists:foldl(GetFromNode,[], db_nodes()),

  WithoutErrorResponses = exclude_error_responses(Responses),

  case WithoutErrorResponses of

    [] 	  -> {error,not_found};

    [_|_] ->

      Sorted = sort_responses(WithoutErrorResponses),
      {_Node,Latest} = lists:nth(1,Sorted),

      update_nodes_with_latest(Responses, Latest, LoopData),
      Latest
  end.


update_nodes_with_latest(Responses, Latest, LoopData) ->

  {ok,{Key, Value, Version}} = Latest,

  % 1. Create the lists of responses not equal to Latest
  Filter = fun({_,Other}) -> Latest /= Other end,
  NotCorrectResponses = lists:filter(Filter, Responses),

  % 2. Create the list of nodes to update
  NodesToUpdate = lists:map(fun({Node,_}) -> Node end, NotCorrectResponses),

  % 3. Update all the nodes from the list with the latest
  UpdateNode = update_node_fun_node(Key, Value, Version, LoopData),
  lists:foreach(UpdateNode, NodesToUpdate),

  ok.

update_node_fun_node(Key, Value, Version, LoopData) ->
  fun(Node) ->
    case (node() == Node) of
      true ->
        write_to_local(Key,Value,Version,LoopData);
      false ->
        ms_db:write_cast(Node, Key,Value,Version)
    end
  end.

write_to_all(Key, Value, LoopData) ->

  Response = read_from_all(Key, LoopData),

  case Response of
    {error, Error} -> {error, Error};

    {ok, Latest} ->
      {Key, _, Version} = Latest,
      NewVersion = Version + 1,
      UpdateNode = update_node_fun_node(Key, Value, NewVersion, LoopData),
      lists:foreach(UpdateNode,db_nodes()),
      read_from_local(Key, LoopData)
  end.

read_from_local(Key, LoopData) ->
  Result = dets:lookup(db_ref(LoopData), Key),
  case Result of
    [] -> {error, not_found};
    [Found] -> {ok, Found}
  end.

write_to_local(Key, Value, Version, LoopData) ->
  ok = dets:insert(db_ref(LoopData), {Key, Value, Version}),
  {ok, {Key, Value, Version}}.

db_nodes() ->
  lists:map(fun(E) -> node(E) end, pg2:get_members(ms_db)).

db_ref([{dbname, DB}]) -> DB.

exclude_error_responses(Responses) ->
  Filter = fun(Response) -> not_error_response(Response) end,
  lists:filter(Filter, Responses).

sort_responses(Responses) ->
  ReverseSort = fun(A,B) ->
    {_,{ok,{_, _, T1}}} = A,
    {_,{ok,{_, _, T2}}} = B,
    T1 >= T2
  end,
  lists:sort(ReverseSort, Responses).

not_error_response({_,{error, _}}) -> false;
not_error_response({_,{ok,_}}) -> true.