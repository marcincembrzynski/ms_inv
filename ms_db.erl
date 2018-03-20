-module(ms_db).
-behaviour(gen_server).
-export([write/2,read/1]).
-export([start_link/0,init/1,handle_call/3,handle_cast/2,stop/0]).
-export([read_from_local/1,read_from_remote/2,write_to_local/3,write_to_remote/4]).
-export([lock/0,unlock/1]).


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
  LockTable = ets:new(lockTable, []),
  ets:delete(LockTable, lock),
  process_flag(trap_exit, true),
  {ok, [{{nodes, Nodes},{dbname, DB},{lockTable, LockTable}}]}.

call(Msg) ->
  gen_server:call(?MODULE, Msg).

call(Node, Msg) ->
  gen_server:call({?MODULE,Node}, Msg).


write(Key, Value) ->
  call({write, {Key, Value}}).


lock() ->
  call(lock).

unlock(Ref) ->
  call({unlock, Ref}).


read(Key) ->
  call({read, Key}).

% remove
read_from_local(Key) ->
  call({read_from_local, Key}).

read_from_remote(Node, Key) ->
  call(Node, {read_from_remote, Key}).


write_to_local(Key, Value, Version) ->
  call({write_to_local, {Key, Value, Version}}).


write_to_remote(Node, Key, Value, Version) ->
  call(Node, {write_to_local, {Key, Value, Version}}).




handle_call(lock, _From, LoopData) ->
  {reply, lock(LoopData), LoopData};



handle_call({unlock, Ref}, _From, LoopData) ->
  {reply, unlock(Ref, LoopData), LoopData};


handle_call({write, {Key, Value}}, _From, LoopData) ->

  {reply, write_to_all(Key, Value, LoopData), LoopData};

handle_call({write_to_local, {Key, Value, Version}}, _From, LoopData) ->
  {reply, write_to_local(Key, Value, Version, LoopData), LoopData};

handle_call({write_to_remote, {Key, Value, Version}}, _From, LoopData) ->
  {reply, write_to_local(Key, Value, Version, LoopData), LoopData};

handle_call({read, Key}, _From, LoopData) ->
  {reply, read_from_all(Key,LoopData), LoopData};

handle_call({read_from_local, Key}, _From, LoopData) ->
  {reply, read_from_local(Key, LoopData), LoopData};

handle_call({read_from_remote, Key}, _From, LoopData) ->
  {reply, read_from_local(Key, LoopData), LoopData}.


handle_cast(stop, LoopData) -> {stop, normal, LoopData}.


read_from_all(Key,LoopData) ->

  Nodes = exclude_current_node(db_nodes(LoopData)),
  %io:format("# nodes, ~p~n",[Nodes]),

  GetFromNode = fun(Node, List) ->
    try ms_db:read_from_remote(Node, Key) of
      Response -> [{Node, Response}] ++ List
    catch
      _:_ -> List
    end
  end,

  %%% initialize list with the value from the current node
  List = [{node(), read_from_local(Key,LoopData)}],

  Responses = lists:foldl(GetFromNode, List, Nodes),
  %io:format("REsponses: ~p~n", [Responses]),


  WithoutErrorResponses = exclude_error_responses(Responses),
  %io:format("##### WithoutErrorResponses: ~p~n", [WithoutErrorResponses]),

  case WithoutErrorResponses of

    [] 	  -> {error,not_found};

    [_|_] ->

      %%% 2. Sort
      Sorted = sort_responses(WithoutErrorResponses),

      %%% 3 cast for tail of the sorted list??? node needed
      %%% get tail of the,

      {_Node,Latest} = lists:nth(1,Sorted),

      update_nodes_with_latest(Responses, Latest, LoopData),
      %%io:format("### InventoryResponses WithoutErrorResponses: ~p~n", [InventoryResponses]),

      Latest
  end.


update_nodes_with_latest(Responses, Latest, LoopData) ->

  {ok,{Key, Value, Version}} = Latest,
  %io:format("#latest inventories ~p~n", [LatestInventory]),

  % 1. Creates the lists of InventoryResponses not equal to LatestInventory

  Filter = fun({_,Other}) -> Latest /= Other end,
  NotCorrectInventories = lists:filter(Filter, Responses),

  % 2. Based on this creates list of nodes to update

  MapToNodes = fun({Node,_}) -> Node end,
  NodesToUpdate = lists:map(MapToNodes, NotCorrectInventories),
  %io:format("Nodes to update ~p~n", [NodesToUpdate]),

  % 3. Updates all the nodes from the list with the latest repository
  % what about the current node?
  % remove current node...

  UpdateNode = update_node_fun_node(Key, Value, Version, LoopData),
  %io:format("#nodesToupdate: ~p~n", [NodesToUpdate]),

  lists:foreach(UpdateNode, NodesToUpdate),

  ok.


update_node_fun_node(Key, Value, Version, LoopData) ->
  fun(Node) ->
    case (node() == Node) of
      true ->
        write_to_local(Key,Value,Version,LoopData);
      false ->
        try ms_db:write_to_remote(Node, Key,Value,Version) of
          _ -> ok
        catch
          _:_ ->
            error
        end
    end
  end.



write_to_all(Key, Value, LoopData) ->

  %io:format("#enter update all ~n"),
  Response = read_from_all(Key, LoopData),
  %%%io:format("ProductInventoryResponse ##, ~p~n", [ProductInventoryResponse]),
  case Response of

    {error, Error} -> {error, Error};

    {ok, Latest} ->

      %%io:format("### read from all ~p~n", [Latest]),
      {Key, _, Version} = Latest,
      %%NewQuantity = apply(Operation,[Quantity,UpdateQuantity]),
      NewVersion = Version + 1,
      UpdateNode = update_node_fun_node(Key, Value, NewVersion, LoopData),
      lists:foreach(UpdateNode,db_nodes(LoopData)),
      read_from_local(Key, LoopData)

  end.




read_from_local(Key, LoopData) ->
  %% process resulsts
  Result = dets:lookup(db_ref(LoopData), Key),
  case Result of
    [] -> {error, not_found};
    [Found] -> {ok, Found}
  end.


write_to_local(Key, Value, Version, LoopData) ->
  ok = dets:insert(db_ref(LoopData), {Key, Value, Version}),
  {ok, {Key, Value, Version}}.


db_nodes([{{nodes, Nodes} ,_ ,_}]) -> Nodes.
db_ref([{_,{dbname, DB},_}]) -> DB.
lock_table([{_,_,{lockTable, LockTable}}]) -> LockTable.

exclude_current_node(Nodes) ->

  Filter = fun(Node) -> Node /= node() end,
  lists:filter(Filter, Nodes).


exclude_error_responses(InventoryResponses) ->

  Filter = fun(ProductResponse) ->
    not_error_response(ProductResponse)
  end,

  lists:filter(Filter, InventoryResponses).


sort_responses(Responses) ->

  ReverseSort = fun(A,B) ->
    {_,{ok,{_, _, T1}}} = A,
    {_,{ok,{_, _, T2}}} = B,
    T1 >= T2
  end,

  lists:sort(ReverseSort, Responses).


not_error_response({_,{error, _}}) -> false;
not_error_response({_,{ok,_}}) -> true.

lock(LoopData) ->
  Result = ets:lookup(lock_table(LoopData), lock),
  case Result of
    [] ->
      Ref = erlang:make_ref(),
      ets:insert(lock_table(LoopData),{lock,Ref}),
      {ok,Ref};

    _ -> {error, locked}

  end.

unlock(Ref, LoopData) ->
  Result = ets:lookup(lock_table(LoopData), lock),
  case Result of
    [{lock, Ref}] ->
      ets:delete(lock_table(LoopData), lock);
    _ -> {error, locked}
  end .
