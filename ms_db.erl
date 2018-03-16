-module(ms_db).
-behaviour(gen_server).
-export([write/2,read/1]).
-export([start_link/0,init/1,handle_call/3,handle_cast/2,stop/0]).
-export([read_from_local/1,read_from_remote/2,write_to_local/2,write_to_remote/3]).


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
  {ok, [{{nodes, Nodes},{dbname, DB}}]}.

call(Msg) ->
  gen_server:call(?MODULE, Msg).

call(Node, Msg) ->
  gen_server:call({?MODULE,Node}, Msg).


write(Key, Value) ->
  call({write, {Key, Value}}).

read(Key) ->
  call({read, Key}).

% remove
read_from_local(Key) ->
  call({read_from_local, Key}).

read_from_remote(Node, Key) ->
  call(Node, {read_from_remote, Key}).


write_to_local(Key, Value) ->
  call({write_to_local, {Key, Value}}).


write_to_remote(Node, Key, Value) ->
  call(Node, {write_to_local, {Key, Value}}).

%%% internal




handle_call({write, {Key, Value}}, _From, LoopData) ->
  {reply, {write, {Key, Value}}, LoopData};

handle_call({write_to_local, {Key, Value}}, _From, LoopData) ->
  {reply, write_to_local(Key, Value, LoopData), LoopData};

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

  WithoutErrorResponses = exclude_error_responses(Responses),
  %%io:format("##### WithoutErrorResponses: ~p~n", [WithoutErrorResponses]),

  case WithoutErrorResponses of

    [] 	  -> {error,not_found};

    [_|_] ->

      %%% 2. Sort
      Sorted = sort_inventories(WithoutErrorResponses),

      %%% 3 cast for tail of the sorted list??? node needed
      %%% get tail of the,

      {_Node,LatestIventory} = lists:nth(1,Sorted),

      %update_nodes_with_latest_inventory(Responses,LatestIventory,LoopData),
      %%io:format("### InventoryResponses WithoutErrorResponses: ~p~n", [InventoryResponses]),

      LatestIventory
  end.




read_from_local(Key, LoopData) ->
  %% process resulsts
  Result = dets:lookup(db_ref(LoopData), Key).


write_to_local(Key, Value, LoopData) ->
  ok = dets:insert(db_ref(LoopData), {Key, Value}),
  {ok, {Key,Value}}.




db_nodes([{{nodes, Nodes},_}]) -> Nodes.
db_ref([{_,{dbname, DB}}]) -> DB.

exclude_current_node(Nodes) ->

  Filter = fun(Node) -> Node /= node() end,
  lists:filter(Filter, Nodes).


exclude_error_responses(InventoryResponses) ->

  Filter = fun(ProductResponse) ->
    not_error_response(ProductResponse)
  end,

  lists:filter(Filter, InventoryResponses).


sort_inventories(InventoryResponses) ->

  ReverseSort = fun(A,B) ->
    {_,{ok,{_, _, _, T1}}} = A,
    {_,{ok,{_, _, _, T2}}} = B,
    T1 >= T2
  end,

  lists:sort(ReverseSort, InventoryResponses).


not_error_response({_,{error, _}}) -> false;
not_error_response({_,{ok,_}}) -> true.