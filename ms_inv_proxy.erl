-module(ms_inv_proxy).
-export([start_link/0,init/1]).
-export([get/2, add/3, remove/3,stop/0, status/0,stop_node/0,validate_operations/2]).
-export([handle_call/3,handle_cast/2]).
-behaviour(gen_server).
-record(loopData, {nodes, error_node}).

start_link() ->
  {ok,[Nodes]} = file:consult(nodes),
  gen_server:start_link({local, ?MODULE}, ?MODULE, {nodes, Nodes}, []).


init(Args) ->
  {nodes, Nodes} = Args,
  process_flag(trap_exit, true),
  PingNode = fun(N) ->
     io:format("ping node: ~p~n", [N]),
      Ping = net_adm:ping(N),
      io:format("ping result: ~p~n", [Ping])
  end,
  lists:foreach(PingNode, Nodes),
  pg2:create(ms_inv),
  LoopData = #loopData{nodes = Nodes},
  {ok, LoopData}.

call(Msg) ->
  gen_server:call(?MODULE, Msg).

get(ProductId, WarehouseId) ->
  call({get, {ProductId, WarehouseId}}).

validate_operations(ProductId, WarehouseId) ->
  call({validate_operations, {ProductId, WarehouseId}}).

remove(ProductId, WarehouseId, Quantity) ->
  call({remove,{ProductId, WarehouseId, Quantity}}).

status() ->
  call({status}).

add(ProductId, WarehouseId, Quantity) ->
  call({add,{ProductId, WarehouseId, Quantity}}).

stop() -> gen_server:cast(?MODULE, stop).

stop_node() -> gen_server:cast(?MODULE, stop_node).

handle_call({status}, _From, LoopData) ->
  Nodes = LoopData#loopData.nodes,
  NodesCount = length(Nodes),
  {reply, get_status(NodesCount, NodesCount, LoopData), LoopData};

handle_call({get, {ProductId, WarehouseId}}, _From, LoopData) ->
  {reply, get_inventory(ProductId, WarehouseId, LoopData), LoopData};

handle_call({validate_operations, {ProductId, WarehouseId}}, _From, LoopData) ->
  {reply, validate_operations(ProductId, WarehouseId, LoopData), LoopData};

handle_call({remove, {ProductId, WarehouseId, RemoveQuantity}}, _From, LoopData) ->
  {Response, NewLoopData} =  remove_inventory(get_active(LoopData), ProductId, WarehouseId, RemoveQuantity, LoopData),
  {reply, Response, NewLoopData};


handle_call({add, {ProductId, WarehouseId, AddQuantity}}, _From, LoopData) ->
  {Response, NewLoopData} = add_inventory(get_active(LoopData), ProductId, WarehouseId, AddQuantity, LoopData),
  {reply, Response, NewLoopData}.

handle_cast(stop_node, LoopData) ->
  ActiveNode = get_active(LoopData),
  io:format("stoping active node: ~p~n", [ActiveNode]),
  ms_inv:stop(ActiveNode),
  {noreply,LoopData};

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.

get_inventory(ProductId, WarehouseId, LoopData) ->
  ms_inv:get(get_active(LoopData), ProductId, WarehouseId).

get_status(0, _NodesCount, _LoopData) ->
  io:format("please try later ~n"),
  error;

get_status(N, NodesCount, LoopData) ->
  try ms_inv:status(get_active(LoopData)) of
    Response -> Response
  catch
    _:_ ->
      ActiveNodes = get_active_nodes(),
      io:format("ms_inv_proxy active nodes count: ~p~n", [length(ActiveNodes)]),
      io:format("ms_inv_proxy active nodes: ~p~n", [ActiveNodes]),
      io:format("ms_inv_proxy retry attempt number: ~p~n", [NodesCount - N + 1]),
      get_status(N - 1, NodesCount, LoopData)
  end.





remove_inventory(Node, ProductId, WarehouseId, RemoveQuantity, LoopData) ->


  try ms_inv:remove(Node, ProductId, WarehouseId, RemoveQuantity) of
    Response -> {Response, LoopData}
  catch
    _:_ ->
      {NewLoopData, ActiveNode} = handle_error(LoopData, Node, remove),
      {remove_inventory(ActiveNode, ProductId, WarehouseId, RemoveQuantity, NewLoopData), NewLoopData}
  end.


add_inventory(Node, ProductId, WarehouseId, AddQuantity, LoopData) ->
  try ms_inv:add(Node, ProductId, WarehouseId, AddQuantity) of
      Response -> {Response, LoopData}
  catch
    _:_ ->
      {NewLoopData, ActiveNode} = handle_error(LoopData, Node, add),
      {add_inventory(ActiveNode, ProductId, WarehouseId, AddQuantity, NewLoopData), NewLoopData}
  end.

handle_error(LoopData, Node, Operation) ->
  NewLoopData = LoopData#loopData{nodes = LoopData#loopData.nodes, error_node = Node},
  io:format("operation: ~p~n", [Operation]),
  io:format("#### error calling node: ~p~n", [Node]),
  io:format("error ms_inv_proxy #active nodes ~p~n", [get_active_nodes()]),
  ActiveNode = get_active(NewLoopData),
  io:format("calling node: ~p~n", [ActiveNode]),
  {NewLoopData, ActiveNode}.

get_active(LoopData) ->
  ErrorNode = LoopData#loopData.error_node,
  ActiveNodes = get_active_nodes(),

  case (ErrorNode /= undefined) of
    true ->
      Filtered = lists:filter(fun(Elem) -> (Elem /= ErrorNode) end, ActiveNodes),
      lists:nth(1, Filtered);
    false ->
      lists:nth(1, ActiveNodes)
  end.



get_active_nodes() ->
  lists:map(fun(Pid) -> node(Pid) end, members()).

members() -> pg2:get_members(ms_inv).

validate_operations(ProductId, WarehouseId, _LoopData) ->

  ActiveNodes = get_active_nodes(),
  GetOperationsOnNode = fun(Node, Acc) ->
    Acc ++ ms_inv:get_operations(Node, ProductId, WarehouseId)
  end,

  List = lists:foldl(GetOperationsOnNode, [], ActiveNodes),

  case List of
    [] -> no_operations;
    _  ->
      CalculateFun = fun(Elem, Acc) ->
        {_,{version, _}, {ref, _ }, _, Quantity, _} = Elem,
        Acc + Quantity
                     end,
      SortedList = lists:sort(operations_sort_fun(), List),

      {_, DuplicatesList} = lists:foldl(find_duplicates_fun(), {0,[]}, SortedList),

      DuplicatesQuantity = lists:foldl(CalculateFun, 0, DuplicatesList),


      {_,{version, _}, {ref, _ }, Start, _, _} = lists:nth(1, SortedList),
      {_,{version, _}, {ref, _ }, _, _, CurrentAvailable} = lists:last(SortedList),

      RealAvailable = lists:foldl(CalculateFun, Start, SortedList),
      Consistent = RealAvailable == CurrentAvailable,
      Balance = RealAvailable - CurrentAvailable,

      RebalanceDuplicates = lists:foldl(rebalance_fun(), {Balance, []}, DuplicatesList),

      {
        {consistent, Consistent},
        {real_available, RealAvailable}, {current_available, CurrentAvailable},
        {balance, Balance},
        {duplicates, DuplicatesList},
        {rebalance_duplicates, RebalanceDuplicates},
        {duplicates_quantity, DuplicatesQuantity}
      }

  end.




rebalance_fun() ->

  fun(Elem, {Balance, DuplicatesList}) ->

    {_,{version, _}, {ref, _}, _, Quantity, _} = Elem,

    case Balance - Quantity =< 0 of
      true ->
        {Balance - Quantity, DuplicatesList ++ [Elem]};
      false ->
        {Balance, DuplicatesList}
    end
  end.

operations_sort_fun() ->
  fun(Elem1, Elem2) ->
    {Key,{version, Version1}, {ref, _}, _, _, _} = Elem1,
    {Key,{version, Version2}, {ref, _}, _, _, _} = Elem2,
    Version1 =< Version2
  end.


find_duplicates_fun() ->
  fun(Elem, Acc) ->

    {PreviousVersion, Duplicates} = Acc,
    {_, {version, CurrentVersion}, {ref, _}, _, _, _} = Elem,

    case PreviousVersion == CurrentVersion of
      true ->
        {CurrentVersion, Duplicates ++ [Elem]};
      false ->
        {CurrentVersion, Duplicates}
    end

  end.
