-module(ms_inv_proxy).
-export([start_link/0,init/1]).
-export([get/2, add/3, remove/3,stop/0,stop_node/0,get_active/0,get_active_nodes/0]).
-export([handle_call/3,handle_cast/2]).
-behaviour(gen_server).

start_link() ->
  {ok,[Nodes]} = file:consult(nodes),
  gen_server:start_link({local, ?MODULE}, ?MODULE, {nodes, Nodes}, []).


init(Args) ->
  {nodes, Nodes} = Args,
  process_flag(trap_exit, true),
  PingNode = fun(N) -> net_adm:ping(N) end,
  lists:foreach(PingNode, Nodes),
  {ok, Args}.

call(Msg) ->
  gen_server:call(?MODULE, Msg).

get(ProductId, WarehouseId) ->
  call({get, {ProductId, WarehouseId}}).

remove(ProductId, WarehouseId, Quantity) ->
  call({remove,{ProductId, WarehouseId, Quantity}}).

add(ProductId, WarehouseId, Quantity) ->
  call({add,{ProductId, WarehouseId, Quantity}}).

stop() -> gen_server:cast(?MODULE, stop).

stop_node() -> gen_server:cast(?MODULE, stop_node).

handle_call({get, {ProductId, WarehouseId}}, _From, LoopData) ->
  {reply, get_inventory(ProductId, WarehouseId), LoopData};

handle_call({remove, {ProductId, WarehouseId, RemoveQuantity}}, _From, LoopData) ->
  {reply, remove_inventory(get_active(), ProductId, WarehouseId, RemoveQuantity), LoopData};


handle_call({add, {ProductId, WarehouseId, AddQuantity}}, _From, LoopData) ->
  {reply, add_inventory(get_active(), ProductId, WarehouseId, AddQuantity), LoopData}.

handle_cast(stop_node, LoopData) ->
  io:format("get_active(), ~p~n", [get_active()]),
  ms_inv:stop(get_active()),
  {noreply,LoopData};

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.

get_inventory(ProductId, WarehouseId) ->
  ms_inv:get(get_active(), ProductId, WarehouseId).


remove_inventory(Node, ProductId, WarehouseId, RemoveQuantity) ->

  try ms_inv:remove(Node, ProductId, WarehouseId, RemoveQuantity) of
    Response -> Response
  catch
    _:_ ->
      io:format("#active nodes ~p~n", [get_active_nodes()]),
      LastNode = lists:last(get_active_nodes()),
      remove_inventory(LastNode, ProductId, WarehouseId, RemoveQuantity)
  end.


add_inventory(Node, ProductId, WarehouseId, AddQuantity) ->
  try ms_inv:add(Node, ProductId, WarehouseId, AddQuantity) of
      Response -> Response
  catch
    _:_ ->
      io:format("#active nodes ~p~n", [get_active_nodes()]),
      LastNode = lists:last(get_active_nodes()),
      add_inventory(LastNode, ProductId, WarehouseId, AddQuantity)
  end.

get_active() ->
  [Pid|_] = members(),
  node(Pid).

get_active_nodes() ->
  lists:map(fun(Pid) -> node(Pid) end, members()).

members() -> pg2:get_members(ms_inv).