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

get(ProductId, CountryId) ->
  call({get, {ProductId, CountryId}}).

remove(ProductId, CountryId, Quantity) ->
  call({remove,{ProductId, CountryId, Quantity}}).

add(ProductId, CountryId, Quantity) ->
  call({add,{ProductId, CountryId, Quantity}}).

stop() -> gen_server:cast(?MODULE, stop).

stop_node() -> gen_server:cast(?MODULE, stop_node).

handle_call({get, {ProductId, CountryId}}, _From, LoopData) ->
  {reply, get_inventory(ProductId, CountryId), LoopData};

handle_call({remove, {ProductId, CountryId, RemoveQuantity}}, _From, LoopData) ->
  {reply, remove_inventory(get_active(), ProductId, CountryId, RemoveQuantity), LoopData};


handle_call({add, {ProductId, CountryId, AddQuantity}}, _From, LoopData) ->
  {reply, add_inventory(get_active(), ProductId, CountryId, AddQuantity), LoopData}.

handle_cast(stop_node, LoopData) ->
  io:format("get_active(), ~p~n", [get_active()]),
  ms_inv:stop(get_active()),
  {noreply,LoopData};

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.

get_inventory(ProductId, CountryId) ->
  %%io:format("## active node: ~p~n", [get_active()]),
  ms_inv:get(get_active(), ProductId, CountryId).


remove_inventory(Node, ProductId, CountryId, RemoveQuantity) ->

  try ms_inv:remove(Node, ProductId, CountryId, RemoveQuantity) of
    Response -> Response
  catch
    _:_ ->
      io:format("#active nodes ~p~n", [get_active_nodes()]),
      LastNode = lists:last(get_active_nodes()),
      remove_inventory(LastNode, ProductId, CountryId, RemoveQuantity)
  end.


add_inventory(Node, ProductId, CountryId, AddQuantity) ->
  try ms_inv:add(Node, ProductId, CountryId, AddQuantity) of
      Response -> Response
  catch
    _:_ ->
      io:format("#active nodes ~p~n", [get_active_nodes()]),
      LastNode = lists:last(get_active_nodes()),
      add_inventory(LastNode, ProductId, CountryId, AddQuantity)
  end.

get_active() ->
  [Pid|_] = members(),
  node(Pid).

get_active_nodes() ->
  lists:map(fun(Pid) -> node(Pid) end, members()).

members() -> pg2:get_members(ms_inv).