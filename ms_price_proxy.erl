-module(ms_price_proxy).
-export([start_link/0,init/1,handle_call/3,handle_cast/2]).
-export([get/2,update/4,stop/0]).

start_link() ->
  {ok,[PriceNodes]} = file:consult(ms_price_nodes),
  gen_server:start_link({local, ?MODULE}, ?MODULE, {price_nodes, PriceNodes}, []).


init(Args) ->
  {price_nodes, Nodes} = Args,
  process_flag(trap_exit, true),
  PingNode = fun(N) ->
    Ping = net_adm:ping(N),
    io:format("node ~p, ping result: ~p~n", [N,Ping])
  end,
  lists:foreach(PingNode, Nodes),
  pg2:create(ms_price),
  {ok, []}.


get(ProductId, CountryId) ->
  call({get, {ProductId, CountryId}}).

update(ProductId, CountryId, Value, Tax) ->
  call({update, {ProductId, CountryId, Value, Tax}}).

stop() -> gen_server:cast(?MODULE, stop).

call(Msg) ->
  gen_server:call(?MODULE, Msg).

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.

handle_call({get, {ProductId, CountryId}}, _From, LoopData) ->
  {reply, get_price(ProductId, CountryId), LoopData};

handle_call({update, {ProductId, CountryId, Value, Tax}}, _From, LoopData) ->
  {reply, update_price(ProductId, CountryId, Value, Tax), LoopData}.

get_price(ProductId, CountryId) ->
  Node = get_active(),
  try ms_price:get(Node, ProductId, CountryId) of
    Response -> Response
  catch
    _:_ -> get_price(ProductId, CountryId)
  end.

update_price(ProductId, CountryId, Value, Tax) ->
  Node = get_active(),
  try ms_price:update(Node, ProductId, CountryId, Value, Tax) of
    Response -> Response
  catch
    _:_ -> ms_price:update(Node, ProductId, CountryId, Value, Tax)
  end.

get_active() ->
  Pids = pg2:get_members(ms_price),
  Pid = lists:nth(1, Pids),
  node(Pid).