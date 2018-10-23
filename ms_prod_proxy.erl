-module(ms_prod_proxy).
-export([start_link/1,init/1,handle_call/3,handle_cast/2,stop/0]).
-export([get/3]).
-behaviour(gen_server).
-record(loopData, {msProdNodes, closestNode}).

start_link(ClosestNode) ->
  {ok,[MsProdNodes]} = file:consult(ms_prod_nodes),
  LoopData = #loopData{msProdNodes = MsProdNodes, closestNode = ClosestNode},
  gen_server:start_link({local, ?MODULE}, ?MODULE, LoopData, []).

init(LoopData) ->

  process_flag(trap_exit, true),
  PingNode = fun(N) ->
    io:format("ping node: ~p~n", [N]),
    Ping = net_adm:ping(N),
    io:format("ping result: ~p~n", [Ping])
  end,

  lists:foreach(PingNode, LoopData#loopData.msProdNodes),
  pg2:create(ms_prod),
  {ok, LoopData}.

stop() -> gen_server:cast(?MODULE, stop).

call(Msg) ->
  gen_server:call(?MODULE, Msg).

get(ProductId, CountryId, LanguageId) ->
  call({get, {ProductId, CountryId, LanguageId}}).

handle_call({get, {ProductId, CountryId, LanguageId}}, _From, LoopData) ->
  Node = LoopData#loopData.closestNode,
  {reply, get_product(Node, ProductId, CountryId, LanguageId), LoopData}.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.

get_product(Node, ProductId, CountryId, LanguageId) ->

  try ms_prod:get(Node, ProductId, CountryId, LanguageId) of
    Response -> Response
  catch
    _:_ ->
      [Pid|_] = pg2:get_members(ms_prod),
      NewNode = node(Pid),
      get_product(NewNode, ProductId, CountryId, LanguageId)
  end.