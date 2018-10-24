-module(ms_prod_proxy).
-export([start_link/0,init/1,handle_call/3,handle_cast/2,stop/0]).
-export([get/3]).
-behaviour(gen_server).
-record(loopData, {msProdNodes, closestNode}).

start_link() ->
  {ok,[MsProdNodes]} = file:consult(ms_prod_nodes),
  LoopData = #loopData{msProdNodes = MsProdNodes},
  gen_server:start_link({local, ?MODULE}, ?MODULE, LoopData, []).

init(LoopData) ->

  process_flag(trap_exit, true),
  FunTime = fun(Node, Acc) ->
      case timer:tc(net_adm, ping, [Node]) of
        {Time, pong} -> Acc ++ [{Node, Time}];
        {_Time, pang} -> Acc
      end
  end,

  TimesList = lists:foldl(FunTime, [], LoopData#loopData.msProdNodes),
  SortedTimeList = lists:sort(fun({_, T1}, {_, T2}) -> T1 =< T2 end, TimesList),
  io:format("TimesList ~p~n", [SortedTimeList]),
  {ClosestNode, _} = lists:nth(1, SortedTimeList),
  io:format("closes node: ~p~n", [ClosestNode]),
  NewLoopData = LoopData#loopData{closestNode = ClosestNode},
  pg2:create(ms_prod),
  {ok, NewLoopData}.

stop() -> gen_server:cast(?MODULE, stop).

call(Msg) ->
  gen_server:call(?MODULE, Msg).

get(ProductId, CountryId, LanguageId) ->
  call({get, {ProductId, CountryId, LanguageId}}).

handle_call({get, {ProductId, CountryId, LanguageId}}, _From, LoopData) ->
  {reply, get_product(ProductId, CountryId, LanguageId, LoopData), LoopData}.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.

get_product(ProductId, CountryId, LanguageId, LoopData) ->
  ActiveNode = get_active_node(LoopData),
  try ms_prod:get(ActiveNode, ProductId, CountryId, LanguageId) of
    Response -> Response
  catch
    _:_ ->
      get_product(ProductId, CountryId, LanguageId, LoopData)
  end.

get_active_node(LoopData) ->
  ActiveNodes = lists:map(fun(P) -> node(P) end, pg2:get_members(ms_prod)),
  ClosestNode = LoopData#loopData.closestNode,
  case lists:member(ClosestNode, ActiveNodes) of
    true ->
      ClosestNode;
    false ->
      [Node|_] = ActiveNodes,
      Node
  end.