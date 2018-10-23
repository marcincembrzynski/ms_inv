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
  FunTime = fun(Node, Acc) -> Acc ++ [{Node, timer:tc(net_adm, ping, [Node])}] end,

  TimesList = lists:foldl(FunTime, [], LoopData#loopData.msProdNodes),

  SortedTimeList = lists:sort(fun({_, {T1,_}}, {_, {T2,_}}) -> T1 =< T2 end, TimesList),
  io:format("TimesList ~p~n", [SortedTimeList]),
  {ClosestNode, _} = lists:nth(1, SortedTimeList),
  io:format("closes node: ~p~n", [ClosestNode]),
  NewLoopData = LoopData#loopData{closestNode = ClosestNode},

  %%lists:foreach(PingNode, LoopData#loopData.msProdNodes),
  pg2:create(ms_prod),
  {ok, NewLoopData}.

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