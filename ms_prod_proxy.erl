-module(ms_prod_proxy).
-export([start_link/0,init/1,handle_call/3,handle_cast/2,stop/0]).
-export([get/3]).
-behaviour(gen_server).
-record(loopData, {msProdNodes, errorNode}).

start_link() ->
  {ok,[MsProdNodes]} = file:consult(ms_prod_nodes),
  LoopData = #loopData{msProdNodes = MsProdNodes},
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
  {reply, get_product(ProductId, CountryId, LanguageId), LoopData}.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.

get_product(ProductId, CountryId, LanguageId) ->
  Pid = pg2:get_closest_pid(ms_prod),
  Node = node(Pid),
  try ms_prod:get(Node, ProductId, CountryId, LanguageId) of
    Response -> Response
  catch
    _:_ -> get_product(ProductId, CountryId, LanguageId)
  end.