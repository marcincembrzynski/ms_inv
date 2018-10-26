-module(ms_price).
-export([stop/0,start_link/0,init/1,handle_call/3]).
-export([get/3,update/5]).
-record(loopData, {msPriceNodes, msProdNodes}).

start_link() ->
  {ok,[PriceNodes]} = file:consult(ms_price_nodes),
  {ok,[ProductNodes]} = file:consult(ms_prod_nodes),
  LoopData=#loopData{msPriceNodes = PriceNodes, msProdNodes = ProductNodes},
  gen_server:start_link({local, ?MODULE}, ?MODULE, LoopData, []).

init(LoopData) ->

  lists:foreach(fun(N) -> net_adm:ping(N) end, LoopData#loopData.msPriceNodes),
  lists:foreach(fun(N) -> net_adm:ping(N) end, LoopData#loopData.msProdNodes),
  process_flag(trap_exit, true),
  pg2:create(?MODULE),
  pg2:join(?MODULE, self()),
  {ok, LoopData}.

get(Node, ProductId, CountryId) ->
  call(Node, {get, {ProductId, CountryId}}).

update(Node, ProductId, CountryId, Value, Tax) ->
  call(Node, {update, {ProductId, CountryId, Value, Tax}}).

stop() ->
  gen_server:cast(?MODULE, stop).

call(Node, Msg) ->
  gen_server:call({?MODULE, Node}, Msg).

handle_call({get, {ProductId, CountryId}}, _From, LoopData) ->
  {reply, get_price(ProductId, CountryId) , LoopData};

handle_call({update, {ProductId, CountryId, Value, Tax}}, _From, LoopData) ->
  {reply, update_price(ProductId, CountryId, {Value, Tax}, LoopData), LoopData}.

get_price(ProductId, CountryId) ->
  Response = ms_db:read({ProductId, CountryId}),
  io:format("#### repsonse: ~p~n", [Response]),
  case Response of
    {ok, {{ProductId, CountryId}, Price, _Version}} ->
      {ok, {ProductId, CountryId, Price}};
    {error, Error} ->
      {error, Error}
  end.

update_price(ProductId, CountryId, Price, LoopData) ->
  PriceUpdate = {price_update, {ProductId, CountryId, Price}},
  gen_server:abcast(LoopData#loopData.msProdNodes, ms_prod, PriceUpdate),
  ms_db:write({ProductId, CountryId}, Price).
