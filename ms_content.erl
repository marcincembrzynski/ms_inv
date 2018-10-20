-module(ms_content).
-record(loopData, {msContentNodes, msProdNodes}).

-export([start_link/0, init/1,handle_call/3]).
-export([get/3,update/4]).

get(Node, ProductId, LanguageId) ->
  call(Node, {get, {ProductId, LanguageId}}).

update(Node, ProductId, LanguageId, Content) ->
  call(Node, {update, {ProductId, LanguageId, Content}}).

start_link() ->
  {ok,[ContentNodes]} = file:consult(ms_content_nodes),
  {ok,[ProductNodes]} = file:consult(ms_prod_nodes),
  LoopData=#loopData{msContentNodes = ContentNodes, msProdNodes = ProductNodes},
  gen_server:start_link({local, ?MODULE}, ?MODULE, LoopData, []).

init(LoopData) ->

  lists:foreach(fun(N) -> net_adm:ping(N) end, LoopData#loopData.msContentNodes),
  lists:foreach(fun(N) -> net_adm:ping(N) end, LoopData#loopData.msProdNodes),
  process_flag(trap_exit, true),
  pg2:create(?MODULE),
  pg2:join(?MODULE, self()),
  {ok, LoopData}.

call(Node, Msg) ->
  gen_server:call({?MODULE, Node}, Msg).

handle_call({get, {ProductId, LanguageId}}, _From, LoopData) ->
  {reply, get_content(ProductId, LanguageId) , LoopData};

handle_call({update, {ProductId, LanguageId, Content}}, _From, LoopData) ->
  {reply, update_content(ProductId, LanguageId, Content, LoopData), LoopData}.

get_content(ProductId, LanguageId) ->
  Response = ms_db:read({ProductId, LanguageId}),
  io:format("# response ~p~n", [Response]),
  case ms_db:read({ProductId, LanguageId}) of
    {ok, {{ProductId, CountryId}, Content, _Version}} ->
      {ok, {ProductId, CountryId, Content}};

    {error, Error} ->
      {error, Error}
  end.

update_content(ProductId, LanguageId, Content, LoopData) ->
  gen_server:abcast(LoopData#loopData.msProdNodes, ms_prod, {content_update, {ProductId, LanguageId, Content}}),
  ms_db:write({ProductId, LanguageId}, Content).