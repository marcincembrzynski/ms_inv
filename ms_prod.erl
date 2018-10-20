-module(ms_prod).
-behaviour(gen_server).
-record(loopData, {dbname, dbref, dbContentName, dbContentRef, msProdNodes}).

-export([start_link/0,init/1,stop/0,stop/1,handle_call/3,handle_cast/2]).
-export([get/4]).


get(Node, ProductId, CountryId, LanguageId) ->
  call(Node, {get, {ProductId, CountryId, LanguageId}}).

start_link() ->
  [DBName|_] = string:split(atom_to_list(node()),"@"),
  DBContentName = string:concat(DBName, "_content"),
  {ok,[ProductNodes]} = file:consult(ms_prod_nodes),
  LoopData = #loopData{dbname = DBName, dbContentName = DBContentName, msProdNodes = ProductNodes},
  gen_server:start_link({local, ?MODULE}, ?MODULE, LoopData, []).

init(LoopData) ->
  {ok, DB} = dets:open_file(LoopData#loopData.dbname, [{type, set}, {file, LoopData#loopData.dbname}]),
  {ok, DBContentRef} = dets:open_file(LoopData#loopData.dbContentName, [{type, set}, {file, LoopData#loopData.dbContentName}]),
  lists:foreach(fun(N) -> net_adm:ping(N) end, LoopData#loopData.msProdNodes),
  NewLoopData = LoopData#loopData{dbref = DB, dbContentRef = DBContentRef},
  pg2:create(?MODULE),
  pg2:join(?MODULE, self()),
  {ok, NewLoopData}.

stop() ->
  gen_server:cast(?MODULE, stop).

stop(Node) ->
  gen_server:cast({?MODULE,Node}, stop_sup).

call(Node, Msg) ->
  gen_server:call({?MODULE,Node}, Msg).


handle_call({get, {ProductId, CountryId, LanguageId}}, _From, LoopData) ->
  {reply, get_product(ProductId, CountryId, LanguageId, LoopData), LoopData}.

handle_cast({inv_update, {ProductId, CountryId, Quantity}}, LoopData) ->
  inv_update({ProductId, CountryId, Quantity}, LoopData),
  {noreply, LoopData};

handle_cast({price_update, {ProductId, CountryId, Price}}, LoopData) ->
  price_update({ProductId, CountryId, Price}, LoopData),
  {noreply, LoopData};

handle_cast({content_update, {ProductId, LanguageId, Content}}, LoopData) ->
  content_update({ProductId, LanguageId, Content}, LoopData),
  {noreply, LoopData};

handle_cast(stop, LoopData) ->
  pg2:leave(?MODULE, self()),
  {stop, normal, LoopData}.

inv_update({ProductId, CountryId, Quantity}, LoopData) ->
  io:format("### inv update ~n"),
  Result = get_product(ProductId, CountryId, LoopData),

  case Result of
    {error, not_found} -> {error, not_found};
    {ok, {Key, {inv, _ }, Price}} ->
      ok = dets:insert(LoopData#loopData.dbref, {Key, {inv, Quantity}, Price})
  end.


price_update({ProductId, CountryId, Price}, LoopData) ->
  io:format("### price update ~n"),
  Result = get_product(ProductId, CountryId, LoopData),

  case Result of
    {error, not_found} -> {error, not_found};
    {ok, {Key, {inv, Inventory}, {price, _}}} ->
      ok = dets:insert(LoopData#loopData.dbref, {Key, {inv, Inventory}, {price, Price}})
  end.

content_update({ProductId, LanguageId, Content}, LoopData) ->
  io:format("### content update ~n"),
  dets:insert(LoopData#loopData.dbContentRef, {{ProductId, LanguageId}, Content}).


get_product(ProductId, CountryId, LanguageId, LoopData) ->
  Content = get_content(ProductId, LanguageId, LoopData),
  Product = get_product(ProductId, CountryId, LoopData),
  case Product of
    {error, not_found} -> {error, not_found};
    {ok, {Key, Inventory, Price}} ->
      Content = get_content(ProductId, LanguageId, LoopData),
      {ok, Key, Inventory, Price, {content, Content}}
  end.

get_product(ProductId, CountryId, LoopData) ->
  Result = dets:lookup(LoopData#loopData.dbref, {ProductId, CountryId}),

  case Result of
    [] -> {error, not_found};
    [{Key, Inventory, Price}] -> {ok, {Key, Inventory, Price}}
  end.

get_content(ProductId, LanguageId, LoopData) ->
  dets:lookup(LoopData#loopData.dbContentRef, {ProductId, LanguageId}).






