-module(ms_inv).
-export([start_link/0,init/1,stop/0]).
-export([get/2,add/3,remove/3]).
-export([handle_call/3,handle_cast/2,terminate/2,logs/0]).
-behaviour(gen_server).


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_Args) ->
  process_flag(trap_exit, true),
  LogTable = ets:new(logTable, []),
  {ok, [LogTable]}.

stop() -> gen_server:cast(?MODULE, stop).

terminate(_Reason, DB) ->
  dets:close(DB),
  ok.

call(Msg) -> 
  gen_server:call(?MODULE, Msg).

%% returns 
%% {ok, {ProductId, CountryId, Quantity, Version}}
%% {error, not_found}
get(ProductId, CountryId) -> 
  call({get, {ProductId, CountryId}}).


%% subtracts given quantity from the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Quantity}}
%% {error, not_found}
%% {error, not_available_quantity}
remove(ProductId, CountryId, Quantity) ->
  call({remove,{ProductId, CountryId, Quantity}}).


%% adds given quantity to the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Quantity}}
%% {error, not_found}
add(ProductId, CountryId, Quantity) -> 
  call({add,{ProductId, CountryId, Quantity}}).


logs() ->
  call(logs).

handle_call({get, {ProductId, CountryId}}, _From, LoopData) ->
  {reply, get_inventory(ProductId, CountryId), LoopData};

handle_call({remove, {ProductId, CountryId, RemoveQuantity}}, From, LoopData) ->

  {reply, remove_inventory(ProductId, CountryId, RemoveQuantity, From, LoopData), LoopData};


handle_call({add, {ProductId, CountryId, AddQuantity}}, From, LoopData) ->
  {reply, add_inventory(ProductId, CountryId, AddQuantity, From, LoopData), LoopData};


handle_call(logs, _From, LoopData) ->
  {reply, logs(LoopData), LoopData}.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.


get_inventory(ProductId, CountryId) ->

  case ms_db:read({ProductId,CountryId}) of
     {ok, {{ProductId, CountryId}, Quantity, _Version}} ->
        {ok, {ProductId, CountryId, Quantity}};

     {error, Error} ->
        {error, Error}
  end.



remove_inventory(ProductId, CountryId, RemoveQuantity, From, LoopData) ->

  Response = ms_db:read({ProductId,CountryId}),
  {ok, {_Key, Value, _Version}} = Response,
  log_insert({'-', br, Value, From}, LoopData),
  %%io:format("- br: ~p~n", [Value]),

  case Response of
    {ok, {_Key,0, _Version}} ->
      {error, not_available_quantity};

    {ok, {Key,Quantity, _}} ->
      {ok, {{ProductId, CountryId}, NewQuantity, _}} = ms_db:write(Key, Quantity - RemoveQuantity),
      %io:format("- ar: ~p~n", [NewQuantity]),
      log_insert({'-', ar, NewQuantity, From}, LoopData),
      {ok, {ProductId, CountryId, NewQuantity}};

    {error, Error} ->
      {error, Error}
  end.



add_inventory(ProductId, CountryId, AddQuantity, From, LoopData) ->

  Response = ms_db:read({ProductId,CountryId}),
  {ok, {_Key, Value, _Version}} = Response,
  log_insert({'+', ba, Value, From}, LoopData),
  %io:format("+ ba: ~p~n", [Value]),

  case Response of
    {ok, {Key,Quantity,_}} ->
      {ok, {{ProductId, CountryId}, NewQuantity, _}} = ms_db:write(Key, Quantity + AddQuantity),
      %io:format("+ aa: ~p~n", [NewQuantity]),
      log_insert({'+', aa, NewQuantity, From}, LoopData),
      {ok, {ProductId, CountryId, NewQuantity}};

    {error, Error} ->
      {error, Error}
  end.


log_insert(Value, LoopData) ->
  %%io:format("loopdata ~p~n", [LoopData]),
  [LogTable] = LoopData,
   ets:insert(LogTable, {erlang:timestamp(), Value}).

logs(LoopData) ->
  [LogTable] = LoopData,
  ets:tab2file(LogTable, "logs.txt").
