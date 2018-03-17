-module(ms_inv).
-export([start_link/0,init/1,stop/0]).
-export([get/2,add/3,remove/3]).
-export([handle_call/3,handle_cast/2,terminate/2]).
-behaviour(gen_server).


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_Args) -> {ok, _Args}.

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

handle_call({get, {ProductId, CountryId}}, _From, LoopData) ->
  {reply, get_inventory(ProductId, CountryId), LoopData};

handle_call({remove, {ProductId, CountryId, RemoveQuantity}}, _From, LoopData) ->

  {reply, remove_inventory(ProductId, CountryId, RemoveQuantity), LoopData};


handle_call({add, {ProductId, CountryId, AddQuantity}}, _From, LoopData) ->
  {reply, add_inventory(ProductId, CountryId, AddQuantity), LoopData}.

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.


get_inventory(ProductId, CountryId) ->
  case ms_db:read({ProductId,CountryId}) of
    {ok, {{ProductId, CountryId}, Quantity, Version}} ->
      {ok, {ProductId, CountryId, Quantity}};

    {error, Error} -> {error, Error}
  end.

remove_inventory(ProductId, CountryId, RemoveQuantity) ->

  case ms_db:read({ProductId,CountryId}) of
    {ok, {Key,0, _Version}} ->
      {error, not_available_quantity};

    {ok, {Key,Quantity, _}} ->
      {ok, {{ProductId, CountryId}, NewQuantity, _}} = ms_db:write(Key, Quantity - RemoveQuantity),
      {ok, {ProductId, CountryId, NewQuantity}};

    {error, Error} -> {error, Error}
  end.

add_inventory(ProductId, CountryId, AddQuantity) ->

  case ms_db:read({ProductId,CountryId}) of
    {ok, {Key,Quantity,_}} ->
      ms_db:write(Key, Quantity + AddQuantity);

    {error, Error} -> {error, Error}
  end.