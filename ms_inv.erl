-module(ms_inv).
-export([start_link/0,init/1,stop/0]).
-export([get/2,get/3,add/3,add/4,remove/3,remove/4]).
-export([handle_call/3,handle_cast/2,terminate/2]).
-behaviour(gen_server).

start_link() ->
  {ok,[Nodes]} = file:consult(nodes),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [{nodes, Nodes}], []).

init(Args) ->
  [{nodes, Nodes}] = Args,
  lists:foreach(fun(N) -> net_adm:ping(N) end, Nodes),

  process_flag(trap_exit, true),
  pg2:create(?MODULE),
  pg2:join(?MODULE, self()),
  {ok, []}.

stop() -> gen_server:cast(?MODULE, stop).

terminate(_Reason, _) ->
  pg2:leave(?MODULE, self()),
  ok.

call(Msg) -> 
  gen_server:call(?MODULE, Msg).

call(Node, Msg) ->
  gen_server:call({?MODULE,Node}, Msg).

%% returns 
%% {ok, {ProductId, CountryId, Quantity}}
%% {error, not_found}
get(ProductId, CountryId) ->
  call({get, {ProductId, CountryId}}).


get(Node, ProductId, CountryId) ->
  call(Node, {get, {ProductId, CountryId}}).

%% subtracts given quantity from the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Quantity}}
%% {error, not_found}
%% {error, not_available_quantity}
remove(ProductId, CountryId, Quantity) ->
  call({remove,{ProductId, CountryId, Quantity}}).

remove(Node, ProductId, CountryId, Quantity) ->
  call(Node, {remove,{ProductId, CountryId, Quantity}}).


%% adds given quantity to the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Quantity}}
%% {error, not_found}
add(ProductId, WarehouseId, Quantity) -> 
  call({add,{ProductId, WarehouseId, Quantity}}).

add(Node, ProductId, WarehouseId, Quantity) ->
  call(Node, {add,{ProductId, WarehouseId, Quantity}}).

handle_call({get, {ProductId, WarehouseId}}, _From, LoopData) ->
  {reply, get_inventory(ProductId, WarehouseId), LoopData};

handle_call({remove, {ProductId, WarehouseId, RemoveQuantity}}, From, LoopData) ->
  {reply, remove_inventory(ProductId, WarehouseId, RemoveQuantity, From, LoopData), LoopData};

handle_call({add, {ProductId, WarehouseId, AddQuantity}}, From, LoopData) ->
  {reply, add_inventory(ProductId, WarehouseId, AddQuantity, From, LoopData), LoopData}.

handle_cast(stop, LoopData) ->
  pg2:leave(?MODULE, self()),
  {stop, normal, LoopData}.

get_inventory(ProductId, WarehouseId) ->

  case ms_db:read({ProductId, WarehouseId}) of
     {ok, {{ProductId, WarehouseId}, Quantity, _Version}} ->
        {ok, {ProductId, WarehouseId, Quantity}};

     {error, Error} ->
        {error, Error}
  end.

remove_inventory(ProductId, WarehouseId, RemoveQuantity, _From, _LoopData) ->

  case ms_db:read({ProductId, WarehouseId}) of
    {ok, {_Key,0, _Version}} ->
      {error, not_available_quantity};

    {ok, {Key,Quantity, _}} ->
      {ok, {{ProductId, WarehouseId}, NewQuantity, _}} = ms_db:write(Key, Quantity - RemoveQuantity),
      {ok, {ProductId, WarehouseId, NewQuantity}};

    {error, Error} ->
      {error, Error}
  end.

add_inventory(ProductId, WarehouseId, AddQuantity, _From, _LoopData) ->

  case ms_db:read({ProductId, WarehouseId}) of
    {ok, {Key,Quantity,_}} ->
      {ok, {{ProductId, WarehouseId}, NewQuantity, _}} = ms_db:write(Key, Quantity + AddQuantity),
      {ok, {ProductId, WarehouseId, NewQuantity}};

    {error, Error} ->
      {error, Error}
  end.