-module(ms_inv).
-export([start_link/0,init/1,stop/0,stop/1]).
-export([get/3,add/4,remove/4,status/1,get_operations/3]).
-export([handle_call/3,handle_cast/2,terminate/2]).
-record(loopData, {logref, msInvNodes, msProdNodes}).
-behaviour(gen_server).

start_link() ->
  {ok,[Nodes]} = file:consult(nodes),
  {ok,[ProductNodes]} = file:consult(ms_prod_nodes),
  LoopData=#loopData{msInvNodes = Nodes, msProdNodes = ProductNodes},
  gen_server:start_link({local, ?MODULE}, ?MODULE, LoopData, []).


init(LoopData) ->

  LogTableName = string:concat(lists:nth(1, string:tokens(atom_to_list(node()), "@")), "_ms_inv_log"),
  {ok, Log_Ref} = dets:open_file(LogTableName, [{type, bag}, {file, LogTableName}]),
  lists:foreach(fun(N) -> net_adm:ping(N) end, LoopData#loopData.msInvNodes),
  process_flag(trap_exit, true),
  pg2:create(?MODULE),
  pg2:join(?MODULE, self()),
  NewLoopData = LoopData#loopData{ logref = Log_Ref},
  {ok, NewLoopData}.


stop() ->
  gen_server:cast(?MODULE, stop).

stop(Node) ->
  gen_server:cast({?MODULE,Node}, stop_sup).

terminate(_Reason, LoopData) ->
  pg2:leave(?MODULE, self()),
  dets:close(LoopData#loopData.logref),
  ok.

call(Node, Msg) ->
  gen_server:call({?MODULE,Node}, Msg).

%% returns 
%% {ok, {ProductId, CountryId, Quantity}}
%% {error, not_found}
get(Node, ProductId, CountryId) ->
  call(Node, {get, {ProductId, CountryId}}).

%% subtracts given quantity from the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Quantity}}
%% {error, not_found}
%% {error, not_available_quantity}
remove(Node, ProductId, CountryId, Quantity) ->
  call(Node, {remove,{ProductId, CountryId, Quantity}}).


%% adds given quantity to the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Quantity}}
%% {error, not_found}
add(Node, ProductId, CountryId, Quantity) ->
  call(Node, {add,{ProductId, CountryId, Quantity}}).

status(Node) ->
  call(Node, {status}).

get_operations(Node, ProductId, CountryId) ->
  call(Node, {operations, ProductId, CountryId}).

handle_call({get, {ProductId, CountryId}}, _From, LoopData) ->
  {reply, get_inventory(ProductId, CountryId), LoopData};

handle_call({remove, {ProductId, CountryId, RemoveQuantity}}, _From, LoopData) ->
  {reply, remove_inventory(ProductId, CountryId, RemoveQuantity, LoopData), LoopData};

handle_call({add, {ProductId, CountryId, AddQuantity}}, _From, LoopData) ->
  {reply, add_inventory(ProductId, CountryId, AddQuantity, LoopData), LoopData};

handle_call({operations, ProductId, CountryId}, _From, LoopData) ->
  {reply, get_operations({ProductId, CountryId}, LoopData), LoopData};

handle_call({status}, _From, LoopData) ->
  {reply, ok, LoopData}.


handle_cast(stop, LoopData) ->
  pg2:leave(?MODULE, self()),
  {stop, normal, LoopData};

handle_cast(stop_sup, _LoopData) ->
  io:format("stopping node ~n"),
  pg2:leave(?MODULE, self()),
  {stop, normal, _LoopData}.

get_inventory(ProductId, CountryId) ->

  case ms_db:read({ProductId, CountryId}) of
     {ok, {{ProductId, CountryId}, Quantity, _Version}} ->
        {ok, {ProductId, CountryId, Quantity}};

     {error, Error} ->
        {error, Error}
  end.

remove_inventory(ProductId, CountryId, RemoveQuantity, LoopData) ->

  case ms_db:read({ProductId, CountryId}) of
      {ok, {_Key, Available , _Version}} ->
          case (Available - RemoveQuantity) >= 0 of
            false ->
              {error, not_available_quantity, {available, Available}, {requested, RemoveQuantity}};

            true ->
              add_inventory(ProductId, CountryId, RemoveQuantity * (-1), LoopData)
          end;

      {error, Error} ->
          {error, Error}
  end.

add_inventory(ProductId, CountryId, AddQuantity, LoopData) ->

  case ms_db:read({ProductId, CountryId}) of
    {ok, {Key, Available, _Version}} ->
      OperationRef = erlang:make_ref(),

      {ok, {{ProductId, CountryId}, NewQuantity, NewVersion}} = ms_db:write(Key, Available + AddQuantity),
      gen_server:abcast(LoopData#loopData.msProdNodes, ms_prod, {inv_update, {ProductId, CountryId, NewQuantity}}),
      log_operation({Key, {version, NewVersion}, {ref, OperationRef}, Available, AddQuantity, NewQuantity}, LoopData),
      {ok, {ProductId, CountryId, NewQuantity}, {ref, OperationRef}};

    {error, Error} ->
      {error, Error}
  end.

log_operation(Log, LoopData) ->
  dets:insert(LoopData#loopData.logref, Log).


get_operations(Key, LoopData) ->
  dets:lookup(LoopData#loopData.logref, Key).


