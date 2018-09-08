-module(ms_inv).
-export([start_link/0,start_link/1,init/1,stop/0,stop/1]).
-export([get/3,add/4,remove/4,status/1,get_operations/3]).
-export([handle_call/3,handle_cast/2,terminate/2]).
-record(loopData, {logref}).
-behaviour(gen_server).

start_link() ->
  {ok,[Nodes]} = file:consult(nodes),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [{nodes, Nodes}], []).

start_link(Node) ->
  {ok,[Nodes]} = file:consult(nodes),
  gen_server:start_link(Node, ?MODULE, [{nodes, Nodes}]).

init(Args) ->
  [{nodes, Nodes}] = Args,
  lists:foreach(fun(N) -> net_adm:ping(N) end, Nodes),
  LogTableName = string:concat(lists:nth(1, string:tokens(atom_to_list(node()), "@")), "_ms_inv_log"),
  {ok, Log_Ref} = dets:open_file(LogTableName, [{type, bag}, {file, LogTableName}]),
  process_flag(trap_exit, true),
  pg2:create(?MODULE),
  pg2:join(?MODULE, self()),
  LoopData = #loopData{ logref = Log_Ref},
  {ok, LoopData}.


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
%% {ok, {ProductId, WarehouseId, Quantity}}
%% {error, not_found}
get(Node, ProductId, WarehouseId) ->
  call(Node, {get, {ProductId, WarehouseId}}).

%% subtracts given quantity from the product inventory
%% returns 
%% {ok, {ProductId, WarehouseId, Quantity}}
%% {error, not_found}
%% {error, not_available_quantity}
remove(Node, ProductId, WarehouseId, Quantity) ->
  call(Node, {remove,{ProductId, WarehouseId, Quantity}}).


%% adds given quantity to the product inventory
%% returns 
%% {ok, {ProductId, WarehouseId, Quantity}}
%% {error, not_found}
add(Node, ProductId, WarehouseId, Quantity) ->
  call(Node, {add,{ProductId, WarehouseId, Quantity}}).

status(Node) ->
  call(Node, {status}).

get_operations(Node, ProductId, WarehouseId) ->
  call(Node, {operations, ProductId, WarehouseId}).

handle_call({get, {ProductId, WarehouseId}}, _From, LoopData) ->
  {reply, get_inventory(ProductId, WarehouseId), LoopData};

handle_call({remove, {ProductId, WarehouseId, RemoveQuantity}}, _From, LoopData) ->
  {reply, remove_inventory(ProductId, WarehouseId, RemoveQuantity, LoopData), LoopData};

handle_call({add, {ProductId, WarehouseId, AddQuantity}}, _From, LoopData) ->
  {reply, add_inventory(ProductId, WarehouseId, AddQuantity, LoopData), LoopData};

handle_call({operations, ProductId, WarehouseId}, _From, LoopData) ->
  {reply, get_operations({ProductId, WarehouseId}, LoopData), LoopData};

handle_call({status}, _From, LoopData) ->
  {reply, ok, LoopData}.


handle_cast(stop, LoopData) ->
  pg2:leave(?MODULE, self()),
  {stop, normal, LoopData};

handle_cast(stop_sup, _LoopData) ->
  io:format("stopping node ~n"),
  pg2:leave(?MODULE, self()),
  {stop, normal, _LoopData}.

get_inventory(ProductId, WarehouseId) ->

  case ms_db:read({ProductId, WarehouseId}) of
     {ok, {{ProductId, WarehouseId}, Quantity, _Version}} ->
        {ok, {ProductId, WarehouseId, Quantity}};

     {error, Error} ->
        {error, Error}
  end.

remove_inventory(ProductId, WarehouseId, RemoveQuantity, LoopData) ->

  case ms_db:read({ProductId, WarehouseId}) of
      {ok, {_Key, Available , _Version}} ->
          case (Available - RemoveQuantity) >= 0 of
            false ->
              {error, not_available_quantity, {available, Available}, {requested, RemoveQuantity}};

            true ->
              add_inventory(ProductId, WarehouseId, RemoveQuantity * (-1), LoopData)
          end;

      {error, Error} ->
          {error, Error}
  end.

add_inventory(ProductId, WarehouseId, AddQuantity, LoopData) ->

  case ms_db:read({ProductId, WarehouseId}) of
    {ok, {Key, Available, _Version}} ->
      OperationRef = erlang:make_ref(),
      {ok, {{ProductId, WarehouseId}, NewQuantity, NewVersion}} = ms_db:write(Key, Available + AddQuantity),
      log_operation({Key, {version, NewVersion}, {ref, OperationRef}, Available, AddQuantity, NewQuantity}, LoopData),
      {ok, {ProductId, WarehouseId, NewQuantity}, {ref, OperationRef}};

    {error, Error} ->
      {error, Error}
  end.


log_operation(Log, LoopData) ->
  dets:insert(LoopData#loopData.logref, Log).

get_operations(Key, LoopData) ->
  dets:lookup(LoopData#loopData.logref, Key).

