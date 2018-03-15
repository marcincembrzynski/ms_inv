-module(ms_inv).
-export([start_link/0,init/1,stop/0]).
-export([update_node_cast/5,update_node/5]).
-export([get_from_node/3]).
-export([get/2,add/3,remove/3]).
-export([terminate/2]).
-export([handle_call/3,handle_cast/2]).
-export([lock/0,unlock/0]).
-behaviour(gen_server).


start_link() ->
  [DBName|_] = string:split(atom_to_list(node()),"@"),
  {ok,[Nodes]} = file:consult(nodes),
  io:format("# initializing with nodes ~p~n",[Nodes]),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [{{nodes, Nodes},{dbname, DBName},{locked,false}}], []).


init([{{nodes, Nodes},{dbname, DBName},{locked,false}}]) ->

  PingNode = fun(N) -> net_adm:ping(N) end,
  lists:foreach(PingNode, Nodes),
  {ok, DB} = dets:open_file(DBName, [{type, set}, {file, DBName}]),
  {ok, [{{nodes, Nodes},{dbname, DB},{lock,false}}]}.

stop() ->
  gen_server:cast(?MODULE, stop).


terminate(_Reason, DB) ->
  dets:close(DB),
  ok.

call(Msg) -> 
  gen_server:call(?MODULE, Msg).

call(Node, Msg) -> 
  gen_server:call({?MODULE,Node}, Msg).

cast(Node, Msg) -> 
  gen_server:cast({?MODULE,Node}, Msg).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% External API for http module ????
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% returns 
%% {ok, {ProductId, CountryId, Quantity, Version}}
%% {error, not_found}
get(ProductId, CountryId) -> 
  call({get_from_all, {ProductId, CountryId}}).


%% subtracts given quantity from the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Quantity, Version}}
%% {error, not_found}
%% {error, not_available_quantity}
remove(ProductId, CountryId, Quantity) ->
  call({remove_from_all,{ProductId, CountryId, Quantity}}).


%% adds given quantity to the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Quantity, Version}}
%% {error, not_found}
add(ProductId, CountryId, Quantity) -> 
  call({add_to_all,{ProductId, CountryId, Quantity}}).


lock() -> call({lock, true}).

unlock() -> call({lock, false}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%% Internal Calls to nodes 


%%% 
%%$ returns {ok, Product}
get_from_node(Node, ProductId, CountryId) ->
	call(Node, {get_from_node, {ProductId, CountryId}}).

update_node(Node, ProductId, CountryId, Quantity, Version) ->
  call(Node, {update_node, {ProductId, CountryId, Quantity, Version}}).

update_node_cast(Node, ProductId, CountryId, Quantity, Version) ->
	cast(Node, {update_node_cast, {ProductId, CountryId, Quantity, Version}}).




%%%%%%%%%%%%%%%
%%% Call handlers
%%%
%%%
% {ProductId, CountryId, Quantity, Version}
handle_call({update_node, ProductInventory}, _From, LoopData) ->
  {reply, update_current_node(ProductInventory, LoopData), LoopData};


handle_call({get_from_node, {ProductId, CountryId}}, _From, LoopData) ->
  {reply, get_from_current_node(ProductId, CountryId, LoopData), LoopData};

handle_call({get_from_all, {ProductId, CountryId}}, _From, LoopData) ->
  {reply, get_latest_inventory(ProductId, CountryId, LoopData), LoopData};

handle_call({remove_from_all, {ProductId, CountryId, Quantity}}, _From, LoopData) ->


  Operation = fun(A,B) -> A - B end,
  %Reply = update_all(ProductId, CountryId, Quantity, Operation, LoopData),
  %{reply, Reply, LoopData};
  check_lock(ProductId, CountryId, Quantity, LoopData, Operation);



handle_call({add_to_all, {ProductId, CountryId, Quantity}}, _From, LoopData) ->
  Operation = fun(A,B) -> A + B end,
  %Reply = update_all(ProductId, CountryId, Quantity, Operation, LoopData),
  check_lock(ProductId, CountryId, Quantity, LoopData, Operation).



check_lock(ProductId, CountryId, Quantity, LoopData, Operation) ->

  IsFree = pushbutton:is_free(),
  io:format("# is free ~p~n", [IsFree]),

  case pushbutton:is_free() of
    false ->
      {reply, {error, locked}, LoopData};
    true ->
      pushbutton:toggle(),
      Reply = update_all(ProductId, CountryId, Quantity, Operation, LoopData),
      pushbutton:toggle(),

      {reply, Reply, LoopData}
  end.


handle_cast(stop, LoopData) -> {stop, normal, LoopData};

handle_cast({update_node_cast, ProductInventory}, LoopData) ->
  update_current_node(ProductInventory, LoopData),
  {noreply, LoopData}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%   update node handler    %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%


update_current_node(ProductInventory, LoopData) ->
  {ProductId, CountryId, Quantity, Version} = ProductInventory,
  ok = dets:insert(db(LoopData), {{ProductId, CountryId}, Quantity, Version}),
  {ok, ProductInventory}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%   get from node handler  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_from_current_node(ProductId, CountryId, LoopData) ->
	Result = dets:lookup(db(LoopData), {ProductId, CountryId}),
	get_from_current_node_response(Result).

get_from_current_node_response([{{ProductId,CountryId},Quantity,Version}]) ->
	{ok, {ProductId,CountryId,Quantity,Version}};

get_from_current_node_response([]) -> {error, not_found}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%
%   get latest inventory  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Gets inventories from all nodes
%

get_latest_inventory(ProductId, CountryId, LoopData) ->

  Nodes = exclude_current_node(inv_nodes(LoopData)),
  %io:format("# nodes, ~p~n",[Nodes]),

  GetFromNode = fun(Node, List) ->
    try ms_inv:get_from_node(Node, ProductId, CountryId) of
      ProductResponse -> [{Node, ProductResponse}] ++ List
    catch
      _:_ -> List
    end
  end,

  %%% initialize list with the value from the current node
  List = [{node(), get_from_current_node(ProductId,CountryId,LoopData)}],
	
  %%% get values from all the nodes
  InventoryResponses = lists:foldl(GetFromNode, List, Nodes),
  %io:format("##### All InventoryResponses: ~p~n", [InventoryResponses]),
	
  %%% 1. Filter

  WithoutErrorResponses = exclude_error_responses(InventoryResponses),
  %%io:format("##### WithoutErrorResponses: ~p~n", [WithoutErrorResponses]),

  case WithoutErrorResponses of
		
    [] 	  -> {error,not_found};
		
    [_|_] ->
			
      %%% 2. Sort
      Sorted = sort_inventories(WithoutErrorResponses),

      %%% 3 cast for tail of the sorted list??? node needed
      %%% get tail of the,
	
      {_Node,LatestIventory} = lists:nth(1,Sorted),

      update_nodes_with_latest_inventory(InventoryResponses,LatestIventory,LoopData),
      %%io:format("### InventoryResponses WithoutErrorResponses: ~p~n", [InventoryResponses]),
			
      LatestIventory
  end.

	%%%


update_all(ProductId, CountryId, UpdateQuantity, Operation, LoopData) ->

  %io:format("#enter update all ~n"),
  ProductInventoryResponse = get_latest_inventory(ProductId, CountryId, LoopData),
  %%%io:format("ProductInventoryResponse ##, ~p~n", [ProductInventoryResponse]),

  %io:format("### is locked ~p~n",[isLocked(LoopData)]),

  case ProductInventoryResponse of
		
    {error, Error} -> {error, Error};

    {ok, ProductInventory} ->
      {ProductId, CountryId, Quantity, Version} = ProductInventory,
      NewQuantity = apply(Operation,[Quantity,UpdateQuantity]),

      case NewQuantity >= 0 of
				
        false ->
          {error, not_available_quantity}, LoopData;
        true ->
          NewVersion = Version + 1,
          UpdateNode = update_node_fun_node(ProductId,CountryId, NewQuantity, NewVersion, LoopData),
          lists:foreach(UpdateNode,inv_nodes(LoopData)),
          %%io:format("#leave update all ~n"),
          get_from_current_node(ProductId, CountryId, LoopData)
      end
  end.


%%%%%%
% helper functions 
%

db([{_,{dbname, DB},_}]) -> DB.

inv_nodes([{{nodes, Nodes},_,_}]) -> Nodes.

exclude_current_node(Nodes) ->

	Filter = fun(Node) -> Node /= node() end,
  lists:filter(Filter, Nodes).

%%%%
%%%% excludes error respones 
exclude_error_responses(InventoryResponses) ->
	
  Filter = fun(ProductResponse) ->
    not_error_response(ProductResponse)
  end,

  lists:filter(Filter, InventoryResponses).

not_error_response({_,{error, _}}) -> false;
not_error_response({_,{ok,_}}) -> true.


sort_inventories(InventoryResponses) ->

  ReverseSort = fun(A,B) ->
    {_,{ok,{_, _, _, T1}}} = A,
    {_,{ok,{_, _, _, T2}}} = B,
    T1 >= T2
  end,

  lists:sort(ReverseSort, InventoryResponses).


%% Updates all the nodes with the latest inventory

update_nodes_with_latest_inventory(InventoryResponses, LatestInventory, LoopData) ->

  {ok,{ProductId, CountryId, Quantity, Version}} = LatestInventory,
  %io:format("#latest inventories ~p~n", [LatestInventory]),

  % 1. Creates the lists of InventoryResponses not equal to LatestInventory

  Filter = fun({_,Other}) -> LatestInventory /= Other end,
	NotCorrectInventories = lists:filter(Filter, InventoryResponses),

  % 2. Based on this creates list of nodes to update

  MapToNodes = fun({Node,_}) -> Node end,
  NodesToUpdate = lists:map(MapToNodes, NotCorrectInventories),
  %io:format("Nodes to update ~p~n", [NodesToUpdate]),

  % 3. Updates all the nodes from the list with the latest repository
  % what about the current node?
  % remove current node...

  UpdateNode = update_node_fun_node(ProductId, CountryId, Quantity, Version, LoopData),

  lists:foreach(UpdateNode, NodesToUpdate),

  ok.

update_node_fun_node(ProductId, CountryId, Quantity, Version, LoopData) ->
  fun(Node) ->
    case (node() == Node) of
      true ->
        update_current_node({ProductId, CountryId, Quantity, Version}, LoopData);

      false ->

        try ms_inv:update_node(Node, ProductId, CountryId, Quantity, Version) of
          _ -> ok
        catch
          _:_ -> error
        end
    end
  end.