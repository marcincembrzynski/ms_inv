-module(ms_inv).
-export([start_link/2,init/1,stop/0]).
-export([update_node/5,update_node_cast/5]).
-export([get_from_node/3]).
-export([get/2,add/3,remove/3]).
-export([start/1]).
-export([start/0]).
-export([start_local/1]).
-export([terminate/2]).
-export([handle_call/3,handle_cast/2]).
-behaviour(gen_server).

%% initialize with list of nodes...
%% move to test file
start_local(N) ->
	Nodes = ['n1@Marcins-MBP','n2@Marcins-MBP','n3@Marcins-MBP'],
	ms_inv:start_link(Nodes, "i" ++ integer_to_list(N)).

start() -> start(nodes).

start(NodesFile) ->
	{ok,[Nodes]} = file:consult(NodesFile),
	ms_inv:start_link(Nodes, "inventory_db").
	

start_link(Nodes,DBName) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [{nodes, Nodes},{dbname, DBName}], []).


%%% initialize with nodes....
%%% db names... 	

init([{nodes, Nodes},{dbname, DBName}]) -> 

	PingNode = fun(N) -> net_adm:ping(N) end,
	lists:foreach(PingNode, Nodes),

 	{ok, DB} = dets:open_file(DBName, [{type, set}, {file, DBName}]), 
	
	{ok, [{nodes, Nodes},{dbname, DB}]}.



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
%% {ok, {ProductId, CountryId, Version, Quantity}}
%% {error, not_found}

get(ProductId, CountryId) -> 
	call({get_from_all, {ProductId, CountryId}}).


%% subtracts given quantity from the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Version, Quantity}}
%% {error, not_found}
%% {error, not_available_quantity}

remove(ProductId, CountryId, Quantity) -> 
	call({remove_from_all,{ProductId, CountryId, Quantity}}).



%% adds given quantity to the product inventory
%% returns 
%% {ok, {ProductId, CountryId, Version, Quantity}}
%% {error, not_found}

add(ProductId, CountryId, Quantity) -> 
	call({add_to_all,{ProductId, CountryId, Quantity}}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%% Internal Calls to nodes 

%lock(Node, ProductId, CountryId) -> ok. 

%unlock(Node, ProductId, CountryId) -> ok. 

%%% 
%%$ returns {ok, Product}
get_from_node(Node, ProductId, CountryId) ->
	call(Node, {get_from_node, {ProductId, CountryId}}). 

%%%
update_node(Node, ProductId, CountryId, Quantity, Version) -> 
	call(Node, {update_node, {ProductId, CountryId, Quantity, Version}}). 


update_node_cast(Node, ProductId, CountryId, Quantity, Version) ->
	cast(Node, {update_node_cast, {ProductId, CountryId, Quantity, Version}}).




%%%%%%%%%%%%%%%
%%% Call handlers
%%%
%%%
% {ProductId, CountryId, Quantity, Version}
handle_call({update_node, Product}, _From, LoopData) ->
	{reply, update_node_(Product, LoopData), LoopData};


handle_call({get_from_node, {ProductId, CountryId}}, _From, LoopData) ->
	{reply, get_from_node_(ProductId, CountryId, LoopData), LoopData};

handle_call({get_from_all, {ProductId, CountryId}}, _From, LoopData) ->
	{reply, get_latest_inventory(ProductId, CountryId, LoopData), LoopData};

handle_call({remove_from_all, {ProductId, CountryId, Quantity}}, _From, LoopData) ->
	Operation = fun(A,B) -> A - B end,
	{reply, update_all(ProductId, CountryId, Quantity, Operation, LoopData), LoopData};

handle_call({add_to_all, {ProductId, CountryId, Quantity}}, _From, LoopData) ->
	Operation = fun(A,B) -> A + B end,
	{reply, update_all(ProductId, CountryId, Quantity, Operation, LoopData), LoopData}.




handle_cast({update_node_cast, ProductInventory}, LoopData) ->
	update_node_(ProductInventory, LoopData),
	{noreply, LoopData}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%   update node handler    %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%


update_node_(ProductInventory, LoopData) -> 
	{ProductId, CountryId, Quantity, Version} = ProductInventory,
	dets:insert(db(LoopData), {{ProductId, CountryId}, Quantity, Version}),
	{ok, ProductInventory}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%   get from node handler  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_from_node_(ProductId, CountryId, LoopData) ->
	Result = dets:lookup(db(LoopData), {ProductId, CountryId}),
	get_from_node_response(Result).

get_from_node_response([{{ProductId,CountryId},Quantity,Version}]) ->
	{ok, {ProductId,CountryId,Quantity,Version}};

get_from_node_response([]) -> {error, not_found}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%
%   get from all handler  %
%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Gets inventories from all nodes
%

get_latest_inventory(ProductId, CountryId, LoopData) ->
	
	Nodes = exclude_current_node(inv_nodes(LoopData)),

	GetFromNode = fun(Node, List) ->
		%%% is alive function
		try ms_inv:get_from_node(Node, ProductId, CountryId) of

			ProductResponse -> [{Node, ProductResponse}] ++ List
		catch 
			_:_ -> List
		end
	end, 

	%%% initialize list with the value from the current node
	List = [{node(),get_from_node_(ProductId,CountryId,LoopData)}],
	
	%%% get values from all the nodes
	InventoryResponses = lists:foldl(GetFromNode, List, Nodes), 
	%%io:format("##### All InventoryResponses: ~p~n", [InventoryResponses]),
	
	%%% 1. Filter

	WithoutErrorResponses = exclude_error_responses(InventoryResponses),
	io:format("##### WithoutErrorResponses: ~p~n", [WithoutErrorResponses]),

	case WithoutErrorResponses of
		
		[] 	  -> {error,not_found};
		
		[_|_] ->  
			
			%%% 2. Sort 
			Sorted = sort_inventories(WithoutErrorResponses),

			%%% 3 cast for tail of the sorted list??? node needed
			%%% get tail of the, 
	
			{_Node,LatestIventory} = lists:nth(1,Sorted), 

			update_nodes_with_latest_inventory(InventoryResponses,LatestIventory),
			%%io:format("### InventoryResponses WithoutErrorResponses: ~p~n", [InventoryResponses]),
			
			LatestIventory
			%%{LatestIventory, {nodes_to_update, NodesToUpdate}, {inventory_reponses, InventoryResponses}}
	end.

	%%%


update_all(ProductId, CountryId, UpdateQuantity, Operation, LoopData) ->
	
	ProductInventoryResponse = get_latest_inventory(ProductId, CountryId, LoopData),
	io:format("ProductInventoryResponse ##, ~p~n", [ProductInventoryResponse]), 

	case ProductInventoryResponse of
		
		{error, Error} -> 
			{error, Error};

		{ok, ProductInventory} ->		
			{ProductId, CountryId, Quantity, Version} = ProductInventory,
			NewQuantity = apply(Operation,[Quantity,UpdateQuantity]),

			case NewQuantity >= 0 of
				
				false -> 
					{error, not_available_quantity};
				true ->
					NewProductInventory = {ProductId, CountryId, NewQuantity, Version},
					update_all(NewProductInventory, LoopData)
			end

	end.




%%%%% change name

update_all(ProductInventory, LoopData) ->
	{ProductId, CountryId, Quantity, Version} = ProductInventory,
	Nodes = exclude_current_node(inv_nodes(LoopData)),
	NewVersion = Version + 1,
	update_node_({ProductId, CountryId, Quantity, NewVersion}, LoopData),

	UpdateNode = fun(Node) ->
		ms_inv:update_node_cast(Node, ProductId,CountryId, Quantity, NewVersion) 
	end,

	lists:foreach(UpdateNode,Nodes),
	io:format("#### after update..."),
	get_from_node_(ProductId, CountryId, LoopData).



	

%%%%%%
% helper functions 
%
%

db([{nodes, _Nodes},{dbname, DB}]) -> DB.

inv_nodes([{nodes, Nodes},{dbname, _DBName}]) -> Nodes.

exclude_current_node(Nodes) ->

	Filter = fun(Node) ->
		Node /= node()
	end,

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

%%%%
sort_inventories(InventoryResponses) ->
	
	ReverseSort = fun(A,B) ->
	
		{_,{ok,{_, _, _, T1}}} = A,
		{_,{ok,{_, _, _, T2}}} = B,

		case (T1 >= T2) of 
			true -> true;
			false -> false 
		end
	end,

	
	lists:sort(ReverseSort, InventoryResponses).



%% Updates all the nodes with the correct inventory

update_nodes_with_latest_inventory(InventoryResponses, LatestInventory) ->

	{ok,{ProductId, CountryId, Quantity, Version}} = LatestInventory,

	% 1. Creates the lists of InventoryResponses not equal to LatestInventory

  Filter = fun({_,Other}) -> LatestInventory /= Other end,
	NotCorrectInventories = lists:filter(Filter, InventoryResponses),

  % 2. Based on this creates list of nodes to update

  MapToNodes = fun({Node,_}) -> Node end,
	NodesToUpdate = lists:map(MapToNodes, NotCorrectInventories),
	io:format("Nodes to update ~p~n", [NodesToUpdate]),


  % 3. Updates all the nodes from the list with the latest repository
	UpdateNodeCast = fun(Node) ->
		ms_inv:update_node_cast(Node, ProductId, CountryId, Quantity, Version)
	end,

	lists:foreach(UpdateNodeCast, NodesToUpdate),

	ok.


