-module(test_mult_ms_inv).
-record(testData, {productId, warehouseId}).
-export([start/3,loop/0,result/0]).

start(Processes, Requests, Interval) ->

  ms_inv_proxy:start_link(),

  case ets:info(?MODULE) of
    undefined ->
      ets:new(?MODULE, [named_table, public]);
    _ ->
      ets:delete_all_objects(?MODULE)
  end,

  {ok, [{inventory,{ProductId, WarehouseId}}]} = file:consult(?MODULE),

  stop_node:start(Interval),

  init(ProductId, WarehouseId, Processes, Requests),

  ok.


init(_, _, 0, _) -> ok;

init(ProductId, WarehouseId, Processes, Requests) ->
  io:format("productId ~p~n", [ProductId]),



  TestData = #testData{productId = ProductId, warehouseId = WarehouseId},

  Units = Requests,
  io:format("ProductId ~p~n" , [ProductId]),
  {ok, {ProductId, WarehouseId, Available}} = ms_inv_proxy:get(ProductId, WarehouseId),
  RemoveResponse = ms_inv_proxy:remove(ProductId, WarehouseId, Available),
  io:format("RemoveResponse init, ~p~n", [RemoveResponse]),
  AddResponse = ms_inv_proxy:add(ProductId, WarehouseId, Units - 1),
  io:format("AddResponse init, ~p~n", [AddResponse]),

  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Requests, TestData},

  init(ProductId + 1, WarehouseId, Processes -1, Requests).



loop() ->

  receive
    {requests, 0, _TestData} ->
      stop_node:stop(),
      io:format("# stop process, ~p~n", [self()]),
      ok;

    {requests, Requests, TestData} ->

      ProductId = TestData#testData.productId,
      WarehouseId = TestData#testData.warehouseId,
      RemoveResponse = ms_inv_proxy:remove(ProductId, WarehouseId, 1),
      ets:insert(?MODULE, {erlang:timestamp(), {ProductId, WarehouseId}, remove, RemoveResponse}),
      self() ! {requests, Requests - 1, TestData},
      loop()

  end.

result() ->

  List = ets:tab2list(?MODULE),

  TimeStampSortList = fun({T1 ,_ ,_,_},{T2 ,_ ,_,_}) -> T1 =< T2 end,
  SortedList = lists:sort(TimeStampSortList, List),
  [{First,_,_,_}|_] = SortedList,
  {Last,_,_,_} = lists:last(SortedList),
  Time = timer:now_diff(Last, First),
  Seconds = Time / 1000000,
  NumberOfOperations = length(SortedList),
  OperationsPerSecond = length(SortedList) / Seconds,


  CollectKeyFun = fun(Elem, Acc) ->
    {Timestamp,Key,_,Response} = Elem,
    ResponseList = maps:get(Key, Acc, []),
    maps:put(Key, lists:append(ResponseList, [{Timestamp,Key,Response}]), Acc)
   end,

  Keys = lists:foldl(CollectKeyFun, maps:new(), List),
  KeysList = maps:keys(Keys),
  SortedKeyList = lists:sort(fun({A, _},{B, _}) -> B >= A end, KeysList),


  ValidateFun = fun(Elem, Acc) ->
    {ProductId, WarehouseId} = Elem,
    Response = ms_inv_proxy:validate_operations(ProductId, WarehouseId),
    lists:append(Acc, [{{ProductId, WarehouseId}, Response}])
  end,

  ValidationList = lists:foldl(ValidateFun, [], SortedKeyList),

  CollectNotConistentFun = fun(Elem, Acc) ->
    {_, {{consistent, Consistent},_ , _,_,_,_,_}} = Elem,
    case Consistent of
      true -> Acc;
      false -> lists:append(Acc, [Elem])
    end
  end,

  NotConsistentProducts = lists:foldl(CollectNotConistentFun, [], ValidationList),

  TimeStampSort = fun({T1 ,_,_},{T2,_ ,_}) -> T1 =< T2 end,

  CollectLastResponsesFun = fun(Key, Acc) ->
    L = maps:get(Key, Keys),
    SL = lists:sort(TimeStampSort, L),
    LastElem = lists:last(SL),
    lists:append(Acc, [LastElem])
  end,

  LastResponsesList = lists:foldl(CollectLastResponsesFun, [], SortedKeyList),


  NotErrorFilterFun = fun(Elem) ->
    case Elem of
      {_Timestamp, _Key, {error, _, _, _}} -> false;
      {_Timestamp, _Key, {ok, _, _}} -> true
    end
  end,

  NotErrorLastResponses = lists:filter(NotErrorFilterFun, LastResponsesList),

  {{seconds, Seconds},
    {number_of_operations, NumberOfOperations},
    {operations_per_second, OperationsPerSecond},
    {not_consistent_products, NotConsistentProducts}, {not_error_last_responses, NotErrorLastResponses}}.

