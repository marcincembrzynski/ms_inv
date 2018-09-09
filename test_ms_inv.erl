-module(test_ms_inv).
-export([start/3, loop/0,result/0]).
-record(testData, {productId, warehouseId}).

start(Processes, Requests, Interval) ->

  ms_inv_proxy:start_link(),

  case ets:info(?MODULE) of
    undefined ->
      ets:new(?MODULE, [named_table, public]);
    _ ->
      ets:delete_all_objects(?MODULE)
  end,

  {ok, [{inventory,{ProductId, WarehouseId}}]} = file:consult(?MODULE),
  TestData = #testData{productId = ProductId, warehouseId = WarehouseId},

  Units = Processes * Requests,
  io:format("Units Needed init, ~p~n", [Units]),
  {ok, {ProductId, WarehouseId, Available}} = ms_inv_proxy:get(ProductId, WarehouseId),
  RemoveResponse = ms_inv_proxy:remove(ProductId, WarehouseId, Available),
  io:format("RemoveResponse init, ~p~n", [RemoveResponse]),
  AddResponse = ms_inv_proxy:add(ProductId, WarehouseId, Units - 1),
  io:format("AddResponse init, ~p~n", [AddResponse]),

  stop_node:start(Interval),

  init(Processes, Requests, TestData),


  ok.


init(0, _, _) -> ok;

init(Processes, Requests, TestData) ->
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Requests, TestData},
  io:format("started process: ~p~n", [Pid]),
  init(Processes - 1, Requests, TestData).


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
      ets:insert(?MODULE, {erlang:timestamp(), remove, RemoveResponse}),
      self() ! {requests, Requests - 1, TestData},
      loop()

  end.


result() ->


  List = ets:tab2list(?MODULE),
  TimeStampSort = fun({T1 ,_ ,_},{T2 ,_ ,_}) -> T1 =< T2 end,
  SortedList = lists:sort(TimeStampSort, List),
  [{First,_,_}|_] = SortedList,
  {Last,_,_} = lists:last(SortedList),
  Time = timer:now_diff(Last, First),
  Seconds = Time / 1000000,
  NumberOfOperations = length(SortedList),
  OperationsPerSecond = length(SortedList) / Seconds,
  {{seconds, Seconds},
    {number_of_operations, NumberOfOperations},
    {operations_per_second, OperationsPerSecond},
    {last_element, lists:last(SortedList)}}.
