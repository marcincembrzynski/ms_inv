-module(requester).
-export([start/2,loop/0,test/0,step/2]).
-record(testData, {startTime, dbRef, productId, warehouseId, counterRef}).


start(Processes, Requests) ->
  {ok, [{inventory,{ProductId,WarehouseId}}]} = file:consult("requester.txt"),
  {ok, DB} = dets:open_file(requests_db, [{type, set}, {file, requests_db}]),
  CounterTable = ets:new(counter, [public]),
  ets:insert(CounterTable, {counter,0}),
  dets:delete_all_objects(DB),
  dets:insert(DB, {erlang:timestamp(), start, ms_inv_proxy:get(ProductId, WarehouseId)}),
  TestData = #testData{ startTime = erlang:timestamp(), dbRef = DB, productId = ProductId, warehouseId = WarehouseId, counterRef = CounterTable},
  io:format("data: ~p~n",[TestData]),
  init(Processes, Requests, TestData).

init(0, _, _) -> ok;

init(Processes,Requests, TestData) ->
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Requests, TestData},
  NewProcesses = Processes - 1,
  init(NewProcesses, Requests, TestData).

test() ->
  {ok, DB} = dets:open_file(requests_db, [{type, set}, {file, requests_db}]),
  LogTable = ets:new(requestes_ets, [public]),
  dets:to_ets(DB, LogTable),


  List = ets:tab2list(LogTable),
  TimeStampSort = fun({T1 ,_ ,_ },{T2 ,_, _}) -> T1 =< T2 end,

  SortedList = lists:sort(TimeStampSort, List),
  Operations = length(SortedList) - 1,

  [Start|Tail] = SortedList,

  {_,start, {ok,{_,_,StartQuantity}}} = Start,

  CorrectnessFun = fun(Elem, PreviousQuantity) ->

    case Elem of
      {_, remove, {ok, {_, _, NewQuantity}}} ->
        true  = (PreviousQuantity - 1 == NewQuantity),
        NewQuantity;

      {_, add, {ok, {_, _, NewQuantity}}} ->

        true = (PreviousQuantity + 1 == NewQuantity),
        NewQuantity;

      {_, add, {error,_}} -> PreviousQuantity;

      {_, remove, {error,_}} -> PreviousQuantity

    end
   end,

  Acc = lists:foldl(CorrectnessFun, StartQuantity, Tail),
  Last = lists:last(SortedList),
  Seconds = calculate_seconds(Start, Last),

  io:format("sorted list length, number of operations: ~p~n", [Operations]),
  io:format("Time taken: ~p~n", [Seconds]),
  io:format("Operations per second: ~p~n", [Operations/Seconds]),

  {acc, Acc, start, Start, last, Last, time, Seconds}.

calculate_seconds(Start, Last) ->
  {StartTimestamp, _, _} = Start,
  {LastTimestamp, _, _} = Last,
  Time = timer:now_diff(LastTimestamp, StartTimestamp),
  Time / 1000000.


loop() ->
  receive
    {requests, 0, TestData} ->
      ProductId = TestData#testData.productId,
      WarehouseId = TestData#testData.warehouseId,
      io:format("#~p~n",[ms_inv_proxy:get(ProductId,WarehouseId)]),
      Time = timer:now_diff(erlang:timestamp(), TestData#testData.startTime),
      Seconds = Time / 1000000,
      io:format("time: ~p~n", [Seconds]);


    {requests, N, TestData} ->

      %% stop node

      stop_node(TestData),


      ProductId = TestData#testData.productId,
      WarehouseId = TestData#testData.warehouseId,
      DBRef = TestData#testData.dbRef,
      ResponseRemove = ms_inv_proxy:remove(ProductId, WarehouseId,1),
      ok = dets:insert(DBRef, {erlang:timestamp(), remove, ResponseRemove}),
      ets:update_counter(TestData#testData.counterRef, counter, 1),

      case ResponseRemove of
        {ok, _} -> ok;
        {error, _} -> io:format("remove error ~p~n", [ResponseRemove])
      end,


      %% add call to ms_inv stop

      Response = ms_inv_proxy:add(ProductId, WarehouseId,1),
      ok = dets:insert(DBRef, {erlang:timestamp(), add, Response}),
      ets:update_counter(TestData#testData.counterRef, counter, 1),
      case Response of
        {ok, _Data} -> ok;
        {error, Error} -> io:format("add error ~p~n", [Error])
      end,

      io:format("## counter: ~p~n", [ets:lookup(TestData#testData.counterRef, counter)]),

      self() ! {requests, N - 1, TestData},
      loop()

  end.

stop_node(TestData) ->
  [{counter, Counter}] = ets:lookup(TestData#testData.counterRef, counter),
  IsStep = step(Counter, 10000),
  case IsStep of
    true ->
      io:format("### stopping node, ~n"),
      ms_inv_proxy:stop_node();
    false ->
      ok
  end,
  TestData.



step(X,Step) when X < Step -> false;
step(X,Step) -> ceil(X / Step) == floor(X / Step).