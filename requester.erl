-module(requester).
-export([start/2,loop/0,test/0]).
-record(testData, {startTime, dbRef, productId, warehouseId, counterRef, messages}).


start(Processes, Messages) ->
  {ok, [{inventory,{ProductId,WarehouseId}}]} = file:consult("requester.txt"),
  {ok, DB} = dets:open_file(requests_db, [{type, set}, {file, requests_db}]),
  CounterTable = ets:new(counter, [public]),
  ets:insert(CounterTable, {counter,0}),
  dets:delete_all_objects(DB),
  dets:insert(DB, {erlang:timestamp(), start, ms_inv_proxy:get(ProductId, WarehouseId)}),

  TestData = #testData{
    startTime = erlang:timestamp(), dbRef = DB, productId = ProductId,
    warehouseId = WarehouseId, counterRef = CounterTable,
    messages = Processes * Messages},

  io:format("data: ~p~n",[TestData]),
  init(Processes, Messages, TestData).

init(0, _, _) -> ok;

init(Processes, Messages, TestData) ->
  Pid = spawn(?MODULE, loop, []),
  send(Pid, Messages, TestData),
  NewProcesses = Processes - 1,
  init(NewProcesses, Messages, TestData).

send(Pid, Messages, TestData) ->
  Pid ! {messages, Messages, TestData}.

loop() ->
  receive
    {messages, 0, TestData} ->

      ProductId = TestData#testData.productId,
      WarehouseId = TestData#testData.warehouseId,
      io:format("#~p~n",[ms_inv_proxy:get(ProductId,WarehouseId)]),
      Time = timer:now_diff(erlang:timestamp(), TestData#testData.startTime),
      Seconds = Time / 1000000,
      io:format("time: ~p~n", [Seconds]);


    {messages, N, TestData} ->


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

      Response = ms_inv_proxy:add(ProductId, WarehouseId,1),
      ok = dets:insert(DBRef, {erlang:timestamp(), add, Response}),
      ets:update_counter(TestData#testData.counterRef, counter, 1),
      case Response of
        {ok, _Data} -> ok;
        {error, Error} -> io:format("add error ~p~n", [Error])
      end,

      send(self(), N - 1, TestData),

      loop()

  end.


test() ->
  {ok, DB} = dets:open_file(requests_db, [{type, set}, {file, requests_db}]),
  %%% clean ets
  LogTable = ets:new(requestes_ets, [public]),
  dets:to_ets(DB, LogTable),


  List = ets:tab2list(LogTable),
  TimeStampSort = fun({T1 ,_ ,_ },{T2 ,_, _}) -> T1 =< T2 end,

  SortedList = lists:sort(TimeStampSort, List),
  NumberOfOperations = length(SortedList) - 1,

  [Start| Operations] = SortedList,

  {_,start, {ok,{_,_,StartQuantity}}} = Start,

  Acc = lists:foldl(correctness_fun(), StartQuantity, Operations),
  Last = lists:last(SortedList),
  {_,_,{_,{_,_,FinalQuantity}}} = Last,
  Seconds = calculate_seconds(Start, Last),

  NumberOfErrors = number_of_errors(Operations),


  io:format("Number of write operations: ~p~n", [NumberOfOperations]),
  io:format("Number of errors: ~p~n", [NumberOfErrors]),
  io:format("Time taken: ~p~n", [Seconds]),
  io:format("Operations per second: ~p~n", [NumberOfOperations /Seconds]),
  io:format("Start quantity: ~p~n", [StartQuantity]),
  io:format("Final quantity: ~p~n", [FinalQuantity]),

  ets:delete(LogTable),


  {acc, Acc, start, Start, last, Last, time, Seconds}.

number_of_errors(Operations) ->
  ErrorFilterFun = fun(Elem) ->
    case Elem of
      {_, add, {error, _}} -> true;

      {_, remove, {error, _}} -> true;

      _ -> false

    end
  end,
  length(lists:filter(ErrorFilterFun, Operations)).


correctness_fun() ->
  fun(Elem, PreviousQuantity) ->

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
  end.



calculate_seconds(Start, Last) ->
  {StartTimestamp, _, _} = Start,
  {LastTimestamp, _, _} = Last,
  Time = timer:now_diff(LastTimestamp, StartTimestamp),
  Time / 1000000.
