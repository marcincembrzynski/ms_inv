-module(requester).
-export([start/2,loop/0,to_ets/1]).
-record(testData, {startTime, dbRef, productId, warehouseId}).


start(Processes, Requests) ->
  {ok, [{inventory,{ProductId,WarehouseId}}]} = file:consult("requester.txt"),
  {ok, DB} = dets:open_file(requests_db, [{type, set}, {file, requests_db}]),
  dets:delete_all_objects(DB),
  dets:insert(DB, {erlang:timestamp(), start, ms_inv_client:get(ProductId, WarehouseId)}),
  TestData = #testData{ startTime = erlang:timestamp(), dbRef = DB, productId = ProductId, warehouseId = WarehouseId},
  io:format("data: ~p~n",[TestData]),
  init(Processes, Requests, TestData).

init(0, _, _) -> ok;

init(Processes,Requests, TestData) ->
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Requests, TestData},
  NewProcesses = Processes - 1,
  init(NewProcesses, Requests, TestData).

to_ets(Name) ->
  {ok, DB} = dets:open_file(requests_db, [{type, set}, {file, requests_db}]),
  LogTable = ets:new(Name, [public]),
  dets:to_ets(DB, LogTable),

  List = ets:tab2list(LogTable),
  TimeStampSort = fun({T1 ,_ ,_ },{T2 ,_, _}) -> T1 =< T2 end,

  SortedList = lists:sort(TimeStampSort, List),

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

      {error, _} -> PreviousQuantity

    end
   end,

  Acc = lists:foldl(CorrectnessFun, StartQuantity, Tail),

  {Acc, Start}.


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


      ProductId = TestData#testData.productId,
      WarehouseId = TestData#testData.warehouseId,
      DBRef = TestData#testData.dbRef,
      ResponseRemove = ms_inv_proxy:remove(ProductId, WarehouseId,1),
      ok = dets:insert(DBRef, {erlang:timestamp(), remove, ResponseRemove}),

      case ResponseRemove of
        {ok, _} -> ok;
        {error, _} -> io:format("remove error ~p~n", [ResponseRemove])
      end,


      %% add call to ms_inv stop

      Response = ms_inv_proxy:add(ProductId, WarehouseId,1),
      ok = dets:insert(DBRef, {erlang:timestamp(), add, Response}),
      case Response of
        {ok, _Data} -> ok;
        {error, Error} -> io:format("add error ~p~n", [Error])
      end,



      self() ! {requests, N - 1, TestData},
      loop()

  end.


