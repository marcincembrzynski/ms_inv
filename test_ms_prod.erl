-module(test_ms_prod).
-export([start/3,loop/0,result/0]).


start(Processes, Requests, ProductsPerRequest) ->

  case ets:info(?MODULE) of
    undefined ->
      ets:new(?MODULE, [named_table, public]);
    _ ->
      ets:delete_all_objects(?MODULE)
  end,
  Start = 1,
  init(Processes, Requests, ProductsPerRequest, Start).

init(0, _, _, _) -> ok;

init(Processes, Requests, ProductsPerRequest, Start) ->
  List = lists:seq(Start, Start + (ProductsPerRequest - 1)),
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Requests, List},

  io:format("started process: ~p~n", [Pid]),
  init(Processes - 1, Requests, ProductsPerRequest, Start + ProductsPerRequest).


loop() ->
  receive
    {requests, 0, _List} ->
      io:format("finished process: ~p~n", [self()]),
      ok;

    {requests, Requests, List} ->
      [ProdId|T] = List,
      Response = ms_prod_proxy:get(ProdId, uk, en),
      ets:insert(?MODULE, {erlang:timestamp(), Response}),
      NewList = T ++ [ProdId],
      self() ! {requests, Requests - 1, NewList},
      loop()

  end.


result() ->


  List = ets:tab2list(?MODULE),
  TimeStampSort = fun({T1 ,_ },{T2 ,_ }) -> T1 =< T2 end,
  SortedList = lists:sort(TimeStampSort, List),
  [{First,_}|_] = SortedList,
  {Last,_} = lists:last(SortedList),
  Time = timer:now_diff(Last, First),
  Seconds = Time / 1000000,
  NumberOfOperations = length(SortedList),
  OperationsPerSecond = length(SortedList) / Seconds,

  NotOkFun = fun(Elem) -> Elem == {error, not_found} end,
  ErrorList = lists:filter(NotOkFun, List),

  {{seconds, Seconds},
    {number_of_operations, NumberOfOperations},
    {error_list, ErrorList},
    {operations_per_second, OperationsPerSecond}}.
