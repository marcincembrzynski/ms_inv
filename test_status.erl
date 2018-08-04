-module(test_status).
-export([start/2,loop/0, result/0]).

start(Proc, Req) ->

    %% create or open ets named table
    case ets:info(?MODULE) of
      undefined ->
        ets:new(?MODULE, [named_table, public]);
      _ ->
        ets:delete_all_objects(?MODULE)
    end,


    init(Proc,Req).

init(0, _) -> ok;

init(Proc, Req) ->
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Req},
  io:format("started process: ~p~n", [Pid]),
  init(Proc - 1, Req).


loop() ->
  receive
    {requests, 0} ->
      io:format("process finished: ~p~n", [self()]),
      ok;

    {requests, Req} ->
      ok = ms_inv_proxy:status(),
      ets:insert(?MODULE, {erlang:timestamp(), {self(), Req}}),
      self() ! {requests, Req - 1},
      loop()

  end.


result() ->


  List = ets:tab2list(?MODULE),
  TimeStampSort = fun({T1 ,_ },{T2 ,_}) -> T1 =< T2 end,
  SortedList = lists:sort(TimeStampSort, List),
  [{First,_}|_] = SortedList,
  {Last,_} = lists:last(SortedList),
   Time = timer:now_diff(Last, First),
   Seconds = Time / 1000000,
   NumberOfOperations = length(SortedList),
   OperationsPerSecond = length(SortedList) / Seconds,
  {{seconds, Seconds}, {number_of_operations, NumberOfOperations}, {operations_per_second, OperationsPerSecond}}.