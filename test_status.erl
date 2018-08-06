-module(test_status).
-export([start/2,loop/0, stop_node/0, result/0]).

start(Proc, Req) ->

    ok = ms_inv_proxy:status(),

    case ets:info(?MODULE) of
      undefined ->
        ets:new(?MODULE, [named_table, public]);
      _ ->
        ets:delete_all_objects(?MODULE)
    end,

    Pid = spawn(?MODULE, stop_node, []),
    Pid ! {stop_node, 1},


    init(Proc,Req, Pid).

init(0, _, _) -> ok;

init(Proc, Req, StopNodePid) ->
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Req, StopNodePid},
  io:format("started process: ~p~n", [Pid]),
  init(Proc - 1, Req, StopNodePid).


loop() ->
  receive
    {requests, 0, StopNodePid} ->
      StopNodePid ! {stop},
      io:format("process finished: ~p~n", [self()]),
      ok;

    {requests, Req, StopNodePid} ->
      ok = ms_inv_proxy:status(),
      ets:insert(?MODULE, {erlang:timestamp(), {self(), Req}}),
      self() ! {requests, Req - 1, StopNodePid},
      loop()

  end.


stop_node() ->
  receive

    {stop} ->
      io:format("stoping stop node: ~n"),
      ok;

    {stop_node, N} ->
      io:format("Stop Node process sleeps.... ~n"),
      timer:sleep(5000),
      io:format("stoping node event number: ~p~n", [N]),
      ms_inv_proxy:stop_node(),
      self() ! {stop_node, N + 1},
      stop_node()


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