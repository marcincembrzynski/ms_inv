-module(test_status).
-export([start/3,loop/0, stop_node/1, result/0]).

start(Proc, Req, Interval) ->

    ok = ms_inv_proxy:status(),

    case ets:info(?MODULE) of
      undefined ->
        ets:new(?MODULE, [named_table, public]);
      _ ->
        ets:delete_all_objects(?MODULE)
    end,

    StopNodePid = init_stop_node(Interval),

    init(Proc,Req, StopNodePid).

init_stop_node(Interval) ->
  MilisecondsInterval = Interval * 1000,
  Pid = spawn(?MODULE, stop_node, [MilisecondsInterval]),
  Pid ! {stop_node, 1},
  Pid.


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


stop_node(Interval) ->
  receive

    {stop} ->
      io:format("stoping stop_node process ~n"),
      ok;

    {stop_node, N} ->
      io:format("stop_node process sleeps.... ~n"),
      %% sleep time as argument
      timer:sleep(Interval),
      io:format("stoping node event number: ~p~n", [N]),
      io:format("--------------------------------- ~n"),
      ms_inv_proxy:stop_node(),
      self() ! {stop_node, N + 1},
      stop_node(Interval)


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