-module(test_ms_inv).
-export([start/3, loop/0,result/0]).

start(Processes, Requests, Interval) ->

  ms_inv_proxy:start_link(),

  case ets:info(?MODULE) of
    undefined ->
      ets:new(?MODULE, [named_table, public]);
    _ ->
      ets:delete_all_objects(?MODULE)
  end,

  Units = Processes * Requests,
  io:format("Units Needed init, ~p~n", [Units]),
  {ok, {9999, uk, Available}} = ms_inv_proxy:get(9999, uk),
  RemoveResponse = ms_inv_proxy:remove(9999, uk, Available),
  io:format("RemoveResponse init, ~p~n", [RemoveResponse]),
  AddResponse = ms_inv_proxy:add(9999, uk, Units - 1),
  io:format("AddResponse init, ~p~n", [AddResponse]),

  stop_node:start(Interval),

  init(Processes, Requests),


  ok.


init(0, _) -> ok;

init(Processes, Requests) ->
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Requests},
  io:format("started process: ~p~n", [Pid]),
  init(Processes - 1, Requests).


loop() ->

  receive
    {requests, 0} ->
      stop_node:stop(),
      io:format("# stop process, ~p~n", [self()]),
      ok;

    {requests, Requests} ->

      RemoveResponse = ms_inv_proxy:remove(9999,uk,1),
      %%io:format("process ~p: remaining requests: ~p, AddResponse: ~p~n",[self(), Requests, AddResponse]),

      ets:insert(?MODULE, {erlang:timestamp(), remove, RemoveResponse}),
      %%RemoveResponse = ms_inv_proxy:add(9999,pl,1),
      %%ets:insert(?MODULE, {erlang:timestamp(), remove, RemoveResponse}),

      self() ! {requests, Requests - 1},

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
  {{seconds, Seconds}, {number_of_operations, NumberOfOperations}, {operations_per_second, OperationsPerSecond}}.
