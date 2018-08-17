-module(stop_node).
-export([start/1,stop/0,loop/1,init/1]).

start(Interval) ->
  init(Interval).

stop() ->
  Pid = whereis(?MODULE),

  case Pid of
    undefined ->
      ok;
    Pid ->
      Pid ! {stop}
  end.


init(Interval) ->
  MilisecondsInterval = Interval * 1000,
  Pid = spawn(?MODULE, loop, [MilisecondsInterval]),
  register(?MODULE, Pid),
  Pid ! {stop_node, 1},
  ok.


loop(Interval) ->
  receive

    {stop} ->
      io:format("stoping stop_node process ~n"),
      ok;

    {stop_node, N} ->

      io:format("--------------------------------- ~n"),
      io:format("stop_node process sleeps.... ~n"),
      timer:sleep(Interval),
      io:format("--------------------------------- ~n"),
      io:format("stoping node event number: ~p~n", [N]),
      io:format("stoping node at: ~p~n", [calendar:local_time()]),
      io:format("--------------------------------- ~n"),
      ms_inv_proxy:stop_node(),
      self() ! {stop_node, N + 1},
      loop(Interval)


  end.
