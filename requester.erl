-module(requester).
-export([start/2,loop/0]).


start(Processes, Requests) ->
  init(Processes, Requests, erlang:timestamp()).

init(0, _, _) -> ok;

init(Processes,Requests, StartTime) ->
  Pid = spawn(?MODULE, loop, []),

  Pid ! {requests, Requests, StartTime},
  NewProcesses = Processes - 1,
  timer:sleep(2),
  init(NewProcesses, Requests, StartTime).


loop() ->
  receive
    {requests, 0, StartTime} ->
      io:format("#~p~n",[ms_inv:get(9999,pl)]),
      Time = timer:now_diff(erlang:timestamp(),StartTime),
      io:format("time: ~p~n", [Time / 1000000]);


    {requests, N, StartTime} ->




      ResponseRemove = ms_inv:remove(9999,pl,1),
      case ResponseRemove of
        {ok, _} -> ok;
        {error, _} -> io:format("remove error ~p~n", [ResponseRemove])
      end,

      timer:sleep(2),

      Response = ms_inv:add(9999,pl,1),
      case Response of
        {ok, _Data} -> ok;
        {error, Error} -> io:format("add error ~p~n", [Error])
      end,

      timer:sleep(2),

      self() ! {requests, N - 1, StartTime},
      loop()

  end.


