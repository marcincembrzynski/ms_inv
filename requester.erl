-module(requester).
-export([start/2,loop/0]).


start(Processes, Requests) ->
  init(Processes, Requests).

init(0,_) -> ok;

init(Processes,Requests) ->
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Requests},
  NewProcesses = Processes - 1,
  timer:sleep(10),
  init(NewProcesses, Requests).


loop() ->
  receive
    {requests, 0} ->
      io:format("finish..... ~n");

    {requests, N} ->

      {ok,{9999,pl,10000, _}} = ms_inv:add(9999,pl,1),
      {ok,{9999,pl,9999, _}} = ms_inv:remove(9999,pl,1),
      io:format("ok ~p~n",[N]),

      %{ok, {9999, pl, Q, _V}} = ms_inv:get(9999,pl),
      %9999 = Q,
      timer:sleep(25),


      self() ! {requests, N - 1},
      loop()

  end.


