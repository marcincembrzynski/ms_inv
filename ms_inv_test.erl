-module(ms_inv_test).
-export([test/2]).
-export([start/1]).

start(N) -> 
	ms_inv_test:test(N, erlang:timestamp()).

test(0, StartTime) -> 
	Time = timer:now_diff(erlang:timestamp(),StartTime),
	io:format("time: ~p~n", [Time]);
	

test(N, StartTime) -> 
	ms_inv:add(1,pl,1),
	ms_inv:remove(1,pl,1),
	test(N - 1, StartTime).
