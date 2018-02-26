-module(ms_inv_test).
-export([test/2]).
-export([start/1]).
-export([populate_db/1]).

start(N) -> 
	ms_inv_test:test(N, erlang:timestamp()).

test(0, StartTime) -> 
	Time = timer:now_diff(erlang:timestamp(),StartTime),
	io:format("time: ~p~n", [Time]);
	

test(N, StartTime) -> 
	ms_inv:add(1,pl,1),
	ms_inv:remove(1,pl,1),
	test(N - 1, StartTime).

populate_db() -> populate_db(inventory_db).


% 100000 % 4
populate_db(DBName) -> 
	{ok, DB} = dets:open_file(DBName, [{type, set}, {file, DBName}]), 
	Warehouses = [pl,de,uk,fr],

	Populate = fun({P,W}) ->
		dets:insert(DB, {{P,W}, P, 1})
	end,
	
	List = [{P,W} || P <- lists:seq(1,100000), W <- [pl,uk,de,fr]],
	lists:foreach(Populate, List), 
	io:format("info: ~p~n", [dets:info(DB)]).


