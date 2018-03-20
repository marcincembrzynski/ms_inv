-module(ms_inv_test).
-export([test/2]).
-export([test2/0]).
-export([start/1, start/2]).
-export([populate_db/0]).


start(Processes, Requests) ->


start(N) -> 
	ms_inv_test:test(N, erlang:timestamp()).

test(0, StartTime) ->
	{ok, {1,pl,1,_}} = ms_inv:get(1,pl),
	Time = timer:now_diff(erlang:timestamp(),StartTime),
	io:format("time: ~p~n", [Time]);
	

test(N, StartTime) -> 
	ms_inv:add(1,pl,1),
	ms_inv:remove(1,pl,1),
	test(N - 1, StartTime).




% 100000 % 4
populate_db() ->
	[DBName|_] = string:split(atom_to_list(node()),"@"),
	{ok, DB} = dets:open_file(DBName, [{type, set}, {file, DBName}]), 

	Populate = fun({P,W}) ->
		dets:insert(DB, {{P,W}, P, 1})
	end,
	
	List = [{P,W} || P <- lists:seq(1,100000), W <- [pl,uk,de,fr]],
	lists:foreach(Populate, List), 
	io:format("info: ~p~n", [dets:info(DB)]).


test2() ->
	{ok, {4,de,4, _}} = ms_inv:get(4,de), 
	{ok, {4,de,5, _}} = ms_inv:add(4,de,1),
	{ok, {4,de,5, _}} = ms_inv:get(4,de), 
	{ok, {4,de,4, _}} = ms_inv:remove(4,de,1),
	{ok, {4,de,4, _}} = ms_inv:get(4,de).


