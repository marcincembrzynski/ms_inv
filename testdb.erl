-module(testdb).
-export([create/0]).

% 100000 * 4 = 400000
create() ->
	[DBName|_] = string:split(atom_to_list(node()),"@"),
	{ok, DB} = dets:open_file(DBName, [{type, set}, {file, DBName}]),

	Populate = fun({P,W}) ->
		dets:insert(DB, {{P,W}, P, 1})
	end,

	List = [{P,W} || P <- lists:seq(1,100000), W <- [pl,uk,de,fr]],
	lists:foreach(Populate, List),
	io:format("info: ~p~n", [dets:info(DB)]).




