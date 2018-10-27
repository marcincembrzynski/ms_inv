-module(testdb).
-export([create/0,createPrice/1,createContentDB/1]).

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


createPrice(PriceDB) ->

	{ok, DB} = dets:open_file(PriceDB, [{type, set}, {file, PriceDB}]),
	Populate = fun({P,C}) ->
		dets:insert(DB, {{P,C}, {P, 20}, 1})
	end,

	List = [{P,C} || P <- lists:seq(1,100000), C <- [pl,uk,de,fr]],
	lists:foreach(Populate, List),
	io:format("info: ~p~n", [dets:info(DB)]).


createContentDB(ContentDBName) ->
	{ok, DB} = dets:open_file(ContentDBName, [{type, set}, {file, ContentDBName}]),

	Languages = [pl,en,de,fr],
	ProductLanguage = [{P,L} || P <- lists:seq(1,100000), L <- Languages],

	PopulateContent = fun({ProductId, LanguageId}) ->
		Title = string:concat(string:concat("Test Title Product ", integer_to_list(ProductId)), atom_to_list(LanguageId)),
		Description = string:concat(string:concat("Test Description Product ", integer_to_list(ProductId)), atom_to_list(LanguageId)),
		Key = {ProductId, LanguageId},
		Value = [{title, Title},{description, Description}],
		Version = 1,
		dets:insert(DB, {Key, Value, Version})
	end,

	lists:foreach(PopulateContent, ProductLanguage),

	io:format("info: ~p~n", [dets:info(DB)]),
	dets:close(DB).






