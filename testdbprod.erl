-module(testdbprod).
-export([create/0]).

% 100000 * 4 = 400000
create() ->
	[DBName|_] = string:split(atom_to_list(node()),"@"),
	io:format("DBName ~p~n", [DBName]),
	{ok, DB} = dets:open_file(DBName, [{type, set}, {file, DBName}]),

	Populate = fun({ProductId, CountryId}) ->
		dets:insert(DB, {{ProductId, CountryId}, {inv, ProductId}, {price, {ProductId, 20}}})
	end,


	List = [{P,W} || P <- lists:seq(1,100000), W <- [pl,uk,de,fr]],
	lists:foreach(Populate, List),
	io:format("info: ~p~n", [dets:info(DB)]),

	Languages = [pl,en,de,fr],
	ProductLanguage = [{P,L} || P <- lists:seq(1,100000), L <- Languages],


  DBContentName = string:concat(DBName, "_content"),
  io:format("DBcontentName ~p~n", [DBContentName]),
  {ok, DBContent} = dets:open_file(DBContentName , [{type, set}, {file, DBContentName }]),


  PopulateContent = fun({ProductId, LanguageId}) ->
    Title = string:concat(string:concat("Test Title Product ", integer_to_list(ProductId)), atom_to_list(LanguageId)),
    Description = string:concat(string:concat("Test Description Product ", integer_to_list(ProductId)), atom_to_list(LanguageId)),
    Value = [{title, Title},{description, Description}],
    dets:insert(DBContent, {{ProductId, LanguageId}, Value})

   end,

  lists:foreach(PopulateContent, ProductLanguage),

  io:format("info: ~p~n", [dets:info(DBContent)]),
	dets:close(DB),
  dets:close(DBContent).




