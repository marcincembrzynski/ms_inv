-module(ms_sup).
-behavior(supervisor).
-export([ms_price_start/0,ms_content_start/0,start_link/1, init/1]).
-export([stop/0]).

ms_content_start() ->
  start_link(ms_content_sup_conf).

ms_price_start() ->
  start_link(ms_price_sup_conf).

start_link(ConfFile) ->
  {ok,[Config]} = file:consult(ConfFile),
  io:format("config: ~p~n", [Config]),
  supervisor:start_link({local,?MODULE}, ?MODULE, Config).

stop() ->
  exit(whereis(?MODULE), shutdown).


init(Config) ->

  Fun = fun({Module, Args}, List) -> lists:append([child(Module, Args)], List) end,
  ChildSpecList = lists:foldl(Fun, [], Config),
  {ok,{{one_for_one, 1, 5}, ChildSpecList}}.

child(Module, Args) ->
  {Module, {Module, start_link, Args},
    permanent, 2000, worker, [Module]}.