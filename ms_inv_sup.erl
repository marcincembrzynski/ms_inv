-module(ms_inv_sup).
-behavior(supervisor).
-export([start_link/0, init/1]).
-export([stop/0]).

start_link() ->
  supervisor:start_link({local,?MODULE},?MODULE, []).

stop() ->
  exit(whereis(?MODULE), shutdown).

init(_) ->
  ChildSpecList = [child(ms_db), child(ms_inv)], {ok,{{one_for_one, 1, 10}, ChildSpecList}}.

child(Module) ->
  {Module, {Module, start_link, []},
    permanent, 2000, worker, [Module]}.