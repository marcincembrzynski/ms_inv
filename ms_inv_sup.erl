-module(ms_inv_sup).
-behavior(supervisor).
-export([start_link/0, init/1]).
-export([stop/0]).

start_link() ->
  supervisor:start_link({local,?MODULE}, ?MODULE, []).

stop() ->
  io:format("### stopping ~n"),
  exit(whereis(?MODULE), shutdown).


init(_) ->
  ChildSpecList = [child(ms_db, [ms_inv_db]), child(ms_inv, []), child(ms_log, ["_ms_inv_log"])], {ok,{{one_for_one, 1, 5}, ChildSpecList}}.

child(Module, Args) ->
  {Module, {Module, start_link, Args},
    permanent, 2000, worker, [Module]}.