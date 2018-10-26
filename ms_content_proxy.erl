-module(ms_content_proxy).
-export([start_link/0,init/1,handle_call/3,handle_cast/2]).
-export([get/2,update/3,stop/0]).

start_link() ->
  {ok,[ContentNodes]} = file:consult(ms_content_nodes),
  gen_server:start_link({local, ?MODULE}, ?MODULE, {content_nodes, ContentNodes}, []).


init(Args) ->
  {content_nodes, Nodes} = Args,
  process_flag(trap_exit, true),
  PingNode = fun(N) ->
    Ping = net_adm:ping(N),
    io:format("node ~p, ping result: ~p~n", [N,Ping])
             end,
  lists:foreach(PingNode, Nodes),
  pg2:create(ms_content),
  {ok, []}.


get(ProductId, LanguageId) ->
  call({get, {ProductId, LanguageId}}).

update(ProductId, LanguageId, Content) ->
  call({update, {ProductId, LanguageId, Content}}).

stop() -> gen_server:cast(?MODULE, stop).

call(Msg) ->
  gen_server:call(?MODULE, Msg).

handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.

handle_call({get, {ProductId, LanguageId}}, _From, LoopData) ->
  {reply, get_content(ProductId, LanguageId), LoopData};

handle_call({update, {ProductId, LanguageId, Content}}, _From, LoopData) ->
  {reply, update_content(ProductId, LanguageId, Content), LoopData}.

get_content(ProductId, LanguageId) ->
  Node = get_active(),
  try ms_content:get(Node, ProductId, LanguageId) of
    Response -> Response
  catch
    _:_ -> ms_content:get(Node, ProductId, LanguageId)
  end.

update_content(ProductId, LanguageId, Content) ->
  %%% retry
  Node = get_active(),
  try ms_content:update(Node, ProductId, LanguageId, Content) of
    Response -> Response
  catch
    _:_ -> ms_content:update(Node, ProductId, LanguageId, Content)
  end.

get_active() ->
  Pids = pg2:get_members(ms_content),
  Pid = lists:nth(1, Pids),
  node(Pid).



