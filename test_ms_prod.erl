-module(test_ms_prod).
-export([start_with_updates/3, start/3,loop/0,result/0,inv_updates_loop/0,price_updates_loop/0,content_updates_loop/0]).


start(Processes, Requests, ProductsPerRequest) ->
  start(Processes, Requests, ProductsPerRequest, false).

start_with_updates(Processes, Requests, ProductsPerRequest) ->
  start(Processes, Requests, ProductsPerRequest, true).

start(Processes, Requests, ProductsPerRequest, Updates) ->

  ms_inv_proxy:start_link(),
  ms_prod_proxy:start_link(),
  ms_price_proxy:start_link(),
  ms_price_proxy:start_link(),
  case ets:info(?MODULE) of
    undefined ->
      ets:new(?MODULE, [named_table, public]);
    _ ->
      ets:delete_all_objects(?MODULE)
  end,
  Start = 1,
  init(Processes, Requests, ProductsPerRequest, Start, Updates).

init(0, _, _, _, _) -> ok;

init(Processes, Requests, ProductsPerRequest, Start, Updates) ->
  List = lists:seq(Start, Start + (ProductsPerRequest - 1)),
  Pid = spawn(?MODULE, loop, []),
  Pid ! {requests, Requests, List},

  case Updates of
    true ->
      InvUpdatePid = spawn(?MODULE, inv_updates_loop, []),
      send_inv_update(InvUpdatePid , List, Pid),

      PriceUpdatePid = spawn(?MODULE, price_updates_loop, []),
      send_price_update(PriceUpdatePid , List, Pid),

      ContentUpdatePid = spawn(?MODULE, content_updates_loop, []),
      send_content_update(ContentUpdatePid , List, Pid);


    false ->
      ok
  end,

  io:format("started process: ~p~n", [Pid]),
  init(Processes - 1, Requests, ProductsPerRequest, Start + ProductsPerRequest, Updates).

loop() ->
  receive
    {requests, 0, _List} ->
      io:format("finished process: ~p~n", [self()]),
      ok;

    {requests, Requests, List} ->
      [ProdId|T] = List,
      Response = ms_prod_proxy:get(ProdId, uk, en),
      ets:insert(?MODULE, {erlang:timestamp(), prod_request, Response}),
      NewList = T ++ [ProdId],
      send_prod_request(self(), Requests - 1, NewList),
      loop()
  end.

inv_updates_loop() ->

  receive
    {inv_update, List, ProdPid} ->
      [ProdId|T] = List,
      Response = ms_inv_proxy:add(ProdId, uk, 1),
      ets:insert(?MODULE, {erlang:timestamp(), inv_update, Response}),
      NewList = T ++ [ProdId],
      case erlang:process_info(ProdPid) of
        undefined ->
          io:format("### stopping inv updates process~n");
        _ ->
          timer:sleep(1000),
          NewList = T ++ [ProdId],
          send_inv_update(self(), NewList, ProdPid),
          inv_updates_loop()
      end
  end.


price_updates_loop() ->

  receive
    {price_update, List, ProdPid} ->
      [ProdId|T] = List,
      Response = ms_price_proxy:update(ProdId, uk, ProdId, 20),
      ets:insert(?MODULE, {erlang:timestamp(), price_update, Response}),
      NewList = T ++ [ProdId],
      case erlang:process_info(ProdPid) of
        undefined ->
          io:format("### stopping price updates process~n");
        _ ->
          timer:sleep(5000),
          NewList = T ++ [ProdId],
          send_price_update(self(), NewList, ProdPid),
          price_updates_loop()
      end
  end.


content_updates_loop() ->

  receive
    {content_update, List, ProdPid} ->
      [ProdId|T] = List,
      Title = string:concat("Test Title Product ", integer_to_list(ProdId)),
      Description = string:concat("Test Description Product ", integer_to_list(ProdId)),
      Content = [{title, Title}, {description, Description}],
      Response = ms_content_proxy:update(ProdId, en, Content),
      ets:insert(?MODULE, {erlang:timestamp(), content_update, Response}),
      NewList = T ++ [ProdId],
      case erlang:process_info(ProdPid) of
        undefined ->
          io:format("### stopping content updates process~n");
        _ ->
          timer:sleep(40000),
          NewList = T ++ [ProdId],
          send_content_update(self(), NewList, ProdPid),
          content_updates_loop()
      end
  end.

send_inv_update(Pid, List, ProdPid) ->
  Pid ! {inv_update, List, ProdPid}.

send_price_update(Pid, List, ProdPid) ->
  Pid ! {price_update, List, ProdPid}.

send_content_update(Pid, List, ProdPid) ->
  Pid ! {content_update, List, ProdPid}.

send_prod_request(Pid, Requests, List) ->
  Pid ! {requests, Requests, List}.


result() ->


  List = lists:filter(result_filter(prod_request), ets:tab2list(?MODULE)),
  InvUpdates = lists:filter(result_filter(inv_update), ets:tab2list(?MODULE)),
  PriceUpdates = lists:filter(result_filter(price_update), ets:tab2list(?MODULE)),
  ContentUpdates = lists:filter(result_filter(content_update), ets:tab2list(?MODULE)),
  TimeStampSort = fun({T1 ,_, _ },{T2 ,_, _ }) -> T1 =< T2 end,
  SortedList = lists:sort(TimeStampSort, List),
  [{First,_,_}|_] = SortedList,
  {Last,_,_} = lists:last(SortedList),
  Time = timer:now_diff(Last, First),
  Seconds = Time / 1000000,
  NumberOfOperations = length(SortedList),
  OperationsPerSecond = length(SortedList) / Seconds,

  NotOkFun = fun(Elem) -> Elem == {error, not_found} end,
  ErrorList = lists:filter(NotOkFun, List),

  {{seconds, Seconds},
    {number_of_prod_requests, NumberOfOperations},
    {inv_updates, length(InvUpdates)},
    {price_updates, length(PriceUpdates)},
    {content_updates, length(ContentUpdates)},
    {error_list, ErrorList},
    {operations_per_second, OperationsPerSecond}}.


result_filter(ReqName) ->
  fun(E) ->
    case E of
      {_, ReqName, _} -> true;
      _ -> false
    end
  end.