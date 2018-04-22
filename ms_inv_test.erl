-module(ms_inv_test).
-export([start_link/0,init/1,stop/0,handle_cast/2,handle_call/3]).
-export([test/1]).
-record(testData, {startTime, productId, warehouseId}).


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_Args) ->
  {ok, DB} = dets:open_file(msinvtestdb, [{type, set}, {file, msinvtestdb}]),
  Response = dets:lookup(DB, requests),
  case Response of
    [{response, 0}] -> {ok, [DB]};
    [] -> {ok, DB}
  end,
  {ok, [DB]}.


call(Msg) ->
  gen_server:call(?MODULE, Msg).


stop() ->
  gen_server:cast(?MODULE, stop).

test(N) ->
  call({test, N}).


handle_cast(stop, LoopData) ->
  {stop, normal, LoopData}.


handle_call({test, N}, _From, LoopData) ->
  {reply, test_init(N, LoopData), LoopData}.

test_init(N, LoopData) ->
  io:format("### init test: ~n"),
  TestData = #testData{ startTime = erlang:timestamp(), productId = 9999, warehouseId = uk},
  test(N, TestData, LoopData).

test(0, TestData, LoopData) ->
  [DB] = LoopData,
  ok = dets:insert(DB, {requests, 0}),
  ProductId = TestData#testData.productId,
  WarehouseId = TestData#testData.warehouseId,
  io:format("#~p~n",[ms_inv_proxy:get(ProductId,WarehouseId)]),
  Time = timer:now_diff(erlang:timestamp(), TestData#testData.startTime),
  Seconds = Time / 1000000,
  io:format("time: ~p~n", [Seconds]);


test(N, TestData, LoopData) ->
  [DB] = LoopData,
  ok = dets:insert(DB, {requests, N}),

  ProductId = TestData#testData.productId,
  WarehouseId = TestData#testData.warehouseId,
  ResponseRemove = ms_inv_proxy:remove(ProductId, WarehouseId,1),
  case ResponseRemove of
    {ok, _} -> ok;
    {error, _} -> io:format("remove error ~p~n", [ResponseRemove])
  end,



  Response = ms_inv_client:add(ProductId, WarehouseId,1),
  case Response of
    {ok, _Data} -> ok;
    {error, Error} -> io:format("add error ~p~n", [Error])
  end,


  Requests = N - 1,
  test(Requests, TestData, LoopData).

