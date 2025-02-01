-module(hypar_view_node).
-behavior(gen_server).


%% API% gen_server API.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

init([ClusterNodes, MyAlias]) ->
  ok.

handle_call(Msg, From, State) ->
  ok.

handle_cast(Msg, State) ->
  ok.

handle_info(Msg, State) ->
  ok.

