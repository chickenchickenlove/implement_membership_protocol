%%%-------------------------------------------------------------------
%% @doc hyparview_and_plumtree_implementation public API
%% @end
%%%-------------------------------------------------------------------

-module(hyparview_and_plumtree_implementation_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    hyparview_and_plumtree_implementation_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
