-module(hypar_erl_reactor).


% For using it between only erlang actor process.
% If we want to HyParView protocol in other environment,
% We should implement hypar_reactor interface for it.
% The 'Reactor' interface handle network and events.


%% Reactor API
-export([send/2, disconnect/1, notify/1]).


% Reactor abstract
send(Target, Message) ->
  gen_server:cast(Target, Message).

disconnect(Endpoint) ->
  ok.

notify(PeerEvent) ->
  % TODO
  % 1. Create ETS Table
  % 2. Put event to ETS Table.
  ok.