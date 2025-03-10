-module(hypar_erl_reactor).


% For using it between only erlang actor process.
% If we want to HyParView protocol in other environment,
% We should implement hypar_reactor interface for it.
% The 'Reactor' interface handle network and events.


%% Reactor API
-export([send/2, disconnect/1, notify/1]).


% Reactor abstract
send(Target, Message) ->
  % In TCP Condition,
  % We should send a message to Node with IP.
  gen_server:cast(Target, Message).

disconnect(Endpoint) ->
  % In TCP Condition,
  % We should disconnect TCP socket when this function is called.
  ok.

notify(PeerEvent) ->
  % TODO
  % 1. Create ETS Table
  % 2. Put event to ETS Table.
  ok.