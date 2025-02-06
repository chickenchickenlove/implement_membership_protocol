-module(plum_tree_protocol).

%% API
-export([handle_msg/2]).

-record(plum_tree_msg, {
  %% TODO : I don't know real type now.
  message_id = 0 :: integer(),
  round = 0 :: integer(),
  endpoint = 0 :: integer()
}).

%%    { Self: Endpoint                                        // ID/address of current node
%%      EagerPushPeers: Set<Endpoint>                         // direct branches of peers to broadcast messages to
%%      LazyPushPeers: Set<Endpoint>                          // peers that periodically receive message IHave to detect undelivered messages
%%      LazyQueue: struct (MessageId * Round * Endpoint) list // lazy peers don't need to be informed right away, IHave can worked in batched manner
%%      Output: Reactor
%%      Config: Config
%%      ReceivedMessages: Map<MessageId, Gossip>              // received messages are stashed to purpose of redelivery. It may need to be prunned from time to time.
%%      Missing: Set<Announcement>                            // messages that are confirmed to be not received
%%      Timers: Set<MessageId> }
-type peer_name() :: atom().

-type announcement() :: #{sender => peer_name(),
                          message_id => integer(),
                          round => integer()}.
-record(?MODULE, {
  self = undefined :: pid(),
  eager_push_peers = #{} :: maps(atom(), pid()),
  lazy_push_peers = #{} :: maps(atom(), pid()),
  lazy_queue = #{},
  reactor = hypar_erl_reactor :: module(),
  % TODO: concreate type
  config = #{},
  % TODO
  received_messages = #{},  %:: maps(integer(), )
  missing = [] :: list(announcement()),
  timers = #{}
}).

handle_msg({neighbor_up, PeerName}=_Msg, State0) ->
  #?MODULE{eager_push_peers=EagerPushPeers0} = State0,
  PeerPid = find_util:get_node_pid(PeerName),
  EagerPushPeers = maps:put(PeerName, PeerPid, EagerPushPeers0),
  State0#?MODULE{eager_push_peers=EagerPushPeers};

handle_msg({neighbor_down, PeerName}=_Msg, State0) ->
  #?MODULE{eager_push_peers=EagerPushPeers0, lazy_push_peers=LazyPushPeers0, missing=Missing0} = State0,

  EagerPushPeers = maps:remove(PeerName, EagerPushPeers0),
  LazyPushPeers = maps:remove(PeerName, LazyPushPeers0),
  Missing = lists:filter(fun (Announcement) ->
                          Sender = maps:get(sender, Announcement),
                          Sender =/= PeerName
                         end, Missing0),

  State0#?MODULE{eager_push_peers=EagerPushPeers, lazy_push_peers=LazyPushPeers, missing=Missing};

handle_msg({broadcast, Data}=_Msg, State0) ->
  #?MODULE{received_messages=ReceivedMessages0} = State0,
  % binary
  MsgId = uuid:get_v4(),
  Gossip = #{msg_id := MsgId,
             round := 0,
             data := Data},

  ReceivedMessages = maps:put(MsgId, Gossip, ReceivedMessages0),
  Self = self(),
  SelfName = find_util:get_node_name_by_pid(Self),

  % Send LAZY PUSH Message
  LazyPushMsg = lazy_push_message(Gossip, SelfName, Self),

  % TODO : Maybe?
  send_message(self(), LazyPushMsg),

  EagerPushMsg = eager_push_message(Gossip, SelfName, Self),

  % TODO : Maybe?
  send_message(self(),  EagerPushMsg),

  State0#?MODULE{received_messages=ReceivedMessages};

handle_msg({eager_push, Gossip, PeerName, _PeerPid}=Msg, State) ->
  #?MODULE{eager_push_peers=EagerPushPeers} = State,

  SelfName = find_util:get_node_name_by_pid(self()),
  ShouldFiltered = [SelfName, PeerName],

  FilteredEagerPushPeer = maps:filter(
    fun(PeerName, _PeerPid) ->
      not lists:member(PeerName, ShouldFiltered)
    end, EagerPushPeers),

  maps:foreach(
    fun(ToPeerName, ToPeerPid) ->
      GossipMsg = gossip_message(Gossip, ToPeerName, ToPeerPid),
      send_message(ToPeerPid, GossipMsg)
    end, FilteredEagerPushPeer),

  State;

handle_msg(_Msg, State) ->
  State.


send_message(ToPid, Msg) ->
  % TODO
  ok.

gossip_message(Gossip, PeerName, PeerPid) ->
  {gossip, PeerName, PeerPid}.

eager_push_message(Gossip, PeerName, PeerPid) ->
  {eager_push, Gossip, PeerName, PeerPid}.

lazy_push_message(Gossip, PeerName, PeerPid) ->
  {lazy_push, Gossip, PeerName, PeerPid}.
