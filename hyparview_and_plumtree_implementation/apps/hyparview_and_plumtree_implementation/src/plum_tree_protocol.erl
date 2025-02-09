-module(plum_tree_protocol).

%% API
-export([broadcast/2]).
-export([handle_msg/2]).
-export([schedule_dispatch/0]).
-compile(no_warn_unused_variables).


%%%
% Lazy push : Send `IHave` Message to lazy peers.
% - Lazy Queue에 있는 것을 Dispatch 되는 시점에 `IHave()` 메세지를 만들어보냄.
% Eager Push : Send Gossip Message to eager peers.
% - 자신이 알고 있는 것을 바로 Gossiping 한다.
% - Graft Message
%   - 나는 그 메세지가 없으니, 나에게 그 메세지 전문을 보내줘.




% 15 seconds
-define(LAZY_PUSH_TTL, 15000).

% 60 seconds
-define(RECEIVED_MESSAGE_EXPIRED_TIME, 60000).

% 60 seconds
-define(CACHED_GRAFT_MESSAGE_EXPIRED_TIME, 60000).

-define(LAZY_BLOOM_FILTER_PUSH_INTERVAL, 1000).
-define(LAZY_PUSH_INTERVAL, 3000).
-define(WAITING_TIME_MSG_ARRIVED, 1000).
-define(IGNORE_ROUND, -1).

-type peer_name() :: atom().
-type uuid() :: uuid:uuid().
-type message_id() :: uuid:uuid().

-record(lazy_record, {
  %% TODO : I don't know real type now.
  message_id :: message_id(),
  round :: integer(),
  peer_name :: peer_name()
}).

-type lazy_record() :: #lazy_record{}.


-record(gossip, {message_id :: message_id(),
                 round = 0 :: integer(), % ROUND : for estimating distance between two nodes.
                 data :: binary()
}).

-record(announcement, {sender :: peer_name(),
                       message_id :: message_id(),
                       round :: integer()}).

-type gossip() :: #gossip{}.
-type announcement() :: #announcement{}.

-record(?MODULE, {self = undefined :: peer_name(),
                  eager_push_peers = sets:new() :: sets:set(peer_name()),
                  lazy_push_peers = sets:new() :: sets:set(peer_name()),
                  lazy_queue = [] :: list(lazy_record()),
                  reactor = hypar_erl_reactor :: module(),
                  % TODO: concreate type
                  config = #{},
                  % TODO: key : msgId, value: Gossip
                  received_messages = #{} :: #{message_id() => gossip()},
                  missing = sets:new() :: sets:set(announcement()),
                  timers = sets:new() :: sets:set(message_id()),
                  cached_graft_msg = sets:new() :: sets:set({message_id(), peer_name()})
}).

% Missing: “IHave를 받았지만 아직 Graft가 날아오거나, 완전히 불필요하다고 확정되지 않은 메시지”를 추적.
% Timers: “타이머가 걸린 메시지 ID들”을 추적.

broadcast(NodeName, Data) ->
  Data = {broadcast, Data},
  ToPid = find_util:get_node_pid(NodeName),
  gen_server:cast(ToPid, Data).


% From HyParView or other membership protocol.
handle_msg({neighbor_up, PeerName}=_Msg, State0) ->
  #?MODULE{eager_push_peers=EagerPushPeers0} = State0,
  EagerPushPeers = sets:add_element(PeerName, EagerPushPeers0),
  State0#?MODULE{eager_push_peers=EagerPushPeers};

% From HyParView or other membership protocol.
handle_msg({neighbor_down, PeerName}=_Msg, State0) ->
  #?MODULE{eager_push_peers=EagerPushPeers0, lazy_push_peers=LazyPushPeers0, missing=Missing0} = State0,

  EagerPushPeers = sets:del_element(PeerName, EagerPushPeers0),
  LazyPushPeers = sets:del_element(PeerName, LazyPushPeers0),

  Missing = sets:filter(
    fun(Announcement) ->
      Sender = maps:get(sender, Announcement),
      Sender =/= PeerName
    end, Missing0),
  State0#?MODULE{eager_push_peers=EagerPushPeers, lazy_push_peers=LazyPushPeers, missing=Missing};

% For Client
handle_msg({broadcast, Data}=_Msg, State0) ->
  #?MODULE{received_messages=ReceivedMessages0} = State0,

  MsgId = uuid:get_v4(),
  Gossip = new_gossip(MsgId, Data),

  ReceivedMessages = maps:put(MsgId, Gossip, ReceivedMessages0),

  MyName = my_name(),
  State1 = eager_push(Gossip, MyName, State0),
  State2 = lazy_push(Gossip, MyName, State1),

  schedule_received_message_expire(Gossip),
  State2#?MODULE{received_messages=ReceivedMessages};

handle_msg({gossip, Gossip, PeerName}, State0) ->
  #?MODULE{received_messages=ReceivedMessages0} = State0,
  MessageId = get_from_gossip(message_id, Gossip),
  UpdatedState =
    case maps:get(MessageId, ReceivedMessages0, undefined) of
      undefined ->
        ReceivedMessages = maps:put(MessageId, Gossip, ReceivedMessages0),
        NextRoundGossip = increment_round_gossip(Gossip),

        State1 = eager_push(NextRoundGossip, PeerName, State0),
        State2 = lazy_push(NextRoundGossip, PeerName, State1),

        State3 = add_eager_and_del_lazy(PeerName, State2),
        schedule_received_message_expire(Gossip),
        State3#?MODULE{received_messages=ReceivedMessages};
      _Message ->
        % It means that we have other eager edge.
        % So, We should remove this edge.
        MyName = my_name(),
        ToPid = find_util:get_node_pid(PeerName),
        PruneMsg = prune_message(MyName),
        send_message(ToPid, PruneMsg),

        add_lazy_and_del_eager(PeerName, State0)
  end,
  invalidate_missing_with_given(Gossip, UpdatedState);

handle_msg({prune, PeerName}, State0) ->
  add_lazy_and_del_eager(PeerName, State0);

handle_msg({dispatch}, #?MODULE{lazy_queue=LazyQueue}=State) ->
  LazyRecords =
    lists:foldl(
      fun(LazyRecord, Acc0) ->
        #lazy_record{message_id=MsgId, round=Round, peer_name=PeerName} = LazyRecord,
        GossipMsgsForPeer = maps:get(PeerName, Acc0, []),
        maps:put(PeerName, [ {MsgId, Round} | GossipMsgsForPeer], Acc0)
      end, #{}, LazyQueue),

  maps:foreach(
    fun(PeerName, Grafts) ->
      IHaveMsg = ihave_message(Grafts, PeerName),
      ToPid = find_util:get_node_pid(PeerName),
      send_message(ToPid, IHaveMsg)
    end, LazyRecords),

  schedule_dispatch(),

  State;

handle_msg({dispatch_short}, #?MODULE{lazy_queue=LazyQueue}=State) ->
  BloomFiltersForEachLazyPeers =
    lists:foldl(
      fun(LazyRecord, Acc) ->
        #lazy_record{message_id=MsgId, peer_name=PeerName} = LazyRecord,
        BloomFilter0 = maps:get(PeerName, Acc, bloom_filter:new()),
        BloomFilter1 = bloom_filter:add(MsgId, BloomFilter0),
        maps:put(PeerName, BloomFilter1, Acc)
      end, #{}, LazyQueue),

  maps:foreach(
    fun(PeerName, BloomFilter) ->
      IHaveBloomMsg = ihave_bloom_message(BloomFilter, PeerName),
      ToPid = find_util:get_node_pid(PeerName),
      send_message(ToPid, IHaveBloomMsg)
    end, BloomFiltersForEachLazyPeers),

  schedule_short_dispatch(),
  State;

% For tree repair.
handle_msg({ihave, Grafts, Sender}, State0) ->
  % We should wait a while before asking graft message.
  % Because lazy push may arrive before eager push.
  State1 = lists:foldl(
    fun({MsgId, Round}, AccState) ->
      if_i_have(AccState, MsgId, Round, Sender)
    end, State0, Grafts),

  #?MODULE{timers=AlreadyHasTimer} = State0,
  #?MODULE{timers=MaybeNewlyGetTimer} = State1,

  NewlyReceivedTimer = sets:filter(
    fun(Timer, _) ->
      not sets:is_element(Timer, AlreadyHasTimer)
    end, MaybeNewlyGetTimer),

  lists:foreach(
    fun(MessageId) ->
      schedule_wait_a_while(MessageId)
    end, sets:to_list(NewlyReceivedTimer)),

  State1;

handle_msg({ihave_bloom, BloomFilter, Sender}, State0) ->
  #?MODULE{missing=Missing0, timers=Timers0, cached_graft_msg=CachedGraftMsg0} = State0,

  MissingMsgs = lists:foldl(
    fun(MissingElement, Acc) ->
      {MessageId, _Round, _Sender} = MissingElement,
      sets:add_element(MessageId, Acc)
    end, sets:new(), Missing0),

  NotReservedMsgYet = lists:filter(
    fun(MessageId) ->
      not lists:member(MessageId, Timers0)
    end, MissingMsgs),

  {Timers, CachedGraftMsg, ReservedMsgAndSender} = lists:foldl(
    fun(MessageId, {TimersAcc, CachedGraftMsgAcc, ReservedMsgAndSenderAcc}) ->
      case bloom_filter:member(MessageId, BloomFilter) of
        true ->
          MsgAndSender = {MessageId, Sender},
          case sets:is_element(MsgAndSender, CachedGraftMsgAcc) of
            true -> {TimersAcc, CachedGraftMsg, ReservedMsgAndSenderAcc};
            false ->
              CachedGraftMsgAcc1 = sets:add_element(MsgAndSender, CachedGraftMsgAcc),
              TimersAcc1 = sets:add_element(MessageId, TimersAcc),
              ReservedMsgAndSenderAcc1 = [MsgAndSender | ReservedMsgAndSenderAcc],

              schedule_wait_a_while_with_bloom_filter(MessageId, Sender),
              {TimersAcc1, CachedGraftMsgAcc1, ReservedMsgAndSenderAcc1}
          end;
        false -> {TimersAcc, CachedGraftMsg, ReservedMsgAndSenderAcc}
      end
    end, {Timers0, CachedGraftMsg, []}, NotReservedMsgYet),

  schedule_cached_graft_message_expire(ReservedMsgAndSender),

  State0#?MODULE{timers=Timers};

% For tree repair.
handle_msg({wait_message_arrived, MessageId}, State0) ->
  #?MODULE{missing=Missing0, timers=Timers0, received_messages=ReceivedMessage} = State0,
  Timers = sets:del_element(MessageId, Timers0),

  State =
    case maps:is_key(MessageId, ReceivedMessage) of
      true -> State0;
      false ->
        {Missing, Announcement} = poll_announcement_randomly(Missing0, MessageId),
        case Announcement of
          undefined -> State0;
          #{sender := SenderName, round := Round} = Announcement ->
            SenderName = maps:get(sender, Announcement),
            SenderPid = find_util:get_node_pid(SenderName),

            State1 = add_eager_and_del_lazy(SenderName, State0),

            MyName = my_name(),
            GraftMsg = graft_message(MessageId, Round, MyName),
            send_message(SenderPid, GraftMsg),
            State1
        end
    end,

  State#?MODULE{missing=Missing, timers=Timers};

handle_msg({wait_message_arrived_with_bloom, MessageId, Sender}, State0) ->
  #?MODULE{timers=Timers0, received_messages=ReceivedMessage} = State0,
  Timers = sets:del_element(MessageId, Timers0),

  State =
    case maps:is_key(MessageId, ReceivedMessage) of
      true -> State0;
      false ->
        State1 = add_eager_and_del_lazy(Sender, State0),
        MyName = my_name(),
        GraftMsg = graft_message(MessageId, ?IGNORE_ROUND, MyName),

        send_message(find_util:get_node_pid(Sender), GraftMsg),
        State1
    end,

  State#?MODULE{timers=Timers};


% For tree repair.
handle_msg({graft, MessageId, _Round, PeerName}, State0) ->
  State1 = add_eager_and_del_lazy(PeerName, State0),
  #?MODULE{received_messages=ReceivedMessages} = State1,

  case maps:get(MessageId, ReceivedMessages, undefined) of
    % Considering false-positive caused by bloom filter.
    % Or it might already be expired.
    undefined -> State1;
    Gossip ->
      Gossip = maps:get(MessageId, ReceivedMessages),
      Myname = find_util:get_node_name_by_pid(self()),
      GossipMsg = gossip_message(Gossip, Myname),

      ToPid = find_util:get_node_pid(PeerName),
      send_message(ToPid, GossipMsg)
  end,

  State1;

handle_msg({lazy_push_expired, ExpiredLazyRecord}, State0) ->
  #?MODULE{lazy_queue=LazyQueue0} = State0,
  LazyQueue = lists: delete(ExpiredLazyRecord, LazyQueue0),
  State0#?MODULE{lazy_queue=LazyQueue};

handle_msg({received_message_expired, MessageId}, State0) ->
  #?MODULE{received_messages=ReceivedMessages0} = State0,
  ReceivedMessages = maps:remove(MessageId, ReceivedMessages0),
  State0#?MODULE{received_messages=ReceivedMessages};

handle_msg({cached_graft_message_expire, ReservedMsgAndSenderList}, State0) ->
  #?MODULE{cached_graft_msg=CachedGraftMessages0} = State0,

  CachedGraftMessage = lists:foldl(
    fun(MsgAndSender, Acc) ->
      sets:del_element(MsgAndSender, Acc)
    end, CachedGraftMessages0, ReservedMsgAndSenderList),
  State0#?MODULE{cached_graft_msg=CachedGraftMessage};

handle_msg(_Msg, State) ->
  State.

% Schedule function
schedule_lazy_push_expired(LazyRecord) ->
  LazyPushExpiredMsg = {lazy_push_expired, LazyRecord},
  send_message_after(?LAZY_PUSH_TTL, self(), LazyPushExpiredMsg).

schedule_received_message_expire(Gossip) ->
  #gossip{message_id=MessageId} = Gossip,
  ReceivedMessageExpiredMsg = {received_message_expired, MessageId},
  send_message_after(?RECEIVED_MESSAGE_EXPIRED_TIME, self(), ReceivedMessageExpiredMsg).

schedule_cached_graft_message_expire(ReservedMsgAndSenderList) ->
  CachedGraftMessageExpire = {cached_graft_message_expire, ReservedMsgAndSenderList},
  send_message_after(?RECEIVED_MESSAGE_EXPIRED_TIME, self(), CachedGraftMessageExpire).

schedule_dispatch() ->
  DispatchMsg = dispatch_message(),
  send_message_after(?LAZY_PUSH_INTERVAL, self(), DispatchMsg).

schedule_short_dispatch() ->
  DispatchShortMsg = dispatch_short_message(),
  send_message_after(?LAZY_BLOOM_FILTER_PUSH_INTERVAL, self(), DispatchShortMsg).

schedule_wait_a_while(MessageId) ->
  WaitMsgArrivedForAWhileMsg = wait_message_arrived_for_a_while_message(MessageId),
  send_message_after(?WAITING_TIME_MSG_ARRIVED, self(), WaitMsgArrivedForAWhileMsg).

schedule_wait_a_while_with_bloom_filter(MessageId, Sender) ->
  WaitMsgArrivedForAWhileMsg = wait_message_arrived_for_a_while_message_with_bloom(MessageId, Sender),
  send_message_after(?WAITING_TIME_MSG_ARRIVED, self(), WaitMsgArrivedForAWhileMsg).

send_message_after(After, ToPid, Message) ->
  timer:apply_after(After, gen_server, cast, [ToPid, Message]).

send_message(ToPid, Msg) ->
  gen_server:cast(ToPid, Msg).

% Plumtree protocol message.
gossip_message(Gossip, PeerName) ->
  {gossip, Gossip, PeerName}.

graft_message(MessageId, Round, PeerName) ->
  {graft, MessageId, Round, PeerName}.

ihave_message(Grafts, PeerName) ->
  {ihave, Grafts, PeerName}.

ihave_bloom_message(BloomFilter, PeerName) ->
  {ihave_bloom, BloomFilter, PeerName}.

dispatch_message() ->
  {dispatch}.

dispatch_short_message() ->
  {dispatch_short}.

wait_message_arrived_for_a_while_message(MessageId) ->
  {wait_message_arrived, MessageId}.

wait_message_arrived_for_a_while_message_with_bloom(MessageId, Sender) ->
  {wait_message_arrived_with_bloom, MessageId, Sender}.

prune_message(PeerName) ->
  {prune, PeerName}.

%%% Private function for plum tree
% It can return `undefined`.
poll_announcement_randomly(Missing0, MessageId) ->

  MissingMatchedWithMsgId = lists:filter(
    fun(Announcement) ->
      AnnouncementMsgId = maps:get(message_id, Announcement),
      MessageId =:= AnnouncementMsgId
    end, sets:to_list(Missing0)),

  Size = length(MissingMatchedWithMsgId),

  case length(MissingMatchedWithMsgId) of
    Size when Size =:= 0 -> {Missing0, undefined};
    Size ->
      RandomIndex = rand:uniform(Size),
      PickedAnnouncement = lists:nth(RandomIndex, MissingMatchedWithMsgId),
      Missing = sets:del_element(PickedAnnouncement, Missing0),

      {Missing, PickedAnnouncement}
  end.

if_i_have(State0, MessageId, Round, Sender) ->
  #?MODULE{received_messages=ReceivedMessage, timers=Timers0, missing=Missing0} = State0,
  case maps:is_key(MessageId, ReceivedMessage) of
    false ->
      % If this message is first seen, we should wait a while.
      % Because, the 'lazy push' can arrive before the 'eager push' if the 'eager push' encounter network latencey.
      case sets:is_element(MessageId, Timers0) of
        % Already reserved. thus, we should ignore this cases to escape cyclic path.
        true ->  State0;
        false ->
          Timers = sets:add_element(MessageId, Timers0),
          MissingElement = {MessageId, Round, Sender},
          Missing = sets:add_element(MissingElement, Missing0),
          State0#?MODULE{timers=Timers, missing=Missing}
      end;
    true -> State0
  end.

add_eager_and_del_lazy(PeerName, State0) ->
  % DONE
  #?MODULE{eager_push_peers=EagerPushPeers0, lazy_push_peers=LazyPushPeers0} = State0,
  EagerPushPeers = sets:add_element(PeerName, EagerPushPeers0),
  LazyPushPeers = sets:del_element(PeerName, LazyPushPeers0),

  State0#?MODULE{eager_push_peers=EagerPushPeers, lazy_push_peers=LazyPushPeers}.

add_lazy_and_del_eager(PeerName, State0) ->
  % DONE
  #?MODULE{eager_push_peers=EagerPushPeers0, lazy_push_peers=LazyPushPeers0} = State0,
  EagerPushPeers = sets:del_element(PeerName, EagerPushPeers0),
  LazyPushPeers = sets:add_element(PeerName, LazyPushPeers0),

  State0#?MODULE{eager_push_peers=EagerPushPeers, lazy_push_peers=LazyPushPeers}.

eager_push(Gossip, PeerName, State) ->
  #?MODULE{eager_push_peers=EagerPushPeers} = State,

  MyName = my_name(),
  ShouldFiltered = [MyName, PeerName],

  FilteredEagerPushPeer = sets:filter(
    fun(PeerNameInFunc) ->
      not lists:member(PeerNameInFunc, ShouldFiltered)
    end, EagerPushPeers),

  lists:foreach(
    fun(ToPeerName) ->
      ToPeerPid = find_util:get_node_pid(ToPeerName),
      GossipMsg = gossip_message(Gossip, ToPeerName),
      send_message(ToPeerPid, GossipMsg)
    end, sets:to_list(FilteredEagerPushPeer)
  ),

  State.

lazy_push(Gossip, PeerName, State0) ->
  #?MODULE{lazy_queue=LazyQueue0, lazy_push_peers=LazyPushPeers0} = State0,
  #gossip{message_id=MessageId, round=Round} = Gossip,

  LazyPushPeers = sets:del_element(PeerName, LazyPushPeers0),
  LazyQueue = lists:foldl(
    fun(LazyPeer, Acc) ->
      LazyRecord = #lazy_record{message_id=MessageId, round=Round, peer_name=LazyPeer},
      schedule_lazy_push_expired(LazyRecord),
      [LazyRecord | Acc]
    end, LazyQueue0, sets:to_list(LazyPushPeers)),


  State0#?MODULE{lazy_queue=LazyQueue}.

invalidate_missing_with_given(Gossip, State0) ->
  #?MODULE{missing=Missing0} = State0,
  #gossip{message_id=MessageId} = Gossip,
  Missing = lists:filter(
    fun(Announcement) ->
      AnnounceMsgId = maps:get(message_id, Announcement),
      not AnnounceMsgId =:= MessageId
    end, sets:to_list(Missing0)),
  State0#?MODULE{missing=Missing}.


%%% Util function
my_name() ->
  find_util:get_node_name_by_pid(self()).

%%% For Gossip record
new_gossip(MessageId, Data) ->
  #gossip{message_id=MessageId, round=0, data=Data}.

increment_round_gossip(#gossip{round=Round} = Gossip0) ->
  Gossip0#gossip{round=Round+1}.

get_from_gossip(message_id, Gossip) ->
  #gossip{message_id=MessageId} = Gossip,
  MessageId;
get_from_gossip(round, Gossip) ->
  #gossip{round=Round} = Gossip,
  Round;
get_from_gossip(data, Gossip) ->
  #gossip{data=Data} = Gossip,
  Data.
