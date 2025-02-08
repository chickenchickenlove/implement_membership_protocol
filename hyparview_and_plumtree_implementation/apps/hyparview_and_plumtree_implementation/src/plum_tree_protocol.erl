-module(plum_tree_protocol).

%% API
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
-define(LAZY_PUST_INTERVAL, 3000).
-define(WAITING_TIME_MSG_ARRIVED, 1000).


-type peer_name() :: atom().
-type uuid() :: uuid:uuid().

-record(lazy_record, {
  %% TODO : I don't know real type now.
  message_id :: uuid(),
  round :: integer(),
  peer_name :: peer_name()
}).

-type lazy_record() :: #lazy_record{}.


-record(gossip, {message_id :: uuid(),
                 round = 0 :: integer(), % ROUND : for estimating distance between two nodes.
                 data :: binary()
}).

-record(announcement, {sender :: peer_name(),
                       message_id :: uuid(),
                       round :: integer()}).

-type gossip() :: #gossip{}.
-type announcement() :: #announcement{}.

-record(?MODULE, {self = undefined :: peer_name(),
                  eager_push_peers = sets:new() :: set(peer_name()),
                  lazy_push_peers = sets:new() :: set(peer_name()),
                  lazy_queue = [] :: list(lazy_record()),
                  reactor = hypar_erl_reactor :: module(),
                  % TODO: concreate type
                  config = #{},
                  % TODO: key : msgId, value: Gossip
                  received_messages = #{} :: maps(message_id(), gossip()),
                  missing = sets:new() :: set(announcement()),
                  timers = sets:new() :: set(uuid())
}).

% Missing: “IHave를 받았지만 아직 Graft가 날아오거나, 완전히 불필요하다고 확정되지 않은 메시지”를 추적.
% Timers: “타이머가 걸린 메시지 ID들”을 추적.


handle_msg({neighbor_up, PeerName}=_Msg, State0) ->
  #?MODULE{eager_push_peers=EagerPushPeers0} = State0,
  EagerPushPeers = sets:add_element(PeerName, EagerPushPeers0),
  State0#?MODULE{eager_push_peers=EagerPushPeers};

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

% 사용자가 호출 -> Gossip 메세지 전송 + Lazy Queue 업데이트.
handle_msg({broadcast, Data}=_Msg, State0) ->
  #?MODULE{received_messages=ReceivedMessages0} = State0,

  MsgId = uuid:get_v4(),
  Gossip = new_gossip(MsgId, Data),

  ReceivedMessages = maps:put(MsgId, Gossip, ReceivedMessages0),

  MyName = my_name(),
  State1 = eager_push(Gossip, MyName, State0),
  State2 = lazy_push(Gossip, MyName, State1),

  State2#?MODULE{received_messages=ReceivedMessages};

handle_msg({gossip, Gossip, PeerName}, State0) ->
  #?MODULE{received_messages=ReceivedMessages0} = State0,
  MessageId = get_from_gossip(message_id, Gossip),
  UpdatedState =
    case maps:get(MessageId, ReceivedMessages0, undefined) of
      % 누군가가 이전에 나에게 해당 메세지를 보냈었다 -> 연결된 Edge가 존재함을 의미.
      % 따라서, 지금 gossip으로 전달된 Edge는 제거되어야 함.
      undefined ->
        % 2. 아직 받지 않은 메세지인 경우
        ReceivedMessages = maps:put(MessageId, Gossip, ReceivedMessages0),
        NextRoundGossip = increment_round_gossip(Gossip),

        State1 = eager_push(NextRoundGossip, PeerName, State0),
        State2 = lazy_push(NextRoundGossip, PeerName, State1),

        State3 = add_eager_and_del_lazy(PeerName, State2),
        State3#?MODULE{received_messages=ReceivedMessages};
      _Message ->
        MyName = my_name(),
        ToPid = find_util:get_node_pid(PeerName),
        PruneMsg = prune_message(MyName),
        send_message(ToPid, PruneMsg),

        add_lazy_and_del_eager(PeerName, State0)
  end,
  invalidate_missing_with_given(Gossip, UpdatedState);

handle_msg({prune, PeerName}, State0) ->
  add_lazy_and_del_eager(PeerName, State0);

% Message 이름은 바껴야 할지도.
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
      % TODO: It can be replaced with `bloom filter` to improve performance.
      % 나는 이런 메세지를 가지고 있다고, 다른 Peer들에게 보낸다.
      IHaveMsg = ihave_message(Grafts, PeerName),
      ToPid = find_util:get_node_pid(PeerName),
      send_message(ToPid, IHaveMsg)
    end, LazyRecords),

  State;


% ihave 메세지에 대해 바로 응답하면 안된다.
% 1. IHave 메세지가 실제 가십 메세지보다 먼저 도착했을 수도 있다. (네트워크 지연으로 인해)
%    - 만약 이것을 무시하고, 즉시 대답하면 클러스터 전체가 불안할 수 있음.
% 따라서 지연시간을 도입한 후에 응답해야한다.

% For tree repair.
handle_msg({ihave, Grafts, Sender}, State0) ->

  State1 = lists:foldl(
    fun({MsgId, Round}, AccState) ->
      if_i_have(AccState, MsgId, Round, Sender)
    end, State0, Grafts),

  % 이미 가지고 있는 타이머는 제외하고, 새롭게 들어온 타이머만 한다.
  % 이미 가지고 있는 타이머는 스케쥴 되었을 것이기 때문이다.

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


% For tree repair.
%
handle_msg({wait_message_arrived, MessageId}, State0) ->
  #?MODULE{missing=Missing0, timers=Timers0} = State0,
  Timers = sets:del_element(MessageId, Timers0),

  {Missing, Announcement} = poll_announcement_randomly(Missing0, MessageId),

  State =
    case Announcement of
      undefined -> State0;
      #{sender => SenderName, round => Round} = Announcement ->
        SenderName = maps:get(sender, Announcement),
        SenderPid = find_util:get_node_pid(SenderName),

        % 나만 eager로 추가할 수도 있겠네.
        % 상대방에게 닿지 않은 경우에는 eager를 제거해야할 것 같음.
        % prune에 의해서 제거될 것 같기도 한데?
        State1 = add_eager_and_del_lazy(SenderName, State0),

        MyName = my_name(),
        GraftMsg = graft_message(MessageId, Round, MyName),
        send_message(SenderPid, GraftMsg),
        State1
    end,

  State#?MODULE{missing=Missing, timers=Timers};

% For tree repair.
handle_msg({graft, MessageId, _Round, PeerName}, State0) ->
  State1 = add_eager_and_del_lazy(PeerName, State0),
  #?MODULE{received_messages=ReceivedMessages} = State1,

  Gossip = maps:get(MessageId, ReceivedMessages),
  Myname = find_util:get_node_name_by_pid(self()),
  GossipMsg = gossip_message(Gossip, Myname),

  ToPid = find_util:get_node_pid(PeerName),
  send_message(ToPid, GossipMsg),
  State1;

handle_msg({lazy_push_expired, ExpiredLazyRecord}, State0) ->
  #?MODULE{lazy_queue=LazyQueue0} = State0,
  LazyQueue = lists:delete(ExpiredLazyRecord, LazyQueue0),
  State0#?MODULE{lazy_queue=LazyQueue};

handle_msg(_Msg, State) ->
  State.

% Schedule function
schedule_lazy_push_expired(LazyRecord) ->
  LazyPushExpiredMsg = {lazy_push_expired, LazyRecord},
  send_message_after(?LAZY_PUSH_TTL, self(), LazyPushExpiredMsg).

schedule_dispatch() ->
  DispatchMsg = dispatch_message(),
  send_message_after(?LAZY_PUST_INTERVAL, self(), DispatchMsg).

schedule_wait_a_while(MessageId) ->
  WaitMsgArrivedForAWhileMsg = wait_message_arrived_for_a_while_message(MessageId),
  send_message_after(?WAITING_TIME_MSG_ARRIVED, self(), WaitMsgArrivedForAWhileMsg).

send_message_after(After, ToPid, Message) ->
  timer:apply_after(After, gen_server, cast, [ToPid, Message]).

send_message(ToPid, Msg) ->
  gen_server:cast(ToPid, Msg).

% For eager_push, On graft message.
gossip_message(Gossip, PeerName) ->
  {gossip, Gossip, PeerName}.

graft_message(MessageId, Round, PeerName) ->
  {graft, MessageId, Round, PeerName}.

ihave_message(Grafts, PeerName) ->
  {ihave, Grafts, PeerName}.

dispatch_message() ->
  {dispatch}.

wait_message_arrived_for_a_while_message(MessageId) ->
  {wait_message_arrived, MessageId}.

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
    fun(PeerName) ->
      not lists:member(PeerName, ShouldFiltered)
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
