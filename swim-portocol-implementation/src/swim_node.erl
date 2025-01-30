-module(swim_node).
-behavior(gen_server).

%% API
-export([start_link/2]).

% gen_server callback
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([main/0]).
-export([send_message/2]).


% 5s
-define(DEFAULT_INIT_JOIN_TIMEOUT, 11000).
-define(NEXT_ROUND_INTERVAL, 11000).
-define(DEFAULT_PING_TIMEOUT, 10000).
-define(DEFAULT_WAIT_CONFIRM_TIMEOUT, 10000).

-record(wait_ping_ack, {
  skip_list = [] :: list(),
  timers = [] :: list(),
  state = first :: first | ping_req
}).

-record(wait_ping_req_ack, {
  requester = undefined :: pid(),
  timers = [] :: list()
}).

-record(wait_confirm, {
  timers = [] :: list()
}).

-record(node_state, {
  incarnation = 0 :: integer(),
  state = undefined :: joining | alive | suspected | dead
}).

-record(?MODULE, {
  my_state = init :: init | ready,
  my_self = undefined,
  % #{pid() => #node_state{}}
  actives = #{},
  suspects = #{} :: {wait_ping_ack, #wait_ping_ack{}} |
                    {wait_ping_req_ack, #wait_ping_req_ack{}} |
                    {wait_confirm, #wait_confirm{}},
  try_join_timer = undefined
}).

-type suspect_type() ::
  wait_ping_ack |
  wait_ping_req_ack |
  wait_confirm.

-type suspect_key() ::
  pid().

-type suspect_value() ::
  {wait_ping_ack, #wait_ping_ack{}} |
  {wait_ping_req_ack, #wait_ping_req_ack{}} |
  {wait_confirm, #wait_confirm{}}.

-type actives() ::
  #{pid() => #node_state{}}.

-type swim_msg_type() ::
  % ping_timeout, Suspect
  {ping_timeout, pid()} |

  % alive, pid which is suspected in previous.
  {alive, pid()} |
  {suspected, pid(), actives()}.

main() ->
  swim_node:start_link([], 'A'),
  timer:sleep(1000),
  swim_node:start_link(['A'], 'B').
  % There are potential concurrency problem.
  % Because 'B'는 is not a cluster member yet.
  % So, Process 'C' may fail to find Node 'B'.
%%  swim_node:start_link(['A', 'B'], 'C').
%%  swim_node:start_link(['A', 'B'], 'C').


start_link(ClusterNodes, MyAlias) ->
  ClusterNodesWithPid = [whereis(ClusterNodeAlias) || ClusterNodeAlias <- ClusterNodes],
  gen_server:start_link(?MODULE, [ClusterNodesWithPid, MyAlias], []).

init([ClusterNodes, MyAlias]) ->
  io:format("[~p] swim_node ~p try to intialized ~n", [self(), self()]),
  erlang:register(MyAlias, self()),
  {ActiveCluster, MyState} = case ClusterNodes of
                    % 클러스터의 첫번째 노드일 때,
                    [] ->
                      Actives = #{self() => #node_state{state=alive}},
                      {Actives, ready};
                    ClusterNodes ->
                      JoiningMsg = joining_message(ClusterNodes),
                      timer:apply_after(0, gen_server, cast, [self(), JoiningMsg]),
                      {#{}, joining}
                  end,

  case MyState of
    ready ->
      io:format("[~p] swim_node ~p has been ready ~n", [self(), self()]),
      NextRoundMsg = next_round_message(self()),
      send_message(NextRoundMsg, self());
    _     -> ok
  end,

  State = #?MODULE{my_self=self(), my_state=MyState, actives=ActiveCluster},
  {ok, State}.

% 모르는 메세지
handle_call(_, _From, _State) ->
  {reply, _State, _State}.

handle_info(_Msg, State) ->
  io:format("[~p] swim_node [~p] receive unknown info message ~p~n", [self(), self(), _Msg]),
  {noreply, State}.

% 클러스터 조인 요청을 할 때
handle_cast({joining, KnownClusterNodes}, State) ->
  io:format("[~p] node ~p try to join the clusters... ~n", [self(), self()]),
  MaybeTimer =
    case KnownClusterNodes of
      [] ->
        error(failed_to_join);
      [Node | Rest] ->
        JoinMsg = join_message(self()),
        send_message(JoinMsg, Node),

        JoiningMsg = joining_message(Rest),
        {ok, Timer} = timer:send_after(?DEFAULT_INIT_JOIN_TIMEOUT, JoiningMsg),
        Timer
      end,
  NewState = State#?MODULE{try_join_timer=MaybeTimer},
  {noreply, NewState};

% 클러스터 조인 요청을 받음.
handle_cast({join, Peer}, State) ->
  io:format("[~p] swim_node ~p receive join request from ~p ~n", [self(), self(), Peer]),
  #?MODULE{actives=Actives} = State,

  NewlyJoinedNodeState = #node_state{state=alive},
  NewActives = maps:put(Peer, NewlyJoinedNodeState, Actives),

  JoinedMsg = joined_message(NewActives),
  lists:foreach(
    fun
      (Pid) when Pid =/= self() -> send_message(JoinedMsg, Pid);
      (_Pid) -> ok
    end, maps:keys(NewActives)),

  NewState = State#?MODULE{actives=NewActives},
  {noreply, NewState};

% 클러스터 Active의 상태가 변화함. Gossip
% 그런데 Joined 메세지를 모든 클러스터가 받지 못할 수도 있음. (네트워크 문제로)
% 그래서 Eventually Consistency?
handle_cast({joined, GossipedActives}, #?MODULE{try_join_timer=PreviousTimer}=State) ->
  io:format("[~p] swim_node ~p receive joined gossip~n", [self(), self()]),
  timer:cancel(PreviousTimer),
  NewState0 = merge_gossip_into_actives_(State, GossipedActives),
  NewState = case is_joining_node(NewState0) of
               true -> process_task_and_update_after_joined(NewState0);
               false -> NewState0
             end,
  {noreply, NewState};

handle_cast({next_round, Pid}, #?MODULE{suspects=Suspects, actives=Actives}=State0) ->
  io:format("[~p] got received {next_round, ~p}~n", [self(), Pid]),
  send_next_round_msg(),

  SuspectsPidList = maps:keys(Suspects),
  ShouldRemoveList = [self(), SuspectsPidList],
  FilteredPeerMap = lists:foldl(
    fun(ShouldRemovePid, Acc) ->
      case maps:is_key(ShouldRemovePid, Acc) of
        true -> maps:remove(ShouldRemovePid, Acc);
        false -> Acc
      end
    end, Actives, ShouldRemoveList),

  State =
    case pick_one_randomly(FilteredPeerMap) of
      undefined -> State0;
      MaybeSuspectPid ->
        io:format("[~p] ~p send ping msg to ~p~n", [self(), self(), MaybeSuspectPid]),

        PingMsg = ping_message(self(), Actives),
        send_message(PingMsg, MaybeSuspectPid),

        PingTimeoutMsg = ping_timeout_message(MaybeSuspectPid),
        {ok, Timer} = send_message_after(?DEFAULT_PING_TIMEOUT, PingTimeoutMsg, self()),

        SkipList = [self(), MaybeSuspectPid],
        WaitPingAck = #wait_ping_ack{skip_list=SkipList, timers=[Timer], state=first},
        SuspectValue = {wait_ping_ack, WaitPingAck},
        UpdatedSuspects = maps:put(MaybeSuspectPid, SuspectValue, Suspects),

        State0#?MODULE{suspects=UpdatedSuspects}
    end,
  {noreply, State};

handle_cast({ping, From, GossipedActives}, #?MODULE{actives=Actives}=State0) ->
  io:format("[~p] ~p got ping message from ~p~n", [self(), self(), From]),

  PingAckMsg = ping_ack_message(self(), Actives),
  send_message(PingAckMsg, From),

  State = merge_gossip_into_actives_(State0, GossipedActives),
  {noreply, State};

% ping_req는
handle_cast({ping_timeout, Suspect}, #?MODULE{suspects=Suspects0, actives=Actives}=State0) ->
  State =
    case maps:get(Suspect, Suspects0) of
      {wait_ping_ack, WaitPingTimerRecord} ->
        #wait_ping_ack{skip_list=SkipList, timers=Timers, state=WaitPingState} = WaitPingTimerRecord,
        case {Timers, WaitPingState} of
          % 이미 다 제거된 경우.
          {_Timers, ping_req} ->
            gossip_suspect_node(State0, Suspect);
          % 남은 Timer가 다 제거될 때까지 기다린다.
          % Timer 정보는 어떻게 알 수 있지?
          {_Timers, first} ->
            OtherCandidates = maps:filter(
              fun(Pid, _) ->
                not lists:member(Pid, SkipList)
              end, Actives),
            case OtherCandidates of
              [] ->
                gossip_suspect_node(State0, Suspect);
              OtherCandidates ->

                send_ping_req_message_max_n_times(3, OtherCandidates, Actives, Suspect),

                PingTimeoutMsg = ping_timeout_message(Suspect),
                {ok, Timer} = timer:apply_after(?DEFAULT_PING_TIMEOUT, swim_node, send_message, [PingTimeoutMsg, self()]),

                WaitPingAck = #wait_ping_ack{timers=[Timer], skip_list=SkipList, state=ping_req},
                SuspectValue = {wait_ping_ack, WaitPingAck},
                UpdatedSuspects = maps:put(Suspect, SuspectValue, Suspects0),
                State0#?MODULE{suspects=UpdatedSuspects}
            end
        end;
      {wait_ping_req_ack, _WaitingPingReqAck} ->
        % delegate to determine it's suspected
        State0;
      _ ->
        State0
    end,
  {noreply, State};

handle_cast({ping_req, Requester, Suspect, _Gossip}, #?MODULE{actives=Actives, suspects=Suspects}=State0) ->
  io:format("[~p] ~p got ping_req message from ~p~n", [self(), self(), Requester]),

  PingMsg = {ping, self(), Actives},
  send_message(PingMsg, Suspect),

  PingTimeoutMsg = ping_timeout_message(Suspect),
  {ok, Timer} = send_message_after(?DEFAULT_PING_TIMEOUT, PingTimeoutMsg, self()),

  WaitPingReqAck = #wait_ping_req_ack{requester=Requester, timers=[Timer]},
  SuspectValue = {wait_ping_req_ack, WaitPingReqAck},
  UpdatedSuspects = maps:put(Suspect, SuspectValue, Suspects),

  State = #?MODULE{suspects=UpdatedSuspects},
  {noreply, State};

handle_cast({ping_ack, FromSuspect, _Gossip}=Msg, #?MODULE{suspects=Suspects0}=State0) ->
  io:format("[~p] ~p got ping_ack message from ~p~n", [self(), self(), FromSuspect]),
  State =
    case maps:is_key(FromSuspect, Suspects0) of
      true ->
        case maps:get(FromSuspect, Suspects0) of
          {wait_ping_req_ack, Record} ->
            % delegate
            % ping_req를 받은 녀석은, ACK를 받는 경우
            % 1. 자신의 suspect list에서 지운다.
            % 2. ping_req를 요청한 녀석에게 응답한다.
            cancel_timers(get_timers(Record)),
            #wait_ping_req_ack{requester=Requester} = Record,
            send_message(Msg, Requester);
          {wait_ping_ack, Record} ->
            % 처음에 ping을 보낸 녀석이..
            % 1. suspect에게 직접 받거나
            % 2. 중재자를 통해서 받은 경우
            % 가 존재하는데
            % 여기서
            % 1. suspect list에서 문제를 제거하기만 하면 된다.
            cancel_timers(get_timers(Record));
          {wait_confirm, Record} ->
            % 지우기 거의 마지막인 녀석이 있음.
            % 여기서 ping_ack를 받으면 살아났다고 볼 수 있다.
            cancel_timers(get_timers(Record));
          _ -> ok
        end,
        % Suspect에서 제거한다.
        UpdateSuspects = maps:remove(FromSuspect, Suspects0),
        State0#?MODULE{suspects=UpdateSuspects};
      false ->
          State0
    end,
  {noreply, State};

% Extend Point
handle_cast({suspected, Suspect, GossipedActives}, State0) ->
  #?MODULE{actives=Actives0, suspects=Suspects0}=State0,
  Self = self(),

  State =
    case Self =:= Suspect of
      % Case1. me is in suspected.
      true ->
        State1 = merge_gossip_into_actives_(State0, GossipedActives),
        #?MODULE{actives=Actives1} = State1,

        GossipedIncarnation = get_incarnation_by_pid(GossipedActives, Self),
        LocalIncarnation = get_incarnation_by_pid(Actives0, Self),

        NewIncarnation = max(GossipedIncarnation, LocalIncarnation) + 1,
        NewNodeState = #node_state{incarnation=NewIncarnation, state=alive},
        Actives = maps:put(Suspect, NewNodeState, Actives1),

        ActivePeerListExceptSelf = lists:delete(Self, maps:keys(Actives)),
        AliveMsg = alive_message(Self, NewIncarnation, Actives),
        send_messages(AliveMsg, ActivePeerListExceptSelf),
        State0;
      false ->
        TimeoutMsg = {wait_confirm_timeout, Suspect},

        {ok, Timer} = send_message_after(?DEFAULT_WAIT_CONFIRM_TIMEOUT, TimeoutMsg, Self),
        SuspectValue = {wait_confirm, Timer},

        UpdatedSuspects = maps:put(Suspect, SuspectValue, Suspects0),
        State0#?MODULE{suspects=UpdatedSuspects}
  end,
  {noreply, State};


handle_cast({wait_confirm_timeout, Suspect}, #?MODULE{actives=Actives0, suspects=Suspects0}=State0) ->
  State = leave_suspect_node(State0, Suspect),
  {noreply, State};

handle_cast({alive, Suspect, GossipedIncarnation, GossipedActive}, #?MODULE{actives=Actives0, suspects=Suspects0}=State0) ->
  % TODO
  % 1. Suspect에 있던 것 제거
  % 2. Timer 제거
  State =
    case maps:get(Suspect, Suspects0) of
      {wait_confirm, WaitConfirmRecord} ->
        LocalIncarnation = get_incarnation_by_pid(Actives0, Suspect),
        case LocalIncarnation < GossipedIncarnation of
          true ->
            cancel_timers(get_timers(WaitConfirmRecord)),
            UpdatedSuspects = maps:remove(Suspect, Suspects0),

            State1 = merge_gossip_into_actives_(State0, GossipedActive),
            State1#?MODULE{suspects=UpdatedSuspects};
          false ->
            merge_gossip_into_actives_(State0, GossipedActive)
        end;
      _ ->
        merge_gossip_into_actives_(State0, GossipedActive)
  end,
  {noreply, State};

handle_cast({left, LeftNode, GossipedIncarnation, GossipedActives}, State0) ->
  #?MODULE{actives=Actives0, suspects=Suspects0}=State0,
  Suspects =
    case maps:get(LeftNode, Suspects0) of
      undefined -> Suspects0;
      {_, Record} ->
        cancel_timers(get_timers(Record)),
        maps:remove(LeftNode, Suspects0)
    end,

  Actives1 =
    case maps:is_key(LeftNode, Actives0) of
      true ->
        #node_state{incarnation=KnownIncarnation} = maps:get(LeftNode, Actives0),
        case KnownIncarnation < GossipedIncarnation of
          true -> maps:put(LeftNode, #node_state{state=dead, incarnation=GossipedIncarnation}, Actives0);
          false -> Actives0
        end;
      false -> Actives0
    end,

  State1 = State0#?MODULE{actives=Actives1},
  State2 = merge_gossip_into_actives_(State1, GossipedActives),
  State = State2#?MODULE{suspects=Suspects},
  {noreply, State};

handle_cast(_Msg, State) ->
  io:format("[~p] got unknown Message ~p~n", [self(), _Msg]),
  {noreply, State}.

merge_gossip_into_actives_(State, GossipedActives) ->
  #?MODULE{actives=Actives} = State,
  GossipedKeys = maps:keys(GossipedActives),
  MergedActives =
    lists:foldl(
      fun(Key, Acc) ->
        MaybeKnownValue = maps:get(Key, Actives, undefined),
        GossipedValue = maps:get(Key, GossipedActives),

        case {MaybeKnownValue, GossipedValue} of
          {undefined, _} ->
            maps:put(Key, GossipedValue, Acc);
          {#node_state{incarnation=KnownIncarnation}, #node_state{incarnation=GossipedIncarnation}} when GossipedIncarnation >= KnownIncarnation ->
            maps:put(Key, GossipedValue, Acc);
          _ ->
            Acc
        end
      end, Actives, GossipedKeys),

  State#?MODULE{actives=MergedActives, try_join_timer=undefined}.

pick_one_randomly(Peers) when map_size(Peers) =:= 0 ->
  undefined;
pick_one_randomly(Peers) ->
  PeersList = maps:keys(Peers),
  Size = length(PeersList),
  RandomIndex = rand:uniform(Size),
  lists:nth(RandomIndex, PeersList).

is_joining_node(State) ->
  State#?MODULE.my_state =:= joining.

process_task_and_update_after_joined(State0) ->
  send_next_round_msg(),
  io:format("[~p] swim_node ~p has been ready~n", [self(), self()]),
  State0#?MODULE{my_state=ready}.

send_next_round_msg() ->
  Msg = next_round_message(self()),
  send_message_after(?NEXT_ROUND_INTERVAL, Msg, self()).

gossip_suspect_node(State0, Suspect) ->
  #?MODULE{actives=Actives0, suspects=Suspects0} = State0,

  NodeStateOfSuspected = maps:get(Suspect, Actives0),
  #node_state{incarnation=Incarnation} = NodeStateOfSuspected,
  NewNodeState = #node_state{incarnation=Incarnation + 1, state=suspected},
  Actives = maps:put(Suspect, NewNodeState, Actives0),

  Self = self(),
  SuspectedMsg = suspected_message(Suspect, Actives),
  SendToSuspectedMsgPeers = maps:remove(Self, Actives),
  send_messages(SuspectedMsg, SendToSuspectedMsgPeers),

  WaitConfirmTimeoutMsg = wait_confirm_timeout_message(Suspect),
  {ok, Timer} = send_message_after(?DEFAULT_WAIT_CONFIRM_TIMEOUT, WaitConfirmTimeoutMsg, Self),

  SuspectValue = {wait_confirm, Suspect, Timer},
  Suspects = maps:put(Suspect, SuspectValue, Suspects0),
  State0#?MODULE{suspects=Suspects, actives=Actives}.

leave_suspect_node(#?MODULE{actives=Actives0, suspects=Suspects0}=State, Suspect) ->
  #node_state{incarnation=PreviousIncarnation} = RemovingNodeState = maps:get(Suspect, Actives0),
  NewIncarnation = PreviousIncarnation + 1,
  NewNodeState = #node_state{state=dead, incarnation=NewIncarnation},

  Actives = maps:put(Suspect, NewNodeState, Actives0),
  ShouldReceiveMsgPeerList0 = maps:remove(self(), Actives),
  ShouldReceiveMsgPeerList = maps:remove(Suspect, ShouldReceiveMsgPeerList0),

  LeftMsg = left_node_message(Suspect, NewIncarnation, Actives),
  send_messages(LeftMsg, ShouldReceiveMsgPeerList),

  Suspects = maps:remove(Suspect, Suspects0),
  State#?MODULE{actives=Actives, suspects=Suspects}.

send_message_max_n_times_(_RemainCount, [], _Msg, _Suspect) ->
  ok;
send_message_max_n_times_(RemainCount, [_Head | _Tail], _Msg, _Suspect) when RemainCount < 0 ->
  ok;
send_message_max_n_times_(RemainCount, [Peer | Tail], Msg, Suspect) when RemainCount < 0 ->
  send_message(Msg, Peer),
  send_message_max_n_times_(RemainCount - 1, Tail, Msg, Suspect).

send_ping_req_message_max_n_times(MaximumNumber, Candidates, Actives, Suspect) ->
  PingReqMsg = ping_req_message(self(), Suspect, Actives),
  CandidatesList = maps:keys(Candidates),
  ShuffledCandidateList = shuffle(CandidatesList),
  send_message_max_n_times_(MaximumNumber, ShuffledCandidateList, PingReqMsg, Suspect),
  ok.

send_messages(Msg, PeerList) ->
  lists:foreach(
    fun(Peer) ->
      send_message(Msg, Peer)
    end, PeerList).

send_message(Msg, Peer) ->
  gen_server:cast(Peer, Msg).

send_message_after(Timeout, Msg, ToPid) ->
  timer:apply_after(Timeout, swim_node, send_message, [Msg, ToPid]).

shuffle(List) ->
  Randomized = [{rand:uniform(), X} || X <- List],
  Sorted = lists:sort(Randomized),
  [X || {_, X} <- Sorted].

cancel_timers([]) ->
  ok;
cancel_timers([Timer|Tail]) ->
  timer:cancel(Timer),
  cancel_timers(Tail).

joining_message(ClusterNodes) ->
  {joining, ClusterNodes}.

join_message(From) ->
  {join, From}.

joined_message(Actives) ->
  {joined, Actives}.

next_round_message(From) ->
  {next_round, From}.

ping_timeout_message(Suspect) ->
  {ping_timeout, Suspect}.

ping_message(From, Actives) ->
  {ping, From, Actives}.

ping_ack_message(From, Actives) ->
  {ping_ack, From, Actives}.

ping_req_message(From, Suspect, Actives) ->
  {ping_req, From, Suspect, Actives}.

suspected_message(Suspect, Actives) ->
  {suspected, Suspect, Actives}.

alive_message(Suspect, Incarnation, Actives) ->
  {alive, Suspect, Incarnation, Actives}.

wait_confirm_timeout_message(Suspect) ->
  {wait_confirm_timeout, Suspect}.

left_node_message(Suspect, Incarnation, Actives) ->
  {left, Suspect, Incarnation, Actives}.

get_timers(#wait_ping_ack{timers=Timers}=_Record) ->
  Timers;
get_timers(#wait_ping_req_ack{timers=Timers}=_Record) ->
  Timers;
get_timers(#wait_confirm{timers=Timers}=_Record) ->
  Timers;
get_timers(Record) ->
  % can't reach here.
  undefined.

get_incarnation_by_pid(Actives, Pid) ->
  #node_state{incarnation=Incarnation} = maps:get(Pid, LocalActives),
  Incarnation.