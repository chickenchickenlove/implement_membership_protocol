-module(swim_node).
-behavior(gen_server).

%% API
-export([start_link/2]).

% gen_server callback
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([main/0]).
-export([send_message/2]).

% For Test.
-export([hibernate_while/2]).


% 5s
-define(DEFAULT_INIT_JOIN_TIMEOUT, 11000).
-define(NEXT_ROUND_INTERVAL, 11000).
-define(DEFAULT_PING_TIMEOUT, 10000).
-define(DEFAULT_WAIT_CONFIRM_TIMEOUT, 10000).
% Theory recommend at least 3 times.
-define(DEFAULT_PING_REQ_TRY, 3).

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
  actives = #{} :: #{pid() => #node_state{}},
  suspects = #{} :: {wait_ping_ack, #wait_ping_ack{}} |
                    {wait_ping_req_ack, #wait_ping_req_ack{}} |
                    {wait_confirm, #wait_confirm{}},
  try_join_timer = undefined
}).

-type suspect_value() ::
  {wait_ping_ack, #wait_ping_ack{}} |
  {wait_ping_req_ack, #wait_ping_req_ack{}} |
  {wait_confirm, #wait_confirm{}}.

-type actives() ::
  #{pid() => #node_state{}}.

-type suspects() ::
  #{pid() => suspect_value()}.

-type suspect() :: pid().
-type from() :: pid().
-type swim_node() :: pid().
-type incarnation() :: integer().

-type swim_msg_type() ::

  {joining, list(swim_node())} |

  % PID which join message from
  {join, from()} |

  % the actives are gossiped active nodes in cluster.
  {joined, actives()} |

  % PID which message from
  {next_round, from()} |

  % PID which message from
  % the actives are gossiped active nodes in cluster.
  {ping, from(), actives()} |

  % PID which message from suspect.
  {ping_timeout, suspect()} |

  {ping_ack, from(), actives()} |

  {ping_req, from(), suspect(), actives()} |

  {suspected, suspect(), incarnation(), actives()} |

  {wait_confirm_timeout, suspect()} |

  {left, suspect(), incarnation(), actives()} |

  {alive, suspect(), incarnation(), actives()}.



main() ->
  % scenario
  % 1. 'A' suspect 'B'.
  % 2. 'B' decline 'A' by sending Alive Message.
  % 3. 'B' still is member of cluster.
  process_flag(trap_exit, true),

  swim_node:start_link([], 'A'),
  timer:sleep(1000),
  swim_node:start_link(['A'], 'B'),
  timer:sleep(1000),

  swim_node:hibernate_while(30000, whereis('B')).

%%main() ->
% scenario
% 1. The node 'B' is terminated by accidantaly.
% 2. The node 'A' is aware of that, and it marked 'B' as dead.
% 3. Only 'A' is member of cluster.
%%  process_flag(trap_exit, true),
%%
%%  swim_node:start_link([], 'A'),
%%  timer:sleep(1000),
%%  swim_node:start_link(['A'], 'B')
%%    timer:sleep(1000),
%%  exit(whereis('B'), exit).


% Concurrency Problem.
% There are potential concurrency problem.
% Because 'B' is not a cluster member yet.
% So, Process 'C' may fail to find Node 'B'.
%%  swim_node:start_link(['A', 'B'], 'C').
%%  swim_node:start_link(['A', 'B'], 'C').


start_link(ClusterNodes, MyAlias) ->
  ClusterNodesWithPid = [whereis(ClusterNodeAlias) || ClusterNodeAlias <- ClusterNodes],
  gen_server:start_link(?MODULE, [ClusterNodesWithPid, MyAlias], []).

hibernate_while(Time, Pid) ->
  % only for test scenario.
  HibernateMsg = {hibernate, Time},
  gen_server:cast(Pid, HibernateMsg).


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

handle_call(_Msg, _From, _State) ->
  % Unknown Message.
  io:format("[~p] swim_node ~p received unknown call message from ~p, Message is ~p~n", [self(), self(), _From, _Msg]),
  {reply, _State, _State}.

handle_info(_Msg, State) ->
  io:format("[~p] swim_node ~p received unknown info message ~p~n", [self(), self(), _Msg]),
  {noreply, State}.


handle_cast({hibernate, Time}, State) ->
  % only for test scenario.
  io:format("[~p] swim_node ~p received hibernate message. it will be hibernated during ~ps.~n", [self(), self(), Time]),
  timer:sleep(Time),
  {noreply, State};

handle_cast({joining, KnownClusterNodes}, State) ->
  io:format("[~p] node ~p try to join the clusters... ~n", [self(), self()]),
  MaybeTimer =
    case KnownClusterNodes of
      [] ->
        error(failed_to_join);
      [Node | Rest] ->
        JoinMsg = join_message(self()),
        send_message(JoinMsg, Node),

        % If Join request fail, node should send request to other known node.c
        % Otherwise, it cannot join the cluster forever.
        JoiningMsg = joining_message(Rest),
        {ok, Timer} = timer:send_after(?DEFAULT_INIT_JOIN_TIMEOUT, JoiningMsg),
        Timer
      end,
  NewState = State#?MODULE{try_join_timer=MaybeTimer},
  {noreply, NewState};

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

  PeersToReceiveMsgMap = filter_useless_nodes_from_actives(Actives),

  State =
    case pick_one_randomly(PeersToReceiveMsgMap) of
      undefined ->
        State0;
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

handle_cast({ping_timeout, Suspect}, #?MODULE{suspects=Suspects0, actives=Actives}=State0) ->
  io:format("[~p] node ~p received ping_timeout message. suspect is ~p~n", [self(), self(), Suspect]),

  State =
    case maps:get(Suspect, Suspects0, undefined) of
      {wait_ping_ack, WaitPingTimerRecord} ->
        #wait_ping_ack{skip_list=SkipList, timers=Timers, state=WaitPingState} = WaitPingTimerRecord,
        case {Timers, WaitPingState} of
          % In case of first NEXT round is failed, and in trying to ping_req.
          {_Timers, ping_req} ->
            gossip_suspect_node(State0, Suspect);
          % In case of first NEXT round try
          {_Timers, first} ->
            OtherCandidates = maps:filter(
              fun(Pid, _) ->
                not lists:member(Pid, SkipList)
              end, Actives),

            case OtherCandidates of
              [] ->
                % There is no node to delegate ping_req at all.
                gossip_suspect_node(State0, Suspect);

              OtherCandidates ->
                send_ping_req_message_max_n_times(?DEFAULT_PING_REQ_TRY, OtherCandidates, Actives, Suspect),

                PingTimeoutMsg = ping_timeout_message(Suspect),
                {ok, Timer} = timer:apply_after(?DEFAULT_PING_TIMEOUT, swim_node, send_message, [PingTimeoutMsg, self()]),

                WaitPingAck = #wait_ping_ack{timers=[Timer], skip_list=SkipList, state=ping_req},
                SuspectValue = {wait_ping_ack, WaitPingAck},
                UpdatedSuspects = maps:put(Suspect, SuspectValue, Suspects0),
                State0#?MODULE{suspects=UpdatedSuspects}
            end
        end;
      {wait_ping_req_ack, _WaitingPingReqAck} ->
        % delegate to determine it's suspected.
        % If node which is responsible of ping_req don't response ping_ack to requester node until timer bomb,
        % The request node's timer will be send timeout message to request node.
        % Then, request node will broadcast suspected message.
        State0;
      undefined ->
        % erlang has concurrency scenario
        % 1. suspect timer is reserved
        % 2. suspect decline by sending alive msg.
        % 3. Node started to handle alive msg.
        % 4. However, the timer has been expired, so ping_timeout message is already reach to node's postbox.
        % 5. Node cancel all timers successfully. however, ping_timeout message still in node's postbox.
        % To prevent for situation above to kill the current node process, we added 'undefined' clauses.
        State0;
      _ ->
        State0
    end,
  {noreply, State};

handle_cast({ping_req, Requester, Suspect, GossipedActives}, State0) ->
  io:format("[~p] ~p got ping_req message from ~p~n", [self(), self(), Requester]),
  #?MODULE{actives=Actives, suspects=Suspects}=State0,

  PingMsg = {ping, self(), Actives},
  send_message(PingMsg, Suspect),

  PingTimeoutMsg = ping_timeout_message(Suspect),
  {ok, Timer} = send_message_after(?DEFAULT_PING_TIMEOUT, PingTimeoutMsg, self()),

  WaitPingReqAck = #wait_ping_req_ack{requester=Requester, timers=[Timer]},
  SuspectValue = {wait_ping_req_ack, WaitPingReqAck},
  UpdatedSuspects = maps:put(Suspect, SuspectValue, Suspects),

  State1 = #?MODULE{suspects=UpdatedSuspects},
  State = merge_gossip_into_actives_(State1, GossipedActives),

  {noreply, State};

handle_cast({ping_ack, FromSuspect, GossipedActives}=Msg, State0) ->
  io:format("[~p] ~p got ping_ack message from ~p~n", [self(), self(), FromSuspect]),
  #?MODULE{suspects=Suspects0}=State0,

  State1 =
    case maps:is_key(FromSuspect, Suspects0) of
      true ->
        case maps:get(FromSuspect, Suspects0) of
          {wait_ping_req_ack, Record} ->
            cancel_timers(get_timers(Record)),
            #wait_ping_req_ack{requester=Requester} = Record,
            send_message(Msg, Requester);
          {wait_ping_ack, Record} ->
            cancel_timers(get_timers(Record));
          {wait_confirm, Record} ->
            cancel_timers(get_timers(Record));
          % Unknown Message. so, it should be ignored.
          _ -> ok
        end,
        UpdateSuspects = maps:remove(FromSuspect, Suspects0),
        State0#?MODULE{suspects=UpdateSuspects};

      false ->
          State0
    end,

  State = merge_gossip_into_actives_(State1, GossipedActives),
  {noreply, State};

% Extend Point
handle_cast({suspected, Suspect, GossipedIncarnation, GossipedActives}, State0) ->

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

        State1;
      false ->
        TimeoutMsg = {wait_confirm_timeout, Suspect},
        {ok, Timer} = send_message_after(?DEFAULT_WAIT_CONFIRM_TIMEOUT, TimeoutMsg, Self),

        NewState0 = merge_gossip_into_actives_(Actives0, GossipedActives),
        MaybeNewNodeStateOfSuspect =
          apply_diff_with_considering_incarnation(Suspect, suspected, Actives0, GossipedIncarnation),
        NewState1 = put_it_into_actives_get_state(Suspect, MaybeNewNodeStateOfSuspect, NewState0),

        SuspectValue = {wait_confirm, Timer},
        UpdatedSuspects = maps:put(Suspect, SuspectValue, Suspects0),

        NewState1#?MODULE{suspects=UpdatedSuspects}
  end,
  {noreply, State};


handle_cast({wait_confirm_timeout, Suspect}, State0) ->
  State = leave_suspect_node(State0, Suspect),
  {noreply, State};

handle_cast({alive, Suspect, GossipedIncarnation, GossipedActive}, #?MODULE{actives=Actives0, suspects=Suspects0}=State0) ->
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
        MaybeNewNodeStateOfLeftNode =
          apply_diff_with_considering_incarnation(LeftNode, dead, Actives0, GossipedIncarnation),

        StateWithNewLeftNode = put_it_into_actives_get_state(LeftNode, MaybeNewNodeStateOfLeftNode, State0),

        #?MODULE{actives=MergedActives} = StateWithNewLeftNode,
        MergedActives;
      false -> Actives0
    end,

  GossipedActivesExceptLeftNode = remove_node_state(LeftNode, GossipedActives),
  State1 = State0#?MODULE{actives=Actives1},
  State2 = merge_gossip_into_actives_(State1, GossipedActivesExceptLeftNode),
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

filter_useless_nodes_from_actives(Actives) ->
  Filter1 =
    fun(ShouldRemovePid, Acc) ->
      case maps:is_key(ShouldRemovePid, Acc) of
        true -> maps:remove(ShouldRemovePid, Acc);
        false -> Acc
      end
    end,

  ShouldRemoveList = [self()],
  FilteredPeerMap1 = lists:foldl(Filter1, Actives, ShouldRemoveList),

  Filter2 =
    fun(_Key, Value) ->
      #node_state{state=State} = Value,
      State =:= alive
    end,

  maps:filter(Filter2, FilteredPeerMap1).

gossip_suspect_node(State0, Suspect) ->
  #?MODULE{actives=Actives0, suspects=Suspects0} = State0,

  NodeStateOfSuspected = maps:get(Suspect, Actives0),
  #node_state{incarnation=Incarnation} = NodeStateOfSuspected,
  NewIncarnation = Incarnation + 1,
  NewNodeState = #node_state{incarnation=NewIncarnation, state=suspected},
  Actives = maps:put(Suspect, NewNodeState, Actives0),

  Self = self(),
  SuspectedMsg = suspected_message(Suspect, NewIncarnation, Actives),
  SendToSuspectedMsgPeerMap = maps:remove(Self, Actives),
  SendToSuspectedMsgPeers = maps:keys(SendToSuspectedMsgPeerMap),
  send_messages(SuspectedMsg, SendToSuspectedMsgPeers),

  WaitConfirmTimeoutMsg = wait_confirm_timeout_message(Suspect),
  {ok, Timer} = send_message_after(?DEFAULT_WAIT_CONFIRM_TIMEOUT, WaitConfirmTimeoutMsg, Self),

  SuspectValue = {wait_confirm, Suspect, Timer},
  Suspects = maps:put(Suspect, SuspectValue, Suspects0),
  State0#?MODULE{suspects=Suspects, actives=Actives}.

leave_suspect_node(#?MODULE{actives=Actives0, suspects=Suspects0}=State, Suspect) ->
  Suspects = maps:remove(Suspect, Suspects0),

  NewIncarnation = get_incarnation_by_pid(Actives0, Suspect) + 1,
  NewNodeState = #node_state{state=dead, incarnation=NewIncarnation},

  Actives = maps:put(Suspect, NewNodeState, Actives0),
  ShouldReceiveMsgPeerMaps0 = maps:remove(self(), Actives),
  ShouldReceiveMsgPeerMaps1 = maps:remove(Suspect, ShouldReceiveMsgPeerMaps0),
  PeersToReceive = maps:keys(ShouldReceiveMsgPeerMaps1),

  LeftMsg = left_node_message(Suspect, NewIncarnation, Actives),
  send_messages(LeftMsg, PeersToReceive),

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

remove_node_state(NodePid, Actives) ->
  maps:remove(NodePid, Actives).

apply_diff_with_considering_incarnation(NodePid, DesiredState, PreviousActives, GossipedIncarnation) ->
  io:format("[~p] node ~p try to merge gossiped information. ~n", [self(), self()]),
  PreviousIncarnation = get_incarnation_by_pid(PreviousActives, NodePid),
  case PreviousIncarnation < GossipedIncarnation of
    true ->
      #node_state{state=DesiredState, incarnation=GossipedIncarnation};
    false ->
      maps:get(NodePid, PreviousActives)
  end.

put_it_into_actives_get_state(NodePid, NodeState, State) ->
  #?MODULE{actives=PreviousActives} = State,
  NewActives = maps:put(NodePid, NodeState, PreviousActives),
  State#?MODULE{actives=NewActives}.

%%% SWIM Protocol Message
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

ping_req_message(From, Suspect, GossipedActives) ->
  {ping_req, From, Suspect, GossipedActives}.

suspected_message(Suspect, Incarnation, Actives) ->
  {suspected, Suspect, Incarnation, Actives}.

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
get_timers(_Record) ->
  % can't reach here.
  undefined.

get_incarnation_by_pid(Actives, Pid) ->
  % If Incarnation Number is -1, it always will be ignored.
  % Because default values is 0.
  case maps:get(Pid, Actives, undefined) of
    undefined -> -1;
    #node_state{incarnation=Incarnation} -> Incarnation
  end.