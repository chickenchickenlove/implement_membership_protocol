-module(hypar_view_node).
-behavior(gen_server).


%% gen_server API.
-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([join/2]).

-export([main/0]).


-type peer_event() ::
  neighbor_up |
  neighbor_down.

-record(config, {local_endpoint = undefined,
                 active_random_walk_length = 5 :: integer(),
                 passive_random_walk_length = 2 :: integer(),
                 active_view_capacity = 4 :: integer(),
                 passive_view_capacity = 24 :: integer(),
                 shuffle_ttl = 2 :: integer(),
                 shuffle_active_view_count = 2 :: integer(),
                 shuffle_passive_view_count = 2 :: integer(),
                 shuffle_interval = 10000 :: integer() % 10s.
}).

-record(?MODULE, {name = undefined :: atom(),
                  pid = undefined :: pid(),
                  active_view = #{} :: map(),
                  passive_view = #{} :: map(),
                  config = #config{} :: #config{},
                  reactor = hypar_erl_reactor :: atom()
}).


main() ->
%%  process_flag(trap_exit, true),
  Config = #config{},

  hypar_view_node:start_link('A', Config),
  timer:sleep(1000),

  hypar_view_node:start_link('B', Config),
  timer:sleep(1000),

  hypar_view_node:start_link('C', Config),
  timer:sleep(1000),

%%  hypar_view_node:start_link('D', Config),
%%  timer:sleep(1000),
%%
%%  hypar_view_node:start_link('E', Config),
%%  timer:sleep(1000),
%%
%%  hypar_view_node:start_link('F', Config),
%%  timer:sleep(1000),

  join('A', 'B'),
  join('B', 'C').
%%  join('C', 'D'),
%%  join('D', 'E'),
%%  join('E', 'F').

start_link(NodeName, Config) ->
  gen_server:start_link(?MODULE, [NodeName, Config], []).


join(NodeName, ClusterNodeName) ->
  NodePid = whereis(NodeName),
  Msg = {api_join, NodeName, ClusterNodeName},
  send_message_async(NodePid, Msg).

init([NodeName, Config]) ->
  SelfPid = self(),
  io:format("[~p] HyParView Node is initialized. NodeName: ~p, Pid: ~p~n", [self(), NodeName, self()]),
  register(NodeName, SelfPid),

  % TODO : Add a scheduling shuffle message code here.
  #config{shuffle_interval=ShuffleInterval} = Config,
  schedule_shuffle(ShuffleInterval),

  State = #?MODULE{name=NodeName, pid=SelfPid, config=Config},
  {ok, State}.

handle_call(Msg, From, State) ->
  {noreply, State}.


handle_cast({api_join, MyName, ClusterNodeName}, State) ->
  io:format("[~p] node ~p received api join message. try to join node ~p to ~p's active view.~n", [self(), self(), ClusterNodeName, MyName]),
  JoinMsg = join_message(ClusterNodeName),
  send_message_async(self(), JoinMsg),

  {noreply, State};

handle_cast({join, NewlyAddedPeerName}, State0) ->
  io:format("[~p] node ~p received join message. Newly Added PeerName ~p.~n", [self(), self(), NewlyAddedPeerName]),
  #?MODULE{config=Config, active_view=ActiveView0, reactor=Reactor} = State0,

  ActiveTTL = get_ttl(active_view, Config),
  ForwardMsg = forward_join_message(NewlyAddedPeerName, ActiveTTL),
  ActiveViewWithoutNewPeerList = get_node_pids(active_view, ActiveView0),
  send_messages_via_reactor(ActiveViewWithoutNewPeerList, ForwardMsg, Reactor),

  HighPriority = true,
  State1 = add_active(NewlyAddedPeerName, HighPriority, State0),

  {noreply, State1};

handle_cast({{forward_join, NewlyAddedPeerName, TTL}, FromPid}, State0) ->
  io:format("[~p] node ~p received forward_join message. Newly Added PeerName ~p and TTL ~p.~n", [self(), self(), NewlyAddedPeerName, TTL]),
  #?MODULE{active_view=ActiveView0, passive_view=Reactor, config=Config} = State0,

  ShouldAddItToActiveRightNow = ttl =:= 0 orelse maps:size(ActiveView0) =:= 0,
  State =
    case ShouldAddItToActiveRightNow of
      true ->
        add_active(NewlyAddedPeerName, true, State0);

      false ->
        PassiveViewTTL = get_ttl(passive_view, Config),
        StateWithNewlyAddedPassivePeer =
          case TTL =:= PassiveViewTTL of
            true ->
              add_passive(NewlyAddedPeerName, State0);
            false ->
              State0
          end,

        FromName = get_node_name_by_pid(FromPid),
        ActiveViewExceptFromNode =  maps:remove(FromName, ActiveView0),
        case pick_one_randomly(ActiveViewExceptFromNode) of
          % No node existed.
          undefined ->
            add_active(NewlyAddedPeerName, true, StateWithNewlyAddedPassivePeer);
          PickedNodeName ->
            NodePid = get_node_pid(PickedNodeName),
            ForwardJoinMsg = forward_join_message(NewlyAddedPeerName, TTL - 1),
            Reactor:send(NodePid, ForwardJoinMsg),
            StateWithNewlyAddedPassivePeer
        end
    end,
  {noreply, State};


handle_cast({{neighbor, HighPriority}, FromPid}, State0) ->
  io:format("[~p] node ~p received neighbor message. FromPid ~p and HighPriority ~p.~n", [self(), self(), FromPid, HighPriority]),
  State =
    case (HighPriority orelse is_active_view_full(State0)) of
      false -> State0;
      true ->
        FromNodeName = get_node_name_by_pid(FromPid),
        add_active(FromNodeName, HighPriority, State0)
    end,
  {noreply, State};


handle_cast({{disconnect, PeerName, Alive, Response}, FromPid}, State0) ->
  io:format("[~p] node ~p received disconnect message. Disconnect PeerName ~p, Alive ~p, Response ~p FromPid ~p.~n",
    [self(), self(), PeerName, Alive, Response, FromPid]),

  #?MODULE{active_view=ActiveView0} = State0,
  HasActiveViewPeerInList = maps:is_key(PeerName, ActiveView0),

  State =
    case HasActiveViewPeerInList of
      false -> State0;
      true ->
        State1 = remove_active(PeerName, State0, Response),
        State2 = remove_passive_with_state(PeerName, State1),
        State3 = promote_to_active_if_can(State2),
        case Alive of
          false -> State3;
          true ->
            #?MODULE{passive_view=PassiveView0} = State3,
            PassiveView1 = maps:put(PeerName, get_node_pid(PeerName), PassiveView0),
            State3#?MODULE{passive_view=PassiveView1}
        end
    end,
  {noreply, State};

handle_cast({do_shuffle}, State) ->
  io:format("[~p] node ~p received do_shuffle message. ~p try to shuffle and send shuffle message to other active.~n", [self(), self(), self()]),
  % TODO : CHECK Shuffle Logic. Because there is no passive view added.
  #?MODULE{config=Config} = State,
  #config{shuffle_interval=ShuffleInterval} = Config,
  schedule_shuffle(ShuffleInterval),

  shuffle(State),
  {noreply, State};

handle_cast({{shuffle, OriginNodeName, ReceivedShuffledNodes, TTL}, FromPid}, State0) ->
  io:format("[~p] node ~p received shuffle message. ~p try to update its passive view or send it to other active node.
  OriginNodeName ~p, TTL ~p~n", [self(), self(), self(), OriginNodeName, TTL]),
  State =
    case TTL =:= 0 of
      true ->
        #?MODULE{passive_view=PassiveView0, reactor=Reactor} = State0,
        PassiveView1 = maps:remove(OriginNodeName, PassiveView0),

        ShuffleNodeCount = length(ReceivedShuffledNodes),
        TakenShuffledNodeNameList = take_max_n(PassiveView1, ShuffleNodeCount),
        ShuffleReplyMsg = shuffle_reply_message(TakenShuffledNodeNameList),

        ToPid = get_node_pid(OriginNodeName),
        send_message_via_reactor(ToPid, ShuffleReplyMsg, Reactor),

        add_all_passive(State0, ReceivedShuffledNodes);
      false ->
        #?MODULE{active_view=ActiveView0, reactor=Reactor} = State0,

        Me = get_node_name_by_pid(self()),
        ActiveView1 = maps:remove(OriginNodeName, ActiveView0),
        ActiveView2 = maps:remove(Me, ActiveView1),

        case pick_one_randomly(ActiveView2) of
          undefined ->
            State0;
          PickedNodeName ->
            ShuffleMsg = shuffle_message(OriginNodeName, ReceivedShuffledNodes, TTL - 1),
            ToPid = get_node_pid(PickedNodeName),

            send_message_via_reactor(ToPid, ShuffleMsg, Reactor),
            State0
        end
    end,
  {noreply, State};


handle_cast({{shuffle_reply, ReceivedShuffledNodes}, FromPid}, State0) ->
  io:format("[~p] node ~p received shuffle reply message. It put into its passive view.~n", [self(), self()]),
  State = add_all_passive(State0, ReceivedShuffledNodes),
  {noreply, State};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Msg, State) ->
  {noreply, State}.


remove_active(PeerName, State0, Respond) ->
  #?MODULE{active_view=ActiveView0, reactor=Reactor} = State0,

  DoesContainPeerInActive = maps:is_key(PeerName, ActiveView0),
  case DoesContainPeerInActive of
    false ->
      State0;

    true ->
      ActiveView1 = maps:remove(PeerName, ActiveView0),

      case Respond of
        true ->
          DisconnectMsg = disconnect_message(true, false),
          ToPid = get_node_pid(PeerName),
          send_message_via_reactor(ToPid, DisconnectMsg, Reactor);
        false ->
          ok
      end,
      % Disconnect TCP Socket.
      Reactor:disconnect(PeerName),

      NeighborEvent = neighbor_event(down, PeerName),
      Reactor:notify(NeighborEvent),

      State1 = add_passive(PeerName, State0),
      State1#?MODULE{active_view=ActiveView1}
  end.

remove_active_if_full(State0) ->
  #?MODULE{active_view=ActiveView0} = State0,
  case is_active_view_full(State0) of
    false -> State0;
    true ->
      case pick_one_randomly(ActiveView0) of
        undefined -> State0;
        PickedPeerName -> remove_active(PickedPeerName, State0, true)
      end
  end.

promote_to_active_if_can(State0) ->
  #?MODULE{active_view=ActiveView0,
           passive_view=PassiveView0,
           reactor=Reactor} = State0,

  case is_active_view_full(State0) of
    true -> State0;
    false ->
      case pick_one_randomly(PassiveView0) of
        undefined -> State0;
        ToPromoteNodeName ->
          % To escape isolated Cluster.
          HighPriority = maps:size(ActiveView0) =:= 0,

          NeighborMsg = neighbor_message(HighPriority),
          NodePid = get_node_pid(ToPromoteNodeName),
          send_message_via_reactor(NodePid, NeighborMsg, Reactor),
          State0
      end
  end.

shuffle(State) ->
  #?MODULE{active_view=ActiveView0} = State,

  case pick_one_randomly(ActiveView0) of
    undefined -> ok;
    PickedNodeName ->
      #?MODULE{passive_view=PassiveView0, reactor=Reactor, config=Config} = State,

      ActiveView1 = maps:remove(PickedNodeName, ActiveView0),
      #config{shuffle_active_view_count=ActiveShuffleCount,
              shuffle_passive_view_count=PassiveShuffleCount,
              shuffle_ttl=ShuffleTTL} = Config,

      TakenActiveForShuffle = take_max_n(ActiveView1, ActiveShuffleCount),
      TakenPassiveForShuffle = take_max_n(PassiveView0, PassiveShuffleCount),

      ShuffleNode = TakenActiveForShuffle ++ TakenPassiveForShuffle,

      Me = get_node_name_by_pid(self()),
      ToPid = get_node_pid(PickedNodeName),
      ShuffleMsg = shuffle_message(Me, ShuffleNode, ShuffleTTL),
      send_message_via_reactor(ToPid, ShuffleMsg, Reactor),
      ok
  end.

take_max_n(PeerMap, Number) ->
  take_max_n_(PeerMap, Number, []).

take_max_n_(PeerMap, Remain, Acc) when map_size(PeerMap) =:= 0 orelse Remain =:= 0->
  Acc;
take_max_n_(PeerMap, Remain, Acc) ->
  io:format("take_max_n_ ~p~n", [PeerMap]),
  Size = maps:size(PeerMap),
  List = maps:keys(PeerMap),
  Index = rand:uniform(Size),
  PeerName = lists:nth(Index, List),
  take_max_n_(maps:remove(PeerName, PeerMap), Remain - 1, [PeerName | Acc]).

remove_passive_with_state(PeerName, State0) ->
  #?MODULE{passive_view=PassiveView0} = State0,
  PassiveView1 = maps:remove(PeerName, PassiveView0),
  State0#?MODULE{passive_view=PassiveView1}.


remove_passive(PeerName, PassiveView) ->
  maps:remove(PeerName, PassiveView).

is_passive_view_full(State) ->
  #?MODULE{config=Config, passive_view=PassiveView} = State,
  #config{passive_view_capacity=Limit} = Config,
  maps:size(PassiveView) >= Limit.


is_active_view_full(State) ->
  #?MODULE{config=Config, active_view=ActiveView} = State,
  #config{active_view_capacity=Limit} = Config,
  maps:size(ActiveView) >= Limit.


add_all_passive(State0, Nodes) ->
  lists:foldl(fun(PeerName, AccState) ->
                add_passive(PeerName, AccState)
              end, State0, Nodes).


add_passive(PeerName, State0) ->
  #?MODULE{active_view=ActiveView, passive_view=PassiveView0, config=Config} = State0,

  AlreadyActiveViewHas = maps:is_key(PeerName, ActiveView),
  AlreadyPassiveViewHas = maps:is_key(PeerName, PassiveView0),
  IsThisMe = whereis(PeerName) =:= self(),

  ShouldAddPassive =
    AlreadyActiveViewHas orelse
    AlreadyPassiveViewHas orelse
    IsThisMe,

  case ShouldAddPassive of
    true ->
      State0;

    false ->
      PassiveView1 =
        case is_passive_view_full(State0) of
          true ->
            PickedKey = pick_one_randomly(PassiveView0),
            maps:remove(PickedKey, PassiveView0);

          false ->
            PassiveView0
        end,
      PassiveView2 = maps:put(PeerName, whereis(PeerName), PassiveView1),
      State0#?MODULE{passive_view=PassiveView2}
  end.

add_active(PeerName, HighPriority, State0) ->
  #?MODULE{active_view=ActiveView0} = State0,

  IsThisMe = is_this_me(PeerName),
  DoesActiveViewAlreadyHas = maps:is_key(PeerName, ActiveView0),

  case IsThisMe orelse DoesActiveViewAlreadyHas of
    true  ->
      State0;
    false ->
      State1 = remove_active_if_full(State0),

      #?MODULE{active_view=ActiveView1, passive_view=PassiveView0, reactor=Reactor} = State1,
      ActiveView2 = maps:put(PeerName, get_node_pid(PeerName), ActiveView1),
      PassiveView1 = remove_passive(PeerName, PassiveView0),

      NeighborMsg = neighbor_message(HighPriority),

      ToPid = get_node_pid(PeerName),
      send_message_via_reactor(ToPid, NeighborMsg, Reactor),

      NeighborEvent = neighbor_event(up, PeerName),
      Reactor:notify(NeighborEvent),

      State1#?MODULE{active_view=ActiveView2, passive_view=PassiveView1}
  end.

send_message_async(NodePid, Msg) ->
  gen_server:cast(NodePid, Msg).

send_message_via_reactor(Pid, Msg, Reactor) ->
  From = self(),
  WithFromMsg = {Msg, From},
  Reactor:send(Pid, WithFromMsg).

send_messages_via_reactor(PidList, Msg, Reactor) ->
  lists:foreach(
    fun(Pid) ->
      send_message_via_reactor(Pid, Msg, Reactor)
    end, PidList).

is_this_me(PeerName) ->
  get_node_pid(PeerName) =:= self().

get_ttl(active_view, Config) ->
  #config{active_random_walk_length=TTL} = Config,
  TTL;
get_ttl(passive_view, Config) ->
  #config{passive_random_walk_length=TTL} = Config,
  TTL;
get_ttl(_, _) ->
  % can't reach here.
  undefined.

get_view_capacity(active, Config) ->
  #config{active_view_capacity=Capacity} = Config,
  Capacity;
get_view_capacity(passive, Config) ->
  #config{passive_view_capacity=Capacity} = Config,
  Capacity;
get_view_capacity(passive, _Config) ->
  % can't reach here.
  undefined.

get_node_name_by_pid(Pid) ->
  case erlang:process_info(Pid, registered_name) of
    {registered_name, Name} -> Name;
    _ -> undefined
  end.


get_node_pid(NodeName) ->
  whereis(NodeName).

get_node_pids(active_view, Views) ->
  maps:values(Views).

pick_node_pid_randomly(PeerMap) when map_size(PeerMap) =:= 0 ->
  undefined;
pick_node_pid_randomly(PeerMap) ->
  PeerPidList = maps:values(PeerMap),
  Size = maps:size(PeerMap),
  RandomIndex = rand:uniform(Size),
  lists:nth(RandomIndex, PeerPidList).

pick_one_randomly(PeerMap) when map_size(PeerMap) =:= 0 ->
  undefined;
pick_one_randomly(PeerMap) ->
  Size = maps:size(PeerMap),
  ViewKeyList = maps:keys(PeerMap),
  Index = rand:uniform(Size),
  lists:nth(Index, ViewKeyList).


schedule_shuffle(ShuffleInterval) ->
  Self = self(),
  Msg = {do_shuffle},
  timer:apply_after(ShuffleInterval, gen_server, cast, [Self, Msg]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%% HyParView Protocol Message function %%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec join_message(
    NewlyAddedPeerName :: atom()
) ->
  {join, atom()}.
join_message(NewlyAddedPeerName) ->
  {join, NewlyAddedPeerName}.

-spec forward_join_message(
    NewlyAddedPeerName :: atom(),
    TTL :: integer()
) ->
  {forward_join, atom(), integer()}.
forward_join_message(NewlyAddedPeerName, TTL) ->
  {forward_join, NewlyAddedPeerName, TTL}.

neighbor_message(HighPriority) ->
  {neighbor, HighPriority}.

disconnect_message(Alive, Respond) ->
  {disconnect, Alive, Respond}.

shuffle_message(Origin, Nodes, TTL) ->
  {shuffle, Origin, Nodes, TTL}.

shuffle_reply_message(Nodes) ->
  {shuffle_reply, Nodes}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%           Neighbor Event            %%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

neighbor_event(up, PeerName) ->
  {neighbor_up, PeerName};
neighbor_event(down, PeerName) ->
  {neighbor_down, PeerName};
neighbor_event(_, _) ->
  % can't reach here.
  {undefined, undefined}.