-module(hypar_view_node).
-behavior(gen_server).


%% gen_server API.
-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([join/2]).


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
                  reactor = hypar_reactor :: atom()
}).


main() ->
  NodeName1 = 'A',
  Config1 = #config{},
  {ok, Pid1} = hypar_view_node:start_link(NodeName1, Config1).

start_link(NodeName, Config) ->
  gen_server:start_link(?MODULE, [NodeName, Config], []).


join(NodeName, ClusterNodeName) ->
  NodePid = whereis(NodeName),
  Msg = {api_join, NodeName, ClusterNodeName},
  send_message_async(NodePid, Msg).

init([NodeName, Config]) ->
  SelfPid = self(),
  register(NodeName, SelfPid),

  % TODO : Add a scheduling shuffle message code here.
  State = #?MODULE{name=NodeName, pid=SelfPid, config=Config},
  {ok, State}.

handle_call(Msg, From, State) ->
  {noreply, State}.


handle_cast({api_join, MyName, ClusterNodeName}, State) ->
  io:format("[~p] node ~p received api join message. try to join node ~p.~n", [self(), self(), ClusterNodeName]),
  #?MODULE{reactor=Reactor} = State,

  ClusterNodePid = whereis(ClusterNodeName),
  JoinMsg = join_message(MyName),

  send_message_via_reactor(ClusterNodePid, JoinMsg, Reactor),
  {noreply, State};

handle_cast({join, NewlyAddedPeerName}, State0) ->
  io:format("[~p] node ~p received join message. Newly Added PeerName ~p.~n", [self(), self(), NewlyAddedPeerName]),

  #?MODULE{config=Config, active_view=ActiveView0, reactor=Reactor} = State0,

  AddedPeerPid = get_node_pid(NewlyAddedPeerName),
  ActiveView = maps:put(NewlyAddedPeerName, AddedPeerPid, ActiveView0),
  ActiveTTL = get_ttl(active_view, Config),

  ForwardMsg = forward_join_message(NewlyAddedPeerName, ActiveTTL),

  ActiveViewWithoutNewPeerList = get_node_pids(active_view, ActiveView0),
  send_messages_via_reactor(ActiveViewWithoutNewPeerList, ForwardMsg, Reactor),
  State = State0#?MODULE{active_view=ActiveView},

  {noreply, State};

handle_cast({forward_join, NewlyAddedPeerName, TTL}, State0) ->
  io:format("[~p] node ~p received forward_join message. Newly Added PeerName ~p and TTL ~p.~n", [self(), self(), NewlyAddedPeerName, TTL]),
  #?MODULE{active_view=ActiveView0, passive_view=Reactor, config=Config} = State0,

  ShouldAddItToActive = ttl =:= 0 orelse maps:size(ActiveView0) =:= 0,

  case ShouldAddItToActive of
    true ->
      add_active(NewlyAddedPeerName, true, State0);

    false ->
      PassiveViewTTL = get_ttl(passive_view, Config),
      State1 =
        case TTL =:= PassiveViewTTL of
          true ->
            add_passive(NewlyAddedPeerName, State0);
          false ->
            State0
        end,

      ActiveViewExceptAddedPeer =  maps:remove(NewlyAddedPeerName, ActiveView0),
      case pick_node_pid_randomly(ActiveViewExceptAddedPeer) of
        % LAST NODE CASE
        undefined ->
          add_active(NewlyAddedPeerName, true, State1);
        NodePid ->
          ForwardJoinMsg = forward_join_message(NewlyAddedPeerName, TTL - 1),
          Reactor:send(NodePid, ForwardJoinMsg),
          State1
      end
  end;

handle_cast(Msg, State) ->
  {noreply, State}.

handle_info(Msg, State) ->
  {noreply, State}.


remove_active_if_full(ActiveView, ActiveViewCapacity) ->
  % TODO
  ok.

add_passive(NewlyAddedPeerName, State0) ->
  % TODO
  ok.

add_active(PeerName, HighPriority, State0) ->
  #?MODULE{active_view=ActiveView0,
           passive_view=PassiveView0,
           reactor=Reactor,
           config=Config} = State0,

  IsThisMe = get_node_pid(PeerName) =:= self(),
  DoesActiveViewAlreadyHas = maps:is_key(PeerName, ActiveView0),

  case IsThisMe orelse DoesActiveViewAlreadyHas of
    true ->
      State0;

    false ->
      PassiveView = maps:remove(PeerName, PassiveView0),

      PeerPid = get_node_pid(PeerName),

      ActiveViewCapacity = get_view_capacity(active, Config),
      ActiveView1 = remove_active_if_full(ActiveView0, ActiveViewCapacity),
      ActiveView = maps:put(PeerName, PeerPid, ActiveView1),

      NeighborMsg = neighbor_message(HighPriority),
      Reactor:send(PeerPid, NeighborMsg),

      UpEvent = neighbor_event(neighbor_up, PeerName),
      Reactor:notify(UpEvent),

      State0#?MODULE{active_view=ActiveView, passive_view=PassiveView}
  end.

send_message_async(NodePid, Msg) ->
  gen_server:cast(NodePid, Msg).

send_message_via_reactor(Pid, Msg, Reactor) ->
  Reactor:send(Pid, Msg).

send_messages_via_reactor(PidList, Msg, Reactor) ->
  lists:foreach(
    fun(Pid) ->
      Reactor:send(Pid, Msg)
    end, PidList).

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

get_node_pid(NodeName) ->
  whereis(NodeName).

get_node_pids(active_view, Views) ->
  maps:values(Views).

pick_node_pid_randomly(PeerMap) when map_size(PeerMap) =:= 0 ->
  undefined;
pick_node_pid_randomly(PeerMap) ->
  PeerPidList = maps:values(PeerMap),
  Size = maps:size(PeerMap),
  RandomIndex = random:uniform(Size),
  lists:nth(RandomIndex, PeerPidList).


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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%           Neighbor Event            %%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

neighbor_event(neighbor_up, PeerName) ->
  {neighbor_up, PeerName};
neighbor_event(neighbor_down, PeerName) ->
  {neighbor_down, PeerName};
neighbor_event(_, _) ->
  % can't reach here.
  {undefined, undefined}.