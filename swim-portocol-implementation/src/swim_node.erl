-module(swim_node).
-behavior(gen_server).

%% API
-export([start_link/2]).

% gen_server callback
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([main/0]).

% 5s
-define(DEFAULT_INIT_JOIN_TIMEOUT, 50000).
-record(?MODULE, {
  my_state = init,
  my_self = undefined,
  actives = #{},
  suspects = #{},
  try_join_timer = undefined
}).

main() ->
  swim_node:start_link([], 'A'),
  timer:sleep(1000),
  swim_node:start_link(['A'], 'B'),
  % There are potential concurrency problem.
  % Because 'B'는 is not a cluster member yet.
  % So, Process 'C' may fail to find Node 'B'.
  swim_node:start_link(['A', 'B'], 'C').
%%  swim_node:start_link(['A', 'B'], 'C').


start_link(ClusterNodes, MyAlias) ->
  ClusterNodesWithPid = [whereis(ClusterNodeAlias) || ClusterNodeAlias <- ClusterNodes],
  gen_server:start_link(?MODULE, [ClusterNodesWithPid, MyAlias], []).

init([ClusterNodes, MyAlias]) ->
  io:format("[~p] swim_node [~p] try to intialized ~n", [self(), self()]),
  erlang:register(MyAlias, self()),
  ActiveCluster = case ClusterNodes of
                    % 클러스터의 첫번째 노드일 때,
                    [] -> #{self() => 1};
                    ClusterNodes ->
                      Msg = {init, ClusterNodes},
                      erlang:send_after(0, self(), Msg),
                      #{}
                  end,
  State = #?MODULE{my_self=self(), actives=ActiveCluster},
  {ok, State}.

% 모르는 메세지
handle_call(_, _From, _State) ->
  {reply, _State, _State}.

% 클러스터 조인 요청을 할 때
handle_info({init, KnownClusterNodes}, State) ->
  MaybeTimer = case KnownClusterNodes of
                 [] -> error(failed_to_join);
                 [Node | Rest] ->
                   Msg = {join, self()},
                   gen_server:cast(Node, Msg),
                   {ok, Timer} = timer:send_after(?DEFAULT_INIT_JOIN_TIMEOUT, {init, Rest}),
                   Timer
               end,
  NewState = State#?MODULE{try_join_timer=MaybeTimer},
  {noreply, NewState};
handle_info(_Msg, State) ->
  {noreply, State}.

% 클러스터 조인 요청을 받음.
handle_cast({join, TryToJoinPid}, State) ->
  io:format("[~p] swim_node [~p] receive join request from ~p ~n", [self(), self(), TryToJoinPid]),
  #?MODULE{actives=Actives} = State,
  NewActives = maps:put(TryToJoinPid, 1, Actives),
  GossipMsg = {joined, NewActives},
  lists:foreach(
    fun
      (Pid) when Pid =/= self() -> gen_server:cast(Pid, GossipMsg);
      (_Pid) -> ok
    end, maps:keys(NewActives)),
  NewState = State#?MODULE{actives=NewActives},
  {noreply, NewState};

% 클러스터 Active의 상태가 변화함. Gossip
handle_cast({joined, NewActives}, #?MODULE{try_join_timer=PreviousTimer}=State) ->
  io:format("[~p] swim_node [~p] receive joined gossip~n", [self(), self()]),
  timer:cancel(PreviousTimer),
  #?MODULE{actives=Actives} = State,
  UpdatedActives = maps:merge(Actives, NewActives),
  NewState = State#?MODULE{actives=UpdatedActives, try_join_timer=undefined},
  {noreply, NewState};

handle_cast(_Msg, State) ->
  {no_reply, State}.