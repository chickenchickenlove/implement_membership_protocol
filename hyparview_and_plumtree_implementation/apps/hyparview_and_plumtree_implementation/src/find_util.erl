-module(find_util).

%% API
-export([get_node_name_by_pid/1]).
-export([get_node_pid/1]).

get_node_name_by_pid(Pid) ->
  case erlang:process_info(Pid, registered_name) of
    {registered_name, Name} -> Name;
    _ -> undefined
  end.

get_node_pid(NodeName) ->
  whereis(NodeName).