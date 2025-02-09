-module(gossip_protocol).

-callback broadcast(NodeName :: atom(), Data :: any()) -> any().
-callback handle_message(Message :: any(), State :: any()) -> UpdatedState :: any().
-callback schedule_loop_if_needed() -> any().