hyparview_and_plumtree_implementation
=====

An OTP application

```shell

$ rebar3 get-deps
$ rebar3 shell

$ 1> hypar_view_node:main().
$ 2> plum_tree_protocol:broadcast('A', 'hello!').
$ 3> plum_tree_protocol:broadcast('B', 'hello!').
```