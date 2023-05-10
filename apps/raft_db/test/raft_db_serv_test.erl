-module(raft_db_serv_test).

-define(TEST_SERVERS, [a1, a2, a3]).

-export([test/0, stop_all/0]).

test() ->
    [raft_db_serv:start_link({Server, ?TEST_SERVERS}) || Server <- ?TEST_SERVERS].

stop_all() ->
    [raft_db_serv:stop(Server) || Server <- ?TEST_SERVERS].
