-module(raft_db_election_test).

-define(TEST_SERVERS, [a1, a2, a3]).

-export([test/0]).

test() ->
    [raft_db_election:start_link({Server, ?TEST_SERVERS}) || Server <- ?TEST_SERVERS].
