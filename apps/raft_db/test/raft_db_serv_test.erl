-module(raft_db_serv_test).

-define(TEST_SERVERS, [a1, a2, a3]).

-export([test/0, stop_all/0, test_speed/1]).

test() ->
    [raft_db_serv:start_link({Server, ?TEST_SERVERS}) || Server <- ?TEST_SERVERS].

stop_all() ->
    [raft_db_serv:stop(Server) || Server <- ?TEST_SERVERS].

recv(0) ->
    ok;
recv(N) ->
    receive
        {N, _R} ->
            recv(N-1)
        after 10000 ->
            timeout
    end.

test_speed(N) ->
    S = self(),
    Leader = raft_db_serv:who_is_leader(?TEST_SERVERS),
    lists:foreach(fun(N)-> spawn(fun() -> R=raft_db_serv:put(Leader, N, N+N), S ! {N,R} end) end, lists:seq(1,N)),
    recv(N).
