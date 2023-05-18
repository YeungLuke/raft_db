-module(raft_db_serv_test).

-define(TEST_SERVERS, [a1, a2, a3]).

-export([start_all/0, stop_all/0, test_speed/1, test_round/2, test_stress/2]).

start_all() ->
    [raft_db_serv:start_link({Server, ?TEST_SERVERS}) || Server <- ?TEST_SERVERS].

stop_all() ->
    [raft_db_serv:stop(Server) || Server <- ?TEST_SERVERS].

wait_for_leader() ->
    case raft_db_serv:who_is_leader(?TEST_SERVERS) of
        {ok, Leader} ->
            Leader;
        {error, _} ->
            timer:sleep(500),
            wait_for_leader()
    end.

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
    {ok, Leader} = raft_db_serv:who_is_leader(?TEST_SERVERS),
    lists:foreach(fun(I)-> spawn(fun() -> R=raft_db_serv:put(Leader, I, I+I), S ! {I,R} end) end, lists:seq(1,N)),
    recv(N).

test_round(0, _) ->
    ok;
test_round(N, Batch) ->
    % 10 is the best batch size for concurrency test on local now
    test_speed(Batch),
    test_round(N-1,Batch).

stress_client(Fun, Key, Pid) ->
    R = Fun(),
    Pid ! {resp, Key, R},
    receive
        stop ->
            ok
        after 0 ->
            stress_client(Fun, Key, Pid)
    end.

manager_loop(Clients, Recved) ->
    receive
        {resp, _Key, _R} ->
            % io:format("recv resp from ~p, result ~p~n", [Key, _RJ),
            manager_loop(Clients, Recved+1);
        {print} ->
            io:format("recv resp ~p times~n", [Recved]),
            manager_loop(Clients, 0);
        stop ->
            io:format("stress test stoped~n"),
            [C ! stop || C <- Clients],
            stop_all()
    end.

stress_manager(N, Time) ->
    Leader = wait_for_leader(),
    Self = self(),
    Clients = [spawn(fun() -> stress_client(fun() -> raft_db_serv:put(Leader, CI, CI*2) end, CI, Self) end) || CI <- lists:seq(1, N)],
    timer:send_interval(1000, {print}),
    timer:send_after(Time*1000+100, stop),
    manager_loop(Clients, 0).

test_stress(N, Time) ->
    start_all(),
    spawn(fun() -> stress_manager(N, Time) end).
