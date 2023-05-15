-module(raft_db_state_machine).

-export([new/0, apply_cmd/2]).

new() ->
    #{}.

apply_cmd(S, {put, Key, Value}) ->
    {S#{Key => Value}, ok};
apply_cmd(S, {get, Key}) ->
    Result =
    case maps:find(Key, S) of
        {ok, Value} ->
            {ok, Value};
        _ ->
            {error, not_found}
    end,
    {S, Result};
apply_cmd(S, {delete, Key}) ->
    {maps:remove(Key, S), ok};
apply_cmd(S, {no_op}) ->
    {S, ok};
apply_cmd(S, _) ->
    {S,{error, unknow}}.
