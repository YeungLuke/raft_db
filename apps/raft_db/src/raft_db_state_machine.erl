-module(raft_db_state_machine).

-export([new/0, apply_cmd/2]).

new() ->
    #{}.

apply_cmd(S, {put, Key, Value}) ->
    {S#{Key => Value}, ok};
apply_cmd(S, {get, Key}) ->
    Result =
    case maps:is_key(Key, S) of
        true ->
            {ok, maps:get(Key, S)};
        _ ->
            {error, not_found}
    end,
    {S, Result};
apply_cmd(S, _) ->
    {S,{error, unknow}}.
