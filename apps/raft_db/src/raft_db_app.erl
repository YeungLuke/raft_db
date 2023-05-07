%%%-------------------------------------------------------------------
%% @doc raft_db public API
%% @end
%%%-------------------------------------------------------------------

-module(raft_db_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    raft_db_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
