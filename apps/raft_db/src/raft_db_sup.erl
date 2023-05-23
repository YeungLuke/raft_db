%%%-------------------------------------------------------------------
%% @doc raft_db top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(raft_db_sup).
-behaviour(supervisor).
-include("raft_db_name.hrl").

-export([start_link/1]).

-export([init/1]).

start_link(Arg) ->
    Names = raft_db_name:names(Arg),
    supervisor:start_link({local, Names#names.sup_name}, ?MODULE, Names).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init(#names{servers=Servers}) when length(Servers) < 3 ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}};
init(Names=#names{server_name=ServerName, machine_name=MachineName}) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [#{id => ServerName,
                    start => {raft_db_serv, start_link, [Names]}},
                  #{id => MachineName,
                    start => {raft_db_state_machine_serv, start_link, [Names]}}],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
