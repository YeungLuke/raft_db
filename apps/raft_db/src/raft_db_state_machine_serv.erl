-module(raft_db_state_machine_serv).
-behaviour(gen_server).

%% API
-export([start/1, stop/1, start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(machine_state, {name,
                        data=raft_db_state_machine:new(),
                        commit_index=0, % index of highest log entry known to be committed (initialized to 0, increases monotonically)
                        last_applied=0, % index of highest log entry applied to state machine (initialized to 0, increases monotonically)
                        cache=#{}}).
start(Name) ->
    raft_db_sup:start_child(Name).

stop(Name) ->
    gen_server:call(Name, stop).

start_link({Name, FileName}) ->
    gen_server:start_link({local, Name}, ?MODULE, {Name, FileName}, []).

init({Name, FileName}) ->
    {ok, Name} = dets:open_file(Name, [{file, FileName}]),
    {ok, #machine_state{name=Name}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({apply, From, To, NeedReply, Cache}, State) ->
    NewState = apply_to_state_machine(State#machine_state{cache=Cache}, From, To, NeedReply),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

get_log(#machine_state{name=Name, cache=Cache}, Index) ->
    case maps:find(Index, Cache) of
        {ok, Log} ->
            Log;
        _ ->
            get_log(Name, Index)
    end;
get_log(Name, Index) ->
    case dets:lookup(Name, Index) of
        [Log] ->
            Log;
        _ ->
            {0, 0, null, null}
    end.

apply_to_state_machine(MS, From, To, _) when From > To ->
    MS;
apply_to_state_machine(MS=#machine_state{data=S}, From, To, NeedReply) ->
    case get_log(MS, From) of
        {From, _, _, Cmd} ->
            {NewS, Result} = raft_db_state_machine:apply_cmd(S, Cmd),
            % reply to client if commitIndex >= index of client
            case maps:find(From, NeedReply) of {ok, Client} -> gen_server:reply(Client, Result); _ -> ok end,
            apply_to_state_machine(MS#machine_state{data=NewS}, From + 1, To, NeedReply)
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
