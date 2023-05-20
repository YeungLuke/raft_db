-module(raft_db_log_state).

-export([load_state/3, close/1, save_vote_info/3,
         append_log/4, append_no_op_log/2, append_leader_logs/5,
         log_info/2, last_log_info/1, last_log_index/1, commit_index/1,
         get_logs_from/3,
         apply_to_state_machine/4, apply_follower_state_machine/1]).

-define(LOOKUP_LOG_SIZE, 1000).
-define(CACHE_SIZE, 2000).

-record(log_state, {name,
                    state_machine,
                    % Volatile state on all servers:
                    commit_index=0, % index of highest log entry known to be committed (initialized to 0, increases monotonically)
                    last_applied=0, % index of highest log entry applied to state machine (initialized to 0, increases monotonically)
                    % Other
                    last_log_info={0, 0},
                    cache=#{}}).

load_vote_info(Name) ->
    case dets:lookup(Name, state) of
        [{state, Term, VotedFor}] ->
            {Term, VotedFor};
        _ ->
            {0, null}
    end.

name({Reg, _Node}) ->
    Reg;
name(Self) when is_atom(Self) ->
    Self.

save_vote_info(Self, Term, VotedFor) ->
    dets:insert(name(Self), {state, Term, VotedFor}).

load_last_log_info(Name) ->
    case dets:lookup(Name, last) of
        [{last, Index}] ->
            log_info(Name, Index);
        _ ->
            {0, 0}
    end.

load_logs_in_range(Name, From, To) ->
    % todo need test more like big table
    % io:format("~p get logs ~p to ~p~n", [Name, From, To]),
    case (To - From) < ?LOOKUP_LOG_SIZE of
        true  ->
            lists:flatten([dets:lookup(Name, I) || I <- lists:seq(From, To)]);
        _ ->
            lists:sort(dets:select(Name, [{{'$1','_','_','_'}, [{'andalso',{'>=','$1',From},{'=<','$1',To}}], ['$_']}]))
    end.

load_to_cache(Name, {LastLogIndex, _}) ->
    LogEntries = load_logs_in_range(Name, max(LastLogIndex - ?CACHE_SIZE, 1), LastLogIndex),
    maps:from_list([{Index, L} || L={Index, _, _, _} <- LogEntries]).

load_state(ServerName, FileName, MachineName) ->
    {ok, ServerName} = dets:open_file(ServerName, [{file, FileName}, {auto_save, 10}]),
    LastLogInfo = load_last_log_info(ServerName),
    {load_vote_info(ServerName), #log_state{name=ServerName,
                                      state_machine=MachineName,
                                      last_log_info=LastLogInfo,
                                      cache=load_to_cache(ServerName, LastLogInfo)}}.

close(#log_state{name=Name}) ->
    dets:close(Name).

get_log(#log_state{name=Name, cache=Cache}, Index) ->
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

log_info(LogState, Index) ->
    {I, Term, _, _} = get_log(LogState, Index),
    {I, Term}.

log_match(_, 0, _) ->
    true;
log_match(LogState, Index, Term) ->
    log_info(LogState, Index) =:= {Index, Term}.

get_logs_in_range(#log_state{name=Name, cache=Cache}, From, To) ->
    case maps:find(From, Cache) of
        {ok, Log} ->
            [Log | [maps:get(N, Cache) || N <- lists:seq(From + 1, To)]];
        _ ->
            load_logs_in_range(Name, From, To)
    end.

get_logs_from(LogState=#log_state{last_log_info={LastIndex, _}}, From, MaxSize) ->
    get_logs_in_range(LogState, max(From, 1), min(From + MaxSize - 1, LastIndex)).

last_log_info(LogState) ->
    LogState#log_state.last_log_info.

last_log_index(#log_state{last_log_info={LastLogIndex, _}}) ->
    LastLogIndex.

commit_index(LogState) ->
    LogState#log_state.commit_index.

delete_conflict_entries(LogState=#log_state{name=Name, cache=Cache, last_log_info={LastLogIndex, Term}},
                        Index) when Index =< LastLogIndex ->
    dets:insert(Name, {last, Index - 1}),
    % [dets:delete(Name, I) || I <- lists:seq(Index, LastLogIndex)],
    % term is wrong, but it will be overwrite in append_log_entries immediately
    LogState#log_state{last_log_info={Index - 1, Term}, cache=maps:without(lists:seq(Index, LastLogIndex), Cache)};
delete_conflict_entries(LogState, _) ->
    LogState.

append_log(LogState=#log_state{last_log_info={LastLogIndex, _}}, Term, Sn, Cmd) ->
    Index = LastLogIndex + 1,
    append_log_entries(LogState, [{Index, Term, Sn, Cmd}]).

append_no_op_log(LogState, Term) ->
    append_log(LogState, Term, null, {no_op}).

% If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
update_follower_commit_index(LeaderCommitIndex, CommitIndex, LastLogIndex) when LeaderCommitIndex > CommitIndex ->
    min(LeaderCommitIndex, LastLogIndex);
update_follower_commit_index(_, CommitIndex, _) ->
    CommitIndex.

append_leader_logs(LogState=#log_state{commit_index=CommitIndex, last_log_info=LastLogInfo},
                   PrevLogIndex, PrevLogTerm, LeaderCommitIndex, LogEntries) ->
    case {log_match(LogState, PrevLogIndex, PrevLogTerm), LogEntries} of
        {false, _} ->
            {LogState, false};
        {true, []} ->
            NewCommitIndex = update_follower_commit_index(LeaderCommitIndex, CommitIndex, LastLogInfo),
            {LogState#log_state{commit_index=NewCommitIndex}, true};
        {true, [{LogIndex, _, _, _}|_]} ->
            {LastLogIndex, LastLogTerm, _, _} = lists:last(LogEntries),
            NewLogState =
            case {LastLogIndex, LastLogTerm} =:= LastLogInfo of
                false ->
                    % If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
                    LogState1 = delete_conflict_entries(LogState, LogIndex),
                    append_log_entries(LogState1, LogEntries);
                _ ->
                    LogState
            end,
            NewCommitIndex = update_follower_commit_index(LeaderCommitIndex, CommitIndex, last_log_index(NewLogState)),
            {NewLogState#log_state{commit_index=NewCommitIndex}, true}
    end.

apply_follower_state_machine(LogState=#log_state{last_applied=LastApplied,
                                                 commit_index=CommitIndex,
                                                 state_machine=StateMachine,
                                                 cache=Cache}) ->
    gen_server:cast(StateMachine, {apply, LastApplied + 1, CommitIndex, #{}, Cache}),
    LogState#log_state{last_applied=CommitIndex}.

append_to_cache(Cache, [Log], _OldLast, NewLast) ->
    NewCache = maps:put(NewLast, Log, Cache),
    case maps:size(NewCache) > ?CACHE_SIZE of
        true ->
            maps:remove(NewLast - ?CACHE_SIZE, NewCache);
        _ ->
            NewCache
    end;
append_to_cache(Cache, LogEntries, OldLast, NewLast) ->
    LogMaps = maps:from_list([{Index, L} || L={Index, _, _, _} <- LogEntries]),
    Merge = maps:merge(Cache, LogMaps),
    CacheSize = maps:size(Cache),
    case maps:size(LogMaps) + CacheSize > ?CACHE_SIZE of
        true ->
            maps:without(lists:seq(OldLast - CacheSize + 1, NewLast - ?CACHE_SIZE), Merge);
        _ ->
            Merge
    end.

append_log_entries(LogState=#log_state{name=Name, cache=Cache, last_log_info={OldLastIndex, _}}, LogEntries) ->
    {LastLogIndex, LastLogTerm, _, _} = lists:last(LogEntries),
    % todo may not save last here
    % io:format("~p append ~p logs, last index ~p~n", [Name, length(LogEntries), LastLogIndex]),
    NewCache = append_to_cache(Cache, LogEntries, OldLastIndex, LastLogIndex),
    dets:insert(Name, LogEntries ++ [{last, LastLogIndex}]),
    LogState#log_state{last_log_info={LastLogIndex, LastLogTerm}, cache=NewCache}.

update_commit_index(LogState=#log_state{commit_index=CommitIndex}, MajorityIndex, Term) ->
    case log_info(LogState, MajorityIndex) of
        {MajorityIndex, Term} when MajorityIndex > CommitIndex ->
            MajorityIndex;
        _ ->
            CommitIndex
    end.

apply_to_state_machine(LogState=#log_state{last_applied=LastApplied,
                                           state_machine=StateMachine,
                                           cache=Cache},
                       MajorityIndex, Term, NeedReply) ->
    NewCommitIndex = update_commit_index(LogState, MajorityIndex, Term),
    {NeedSendReply, NewNeedReply} = 
    case maps:size(NeedReply) of
        0 ->
            {#{}, #{}};
        _ ->
        {maps:with(lists:seq(LastApplied + 1, NewCommitIndex), NeedReply),
         maps:without(lists:seq(LastApplied + 1, NewCommitIndex), NeedReply)}
    end,
    gen_server:cast(StateMachine, {apply, LastApplied + 1, NewCommitIndex, NeedSendReply, Cache}),
    {LogState#log_state{commit_index=NewCommitIndex,
                        last_applied=NewCommitIndex}, NewNeedReply}.
