-module(raft_db_log_state).
-include("./raft_db_log_state.hrl").

-export([load_state/1, save_vote_info/3, append_log/4, append_no_op_log/2, append_leader_logs/5, last_log_info/1]).

name({_Reg, Node}) when is_atom(Node) ->
    Node;
name(Self) when is_atom(Self) ->
    Self.

file_name(Self) ->
    atom_to_list(name(Self)) ++ ".db".

load_vote_info(Name) ->
    case dets:lookup(Name, state) of
        [{state, Term, VotedFor}] ->
            {Term, VotedFor};
        _ ->
            {0, null}
    end.

save_vote_info(Self, Term, VotedFor) ->
    dets:insert(name(Self), {state, Term, VotedFor}).

log_info(Name, Index) ->
    case dets:lookup(Name, Index) of
        [{Index, Term, _, _}] ->
            {Index, Term};
        _ ->
            {0, 0}
    end.

load_last_log_info(Name) ->
    case dets:lookup(Name, last) of
        [{last, Index}] ->
            log_info(Name, Index);
        _ ->
            {0, 0}
    end.

log_match(_, 0, _) ->
    true;
log_match(Name, Index, Term) ->
    case dets:lookup(Name, Index) of
        [{Index, Term, _, _}] ->
            true;
        _ ->
            false
    end.

load_state(Self) ->
    Name = name(Self),
    FileName = file_name(Self),
    {ok, Name} = dets:open_file(Name, [{file, FileName}]),
    {load_vote_info(Self), #log_state{name=Name,last_log_info=load_last_log_info(Name)}}.

last_log_info(LogState) ->
    LogState#log_state.last_log_info.

delete_conflict_entries(Name, {LastLogIndex, _}, Index) when Index =< LastLogIndex ->
    dets:insert(Name, {last, LastLogIndex - 1});
    % [dets:delete(Name, I) || I <- lists:seq(Index, LastLogIndex)];
delete_conflict_entries(_, _, _) ->
    ok.

append_log(LogState=#log_state{last_log_info={LastLogIndex, _}}, Term, Sn, Cmd) ->
    Index = LastLogIndex + 1,
    append_log_entries(LogState, [{Index, Term, Sn, Cmd}]).

append_no_op_log(LogState, Term) ->
    append_log(LogState, Term, null, {no_op}).

append_leader_logs(LogState=#log_state{name=Name, last_log_info=LastLogInfo, commit_index=CommitIndex},
                   LeaderLastIndex, LeaderLastTerm, LeaderCommitIndex, LogEntries) ->
    case log_match(Name, LeaderLastIndex, LeaderLastTerm) of
        false ->
            {LogState, false};
        true ->
            NewLogState =
            case LogEntries of
                [] ->
                    LogState;
                [{LogIndex, _, _, _}|_] ->
                    % If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
                    delete_conflict_entries(Name, LastLogInfo, LogIndex),
                    append_log_entries(LogState, LogEntries)
            end,
            {NewLastLogIndex, _} = last_log_info(NewLogState),
            % If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            NewCommitIndex = 
            case LeaderCommitIndex > CommitIndex of
                true ->
                    min(LeaderCommitIndex, NewLastLogIndex);
                false ->
                    CommitIndex
            end,
            {NewLogState#log_state{commit_index=NewCommitIndex}, true}
    end.

append_log_entries(LogState=#log_state{name=Name}, LogEntries) ->
    {LastLogIndex, LastLogTerm, _, _} = lists:last(LogEntries),
    % todo may not save last here
    dets:insert(Name, LogEntries ++ [{last, LastLogIndex}]),
    LogState#log_state{last_log_info={LastLogIndex, LastLogTerm}}.
