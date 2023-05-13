-module(raft_db_serv).
-behaviour(gen_server).

%% API
-export([start/1, stop/1, start_link/1, who_is_leader/1, put/3, get/2, delete/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(HEARTBEAT_TIME_INTERVAL, 300).
-define(ELECTION_TIMEOUT, 1000).
-define(MAX_REPLY_SIZE, 200).
-define(MAX_LOG_SIZE, 300).
-define(LOOKUP_LOG_SIZE, 200).

-record(follower_info, {href=null, last_msg_type=null}).

-record(follower_state, {}).
-record(candidate_state, {votes = sets:new()}).
-record(leader_state, {followers_info = #{},
                       lived_servers = sets:new()}).

-record(log_state, {state_machine=raft_db_state_machine:new(),
                    % Volatile state on all servers:
                    commit_index=0, % index of highest log entry known to be committed (initialized to 0, increases monotonically)
                    last_applied=0, % index of highest log entry applied to state machine (initialized to 0, increases monotonically)
                    % Volatile state on leaders
                    next_index=#{}, % for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
                    match_index=#{}, % for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
                    % Other
                    need_reply=[],
                    last_log_info={0, 0}}).

-record(state, {status = follower,
                vote_for = null, % candidateId that received vote in current term (or null if none)
                cur_term = 0, % latest term server has seen (initialized to 0 on first boot, increases monotonically)
                self,
                servers,
                leader = null,
                tref,
                log_state=#log_state{},
                dedicate_state = #follower_state{}}).

start(Name) ->
    raft_db_sup:start_child(Name).

stop(Name) ->
    gen_server:call(Name, stop).

who_is_leader(Servers) when is_list(Servers) ->
    Server = lists:nth(rand:uniform(length(Servers)), Servers),
    who_is_leader(Server);
who_is_leader(Server) ->
    case gen_server:call(Server, who_is_leader) of
        Server ->
            Server;
        null ->
            no_leader;
        Other ->
            who_is_leader(Other)
    end.

call(Servers, Cmd) when is_list(Servers) ->
    case who_is_leader(Servers) of
        no_leader ->
            {error, no_leader};
        Leader ->
            call(Leader, Cmd)
    end;
call(Server, Cmd) ->
    gen_server:call(Server, {cmd, client_sn_todo, Cmd}).

put(Server, Key, Value) ->
    call(Server, {put, Key, Value}).

get(Server, Key) ->
    call(Server, {get, Key}).

delete(Server, Key) ->
    call(Server, {delete, Key}).

start_link({{Name, Node}=Self, Servers}) when is_atom(Node) ->
    gen_server:start_link({local, Name}, ?MODULE, {Self, Servers}, []);
start_link({Self, Servers}) when is_atom(Self) ->
    gen_server:start_link({local, Self}, ?MODULE, {Self, Servers}, []).

name({_Reg, Node}) when is_atom(Node) ->
    Node;
name(Self) when is_atom(Self) ->
    Self.

file_name(Self) ->
    atom_to_list(name(Self)) ++ ".db".

load_state(Self) ->
    Name = name(Self),
    FileName = file_name(Self),
    {ok, Name} = dets:open_file(Name, [{file, FileName}]),
    {vote_info(Self), last_log_info(Self)}.

save_state(Self, NewTerm, OldTerm, NewVotedFor, OldVoteFor) when NewTerm > OldTerm orelse NewVotedFor =/= OldVoteFor ->
    dets:insert(name(Self), {state, NewTerm, NewVotedFor});
save_state(_, _, _, _, _) ->
    ok.

vote_info(Self) ->
    case dets:lookup(name(Self), state) of
        [{state, Term, VotedFor}] ->
            {Term, VotedFor};
        _ ->
            {0, null}
    end.

log_info(Self, Index) ->
    case dets:lookup(name(Self), Index) of
        [{Index, Term, _, _}] ->
            {Index, Term};
        _ ->
            {0, 0}
    end.

last_log_info(Self) ->
    case dets:lookup(name(Self), last) of
        [{last, Index}] ->
            log_info(Self, Index);
        _ ->
            {0, 0}
    end.

log_match(_Self, 0, _) ->
    true;
log_match(Self, Index, Term) ->
    case dets:lookup(name(Self), Index) of
        [{Index, Term, _, _}] ->
            true;
        _ ->
            false
    end.

append_log_entries(Self, LogEntries) ->
    {LastLogIndex, LastLogTerm, _, _} = lists:last(LogEntries),
    dets:insert(name(Self), LogEntries ++ [{last, LastLogIndex}]),
    {LastLogIndex, LastLogTerm}.

get_logs_from(Self, From, To) ->
    Name = name(Self),
    % todo need test more like big table
    case (To - From) < ?LOOKUP_LOG_SIZE of
        true  ->
            lists:flatten([dets:lookup(Name, I) || I <- lists:seq(max(From, 1), To)]);
        _ ->
            lists:sort(dets:select(Name, [{{'$1','_','_','_'}, [{'andalso',{'>=','$1',From},{'=<','$1',To}}], ['$_']}]))
    end.

apply_to_state_machine(_, S, From, To, Results) when From > To ->
    {S, Results};
apply_to_state_machine(Self, S, From, To, Results) ->
    case dets:lookup(name(Self), From) of
        [{From, _, _, Cmd}] ->
            {NewS, Result} = raft_db_state_machine:apply_cmd(S, Cmd),
            apply_to_state_machine(Self, NewS, From + 1, To, Results#{From => Result});
        _ ->
            apply_to_state_machine(Self, S, From + 1, To, Results)
    end.

apply_to_state_machine(Self, S, From, To) ->
    apply_to_state_machine(Self, S, From, To, #{}).

delete_conflict_entries(Self, {LastLogIndex, _}, Index) when Index =< LastLogIndex ->
    Name = name(Self),
    dets:insert(Name, {last, LastLogIndex - 1});
    % [dets:delete(Name, I) || I <- lists:seq(Index, LastLogIndex)];
delete_conflict_entries(_, _, _) ->
    ok.

is_candidate_up_to_date({CurLastLogIndex, CurLastLogTerm}, {CandidateLastLogIndex, CandidateLastLogTerm}) ->
    CandidateLastLogTerm > CurLastLogTerm orelse 
        (CandidateLastLogTerm =:= CurLastLogTerm andalso CandidateLastLogIndex >= CurLastLogIndex).

init({Self, Servers}) ->
    {{Term, VotedFor}, LastLogInfo} = load_state(Self),
    log("I am follower ~p in term ~p~n", [Self, Term]),
    TRef = erlang:start_timer(rand:uniform(?ELECTION_TIMEOUT) + ?ELECTION_TIMEOUT, self(), election_timeout),
    {ok, #state{vote_for=VotedFor, cur_term=Term, self=Self, servers=Servers, tref=TRef,
                log_state=#log_state{last_log_info=LastLogInfo}}}.

handle_call(who_is_leader, _From, State) ->
    {reply, State#state.leader, State};
handle_call({cmd, Sn, Cmd}, From, #state{status=leader,
                                         log_state=LogState=#log_state{last_log_info={LastLogIndex, _}, need_reply=NeedReply},
                                         cur_term=CurTerm,
                                         self=Self,
                                         dedicate_state=DState}=State) ->
    case length(NeedReply) < ?MAX_REPLY_SIZE of
        true ->
            Index = LastLogIndex + 1,
            LastLogInfo = append_log_entries(Self, [{Index, CurTerm, Sn, Cmd}]),
            NewLogState = LogState#log_state{need_reply=[{Index, From}|NeedReply], last_log_info=LastLogInfo},
            NewFollowersInfo = replicate_logs(CurTerm, Self, NewLogState, DState#leader_state.followers_info),
            % log("I am leader ~p in term ~p, I am replicating log index ~p~n",[Self, CurTerm, Index]),
            {noreply, State#state{log_state=NewLogState,
                                  dedicate_state=DState#leader_state{followers_info=NewFollowersInfo}}};
        _ ->
            {reply, {error, too_many_requests}, State}
    end;
handle_call(stop, _From, #state{self=Self} = State) ->
    dets:close(name(Self)),
    {stop, normal, stopped, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({request_vote, Term, CandidateId, CandidateLastLogInfo},
            #state{cur_term=CurTerm, self=Self, log_state=#log_state{last_log_info=LastLogInfo}}=State)
  when Term > CurTerm ->
    UpToDate = is_candidate_up_to_date(LastLogInfo, CandidateLastLogInfo),
    case UpToDate of
        true ->
            send_msg(CandidateId, {response_vote, Self, Term, true}),
            {noreply, convert_to_follower(Term, CandidateId, State)};
        false ->
            send_msg(CandidateId, {response_vote, Self, Term, false}),
            {noreply, convert_to_follower(Term, null, State)}
    end;
handle_cast({request_vote, CurTerm, CandidateId, CandidateLastLogInfo},
            #state{status=follower, vote_for=VotedFor, cur_term=CurTerm, self=Self, log_state=#log_state{last_log_info=LastLogInfo}}=State) ->
    case (VotedFor =:= null orelse VotedFor =:= CandidateId) andalso is_candidate_up_to_date(LastLogInfo, CandidateLastLogInfo) of
        true ->
            send_msg(CandidateId, {response_vote, Self, CurTerm, true}),
            {noreply, convert_to_follower(CurTerm, CandidateId, State)};
        _ ->
            send_msg(CandidateId, {response_vote, Self, CurTerm, false}),
            {noreply, State}
    end;
handle_cast({request_vote, Term, CandidateId, _}, #state{cur_term=CurTerm,self=Self}=State) when Term < CurTerm ->
    send_msg(CandidateId, {response_vote, Self, CurTerm, false}),
    {noreply, State};
handle_cast({response_vote, _Server, Term, _}, #state{cur_term=CurTerm}=State) when Term > CurTerm ->
    {noreply, convert_to_follower(Term, null, State)};
handle_cast({response_vote, Server, CurTerm, true}, #state{status=candidate,
                                                           cur_term=CurTerm,
                                                           servers=Servers,
                                                           dedicate_state=DState}=State)->
    NewVotes = sets:add_element(Server, DState#candidate_state.votes),
    case sets:size(NewVotes) >= (length(Servers) div 2) of
        true ->
            {noreply, convert_to_leader(State)};
        _ ->
            {noreply, State#state{dedicate_state=DState#candidate_state{votes=NewVotes}}}
    end;
handle_cast({append_entries, Term, LeaderId, _, _, _, _},
            #state{cur_term=CurTerm, self=Self}=State) when Term < CurTerm ->
    send_msg(LeaderId, {response_entries, Self, CurTerm, false, not_change}),
    {noreply, State};
handle_cast({append_entries, Term, LeaderId, LastLogIndex, LastLogTerm, LogEntries, LeaderCommitIndex},
            #state{status=Status, vote_for=VotedFor, cur_term=CurTerm, self=Self,
                   log_state=LogState=#log_state{last_log_info=LastLogInfo, commit_index=CommitIndex}}=State)
  when Term > CurTerm orelse (Term =:= CurTerm andalso Status =/= leader) ->
    % todo ignore repeat msg
    % todo If commitIndex > lastApplied apply to state machine
    {NewLastLogInfo={NewLastLogIndex, _}, Succ} =
    case log_match(Self, LastLogIndex, LastLogTerm) of
        false ->
            {LastLogInfo, false};
        true ->
            case LogEntries of
                [] ->
                    {LastLogInfo, true};
                [{LogIndex, _, _, _}|_] ->
                    % If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
                    delete_conflict_entries(Self, LastLogInfo, LogIndex),
                    {append_log_entries(Self, LogEntries), true}
            end
    end,
    send_msg(LeaderId, {response_entries, Self, Term, Succ, NewLastLogIndex}),
    NewVotedFor = case Term > CurTerm of true -> null; false -> VotedFor end,
    % If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    NewCommitIndex = 
    case LeaderCommitIndex > CommitIndex of
        true ->
            min(LeaderCommitIndex, NewLastLogIndex);
        false ->
            CommitIndex
    end,
    {noreply, convert_to_follower(Term, NewVotedFor, LeaderId,
                                  State#state{log_state=LogState#log_state{last_log_info=NewLastLogInfo, commit_index=NewCommitIndex}})};
handle_cast({response_entries, _Server, Term, _, _}, #state{cur_term=CurTerm}=State) when Term > CurTerm ->
    {noreply, convert_to_follower(Term, null, State)};
handle_cast({response_entries, Server, CurTerm, Succ, FollowerLastIndex},
            #state{status=leader, cur_term=CurTerm, tref=TRef,
                   servers=Servers, self=Self,
                   log_state=LogState=#log_state{next_index=NextIndex,
                                                 match_index=MatchIndex,
                                                 commit_index=CommitIndex,
                                                 last_applied=LastApplied,
                                                 last_log_info={LastLogIndex, _}},
                   dedicate_state=DState=#leader_state{followers_info=FollowersInfo,lived_servers=LivedServers}}=State) ->
    NewLivedServers = sets:add_element(Server, LivedServers),
    {NewTRef, NewDState} = 
    case sets:size(NewLivedServers) >= (length(Servers) div 2) of
        true ->
            erlang:cancel_timer(TRef),
            {erlang:start_timer(2 * ?ELECTION_TIMEOUT, self(), election_timeout), DState#leader_state{lived_servers=sets:new()}};
        _ ->
            {TRef, DState#leader_state{lived_servers=NewLivedServers}}
    end,
    FollowerInfo = (maps:get(Server, FollowersInfo))#follower_info{last_msg_type=null},
    case Succ of
        true ->
            % update nextIndex and matchIndex for follwer
            {NewNext, NewMatch} =
            case FollowerLastIndex of
                not_change ->
                    NewN = maps:get(Server, NextIndex),
                    {NewN, NewN - 1};
                FollowerLastIndex when FollowerLastIndex =< LastLogIndex ->
                    {FollowerLastIndex + 1, FollowerLastIndex};
                % old leader may have more logs
                _ ->
                    {LastLogIndex + 1, LastLogIndex}
            end,
            NewLogState = LogState#log_state{next_index=NextIndex#{Server := NewNext},
                                             match_index=MatchIndex#{Server := NewMatch}},
            % If there exists an N such that N > commitIndex, a majority of matchIndex[i] >=  N, 
            % and log[N].term == currentTerm: set commitIndex = N
            N = lists:nth(length(Servers) div 2 + 1, lists:sort(maps:values(NewLogState#log_state.match_index))),
            NewCommitIndex =
            case log_info(Self, N) of
                {N, CurTerm} when N > CommitIndex ->
                    N;
                _ ->
                    CommitIndex
            end,
            % apply to state machine (todo if too many applies, and reply is empty, just apply some, or do apply in another proc)
            {NewS, Results} = 
            case LastApplied < NewCommitIndex of
                true ->
                    apply_to_state_machine(Self, NewLogState#log_state.state_machine,
                                           LastApplied + 1, NewCommitIndex);
                _ ->
                    {NewLogState#log_state.state_machine, #{}}
            end,
            % reply to client if commitIndex >= index of client
            [gen_server:reply(From, maps:get(Index, Results)) ||
             {Index, From} <- NewLogState#log_state.need_reply,
             NewCommitIndex >= Index, Index > LastApplied], 
            NewNeedReply = [{Index, From} || {Index, From} <- NewLogState#log_state.need_reply, NewCommitIndex < Index],
            % if follower matchindex =/= last, send remain log entries
            NewFollowerInfo = 
            case NewMatch < LastLogIndex of
                true ->
                    replicate_one_follower(CurTerm, Self, Server, LogState, FollowerInfo);
                _ ->
                    FollowerInfo
            end,
            {noreply, State#state{tref=NewTRef,
                                  dedicate_state=NewDState#leader_state{followers_info=FollowersInfo#{Server => NewFollowerInfo}},
                                  log_state=NewLogState#log_state{commit_index=NewCommitIndex,
                                                                  last_applied=NewCommitIndex,
                                                                  need_reply=NewNeedReply,
                                                                  state_machine=NewS}}};
        false ->
            % decrement nextIndex and retry
            PrevIndex = maps:get(Server, LogState#log_state.next_index) - 1,
            NewLogState =
            case PrevIndex > 0 of
                true ->
                    LogState#log_state{next_index=(LogState#log_state.next_index)#{Server := PrevIndex}};
                _ ->
                    LogState
            end,
            NewFollowerInfo = replicate_one_follower(CurTerm, Self, Server, LogState, FollowerInfo),
            {noreply, State#state{tref=NewTRef, log_state=NewLogState,
                                  dedicate_state=NewDState#leader_state{followers_info=FollowersInfo#{Server => NewFollowerInfo}}}}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

send_msg(To, Msg) ->
    gen_server:cast(To, Msg).

handle_info({timeout, TRef, election_timeout}, #state{tref=TRef}=State) ->
    {noreply, convert_to_candidate(State)};
handle_info({timeout, HRef, {heartbeat_timeout, Follower}},
            #state{status=leader, cur_term=CurTerm, self=Self, log_state=LogState,
                   dedicate_state=DState=#leader_state{followers_info=FollowersInfo}}=State) ->
    case FollowersInfo of
        #{Follower := FollowerInfo=#follower_info{href=HRef}} ->
            NewFollowerInfo = replicate_one_follower(CurTerm, Self, Follower, LogState, FollowerInfo#follower_info{href=null}),
            {noreply, State#state{dedicate_state=DState#leader_state{followers_info=FollowersInfo#{Follower => NewFollowerInfo}}}};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

convert_to_candidate(#state{cur_term=CurTerm, self=Self, servers=Servers, log_state=#log_state{last_log_info=LastLogInfo}}=State) ->
    NewTerm = CurTerm + 1,
    log("I am candidate ~p in term ~p~n", [Self, NewTerm]),
    cancel_timers(State),
    save_state(Self, NewTerm, CurTerm, Self, null),
    [send_msg(Other, {request_vote, NewTerm, Self, LastLogInfo}) || Other <- Servers, Other =/= Self],
    TRef = erlang:start_timer(rand:uniform(?ELECTION_TIMEOUT) + ?ELECTION_TIMEOUT, self(), election_timeout),
    State#state{status=candidate,
                vote_for=Self,
                cur_term=NewTerm,
                leader=null,
                tref=TRef,
                dedicate_state=#candidate_state{}}.

convert_to_leader(#state{cur_term=Term, self=Self, servers=Servers, log_state=LogState=#log_state{last_log_info={LastLogIndex, _}}}=State) ->
    log("I am leader ~p in term ~p~n",[Self, Term]),
    cancel_timers(State),
    Followers = Servers -- [Self],
    NextIndex = from_keys(Followers, LastLogIndex + 1),
    MatchIndex = from_keys(Followers, 0),
    NewLastLogInfo = append_log_entries(Self, [{LastLogIndex + 1, Term, null, {no_op}}]),
    NewLogState = LogState#log_state{next_index=NextIndex, match_index=MatchIndex, need_reply=[], last_log_info=NewLastLogInfo},
    FollowersInfo = send_heartbeat(Term, Self, NewLogState, from_keys(Followers, #follower_info{})),
    TRef = erlang:start_timer(?ELECTION_TIMEOUT, self(), election_timeout),
    State#state{status=leader, tref=TRef, leader=Self,
                dedicate_state=#leader_state{followers_info=FollowersInfo}, log_state=NewLogState}.

convert_to_follower(NewTerm, NewVotedFor, State) ->
    convert_to_follower(NewTerm, NewVotedFor, null, State).

convert_to_follower(NewTerm, NewVotedFor, NewLeader, #state{cur_term=OldTerm, vote_for=OldVoteFor, self=Self}=State) ->
    case NewTerm > OldTerm orelse State#state.status =/= follower of
        true ->
            log("I am follower ~p in term ~p, old term  ~p~n", [Self, NewTerm, OldTerm]);
        _ ->
            ok
    end,
    cancel_timers(State),
    save_state(Self, NewTerm, OldTerm, NewVotedFor, OldVoteFor),
    TRef = erlang:start_timer(rand:uniform(?ELECTION_TIMEOUT) + ?ELECTION_TIMEOUT,
                              self(),
                              election_timeout),
    State#state{status=follower,
                vote_for=NewVotedFor,
                cur_term=NewTerm,
                leader=NewLeader,
                tref=TRef,
                dedicate_state=#follower_state{}}.

cancel_timers(#state{tref=TRef}) ->
    erlang:cancel_timer(TRef);
cancel_timers(_) ->
    ok.

get_follower_missing_log(Self, Follower, #log_state{next_index=NextIndex, last_log_info={LastIndex, _}}) ->
    PrevIndex = maps:get(Follower, NextIndex) - 1,
    {PrevIndex, PrevTerm} = log_info(Self, PrevIndex),
    % control log size for msg
    LogEntries = get_logs_from(Self, PrevIndex + 1, min(PrevIndex + ?MAX_LOG_SIZE, LastIndex)),
    {PrevIndex, PrevTerm, LogEntries}.

replicate_one_follower(Term, Self, Follower, LogState, FollowerInfo=#follower_info{last_msg_type=LastType, href=HRef}) ->
    case LastType =:= normal andalso is_reference(HRef) of
        true ->
            FollowerInfo;
        _ ->
            case is_reference(HRef) of true -> erlang:cancel_timer(HRef); _ -> ok end,
            {PrevIndex, PrevTerm, LogEntries} = get_follower_missing_log(Self, Follower, LogState),
            Msg = {append_entries, Term, Self, PrevIndex, PrevTerm, LogEntries, LogState#log_state.commit_index},
            send_msg(Follower, Msg),
            % todo calc timeout by msg length
            MsgType = case LogEntries of [] -> heartbeat; _ -> normal end,
            NewHRef = erlang:start_timer(?HEARTBEAT_TIME_INTERVAL, self(), {heartbeat_timeout, Follower}),
            FollowerInfo#follower_info{last_msg_type=MsgType, href=NewHRef}
    end.

send_heartbeat(Term, Self, LogState, FollowersInfo) ->
    replicate_logs(Term, Self, LogState, FollowersInfo).

replicate_logs(Term, Self, LogState, FollowersInfo) ->
    % todo get follower missing Log can cache
    maps:map(fun(Follower, FollowerInfo) -> replicate_one_follower(Term, Self, Follower, LogState, FollowerInfo) end,
             FollowersInfo).

% not exist in otp 20
from_keys(Keys, Value) ->
    maps:from_list([{Key, Value} || Key <- Keys]).

log(Format, Args) ->
    io:format(Format, Args).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
