-module(raft_db_serv).
-behaviour(gen_server).

%% API
-export([start/1, stop/1, start_link/1, who_is_leader/1, put/3, get/2, delete/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(ELECTION_TIMEOUT, 1000).
-define(MAX_REPLY_SIZE, 500).
-define(MAX_LOG_SIZE, 200).

-record(follower_state, {}).
-record(candidate_state, {votes = sets:new()}).
-record(leader_state, {followers_info = #{},
                       lived_servers = sets:new()}).

-record(state, {status = follower,
                vote_for = null, % candidateId that received vote in current term (or null if none)
                cur_term = 0, % latest term server has seen (initialized to 0 on first boot, increases monotonically)
                self,
                servers,
                leader = null,
                tref,
                log_state,
                need_reply=#{},
                dedicate_state = #follower_state{}}).

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

start(Name) ->
    raft_db_sup:start_child(Name).

stop(Name) ->
    gen_server:call(Name, stop).

start_link({{Name, Node}=Self, Servers}) when is_atom(Node) ->
    gen_server:start_link({local, Name}, ?MODULE, {Self, Servers}, []);
start_link({Self, Servers}) when is_atom(Self) ->
    gen_server:start_link({local, Self}, ?MODULE, {Self, Servers}, []).

init({Self, Servers}) ->
    {{Term, VotedFor}, LogState} = raft_db_log_state:load_state(Self),
    log("I am follower ~p in term ~p~n", [Self, Term]),
    TRef = erlang:start_timer(rand:uniform(?ELECTION_TIMEOUT) + ?ELECTION_TIMEOUT, self(), election_timeout),
    {ok, #state{vote_for=VotedFor, cur_term=Term, self=Self, servers=Servers, tref=TRef,
                log_state=LogState}}.

handle_call(who_is_leader, _From, State) ->
    {reply, State#state.leader, State};
handle_call({cmd, Sn, Cmd}, From, #state{status=leader,
                                         log_state=LogState,
                                         cur_term=CurTerm,
                                         self=Self,
                                         need_reply=NeedReply,
                                         dedicate_state=DState}=State) ->
    case maps:size(NeedReply) < ?MAX_REPLY_SIZE of
        true ->
            NewLogState = raft_db_log_state:append_log(LogState, CurTerm, Sn, Cmd),
            Index = raft_db_log_state:last_log_index(NewLogState),
            NewFollowersInfo = replicate_logs(CurTerm, Self, NewLogState, DState#leader_state.followers_info),
            % log("I am leader ~p in term ~p, I am replicating log index ~p~n",[Self, CurTerm, Index]),
            {noreply, State#state{log_state=NewLogState,
                                  need_reply=NeedReply#{Index => From},
                                  dedicate_state=DState#leader_state{followers_info=NewFollowersInfo}}};
        _ ->
            {reply, {error, too_many_requests}, State}
    end;
handle_call(stop, _From, #state{log_state=LogState} = State) ->
    raft_db_log_state:close(LogState),
    {stop, normal, stopped, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({request_vote, Term, CandidateId, CandidateLastLogInfo},
            #state{cur_term=CurTerm, self=Self, log_state=LogState}=State)
  when Term > CurTerm ->
    UpToDate = is_candidate_up_to_date(raft_db_log_state:last_log_info(LogState), CandidateLastLogInfo),
    case UpToDate of
        true ->
            send_msg(CandidateId, {response_vote, Self, Term, true}),
            {noreply, convert_to_follower(Term, CandidateId, State)};
        false ->
            send_msg(CandidateId, {response_vote, Self, Term, false}),
            {noreply, convert_to_follower(Term, null, State)}
    end;
handle_cast({request_vote, CurTerm, CandidateId, CandidateLastLogInfo},
            #state{status=follower, vote_for=VotedFor, cur_term=CurTerm, self=Self, log_state=LogState}=State) ->
    case (VotedFor =:= null orelse VotedFor =:= CandidateId) andalso
            is_candidate_up_to_date(raft_db_log_state:last_log_info(LogState), CandidateLastLogInfo) of
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
            #state{status=Status, vote_for=VotedFor, cur_term=CurTerm, self=Self, log_state=LogState}=State)
  when Term > CurTerm orelse (Term =:= CurTerm andalso Status =/= leader) ->
    % todo ignore repeat msg
    % todo If commitIndex > lastApplied apply to state machine
    {NewLogState, Succ} = raft_db_log_state:append_leader_logs(LogState, LastLogIndex, LastLogTerm, LeaderCommitIndex, LogEntries),
    send_msg(LeaderId, {response_entries, Self, Term, Succ, raft_db_log_state:last_log_index(NewLogState)}),
    NewVotedFor = case Term > CurTerm of true -> null; false -> VotedFor end,
    {noreply, convert_to_follower(Term, NewVotedFor, LeaderId,
                                  State#state{log_state=NewLogState})};
handle_cast({response_entries, _Server, Term, _, _}, #state{cur_term=CurTerm}=State) when Term > CurTerm ->
    {noreply, convert_to_follower(Term, null, State)};
handle_cast({response_entries, Server, CurTerm, Succ, FollowerLastIndex},
            #state{status=leader, cur_term=CurTerm, tref=TRef,
                   servers=Servers, self=Self,
                   log_state=LogState,
                   need_reply=NeedReply,
                   dedicate_state=DState=#leader_state{followers_info=FollowersInfo,
                                                       lived_servers=LivedServers}}=State) ->
    NewLivedServers = sets:add_element(Server, LivedServers),
    NewTRef = reset_election_timeout(NewLivedServers, Servers, TRef),

    FollowerInfo1 = raft_db_follower_info:reponced((maps:get(Server, FollowersInfo))),
    case Succ of
        true ->
            % update nextIndex and matchIndex for follower
            LastLogIndex = raft_db_log_state:last_log_index(LogState),
            FollowerInfo2 = raft_db_follower_info:update_index(FollowerInfo1, FollowerLastIndex, LastLogIndex),
            % If there exists an N such that N > commitIndex, a majority of matchIndex[i] >=  N, 
            % and log[N].term == currentTerm: set commitIndex = N
            MajorityIndex = raft_db_follower_info:majority_index(FollowersInfo#{Server := FollowerInfo2}),
            % apply to state machine (todo if too many applies, and reply is empty, just apply some, or do apply in another proc)
            {NewLogState, Results} = raft_db_log_state:apply_to_state_machine(LogState, MajorityIndex, CurTerm, NeedReply),
            % reply to client if commitIndex >= index of client
            NewNeedReply = maps:fold(fun(Index, V, Reply) ->
                                        {From, New} = maps:take(Index, Reply),
                                        gen_server:reply(From, V),
                                        New
                                     end, NeedReply, Results),
            % if follower matchindex < last, send remain log entries
            FollowerInfo3 = send_remaining_logs(CurTerm, Self, Server, LogState, FollowerInfo2),
            {noreply, State#state{tref=NewTRef,
                                  need_reply=NewNeedReply,
                                  dedicate_state=DState#leader_state{followers_info=FollowersInfo#{Server := FollowerInfo3},
                                                                     lived_servers=NewLivedServers},
                                  log_state=NewLogState}};
        false ->
            % decrement nextIndex and retry
            FollowerInfo2 = raft_db_follower_info:decrement_next_index(FollowerInfo1),
            FollowerInfo3 = replicate_one_follower(CurTerm, Self, Server, LogState, FollowerInfo2),
            {noreply, State#state{tref=NewTRef,
                                  dedicate_state=DState#leader_state{followers_info=FollowersInfo#{Server := FollowerInfo3},
                                                                     lived_servers=NewLivedServers}}}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, TRef, election_timeout}, #state{tref=TRef}=State) ->
    {noreply, convert_to_candidate(State)};
handle_info({timeout, HRef, {heartbeat_timeout, Follower}},
            #state{status=leader, cur_term=CurTerm, self=Self, log_state=LogState,
                   dedicate_state=DState=#leader_state{followers_info=FollowersInfo}}=State) ->
    FollowerInfo = maps:get(Follower, FollowersInfo),
    case raft_db_follower_info:is_cur_href(FollowerInfo, HRef) of
        true ->
            NewFollowerInfo = replicate_one_follower(CurTerm, Self, Follower, LogState,
                                                     raft_db_follower_info:timeout(FollowerInfo)),
            {noreply, State#state{dedicate_state=DState#leader_state{followers_info=FollowersInfo#{Follower => NewFollowerInfo}}}};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

convert_to_candidate(#state{cur_term=CurTerm, self=Self, servers=Servers, log_state=LogState}=State) ->
    NewTerm = CurTerm + 1,
    log("I am candidate ~p in term ~p~n", [Self, NewTerm]),
    cancel_timers(State),
    save_state(Self, NewTerm, CurTerm, Self, null),
    Msg = {request_vote, NewTerm, Self, raft_db_log_state:last_log_info(LogState)},
    [send_msg(Other, Msg) || Other <- Servers, Other =/= Self],
    TRef = erlang:start_timer(rand:uniform(?ELECTION_TIMEOUT) + ?ELECTION_TIMEOUT, self(), election_timeout),
    State#state{status=candidate,
                vote_for=Self,
                cur_term=NewTerm,
                leader=null,
                tref=TRef,
                dedicate_state=#candidate_state{}}.

convert_to_leader(#state{cur_term=Term, self=Self, servers=Servers, log_state=LogState}=State) ->
    log("I am leader ~p in term ~p, last index ~p~n",[Self, Term, raft_db_log_state:last_log_index(LogState)]),
    cancel_timers(State),
    Followers = Servers -- [Self],
    NewLogState = raft_db_log_state:append_no_op_log(LogState, Term),
    FollowersInfo = send_heartbeat(Term, Self, NewLogState,
                                   raft_db_follower_info:new(Followers, raft_db_log_state:last_log_index(LogState))),
    TRef = erlang:start_timer(?ELECTION_TIMEOUT, self(), election_timeout),
    State#state{status=leader, tref=TRef, leader=Self,
                dedicate_state=#leader_state{followers_info=FollowersInfo},
                log_state=NewLogState}.

convert_to_follower(NewTerm, NewVotedFor, State) ->
    convert_to_follower(NewTerm, NewVotedFor, null, State).

convert_to_follower(NewTerm, NewVotedFor, NewLeader, #state{cur_term=OldTerm, vote_for=OldVoteFor, self=Self}=State) ->
    case NewTerm > OldTerm orelse State#state.status =/= follower of
        true ->
            log("I am follower ~p in term ~p, old term ~p, last index ~p~n", [Self, NewTerm, OldTerm,
                    raft_db_log_state:last_log_index(State#state.log_state)]);
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

send_msg(To, Msg) ->
    gen_server:cast(To, Msg).

save_state(Self, NewTerm, OldTerm, NewVotedFor, OldVoteFor) when NewTerm > OldTerm orelse NewVotedFor =/= OldVoteFor ->
    raft_db_log_state:save_vote_info(Self, NewTerm, NewVotedFor);
save_state(_, _, _, _, _) ->
    ok.

is_candidate_up_to_date({CurLastLogIndex, CurLastLogTerm}, {CandidateLastLogIndex, CandidateLastLogTerm}) ->
    CandidateLastLogTerm > CurLastLogTerm orelse 
        (CandidateLastLogTerm =:= CurLastLogTerm andalso CandidateLastLogIndex >= CurLastLogIndex).

cancel_timers(#state{tref=TRef}) ->
    erlang:cancel_timer(TRef).

reset_election_timeout(NewLivedServers, Servers, TRef) ->
    case sets:size(NewLivedServers) >= (length(Servers) div 2) of
        true ->
            erlang:cancel_timer(TRef),
            erlang:start_timer(2 * ?ELECTION_TIMEOUT, self(), election_timeout);
        _ ->
            TRef
    end.

get_follower_missing_log(FollowerInfo, LogState) ->
    PrevIndex = raft_db_follower_info:next_index(FollowerInfo) - 1,
    {PrevIndex, PrevTerm} = raft_db_log_state:log_info(LogState, PrevIndex),
    % control log size for msg
    LogEntries = raft_db_log_state:get_logs_from(LogState, PrevIndex + 1, ?MAX_LOG_SIZE),
    {PrevIndex, PrevTerm, LogEntries}.

replicate_one_follower(Term, Self, Follower, LogState, FollowerInfo) ->
    case raft_db_follower_info:replicate_not_responced(FollowerInfo) of
        true ->
            FollowerInfo;
        _ ->
            {PrevIndex, PrevTerm, LogEntries} = get_follower_missing_log(FollowerInfo, LogState),
            Msg = {append_entries, Term, Self, PrevIndex, PrevTerm, LogEntries, raft_db_log_state:commit_index(LogState)},
            send_msg(Follower, Msg),
            raft_db_follower_info:update_replicate_info(FollowerInfo, Follower, LogEntries)
    end.

send_remaining_logs(Term, Self, Server, LogState, FollowerInfo) ->
    case raft_db_follower_info:is_up_to_date(FollowerInfo, raft_db_log_state:last_log_index(LogState)) of
        true ->
            replicate_one_follower(Term, Self, Server, LogState, FollowerInfo);
        _ ->
            FollowerInfo
    end.

send_heartbeat(Term, Self, LogState, FollowersInfo) ->
    replicate_logs(Term, Self, LogState, FollowersInfo).

replicate_logs(Term, Self, LogState, FollowersInfo) ->
    % todo get follower missing Log can cache
    maps:map(fun(Follower, FollowerInfo) -> replicate_one_follower(Term, Self, Follower, LogState, FollowerInfo) end,
             FollowersInfo).

log(Format, Args) ->
    io:format(Format, Args).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
