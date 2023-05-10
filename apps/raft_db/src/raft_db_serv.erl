-module(raft_db_serv).
-behaviour(gen_server).

%% API
-export([start/1, stop/1, start_link/1, who_is_leader/1, put/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(HEARTBEAT_TIME_INTERVAL, 100).
-define(ELECTION_TIMEOUT, 500).

-record(follower_state, {}).
-record(candidate_state, {votes = sets:new()}).
-record(leader_state, {href,
                       lived_servers = sets:new()}).

-record(log_state, {logs=[], % log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
                    % Volatile state on all servers:
                    commit_index=0, % index of highest log entry known to be committed (initialized to 0, increases monotonically)
                    last_applied=0, % index of highest log entry applied to state machine (initialized to 0, increases monotonically)
                    % Volatile state on leaders
                    next_index=[], % for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
                    match_index=[], % for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
                    % Other
                    replicating=[]}).

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
            null;
        Other ->
            who_is_leader(Other)
    end.

put(Server, Key, Value) ->
    gen_server:call(Server, {put, Key, Value}).

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
    case dets:lookup(Name, state) of
        [{state, Term, VotedFor}] ->
            {Term, VotedFor};
        _ ->
            {0, null}
    end.
    % case file:read_file(FileName) of
    %     {ok, Binary} ->
    %         binary_to_term(Binary);
    %     {error, _} ->
    %         {0, null}
    % end.

save_state(Self, NewTerm, OldTerm, NewVotedFor, OldVoteFor) when NewTerm > OldTerm orelse NewVotedFor =/= OldVoteFor ->
    % FileName = file_name(Self),
    % file:write_file(FileName, term_to_binary({NewTerm, NewVotedFor}));
    dets:insert(name(Self), {state, NewTerm, NewVotedFor});
save_state(_, _, _, _, _) ->
    ok.

last_log_info(Self) ->
    Name = name(Self),
    case dets:lookup(Name, last) of
        [{last, Index}] ->
            case dets:lookup(Name, Index) of
                [{Index, Term, _, _}] ->
                    {Index, Term};
                _ ->
                    {0, 0}
            end;
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
    {Index, _, _, _} = lists:last(LogEntries),
    dets:insert(name(Self), LogEntries ++ [{last, Index}]).

delete_conflict_entries(Self, Index, _Term) ->
    Name = name(Self),
    case dets:lookup(Name, last) of
        [{last, LastIndex}] when Index =< LastIndex ->
            [dest:delete(Name, I) || I <- lists:seq(Index, LastIndex)];
        _ ->
            ok
    end.

init({Self, Servers}) ->
    {Term, VotedFor} = load_state(Self),
    log("I am follower ~p in term ~p~n", [Self, Term]),
    TRef = erlang:start_timer(rand:uniform(?ELECTION_TIMEOUT) + ?ELECTION_TIMEOUT, self(), election_timeout),
    {ok, #state{vote_for=VotedFor, cur_term=Term, self=Self, servers=Servers, tref=TRef}}.

handle_call(who_is_leader, _From, State) ->
    {reply, State#state.leader, State};
handle_call({put, Key, Value}, _From, #state{status=leader,
                                             log_state=LogState,
                                             cur_term=CurTerm,
                                             servers=Servers,
                                             self=Self,
                                             dedicate_state=DState}=State) ->
    case LogState#log_state.replicating of
        [] ->
            {LastLogIndex, LastLogTerm} = last_log_info(Self),
            Index = LastLogIndex + 1,
            LogEntry = {Index, CurTerm, client_sn_todo, {Key, Value}},
            dets:insert(name(Self), [LogEntry, {last, Index}]),
            cancel_timers(State#state.dedicate_state),
            HRef = replicate_logs(CurTerm, Self, Servers, LastLogIndex, LastLogTerm, [LogEntry], LogState#log_state.commit_index),
            {noreply, State#state{log_state=LogState#log_state{replicating=[LogEntry]},
                                  dedicate_state=DState#leader_state{href=HRef}}};
        _ ->
            {reply, todo, State}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({request_vote, Term, CandidateId, {_LastLogIndex, _LastLogTerm}},
            #state{cur_term=CurTerm,self=Self}=State)
  when Term > CurTerm ->
    send_msg(CandidateId, {response_vote, Self, Term, true}),
    {noreply, convert_to_follower(Term, CandidateId, State)};
handle_cast({request_vote, CurTerm, CandidateId, _}, #state{status=follower,
                                                         vote_for=VotedFor,
                                                         cur_term=CurTerm,
                                                         self=Self}=State) ->
    case VotedFor =:= null orelse VotedFor =:= CandidateId of
        true ->
            send_msg(CandidateId, {response_vote, Self, CurTerm, true}),
            {noreply, convert_to_follower(CurTerm, CandidateId, State)};
        _ ->
            send_msg(CandidateId, {response_vote, Self, CurTerm, false}),
            {noreply, State}
    end;
handle_cast({request_vote, Term, CandidateId, _}, #state{cur_term=CurTerm,self=Self}=State) when Term < CurTerm ->
    send_msg(CandidateId, {response_vote, Self, CurTerm, false}),% should be CurTerm???
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
handle_cast({append_entries, CurTerm, LeaderId}, #state{status=Status,
                                                        vote_for=VotedFor,
                                                        cur_term=CurTerm,
                                                        self=Self}=State) when Status =/= leader ->
    send_msg(LeaderId, {response_entries, Self, CurTerm, true}),
    {noreply, convert_to_follower(CurTerm, VotedFor, LeaderId, State)};
handle_cast({append_entries, Term, LeaderId}, #state{cur_term=CurTerm,
                                                     self=Self}=State) when Term > CurTerm ->
    send_msg(LeaderId, {response_entries, Self, Term, true}),
    {noreply, convert_to_follower(Term, null, LeaderId, State)};
handle_cast({append_entries, Term, LeaderId},  #state{cur_term=CurTerm,
                                                      self=Self}=State) when Term < CurTerm ->
    send_msg(LeaderId, {response_entries, Self, CurTerm, false}),
    {noreply, State};
handle_cast({append_entries, CurTerm, LeaderId, LastLogIndex, LastLogTerm, LogEntries, CommitIndex},
            #state{status=Status, vote_for=VotedFor, cur_term=CurTerm, self=Self}=State) when Status =/= leader ->
    case log_match(Self, LastLogIndex, LastLogTerm) of
        false ->
            send_msg(LeaderId, {response_entries, Self, CurTerm, false});
        true ->
            case LogEntries of
                [] ->
                    ok;
                [{Index, Term, _, _}|_] ->
                    % If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
                    delete_conflict_entries(Self, Index, Term),
                    append_log_entries(Self, LogEntries)
            end,
            send_msg(LeaderId, {response_entries, Self, CurTerm, true})
    end,
    {noreply, convert_to_follower(CurTerm, VotedFor, LeaderId, State)}; % todo If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
handle_cast({response_entries, _Server, Term, _}, #state{cur_term=CurTerm}=State) when Term > CurTerm ->
    {noreply, convert_to_follower(Term, null, State)};
handle_cast({response_entries, Server, CurTerm, true}, #state{status=leader,
                                                              cur_term=CurTerm,
                                                              tref=TRef,
                                                              servers=Servers,
                                                              dedicate_state=DState}=State)->
    NewLivedServers = sets:add_element(Server, DState#leader_state.lived_servers),
    case sets:size(NewLivedServers) >= (length(Servers) div 2) of
        true ->
            erlang:cancel_timer(TRef),
            NewTRef = erlang:start_timer(2 * ?ELECTION_TIMEOUT, self(), election_timeout),
            NewDState = DState#leader_state{lived_servers=sets:new()},
            {noreply, State#state{tref=NewTRef, dedicate_state=NewDState}};
        _ ->
            NewDState = DState#leader_state{lived_servers=NewLivedServers},
            {noreply, State#state{dedicate_state=NewDState}}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

send_msg(To, Msg) ->
    gen_server:cast(To, Msg).

handle_info({timeout, TRef, election_timeout}, #state{tref=TRef}=State) ->
    {noreply, convert_to_candidate(State)};
handle_info({timeout, HRef, heartbeat_timeout}, #state{status=Status,
                                                       cur_term=CurTerm,
                                                       self=Self,
                                                       servers=Servers,
                                                       dedicate_state=DState}=State)
  when Status =:= leader, DState#leader_state.href =:= HRef ->
    NewHRef = send_heartbeat(CurTerm, Self, Servers),
    {noreply, State#state{dedicate_state=DState#leader_state{href=NewHRef}}};
handle_info(_Info, State) ->
    {noreply, State}.

convert_to_candidate(#state{cur_term=CurTerm, self=Self, servers=Servers}=State) ->
    NewTerm = CurTerm + 1,
    log("I am candidate ~p in term ~p~n", [Self, NewTerm]),
    cancel_timers(State),
    save_state(Self, NewTerm, CurTerm, Self, null),
    [send_msg(Other, {request_vote, NewTerm, Self, {1, 1}}) || Other <- Servers, Other =/= Self],
    TRef = erlang:start_timer(rand:uniform(?ELECTION_TIMEOUT) + ?ELECTION_TIMEOUT, self(), election_timeout),
    State#state{status=candidate,
                vote_for=Self,
                cur_term=NewTerm,
                leader=null,
                tref=TRef,
                dedicate_state=#candidate_state{}}.

convert_to_leader(#state{cur_term=Term, self=Self, servers=Servers}=State) ->
    log("I am leader ~p in term ~p~n",[Self, Term]),
    cancel_timers(State),
    HRef = send_heartbeat(Term, Self, Servers),
    TRef = erlang:start_timer(?ELECTION_TIMEOUT, self(), election_timeout),
    State#state{status=leader, tref=TRef, leader=Self, dedicate_state=#leader_state{href=HRef}}.

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

cancel_timers(#state{tref=TRef, dedicate_state=DState}) ->
    erlang:cancel_timer(TRef),
    cancel_timers(DState);
cancel_timers(#leader_state{href=HRef}) ->
    erlang:cancel_timer(HRef);
cancel_timers(_) ->
    ok.

send_heartbeat(Term, Self, Servers) ->
    [send_msg(Other, {append_entries, Term, Self}) || Other <- Servers, Other =/= Self],
    erlang:start_timer(rand:uniform(?HEARTBEAT_TIME_INTERVAL), self(), heartbeat_timeout).

replicate_logs(Term, Self, Servers, LastLogIndex, LastLogTerm, LogEntries, CommitIndex) ->
    [send_msg(Other, {append_entries, Term, Self, LastLogIndex, LastLogTerm, LogEntries, CommitIndex}) || Other <- Servers, Other =/= Self],
    erlang:start_timer(rand:uniform(?HEARTBEAT_TIME_INTERVAL), self(), heartbeat_timeout).

log(Format, Args) ->
    io:format(Format, Args).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
