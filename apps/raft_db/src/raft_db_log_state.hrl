-record(log_state, {name,
                    state_machine=raft_db_state_machine:new(),
                    % Volatile state on all servers:
                    commit_index=0, % index of highest log entry known to be committed (initialized to 0, increases monotonically)
                    last_applied=0, % index of highest log entry applied to state machine (initialized to 0, increases monotonically)
                    % Volatile state on leaders
                    next_index=#{}, % for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
                    match_index=#{}, % for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
                    % Other
                    last_log_info={0, 0}}).
