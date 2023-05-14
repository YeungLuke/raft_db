-module(raft_db_follower_info).

-export([new/2, update_index/3, update_replicate_info/3, majority_index/1, timeout/1, decrement_next_index/1,
         is_cur_href/2, is_up_to_date/2, replicate_not_responced/1, next_index/1, reponced/1]).

-define(HEARTBEAT_TIME_INTERVAL, 300).

-record(follower_info, {href=null,
                        last_msg_type=null,
                        % for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
                        next_index,
                        % index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
                        match_index=0}).

new(LeaderLastIndex) ->
    #follower_info{next_index=LeaderLastIndex + 1}.

new(Followers, LeaderLastIndex)->
    from_keys(Followers, new(LeaderLastIndex)).

% not exist in otp 20
from_keys(Keys, Value) ->
    maps:from_list([{Key, Value} || Key <- Keys]).

update_index(FollowerInfo, FollowerLastIndex, LastLogIndex) ->
    {NewNext, NewMatch} =
    case FollowerLastIndex of
        not_change ->
            NextIndex = FollowerInfo#follower_info.next_index,
            {NextIndex, NextIndex - 1};
        FollowerLastIndex when FollowerLastIndex =< LastLogIndex ->
            {FollowerLastIndex + 1, FollowerLastIndex};
        % old leader may have more logs
        _ ->
            {LastLogIndex + 1, LastLogIndex}
    end,
    FollowerInfo#follower_info{next_index=NewNext, match_index=NewMatch}.

majority_index(FollowersInfo) ->
    lists:nth(maps:size(FollowersInfo) div 2 + 1,
              lists:sort([MatchIndex || #follower_info{match_index=MatchIndex} <- maps:values(FollowersInfo)])).

next_index(#follower_info{next_index=NextIndex}) ->
    NextIndex.

is_up_to_date(#follower_info{match_index=MatchIndex}, LastLogIndex) ->
    MatchIndex < LastLogIndex.

replicate_not_responced(#follower_info{last_msg_type=MsgType, href=HRef})  ->
    MsgType =:= normal andalso is_reference(HRef).

cancel_timer(#follower_info{href=HRef}) when is_reference(HRef) ->
    erlang:cancel_timer(HRef);
cancel_timer(_) ->
    ok.

update_replicate_info(FollowerInfo, Follower, LogEntries) ->
    cancel_timer(FollowerInfo),
    MsgType = case LogEntries of [] -> heartbeat; _ -> normal end,
    % todo calc timeout by msg length
    NewHRef = erlang:start_timer(?HEARTBEAT_TIME_INTERVAL, self(), {heartbeat_timeout, Follower}),
    FollowerInfo#follower_info{last_msg_type=MsgType, href=NewHRef}.

timeout(FollowerInfo) ->
    FollowerInfo#follower_info{href=null}.

reponced(FollowerInfo) ->
    FollowerInfo#follower_info{last_msg_type=null}.

is_cur_href(#follower_info{href=HRef}, MsgHRef) ->
    HRef =:= MsgHRef.

decrement_next_index(FollowerInfo=#follower_info{next_index=NextIndex}) when NextIndex > 0 ->
    FollowerInfo#follower_info{next_index=NextIndex - 1};
decrement_next_index(FollowerInfo) ->
    FollowerInfo.