% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

% Algorithm implemented here is based on the "A Fair Share Scheduler" by Judy Kay
% and Piers Lauder [1].
%
% [1] https://proteusmaster.urcf.drexel.edu/urcfwiki/images/KayLauderFairShare.pdf
%

-module(couch_replicator_share).

-export([
    init/0,
    clear/0,

    update_shares/2,

    job_added/1,
    job_removed/1,

    get_priority/1,
    charge/3,

    decay_priorities/0,
    update_priority/1,
    update_usage/0,

    dump_state/0
]).


-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").


-define(DEFAULT_USAGE_COEFF, 0.2).
-define(DEFAULT_PRIORITY_COEFF, 0.8).
-define(MIN_SHARES, 1).
-define(MAX_SHARES, 1000).
-define(DEFAULT_SHARES, 100).

-define(SHARES, couch_replicator_shares).
-define(PRIORITIES, couch_replicator_priorities).
-define(USAGE, couch_replicator_usage).
-define(CHARGES, couch_replicator_stopped_usage).
-define(NUM_JOBS, couch_replicator_num_jobs).


init() ->
    EtsOpts = [named_table, public],
    ?SHARES = ets:new(?SHARES, EtsOpts),          % {Key, Shares}
    ?PRIORITIES = ets:new(?PRIORITIES, EtsOpts),  % {JobId, Priority}
    ?USAGE = ets:new(?USAGE, EtsOpts),            % {Key, Usage}
    ?CHARGES = ets:new(?CHARGES, EtsOpts),        % {Key, Charges}
    ?NUM_JOBS = ets:new(?NUM_JOBS, EtsOpts),      % {Key, NumJobs}
    lists:foreach(fun({K, V}) ->
        update_shares(list_to_binary(K), list_to_integer(V))
    end, config:get("replicator.shares")).


clear() ->
    Tables = [?SHARES, ?PRIORITIES, ?USAGE, ?CHARGES, ?NUM_JOBS],
    lists:foreach(fun(T) -> ets:delete(T) end, Tables).


% This should be called when user updates the replicator.shares config section
%
update_shares(Key, Shares) when is_integer(Shares) ->
    ets:insert(?SHARES, {Key, min(?MAX_SHARES, max(?MIN_SHARES, Shares))}).


job_added(#job{} = Job) ->
    Key = key(Job),
    ets:update_counter(?NUM_JOBS, Key, 1, {Key, 0}),
    ets:insert(?PRIORITIES, {Job#job.id, 0}).


job_removed(#job{} = Job) ->
    Key = key(Job),
    ets:delete(?PRIORITIES, Job#job.id),
    case ets:update_counter(?NUM_JOBS, Key, -1, {Key, 0}) of
        N when is_integer(N), N =< 0 ->
            true = ets:delete(?NUM_JOBS, Key);
        N when is_integer(N), N > 0 ->
            ok
    end.


get_priority(JobId) ->
    % Not found means it was removed because it's value was 0
    case ets:lookup(?PRIORITIES, JobId) of
        [{_, Priority}] -> Priority;
        [] -> 0
    end.


charge(#job{pid = undefined}, _, _) ->
    0;

charge(#job{} = Job, Interval, {_, _, _} = Now) when is_integer(Interval) ->
    Key = key(Job),
    Charges = job_charges(Job, Interval, Now),
    ets:update_counter(?CHARGES, Key, Charges, {Key, 0}).


% In [1] this described in the "Decay of Process Priorities" section
%
decay_priorities() ->
    decay(?PRIORITIES, priority_coeff()),
    % If priority becomes 0, it's removed. When looking it up, if it
    % is missing we assume it is 0
    clear_zero(?PRIORITIES).


% This is the main part of the alrgorithm. In [1] it is described in the
% "Priority Adjustment" section.
%
update_priority(#job{} = Job) ->
    Id = Job#job.id,
    Key = key(Job),
    Shares = shares(Key),
    Priority = (usage(Key) * num_jobs(Key)) / (Shares * Shares),
    ets:update_counter(?PRIORITIES, Id, round(Priority), {Id, 0}).


% This is the "User-Level Scheduling" part from [1]
%
update_usage() ->
    decay(?USAGE, usage_coeff()),
    clear_zero(?USAGE),
    ets:foldl(fun({Key, Charges}, Cnt) ->
        Cnt + ets:update_counter(?USAGE, Key, Charges, {Key, 0})
    end, 0, ?CHARGES),
    % Start each interval with a fresh charges table
    ets:delete_all_objects(?CHARGES).


% Debugging functions
%
dump_state() ->
    Tables = [?SHARES, ?PRIORITIES, ?USAGE, ?CHARGES, ?NUM_JOBS],
    [{T, lists:sort(ets:tab2list(T))} || T <- Tables].


% Private helper functions

usage(Key) ->
    case ets:lookup(?USAGE, Key) of
        [{_, Usage}] -> Usage;
        [] -> 0
    end.


num_jobs(Key) ->
    case ets:lookup(?NUM_JOBS, Key) of
        [{_, NumJobs}] -> NumJobs;
        [] -> 0
    end.


shares(Key) ->
    case ets:lookup(?SHARES, Key) of
        [{_, Shares}] -> Shares;
        [] -> ?DEFAULT_SHARES
    end.


decay(Ets, Coeff) when is_atom(Ets) ->
    Head = {'$1', '$2'},
    Result = {{'$1', {round, {'*', '$2', {const, Coeff}}}}},
    ets:select_replace(Ets, [{Head, [], [Result]}]).


clear_zero(Ets) when is_atom(Ets) ->
    ets:select_delete(Ets, [{{'_', 0}, [], [true]}]).


key(#job{} = Job) ->
    Rep = Job#job.rep,
    case is_binary(Rep#rep.db_name) of
        true -> Rep#rep.db_name;
        false -> (Rep#rep.user_ctx)#user_ctx.name
    end.


% Jobs are charged based on the amount of time the job was running during the
% last scheduling interval
%
job_charges(#job{} = Job, Interval, {_, _, _} = Now) ->
    TimeRunning = timer:now_diff(Now, last_started(Job)) div 1000,
    min(Interval, max(0, TimeRunning)).


last_started(#job{} = Job) ->
    case lists:keyfind(started, 1, Job#job.history) of
        false -> {0, 0, 0};  % In case user set too low of a max history
        {started, When} -> When
    end.


% Config helper functions

priority_coeff() ->
    % This is the K2 coefficient from [1]
    Default = ?DEFAULT_PRIORITY_COEFF,
    Val = float_val(config:get("replicator", "priority_coeff"), Default),
    max(0.0, min(1.0, Val)).


usage_coeff() ->
    % This is the K1 coefficient from [1]
    Default = ?DEFAULT_USAGE_COEFF,
    Val = float_val(config:get("replicator", "usage_coeff"), Default),
    max(0.0, min(1.0, Val)).


float_val(undefined, Default) ->
    Default;

float_val(Str, Default) when is_list(Str) ->
    try list_to_integer(Str) of
        Int -> float(Int)
    catch error:badarg ->
        try list_to_float(Str) of
            Val -> Val
        catch error:badarg ->
            Default
        end
    end.
