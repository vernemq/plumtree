%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Erlio GmbH.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(plumtree_metadata_leveldb_instance_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/0,
         get_instance_id_for_key/1]).

%% Supervisor callbacks
-export([init/1]).

-define(NR_OF_LEVELDBs, 8).
-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

get_instance_id_for_key(Key) ->
    case lists:keyfind(workers, 1, supervisor:count_children(?MODULE)) of
        false ->
            exit({?MODULE, no_active_instance});
        {_, NrOfWorkers} when NrOfWorkers > 0 ->
            erlang:phash2(Key) rem NrOfWorkers
    end.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init(_) ->
    NrOfInstances = application:get_env(plumtree, nr_of_meta_instances,
                                        ?NR_OF_LEVELDBs),
    LeveldbOpts = application:get_env(plumtree, meta_leveldb_opts, []),
    Children =
    [?CHILD(plumtree_metadata_leveldb_instance:name(Id),
            plumtree_metadata_leveldb_instance,
            worker,
            [Id, LeveldbOpts])
     || Id <- lists:seq(0, NrOfInstances -1)],
    {ok, {{one_for_one, 5, 1}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
