%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Helium Systems, Inc.  All Rights Reserved.
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

-module(plumtree_peer_service_manager).
-include_lib("kernel/include/logger.hrl").
-define(TBL, cluster_state).

-export([init/0, get_local_state/0, get_actor/0, update_state/1, delete_state/0]).

init() ->
    %% setup ETS table for cluster_state
    _ = try ets:new(?TBL, [named_table, public, set, {keypos, 1}]) of
            _Res ->
                gen_actor(),
                maybe_load_state_from_disk(),
                ok
        catch
            error:badarg ->
                ?LOG_WARNING("Table ~p already exists", [?TBL])
                %%TODO rejoin logic
        end,
    ok.

%% @doc return local node's view of cluster membership
get_local_state() ->
   case hd(ets:lookup(?TBL, cluster_state)) of
       {cluster_state, State} ->
           {ok, State};
       _Else ->
           {error, _Else}
   end.

%% @doc return local node's current actor
get_actor() ->
    case hd(ets:lookup(?TBL, actor)) of
        {actor, Actor} ->
            {ok, Actor};
        _Else ->
            {error, _Else}
    end.

%% @doc update cluster_state
update_state(State) ->
    write_state_to_disk(State),
    ets:insert(?TBL, {cluster_state, State}).

delete_state() ->
    delete_state_from_disk().

%%% ------------------------------------------------------------------
%%% internal functions
%%% ------------------------------------------------------------------

%% @doc initialize singleton cluster
add_self() ->
    Initial = riak_dt_orswot:new(),
    Actor = ets:lookup(?TBL, actor),
    {ok, LocalState} = riak_dt_orswot:update({add, node()}, Actor, Initial),
    update_state(LocalState).

%% @doc generate an actor for this node while alive
gen_actor() ->
    Node = atom_to_list(node()),
    {M, S, U} = erlang:timestamp(),
    TS = integer_to_list(M * 1000 * 1000 * 1000 * 1000 + S * 1000 * 1000 + U),
    Term = Node ++ TS,
    Actor = crypto:hash(sha, Term),
    ets:insert(?TBL, {actor, Actor}).

data_root() ->
    case application:get_env(plumtree, plumtree_data_dir) of
        {ok, PRoot} -> filename:join(PRoot, "peer_service");
        undefined -> undefined
    end.

write_state_to_disk(State) ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            ?LOG_INFO("writing state ~p to disk ~p",
                       [State, riak_dt_orswot:to_binary(State)]),
            ok = file:write_file(File,
                                 riak_dt_orswot:to_binary(State))
    end.

delete_state_from_disk() ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            case file:delete(File) of
                ok ->
                    ?LOG_INFO("Leaving cluster, removed cluster_state");
                {error, Reason} ->
                    ?LOG_INFO("Unable to remove cluster_state for reason ~p", [Reason])
            end
    end.

maybe_load_state_from_disk() ->
    case data_root() of
        undefined ->
            add_self();
        Dir ->
            case filelib:is_regular(filename:join(Dir, "cluster_state")) of
                true ->
                    {ok, Bin} = file:read_file(filename:join(Dir,
                                                             "cluster_state")),
                    {ok, State} = riak_dt_orswot:from_binary(Bin),
                    ?LOG_INFO("read state from file ~p~n", [State]),
                    update_state(State);
                false ->
                    add_self()
            end
    end.
