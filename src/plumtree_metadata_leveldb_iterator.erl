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

-module(plumtree_metadata_leveldb_iterator).

-behaviour(gen_server).

%% API functions
-export([new/2,
         new/3,
         new/4,
         start_link/1,
         next/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {instances=[],
                full_prefix,
                key_match,
                monitor,
                keys_only=false}).

%%%===================================================================
%%% API functions
%%%===================================================================

new(FullPrefix, KeyMatch) ->
    new(FullPrefix, KeyMatch, self(), []).
new(FullPrefix, KeyMatch, Owner) when is_pid(Owner) ->
    new(FullPrefix, KeyMatch, Owner, []);
new(FullPrefix, KeyMatch, Opts) when is_list(Opts) ->
    new(FullPrefix, KeyMatch, self(), Opts).
new(FullPrefix, KeyMatch, Owner, Opts) ->
    case supervisor:start_child(plumtree_metadata_leveldb_iterator_sup,
                                [[FullPrefix, KeyMatch, Owner, Opts]]) of
        {ok, Pid} ->
            next(Pid);
        {error, Reason} ->
            exit(Reason)
    end.

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

next(Pid) ->
    gen_server:call(Pid, iterate, infinity).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([FullPrefix, KeyMatch, Owner, Opts]) ->
    Instances = supervisor:which_children(plumtree_metadata_leveldb_instance_sup),
    InstanceIds = [Id || {Id, Pid, worker, _} <- Instances, is_pid(Pid)],
    case length(Instances) == length(InstanceIds) of
        true ->
            MRef = monitor(process, Owner),
            {ok, #state{instances=InstanceIds,
                        full_prefix=FullPrefix,
                        key_match=KeyMatch,
                        monitor=MRef,
                        keys_only=lists:member(keys_only, Opts)}};
        false ->
            {stop, not_all_instances_running}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(iterate, _From, #state{instances=Instances,
                                   full_prefix=FullPrefix,
                                   key_match=KeyMatch,
                                   keys_only=KeysOnly} = State) ->
    case iterate(Instances, FullPrefix, KeyMatch, KeysOnly) of
        {Val, NewInstances} ->
            {reply, Val, State#state{instances=NewInstances}};
        done ->
            {stop, normal, '$end_of_table', State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN', MRef, process, _, _}, #state{monitor=MRef} = State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
iterate([{Itr, _Instance}|_] = Instances, FullPrefix, KeyMatch, KeysOnly) ->
    Res = eleveldb:iterator_move(Itr, prefetch),
    iterate(Res, Instances, FullPrefix, KeyMatch, KeysOnly);
iterate([Instance|Rest], FullPrefix, KeyMatch, KeysOnly) when is_atom(Instance)->
    Itr = plumtree_metadata_leveldb_instance:iterator(Instance, KeysOnly),
    FirstKey = first_key(FullPrefix),
    Res = eleveldb:iterator_move(Itr, FirstKey),
    iterate(Res, [{Itr, Instance}|Rest], FullPrefix, KeyMatch, KeysOnly);
iterate([], _, _, _) -> done.


iterate({error, _}, [_|Rest], FullPrefix, KeyMatch, KeysOnly) ->
    iterate(Rest, FullPrefix, KeyMatch, KeysOnly);
iterate(OkVal, Instances, FullPrefix, KeyMatch, KeysOnly) when element(1, OkVal) == ok ->
    %% OkVal has the form of
    %% {ok, Key} or {ok, Key, Val}
    BKey = element(2, OkVal),
    PrefixedKey = sext:decode(BKey),
    case prefix_match(PrefixedKey, FullPrefix) of
        {true, Key} ->
            case key_match(Key, KeyMatch) of
                true when KeysOnly ->
                    {{PrefixedKey, self()}, Instances};
                true ->
                    BVal = element(3, OkVal),
                    {{{PrefixedKey, binary_to_term(BVal)}, self()}, Instances};
                false ->
                    iterate(Instances, FullPrefix, KeyMatch, KeysOnly)
            end;
        false ->
            [{Itr, Instance}|RestInstances] = Instances,
            ok = plumtree_metadata_leveldb_instance:iterator_close(Instance, Itr),
            iterate(RestInstances, FullPrefix, KeyMatch, KeysOnly)
    end.

prefix_match({_, Key}, undefined) -> {true, Key};
prefix_match({{Prefix, _}, Key}, {Prefix, undefined}) -> {true, Key};
prefix_match({{_, SubPrefix}, Key}, {undefined, SubPrefix}) -> {true, Key};
prefix_match({FullPrefix, Key}, {_,_} = FullPrefix) -> {true, Key};
prefix_match(_, _) -> false.

key_match(_, undefined) -> true;
key_match(Key, KeyMatch) -> KeyMatch(Key).

first_key(undefined) -> first;
first_key({undefined, undefined}) -> first;
first_key({Prefix, undefined}) -> sext:encode({{Prefix, ''}, ''});
first_key({_, _}=FullPrefix) -> sext:encode({FullPrefix, ''}).
