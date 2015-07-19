%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(plumtree_metadata_manager).

-behaviour(gen_server).
-behaviour(plumtree_broadcast_handler).

%% API
-export([start_link/0,
         get/1,
         get/2,
         iterator/0,
         iterator/1,
         iterator/2,
         remote_iterator/1,
         remote_iterator/2,
         iterate/1,
         iterator_prefix/1,
         iterator_value/1,
         iterator_done/1,
         iterator_close/1,
         put/3,
         merge/3]).

%% plumtree_broadcast_handler callbacks
-export([broadcast_data/1,
         merge/2,
         is_stale/1,
         graft/1,
         exchange/1]).

%% used by cleanup
-export([force_delete/1]).

%% utilities
-export([size/1,
         subscribe/1,
         unsubscribe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([metadata_iterator/0]).

-include("plumtree_metadata.hrl").

-define(SERVER, ?MODULE).
-define(SUBS, plumtree_metadata_subscriptions).

-record(state, {}).

-record(metadata_iterator, {
          prefix :: metadata_prefix(),
          match  :: term(),
          pos    :: term(),
          obj    :: {metadata_key(), metadata_object()} | undefined,
          done   :: boolean()
         }).

-opaque metadata_iterator() :: #metadata_iterator{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Reads the value for a prefixed key. If the value does not exist `undefined' is
%% returned. otherwise a Dotted Version Vector Set is returned. When reading the value
%% for a subsequent call to put/3 the context can be obtained using
%% plumtree_metadata_object:context/1. Values can obtained w/ plumtree_metadata_object:values/1.
-spec get(metadata_pkey()) -> metadata_object() | undefined.
get({{Prefix, SubPrefix}, _Key}=PKey) when (is_binary(Prefix) orelse is_atom(Prefix)) andalso
                                           (is_binary(SubPrefix) orelse is_atom(SubPrefix)) ->
    read(PKey).

%% @doc Same as get/1 but reads the value from `Node'
-spec get(node(), metadata_pkey()) -> metadata_object() | undefined.
get(Node, PKey) when node() =:= Node ->
    ?MODULE:get(PKey);
get(Node, {{Prefix, SubPrefix}, _Key}=PKey)
  when (is_binary(Prefix) orelse is_atom(Prefix)) andalso
       (is_binary(SubPrefix) orelse is_atom(SubPrefix)) ->
    gen_server:call({?SERVER, Node}, {get, PKey}, infinity).

-spec size(metadata_prefix()) -> integer().
size(FullPrefix) ->
    It = new_iterator(FullPrefix, undefined, [keys_only]),
    calc_size(It, 0).

calc_size(It, Acc) ->
    case iterator_done(It) of
        true ->
            iterator_close(It),
            Acc;
        false ->
            calc_size(iterate(It), Acc + 1)
    end.

-spec subscribe(metadata_prefix()) -> ok.
subscribe(FullPrefix) ->
    gen_server:call(?SERVER, {subscribe, FullPrefix, self()}, infinity).

-spec unsubscribe(metadata_prefix()) -> ok.
unsubscribe(FullPrefix) ->
    gen_server:call(?SERVER, {unsubscribe, FullPrefix, self()}, infinity).



%% @doc Returns a full-prefix iterator: an iterator for all full-prefixes that have keys stored under them
%% When done with the iterator, iterator_close/1 must be called
-spec iterator() -> metadata_iterator().
iterator() ->
    iterator(undefined).

%% @doc Returns a sub-prefix iterator for a given prefix.
%% When done with the iterator, iterator_close/1 must be called
-spec iterator(binary() | atom()) -> metadata_iterator().
iterator(Prefix) when is_binary(Prefix) or is_atom(Prefix) ->
    open_iterator(undefined, Prefix).

%% @doc Return an iterator for keys stored under a prefix. If KeyMatch is undefined then
%% all keys will may be visted by the iterator. Otherwise only keys matching KeyMatch will be
%% visited.
%%
%% KeyMatch can be either:
%%   * an erlang term - which will be matched exactly against a key
%%   * '_' - which is equivalent to undefined
%%   * an erlang tuple containing terms and '_' - if tuples are used as keys
%%   * this can be used to iterate over some subset of keys
%%
%% When done with the iterator, iterator_close/1 must be called
-spec iterator(metadata_prefix() , term()) -> metadata_iterator().
iterator({Prefix, SubPrefix}=FullPrefix, KeyMatch)
  when (is_binary(Prefix) orelse is_atom(Prefix)) andalso
       (is_binary(SubPrefix) orelse is_atom(SubPrefix)) ->
    open_iterator(FullPrefix, KeyMatch).

%% @doc Create an iterator on `Node'. This allows for remote iteration by having
%% he iterator created iterates all full-prefixes. Once create the rest of the
%% iterator API may be used as usual. When done with the iterator, iterator_close/1
%% must be called
-spec remote_iterator(node()) -> metadata_iterator().
remote_iterator(Node) ->
    remote_iterator(Node, undefined).

%% @doc Create an iterator on `Node'. When `Perfix' is not a full prefix,
%% the iterator created iterates all sub-prefixes under `Prefix'. Otherse,
%% the iterator iterates all keys under a prefix. Once created the rest of the
%% iterator API may be used as usual. When done with the iterator,
%% iterator_close/1 must be called
-spec remote_iterator(node(), metadata_prefix() | binary() | atom() | undefined) -> metadata_iterator().
remote_iterator(Node, Prefix) when is_atom(Prefix) or is_binary(Prefix) ->
    gen_server:call({?SERVER, Node}, {open_remote_iterator, self(), undefined, Prefix}, infinity);
remote_iterator(Node, FullPrefix) when is_tuple(FullPrefix) ->
    gen_server:call({?SERVER, Node}, {open_remote_iterator, self(), FullPrefix, undefined}, infinity).

%% @doc advance the iterator by one key, full-prefix or sub-prefix
-spec iterate(metadata_iterator()) -> metadata_iterator().
iterate(Iterator) ->
    next_iterator(Iterator).

%% @doc return the full-prefix or prefix being iterated by this iterator. If the iterator is a
%% full-prefix iterator undefined is returned.
-spec iterator_prefix(metadata_iterator()) ->
    metadata_prefix() | undefined | binary() | atom().
iterator_prefix(#metadata_iterator{prefix=undefined,match=undefined}) -> undefined;
iterator_prefix(#metadata_iterator{prefix=undefined,match=Prefix}) -> Prefix;
iterator_prefix(#metadata_iterator{prefix=Prefix}) -> Prefix.

%% @doc return the key and object or the prefix pointed to by the iterator
-spec iterator_value(metadata_iterator()) ->
    {metadata_key(), metadata_object()} | metadata_prefix() | binary() | atom().
iterator_value(#metadata_iterator{prefix=undefined,match=undefined,pos=Pos}) -> Pos;
iterator_value(#metadata_iterator{obj=Obj}) -> Obj.

%% @doc returns true if there are no more keys or prefixes to iterate over
-spec iterator_done(metadata_iterator()) -> boolean().
iterator_done(#metadata_iterator{done=Done}) -> Done.

%% @doc Closes the iterator. This function must be called on all open iterators
-spec iterator_close(metadata_iterator()) -> ok.
iterator_close(#metadata_iterator{prefix=undefined,match=undefined}) ->
    ok;
iterator_close(It) -> finish_iterator(It).

%% @doc Sets the value of a prefixed key. The most recently read context (see get/1)
%% should be passed as the second argument to prevent unneccessary siblings.
-spec put(metadata_pkey(),
          metadata_context() | undefined,
          metadata_value() | metadata_modifier()) -> metadata_object().
put(PKey, undefined, ValueOrFun) ->
    %% nil is an empty version vector for dvvset
    put(PKey, [], ValueOrFun);
put({{Prefix, SubPrefix}, _Key}=PKey, Context, ValueOrFun)
  when (is_binary(Prefix) orelse is_atom(Prefix)) andalso
       (is_binary(SubPrefix) orelse is_atom(SubPrefix)) ->
    read_modify_write(PKey, Context, ValueOrFun).

%% @doc forcefully deletes the key
-spec force_delete(metadata_pkey()) -> ok.
force_delete({{Prefix, SubPrefix}, _Key}=PKey)
  when (is_binary(Prefix) orelse is_atom(Prefix)) andalso
       (is_binary(SubPrefix) orelse is_atom(SubPrefix)) ->
    gen_server:call(?SERVER, {force_delete, PKey}, infinity).

%% @doc same as merge/2 but merges the object on `Node'
-spec merge(node(), {metadata_pkey(), undefined | metadata_context()}, metadata_object()) -> boolean().
merge(Node, {PKey, _Context}, Obj) ->
    gen_server:call({?SERVER, Node}, {merge, PKey, Obj}, infinity).

%%%===================================================================
%%% plumtree_broadcast_handler callbacks
%%%===================================================================

%% @doc Deconstructs are broadcast that is sent using `plumtree_metadata_manager' as the
%% handling module returning the message id and payload.
-spec broadcast_data(metadata_broadcast()) -> {{metadata_pkey(), metadata_context()},
                                               metadata_object()}.
broadcast_data(#metadata_broadcast{pkey=Key, obj=Obj}) ->
    Context = plumtree_metadata_object:context(Obj),
    {{Key, Context}, Obj}.

%% @doc Merges a remote copy of a metadata record sent via broadcast w/ the local view
%% for the key contained in the message id. If the remote copy is causally older than
%% the current data stored then `false' is returned and no updates are merged. Otherwise,
%% the remote copy is merged (possibly generating siblings) and `true' is returned.
-spec merge({metadata_pkey(), undefined | metadata_context()}, undefined | metadata_object()) -> boolean().
merge({PKey, _Context}, Obj) ->
    gen_server:call(?SERVER, {merge, PKey, Obj}, infinity).

%% @doc Returns false if the update (or a causally newer update) has already been
%% received (stored locally).
-spec is_stale({metadata_pkey(), metadata_context()}) -> boolean().
is_stale({PKey, Context}) ->
    gen_server:call(?SERVER, {is_stale, PKey, Context}, infinity).

%% @doc returns the object associated with the given key and context (message id) if
%% the currently stored version has an equal context. otherwise stale is returned.
%% because it assumed that a grafted context can only be causally older than the local view
%% a stale response means there is another message that subsumes the grafted one
-spec graft({metadata_pkey(), metadata_context()}) ->
                   stale | {ok, metadata_object()} | {error, term()}.
graft({PKey, Context}) ->
    case ?MODULE:get(PKey) of
        undefined ->
            %% There would have to be a serious error in implementation to hit this case.
            %% Catch if here b/c it would be much harder to detect
            lager:error("object not found during graft for key: ~p", [PKey]),
            {error, {not_found, PKey}};
         Obj ->
            graft(Context, Obj)
    end.

graft(Context, Obj) ->
    case plumtree_metadata_object:equal_context(Context, Obj) of
        false ->
            %% when grafting the context will never be causally newer
            %% than what we have locally. Since its not equal, it must be
            %% an ancestor. Thus we've sent another, newer update that contains
            %% this context's information in addition to its own.  This graft
            %% is deemed stale
            stale;
        true ->
            {ok, Obj}
    end.

%% @doc Trigger an exchange
-spec exchange(node()) -> {ok, pid()} | {error, term()}.
exchange(Peer) ->
    Timeout = app_helper:get_env(plumtree, metadata_exchange_timeout, 60000),
    case plumtree_metadata_exchange_fsm:start(Peer, Timeout) of
        {ok, Pid} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason};
        ignore ->
            {error, ignore}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}} |
                  {ok, #state{}, non_neg_integer() | infinity} |
                  ignore |
                  {stop, term()}.
init([]) ->
    new_subscription_table(),
    {ok, #state{}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({merge, PKey, Obj}, _From, State) ->
    Result = read_merge_write(PKey, Obj),
    {reply, Result, State};
handle_call({get, PKey}, _From, State) ->
    Result = read(PKey),
    {reply, Result, State};
handle_call({force_delete, PKey}, _From, State) ->
    {Result, NewState} = force_delete(PKey, State),
    {reply, Result, NewState};
handle_call({open_remote_iterator, Pid, FullPrefix, KeyMatch}, _From, State) ->
    Iterator = new_remote_iterator(Pid, FullPrefix, KeyMatch),
    {reply, Iterator, State};
handle_call({iterate, RemoteRef}, _From, State) ->
    Next = next_iterator(RemoteRef, State),
    {reply, Next, State};
handle_call({is_stale, PKey, Context}, _From, State) ->
    Existing = read(PKey),
    IsStale = plumtree_metadata_object:is_stale(Context, Existing),
    {reply, IsStale, State};
handle_call({subscribe, FullPrefix, Pid}, _From, State) ->
    Ref = monitor(process, Pid),
    ets:insert(?SUBS, {FullPrefix, {Pid, Ref}}),
    {reply, ok, State};
handle_call({unsubscribe, FullPrefix, Pid}, _From, State) ->
    Subs = ets:lookup(?SUBS, FullPrefix),
    lists:foreach(fun({_, {SubPid, MRef}} = Obj) when SubPid == Pid ->
                          demonitor(MRef, [flush]),
                          ets:delete_object(?SUBS, Obj);
                     (_) -> ignore
                  end, Subs),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    ets:match_delete(?SUBS, {'_', {Pid, Ref}}),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_remote_iterator(Pid, FullPrefix, KeyMatch) ->
    ObjectMatch = iterator_match(KeyMatch),
    Ret = plumtree_metadata_leveldb_iterator:new(FullPrefix, ObjectMatch, Pid),
    new_iterator(Ret).

open_iterator(FullPrefix, KeyMatch) ->
    new_iterator(FullPrefix, KeyMatch).

next_iterator(It=#metadata_iterator{done=true}) ->
    %% general catch-all for all iterators
    It;
next_iterator(It=#metadata_iterator{pos=Pos}) ->
    %% key/value iterator
    next_iterator(It, plumtree_metadata_leveldb_iterator:next(Pos)).

next_iterator(It, '$end_of_table') ->
    %% general catch-all for all iterators
    It#metadata_iterator{done=true,
                         pos=undefined,
                         obj=undefined};
next_iterator(It, {{{_FullPrefix, Key}, Val}, Cont}) ->
    %% sub-prefix or key/value iterator
    It#metadata_iterator{pos=Cont,
                         obj={Key, Val}};
next_iterator(It, {{_FullPrefix, Key}, Cont}) ->
    %% sub-prefix or key/value iterator
    It#metadata_iterator{pos=Cont,
                         obj={Key, undefined}}.

%% universal empty iterator
empty_iterator() ->
    #metadata_iterator{
                pos=undefined,
                obj=undefined,
                done=true
               }.

new_iterator(FullPrefix, KeyMatch) ->
    new_iterator(FullPrefix, KeyMatch, []).
new_iterator(FullPrefix, KeyMatch, Opts) ->
    %% key/value iterator
    ObjectMatch = iterator_match(KeyMatch),
    Ret = plumtree_metadata_leveldb_iterator:new(FullPrefix, ObjectMatch, Opts),
    new_iterator(Ret).

new_iterator('$end_of_table') ->
    %% catch-all for empty iterator of all types
    empty_iterator();
new_iterator({{{FullPrefix, Key}, Val}, Cont}) ->
    %% key/value iterator
    #metadata_iterator{
       prefix=FullPrefix,
       pos=Cont,
       obj={Key, Val},
       done=false
      };
new_iterator({{FullPrefix, Key}, Cont}) ->
    %% key/value iterator
    #metadata_iterator{
       prefix=FullPrefix,
       pos=Cont,
       obj={Key, undefined},
       done=false
      }.

finish_iterator(#metadata_iterator{done=true}) ->
    ok;
finish_iterator(It) ->
    Next = next_iterator(It),
    finish_iterator(Next).

iterator_match(undefined) -> undefined;
iterator_match(KeyMatch) when is_function(KeyMatch) -> KeyMatch.

read_modify_write(PKey, Context, ValueOrFun) ->
    Existing = read(PKey),
    Modified = plumtree_metadata_object:modify(Existing, Context, ValueOrFun, node()),
    store(PKey, Modified).

read_merge_write(PKey, Obj) ->
    Existing = read(PKey),
<<<<<<< HEAD
    case plumtree_metadata_object:reconcile(Obj, Existing) of
        false -> false;
        {true, Reconciled} ->
            store(PKey, Reconciled),
            true
    end.

store({FullPrefix, Key}=PKey, Metadata) ->
    Hash = plumtree_metadata_object:hash(Metadata),
    OldObj =
    case read(PKey) of
        undefined ->
            undefined;
        OldMeta ->
            [Val|_] = plumtree_metadata_object:values(OldMeta),
            Val
    end,
    Event =
    case plumtree_metadata_object:values(Metadata) of
        ['$deleted'|_] ->
            {deleted, FullPrefix, Key, OldObj};
        [NewObj|_] ->
            {updated, FullPrefix, Key, OldObj, NewObj}
    end,
    plumtree_metadata_hashtree:insert(PKey, Hash),
    plumtree_metadata_leveldb_instance:put(PKey, Metadata),
    trigger_subscription_event(FullPrefix, Event,
                              ets:lookup(?SUBS, FullPrefix)),
    Metadata.

force_delete({FullPrefix, Key}=PKey,
             #state{storage_mod=Mod,
                    storage_mod_state=ModSt} = State) ->
    Tab = ets_tab(FullPrefix),
    ets:delete(Tab, Key),
    {ok, NewModSt} = Mod:delete(FullPrefix, Key, ModSt),
    ok = plumtree_metadata_hashtree:delete(PKey),
    {ok, State#state{storage_mod_state=NewModSt}}.

trigger_subscription_event(FullPrefix, Event, [{FullPrefix, {Pid, _}}|Rest]) ->
    Pid ! Event,
    trigger_subscription_event(FullPrefix, Event, Rest);
trigger_subscription_event(FullPrefix, Event, [_|Rest]) ->
    trigger_subscription_event(FullPrefix, Event, Rest);
trigger_subscription_event(_, _, []) -> ok.

read(PKey) ->
    case plumtree_metadata_leveldb_instance:get(PKey) of
        {ok, Val} -> Val;
        {error, not_found} ->
           undefined;
        {error, Reason} ->
            lager:warning("can't lookup key ~p due to ~p", [PKey, Reason]),
            undefined
    end.

new_subscription_table() ->
    ets:new(?SUBS,[bag, public, named_table,
             {read_concurrency, true}, {write_concurrency, true}]).
