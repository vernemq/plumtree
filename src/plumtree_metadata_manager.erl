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
         start_link/1,
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

%% used by storage backends
-export([init_ets_for_full_prefix/1]).

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
-define(ETS, metadata_manager_prefixes_ets).
-define(DEFAULT_STORAGE_BACKEND, plumtree_dets_metadata_manager).

-record(state, {
          %% identifier used in logical clocks
          serverid              :: term(),

          storage_mod           :: atom(),
          storage_mod_state     :: any(),

          %% an ets table to hold iterators opened
          %% by other nodes
          iterators             :: ets:tab(),

          subscriptions         :: list({metadata_prefix(), {pid(), reference()}})
         }).

-record(metadata_iterator, {
          prefix :: metadata_prefix(),
          match  :: term(),
          pos    :: term(),
          obj    :: {metadata_key(), metadata_object()} | undefined,
          done   :: boolean(),
          tab    :: ets:tab()
         }).

-record(remote_iterator, {
          node   :: node(),
          ref    :: reference(),
          prefix :: metadata_prefix() | atom() | binary()
         }).

-opaque metadata_iterator() :: #metadata_iterator{}.
-type remote_iterator()     :: #remote_iterator{}.

-type mm_path_opt()     :: {data_dir, file:name_all()}.
-type mm_nodename_opt() :: {nodename, term()}.
-type mm_opt()          :: mm_path_opt() | mm_nodename_opt().
-type mm_opts()         :: [mm_opt()].

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start plumtree_metadadata_manager and link to calling process.
%%
%% The following options can be provided:
%%    * data_dir: the root directory to place cluster metadata files.
%%                if not provided this defaults to the `cluster_meta' directory in
%%                plumtree's `platform_data_dir'.
%%    * nodename: the node identifier (for use in logical clocks). defaults to node()
-spec start_link(mm_opts()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Opts], []).

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


-spec init_ets_for_full_prefix(metadata_pkey()) -> ets:tid().
init_ets_for_full_prefix(FullPrefix) ->
    init_ets(FullPrefix).

-spec size(metadata_pkey()) -> non_neg_integer().
size(FullPrefix) ->
    case ets_tab(FullPrefix) of
        undefined -> 0;
        TabId -> ets:info(TabId, size)
    end.

-spec subscribe(metadata_pkey()) -> ok.
subscribe(FullPrefix) ->
    gen_server:call(?SERVER, {subscribe, FullPrefix, self()}, infinity).

-spec unsubscribe(metadata_pkey()) -> ok.
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
%% the metadata manager keep track of the actual iterator (since ets continuations cannot
%% cross node boundaries). The iterator created iterates all full-prefixes. Once created
%% the rest of the iterator API may be used as usual. When done with the iterator,
%% iterator_close/1 must be called
-spec remote_iterator(node()) -> remote_iterator().
remote_iterator(Node) ->
    remote_iterator(Node, undefined).

%% @doc Create an iterator on `Node'. This allows for remote iteration
%% by having the metadata manager keep track of the actual iterator
%% (since ets continuations cannot cross node boundaries). When
%% `Perfix' is not a full prefix, the iterator created iterates all
%% sub-prefixes under `Prefix'. Otherse, the iterator iterates all keys
%% under a prefix. Once created the rest of the iterator API may be used as usual.
%% When done with the iterator, iterator_close/1 must be called
-spec remote_iterator(node(), metadata_prefix() | binary() | atom() | undefined) -> remote_iterator().
remote_iterator(Node, Prefix) when is_atom(Prefix) or is_binary(Prefix) ->
    Ref = gen_server:call({?SERVER, Node}, {open_remote_iterator, self(), undefined, Prefix}, infinity),
    #remote_iterator{ref=Ref,prefix=Prefix,node=Node};
remote_iterator(Node, FullPrefix) when is_tuple(FullPrefix) ->
    Ref = gen_server:call({?SERVER, Node}, {open_remote_iterator, self(), FullPrefix, undefined}, infinity),
    #remote_iterator{ref=Ref,prefix=FullPrefix,node=Node}.

%% @doc advance the iterator by one key, full-prefix or sub-prefix
-spec iterate(metadata_iterator() | remote_iterator()) -> metadata_iterator() | remote_iterator().
iterate(It=#remote_iterator{ref=Ref,node=Node}) ->
    gen_server:call({?SERVER, Node}, {iterate, Ref}, infinity),
    It;
iterate(Iterator) ->
    next_iterator(Iterator).

%% @doc return the full-prefix or prefix being iterated by this iterator. If the iterator is a
%% full-prefix iterator undefined is returned.
-spec iterator_prefix(metadata_iterator() | remote_iterator()) ->
                             metadata_prefix() | undefined | binary() | atom().
iterator_prefix(#remote_iterator{prefix=Prefix}) -> Prefix;
iterator_prefix(#metadata_iterator{prefix=undefined,match=undefined}) -> undefined;
iterator_prefix(#metadata_iterator{prefix=undefined,match=Prefix}) -> Prefix;
iterator_prefix(#metadata_iterator{prefix=Prefix}) -> Prefix.

%% @doc return the key and object or the prefix pointed to by the iterator
-spec iterator_value(metadata_iterator() | remote_iterator()) ->
                            {metadata_key(), metadata_object()} |
                            metadata_prefix() | binary() | atom().
iterator_value(#remote_iterator{ref=Ref,node=Node}) ->
    gen_server:call({?SERVER, Node}, {iterator_value, Ref}, infinity);
iterator_value(#metadata_iterator{prefix=undefined,match=undefined,pos=Pos}) -> Pos;
iterator_value(#metadata_iterator{obj=Obj}) -> Obj.

%% @doc returns true if there are no more keys or prefixes to iterate over
-spec iterator_done(metadata_iterator() | remote_iterator()) -> boolean().
iterator_done(#remote_iterator{ref=Ref,node=Node}) ->
    gen_server:call({?SERVER, Node}, {iterator_done, Ref}, infinity);
iterator_done(#metadata_iterator{done=Done}) -> Done.

%% @doc Closes the iterator. This function must be called on all open iterators
-spec iterator_close(metadata_iterator() | remote_iterator()) -> ok.
iterator_close(#remote_iterator{ref=Ref,node=Node}) ->
    gen_server:call({?SERVER, Node}, {iterator_close, Ref}, infinity);
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
    gen_server:call(?SERVER, {put, PKey, Context, ValueOrFun}, infinity).

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
-spec init([mm_opts()]) -> {ok, #state{}} |
                           {ok, #state{}, non_neg_integer() | infinity} |
                           ignore |
                           {stop, term()}.
init([Opts]) ->
    ?ETS = ets:new(?ETS, [named_table,
                          {read_concurrency, true}, {write_concurrency, true}]),
    StorageMod = app_helper:get_prop_or_env(storage_mod, Opts,
                                            plumtree, ?DEFAULT_STORAGE_BACKEND),
    StorageModOpts = app_helper:get_prop_or_env(storage_mod_opts, Opts,
                                                plumtree, []),
    case StorageMod:init(StorageModOpts) of
        {ok, StorageModState} ->
            Nodename = proplists:get_value(nodename, Opts, node()),
            State = #state{serverid=Nodename,
                           storage_mod=StorageMod,
                           storage_mod_state=StorageModState,
                           iterators=new_ets_tab(),
                           subscriptions=[]},
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({put, PKey, Context, ValueOrFun}, _From, State) ->
    {Result, NewState} = read_modify_write(PKey, Context, ValueOrFun, State),
    {reply, Result, NewState};
handle_call({merge, PKey, Obj}, _From, State) ->
    {Result, NewState} = read_merge_write(PKey, Obj, State),
    {reply, Result, NewState};
handle_call({get, PKey}, _From, State) ->
    Result = read(PKey),
    {reply, Result, State};
handle_call({open_remote_iterator, Pid, FullPrefix, KeyMatch}, _From, State) ->
    Iterator = new_remote_iterator(Pid, FullPrefix, KeyMatch, State),
    {reply, Iterator, State};
handle_call({iterate, RemoteRef}, _From, State) ->
    Next = next_iterator(RemoteRef, State),
    {reply, Next, State};
handle_call({iterator_value, RemoteRef}, _From, State) ->
    Res = from_remote_iterator(fun iterator_value/1, RemoteRef, State),
    {reply, Res, State};
handle_call({iterator_done, RemoteRef}, _From, State) ->
    Res = case from_remote_iterator(fun iterator_done/1, RemoteRef, State) of
              undefined -> true; %% if we don't know about iterator, treat it as done
              Other -> Other
          end,
    {reply, Res, State};
handle_call({iterator_close, RemoteRef}, _From, State) ->
    close_remote_iterator(RemoteRef, State),
    {reply, ok, State};
handle_call({is_stale, PKey, Context}, _From, State) ->
    Existing = read(PKey),
    IsStale = plumtree_metadata_object:is_stale(Context, Existing),
    {reply, IsStale, State};
handle_call({subscribe, FullPrefix, Pid}, _From, #state{subscriptions=Subs} = State) ->
    Ref = monitor(process, Pid),
    NewSubs = lists:usort([{FullPrefix, {Pid, Ref}}|Subs]),
    {reply, ok, State#state{subscriptions=NewSubs}};
handle_call({unsubscribe, FullPrefix, Pid}, _From, #state{subscriptions=Subs} = State) ->
    NewSubs =
    lists:foldl(fun({F, {P, Ref}} = Obj, AccSubs)
                      when (F == FullPrefix) and (P == Pid) ->
                        demonitor(Ref, [flush]),
                        AccSubs -- [Obj];
                   (_, AccSubs) ->
                        AccSubs
                end, Subs, Subs),
    {reply, ok, State#state{subscriptions=NewSubs}}.


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
handle_info({'DOWN', Ref, process, Pid, _Reason}, #state{subscriptions=Subs} = State) ->
    NewState =
    case lists:keyfind({Pid, Ref}, 2, Subs) of
        false ->
            close_remote_iterator(Ref, State),
            State;
        Obj ->
            State#state{subscriptions=Subs -- [Obj]}
    end,
    {noreply, NewState}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(Reason, #state{storage_mod=Mod, storage_mod_state=ModSt}) ->
    Mod:terminate(Reason, ModSt).

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_remote_iterator(Pid, FullPrefix, KeyMatch, #state{iterators=Iterators}) ->
    Ref = monitor(process, Pid),
    Iterator = open_iterator(FullPrefix, KeyMatch),
    ets:insert(Iterators, [{Ref, Iterator}]),
    Ref.


from_remote_iterator(Fun, RemoteRef, State) ->
    case ets:lookup(State#state.iterators, RemoteRef) of
        [] -> undefined;
        [{RemoteRef, It}] -> Fun(It)
    end.

close_remote_iterator(Ref, State=#state{iterators=Iterators}) ->
    from_remote_iterator(fun iterator_close/1, Ref, State),
    ets:delete(Iterators, Ref).

open_iterator(undefined, KeyMatch) ->
    %% full or sub-prefix iterator
    new_iterator(undefined, KeyMatch, ?ETS);
open_iterator(FullPrefix, KeyMatch) ->
    %% key/value iterator
    case ets_tab(FullPrefix) of
        undefined -> empty_iterator(FullPrefix, KeyMatch, undefined);
        Tab -> new_iterator(FullPrefix, KeyMatch, Tab)
    end.

next_iterator(It=#metadata_iterator{done=true}) ->
    %% general catch-all for all iterators
    It;
next_iterator(It=#metadata_iterator{prefix=undefined,match=undefined,tab=Tab,pos=Pos}) ->
    %% full-prefix iterator
    next_iterator(It, ets:next(Tab, Pos));
next_iterator(It=#metadata_iterator{prefix=undefined,pos=Pos}) ->
    %% sub-prefix iterator
    next_iterator(It, ets:select(Pos));
next_iterator(It=#metadata_iterator{pos=Pos}) ->
    %% key/value iterator
    next_iterator(It, ets:match_object(Pos)).

next_iterator(Ref, #state{iterators=Iterators}) when is_reference(Ref) ->
    %% remote iterator
    case ets:lookup(Iterators, Ref) of
        [] -> ok;
        [{Ref, It}] ->
            Next = next_iterator(It),
            ets:insert(Iterators, [{Ref, Next}])
    end,
    Ref;
next_iterator(It, '$end_of_table') ->
    %% general catch-all for all iterators
    It#metadata_iterator{done=true,
                         pos=undefined,
                         obj=undefined};
next_iterator(It=#metadata_iterator{prefix=undefined,match=undefined},Next) ->
    %% full-prefix iterator
    It#metadata_iterator{pos=Next};
next_iterator(It, {[Next], Cont}) ->
    %% sub-prefix or key/value iterator
    It#metadata_iterator{pos=Cont,
                         obj=Next}.

%% universal empty iterator
empty_iterator(FullPrefix, KeyMatch, Tab) ->
    #metadata_iterator{
                prefix=FullPrefix,
                match=KeyMatch,
                pos=undefined,
                obj=undefined,
                done=true,
                tab=Tab
               }.

new_iterator(undefined, undefined, Tab) ->
    %% full-prefix iterator
    new_iterator(undefined, undefined, Tab, ets:first(Tab));
new_iterator(undefined, Prefix, Tab) ->
    %% sub-prefix iterator
    new_iterator(undefined, Prefix, Tab,
                 ets:select(Tab, [{{{Prefix,'$1'},'_'},[],['$1']}], 1));
new_iterator(FullPrefix, KeyMatch, Tab) ->
    %% key/value iterator
    ObjectMatch = iterator_match(KeyMatch),
    new_iterator(FullPrefix, KeyMatch, Tab, ets:match_object(Tab, ObjectMatch, 1)).

new_iterator(FullPrefix, KeyMatch, Tab, '$end_of_table') ->
    %% catch-all for empty iterator of all types
    empty_iterator(FullPrefix, KeyMatch, Tab);
new_iterator(undefined, undefined, Tab, First) ->
    %% full-prefix iterator
    #metadata_iterator{
       prefix=undefined,
       match=undefined,
       pos=First,
       obj=undefined,
       done=false,
       tab=Tab
      };
new_iterator(undefined, Prefix, Tab, {[First], Cont}) ->
    %% sub-prefix iterator
    #metadata_iterator{
       prefix=undefined,
       match=Prefix,
       pos=Cont,
       obj=First,
       done=false,
       tab=Tab
      };
new_iterator(FullPrefix, KeyMatch, Tab, {[First], Cont}) ->
    %% key/value iterator
    #metadata_iterator{
       prefix=FullPrefix,
       match=KeyMatch,
       pos=Cont,
       obj=First,
       done=false,
       tab=Tab
      }.

finish_iterator(#metadata_iterator{done=true}) ->
    ok;
finish_iterator(It) ->
    Next = next_iterator(It),
    finish_iterator(Next).

iterator_match(undefined) ->
    '_';
iterator_match(KeyMatch) ->
    {KeyMatch, '_'}.

read_modify_write(PKey, Context, ValueOrFun, State=#state{serverid=ServerId}) ->
    Existing = read(PKey),
    Modified = plumtree_metadata_object:modify(Existing, Context, ValueOrFun, ServerId),
    store(PKey, ValueOrFun == '$deleted', Modified, State).

read_merge_write(PKey, Obj, State) ->
    Existing = read(PKey),
    case plumtree_metadata_object:reconcile(Obj, Existing) of
        false -> {false, State};
        {true, Reconciled} ->
            {_, NewState} = store(PKey, Reconciled, State),
            {true, NewState}
    end.

store(PKey, Metadata, State) ->
    IsDelete = plumtree_metadata_object:value(Metadata) == '$deleted',
    store(PKey, IsDelete, Metadata, State).

store({FullPrefix, Key}=PKey, IsDelete, Metadata, State) ->
    #state{storage_mod=Mod,
           storage_mod_state=ModSt,
           subscriptions=Subs
          } = State,

    _ = maybe_init_ets(FullPrefix),
    Hash = plumtree_metadata_object:hash(Metadata),
    Tab = ets_tab(FullPrefix),
    {Event, ValToStore} =
    case IsDelete of
        true ->
            Old = ets:lookup(Tab, Key),
            ets:delete(Tab, Key),
            {{delete, FullPrefix, Key,
              [Val || {_, Val} <- Old]}, '$deleted'};
        false ->
            ets:insert(Tab, {Key, Metadata}),
            {{write, FullPrefix, Key, Metadata}, Metadata}
    end,
    plumtree_metadata_hashtree:insert(PKey, Hash),
    {ok, NewModSt} = Mod:store(FullPrefix, [{Key, ValToStore}], ModSt),
    trigger_subscription_event(FullPrefix, Event, Subs),
    {Metadata, State#state{storage_mod_state=NewModSt}}.

trigger_subscription_event(FullPrefix, Event, [{FullPrefix, {Pid, _}}|Rest]) ->
    Pid ! Event,
    trigger_subscription_event(FullPrefix, Event, Rest);
trigger_subscription_event(FullPrefix, Event, [_|Rest]) ->
    trigger_subscription_event(FullPrefix, Event, Rest);
trigger_subscription_event(_, _, []) -> ok.

read({FullPrefix, Key}) ->
    case ets_tab(FullPrefix) of
        undefined -> undefined;
        TabId -> read(Key, TabId)
    end.

read(Key, TabId) ->
    case ets:lookup(TabId, Key) of
        [] -> undefined;
        [{Key, MetaRec}] -> MetaRec
    end.

ets_tab(FullPrefix) ->
    case ets:lookup(?ETS, FullPrefix) of
        [] -> undefined;
        [{FullPrefix, TabId}] -> TabId
    end.

maybe_init_ets(FullPrefix) ->
    case ets_tab(FullPrefix) of
        undefined -> init_ets(FullPrefix);
        _TabId -> ok
    end.

init_ets(FullPrefix) ->
    TabId = new_ets_tab(),
    ets:insert(?ETS, [{FullPrefix, TabId}]),
    TabId.

new_ets_tab() ->
    ets:new(undefined, [ordered_set, {read_concurrency, true}, {write_concurrency, true}]).
