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
%%
%% Initialization code is from basho/riak_kv
%% -------------------------------------------------------------------

-module(plumtree_metadata_leveldb_instance).

-behaviour(gen_server).

%% API functions
-export([start_link/2,
         name/1,
         get/1,
         put/2,
         delete/1,
         status/1,
         data_size/1,
         iterator/2,
         iterator_close/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {ref :: eleveldb:db_ref(),
                data_root :: string(),
                open_opts = [],
                config :: config(),
                read_opts = [],
                write_opts = [],
                fold_opts = [{fill_cache, false}],
                open_iterators = []
               }).

-type config() :: [{atom(), term()}].

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(InstanceId, Opts) ->
    gen_server:start_link({local, name(InstanceId)}, ?MODULE,
                          [InstanceId, Opts], []).

get(Key) ->
    InstanceId = plumtree_metadata_leveldb_instance_sup:get_instance_id_for_key(Key),
    Name = name(InstanceId),
    gen_server:call(Name, {get, Key}, infinity).

put(Key, Value) ->
    InstanceId = plumtree_metadata_leveldb_instance_sup:get_instance_id_for_key(Key),
    Name = name(InstanceId),
    gen_server:call(Name, {put, Key, Value}, infinity).

delete(Key) ->
    InstanceId = plumtree_metadata_leveldb_instance_sup:get_instance_id_for_key(Key),
    Name = name(InstanceId),
    gen_server:call(Name, {delete, Key}, infinity).

name(Id) ->
    list_to_atom("plmtrlvld_" ++ integer_to_list(Id)).

status(InstanceId) ->
    Name = name(InstanceId),
    gen_server:call(Name, status, infinity).

data_size(InstanceId) ->
    Name = name(InstanceId),
    gen_server:call(Name, data_size, infinity).


iterator(Instance, KeysOnly) when is_pid(Instance) or is_atom(Instance) ->
    gen_server:call(Instance, {new_iterator, self(), KeysOnly}, infinity).

iterator_close(Instance, Itr) when is_pid(Instance) or is_atom(Instance) ->
    gen_server:call(Instance, {close_iterator, Itr}, infinity).

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
init([InstanceId, Opts]) ->
    %% Initialize random seed
    rnd:seed(time_compat:timestamp()),

    %% Get the data root directory
    DataDir1 = filename:join(app_helper:get_prop_or_env(plumtree_data_dir, Opts, plumtree),
                             "meta"),
    DataDir2 = filename:join(DataDir1, integer_to_list(InstanceId)),

    %% Initialize state
    S0 = init_state(DataDir2, Opts),
    process_flag(trap_exit, true),
    case open_db(S0) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
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
handle_call({get, Key}, _From, #state{read_opts=ReadOpts, ref=Ref} = State) ->
    case eleveldb:get(Ref, sext:encode(Key), ReadOpts) of
        {ok, Value} ->
            {reply, {ok, binary_to_term(Value)}, State};
        not_found  ->
            {reply, {error, not_found}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({put, Key, Value}, _From, #state{write_opts=WriteOpts, ref=Ref} = State) ->
    Update = [{put, sext:encode(Key), term_to_binary(Value)}],
    %% Perform the write...
    case eleveldb:write(Ref, Update, WriteOpts) of
        ok ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({delete, Key}, _From, #state{write_opts=WriteOpts, ref=Ref} = State) ->
    Update = [{delete, sext:encode(Key)}],
    %% Perform the write...
    case eleveldb:write(Ref, Update, WriteOpts) of
        ok ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(status, _From, #state{ref=Ref} = State) ->
    {ok, Stats} = eleveldb:status(Ref, <<"leveldb.stats">>),
    {ok, ReadBlockError} = eleveldb:status(Ref, <<"leveldb.ReadBlockError">>),
    {reply, [{stats, Stats}, {read_block_error, ReadBlockError}], State};
handle_call(data_size, _From, #state{ref=Ref} = State) ->
    Reply =
    try {ok, <<SizeStr/binary>>} = eleveldb:status(Ref, <<"leveldb.total-bytes">>),
         list_to_integer(binary_to_list(SizeStr)) of
        Size ->
            {Size, bytes}
    catch
        error:_ ->
            undefined
    end,
    {reply, Reply, State};
handle_call({new_iterator, Owner, KeysOnly}, _From, #state{ref=Ref, fold_opts=FoldOpts, open_iterators=OpenIterators} = State) ->
    MRef = monitor(process, Owner),
    {ok, Itr} =
    case KeysOnly of
        true ->
            eleveldb:iterator(Ref, FoldOpts, keys_only);
        false ->
            eleveldb:iterator(Ref, FoldOpts)
    end,
    {reply, Itr, State#state{open_iterators=[{MRef, Itr}|OpenIterators]}};
handle_call({close_iterator, Itr}, _From, #state{open_iterators=OpenIterators} = State) ->
    {MRef, _} = lists:keyfind(Itr, 2, OpenIterators),
    demonitor(MRef, [flush]),
    {reply, ok, State#state{open_iterators=lists:keydelete(MRef, 1, OpenIterators)}}.

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
handle_info({'DOWN', MRef, process, _, _}, #state{open_iterators=OpenIterators} = State) ->
    case lists:keyfind(MRef, 1, OpenIterators) of
        false ->
            ignore;
        {_, Itr} ->
            eleveldb:iterator_close(Itr)
    end,
    {noreply, State#state{open_iterators=lists:keydelete(MRef, 1, OpenIterators)}};
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
terminate(_Reason, #state{ref=Ref}) ->
    eleveldb:close(Ref),
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

%% @private
init_state(DataRoot, Config) ->
    %% Get the data root directory
    filelib:ensure_dir(filename:join(DataRoot, "dummy")),

    %% Merge the proplist passed in from Config with any values specified by the
    %% eleveldb app level; precedence is given to the Config.
    MergedConfig = orddict:merge(fun(_K, VLocal, _VGlobal) -> VLocal end,
                                 orddict:from_list(Config), % Local
                                 orddict:from_list(application:get_all_env(eleveldb))), % Global

    %% Use a variable write buffer size in order to reduce the number
    %% of vnodes that try to kick off compaction at the same time
    %% under heavy uniform load...
    WriteBufferMin = config_value(write_buffer_size_min, MergedConfig, 30 * 1024 * 1024),
    WriteBufferMax = config_value(write_buffer_size_max, MergedConfig, 60 * 1024 * 1024),
    WriteBufferSize = WriteBufferMin + rnd:uniform(1 + WriteBufferMax - WriteBufferMin),

    %% Update the write buffer size in the merged config and make sure create_if_missing is set
    %% to true
    FinalConfig = orddict:store(write_buffer_size, WriteBufferSize,
                                orddict:store(create_if_missing, true, MergedConfig)),

    %% Parse out the open/read/write options
    {OpenOpts, _BadOpenOpts} = eleveldb:validate_options(open, FinalConfig),
    {ReadOpts, _BadReadOpts} = eleveldb:validate_options(read, FinalConfig),
    {WriteOpts, _BadWriteOpts} = eleveldb:validate_options(write, FinalConfig),

    %% Use read options for folding, but FORCE fill_cache to false
    FoldOpts = lists:keystore(fill_cache, 1, ReadOpts, {fill_cache, false}),

    %% Warn if block_size is set
    SSTBS = proplists:get_value(sst_block_size, OpenOpts, false),
    BS = proplists:get_value(block_size, OpenOpts, false),
    case BS /= false andalso SSTBS == false of
        true ->
            lager:warning("eleveldb block_size has been renamed sst_block_size "
                          "and the current setting of ~p is being ignored.  "
                          "Changing sst_block_size is strongly cautioned "
                          "against unless you know what you are doing.  Remove "
                          "block_size from app.config to get rid of this "
                          "message.\n", [BS]);
        _ ->
            ok
    end,

    %% Generate a debug message with the options we'll use for each operation
    lager:info("Datadir ~s options for LevelDB: ~p\n",
                [DataRoot, [{open, OpenOpts}, {read, ReadOpts}, {write, WriteOpts}, {fold, FoldOpts}]]),
    #state { data_root = DataRoot,
             open_opts = OpenOpts,
             read_opts = ReadOpts,
             write_opts = WriteOpts,
             fold_opts = FoldOpts,
             config = FinalConfig }.

config_value(Key, Config, Default) ->
    case orddict:find(Key, Config) of
        error ->
            Default;
        {ok, Value} ->
            Value
    end.

open_db(State) ->
    RetriesLeft = app_helper:get_env(plumtree, eleveldb_open_retries, 30),
    open_db(State, max(1, RetriesLeft), undefined).

open_db(_State0, 0, LastError) ->
    {error, LastError};
open_db(State0, RetriesLeft, _) ->
    case eleveldb:open(State0#state.data_root, State0#state.open_opts) of
        {ok, Ref} ->
            {ok, State0#state { ref = Ref }};
        %% Check specifically for lock error, this can be caused if
        %% a crashed instance takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    SleepFor = app_helper:get_env(plumtree, eleveldb_open_retry_delay, 2000),
                    lager:debug("Plumtree Leveldb backend retrying ~p in ~p ms after error ~s\n",
                                [State0#state.data_root, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(State0, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
