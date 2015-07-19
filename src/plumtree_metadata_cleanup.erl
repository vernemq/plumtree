-module(plumtree_metadata_cleanup).

-behaviour(gen_server).

%% API functions
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {full_prefix, interval, deleted}).

-define(TOMBSTONE, '$deleted').

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
start_link(FullPrefix) ->
    gen_server:start_link(?MODULE, [FullPrefix], []).

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
init([FullPrefix]) ->
    {_, Deleted} = plumtree_metadata:fold(fun cleanup_tombstones/2,
                                          {FullPrefix, gb_sets:new()},
                                          FullPrefix, [{resolver, lww}]),
    CleanupInterval = app_helper:get_prop_or_env(cleanup_interval, [],
                                                 plumtree, 10000),
    erlang:send_after(CleanupInterval, self(), cleanup),
    {ok, #state{full_prefix=FullPrefix,
                interval=CleanupInterval,
                deleted=Deleted}}.

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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
handle_info(cleanup, #state{full_prefix=FullPrefix,
                            interval=Interval,
                            deleted=Deleted} = State) ->
    {_, NewDeleted} = plumtree_metadata:fold(fun cleanup_tombstones/2,
                                             {FullPrefix, Deleted},
                                             FullPrefix, [{resolver, lww}]),
    erlang:send_after(Interval, self(), cleanup),
    {noreply, State#state{deleted=NewDeleted}}.

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
cleanup_tombstones({Key, ?TOMBSTONE}, {FullPrefix, Set}) ->
    case gb_sets:is_element(Key, Set) of
        true ->
            %% delete
            plumtree_metadata_manager:force_delete({FullPrefix, Key}),
            {FullPrefix, gb_sets:delete(Key, Set)};
        false ->
            {FullPrefix, gb_sets:add(Key, Set)}
    end;
cleanup_tombstones({Key, _}, {FullPrefix, Set}) ->
    {FullPrefix, gb_sets:delete_any(Key, Set)}.
