-module(plumtree_leveldb_metadata_manager).
-export([init/1,
         store/3,
         terminate/2]).

-define(MANIFEST, cluster_meta_manifest).
-define(MANIFEST_FILENAME, "manifest_lvldb.dets").
-define(DEFAULT_DB_OPTS, [{create_if_missing, true}]).
-define(TOMBSTONE, '$deleted').

-record(state, {data_root, tab_refs=[]}).

init(Opts) ->
    case data_root(Opts) of
        undefined ->
            {error, no_data_dir};
        DataRoot ->
            State = #state{data_root=DataRoot},
            {ok, _} = init_manifest(State),
            {ok, init_from_files(State)}
    end.

store(FullPrefix, Objs, State) ->
    #state{tab_refs=Refs} = NewState = maybe_init_lvldb(FullPrefix, State),
    {_, TabRef} = lists:keyfind(FullPrefix, 1, Refs),
    Updates = objs_to_updates(Objs, []),
    ok = lvldb_insert(TabRef, Updates),
    {ok, NewState}.

objs_to_updates([{Key, Val}|Rest], Acc) ->
    objs_to_updates(Rest, [{put, term_to_binary(Key), term_to_binary(Val)}|Acc]);
objs_to_updates([], Acc) -> Acc.

terminate(_Reason, State) ->
    close_lvldb_tabs(State#state.tab_refs),
    ok = close_manifest().

data_root(Opts) ->
    case proplists:get_value(data_dir, Opts) of
        undefined -> default_data_root();
        Root -> Root
    end.

default_data_root() ->
    case application:get_env(plumtree, plumtree_data_dir) of
        {ok, PRoot} -> filename:join(PRoot, "lvldb_cluster_meta");
        undefined -> undefined
    end.

init_manifest(State) ->
    ManifestFile = filename:join(State#state.data_root, ?MANIFEST_FILENAME),
    ok = filelib:ensure_dir(ManifestFile),
    {ok, ?MANIFEST} = dets:open_file(?MANIFEST, [{file, ManifestFile}]).

close_manifest() ->
    dets:close(?MANIFEST).

init_from_files(State) ->
    %% TODO: do this in parallel
    manifest_fold_filenames(fun init_from_file/3, State).

init_from_file(FullPrefix, FileName, #state{tab_refs=Refs} = State) ->
    {ok, TabRef} = eleveldb:open(FileName, ?DEFAULT_DB_OPTS),
    TabId = plumtree_metadata_manager:init_ets_for_full_prefix(FullPrefix),
    eleveldb:fold(TabRef, fun({Key, Val}, _Acc) ->
                                  ets:insert(TabId, {binary_to_term(Key),
                                                     binary_to_term(Val)})
                          end, undefined, []),
    State#state{tab_refs=[{FullPrefix, TabRef}|Refs]}.

maybe_init_lvldb(FullPrefix, #state{data_root=DataRoot} = State) ->
    case filelib:is_file(lvldb_file(DataRoot, FullPrefix)) of
        true ->
            State;
        false ->
            init_lvldb(FullPrefix, State)
    end.

init_lvldb(FullPrefix, #state{data_root=DataRoot, tab_refs=Refs} = State) ->
    FileName = lvldb_file(DataRoot, FullPrefix),
    {ok, TabRef} = eleveldb:open(FileName, ?DEFAULT_DB_OPTS),
    manifest_insert([{FullPrefix, FileName}]),
    State#state{tab_refs=[{FullPrefix, TabRef}|Refs]}.

close_lvldb_tabs(Refs) ->
    lists:foreach(fun close_lvldb_tab/1, Refs).

close_lvldb_tab({_FullPrefix, TabRef}) ->
    eleveldb:close(TabRef).

lvldb_insert(TabRef, Updates) ->
    eleveldb:write(TabRef, Updates, []).

lvldb_file(DataRoot, FullPrefix) ->
    filename:join(DataRoot, lvldb_filename(FullPrefix)).

lvldb_filename({Prefix, SubPrefix}=FullPrefix) ->
    MD5Prefix = lvldb_filename_part(Prefix),
    MD5SubPrefix = lvldb_filename_part(SubPrefix),
    Trailer = lvldb_filename_trailer(FullPrefix),
    io_lib:format("~s-~s-~s.db", [MD5Prefix, MD5SubPrefix, Trailer]).

lvldb_filename_part(Part) when is_atom(Part) ->
    lvldb_filename_part(list_to_binary(atom_to_list(Part)));
lvldb_filename_part(Part) when is_binary(Part) ->
    <<MD5Int:128/integer>> = crypto:hash(md5, (Part)),
    integer_to_list(MD5Int, 16).

lvldb_filename_trailer(FullPrefix) ->
    [lvldb_filename_trailer_part(Part) || Part <- tuple_to_list(FullPrefix)].

lvldb_filename_trailer_part(Part) when is_atom(Part) ->
    "1";
lvldb_filename_trailer_part(Part) when is_binary(Part)->
    "0".

manifest_insert(Objs) ->
    ok = dets:insert(?MANIFEST, Objs),
    ok = dets:sync(?MANIFEST).

manifest_fold_filenames(Fun, Acc0) ->
    dets:foldl(fun({FullPrefix, FileName}, Acc) ->
                       Fun(FullPrefix, FileName, Acc)
               end, Acc0, ?MANIFEST).
