-module(plumtree_dets_metadata_manager).
-export([init/1,
         store/3,
         delete/3,
         terminate/2]).

-define(MANIFEST, cluster_meta_manifest).
-define(MANIFEST_FILENAME, "manifest.dets").
-define(TOMBSTONE, '$deleted').

-record(state, {data_root}).

init(Opts) ->
    case data_root(Opts) of
        undefined ->
            {error, no_data_dir};
        DataRoot ->
            State = #state{data_root=DataRoot},
            {ok, _} = init_manifest(State),
            init_from_files(State),
            {ok, #state{data_root=DataRoot}}
    end.

store(FullPrefix, Objs, State) ->
    maybe_init_dets(FullPrefix, State#state.data_root),
    ok = dets_insert(dets_tabname(FullPrefix), Objs),
    {ok, State}.

delete(FullPrefix, Key, State) ->
    maybe_init_dets(FullPrefix, State#state.data_root),
    ok = dets_delete(dets_tabname(FullPrefix), Key),
    {ok, State}.


terminate(_Reason, _State) ->
    close_dets_tabs(),
    ok = close_manifest().

data_root(Opts) ->
    case proplists:get_value(data_dir, Opts) of
        undefined -> default_data_root();
        Root -> Root
    end.

default_data_root() ->
    case application:get_env(plumtree, plumtree_data_dir) of
        {ok, PRoot} -> filename:join(PRoot, "cluster_meta");
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
    dets_fold_tabnames(fun init_from_file/2, State).

init_from_file(TabName, State) ->
    FullPrefix = dets_tabname_to_prefix(TabName),
    FileName = dets_file(State#state.data_root, FullPrefix),
    {ok, TabName} = dets:open_file(TabName, [{file, FileName}]),
    TabId = plumtree_metadata_manager:init_ets_for_full_prefix(FullPrefix),
    TabId = dets:to_ets(TabName, TabId),
    State.

maybe_init_dets(FullPrefix, DataRoot) ->
    case dets:info(dets_tabname(FullPrefix)) of
        undefined -> init_dets(FullPrefix, DataRoot);
        _ -> ok
    end.

init_dets(FullPrefix, DataRoot) ->
    TabName = dets_tabname(FullPrefix),
    FileName = dets_file(DataRoot, FullPrefix),
    {ok, TabName} = dets:open_file(TabName, [{file, FileName}]),
    dets_insert(?MANIFEST, [{FullPrefix, TabName, FileName}]).

close_dets_tabs() ->
    dets_fold_tabnames(fun close_dets_tab/2, undefined).

close_dets_tab(TabName, _Acc) ->
    dets:close(TabName).

dets_insert(TabName, Objs) ->
    ok = dets:insert(TabName, Objs),
    ok = dets:sync(TabName).

dets_delete(TabName, Key) ->
    ok = dets:delete(TabName, Key),
    ok = dets:sync(TabName).

dets_tabname(FullPrefix) -> {?MODULE, FullPrefix}.
dets_tabname_to_prefix({?MODULE, FullPrefix}) ->  FullPrefix.

dets_file(DataRoot, FullPrefix) ->
    filename:join(DataRoot, dets_filename(FullPrefix)).

dets_filename({Prefix, SubPrefix}=FullPrefix) ->
    MD5Prefix = dets_filename_part(Prefix),
    MD5SubPrefix = dets_filename_part(SubPrefix),
    Trailer = dets_filename_trailer(FullPrefix),
    io_lib:format("~s-~s-~s.dets", [MD5Prefix, MD5SubPrefix, Trailer]).

dets_filename_part(Part) when is_atom(Part) ->
    dets_filename_part(list_to_binary(atom_to_list(Part)));
dets_filename_part(Part) when is_binary(Part) ->
    <<MD5Int:128/integer>> = crypto:hash(md5, (Part)),
    integer_to_list(MD5Int, 16).

dets_filename_trailer(FullPrefix) ->
    [dets_filename_trailer_part(Part) || Part <- tuple_to_list(FullPrefix)].

dets_filename_trailer_part(Part) when is_atom(Part) ->
    "1";
dets_filename_trailer_part(Part) when is_binary(Part)->
    "0".

dets_fold_tabnames(Fun, Acc0) ->
    dets:foldl(fun({_FullPrefix, TabName, _FileName}, Acc) ->
                       Fun(TabName, Acc)
               end, Acc0, ?MANIFEST).
