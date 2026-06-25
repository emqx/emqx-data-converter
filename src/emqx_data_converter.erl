-module(emqx_data_converter).

-feature(maybe_expr, enable).

-include("emqx_data_converter.hrl").

%% API exports
-export([main/1]).

-define(CLI_OPTS,
        [{output_dir, $o, "output-dir", {string, undefined},
          "A directory path where output EMQX 5.1 backup tar.gz file will be written. "
          "If omitted, the file is written to CWD."},
         {user_id_type, $u, "user-id-type", {binary, undefined},
          "User type (clientid or username) of built-in DB (Mnesia) authentication credentials to migrate. "
          "EMQX 4.4 supports both clientid and username credentials at the same time, while EMQX 5.1 or later uses "
          "one type at a time. If this option is not provided, the user type that has more credentials in "
          "the input file will be chosen."},
         {jwt_type, $j, "jwt-type", {binary, undefined},
          "JWT authentication type to migrate. Possible values: hmac-based, public-key, jwks. EMQX 5.1 or later supports "
          "only one of the aforementioned types at a time. If this option is omitted, JWT authentication is migrated "
          "according to the following (descending) precedence: 1. jwks, 2. public-key, 3. hmac-based."},
         {edition, $e, "emqx-edition", {string, "ee"},
          "EMQX edition of the both input and output backup files. Possible values: ee, ce. "
          "Please note that EMQX 5.1 or later doesn't allow to import ee backup file to ce cluster."},
         {input_file, undefined, undefined, string, "Input EMQX 4.4 backup JSON file path."},
         {data_files_dir, $r, "data-files-dir", {string, undefined},
          "Path to the dir that contains EMQX 4.4.x DCD and DCL files. "
          "The DCD and DCL files can be found in the data dir of the EMQX 4.4.x installation. i.e. data files for emqx_retainer are named as 'emqx_retainer.DCD' and 'emqx_retainer.DCL'. "
          "If the retainer used RAM copy in EMQX 4.4.x, you can generate the files by running the following command: "
          "  emqx eval 'mnesia:dump_tables([emqx_retainer])'"
          "After data is imported into EMQX 5.x, run the following command to reindex the retain topics: "
          "  emqx ctl retainer reindex start"
         }
        ]).

-define(tar(_FileName_), _FileName_ ++ ?TAR_SUFFIX).
-define(TAR_SUFFIX, ".tar.gz").

-define(TAB_PD(APP), {?MODULE, tab, APP}).

-define(fmt_tar_err(_Expr_),
        fun() ->
                case _Expr_ of
                    {error, _Reason_} -> {error, erl_tar:format_error(_Reason_)};
                    _Other_ -> _Other_
                end
        end()).

-define(META_FILENAME, "META.hocon").
-define(CLUSTER_HOCON_FILENAME, "cluster.hocon").
-define(BACKUP_MNESIA_DIR, "mnesia").
-define(AUTHN_CHAIN, 'mqtt:global').
%% Current version used as base for the converter
-define(EMQX_VERSION, "5.8.1").

-define(PLACEHOLDERS,
        [{<<"${username}">>, <<"%u">>},
         {<<"${clientid}">>, <<"%c">>},
         {<<"${cert_common_name}">>, <<"%C">>},
         {<<"${cert_subject}">>, <<"%d">>},
         {<<"${peerhost}">>, <<"%a">>},
         {<<"${password}">>, <<"%P">>}
        ]).

%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point

main([]) ->
    show_usage_exit(1);
main([H]) when H =:= "-h"; H =:= "--help" ->
    show_usage_exit(0);
main([V]) when V =:= "-v"; V =:= "--version" ->
    ok = application:load(emqx_data_converter),
    {_, _, Ver} = lists:keyfind(emqx_data_converter, 1, application:loaded_applications()),
    io:format("~s~n", [Ver]),
    halt(0);
main(Args) ->
    {ok, {Opts, _}} = getopt:parse(?CLI_OPTS, Args),
    validate_input_file(proplists:get_value(input_file, Opts)),
    validate_jwt_type(proplists:get_value(jwt_type, Opts)),
    validate_user_id_type(proplists:get_value(user_id_type, Opts)),
    validate_edition(proplists:get_value(edition, Opts)),
    application:ensure_all_started(emqx_data_converter),
    try
        main1(Opts)
    after
        MnesaDir = mnesia_monitor:get_env(dir),
        file:del_dir_r(MnesaDir)
    end.

%%====================================================================
%% Internal functions
%%====================================================================

validate_input_file(undefined) ->
    log_error("Missing required argument: <input_file>"),
    show_usage_exit(1);
validate_input_file(_) -> ok.

validate_user_id_type(T) when T =:= <<"clientid">>;
                              T =:= <<"username">>;
                              T =:= undefined ->
    ok;
validate_user_id_type(T) ->
    log_error("Invalid user-id-type: ~s", [T]),
    show_usage_exit(1).

validate_jwt_type(T) when T =:= <<"public-key">>;
                          T =:= <<"hmac-based">>;
                          T =:= <<"jwks">>;
                          T =:= undefined ->
    ok;
validate_jwt_type(T) ->
    log_error("Invalid jwt-type: ~s", [T]),
    show_usage_exit(1).

validate_edition(E) when E =:= "ee"; E =:= "ce" ->
    ok;
validate_edition(E) ->
    log_error("Invalid EMQX edition: ~s", [E]),
    show_usage_exit(1).

show_usage_exit(Reason) ->
    getopt:usage(?CLI_OPTS, escript:script_name(), standard_io),
    halt(Reason).

main1(Opts) ->
    OutputDir = case proplists:get_value(output_dir, Opts) of
                    undefined ->
                        {ok, CWD} = file:get_cwd(),
                        CWD;
                    ODir -> ODir
                end,
    UserIdType = proplists:get_value(user_id_type, Opts),
    JwtType = proplists:get_value(jwt_type, Opts),
    {ok, InputBin} = file:read_file(proplists:get_value(input_file, Opts)),
    InputMap = jsone:decode(InputBin),
    ok = setup_mnesia(),
    ok = copy_data_files(proplists:get_value(data_files_dir, Opts)),
    ok = convert_retained_messages(),
    {ok, UserIdType1} = convert_auth_mnesia(InputMap, UserIdType),
    ok = convert_acl_mnesia(InputMap),
    ok = convert_blacklist_mnesia(InputMap),
    ok = convert_emqx_app_mnesia(InputMap),
    OutRawConf0 = convert_auth_modules(InputMap, #{output_dir => OutputDir,
                                                  user_id_type => UserIdType1,
                                                  jwt_type => JwtType}),
    OutRawConf1 = convert_psk_auth(InputMap, OutRawConf0),
    OutRawConf2 = convert_mqtt_subscriber(InputMap, OutRawConf1),
    OutRawConf = convert_retainer_module(InputMap, OutRawConf2),
    OutRawConfRule = convert_rules_resources(InputMap, OutRawConf),
    {BackupName, TarDescriptor} = prepare_new_backup(OutputDir),
    Edition = proplists:get_value(edition, Opts),
    {ok, BackupTarName} = export(OutRawConfRule, BackupName, TarDescriptor, Edition),
    file:del_dir_r(BackupName),
    log_info("Converted to EMQX 5.1 backup file: ~s", [BackupTarName]).

setup_mnesia() ->
    mnesia:delete_schema([node()]),
    ok = mnesia:create_schema([node()]),
    ok = mnesia:start(),
    LazyTables = [
        emqx_app,
        emqx_authn_mnesia,
        emqx_psk,
        emqx_acl,
        emqx_banned,
        emqx_retainer_message
    ],
    TabSpecs = maps:without(LazyTables, tabs_spec()),
    maps:foreach(fun create_table/2, TabSpecs).

tabs_spec() ->
    #{emqx_authn_mnesia =>
          [{type, ordered_set},
           {record_name, user_info},
           {attributes, record_info(fields, user_info)}
          ],
      emqx_acl =>
          [{type, ordered_set},
           {record_name, emqx_acl},
           {attributes, record_info(fields, emqx_acl)}
          ],
      emqx_retainer_message =>
          [{type, ordered_set},
           {record_name, retained_message},
           {attributes, record_info(fields, retained_message)}
          ],
      emqx_banned =>
          [{type, set},
           {record_name, banned},
           {attributes, record_info(fields, banned)}
          ],
      emqx_psk =>
          [{type, ordered_set},
           {record_name, psk_entry},
           {attributes, record_info(fields, psk_entry)}
          ],
      emqx_app =>
          [{type, set},
           {record_name, emqx_app},
           {attributes, record_info(fields, emqx_app)}
          ]
     }.

create_table(Name, Opts0) ->
    Opts = [{disc_copies, [node()]} | Opts0],
    {atomic, ok} = mnesia:create_table(Name, Opts),
    ok = mnesia:wait_for_tables([Name], infinity),
    put(?TAB_PD(Name), #{created => true}),
    ok.

create_table_lazy(Name) ->
    Opts = maps:get(Name, tabs_spec()),
    create_table(Name, Opts).

get_table_status_pd(Name) ->
    get(?TAB_PD(Name)).

prepare_new_backup(OutputDir) ->
    Ts = erlang:system_time(millisecond),
    {{Y, M, D}, {HH, MM, SS}} = local_datetime(Ts),
    BackupBaseName = str(
        io_lib:format(
            "emqx-export-~0p-~2..0b-~2..0b-~2..0b-~2..0b-~2..0b.~3..0b",
            [Y, M, D, HH, MM, SS, Ts rem 1000]
        )
    ),
    BackupName = filename:join(OutputDir, BackupBaseName),
    BackupTarName = ?tar(BackupName),
    {ok, TarDescriptor} = ?fmt_tar_err(erl_tar:open(BackupTarName, [write, compressed])),
    {BackupName, TarDescriptor}.

export(OutRawConf, BackupName, TarDescriptor, Edition) ->
    BackupBaseName = filename:basename(BackupName),
    BackupTarName = ?tar(BackupName),
    Meta = #{
             version => ?EMQX_VERSION,
             edition => Edition
            },
    MetaBin = bin(hocon_pp:do(Meta, #{})),
    MetaFileName = filename:join(BackupBaseName, ?META_FILENAME),
    ok = ?fmt_tar_err(erl_tar:add(TarDescriptor, MetaBin, MetaFileName, [])),
    ok = export_mnesia_tabs(TarDescriptor, BackupName, BackupBaseName),
    RawConfBin = bin(hocon_pp:do(OutRawConf, #{})),
    ConfNameInArchive = filename:join(BackupBaseName, ?CLUSTER_HOCON_FILENAME),
    ok = ?fmt_tar_err(erl_tar:add(TarDescriptor, RawConfBin, ConfNameInArchive, [])),
    ok = ?fmt_tar_err(erl_tar:close(TarDescriptor)),
    {ok, BackupTarName}.

export_mnesia_tabs(TarDescriptor, BackupName, BackupBaseName) ->
    lists:foreach(
        fun(Tab) -> export_mnesia_tab(TarDescriptor, Tab, BackupName, BackupBaseName) end,
        maps:keys(tabs_spec())
    ).

export_mnesia_tab(TarDescriptor, TabName, BackupName, BackupBaseName) ->
    case get_table_status_pd(TabName) of
        #{created := true} ->
            do_export_mnesia_tab1(TarDescriptor, TabName, BackupName, BackupBaseName);
        _ ->
            ok
    end.

do_export_mnesia_tab1(TarDescriptor, TabName, BackupName, BackupBaseName) ->
    {ok, MnesiaBackupName} = do_export_mnesia_tab2(TabName, BackupName),
    NameInArchive = mnesia_backup_name(BackupBaseName, TabName),
    ok = ?fmt_tar_err(erl_tar:add(TarDescriptor, MnesiaBackupName, NameInArchive, [])),
    _ = file:delete(MnesiaBackupName),
    ok.

do_export_mnesia_tab2(TabName, BackupName) ->
    Node = node(),
    try
        {ok, TabName, [Node]} = mnesia:activate_checkpoint(
            [{name, TabName}, {min, [TabName]}, {allow_remote, false}]
        ),
        MnesiaBackupName = mnesia_backup_name(BackupName, TabName),
        ok = filelib:ensure_dir(MnesiaBackupName),
        ok = mnesia:backup_checkpoint(TabName, MnesiaBackupName),
        {ok, MnesiaBackupName}
    after
        mnesia:deactivate_checkpoint(TabName)
    end.

mnesia_backup_name(Path, TabName) ->
    filename:join([Path, ?BACKUP_MNESIA_DIR, atom_to_list(TabName)]).

copy_data_files(undefined) -> ok;
copy_data_files(DataFilesDir) ->
    MnesaDir = mnesia_monitor:get_env(dir),
    DataFiles = filelib:wildcard(filename:join([DataFilesDir, "*.DCD"]))
             ++ filelib:wildcard(filename:join([DataFilesDir, "*.DCL"])),
    lists:foreach(fun(File) ->
            {ok, _} = file:copy(File, filename:join(MnesaDir, filename:basename(File)))
        end, DataFiles).

convert_retained_messages() ->
    case mnesia_lib:exists(mnesia_lib:tab2dcd(emqx_retainer)) of
        true ->
            create_table_lazy(emqx_retainer_message),
            ets:new(emqx_retainer, [set, named_table, public, {keypos, 2}]),
            mnesia_log:dcd2ets(emqx_retainer),
            MsgNum = ets:foldl(fun({retained, Topic, Message44, Expiry}, Count) ->
                Msg = convert_to_message_5(Message44),
                ok = mnesia:dirty_write(emqx_retainer_message,
                        #retained_message{topic = Topic, msg = Msg, expiry_time = Expiry}),
                Count + 1
            end, 0, emqx_retainer),
            log_info("Converted ~B retained messages.", [MsgNum]);
        false ->
            log_warning("No retained messages to migrate.")
    end.

convert_auth_mnesia(#{<<"auth_mnesia">> := []}, UserIdType) ->
    {ok, UserIdType};
convert_auth_mnesia(#{<<"auth_mnesia">> := AuthMnesiaData}, UserIdType) ->
    create_table_lazy(emqx_authn_mnesia),
    UserIdType1 = user_type(AuthMnesiaData, UserIdType),
    lists:foreach(
      fun(#{<<"login">> := L, <<"type">> := T, <<"password">> := P}) when T =:= UserIdType1 ->
              <<Salt:32, PHash/binary>> = base64:decode(P),
              ok = mnesia:dirty_write(
                     emqx_authn_mnesia,
                     #user_info{user_id = {?AUTHN_CHAIN, L},
                                password_hash = PHash,
                                salt = <<Salt:32>>,
                                is_superuser = false}
                    );
         (_) ->
              ok
      end,
      AuthMnesiaData
     ),
    {ok, UserIdType1};
convert_auth_mnesia(_InputMap, UserIdType) ->
    {ok, UserIdType}.

user_type(AuthMnesiaData, undefined) ->
    {UserCount, ClientCount} =
        lists:foldl(fun(#{<<"type">> := <<"username">>}, {UAcc, CAcc}) ->
                            {UAcc+1, CAcc};
                       (#{<<"type">> := <<"clientid">>}, {UAcc, CAcc}) ->
                            {UAcc, CAcc+1}
                    end,
                    {0, 0},
                    AuthMnesiaData),
    case {UserCount, ClientCount} of
        {0, 0} -> undefined;
        {C, C} ->
            log_info(
              "Input file has equal numbers of username and clientid internal DB credentials, "
              "choosing username for migration. If you need to migrate clientid instead, please re-run the converter with: "
              "--user-id-type clientid"),
            <<"username">>;
        {U, C} ->
            {Chosen, Discarded} = case U > C of
                                      true -> {<<"username">>, <<"clientid">>};
                                      _ -> {<<"clientid">>, <<"username">>}
                                  end,
            log_info(
              "Choosing ~s user-id-type for migrating Internal DB credentials. If you need to migrate ~s instead, "
              "please re-run the converter with: --user-id-type ~s",
              [Chosen, Discarded, Discarded]),
            Chosen
    end;
user_type(_AuthMnesiaData, UserType) ->
    UserType.

convert_acl_mnesia(#{<<"acl_mnesia">> := []}) ->
    ok;
convert_acl_mnesia(#{<<"acl_mnesia">> := AclMnesiaData}) ->
    create_table_lazy(emqx_acl),
    KeyFun = fun(#{<<"type">> := <<"all">>}) -> ?ACL_TABLE_ALL;
                (#{<<"type">> := Type0, <<"type_value">> := Val}) ->
                     Type = case Type0 of
                                <<"clientid">> -> ?ACL_TABLE_CLIENTID;
                                <<"username">> -> ?ACL_TABLE_USERNAME
                            end,
                     {Type, Val}
             end,
    GroupedAcl = maps:groups_from_list(KeyFun, AclMnesiaData),
    maps:foreach(
      fun(Who, Rules0) ->
              Rules = shrink_acl_rules(lists:map(fun convert_acl_mnesia_rule/1, Rules0)),
              ok = mnesia:dirty_write(#emqx_acl{who = Who, rules = Rules})
      end,
      GroupedAcl);
convert_acl_mnesia(_InutMap) ->
    ok.

convert_acl_mnesia_rule(#{<<"action">> := Action, <<"access">> := Permission, <<"topic">> := Topic}) ->
    Placeholders = [{<<"${username}">>, <<"%u">>}, {<<"${clientid}">>, <<"%c">>}],
    {permission(Permission), action(Action), convert_placeholders(Topic, Placeholders)}.

action(<<"pub">>) -> publish;
action(<<"sub">>) -> subscribe.

permission(<<"allow">>) -> allow;
permission(<<"deny">>) -> deny.

shrink_acl_rules(Rules0) ->
    KeyFun = fun({Permission, _Action, Topic}) -> {Permission, Topic} end,
    lists:flatmap(
      %% at this stage, action can only be publish or subscribe
      fun([{Perm, Action1, Topic}, {Perm, Action2, Topic}]) when Action1 =/= Action2 ->
              [{Perm, all, Topic}];
         (Rules) -> Rules
      end,
      maps:values(maps:groups_from_list(KeyFun, Rules0))
     ).

convert_blacklist_mnesia(#{<<"blacklist">> := []}) ->
    ok;
convert_blacklist_mnesia(#{<<"blacklist">> := BlackList}) ->
    create_table_lazy(emqx_banned),
    lists:foreach(
      fun(#{<<"who">> := Who, <<"reason">> := Reason, <<"by">> := By,
            <<"at">> := At, <<"until">> := Until}) ->
              Who1 = banned_who(Who),
              ok = mnesia:dirty_write(
                     emqx_banned,
                     #banned{who = Who1, by = By, reason = Reason, at = At, until = Until}
                    )
      end,
      BlackList);
convert_blacklist_mnesia(_InputMap) ->
    ok.

banned_who(#{<<"peerhost">> := Addr}) ->
    {ok, IPAddr} = inet:parse_address(Addr),
    {peerhost, IPAddr};
banned_who(#{<<"username">> := User}) ->
    {username, User};
banned_who(#{<<"clientid">> := Client}) ->
    {clientid, Client}.

convert_emqx_app_mnesia(#{<<"apps">> := []}) ->
    ok;
convert_emqx_app_mnesia(#{<<"apps">> := Apps}) ->
    create_table_lazy(emqx_app),
    lists:foreach(fun(#{<<"id">> := <<"admin">>}) -> ok;
                     (#{<<"id">> := Id, <<"secret">> := Sec, <<"name">> := N, <<"desc">> := D,
                       <<"status">> := St, <<"expired">> := Exp}) ->
                          Exp1 = case Exp of
                                     <<"undefined">> -> infinity;
                                     Ts -> Ts
                                 end,
                          ok = mnesia:dirty_write(
                                 #emqx_app{api_key = Id, api_secret_hash = hash(Sec), desc = D,
                                           name = N, expired_at = Exp1, enable = St})
                  end,
                  Apps);
convert_emqx_app_mnesia(_InputMap) ->
    ok.

%% Copy of `emqx_dashboard_admin:hash/1` (EMQX 5.1)
hash(Password) ->
    SaltBin = salt(),
    <<SaltBin/binary, (sha256(SaltBin, Password))/binary>>.

salt() ->
    <<X:16/big-unsigned-integer>> = crypto:strong_rand_bytes(2),
    iolist_to_binary(io_lib:format("~4.16.0b", [X])).

sha256(SaltBin, Password) ->
    crypto:hash('sha256', <<SaltBin/binary, Password/binary>>).

convert_psk_auth(#{<<"modules">> := Modules}, OutRawConf) ->
    case get_modules_by_type(<<"psk_authentication">>, Modules) of
        [#{<<"enabled">> := IsEnabled, <<"config">> := #{<<"psk_file">> := #{<<"file">> := F}}}] ->
            psk_auth(IsEnabled, F, OutRawConf);
        _ ->
            OutRawConf
    end;
convert_psk_auth(_InputMap, OutRawConf) ->
    OutRawConf.

convert_retainer_module(#{<<"modules">> := Modules}, OutRawConf) ->
    case get_modules_by_type(<<"retainer">>, Modules) of
        [#{<<"enabled">> := IsEnabled, <<"config">> := Conf}] ->
            ConvertStorage = fun(<<"disc_only">>) -> <<"disc">>; (T) -> T end,
            RetainerConf = convert_fields(
                [ {backend, {'$spec',
                        [ {enable, {'$value', true}}
                        , {type, {'$value', built_in_database}}
                        , {storage_type, {<<"storage_type">>, <<"ram">>, ConvertStorage}}
                        , {max_retained_messages, {<<"max_retained_messages">>, 0}}
                        ]}}
                , {max_payload_size, {<<"max_payload_size">>, <<"1MB">>}}
                , {msg_expiry_interval, {<<"expiry_interval">>, 0}}
                , {stop_publish_clear_msg, {<<"stop_publish_clear_msg">>, false}}
                , {enable, {'$value', IsEnabled}}
                ], Conf),
            OutRawConf#{<<"retainer">> => RetainerConf};
        _ ->
            OutRawConf
    end;
convert_retainer_module(_InputMap, OutRawConf) ->
    OutRawConf.

convert_mqtt_subscriber(#{<<"modules">> := Modules}, OutRawConf0) ->
    case get_modules_by_type(<<"mqtt_subscriber">>, Modules) of
        [#{<<"enabled">> := IsEnabled, <<"config">> := Conf, <<"id">> := _Id0}] ->
            %% Id0 here is always `mqtt_subscriber`
            RandId0 = emqx_data_converter_utils:random_id(6),
            ConnectorName = make_component_name(RandId0, <<"module:">>, <<"c-">>),
            %% EMQX 5.6.0 doesn't support multiple subscriptions in a MQTT source,
            %% so we create a source for each subscription
            {OutConf1, MqttSourceIds} = lists:foldl(fun(SubOpts, {ConfAcc, NameAcc}) ->
                    Topic = maps:get(<<"topic">>, SubOpts),
                    QoS = maps:get(<<"qos">>, SubOpts, 0),
                    MqttSourceConf = #{
                        <<"enable">> => IsEnabled,
                        <<"connector">> => ConnectorName,
                        <<"parameters">> => #{<<"topic">> => Topic, <<"qos">> => QoS}
                    },
                    RandId = emqx_data_converter_utils:random_id(6),
                    SourceId = <<"s-", RandId/binary>>,
                    {add_type_name_conf(<<"sources">>, <<"mqtt">>, SourceId, MqttSourceConf, ConfAcc),
                     [SourceId | NameAcc]}
                end, {OutRawConf0, []}, maps:get(<<"subscription_opts">>, Conf, [])),
            ConnectorConf = convert_mqtt_connector_fields(Conf),
            OutConf2 = add_type_name_conf(<<"connectors">>, <<"mqtt">>, ConnectorName, ConnectorConf, OutConf1),
            %% we also add a rule that republish the topics to local broker.
            RuleName = make_component_name(RandId0, <<"module:">>, <<"r-">>),
            RuleConf = make_source_mqtt_republish_rule(MqttSourceIds),
            add_type_name_conf(<<"rule_engine">>, <<"rules">>, RuleName, RuleConf, OutConf2);
        _ ->
            OutRawConf0
    end;
convert_mqtt_subscriber(_InputMap, OutRawConf) ->
    OutRawConf.

psk_auth(IsEnabled, FileContent, OutRawConf) ->
    case binary:split(FileContent, <<"\n">>, [global, trim_all]) of
        [] ->
            ok;
        PSKs ->
            create_table_lazy(emqx_psk),
            lists:foreach(
              fun(IdentitySec) ->
                      [Identity, Secret] = binary:split(IdentitySec, <<":">>, [trim_all]),
                      ok = mnesia:dirty_write(emqx_psk,
                                              #psk_entry{psk_id = Identity, shared_secret = Secret})
              end,
              PSKs
              )
    end,
    OutRawConf#{<<"psk_authentication">> => #{<<"enable">> => IsEnabled}}.

convert_auth_modules(#{<<"modules">> := Modules} = InputMap, Opts) ->
    Mapping = #{<<"mnesia_authentication">> => fun convert_mnesia_auth/2,
                <<"internal_acl">> => fun convert_file_authz/2,
                <<"redis_authentication">> => fun convert_redis_auth/2,
                <<"pgsql_authentication">> => fun convert_pgsql_auth/2,
                <<"mysql_authentication">> => fun convert_mysql_auth/2,
                <<"mongo_authentication">> => fun convert_mongo_auth/2,
                <<"http_authentication">> => fun convert_http_auth/2,
                <<"jwt_authentication">> => fun convert_jwt_auth/2
               },
    {AuthnList, AuthzList} =
        lists:foldr(
          fun(#{<<"type">> := T} = Mod, {AuthnAcc, AuthzAcc}) when is_map_key(T, Mapping) ->
                  HandlerFun = maps:get(T, Mapping),
                  {Authn, Authz} = HandlerFun(Mod, Opts),
                  {[Authn | AuthnAcc], [Authz | AuthzAcc]};
             (_, Acc) -> Acc
          end,
          {[], []},
          Modules),
    FilterFun = fun(undefined) -> false; (#{}) -> true end,
    EmqxHotConf = hotconf(<<"emqx">>, InputMap),
    AuthnOrder = parse_auth_order(maps:get(<<"auth_order">>, EmqxHotConf, <<"none">>)),
    AuthzOrder = parse_acl_order(maps:get(<<"acl_order">>, EmqxHotConf, <<"none">>)),
    AuthnList1 = sort_by_order(lists:filter(FilterFun, AuthnList), AuthnOrder),
    AuthzList1 = sort_by_order(lists:filter(FilterFun, AuthzList), AuthzOrder),
    DenyAction = maps:get(<<"acl_deny_action">>, EmqxHotConf, <<>>),
    NoMatch = maps:get(<<"acl_nomatch">>, EmqxHotConf, <<>>),
    OutAuthzConf = #{<<"sources">> => AuthzList1},
    OutAuthzConf1 = put_unless_empty(<<"deny_action">>, DenyAction, OutAuthzConf),
    OutAuthzConf2 = put_unless_empty(<<"no_match">>, NoMatch, OutAuthzConf1),
    #{<<"authentication">> => AuthnList1,
      <<"authorization">> => OutAuthzConf2};
convert_auth_modules(_InConf, _Opts) ->
    #{}.

hotconf(Name, InputMap) ->
    case InputMap of
        #{<<"configs">> := Configs} ->
            case lists:filter(fun(#{<<"name">> := N}) -> N =:= Name; (_) -> false end, Configs) of
                [#{<<"confs">> := Confs}] -> Confs;
                _ -> #{}
            end;
        _ -> #{}
    end.

sort_by_order(AuthList, Order) ->
    {SortedByOrder, Rem} =
        lists:foldr(fun({Key, Val}, {SortedAcc, RemAuthL}) ->
                            case lists:filter(fun(#{Key := V}) -> V =:= Val; (_) -> false end, RemAuthL) of
                                [Auth] ->
                                    {[Auth | SortedAcc], lists:delete(Auth, RemAuthL)};
                                _ ->
                                    {SortedAcc, RemAuthL}
                            end
                    end,
                    {[], AuthList},
                    Order),
    SortedByOrder ++ Rem.

parse_auth_order(AuthOrder) ->
    parse_auth_acl_order(fun parse_auth_name/1, AuthOrder).

parse_acl_order(AclOrder) ->
    parse_auth_acl_order(fun parse_acl_name/1, AclOrder).

parse_auth_acl_order(NameParser, CSV) when is_function(NameParser) ->
    do_parse_auth_acl_order(NameParser, string:tokens(str(CSV), ", ")).

do_parse_auth_acl_order(_, []) -> [];
do_parse_auth_acl_order(Parser, ["none" | Names]) ->
    %% "none" is the default config value
    do_parse_auth_acl_order(Parser, Names);
do_parse_auth_acl_order(Parser, [Name0 | Names]) ->
    Name = Parser(Name0),
    [Name | do_parse_auth_acl_order(Parser, Names)].

parse_auth_name(Name) ->
    case parse_auth_name1(Name) of
        {T, N} -> {T, N};
        N -> {<<"backend">>, N}
    end.

parse_auth_name1("http") -> <<"http">>;
parse_auth_name1("jwt") -> {<<"mechanism">>, <<"jwt">>};
parse_auth_name1("mnesia") -> <<"built_in_database">>;
parse_auth_name1("mongodb") -> <<"mongodb">>;
parse_auth_name1("mongo") -> <<"mongodb">>;
parse_auth_name1("mysql") ->  <<"mysql">>;
parse_auth_name1("pgsql") -> <<"postgresql">>;
parse_auth_name1("postgres") -> <<"postgresql">>;
parse_auth_name1("redis") -> <<"redis">>;
parse_auth_name1(Other) -> Other.

parse_acl_name(Name) ->
    {<<"type">>, parse_acl_name1(Name)}.

parse_acl_name1("file") -> <<"file">>;
parse_acl_name1("internal") -> <<"file">>;
parse_acl_name1("http") -> <<"http">>;
parse_acl_name1("mnesia") -> <<"built_in_database">>;
parse_acl_name1("mongo") -> <<"mongodb">>;
parse_acl_name1("mongodb") -> <<"mongodb">>;
parse_acl_name1("mysql") -> <<"mysql">>;
parse_acl_name1("pgsql") -> <<"postgresql">>;
parse_acl_name1("postgres") -> <<"postgresql">>;
parse_acl_name1("redis") -> <<"redis">>;
parse_acl_name1(Other) -> Other.

convert_redis_auth(#{<<"enabled">> := IsEnabled, <<"config">> := InConf}, _Opts) ->
    SSL = convert_ssl_opts(maps:get(<<"ssl">>, InConf, false), InConf),
    #{<<"type">> := RedisType,
      <<"server">> := Server,
      <<"pool_size">> := PoolSize,
      <<"database">> := Database,
      <<"password_hash">> := PasswHash} = InConf,

    OutConf = #{<<"enable">> => IsEnabled,
                <<"redis_type">> => RedisType,
                <<"pool_size">> => PoolSize,
                <<"database">> => Database,
                <<"ssl">> => SSL},
    OutConf1 = case RedisType of
                  <<"single">> ->
                      OutConf#{<<"server">> => Server};
                  <<"cluster">> ->
                       maps:remove(<<"database">>, OutConf#{<<"servers">> => Server});
                  <<"sentinel">> ->
                       SentinelName = maps:get(<<"sentinel">>, InConf),
                       OutConf#{<<"servers">> => Server, <<"sentinel">> => SentinelName}
               end,
    RedisPassw = maps:get(<<"password">>, InConf, <<>>),
    OutConf2 = put_unless_empty(<<"password">>, RedisPassw, OutConf1),

    AclCmd = string:trim(maps:get(<<"acl_cmd">>, InConf, <<>>)),
    AuthnCmd = string:trim(maps:get(<<"auth_cmd">>, InConf, <<>>)),
    SuperCmd = string:trim(maps:get(<<"super_cmd">>, InConf, <<>>)),

    AuthnConf = redis_authn(AuthnCmd, SuperCmd, PasswHash, OutConf2),
    AuthzConf = redis_authz(AclCmd, OutConf2),
    warn_if_no_auth("Redis", AuthnConf, AuthzConf),
    {AuthnConf, AuthzConf}.

redis_authn(<<>> = _AuthnCmd, _SuperCmd, _PasswHash, _Conf) ->
    undefined;
redis_authn(AuthnCmd, SuperCmd, PasswHash, Conf) ->
    case convert_redis_authn_q(AuthnCmd, SuperCmd) of
        undefined ->
            undefined;
        AuthnCmd1 -> Conf#{<<"cmd">> => convert_placeholders(AuthnCmd1),
                           <<"mechanism">> => <<"password_based">>,
                           <<"backend">> => <<"redis">>,
                           <<"password_hash_algorithm">> => convert_passw_hash("Redis", PasswHash)
                          }
    end.

redis_authz(<<>> = _AclCmd, _Conf) ->
    undefined;
redis_authz(AclCmd, Conf) ->
    log_warning(
      "Redis ACL data must be updated manually to be compatible with EMQX 5.1 or later, "
      "the config will be migrated but it won't work in EMQX 5.x if data is not changed, "
      "please see more details at: "
      "https://docs.emqx.com/en/enterprise/v5.1/deploy/upgrade-from-v4.html#redis-1 \n"
     ),
    Conf#{<<"cmd">> => convert_placeholders(AclCmd),
          <<"type">> => <<"redis">>
         }.

is_supported_redis_cmd("HGET") ->
    true;
is_supported_redis_cmd("HMGET") ->
    true;
is_supported_redis_cmd(Cmd) ->
    log_warning(
      "Skipping Redis authentication, as \"auth_cmd\": ~s is not supported by "
      "EMQX 5.x: only HGET and HMGET commands are allowed.", [Cmd]),
    false.

is_supported_redis_fields(Fields) ->
    HasPassHash = lists:member("password_hash", Fields) orelse lists:member("password", Fields),
    HasPassHash
        orelse log_warning("Skipping Redis authentication, as \"auth_cmd\" is not supported "
                           "by EMQX 5.x: fields ~p miss required value: password_hash or "
                           "password.", [Fields]),
    HasPassHash.

convert_redis_authn_q(AuthnQ, SuperQ) ->
    case string:tokens(str(AuthnQ), " ") of
        [Cmd, Key, F | Fs] ->
            Fields = [F | Fs],
            IsSupported = is_supported_redis_cmd(string:uppercase(Cmd))
                andalso is_supported_redis_fields(Fields),
            case IsSupported of
                true ->
                    Fields1 = maybe_add_is_super_redis_field(Cmd, Key, Fields, SuperQ),
                    bin(lists:join(" ", [Cmd, Key, lists:join(" ", Fields1)]));
                false ->
                    undefined
            end;
         _ ->
            log_warning("Skipping Redis authentication, as \"auth_cmd\": ~s is not "
                        "supported by EMQX 5.x. It must use only HGET or HMGET command and include "
                        " password or password_hash field", [AuthnQ]),
            undefined
    end.

maybe_add_is_super_redis_field(_AuthnCmd, _Key, AuthnFields, <<>> = _SuperQ) ->
    AuthnFields;
maybe_add_is_super_redis_field(AuthnCmd, AuthnKey, AuthnFields, SuperQ) ->
    case string:tokens(str(SuperQ), " ") of
        [Cmd, Key, "is_superuser"] ->
            case string:uppercase(Cmd) =:= AuthnCmd andalso Key =:= AuthnKey of
                true ->
                    AuthnFields ++ ["is_superuser"];
                false ->
                    log_warning(
                      "Input Redis auth configuration has superuser query: ~s, "
                      "which is not compatible with EMQX 5.x. If you need to give clients "
                      "super-user permissions, please add  is_superuser field to the Redis "
                      "authentication query command and Redis Hash data manually.",
                      [SuperQ]
                     ),
                    AuthnFields
            end
    end.

convert_pgsql_auth(Module, Opts) ->
    convert_sql_auth("PostgreSQL", Module, Opts).

convert_mysql_auth(Module, Opts) ->
    convert_sql_auth("MySQL", Module, Opts).

convert_sql_auth(DBType, #{<<"enabled">> := IsEnabled, <<"config">> := InConf}, _Opts) ->
    SSL = convert_ssl_opts(maps:get(<<"ssl">>, InConf, false), InConf),
    #{<<"server">> := Server,
      <<"pool_size">> := PoolSize,
      <<"database">> := Database,
      <<"password">> := DBPassw,
      <<"user">> := DBUser,
      <<"password_hash">> := PasswHash} = InConf,
    OutConf = #{<<"enable">> => IsEnabled,
                <<"server">> => Server,
                <<"pool_size">> => PoolSize,
                <<"database">> => Database,
                <<"password">> => DBPassw,
                <<"username">> => DBUser,
                <<"ssl">> => SSL},
    AclQuery =  string:trim(maps:get(<<"acl_query">>, InConf, <<>>)),
    AuthnQuery = string:trim(maps:get(<<"auth_query">>, InConf, <<>>)),
    SuperQuery = string:trim(maps:get(<<"super_query">>, InConf, <<>>)),
    AuthnConf = sql_authn(DBType, AuthnQuery, SuperQuery, PasswHash, OutConf),
    AuthzConf = sql_authz(DBType, AclQuery, OutConf),
    warn_if_no_auth(DBType, AuthnConf, AuthzConf),
    {AuthnConf, AuthzConf}.

%% parses columns and table in basic select queries
parse_sql(Query) ->
    {ok, MP} = re:compile("select\s+(.*)\s+from\s+(.*)\s*(where\s.*)", [caseless]),
    case re:run(string:lowercase(str(Query)), MP, [{capture, all_but_first, list}]) of
        {match, [Columns0, Table, Rem]} ->
            Columns = [string:trim(F) || F <- string:split(Columns0, ",")],
            {ok, {Columns, string:trim(Table), Rem}};
        _ ->
            error
    end.

reconstruct_sql(Columns, Table, Rem) ->
    bin("select " ++ lists:join(", ", Columns) ++ " from " ++ Table ++ " " ++ Rem).

sql_authn(_Type, <<>> = _AuthnQuery, _SuperQuery, _PasswHash, _Conf) ->
    undefined;
sql_authn(Type, AuthnQuery, SuperQuery, PasswHash, Conf) ->
    Query = convert_placeholders(convert_sql_authn_q(Type, AuthnQuery, SuperQuery)),
    Conf#{<<"query">> => Query,
          <<"mechanism">> => <<"password_based">>,
          <<"backend">> => bin(string:lowercase(Type)),
          <<"password_hash_algorithm">> => convert_passw_hash(Type, PasswHash)}.

sql_authz(_Type, <<>> = _AclQuery, _Conf) ->
    undefined;
sql_authz(Type, AclQuery, Conf) ->
    log_warning(
      "~s ACL data and query must be updated manually to be compatible with EMQX 5.1 or later, "
      "the config will be migrated but it won't work in EMQX 5.x if data/query is not changed, "
      "please see more details at: "
      "https://docs.emqx.com/en/enterprise/v5.1/deploy/upgrade-from-v4.html#mysql-postgresql",
      [Type]
     ),
    Conf#{<<"query">> => convert_placeholders(AclQuery),
          <<"type">> => bin(string:lowercase(Type))
         }.

convert_sql_authn_q(Type, AuthnQ, SuperQ) ->
    case parse_sql(AuthnQ) of
        {ok, {Columns, Table, Rem}} ->
            Columns1 = sql_authn_columns(Columns),
            Columns2 = maybe_add_is_super_sql_column(Type, Columns1, Table, SuperQ),
            reconstruct_sql(Columns2, Table, Rem);
        _ ->
            log_warning(
              "Failed to parse ~s authentication query: ~s. "
              "It will be converted but may fail to work properly in EMQX 5.1 or later. "
              "Please update the query and/or data manually according to the documentation: "
              "https://docs.emqx.com/en/enterprise/v5.1/deploy/upgrade-from-v4.html#mysql-postgresql",
              [Type, AuthnQ]
             ),
            AuthnQ
    end.

maybe_add_is_super_sql_column(_Type, Columns, _Table, <<>> = _SuperQ) ->
    Columns;
maybe_add_is_super_sql_column(Type, Columns, Table, SuperQ) ->
    case parse_sql(SuperQ) of
        {ok, {[IsSuper0], Table, _Rem}} ->
            IsSuper = super_column(IsSuper0),
            Columns ++ [IsSuper];
        _ ->
            log_warning("Cannot convert ~s superuser query: ~s. It (probably) uses "
                        "a different table than the main authentication query: ~s.",
                        [Type, SuperQ, Table]),
            Columns
    end.

sql_authn_columns([Passw, Salt]) ->
    [passw_column(Passw), salt_column(Salt)];
sql_authn_columns([Passw]) ->
    [passw_column(Passw)].

passw_column(P) when P =:= "password"; P =:= "password_hash" -> P;
passw_column(P) -> maybe_add_sql_alias(P, "password_hash").

salt_column("salt" = S) -> S;
salt_column(S) -> maybe_add_sql_alias(S, "salt").

super_column("is_superuser" = S) -> S;
super_column(S) -> maybe_add_sql_alias(S, "is_superuser").

maybe_add_sql_alias(Name, Alias) ->
    case string:find(Name, " as ") of
        nomatch -> Name ++ " as " ++ Alias;
        _ -> Name
    end.

convert_mongo_auth(#{<<"enabled">> := IsEnabled, <<"config">> := InConf}, _Opts) ->
    SSL = convert_ssl_opts(maps:get(<<"ssl">>, InConf, false), InConf),
    #{<<"type">> := MongoType,
     <<"pool_size">> := PoolSize,
     <<"server">> := Server,
     <<"database">> := DB,
     <<"srv_record">> := SrvRecord
    } = InConf,
    case mongo_type(MongoType, Server, InConf) of
        undefined ->
            undefined;
        OutConf ->
            OutConf1 = OutConf#{<<"enable">> => IsEnabled,
                                <<"ssl">> => SSL,
                                <<"srv_record">> => SrvRecord,
                                <<"pool_size">> => PoolSize,
                                <<"database">> => DB
                               },
            OptionalConf = #{<<"username">> => maps:get(<<"login">>, InConf, <<>>),
                             <<"password">> => maps:get(<<"password">>, InConf, <<>>),
                             <<"auth_source">> => maps:get(<<"auth_source">>, InConf, <<>>),
                             <<"w_mode">> => maps:get(<<"w_mode">>, InConf, <<>>)},
            OutConf2 = maps:merge(OutConf1, maps:filter(fun(_K, V) -> V =/= <<>> end, OptionalConf)),
            OutConf3 = maybe_add_mongo_topology(InConf, OutConf2),
            {mongo_authn(InConf, OutConf3), mongo_authz(InConf, OutConf3)}
    end.

mongo_authz(InConf, OutMongoConf) ->
    Collection = string:trim(maps:get(<<"acl_query_collection">>, InConf, <<>>)),
    Selectors = maps:get(<<"acl_query_selectors">>, InConf, []),
    case {Collection, Selectors} of
        {C, S} when C =/= <<>>, S =/= [] ->
            log_warning(
              "EMQX 4.4 MongoDB ACL data schema is not compatible with EMQX 5.1 or later. "
              "The configuration will be migrated but ACL data must be updated manually in MongoDB. "
              "Otherwise, MongoDB authorization won't function properly in EMQX 5.x. Please see more details at: "
              "https://docs.emqx.com/en/enterprise/v5.1/deploy/upgrade-from-v4.html#mongodb"
             ),
            Filter = lists:foldl(fun(#{<<"acl_query_selector">> := Sel}, Acc) ->
                                         maps:merge(Acc, convert_mongo_selector(Sel))
                                 end, #{}, Selectors),
            OutMongoConf#{<<"type">> => <<"mongodb">>,
                          <<"collection">> => Collection,
                          <<"filter">> => Filter};
        _ ->
            undefined
    end.

mongo_authn(InConf, OutMongoConf) ->
    Collection = string:trim(maps:get(<<"auth_query_collection">>, InConf, <<>>)),
    PasswHash = string:trim(maps:get(<<"auth_query_password_hash">>, InConf, <<>>)),
    Selector = string:trim(maps:get(<<"auth_query_selector">>, InConf, <<>>)),
    Fields = string:trim(maps:get(<<"auth_query_password_field">>, InConf, <<>>)),
    case Collection of
        <<>> ->
            undefined;
        _ ->
            AuthnConf =
                OutMongoConf#{<<"backend">> => <<"mongodb">>,
                              <<"mechanism">> => <<"password_based">>,
                              <<"password_hash_algorithm">> => convert_passw_hash("MongoDB", PasswHash),
                              <<"collection">> => Collection,
                              <<"filter">> => convert_mongo_selector(Selector)},
            AuthnConf1 = maybe_add_mongo_superuser(InConf, Collection, AuthnConf),
            maps:merge(AuthnConf1, convert_mongo_fields(Fields))
    end.

maybe_add_mongo_superuser(InConf, AuthnCollection, AuthnConf) ->
    SuperCollection = string:trim(maps:get(<<"super_query_collection">>, InConf, <<>>)),
    SuperField = string:trim(maps:get(<<"super_query_field">>, InConf, <<>>)),
    %% Also check superuser selector to match authn selector?
    case {SuperCollection, SuperField} of
        {<<>>, <<>>} ->
            AuthnConf;
        {AuthnCollection, <<>>} ->
            log_info(
              "MongoDB auth \"super_query_field\" field is empty, "
              "default value \"is_superuser\" will be used."),
            AuthnConf#{<<"is_superuser_field">> => <<"is_superuser">>};
        {AuthnCollection, _} ->
            AuthnConf#{<<"is_superuser_field">> => SuperField};
        {SuperCollection, _} ->
            log_warning(
              "Separate MongoDB collection is used as \"super_query_collection\", "
              "which is not compatible with EMQX 5.1 or later. If you need to give clients super-user "
              "permissions in EMQX 5.x, please add is_superuser field to the main authentication "
              "collection."),
            AuthnConf
    end.

convert_mongo_selector(Selector) ->
    [Field, Placeholder] = string:split(Selector, <<"=">>),
    #{Field => convert_placeholders(Placeholder)}.

convert_mongo_fields(Fields) ->
    case [string:trim(F) || F <- string:split(Fields, ",")] of
        [PasswF] ->
            #{<<"password_hash_field">> => PasswF};
        [PasswF, SaltF] ->
            #{<<"password_hash_field">> => PasswF,
              <<"salt_field">> => SaltF}
    end.

mongo_type(<<"unknown">> = _Type, _Server, _InConfig) ->
    %%TODO: add CLI opt for this case?
    log_warning(
      "Skipping MongoDB auth/acl config because it uses \"unknown\" topology, "
      "which is not compatible with EMQX 5.1 or later. Please set a defined MongoDB topology in the input file "
      "and run the converter again."
     ),
    undefined;
mongo_type(<<"single">> = Type, Server, _InConf) ->
    #{<<"server">> => Server,
      <<"mongo_type">> => Type};
mongo_type(<<"rs">> = Type, Server, InConf) ->
    Conf = #{<<"servers">> => Server,
             <<"mongo_type">> => Type},
    put_unless_empty(<<"r_mode">>, maps:get(<<"r_mode">>, InConf, <<>>), Conf);
mongo_type(<<"sharded">> = Type, Server, _InConf) ->
    #{<<"servers">> => Server,
      <<"mongo_type">> => Type}.

maybe_add_mongo_topology(InConf, OutConf) ->
    Topology =
        #{<<"pool_size">> => maps:get(<<"topology_pool_size">>, InConf, <<>>),
          <<"max_overflow">> => maps:get(<<"topology_max_overflow">>, InConf, <<>>),
          <<"overflow_ttl">> => maps:get(<<"topology_overflow_ttl">>, InConf, <<>>),
          <<"overflow_check_period">> => maps:get(<<"topology_overflow_check_period">>, InConf, <<>>),
          <<"local_threshold_ms">> => maps:get(<<"topology_local_threshold_ms">>, InConf, <<>>),
          <<"connect_timeout_ms">> => maps:get(<<"topology_connect_timeout_ms">>, InConf, <<>>),
          <<"socket_timeout_ms">> => maps:get(<<"topology_socket_timeout_ms">>, InConf, <<>>),
          <<"server_selection_timeout_ms">> => maps:get(<<"topology_server_selection_timeout_ms">>, InConf, <<>>),
          <<"wait_queue_timeout_ms">> => maps:get(<<"topology_wait_queue_timeonut_ms">>, InConf, <<>>),
          <<"heartbeat_frequency_ms">> => maps:get(<<"topology_heartbeat_frequency_ms">>, InConf, <<>>),
          <<"min_heartbeat_frequency_ms">> => maps:get(<<"topology_min_heartbeat_frequency_ms">>, InConf, <<>>)
         },
    case maps:filter(fun(_K, V) -> V =/= <<>> end, Topology) of
        EmptyMap when map_size(EmptyMap) =:= 0 ->
            OutConf;
        Map ->
            OutConf#{<<"topology">> => Map}
    end.

convert_http_auth(#{<<"enabled">> := IsEnabled, <<"config">> := InConf}, _Opts) ->
    log_warning(
      "EMQX 4.4 HTTP Auth/ACL service behaviour is not compatible with EMQX 5.1 or later.~n"
      "The configuration will be converted but it won't function properly, "
      "until HTTP service behaviour is updated accordingly, please read the documentation for more details:~n"
      "    https://docs.emqx.com/en/enterprise/v5.1/deploy/upgrade-from-v4.html#http~n"
      "    https://docs.emqx.com/en/enterprise/v5.1/deploy/upgrade-from-v4.html#http-1~n"
      "    https://docs.emqx.com/en/enterprise/v5.1/access-control/authn/http.html#post-request~n"
      "    https://docs.emqx.com/en/enterprise/v5.1/access-control/authz/http.html"),
    SSL = convert_ssl_opts(maps:get(<<"ssl">>, InConf, false), InConf),
    #{<<"pool_size">> := PoolSize,
      <<"method">> := Method,
      <<"req_content_type">> := ContentType
     } = InConf,
    Headers = maps:get(<<"http_headers">>, InConf, #{}),
    Headers1 = Headers#{<<"content-type">> => ContentType},
    OutConf = #{<<"enable">> => IsEnabled,
                <<"method">> => string:lowercase(Method),
                <<"pool_size">> => PoolSize,
                <<"ssl">> => SSL,
                <<"headers">> => Headers1},
    HTTPOpts = #{<<"request_timeout">> => maps:get(<<"http_opts_timeout">>, InConf, <<>>),
                 <<"max_retries">> => maps:get(<<"http_opts_retry_times">>, InConf, <<>>),
                 <<"enable_pipelining">> => maps:get(<<"http_opts_pipelining">>, InConf, <<>>),
                 <<"connect_timeout">> => maps:get(<<"http_opts_connect_timeout">>, InConf, <<>>)
                },
    HTTPOpts1 = maps:filter(fun(_K, V) when is_binary(V) -> string:trim(V) =/= <<>>;
                               (_, _) -> true
                            end, HTTPOpts),
    OutConf1 = maps:merge(OutConf, HTTPOpts1),
    AuthUrl = string:trim(maps:get(<<"auth_req">>, InConf, <<>>)),
    SuperUrl = string:trim(maps:get(<<"super_req">>, InConf, <<>>)),
    AuthParams = string:trim(maps:get(<<"auth_req_params">>, InConf, <<>>)),
    AclUrl = string:trim(maps:get(<<"acl_req">>, InConf, <<>>)),
    AclParams = string:trim(maps:get(<<"acl_req_params">>, InConf, <<>>)),
    {http_authn(AuthUrl, AuthParams, SuperUrl, OutConf1), http_authz(AclUrl, AclParams, OutConf1)}.

http_authn(AuthUrl, AuthParams, SuperUrl, OutConf) when AuthUrl =/= <<>>, AuthParams =/= <<>> ->
    case SuperUrl of
        <<>> -> ok;
        _ ->
            log_warning(
              "EMQX 4.4 HTTP Auth/ACL uses super_req: ~s which is not compatible with EMQX 5.1 or later.~n"
              "If you need to give clients super-user permissions in EMQX 5.x, please add is_superuser "
              "field to the authentication response body.",
              [SuperUrl])
    end,
    OutConf#{<<"mechanism">> => <<"password_based">>,
             <<"backend">> => <<"http">>,
             <<"url">> => AuthUrl,
             <<"body">> => convert_http_req_params(AuthParams, [])};
http_authn(_AuthUrl, _AuthParams, _SuperUrl, _OutConf) ->
    undefined.

http_authz(AclUrl, AclParams, OutConf) when AclUrl =/= <<>>, AclParams =/= <<>> ->
    Placeholders = [{<<"${proto_name}">>, <<"%r">>},
                    %% "%p" -> sockport not suported in EMQX 5.x?
                    {<<"${action}">>, <<"%A">>},
                    {<<"${topic}">>, <<"%t">>},
                    {<<"${mountpoint}">>, <<"%m">>}],
    OutConf#{<<"type">> => <<"http">>,
             <<"url">> => AclUrl,
             <<"body">> => convert_http_req_params(AclParams, Placeholders)};
http_authz(_AclUrl, _AclParams, _OutConf) ->
    undefined.

convert_http_req_params(Params, ExtraPlaceholders) ->
    lists:foldr(
      fun(KV, Acc) ->
              [K, V] = string:split(string:trim(KV), <<"=">>),
              Acc#{K => V}
      end,
      #{},
      string:split(convert_placeholders(Params, ExtraPlaceholders ++ ?PLACEHOLDERS), <<",">>)
     ).

convert_jwt_auth(#{<<"enabled">> := IsEnabled, <<"config">> := InConf}, Opts) ->
    log_warning(
      "JWT authentication module is present in the input file. "
      "It will be converted to EMQX 5.1, but tokens that use EMQX 4.4 placeholders (%u and %c) "
      "in ACL claims are not compatible with EMQX 5.1 or later.~n"
      "For example: ~n"
      "    EMQX 4.4:  {..., \"acl\": {\"pub\": [\"some/stats/%c\"]}, ...}~n"
      "    EMQX 5.1 or later:  {..., \"acl\": {\"pub\": [\"some/stats/${clientid}\"]}, ...}~n"
      "If you use placeholders in token claims, please make sure to update them to EMQX 5.x format:~n"
      "    %u -> ${username}~n"
      "    %c -> ${clientid}"),
     #{<<"verify_claims">> := VerifyClaims,
       <<"secret">> := Secret0,
       <<"jwks_addr">> := JWKSAddr0,
       <<"from">> := From,
       <<"claims">> := Claims,
       <<"acl_claim_name">> := AclClaimName
      } = InConf,
    Secret = string:trim(Secret0),
    PubKey = string:trim(maps:get(<<"file">>, maps:get(<<"pubkey">>, InConf, #{}), <<>>)),
    JWKSAddr = string:trim(JWKSAddr0),

    Conf = jwt_type(Secret, PubKey, JWKSAddr, maps:get(jwt_type, Opts)),
    {Conf#{<<"mechanism">> => <<"jwt">>,
          <<"from">> => From,
          <<"enable">> => IsEnabled,
          <<"acl_claim_name">> => AclClaimName,
          <<"verify_claims">> => jwt_claims(VerifyClaims, Claims)
          },
     undefined}.

jwt_claims(true = _VerifyClaims, Claims) ->
    maps:map(
      fun(_Name, <<"%u">>) -> <<"${username}">>;
         (_Name, <<"%c">>) -> <<"${clientid}">>;
         (_Name, Other) -> Other
      end,
      Claims
     );
jwt_claims(_VerifyClaims, _Claims) ->
    #{}.

jwt_type(Secret, PubKey, JWKAddr, UserJwtType) ->
    case UserJwtType of
        <<"jwks">> when JWKAddr =/= <<>> ->
            jwt_jwks_type(JWKAddr);
        <<"public-key">> when PubKey =/= <<>> ->
            jwt_pubkey_type(PubKey);
        <<"hmac-based">> when Secret =/= <<>> ->
            jwt_hmac_type(Secret);
        undefined ->
            jwt_type(Secret, PubKey, JWKAddr);
        T ->
            log_warning(
              "Requested JWT authentication type ~s is not configured in the input file, "
              "falling back to the default precedence: jwks, public-key, hmac-based",
              [T]),
            jwt_type(Secret, PubKey, JWKAddr)
    end.

jwt_type(Secret, PubKey, JWKAddr) when JWKAddr =/= <<>> ->
    maybe_warn_unused_jwt("JWKS", [{"HMAC based secret", Secret}, {"Public key", PubKey}]),
    jwt_jwks_type(JWKAddr);
jwt_type(Secret, PubKey, _JWKAddr) when PubKey =/= <<>> ->
    maybe_warn_unused_jwt("Public key", [{"HMAC based secret", Secret}]),
    jwt_pubkey_type(PubKey);
jwt_type(Secret, _PubKey, _JWKAddr) when Secret =/= <<>> ->
    jwt_hmac_type(Secret).

jwt_jwks_type(JWKAddr) ->
    #{<<"ssl">> => #{<<"enable">> => false},
      <<"endpoint">> => JWKAddr,
      <<"use_jwks">> => true
     }.

jwt_pubkey_type(PubKey) ->
    #{<<"algorithm">> => <<"public-key">>,
      <<"public_key">> => PubKey,
      <<"use_jwks">> => false}.

jwt_hmac_type(Secret) ->
     #{<<"algorithm">> => <<"hmac-based">>,
      <<"secret">> => Secret,
      <<"use_jwks">> => false}.

maybe_warn_unused_jwt(_UsedType, UnusedList) ->
    %%TODO: improve message
    [log_warning(
       "JWT ~s won't be migrated as EMQX 5.x allows using only one JWT mechanism at a time.",
       [Name]
      ) || {Name, Val} <- UnusedList, Val =/= <<>>].

convert_mnesia_auth(#{<<"enabled">> := IsEnabled, <<"config">> := InConf}, Opts) ->
    UserIdType = case maps:get(user_id_type, Opts) of
                     %% No CLI opt provided, no Mnesia data in the input file to choose the type:
                     %% fallback to username
                     undefined -> <<"username">>;
                     Type -> Type
                 end,
    #{<<"password_hash">> := HashFunName} = InConf,
    AuthnConf = #{<<"enable">> => IsEnabled,
                  <<"backend">> => <<"built_in_database">>,
                  <<"mechanism">> => <<"password_based">>,
                  <<"password_hash_algorithm">> =>
                      #{<<"name">> => HashFunName,
                        <<"salt_position">> => <<"prefix">>},
                  <<"user_id_type">> => UserIdType},
    AuthzConf = #{<<"enable">> => IsEnabled, <<"type">> => <<"built_in_database">>},
    {AuthnConf, AuthzConf}.

warn_if_no_auth(Type, undefined, undefined) ->
    log_warning(
      "Skipping ~s Authn/Acl configuration as both acl query and auth query "
      "\"auth_cmd\" are empty. Such configuration would fail to load in EMQX 5.1 or later version, "
      "as cmd/query is a required field.",
      [Type]);
warn_if_no_auth(_Type, _Authn, _Authz) ->
    ok.

convert_file_authz(#{<<"enabled">> := IsEnabled, <<"config">> := Conf}, Opts) ->
    Authn = undefined,
    Authz = case Conf of
                #{<<"acl_rule_file">> := #{<<"file">> := FileContent}} ->
                    convert_acl_rules(IsEnabled, string:trim(FileContent), Opts);
                _ ->
                    log_warning("Skipping ACL file authorization, as ACL file is missing."),
                    undefined
            end,
    {Authn, Authz}.

convert_acl_rules(_IsEnabled, <<>> = _AclRules, _Opts) ->
    log_warning("Skipping ACL file authorization, as ACL file content is empty."),
    undefined;
convert_acl_rules(IsEnabled, AclRulesBin, Opts) ->
    TmpFile = filename:join(maps:get(output_dir, Opts), "acl.conf"),
    try
        ok = file:write_file(TmpFile, AclRulesBin),
        {ok, AclRules0} = file:consult(TmpFile),
        AclRules = lists:map(
                     fun({Permission, Who, Action, Topics}) ->
                             Rule = {Permission, Who, convert_access_type(Action), convert_topics(Topics)},
                             io_lib:format("~p.~n~n", [Rule]);
                        (AllRule) ->
                             io_lib:format("~p.~n~n", [AllRule])
                     end,
                     AclRules0),
        AclRules1 = iolist_to_binary([?ACL_FILE_COMMENTS | AclRules]),
        #{<<"enable">> => IsEnabled,
          <<"type">> => <<"file">>,
          <<"rules">> => AclRules1}
    catch
        _:Reason:St ->
            log_error(
              "failed to convert ACL file, reason: ~p, stacktrace: ~p",
              [Reason, St]),
            undefined
    after
        file:delete(TmpFile)
    end.

convert_access_type(subscribe) -> subscribe;
convert_access_type(publish) -> publish;
convert_access_type(pubsub) -> all.

convert_topics(Topics) when is_list(Topics) ->
    [convert_topic(T) || T <- Topics];
convert_topics(Topic) ->
    %% EMQX 5 supports lists only
    [convert_topic(Topic)].

convert_topic({eq, Topic}) -> {eq, Topic};
convert_topic(Topic) ->
    lists:foreach(
      fun(UnsupportedTmpl) ->
              case binary:match(bin(Topic), UnsupportedTmpl) of
                  nomatch -> ok;
                  _Found ->
                      log_warning(
                        "ACL file contains \"~s\" placeholder in the topic filter: "
                        "\"~s\", which is not supported in EMQX 5.1 or later. "
                        "ACL file will be migrated but it will require a manual correction "
                        "after importing to EMQX 5.1 or later version in order to function properly.",
                        [UnsupportedTmpl, Topic])
              end
      end,
      [<<"%cna">>, <<"%cida">>]
     ),
    Placeholders = [{<<"${username}">>, <<"%u">>}, {<<"${clientid}">>, <<"%c">>}],
    str(convert_placeholders(bin(Topic), Placeholders)).

convert_placeholders(Str) ->
    convert_placeholders(Str, ?PLACEHOLDERS).

convert_placeholders(Str, Placeholders) ->
    lists:foldl(
      fun({ToTempl, FromTempl}, Acc) ->
              re:replace(Acc, <<"'?", FromTempl/binary, "'?">>, ToTempl, [{return, binary}, global])
      end,
      Str,
      Placeholders).

convert_passw_hash(AuthnName, PassHash) ->
    case binary:split(PassHash, <<",">>) of
        [<<"salt">>, HashFunName] ->
            hash_algoritm(<<"prefix">>, HashFunName, PassHash, AuthnName);
        [HashFunName, <<"salt">>] ->
            hash_algoritm(<<"suffix">>, HashFunName, PassHash, AuthnName);
        [HashFunName] ->
            hash_algoritm(<<"disable">>, HashFunName, PassHash, AuthnName)
    end.

hash_algoritm(_SaltPos, <<"bcrypt">>, PassHash, AuthnName) ->
    log_warning(
      "~s authentication configuration defines bcrypt hash algorithm with salt: \"~s\". "
      "EMQX 5.1 or later ignores salt field stored in external DB for bcrypt and expects the salt to be "
      "a part of password hash field value, as it is usually included in the bcrypt hash-string.",
      [AuthnName, PassHash]),
    #{<<"name">> => <<"bcrypt">>};
hash_algoritm(SaltPos, HashFunName, _PassHash, _AuthnName) ->
    #{<<"name">> => HashFunName,
      <<"salt_position">> => SaltPos}.

convert_ssl_opts(false, _InConf) -> #{<<"enable">> => false};
convert_ssl_opts(true, InConf) ->
    SSL = convert_ssl_files(InConf, #{<<"enable">> => true}),
    Verify = case maps:get(<<"verify">>, InConf, false) of
                 true -> <<"verify_peer">>;
                 false -> <<"verify_none">>
             end,
    SNI = maps:get(<<"server_name_indication">>, InConf, <<>>),
    SSL1 = put_unless_empty(<<"server_name_indication">>, SNI, SSL#{<<"verify">> => Verify}),
    %% Available only for InfluxDB
    add_tls_version(InConf, SSL1).

convert_ssl_files(InConf, SSL) ->
    Keys = [<<"cacertfile">>, <<"certfile">>, <<"keyfile">>],
    lists:foldl(fun(Key, Acc) -> convert_ssl_file(Key, InConf, Acc) end, SSL, Keys).

convert_ssl_file(Key, InConf, SSL) ->
    case InConf of
        #{Key := #{<<"file">> := Content}} when Content /= <<>> ->
            SSL#{Key => Content};
        _ -> SSL
    end.

add_tls_version(InConf, SSL1) ->
    Versions = case InConf of
        #{<<"tls_version">> := TLSVersion} ->
            ssl_versions(TLSVersion);
        #{<<"tls_versions">> := TLSVersionsStr} ->
            ssl_versions(TLSVersionsStr);
        _-> <<>>
    end,
    put_unless_empty(<<"versions">>, Versions, SSL1).

ssl_versions([]) -> <<>>;
ssl_versions(<<>>) -> <<>>;
ssl_versions(Versions) when is_list(Versions), is_binary(hd(Versions)) ->
    {_, AvailableVsns} = lists:keyfind(available, 1, ssl:versions()),
    AvailableVsns1 = [atom_to_binary(A) || A <- AvailableVsns],
    case lists:filter(fun(V) -> lists:member(V, AvailableVsns1) end, Versions) of
        [] -> erlang:error(#{ reason => no_available_tls_version
                            , desired => Versions
                            , available => AvailableVsns1
                            });
        Filtered -> Filtered
    end;
ssl_versions(Versions) when is_binary(Versions) ->
    ssl_versions([list_to_binary(R) || R <- string:lexemes(binary_to_list(Versions), ", ")]).

put_unless_empty(_Key, <<>>, Map) ->
    Map;
put_unless_empty(Key, Val, Map) ->
    Map#{Key => Val}.

str(Data) when is_atom(Data) ->
    atom_to_list(Data);
str(Data) ->
    unicode:characters_to_list(Data).

bin(Data) when is_atom(Data) ->
    atom_to_binary(Data, utf8);
bin(Data) ->
    unicode:characters_to_binary(Data).

local_datetime(MillisecondTs) ->
    calendar:system_time_to_local_time(MillisecondTs, millisecond).

convert_rules_resources(InputMap, OutRawConf) ->
    Rules = maps:get(<<"rules">>, InputMap, []),
    Resources = maps:get(<<"resources">>, InputMap, []),
    {OutRules, OutActions, OutConnectors} =
        lists:foldl(
          fun(Rule, {RulesAcc, ActionsAcc, ConnectorsAcc}) ->
                  {{OutRuleId, OutRule}, ActionsAcc1, ConnectorsAcc1}
                        = convert_rule(Rule, Resources, ActionsAcc, ConnectorsAcc),
                  {RulesAcc#{OutRuleId => OutRule}, ActionsAcc1, ConnectorsAcc1}
          end,
          {#{}, #{}, #{}},
          Rules),
    %% TODO convert resources not bound to any actions, but it may be possible only for a subset of
    %% EMQX 5.1 or later bridges that don't require a CMD
    emqx_data_converter_utils:deep_merge(OutRawConf, #{
        <<"rule_engine">> => #{
            <<"rules">> => OutRules
        },
        <<"actions">> => OutActions,
        <<"connectors">> => OutConnectors
    }).

convert_rule(Rule, Resources, ActionsIn, ConnectorsIn) ->
    #{<<"id">> := Id, <<"rawsql">> := SQL, <<"enabled">> := IsEnabled,
      <<"description">> := Desc, <<"actions">> := Actions} = Rule,
    {OutActionIds, OutActions, OutConnectors} =
        lists:foldr(
          fun(Action, {ActionIdsAcc, ActionsAcc, ConnectorsAcc}) ->
                  case convert_action_resource(Action, Resources) of
                      {standalone_action, ActionOut} ->
                          {[ActionOut | ActionIdsAcc], ActionsAcc, ConnectorsAcc};
                      {OutActionId, OutAction0, OutConnector} ->
                          ActionsIdsAcc1 = [OutActionId | ActionIdsAcc],
                          {ActionType, ActionName, ActionConf0} = OutAction0,
                          #{ action := ActionConf
                           , conn_acc := ConnectorsAcc1
                           } = dedup_connector_config(OutConnector, ActionConf0, ConnectorsAcc),
                          OutAction = {ActionType, ActionName, ActionConf},
                          ActionsAcc1 = add_config_by_type_name(OutAction, ActionsAcc),
                          {ActionsIdsAcc1, ActionsAcc1, ConnectorsAcc1};
                      undefined ->
                          {ActionIdsAcc, ActionsAcc, ConnectorsAcc}
                  end
          end,
          {[], ActionsIn, ConnectorsIn},
          Actions),
    Id1 = binary:replace(Id, <<":">>, <<"_">>, [global]),
    OutRule = {Id1, #{<<"sql">> => SQL,
                      <<"enable">> => IsEnabled,
                      <<"description">> => Desc,
                      <<"actions">> => OutActionIds}},
    {OutRule, OutActions, OutConnectors}.

convert_action_resource(#{<<"name">> := Name, <<"id">> := Id,
                 <<"args">> := #{<<"$resource">> := ResId} = ActArgs} = _Action,
               Resources) ->
    %% NOTE: EMQX 4.4 fallbacks are ignored, log them if they are not empty?
    case resource_by_id(ResId, Resources) of
        #{<<"id">> := ResId, <<"type">> := ResType, <<"config">> := ResConf} = ResParams ->
            case do_convert_action_resource(Name, Id, ActArgs, ResId, ResType, ResConf) of
                {{ActType, ActName, ActConf}, {ConnType, ConnName, ConnConf}} ->
                    OutActionId = <<ActType/binary, ":", ActName/binary>>,
                    {OutActionId,
                        {ActType, ActName, with_common_action_fields(ActArgs, ActConf, ConnName)},
                        {ConnType, ConnName, with_common_connnector_fields(ResParams, ConnConf)}};
                undefined ->
                    undefined
            end;
        undefined ->
            %% drop actions that have bad resource links
            log_warning(
              "Skipping rule action \"~s\", as the resource \"~s\" it referenced is missing in the input file.",
              [Id, ResId]),
            undefined
    end;
convert_action_resource(#{<<"name">> := Name, <<"id">> := Id, <<"args">> := ActArgs} = _Action,
               _Resources) ->
    convert_standlone_action(Name, Id, ActArgs).

convert_standlone_action(<<"republish">>, _ActId, Args) ->
    SetUserProps = fun
        ([]) -> <<>>;
        (_) ->
            %% The user_properties_template in EMQX 4.x are key-value pairs, but in EMQX 5.x we only
            %% support a single ${var}, so we can do nothing but dropping the old values and
            %% set it to ${pub_props.'User-Property'}. We hope it work if the source message contains
            %% the pub_props field, or it's the user's responsibility to update the SQL to make it work.
            <<"${pub_props.'User-Property'}">>
    end,
    ActionConf = #{
        function => <<"republish">>,
        args => convert_fields(
            [ {topic, {<<"target_topic">>, '$required'}}
            , {qos, {<<"target_qos">>, 0}}
            , {retain, {<<"target_retain">>, false}}
            , {payload, {<<"payload_tmpl">>, <<>>, fun convert_payload_tmpl/1}}
            , {mqtt_properties, {<<"mqtt_properties_template">>, [], fun kv_pairs_to_map/1}}
            , {user_properties, {<<"user_properties_template">>, [], SetUserProps}}
            ], Args)
    },
    {standalone_action, ActionConf};
convert_standlone_action(<<"inspect">>, _ActId, Args) ->
    {standalone_action, #{
        function => <<"console">>,
        args => Args
    }}.

with_common_action_fields(ActArgs, ActConf, ConnName) ->
    filter_out_empty(
        ActConf#{
            <<"description">> => maps:get(<<"description">>, ActArgs, <<>>),
            <<"tags">> => maps:get(<<"tag">>, ActArgs, <<>>),
            <<"connector">> => ConnName
        }).

with_common_connnector_fields(ResParams, ConnConf) ->
    filter_out_empty(
        ConnConf#{
            <<"description">> => maps:get(<<"description">>, ResParams, <<>>),
            <<"tags">> => maps:get(<<"tag">>, ResParams, <<>>)
        }).

-define(DATA_ACTION, <<"data_to_", _/binary>>).

do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId,
                          <<"backend_redis_", RedisType/binary>>, ResConf) ->
    #{<<"cmd">> := _Cmd} = Args,
    redis_action_resource(Args, ResId, RedisType, ResConf);
do_convert_action_resource(?DATA_ACTION, ActId, Args, ResId, <<"backend_", RDBMS/binary>>, ResConf)
  when RDBMS =:= <<"pgsql">>;
       RDBMS =:= <<"mysql">>;
       RDBMS =:= <<"sqlserver">>;
       RDBMS =:= <<"oracle">>;
       RDBMS =:= <<"matrix">>;
       RDBMS =:= <<"timescale">> ->
    sqldb_action_resource(RDBMS, ActId, Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId,
                          <<"backend_mongo_", MongoType/binary>>, ResConf) ->
    mongodb_action_resource(Args, ResId, MongoType, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"backend_cassa">>, ResConf) ->
    cassandra_action_resource(Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"backend_clickhouse">>, ResConf) ->
    clickhouse_action_resource(Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"backend_dynamo">>, ResConf) ->
    dynamo_action_resource(Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"backend_hstreamdb">>, ResConf) ->
    hstreamdb_action_resource(Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId,
                          <<"backend_influxdb_http", InfluxVer/binary>>, ResConf) ->
    log_warning(
      "EMQX 5.1 or later InfluxDB bridge has no \"int_suffix\" alternative.~n"
      "If needed, please add necessary suffixes manually to EMQX 5.1 or later \"write_syntax\""),
    influxdb_action_resource(Args, InfluxVer, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"backend_opentsdb">>, ResConf) ->
    opentsdb_action_resource(Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"backend_tdengine">>, ResConf) ->
    tdengine_action_resource(Args, ResId, ResConf);
%%do_convert_action_resource(?DATA_ACTION, ActId, Args, ResId, <<"backend_iotdb">>, ResConf) ->
%%    %% TODO: looks like it may need rewriting rule sql to port it to EMQX5.1
do_convert_action_resource(?DATA_ACTION, ActId, Args, ResId, <<"web_hook">>, ResConf) ->
    webhook_action_resource(ActId, Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"bridge_pulsar">>, ResConf) ->
    pulsar_producer_action_resource(Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"bridge_rabbit">>, ResConf) ->
    rabbit_producer_action_resource(Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, _ActId, Args, ResId, <<"bridge_rocket">>, ResConf) ->
    rocket_producer_action_resource(Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, ActId, Args, ResId, <<"bridge_kafka">>, ResConf) ->
    kafka_action_resource(ActId, Args, ResId, ResConf);
do_convert_action_resource(?DATA_ACTION, ActId, Args, ResId, <<"bridge_gcp_pubsub">>, ResConf) ->
    gcp_pubsub_action_resource(ActId, Args, ResId, ResConf);
%% NOTE that "mqtt_rpc" is not support in EMQX 5.x
do_convert_action_resource(?DATA_ACTION, ActId, Args, ResId, <<"bridge_mqtt">>, ResConf) ->
    mqtt_action_resource(ActId, Args, ResId, ResConf);
do_convert_action_resource(Action, _ActId, _Args, _ResId, ResType, _ResConf) ->
    log_warning(
      "EMQX 4.4 action: ~s and/or resource: ~p are not supported by EMQX 5.6.0 or later "
      "and will be skipped.",
      [Action, ResType]),
    undefined.

%% Dashboard doesn't allow creating an action without resource, this function must be safe
%% to use with a valid input file
resource_by_id(ResId, Resources) ->
    case lists:filter(fun(#{<<"id">> := Id}) -> Id =:= ResId end, Resources) of
        [Resource] -> Resource;
        [] -> undefined
    end.

make_action_name(ResourceId) ->
    make_component_name(ResourceId, <<"resource:">>, <<"a-">>).

make_connector_name(ResourceId) ->
    make_component_name(ResourceId, <<"resource:">>, <<"c-">>).

make_component_name(ResourceId, OldPrefix, Prefix) ->
    ResourceId1 = case string:prefix(ResourceId, OldPrefix) of
                      nomatch -> ResourceId;
                      Id -> Id
                  end,
    ResourceId2 = binary:replace(ResourceId1, <<":">>, <<"_">>, [global]),
    N = integer_to_binary(get_res_id_counter(ResourceId)),
    <<Prefix/binary, ResourceId2/binary, N/binary>>.

%% To handle the same resource having multiple actions.
get_res_id_counter(ResourceId) ->
    case get({?MODULE, counter, ResourceId}) of
        undefined ->
            put({?MODULE, counter, ResourceId}, 1),
            0;
        N ->
            put({?MODULE, counter, ResourceId}, N + 1),
            N
    end.

filter_out_empty(Map) ->
    maps:filter(fun(_K, V) -> V =/= <<>> end, Map).

common_args_to_action_res_opts(Args) ->
    %% Async is default for both 4.4 and 5.1+
    Mode = maps:get(<<"insert_mode">>, Args, <<"async">>),
    ResOpts = #{<<"query_mode">> => Mode},
    case Args of
        #{<<"enable_batch">> := true, <<"batch_size">> := BatchSize} = Conf when BatchSize =/= <<>> ->
            %% batch_time is not converted as it's hidden in EMQX 5
            ResOpts#{
                <<"batch_size">> => BatchSize,
                <<"batch_time">> => maps:get(<<"batch_time">>, Conf, <<"10ms">>)
            };
        _ ->
            ResOpts
    end.

redis_action_resource(#{<<"cmd">> := Cmd} = Args, ResId, RedisType, ResConf) ->
    CommonFields = [<<"server">>, <<"servers">>, <<"pool_size">>,
                    <<"database">>, <<"password">>, <<"sentinel">>],
    ConnParams0 = filter_out_empty(maps:with(CommonFields, ResConf)),
    ConnParams1 = case RedisType of
        <<"cluster">> -> maps:remove(<<"database">>, ConnParams0);
        _  -> ConnParams0
    end,
    ConnParams = ConnParams1#{<<"redis_type">> => RedisType},
    ConnectorConf = #{
        <<"parameters">> => ConnParams,
        <<"ssl">> => convert_ssl_opts(maps:get(<<"ssl">>, ResConf, false), ResConf)
    },
    ActionParams =
        #{ <<"redis_type">> => RedisType
         , <<"command_template">> => [bin(L) || L <- string:lexemes(str(Cmd), " ")]
         },
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"redis">>, make_action_name(ResId), ActionConf},
    Connector = {<<"redis">>, make_connector_name(ResId), ConnectorConf},
    {Action, Connector}.

sqldb_action_resource(RDBMS, _ActionId, #{<<"sql">> := SQL} = Args, ResId, ResConf) ->
    ResConf1 = case ResConf of
                   %% MySQL and Oracle
                   #{<<"user">> := Username} -> ResConf#{<<"username">> => Username};
                   _ -> ResConf
               end,
    CommonFields = [<<"server">>,
                    <<"pool_size">>,
                    <<"database">>,
                    <<"password">>,
                    <<"username">>,
                    <<"driver">>, %% sqlserver
                    <<"sid">>, %% oracle
                    <<"service_name">> %% oracle
                   ],
    ActionConfs = #{
        <<"parameters">> => #{<<"sql">> => SQL},
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    OutConnConf = filter_out_empty(maps:with(CommonFields, ResConf1)),
    OutConnConf1 = maybe_add_ssl_sql(RDBMS, OutConnConf, ResConf1),
    Connector = {RDBMS, make_connector_name(ResId), OutConnConf1},
    Action = {RDBMS, make_action_name(ResId), ActionConfs},
    {Action, Connector}.

maybe_add_ssl_sql(RDBMS, OutConf, _ResConf) when RDBMS =:= <<"oracle">>;
                                                 RDBMS =:= <<"sqlserver">> ->
    OutConf;
maybe_add_ssl_sql(_RDBMS, OutConf, ResConf) ->
    OutConf#{<<"ssl">> => convert_ssl_opts(maps:get(<<"ssl">>, ResConf, false), ResConf)}.

mongodb_action_resource(#{<<"payload_tmpl">> := PayloadTemplate, <<"collection">> := Collection} = Args, ResId, MongoType, ResConf) ->
    ConnParams0 =
        case MongoType of
            <<"single">> ->
                 Params0 = maps:with([<<"servers">>, <<"w_mode">>], ResConf),
                 emqx_data_converter_utils:rename(<<"servers">>, <<"server">>, Params0);
            <<"rs">> ->
                 Params0 = maps:with(
                             [ <<"servers">>
                             , <<"w_mode">>
                             , <<"r_mode">>
                             , <<"rs_set_name">>
                             ], ResConf),
                 emqx_data_converter_utils:rename(<<"rs_set_name">>, <<"replica_set_name">>, Params0);
            <<"sharded">> ->
                 maps:with(
                   [ <<"servers">>
                   , <<"w_mode">>
                   ], ResConf)
        end,
    ConnParams = ConnParams0#{<<"mongo_type">> => MongoType},
    Username = maps:get(<<"login">>, ResConf, <<>>),
    CommonFields = [<<"auth_source">>,
                    <<"pool_size">>,
                    <<"database">>,
                    <<"password">>,
                    <<"srv_record">>],
    ConnConf0 = maps:with(CommonFields, ResConf),
    ConnConf1 = ConnConf0#{
        <<"username">> => Username,
        <<"ssl">> => convert_ssl_opts(maps:get(<<"ssl">>, ResConf, false), ResConf)},
    ConnConf2 = case ResConf of
                   #{<<"connectTimeoutMS">> := Timeout} when is_integer(Timeout) ->
                       TimeoutBin = <<(integer_to_binary(Timeout))/binary, "ms">>,
                       ConnConf1#{<<"topology">> => #{<<"connect_timeout_ms">> => TimeoutBin}};
                   _ -> ConnConf1
               end,
    ConnConf = ConnConf2#{<<"parameters">> => ConnParams},
    Connector = {<<"mongodb">>, make_connector_name(ResId), ConnConf},
    ActionParams = #{
        <<"collection">> => Collection,
        <<"payload_template">> => PayloadTemplate
    },
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"mongodb">>, make_action_name(ResId), ActionConf},
    {Action, Connector}.

cassandra_action_resource(#{<<"sql">> := SQL} = Args, ResId, #{<<"nodes">> := Servers} = ResConf) ->
    ActionParams = #{<<"cql">> => SQL},
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"cassandra">>, make_action_name(ResId), ActionConf},
    CommonFields = [<<"keyspace">>,
                    <<"password">>,
                    <<"pool_size">>,
                    <<"username">>],
    ConnConf0 = filter_out_empty(maps:with(CommonFields, ResConf)),
    ConnConf = ConnConf0#{
        <<"servers">> => Servers,
        <<"ssl">> => convert_ssl_opts(maps:get(<<"ssl">>, ResConf, false), ResConf)},
    Connector = {<<"cassandra">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

clickhouse_action_resource(#{<<"sql">> := SQL} = Args, ResId, #{<<"server">> := URL} = ResConf) ->
    ActionParams = #{<<"sql">> => SQL},
    ActionConf = #{<<"parameters">> => ActionParams,
                   <<"resource_opts">> => common_args_to_action_res_opts(Args)},
    Action = {<<"clickhouse">>, make_action_name(ResId), ActionConf},
    CommonFields = [<<"database">>,
                    <<"pool_size">>],
    Passw = maps:get(<<"key">>, ResConf, <<>>),
    Username = maps:get(<<"user">>, ResConf, <<>>),
    ConnConf0 = maps:with(CommonFields, ResConf),
    ConnConf = ConnConf0#{<<"password">> => Passw,
                        <<"username">> => Username,
                        <<"url">> => URL},
    Connector = {<<"clickhouse">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

dynamo_action_resource(Args, ResId, ResConf) ->
    ActionParams =
        maps:with(
          [ <<"table">>
          , <<"hash_key">>
          , <<"range_key">>
          ],
          Args),
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"dynamo">>, make_action_name(ResId), ActionConf},
    CommonFields = [<<"pool_size">>,
                    <<"url">>,
                    <<"region">>,
                    <<"aws_access_key_id">>,
                    <<"aws_secret_access_key">>],
    ConnConf = filter_out_empty(maps:with(CommonFields, ResConf)),
    Connector = {<<"dynamo">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

hstreamdb_action_resource(Args, ResId, #{<<"server">> := URL} = ResConf) ->
    #{<<"stream">> := Stream, <<"payload_tmpl">> := PayloadTempl} = Args,
    ActionParams = filter_out_empty(#{
        <<"stream">> => Stream,
        <<"record_template">> => PayloadTempl,
        <<"aggregation_pool_size">> => maps:get(<<"aggregation_pool_size">>, Args, <<>>),
        <<"writer_pool_size">> => maps:get(<<"writer_pool_size">>, Args, <<>>),
        <<"partition_key">> => maps:get(<<"partitioning_key">>, Args, <<>>)
    }),
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"hstreamdb">>, make_action_name(ResId), ActionConf},
    GRPCTimeout = case ResConf of
                      #{<<"grpc_timeout">> := T} when is_integer(T) ->
                          <<(integer_to_binary(T))/binary, "ms">>;
                      _ ->
                          <<>>
                  end,
    ConnConf1 =
        #{<<"url">> => URL,
          <<"grpc_timeout">> => GRPCTimeout,
          <<"ssl">> => convert_ssl_opts(maps:get(<<"ssl">>, ResConf, false), ResConf)},
    ConnConf = filter_out_empty(ConnConf1),
    Connector = {<<"hstreamdb">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

influxdb_action_resource(Args, InfluxVer, ResId, #{<<"host">> := H, <<"port">> := P} = ResConf) ->
    #{<<"measurement">> := Measurement,
      <<"tags">> := Tags,
      <<"fields">> := Fields,
      <<"timestamp">> := Ts
     } = Args,
    TagsBin = influx_fields_bin(Tags),
    FieldsBin = influx_fields_bin(Fields),
    WriteSyntax0 = maybe_append_influx_tags(Measurement, TagsBin),
    WriteSyntax = string:trim(<<WriteSyntax0/binary, " ", FieldsBin/binary, " ", Ts/binary>>),
    ActionParams0 = maps:with([<<"precision">>], ResConf),
    ActionParams = ActionParams0#{<<"write_syntax">> => WriteSyntax},
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"influxdb">>, make_action_name(ResId), ActionConf},
    SSL = convert_ssl_opts(maps:get(<<"ssl">>, ResConf, false),
                           ResConf#{<<"ssl">> => maps:get(<<"https_enabled">>, ResConf, false)}),
    ConnParams =
        case InfluxVer of
            <<>> ->
                ConnParams0 = maps:with([<<"database">>, <<"password">>, <<"username">>], ResConf),
                ConnParams0#{<<"influxdb_type">> => <<"influxdb_api_v1">>};
            <<"_v2">> ->
                ConnParams0 = maps:with([<<"org">>, <<"token">>, <<"bucket">>], ResConf),
                ConnParams0#{<<"influxdb_type">> => <<"influxdb_api_v2">>}
        end,
    ConnConf = #{
        <<"parameters">> => ConnParams,
        <<"server">> => <<H/binary, ":", (integer_to_binary(P))/binary>>,
        <<"ssl">> => SSL
    },
    Connector = {<<"influxdb">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

maybe_append_influx_tags(Measurement, <<>> = _Tags) ->
    Measurement;
maybe_append_influx_tags(Measurement, Tags) ->
    <<Measurement/binary, ",", Tags/binary>>.
influx_fields_bin(FieldsOrTags) ->
    %% TODO: quote string literals in field values?
    bin(lists:join(<<",">>, [<<K/binary, "=", V/binary>> || {K, V} <- maps:to_list(FieldsOrTags)])).

opentsdb_action_resource(Args, ResId, ResConf) ->
    %% opentsdb's only action parameter is `data', which has no 4.4 equivalent...
    ActionConf = #{
        <<"parameters">> => #{},
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"opents">>, make_action_name(ResId), ActionConf},
    ConnConf = maps:merge(maps:with([<<"summary">>, <<"details">>], Args),
                         maps:with([<<"server">>, <<"pool_size">>], ResConf)),
    Connector = {<<"opents">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

tdengine_action_resource(#{<<"sql">> := SQL} = Args, ResId, #{<<"host">> := H, <<"port">> := P} = ResConf) ->
    ActionParams = filter_out_empty(#{
        <<"database">> => maps:get(<<"db_name">>, Args, <<>>),
        <<"sql">> => SQL
    }),
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"tdengine">>, make_action_name(ResId), ActionConf},
    CommonFields = [<<"pool_size">>,
                    <<"password">>,
                    <<"username">>],
    ConnConf0 = maps:with(CommonFields, ResConf),
    ConnConf1 = ConnConf0#{<<"server">> => <<H/binary, ":", (integer_to_binary(P))/binary>>},
    ConnConf = filter_out_empty(ConnConf1),
    Connector = {<<"tdengine">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

webhook_action_resource(_ActionId, Args, ResId, ResConf) ->
    AllConfsIn = maps:merge(Args, ResConf),
    RequestTTL = maps:get(<<"request_timeout">>, AllConfsIn, <<"5s">>),
    ActionConn = #{
        <<"parameters">> => convert_fields(
            [ {method, {<<"method">>, <<"POST">>, fun string:lowercase/1}}
            , {headers, {<<"headers">>, #{}}}
            , {body, {<<"body">>, <<>>, fun convert_payload_tmpl/1}}
            , {path, {<<"path">>, <<>>}}
            ], AllConfsIn),
        <<"resource_opts">> => #{<<"request_ttl">> => RequestTTL}
    },
    IsSslEnabled = infer_ssl_from_uri(maps:get(<<"url">>, ResConf)),
    ConnConf1 = convert_fields(
        [ {connect_timeout, {<<"connect_timeout">>, <<"5s">>}}
        , {pool_size, {<<"pool_size">>, 8}}
        , {url, {<<"url">>, '$required'}}
        , {enable_pipelining, {<<"enable_pipelining">>, false, fun(false) -> 1; (true) -> 100 end}}
        , {ssl, {'$value', convert_ssl_opts(IsSslEnabled, ResConf)}}
        ], ResConf),
    Action = {<<"http">>, make_action_name(ResId), ActionConn},
    Connector = {<<"http">>, make_connector_name(ResId), filter_out_empty(ConnConf1)},
    {Action, Connector}.

pulsar_producer_action_resource(#{<<"topic">> := Topic} = Args, ResId, #{<<"servers">> := Servers} = ResConf) ->
    Key = key_to_template(maps:get(<<"key">>, Args, <<>>)),
    Tmpl = maps:get(<<"payload_tmpl">>, Args, <<>>),
    Msg = put_unless_empty(<<"value">>, Tmpl, #{<<"key">> => Key}),
    Buffer = filter_out_empty(
               #{<<"mode">> => buffer_mode(maps:get(<<"buffer_mode">>, Args, <<>>)),
                 <<"segment_bytes">> => maps:get(<<"segment_bytes">>, Args, <<>>),
                 <<"per_partition_limit">> => maps:get(<<"max_total_bytes">>, Args, <<>>)
                }),
    ActionParams0 =
        maps:with(
          [ <<"sync_timeout">>
          , <<"compression">>
          , <<"send_buffer">>
          , <<"batch_size">>
          ],
          ResConf
         ),
    ActionParams = filter_out_empty(ActionParams0#{
        <<"message">> => Msg,
        <<"pulsar_topic">> => Topic,
        <<"buffer">> => Buffer,
        <<"retention_period">> => maps:get(<<"retention_period">>, Args, <<>>),
        <<"strategy">> => maps:get(<<"strategy">>, Args, <<>>)
    }),
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"pulsar">>, make_action_name(ResId), ActionConf},
    Authn = case maps:get(<<"authentication_type">>, ResConf, <<"none">>) of
                <<"none">> ->
                    <<"none">>;
                <<"basic">> ->
                    [User, Pass] = binary:split(maps:get(<<"secret">>, ResConf), <<":">>),
                    #{<<"username">> => User, <<"password">> => Pass};
                <<"token">> ->
                    #{<<"jwt">> => maps:get(<<"jwt">>, ResConf)}
            end,
    IsSslEnabled = lists:any(fun(B) -> B =:= true end,
        [infer_ssl_from_uri(Uri) || Uri <- string:lexemes(Servers, ", ")]),
    ConnConf1 = #{<<"servers">> => Servers,
                  <<"authentication">> => Authn,
                  <<"ssl">> => convert_ssl_opts(IsSslEnabled, ResConf)
                 },
    ConnConf = filter_out_empty(ConnConf1),
    Connector = {<<"pulsar">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

%% Pulsar, Kafka producer
key_to_template(<<"none">>) -> '$absent';
%% TODO: what if the rule doesn't select it?
key_to_template(<<"topic">>) -> <<"${.topic}">>;
key_to_template(<<"clientid">>) -> <<"${.clientid}">>;
key_to_template(<<"username">>) -> <<"${.username}">>;
key_to_template(Other) -> Other.

buffer_mode(<<"Memory">>) -> <<"memory">>;
buffer_mode(<<"Disk">>) -> <<"disk">>;
buffer_mode(<<"Memory+Disk">>) -> <<"hybrid">>;
buffer_mode(<<>>) -> '$absent'.

rabbit_producer_action_resource(Args, ResId, #{<<"server">> := Server} = ResConf) ->
    #{<<"exchange">> := Exchange, <<"routing_key">> := RoutingKey} = Args,
    DeliveryMode = case maps:get(<<"durable">>, Args, false) of
        true -> <<"persistent">>;
        false -> <<"non_persistent">>
    end,
    ActionParams = filter_out_empty(#{
        <<"routing_key">> => RoutingKey,
        <<"exchange">> => Exchange,
        <<"payload_template">> => maps:get(<<"payload_tmpl">>, Args, <<>>),
        <<"delivery_mode">> => DeliveryMode
    }),
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"rabbitmq">>, make_action_name(ResId), ActionConf},
    {Host, Port} = case binary:split(Server, <<":">>) of
                       [H, P] -> {H, binary_to_integer(P)};
                       [H] -> {H, <<>>}
                   end,
    CommonFields = [<<"username">>, <<"password">>, <<"pool_size">>,
                    <<"virtual_host">>, <<"timeout">>, <<"heartbeat">>],
    ConnConf0 = maps:with(CommonFields, ResConf),
    ConnConf1 = ConnConf0#{<<"server">> => Host, <<"port">> => Port},
    ConnConf = filter_out_empty(ConnConf1),
    Connector = {<<"rabbitmq">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

rocket_producer_action_resource(Args, ResId, ResConf) ->
    ActionParams0 =
        maps:with(
          [ <<"send_buffer">>
          , <<"refresh_interval">>
          , <<"sync_timeout">>
          ],
          ResConf),
    ActionParams = filter_out_empty(ActionParams0#{
        <<"topic">> => maps:get(<<"topic">>, Args),
        <<"template">> => maps:get(<<"payload_tmpl">>, Args, <<>>),
        <<"strategy">> => get_and_maybe_warn_not_supported(
                            "RocketMQ bridge",
                            <<"strategy">>,
                            maps:get(<<"strategy">>, Args, <<>>),
                            [<<>>, <<"roundrobin">>])
    }),
    get_and_maybe_warn_not_supported("RocketMQ bridge", "key", maps:get(<<"key">>, ResConf, <<>>)),
    ActionConf = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => common_args_to_action_res_opts(Args)
    },
    Action = {<<"rocketmq">>, make_action_name(ResId), ActionConf},
    CommonFields =
        [ <<"servers">>
        , <<"secret_key">>
        , <<"access_key">>
        , <<"security_token">>
        , <<"namespace">>
        ],
    ConnConf = maps:with(CommonFields, ResConf),
    Connector = {<<"rocketmq">>, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

kafka_action_resource(_ActionId, Args, ResId, ResConf) ->
    ConnectorType = <<"kafka_producer">>,
    ConvertStrategy = fun
        (<<"roundrobin">>) ->
            log_warning(
              "Round-robin partition strategy is not supported by Kafka producer "
              "bridge in EMQX 5.1 or later. It will use \"random\" strategy by default."),
            <<"random">>;
        (S) -> S
    end,
    ConvertKafkaExtHeaders = fun(OldHeaders) ->
        lists:map(fun(#{<<"key">> := Key, <<"value">> := Val}) ->
            #{<<"kafka_ext_header_key">> => Key, <<"kafka_ext_header_value">> => Val}
        end, OldHeaders)
    end,
    ConvertTcpKeepalive = fun
        (<<>>) -> '$absent';
        (Val) ->
            case hocon_postprocess:duration(Val) of
                0 -> '$absent';
                I when is_integer(I) ->
                    ISecBin = integer_to_binary(ceiling(I / 1000)),
                    %% Default probes is 3, see 4.4 emqx_bridge_kafka_actions.erl: tcp_keepalive/1
                    <<ISecBin/binary, ",", ISecBin/binary, ",3">>;
                _ -> '$absent'
            end
    end,
    %% Drop the following fields as they are for 4.4 only, drop them silently:
    %%     "message_accumulation_interval",
    %%     "message_accumulation_size",
    %%     "message_accumulation_keep_msg_order",
    %%     "message_accumulation_drop_factor"
    AllConfsIn = maps:merge(Args, ResConf),
    ActionParams = convert_fields(
        [ {required_acks, {<<"required_acks">>, <<"all_isr">>}}
        , {topic, {<<"topic">>, '$required'}}
        , {partition_count_refresh_interval, {<<"partition_count_refresh_interval">>, <<"60s">>}}
        , {kafka_headers, {<<"kafka_headers">>, '$absent'}}
        , {compression, {<<"compression">>, <<"no_compression">>}}
        , {sync_query_timeout, {<<"sync_timeout">>, <<"3s">>}}
        , {max_batch_bytes, {<<"max_batch_bytes">>, <<"900KB">>}}
        , {query_mode, {<<"type">>, <<"async">>}}
        , {partition_strategy, {<<"strategy">>, <<"random">>, ConvertStrategy}}
        , {kafka_header_value_encode_mode,
            {<<"kafka_header_value_encode_mode">>, <<"NONE">>, fun string:lowercase/1}}
        , {kafka_ext_headers, {<<"kafka_ext_headers">>, [], ConvertKafkaExtHeaders}}
        , {buffer, {'$spec',
            [ {segment_bytes, {<<"segments_bytes">>, <<"100MB">>}}
            , {mode, {<<"cache_mode">>, <<"Memory">>, fun buffer_mode/1}}
            , {memory_overload_protection, {<<"highmem_drop">>, <<"false">>}}
            , {per_partition_limit, {<<"max_total_bytes">>, <<"2GB">>}}
            ]}}
        , {message, {'$spec',
            [ {key, {<<"key">>, <<"none">>, fun key_to_template/1}}
            , {value, {<<"payload_tmpl">>, <<>>, fun convert_payload_tmpl/1}}
            ]}}
        ], AllConfsIn),
    ConnConf = convert_fields(
        [ {bootstrap_hosts, {<<"servers">>, '$required'}}
        , {authentication, {<<"authentication_mechanism">>, not_found, fun kafka_auth/2}}
        , {min_metadata_refresh_interval, {<<"min_metadata_refresh_interval">>, <<"3s">>}}
        , {ssl, {<<"ssl">>, false, fun convert_ssl_opts/2}}
        , {socket_opts,
            {'$spec',
                [ {sndbuf, {<<"send_buffer">>, <<"1024KB">>}}
                , {tcp_keepalive, {<<"tcp_keepalive">>, <<"0s">>, ConvertTcpKeepalive}}
                ]}}
        ], AllConfsIn),
    Action = {ConnectorType, make_action_name(ResId), #{<<"parameters">> => ActionParams}},
    Connector = {ConnectorType, make_connector_name(ResId), ConnConf},
    {Action, Connector}.

gcp_pubsub_action_resource(_ActionId, Args, ResId, ResConf) ->
    ConnectorType = <<"gcp_pubsub_producer">>,
    AllConfsIn = maps:merge(Args, ResConf),
    ConnConfs = convert_fields(
        [ {connect_timeout, {<<"connect_timeout">>, <<"5s">>}}
        , {request_timeout, {<<"request_timeout">>, <<"5s">>}}
        , {pool_size, {<<"pool_size">>, 8}}
        , {pipelining, {<<"pipelining">>, 100}}
        , {service_account_json, {<<"service_account_json">>, '$required', fun file_content/1}}
        ], AllConfsIn),
    ActionParams = convert_fields(
        [ {pubsub_topic, {<<"pubsub_topic">>, '$required'}}
        , {payload_template, {<<"payload_template">>, <<"${.}">>}}
        , {attributes_template, {<<"attributes">>, #{}, fun map_to_kv_pairs/1}}
        , {ordering_key_template, {<<"ordering_key">>, '$absent'}}
        ], AllConfsIn),
    ActionResOpts = convert_fields(
        [ {query_mode, {<<"flush_mode">>, <<"sync">>}}
        , {request_ttl, {<<"sync_timeout">>, <<"5s">>}}
        , {worker_pool_size, {<<"batch_pool_size">>, 4}}
        , {batch_size, {<<"batch_size">>, 100}}
        , {batch_time, {<<"flush_period_ms">>, <<"10ms">>}}
        ], AllConfsIn),
    ActionConfs = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => ActionResOpts
    },
    Connector = {ConnectorType, make_connector_name(ResId), ConnConfs},
    Action = {ConnectorType, make_action_name(ResId), ActionConfs},
    {Action, Connector}.

mqtt_action_resource(_ActionId, Args, ResId, ResConf) ->
    ConnectorType = <<"mqtt">>,
    BufferMode = fun(<<"on">>) -> <<"volatile_offload">>; (<<"off">>) -> <<"memory_only">> end,
    MakeTopic = fun(ForwardTopic0, ConfIn) ->
        ForwardTopic = case ForwardTopic0 of
            <<>> ->
                %% NOTE: inheriting topic from the source MQTT message are not supported in EMQX 5.x,
                %%   i.e. setting "topic = ${topic}" may or may not work as expected.
                <<"${topic}">>;
            _ -> ForwardTopic0
        end,
        MountPoint = maps:get(<<"mountpoint">>, ConfIn, <<>>),
        topic_prepend(MountPoint, ForwardTopic)
    end,
    ConnConfs = convert_mqtt_connector_fields(ResConf),
    %% NOTE: inheriting 'qos', 'retain' from the source MQTT message are not supported in EMQX 5.x,
    %%   i.e. setting "qos = ${qos}" may or may not work as expected.
    ConvertQoS = fun
        (<<"inherited_from_source_msg">>) -> <<"${qos}">>;
        (<<"0">>) -> 0; (<<"1">>) -> 1; (<<"2">>) -> 2 end,
    ActionParams = convert_fields(
        [ {topic, {<<"forward_topic">>, <<>>, MakeTopic}}
        , {qos, {<<"qos">>, <<"inherited_from_source_msg">>, ConvertQoS}}
        , {payload, {<<"payload_template">>, <<>>, fun convert_payload_tmpl/1}}
        ], maps:merge(Args, ResConf)),
    ActionResOpts = convert_fields(
        [ {buffer_mode, {<<"disk_cache">>, <<"off">>, BufferMode}}
        ], ResConf),
    ActionConfs = #{
        <<"parameters">> => ActionParams,
        <<"resource_opts">> => ActionResOpts
    },
    Connector = {ConnectorType, make_connector_name(ResId), ConnConfs},
    Action = {ConnectorType, make_action_name(ResId), ActionConfs},
    {Action, Connector}.

convert_mqtt_connector_fields(ConfIn) ->
    ConvertMqttVsn = fun (<<"mqttv3">>) -> <<"v3">>; (<<"mqttv4">>) -> <<"v4">>;
        (<<"mqttv5">>) -> <<"v5">> end,
    %% Following fields are not supported in EMQX 5.x, drop them silently:
    %%  - append
    %%  - reconnect_interval
    convert_fields(
        [ {server, {<<"address">>, '$required'}}
        , {pool_size, {<<"pool_size">>, 8}}
        , {clientid_prefix, {<<"clientid">>, <<"client">>}}
        , {username, {<<"username">>, '$absent'}}
        , {password, {<<"password">>, '$absent'}}
        , {proto_ver, {<<"proto_ver">>, <<"mqttv4">>, ConvertMqttVsn}}
        , {keepalive, {<<"keepalive">>, <<"60s">>}}
        , {retry_interval, {<<"retry_interval">>, <<"20s">>}}
        , {bridge_mode, {<<"bridge_mode">>, false}}
        , {ssl, {<<"ssl">>, false, fun convert_ssl_opts/2}}
        ], ConfIn).

%% =============================================================================
%% Helper functions
%% =============================================================================
ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

kafka_auth(<<"NONE">>, _ResConf) -> <<"none">>;
kafka_auth(AuthMethod, ResConf) when
        AuthMethod =:= not_found;
        AuthMethod =:= <<"PLAIN">> ->
    case maps:get(<<"username">>, ResConf, <<>>) of
        <<>> -> <<"none">>;
        _ ->
            UserPass = maps:with([<<"username">>, <<"password">>], ResConf),
            UserPass#{<<"mechanism">> => <<"plain">>}
    end;
kafka_auth(AuthMethod, ResConf) when
        AuthMethod =:= <<"SCRAM_SHA_256">>;
        AuthMethod =:= <<"SCRAM_SHA_512">> ->
    A = maps:with([<<"username">>, <<"password">>], ResConf),
    A#{<<"mechanism">> => string:lowercase(AuthMethod)};
kafka_auth(<<"KERBEROS">>, ResConf) ->
    #{<<"kerberos_principal">> := Principal} = ResConf,
    case ResConf of
        #{<<"kerberos_keytab_path">> := KeyTabPath} ->
            log_warning(
              "Kafka uses Kerberos authnetication, please make sure that "
              "keytab file is copied to EMQX 5, EMQX 4.4 file path: ~s",
              [KeyTabPath]),
            #{<<"kerberos_keytab_file">> => KeyTabPath,
              <<"kerberos_principal">> => Principal};
        #{<<"kerberos_keytab">> := #{<<"filename">> := Filename, <<"file">> := _}} ->
            log_warning(
              "The input file contains Kerberos keytab file \"~s\" content "
              "for Kafka bridge authentication, which can't be migrated to EMQX 5.1 or later.~n"
              "The bridge config will be migrated without authentication, "
              "Please create keytab files on EMQX 5.1 (or later) nodes "
              "and add their paths in Kafka bridge config manually.",
              [Filename]),
            '$absent'
    end.

get_and_maybe_warn_not_supported(ResourceDesc, Key, Val) ->
    get_and_maybe_warn_not_supported(ResourceDesc, Key, Val, [<<>>]).
get_and_maybe_warn_not_supported(ResourceDesc, Key, Val0, Supported) ->
    Val = string:trim(Val0),
    case lists:member(Val, Supported) of
        true ->
            Val;
        false ->
            log_warning(
              "Resource: \"~s\" has field: \"~s\"=\"~s\", which is not supported "
              "in EMQX 5.1 or later",
              [ResourceDesc, Key, Val]),
            <<>>
    end.

infer_ssl_from_uri(Url) when is_binary(Url) ->
    case uri_string:normalize(Url) of
        {error, Class, Reason} ->
            throw({invalid_url, {Url, Class, Reason}});
        URIStr ->
            case uri_string:parse(URIStr) of
                #{scheme := <<"https">>} -> true;
                #{scheme := <<"hstreams">>} -> true;
                #{scheme := <<"pulsar+ssl">>} -> true;
                #{scheme := _} -> false
            end
    end.

%% Deduplicate if the exact same configuration exists
dedup_connector_config({ConnType, _ConnName, ConnConf} = Connector, ActionConf0, ConnAcc0) ->
    maybe
        #{ConnType := NamesToConfs} ?= ConnAcc0,
        {ok, SameConfigName} ?= find_same_config(ConnConf, NamesToConfs),
        ActionConf = ActionConf0#{<<"connector">> := SameConfigName},
        #{action => ActionConf, conn_acc => ConnAcc0}
    else
        _ ->
            ConnAcc = add_config_by_type_name(Connector, ConnAcc0),
            #{action => ActionConf0, conn_acc => ConnAcc}
    end.

find_same_config(Conf, NamesToConfs) ->
    WithSameConf = [Name
                    || {Name, Conf0} <- maps:to_list(NamesToConfs),
                       Conf0 == Conf],
    case WithSameConf of
        [] ->
            error;
        [Name] ->
            {ok, Name}
    end.

add_config_by_type_name({Type, Name, Conf}, MapIn) ->
    UpdateFun = fun(ConfByType) -> ConfByType#{Name => Conf} end,
    maps:update_with(Type, UpdateFun, #{Name => Conf}, MapIn).

convert_fields(Spec, ConfInList) when is_list(ConfInList) ->
    convert_fields(Spec, lists:foldl(fun(ConfIn, ConfOut) ->
            maps:merge(ConfOut, ConfIn)
        end, #{}, ConfInList));

convert_fields(Spec, ConfIn) when is_map(ConfIn) ->
    lists:foldl(
        fun ({NewKey, {'$value', Value}}, ConfOut) ->
                ConfOut#{NewKey => Value};
            ({NewKey, {'$spec', SubSpec}}, ConfOut) ->
                ConfOut#{NewKey => convert_fields(SubSpec, ConfIn)};
            ({NewKey, {OldKey, Default}}, ConfOut) ->
                do_convert_fields(NewKey, OldKey, Default, undefined, ConfIn, ConfOut);
            ({NewKey, {OldKey, Default, ConvertFun}}, ConfOut) ->
                do_convert_fields(NewKey, OldKey, Default, ConvertFun, ConfIn, ConfOut)
        end, #{}, Spec).

do_convert_fields(NewKey, OldKey, Default, ConvertFun, ConfIn, ConfOut) ->
    case maps:get(OldKey, ConfIn, Default) of
        '$absent' -> ConfOut;
        '$required' ->
            throw({missing_required_fields, #{missing_key => OldKey, config => ConfIn}});
        Val when is_function(ConvertFun) ->
            ConvertedVal = case erlang:fun_info(ConvertFun, arity) of
                {arity, 1} -> ConvertFun(Val);
                {arity, 2} -> ConvertFun(Val, ConfIn);
                _ -> throw({invalid_converter, arity_should_be_1_or_2})
            end,
            case ConvertedVal of
                '$absent' -> ConfOut;
                ConvertedVal -> ConfOut#{NewKey => ConvertedVal}
            end;
        Val ->
            ConfOut#{NewKey => Val}
    end.

kv_pairs_to_map(KvList) when is_list(KvList) ->
    lists:foldl(fun(#{<<"key">> := K, <<"value">> := V}, Acc) ->
            maps:put(K, V, Acc)
        end, #{}, KvList).
map_to_kv_pairs(Map) when is_map(Map) ->
    lists:map(fun({K, V}) ->
            #{<<"key">> => K, <<"value">> => V}
        end, maps:to_list(Map)).

file_content(#{<<"file">> := Content}) when is_binary(Content) ->
    Content;
file_content(#{<<"file">> := Content}) when is_map(Content) ->
    jsone:decode(Content).

convert_payload_tmpl(<<>>) -> <<"${.}">>;
convert_payload_tmpl(Tmpl) -> Tmpl.

%% copied from emqx_topic.erl
topic_prepend(undefined, W) -> bin(W);
topic_prepend(<<>>, W) -> bin(W);
topic_prepend(Parent0, W) ->
    Parent = bin(Parent0),
    case binary:last(Parent) of
        $/ -> <<Parent/binary, (bin(W))/binary>>;
        _ -> <<Parent/binary, $/, (bin(W))/binary>>
    end.

get_modules_by_type(Type, Modules) ->
    lists:filter(fun
            (#{<<"type">> := T}) when T =:= Type -> true;
            (_) -> false
        end, Modules).

add_type_name_conf(Component, Type, Name, Conf, ConfIn) ->
    Components = maps:get(Component, ConfIn, #{}),
    TypedComponents = maps:get(Type, Components, #{}),
    ConfIn#{
        Component => Components#{
            Type => TypedComponents#{Name => Conf}
        }
    }.

make_source_mqtt_republish_rule(MqttSourceIds) ->
    QuotedTopics = [<<"\"$bridges/mqtt:", Id/binary, "\"">> || Id <- MqttSourceIds],
    Topics = list_to_binary(lists:join(<<",">>, QuotedTopics)),
    #{
        <<"sql">> => <<"SELECT * FROM ", Topics/binary>>,
        <<"enable">> => true,
        <<"description">> => <<"Republish messages from MQTT sources to local topics">>,
        <<"actions">> => [#{
            <<"function">> => <<"republish">>,
            <<"args">> => #{
                <<"topic">> => <<"${topic}">>,
                <<"qos">> => <<"${qos}">>,
                <<"retain">> => <<"${retain}">>,
                <<"payload">> => <<"${payload}">>,
                <<"user_properties">> => <<"${pub_props.'User-Property'}">>
            }
        }]
    }.

convert_to_message_5(?message_4_4(Id, QoS, From, Flags, Headers, Topic, Payload, Timestamp)) ->
   ?message_5(Id, QoS, From, Flags, Headers, Topic, Payload, Timestamp, #{}).

log_info(Msg) ->
    log_info(Msg, []).

log_info(Msg, Args) ->
    log(info, Msg, Args).

log_warning(Msg) ->
    log_warning(Msg, []).

log_warning(Msg, Args) ->
    log(warning, Msg, Args).

log_error(Msg) ->
    log_error(Msg, []).

log_error(Msg, Args) ->
    log(error, Msg, Args).

log(Level0, Msg, Args) ->
    LevelTag = "[" ++ string:uppercase(atom_to_list(Level0)) ++ "] ",
    io:format(LevelTag ++ Msg ++ "\n\n", Args).
