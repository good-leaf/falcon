-module(report_timer).

-behaviour(gen_server).

-include("../include/falcon.hrl").
%% API
-export([start_link/6]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-compile(export_all).

-export([
    gen_key/1,
    gen_key/2,
    timer_expiry/1,
    default_callback/3
]).

-define(SERVER, ?MODULE).

-record(state, {
    metric,
    ntimer,
    report_time,
    module,
    function,
    args,
    type,
    local_data = #{}
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Metric :: binary(), Type :: binary(), ReportTime :: integer(), CallBackModule :: atom(), CallBackFun :: integer(), CallBackArgs :: list()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Metric, Type, ReportTime, CallBackModule, CallBackFun, CallBackArgs) ->
    AtomMetric = binary_to_atom(Metric, utf8),
    gen_server:start_link({local, AtomMetric}, ?MODULE, [Metric, Type, ReportTime, CallBackModule, CallBackFun, CallBackArgs], []).

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
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Metric, Type, ReportTime, CallBackModule, CallBackFun, CallBackArgs]) ->
    NameTimer = binary_to_atom(<<"falcon_", Metric/binary>>, utf8),
    {ok, _Pid} = chronos:start_link(NameTimer),
    _TS = chronos:start_timer(NameTimer, NameTimer, ReportTime * 1000,
        {?MODULE, timer_expiry, [Metric]}),
    {ok, #state{metric = Metric, type = Type, ntimer = NameTimer, report_time = ReportTime, module = CallBackModule, function = CallBackFun, args = CallBackArgs}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({timer_expiry, Metric}, _From, #state{report_time = RTime} = State) ->
    error_logger:info_msg("timer_expiry for ~p~n", [State]),
    timer_handle(State),
    _TS = chronos:start_timer(State#state.ntimer,
        State#state.ntimer, RTime * 1000,
        {?MODULE, timer_expiry, [Metric]}),
    {reply, ok, State#state{local_data = #{}}};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({incr, Metric}, #state{local_data = LocalData} = State) ->
    Key = gen_key(Metric),
    Value = maps:get(Key, LocalData, 0),
    {noreply, State#state{type = <<"incr">>, local_data = LocalData#{Key => Value + 1}}};
handle_cast({incr, Metric, TimeStamp}, #state{local_data = LocalData} = State) ->
    Key = gen_key(Metric, TimeStamp),
    Value = maps:get(Key, LocalData, 0),
    {noreply, State#state{type = <<"incr">>, local_data = LocalData#{Key => Value + 1}}};
handle_cast({incrby, Metric, Value}, #state{local_data = LocalData} = State) ->
    Key = gen_key(Metric),
    OldValue = maps:get(Key, LocalData, 0),
    {noreply, State#state{type = <<"incrby">>, local_data = LocalData#{Key => Value + OldValue}}};
handle_cast({incrby, Metric, Value, TimeStamp}, #state{local_data = LocalData} = State) ->
    Key = gen_key(Metric, TimeStamp),
    OldValue = maps:get(Key, LocalData, 0),
    {noreply, State#state{type = <<"incrby">>, local_data = LocalData#{Key => Value + OldValue}}};
handle_cast({set, Metric, Value}, #state{local_data = LocalData} = State) ->
    Key = gen_key(Metric),
    {noreply, State#state{type = <<"set">>, local_data = LocalData#{Key => Value}}};
handle_cast({set, Metric, Value, TimeStamp}, #state{local_data = LocalData} = State) ->
    Key = gen_key(Metric, TimeStamp),
    {noreply, State#state{type = <<"set">>, local_data = LocalData#{Key => Value}}};
handle_cast(_Request, State) ->
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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
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
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

timer_expiry(Metric) ->
    ServerName = binary_to_atom(Metric, utf8),
    gen_server:call(whereis(ServerName), {timer_expiry, Metric}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
timer_handle(#state{metric = Metric, type = Type, report_time = RTime, module = Module, function = Function, args = Args}) ->
    try
        reflush_cache(Metric, RTime, Type),
        case cfalcon:check_leader() of
            true ->
                {TimeStamp, Value, Tags} = Module:Function(Metric, RTime, Args),
                report(Metric, TimeStamp, Value, generate_tags(Tags), RTime);
            false ->
                ignore
        end
    catch
        E:R ->
            error_logger:error_msg("timer_handle error:~p, reason:~p, bt:~p", [E, R, erlang:get_stacktrace()])
    end.

%%reflush(#state{type = Type, local_data = LocalData}) ->
%%    case Type of
%%        <<"incr">> ->
%%            lists:foreach(fun({K, V}) -> reflush_data(K, V, "INCRBY") end, maps:to_list(LocalData));
%%        <<"incrby">> ->
%%            lists:foreach(fun({K, V}) -> reflush_data(K, V, "INCRBY") end, maps:to_list(LocalData));
%%        <<"set">> ->
%%            lists:foreach(fun({K, V}) -> reflush_data(K, V, "SET") end, maps:to_list(LocalData));
%%        _Type ->
%%            ok
%%    end.

reflush_cache(Metric, RTime, Type) ->
    CurrentTime = timestamp() - RTime,
    EKey = gen_key(Metric, CurrentTime),
    case count:get(EKey) of
        [] ->
            ok;
        [{_Key, Value}] ->
            case Type of
                <<"incr">> ->
                    reflush_data(EKey, Value, <<"INCRBY">>);
                <<"incrby">> ->
                    reflush_data(EKey, Value, <<"INCRBY">>);
                <<"set">> ->
                    reflush_data(EKey, Value, <<"SET">>);
                _Type ->
                    ok
            end,
            count:del(EKey)
    end.



-spec report(Metric :: binary(), TimeStamp :: integer(), Value :: integer(), Tags :: binary(), Step :: integer()) -> any().
report(Metric, TimeStamp, Value, Tags, Step) ->
    Body = [
        [
            {metric, Metric},
            {timestamp, TimeStamp},
            {counterType, <<"GAUGE">>},
            {endpoint, endpoint(?ENDPOINT)},
            {step, Step},
            {tags, Tags},
            {value, Value}
        ]
    ],
    send(post, ?FALCON_URL, [], jsx:encode(Body), [], ?FALCON_RETRY).

endpoint(undefined) ->
    host_name();
endpoint(EndPoint) ->
    EndPoint.

host_name() ->
    case application:get_env(falcon, host_name, <<>>) of
        <<>> ->
            {ok, Host} = inet:gethostname(),
            case ?IS_SHORT_HOSTNAME of
                true ->
                    [SHost | _IP] = string:tokens(Host, "@"),
                    application:set_env(falcon, host_name, list_to_binary(SHost));
                false ->
                    application:set_env(falcon, host_name, list_to_binary(Host))
            end,
            application:get_env(falcon, host_name, <<>>);
        HostName ->
            HostName
    end.

send(_Method, _Url, _Headers, _Body, _Options, 0) ->
    ok;
send(Method, Url, Headers, Body, Options, Retry) ->
    case httpc:request(Method, {Url, Headers, "application/json", Body}, Options, [{body_format, binary}]) of
        {ok, {{_Version, 200, _Msg}, _Server, Response}} ->
            error_logger:info_msg("send url:~p, body:~p, res_body:~p", [Url, Body, Response]);
        {ok, Result} ->
            error_logger:error_msg("send url:~p, body:~p, result:~p", [Url, Body, Result]),
            send(Method, Url, Headers, Body, Options, Retry - 1);
        {error, Error} ->
            error_logger:error_msg("send url:~p, body:~p, error:~p", [Url, Body, Error]),
            send(Method, Url, Headers, Body, Options, Retry - 1)
    end.

to_binary(X) when is_integer(X) -> integer_to_binary(X);
to_binary(X) when is_binary(X) -> X;
to_binary(X) when is_float(X) -> float_to_binary(X);
to_binary(X) when is_atom(X) -> atom_to_binary(X, utf8);
to_binary(X) when is_list(X) ->
    case io_lib:printable_list(X) of
        true ->
            list_to_binary(X);
        false ->
            jsx:encode(X)
    end.

generate_tags(List) ->
    lists:foldl(fun(T, <<>>) ->
        {K, V} = T,
        BK = to_binary(K),
        BV = to_binary(V),
        <<BK/binary, "=", BV/binary>>;
        (T, Acc) ->
            {K, V} = T,
            BK = to_binary(K),
            BV = to_binary(V),
            <<Acc/binary, ",", BK/binary, "=", BV/binary>> end, <<>>, List).

%指标统计 自增
falcon_incr(Key) ->
    NewKey = gen_key(Key),
    fredis:qpexcute_retry([["INCR", NewKey], ["EXPIRE", NewKey, ?KEY_EXPRIRED]], ?FALCON_REDIS_RETRY).

falcon_incr(Key, TimeStamp) ->
    NewKey = gen_key(Key, TimeStamp),
    fredis:qpexcute_retry([["INCR", NewKey], ["EXPIRE", NewKey, ?KEY_EXPRIRED]], ?FALCON_REDIS_RETRY).

%指标统计 累加
falcon_incrby(Key, Value) ->
    NewKey = gen_key(Key),
    fredis:qpexcute_retry([["INCRBY", NewKey, Value], ["EXPIRE", NewKey, ?KEY_EXPRIRED]], ?FALCON_REDIS_RETRY).

falcon_incrby(Key, Value, TimeStamp) ->
    NewKey = gen_key(Key, TimeStamp),
    fredis:qpexcute_retry([["INCRBY", NewKey, Value], ["EXPIRE", NewKey, ?KEY_EXPRIRED]], ?FALCON_REDIS_RETRY).

%指标统计 更新
falcon_set(Key, Value) ->
    NewKey = gen_key(Key),
    fredis:qpexcute_retry([["SET", NewKey, Value], ["EXPIRE", NewKey, ?KEY_EXPRIRED]], ?FALCON_REDIS_RETRY).

falcon_set(Key, Value, TimeStamp) ->
    NewKey = gen_key(Key, TimeStamp),
    fredis:qpexcute_retry([["SET", NewKey, Value], ["EXPIRE", NewKey, ?KEY_EXPRIRED]], ?FALCON_REDIS_RETRY).

reflush_data(Key, Value, Cmd) ->
    fredis:qpexcute_retry([[Cmd, Key, Value], ["EXPIRE", Key, ?KEY_EXPRIRED]], ?FALCON_REDIS_RETRY).

date_format() ->
    Now = erlang:system_time(micro_seconds),
    BaseDate = calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
    Time = calendar:gregorian_seconds_to_datetime(BaseDate + Now div 1000000),
    {{Y, M, D}, {H, Mi, _S}} = calendar:universal_time_to_local_time(Time),
    list_to_binary(io_lib:format("~4..0b~2..0b~2..0b~2..0b~2..0b", [Y, M, D, H, Mi])).

date_format(TimeStamp) ->
    Now = TimeStamp * 1000000,
    BaseDate = calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
    Time = calendar:gregorian_seconds_to_datetime(BaseDate + Now div 1000000),
    {{Y, M, D}, {H, Mi, _S}} = calendar:universal_time_to_local_time(Time),
    list_to_binary(io_lib:format("~4..0b~2..0b~2..0b~2..0b~2..0b", [Y, M, D, H, Mi])).

get_key_prefix() ->
    case application:get_env(falcon, key_prefix, undefined) of
        undefined ->
            [Service, _Info] = binary:split(atom_to_binary(node(), utf8), <<"@">>),
            application:set_env(falcon, key_prefix, Service),
            application:get_env(falcon, key_prefix, <<>>);
        Prefix ->
            Prefix
    end.

gen_key(Key) ->
    Prefix = get_key_prefix(),
    Date = date_format(),
    <<Prefix/binary, "_", Key/binary, "_", Date/binary>>.

gen_key(Key, TimeStamp) ->
    Prefix = get_key_prefix(),
    Date = date_format(TimeStamp),
    <<Prefix/binary, "_", Key/binary, "_", Date/binary>>.

default_callback(Metric, ReportTime, Args) ->
    LastTimeStamp = timestamp() - 2 * ReportTime,
    SaveKey = gen_key(Metric, LastTimeStamp),
    {ok, Value} = fredis:excute_retry(["GET", SaveKey], ?FALCON_REDIS_RETRY),
    {LastTimeStamp, convert_value(Value), Args}.

timestamp() ->
    {M, S, _MS} = os:timestamp(),
    M * 1000000 + S.

convert_value(undefined) ->
    0;
convert_value(Value) ->
    binary_to_integer(Value).