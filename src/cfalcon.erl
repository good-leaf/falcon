-module(cfalcon).

-behaviour(gen_server).

%% API
-export([start_link/0]).

-include("../include/falcon.hrl").

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([
    report/5,
    check_leader/0,
    callback/1,
    metric_register/5
]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init([]) ->
    erlang:send_after(10, ?SERVER, node_register),
    {ok, #state{}}.

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
handle_info(node_register, State) ->
    node_register(),
    erlang:send_after(?REG_TIMEVAL, ?SERVER, node_register),
    {noreply, State};
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
node_register() ->
    try
        Node = atom_to_binary(node(), utf8),
        eredis_cluster:q(["SETEX", <<"falcon_", Node/binary>>, ?REDIS_NODE_EXPIRED, Node])
    catch
        E:R  ->
            error_logger:error_msg("node_register error:~p, reason:~p", [E, R])
    end.

check_leader() ->
    try
        Result = eredis_cluster:qa(["KEYS", <<"falcon_*">>]),
        Node = atom_to_binary(node(), utf8),
        <<"falcon_", Node/binary>> == lists:last(lists:sort(lists:foldl(fun(Tuple, Acc) -> {ok, L} = Tuple, Acc ++ L end, [], Result)))
    catch
        E:R  ->
            error_logger:error_msg("check_leader error:~p, reason:~p", [E, R]),
            false
    end.

-spec report(Metric::binary(), TimeStamp::integer(), Value::integer(), Tags::binary(), Step::integer()) -> any().
report(Metric, TimeStamp, Value, Tags, Step) ->
    Body = [
        {metric, Metric},
        {timestamp, TimeStamp},
        {counterType, <<"GAUGE">>},
        {endpoint, endpoint(?ENDPOINT)},
        {step, Step},
        {tags, Tags},
        {value, Value}
    ],
    send(post, ?FALCON_URL, [], jsx:encode(Body), [{timeout, 2000}, {connect_timeout, 1000}], ?RETRY).

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
                    [SHost|_IP] = string:tokens(Host, "@"),
                    application:set_env(falcon, host_name, list_to_binary(SHost));
                false ->
                    application:set_env(falcon, host_name, list_to_binary(Host))
            end,
            application:get_env(falcon, host_name);
        HostName ->
            HostName
    end.

send(_Method, _Url, _Headers, _Body, _Options, 0) ->
    ok;
send(Method, Url, Headers, Body, Options, Retry) ->
    case httpc:request(Method, {Url, Headers, "application/json", Body}, Options, [{body_format, binary}]) of
        {ok, {{_Version,200, _Msg},_Server, Response}} ->
            error_logger:info_msg("send url:~p, body:~p, res_body:~p", [Url, Body, Response]);
        {ok, Result} ->
            error_logger:error_msg("send url:~p, body:~p, result:~p", [Url, Body, Result]),
            send(Method, Url, Headers, Body, Options, Retry -1);
        {error, Error} ->
            error_logger:error_msg("send url:~p, body:~p, error:~p", [Url, Body, Error]),
            send(Method, Url, Headers, Body, Options, Retry -1)
    end.

%ReportTime 秒
-spec metric_register(atom(), integer(), atom(), atom(), list()) -> any().
metric_register(TServer, ReportTime, CallBackModule, CallBackFun, CallBackArgs) ->
    report_sup:start_child(TServer, ReportTime, CallBackModule, CallBackFun, CallBackArgs).

callback(CallBackArgs) ->
    Tags = lists:foldl(fun(T, <<>>) ->
        {K, V} = T,
        BK = to_binary(K),
        BV = to_binary(V),
        <<BK/binary, "=", BV/binary>>;
        (T, Acc) ->
            {K, V} = T,
            BK = to_binary(K),
            BV = to_binary(V),
            <<Acc/binary, ",", BK/binary, "=", BV/binary>> end, <<>>, CallBackArgs),
    {<<"metric_name">>, timestamp(), 100, Tags}.

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

timestamp() ->
    {M, S, _MS} = os:timestamp(),
    M * 1000000 + S.