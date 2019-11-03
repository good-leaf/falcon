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
	type
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
	spawn(?MODULE, timer_handle, [convert_state(State)]),
	_TS = chronos:start_timer(State#state.ntimer,
		State#state.ntimer, RTime * 1000,
		{?MODULE, timer_expiry, [Metric]}),
	{reply, ok, State};
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
convert_state(#state{metric = Metric, type = Type, report_time = RTime, module = Module,
	function = Function, args = Args}) ->
	#{
		metric => Metric,
		type => Type,
		report_time => RTime,
		module => Module,
		function => Function,
		args => Args
	}.

timer_handle(#{metric := Metric, type := Type, report_time := RTime, module := Module,
	function := Function, args := Args}) ->
	try
		reflush_cache(Metric, RTime, Type, RTime),
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

reflush_cache(Metric, RTime, Type, RTime) ->
	CurrentTime = timestamp() - 2 * RTime,
	EKey = gen_key(Metric, CurrentTime),
	case count:get(EKey) of
		[] ->
			ok;
		[{_Key, Value}] ->
			case Type of
				<<"incr">> ->
					reflush_data(EKey, Value, <<"INCRBY">>, RTime);
				<<"incrby">> ->
					reflush_data(EKey, Value, <<"INCRBY">>, RTime);
				<<"set">> ->
					reflush_data(EKey, Value, <<"SET">>, RTime);
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
	send(post, ?FALCON_URL, [], jsx:encode(Body), ?FALCON_OPTION, ?FALCON_RETRY).

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

reflush_data(Key, Value, Cmd, RTime) ->
	fredis:qpexcute_retry([[Cmd, Key, Value], ["EXPIRE", Key, 10 * RTime]], ?FALCON_REDIS_RETRY).

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
	%考虑集群中节点上报周期差值，当前事件上报三个周期前的数据
	LastTimeStamp = timestamp() - 3 * ReportTime,
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