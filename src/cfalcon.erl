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
    incr/1,
    incr/2,
    incrby/2,
    incrby/3,
    set/2,
    set/3,
    gen_key/1,
    gen_key/2,
    check_leader/0,
    metric_register/3,
    metric_register/5
]).

-export([
    test/0,
    test/1,
    ptest/1]).
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

%ReportTime 秒
-spec metric_register(binary(), integer(), atom(), atom(), list()) -> any().
metric_register(Metric, ReportTime, CallBackModule, CallBackFun, CallBackArgs) ->
    report_sup:start_child(Metric, ReportTime, CallBackModule, CallBackFun, CallBackArgs).

-spec metric_register(binary(), integer(), list()) -> any().
metric_register(Metric, ReportTime, CallBackArgs) ->
    metric_register(Metric, ReportTime, report_timer, default_callback, CallBackArgs).

gen_key(Metric) ->
    report_timer:gen_key(Metric).

gen_key(Metric, TimeStamp) ->
    report_timer:gen_key(Metric, TimeStamp).

incr(Metric) ->
    ServerName = binary_to_atom(Metric, utf8),
    gen_server:cast(whereis(ServerName), {incr, Metric}).

incr(Metric, TimeStamp) ->
    ServerName = binary_to_atom(Metric, utf8),
    gen_server:cast(whereis(ServerName), {incr, Metric, TimeStamp}).

incrby(Metric, Value) ->
    ServerName = binary_to_atom(Metric, utf8),
    gen_server:cast(whereis(ServerName), {incrby, Metric, Value}).

incrby(Metric, Value, TimeStamp) ->
    ServerName = binary_to_atom(Metric, utf8),
    gen_server:cast(whereis(ServerName), {incrby, Metric, Value, TimeStamp}).

set(Metric, Value) ->
    ServerName = binary_to_atom(Metric, utf8),
    gen_server:cast(whereis(ServerName), {set, Metric, Value}).

set(Metric, Value, TimeStamp) ->
    ServerName = binary_to_atom(Metric, utf8),
    gen_server:cast(whereis(ServerName), {set, Metric, Value, TimeStamp}).
%%%===================================================================
%%% Internal functions
%%%===================================================================
node_register() ->
    try
        Node = atom_to_binary(node(), utf8),
        fredis:excute_retry(["SETEX", <<"falcon_", Node/binary>>, ?REDIS_NODE_EXPIRED, Node], ?FALCON_REDIS_RETRY)
    catch
        E:R ->
            error_logger:error_msg("node_register error:~p, reason:~p", [E, R])
    end.

check_leader() ->
    try
        case ?ENDPOINT of
            undefined ->
                %按照节点上报
                true;
            _EndPoint ->
                [Service, _Info] = binary:split(atom_to_binary(node(), utf8), <<"@">>),
                Result = eredis_cluster:qa(["KEYS", <<"falcon_", Service/binary, "*">>]),
                Node = atom_to_binary(node(), utf8),
                <<"falcon_", Node/binary>> == lists:last(lists:sort(lists:foldl(fun(Tuple, Acc) -> {ok, L} = Tuple,
                    Acc ++ L end, [], Result)))
        end
    catch
        E:R ->
            error_logger:error_msg("check_leader error:~p, reason:~p", [E, R]),
            false
    end.

timestamp() ->
    {M, S, _MS} = os:timestamp(),
    M * 1000000 + S.

umilltimestamp() ->
    {M, S, MS} = os:timestamp(),
    M * 1000000000000 + S * 1000000 + MS.

generate_uuid() ->
    Bin = crypto:strong_rand_bytes(12),
    Time = integer_to_binary(umilltimestamp()),
    NewBin = <<Bin/binary, Time/binary>>,
    Sig = erlang:md5(NewBin),
    iolist_to_binary([io_lib:format("~2.16.0b", [S]) || S <- binary_to_list(Sig)]).

%事例
ptest(Num) ->
    Fun = fun(_N) ->
        spawn(?MODULE, test, [generate_uuid()])
          end,
    [Fun(N) || N <- lists:seq(1, Num)].

test(Key) ->
    test_incr(<<"incr", Key/binary>>),
    test_incr1(<<"incr1", Key/binary>>),
    test_incrby(<<"incrby", Key/binary>>, 100),
    test_incrby1(<<"incrby1", Key/binary>>, 200),
    test_set(<<"set", Key/binary>>, 300),
    test_set1(<<"set1", Key/binary>>, 400).

test() ->
    test(<<>>).

test_incr(Metric) ->
    metric_register(Metric, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    incr(Metric).

test_incr1(Metric) ->
    metric_register(Metric, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    incr(Metric, timestamp()).

test_incrby(Metric, Value) ->
    metric_register(Metric, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    incrby(Metric, Value).

test_incrby1(Metric, Value) ->
    metric_register(Metric, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    incrby(Metric, Value, timestamp()).

test_set(Metric, Value) ->
    metric_register(Metric, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    set(Metric, Value).

test_set1(Metric, Value) ->
    metric_register(Metric, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    set(Metric, Value, timestamp()).





