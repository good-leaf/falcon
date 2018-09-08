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
    metric_register/4,
    metric_register/6
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
    erlang:send_after(10, ?SERVER, node_ping),
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
handle_info(node_ping, State) ->
    node_ping(),
    erlang:send_after(?PING_TIMEVAL, ?SERVER, node_ping),
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
-spec metric_register(binary(), binary(), integer(), atom(), atom(), list()) -> any().
metric_register(Metric, Type, ReportTime, CallBackModule, CallBackFun, CallBackArgs) ->
    AtomMetric = binary_to_atom(Metric, utf8),
    case whereis(AtomMetric) of
        undefined ->
            report_sup:start_child(Metric, Type, ReportTime, CallBackModule, CallBackFun, CallBackArgs);
        Pid when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true ->
                    ok;
                false ->
                    report_sup:start_child(Metric, Type, ReportTime, CallBackModule, CallBackFun, CallBackArgs)
            end
    end.

-spec metric_register(binary(), binary(), integer(), list()) -> any().
metric_register(Metric, Type, ReportTime, CallBackArgs) ->
    metric_register(Metric, Type, ReportTime, report_timer, default_callback, CallBackArgs).

gen_key(Metric) ->
    report_timer:gen_key(Metric).

gen_key(Metric, TimeStamp) ->
    report_timer:gen_key(Metric, TimeStamp).

incr(Metric) ->
    count:incr(gen_key(Metric, timestamp())).

incr(Metric, TimeStamp) ->
    count:incr(gen_key(Metric, TimeStamp)).

incrby(Metric, Value) ->
    count:incrby(gen_key(Metric, timestamp()), Value).

incrby(Metric, Value, TimeStamp) ->
    count:incrby(gen_key(Metric, TimeStamp), Value).

set(Metric, Value) ->
    count:set(gen_key(Metric, timestamp()), Value).

set(Metric, Value, TimeStamp) ->
    count:set(gen_key(Metric, TimeStamp), Value).
%%%===================================================================
%%% Internal functions
%%%===================================================================
node_ping() ->
    try
        Nodes = get_nodes(),
        AliveNodes = lists:foldl(fun(N, Acc) -> case net_adm:ping(N) of
                                    pong ->
                                        [N | Acc];
                                    R ->
                                        error_logger:warning_msg("node_ping:~p, result:~p, ", [N, R]),
                                        Acc
                                end end,[], Nodes),
        application:set_env(falcon, alive_nodes, AliveNodes)
    catch
        E:R ->
            error_logger:error_msg("node_ping error:~p, reason:~p", [E, R])
    end.

get_nodes() ->
    [NodePrefix | _NodeTail] = binary:split(atom_to_binary(node(),utf8), <<"@">>),
    lists:foldl(fun(RemoteNode, Acc) ->
        [RemoteNodePrefix | _RemoteNodeTail] = binary:split(atom_to_binary(RemoteNode,utf8), <<"@">>),
        case RemoteNodePrefix == NodePrefix of
            true ->
                [RemoteNode | Acc];
            false ->
                Acc
        end end, [], lists:usort(?FALCON_NODES ++ nodes())).

check_leader() ->
    Nodes = application:get_env(falcon, alive_nodes, []),
    node() == lists:last(lists:usort([node() | Nodes])).

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
    test_incrby(<<"incrby", Key/binary>>, 2),
    test_set(<<"set", Key/binary>>, 3).

test() ->
    test(<<>>).

test_incr(Metric) ->
    metric_register(Metric, <<"incr">>, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    incr(Metric, timestamp()).

test_incrby(Metric, Value) ->
    metric_register(Metric, <<"incrby">>, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    incrby(Metric, Value).

test_set(Metric, Value) ->
    metric_register(Metric, <<"set">>, 60, report_timer, default_callback, [{<<"env">>, <<"test">>}]),
    set(Metric, Value, timestamp()).





