-module(report_timer).

-behaviour(gen_server).

%% API
-export([start_link/5]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([
    timer_expiry/1
]).

-define(SERVER, ?MODULE).

-record(state, {
    tserver,
    report_time,
    module,
    function,
    args
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
-spec(start_link(TServer::atom(), ReportTime::integer(), CallBackModule::atom(), CallBackFun::integer(), CallBackArgs::list()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(TServer, ReportTime, CallBackModule, CallBackFun, CallBackArgs) ->
    Name = atom_to_binary(TServer, utf8),
    ServerName = binary_to_atom(<<"falcon_", Name/binary>>, utf8),
    gen_server:start_link({local, ServerName}, ?MODULE, [TServer, ReportTime, CallBackModule, CallBackFun, CallBackArgs], []).

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
init([TServer, ReportTime, CallBackModule, CallBackFun, CallBackArgs]) ->
    {ok, _Pid} = chronos:start_link(TServer),
    _TS = chronos:start_timer(TServer, TServer, ReportTime * 1000,
        {?MODULE, timer_expiry, [TServer]}),
    {ok, #state{tserver = TServer, report_time = ReportTime, module = CallBackModule, function = CallBackFun, args = CallBackArgs }}.

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
handle_call({timer_expiry, Server}, _From, #state{report_time = RTime} = State) ->
    error_logger:info_msg("timer_expiry for ~p~n", [State]),
    timer_handle(State),
    _TS = chronos:start_timer(State#state.tserver,
        State#state.tserver, RTime * 1000,
        {?MODULE, timer_expiry, [Server]}),
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

timer_expiry(Timer) ->
    Name = atom_to_binary(Timer, utf8),
    ServerName = binary_to_atom(<<"falcon_", Name/binary>>, utf8),
    gen_server:call(whereis(ServerName), {timer_expiry, Timer}).
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
timer_handle(#state{tserver = Name, report_time = RTime, module = Module, function = Function, args = Args}) ->
    case cfalcon:check_leader() of
        true ->
            try
                {Metric, TimeStamp, Value, Tags} = Module:Function(Name, Args),
                cfalcon:report(Metric, TimeStamp, Value, cfalcon:generate_tags(Tags), RTime)
            catch
                E:R  ->
                    error_logger:error_msg("report error:~p, reason:~p, bt:~p", [E, R, erlang:get_stacktrace()])
            end;
        false ->
            ignore
    end.
