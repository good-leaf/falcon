-module(task_timer).

-behaviour(gen_server).

-include("../include/falcon.hrl").
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
	timer_expiry/1,
	task_callback/3,
	timer_handle/1,
	async_report/5
]).

-define(SERVER, ?MODULE).

-record(state, {
	task_name,
	ntimer,
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
-spec(start_link(TaskName :: binary(), ReportTime :: integer(), CallBackModule :: atom(), CallBackFun :: integer(), CallBackArgs :: list()) ->
	{ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(TaskName, ReportTime, CallBackModule, CallBackFun, CallBackArgs) ->
	AtomTaskName = binary_to_atom(TaskName, utf8),
	gen_server:start_link({local, AtomTaskName}, ?MODULE, [TaskName, ReportTime, CallBackModule, CallBackFun, CallBackArgs], []).

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
init([TaskName, ReportTime, CallBackModule, CallBackFun, CallBackArgs]) ->
	NameTimer = binary_to_atom(<<"falcon_", TaskName/binary>>, utf8),
	{ok, _Pid} = chronos:start_link(NameTimer),
	_TS = chronos:start_timer(NameTimer, NameTimer, ReportTime * 1000,
		{?MODULE, timer_expiry, [TaskName]}),
	{ok, #state{task_name = TaskName, ntimer = NameTimer, report_time = ReportTime, module = CallBackModule, function = CallBackFun, args = CallBackArgs}}.

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
handle_call({timer_expiry, TaskName}, _From, #state{report_time = RTime} = State) ->
	error_logger:info_msg("timer_expiry for ~p~n", [State]),
	spawn(?MODULE, timer_handle, [convert_state(State)]),
	_TS = chronos:start_timer(State#state.ntimer,
		State#state.ntimer, RTime * 1000,
		{?MODULE, timer_expiry, [TaskName]}),
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
convert_state(#state{task_name = TaskName, report_time = RTime, module = Module,
	function = Function, args = Args}) ->
	#{
		task_name => TaskName,
		report_time => RTime,
		module => Module,
		function => Function,
		args => Args
	}.

timer_handle(#{task_name := TaskName, report_time := RTime, module := Module,
	function := Function, args := Args}) ->
	try
		case cfalcon:check_leader() of
			true ->
				spawn(?MODULE, async_report, [TaskName, RTime, Module, Function, Args]);
			false ->
				ignore
		end
	catch
		E:R ->
			error_logger:error_msg("timer_handle error:~p, reason:~p, bt:~p", [E, R, erlang:get_stacktrace()])
	end.

async_report(TaskName, RTime, Module, Function, Args) ->
	ReportList = Module:Function(TaskName, RTime, Args),
	lists:foreach(fun({Metric, TimeStamp, Value, Tags}) ->
		report_timer:report(Metric, TimeStamp, Value, report_timer:generate_tags(Tags), RTime)
	              end, ReportList).


%cfalcon:task_register(<<"task">>, 60, task_timer, task_callback, [{<<"env">>, <<"dev">>}])
task_callback(_TaskName, _RTime, Args) ->
	[{<<"metric">>, report_timer:timestamp(), 100, Args}].
