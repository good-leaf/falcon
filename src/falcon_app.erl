-module(falcon_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	cfalcon:start_link(),
	count:start_link(),
	report_sup:start_link(),
	falcon_sup:start_link().

stop(_State) ->
	ok.
