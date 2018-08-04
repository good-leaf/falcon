-module(fredis).

-export([
    excute/1,
    qp_excute/1,
    qpexcute_retry/2,
    excute_retry/2
]).

qp_excute(Cmd) ->
    try
        case eredis_cluster:qp(Cmd) of
            {error, Reason} ->
                error_logger:error_msg("excute cmd:~p failed, reason:~p", [Cmd, Reason]),
                {error, Reason};
            Payload ->
                Payload
        end
    catch
        E:R  ->
            error_logger:error_msg("excute cmd:~p failed, error:~p, reason:~p", [Cmd, E, R]),
            {error, R}
    end.

excute(Cmd) ->
    try
        case eredis_cluster:q(Cmd) of
            {error, Reason} ->
                error_logger:error_msg("excute cmd:~p failed, reason:~p", [Cmd, Reason]),
                {error, Reason};
            Payload ->
                Payload
        end
    catch
        E:R  ->
            error_logger:error_msg("excute cmd:~p failed, error:~p, reason:~p", [Cmd, E, R]),
            {error, R}
    end.

qpexcute_retry(Cmd, 1) ->
    qp_excute(Cmd);
qpexcute_retry(Cmd, Retry) ->
    case qp_excute(Cmd) of
        {error, _Reason} ->
            qpexcute_retry(Cmd, Retry -1);
        Payload ->
            Payload
    end.

excute_retry(Cmd, 1) ->
    excute(Cmd);
excute_retry(Cmd, Retry) ->
    case excute(Cmd) of
        {error, _Reason} ->
            excute_retry(Cmd, Retry -1);
        Payload ->
            Payload
    end.
