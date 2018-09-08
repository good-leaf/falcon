-define(ENDPOINT, application:get_env(falcon, endpoint, undefined)).
-define(IS_SHORT_HOSTNAME, application:get_env(falcon, short_hostname, true)).
-define(FALCON_URL, application:get_env(falcon, falcon_url, "http://127.0.0.1:1988/v1/push")).
-define(FALCON_OPTION, application:get_env(falcon, falcon_option, [{timeout, 2000}, {connect_timeout, 1000}])).
-define(FALCON_NODES, application:get_env(falcon, nodes, [])).
-define(FALCON_RETRY, application:get_env(falcon, falcon_retry, 1)).
-define(FALCON_REDIS_RETRY, application:get_env(falcon, redis_retry, 2)).
-define(PING_TIMEVAL, application:get_env(falcon, node_ping_time, 30000)).

