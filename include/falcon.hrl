-define(ENDPOINT, application:get_env(falcon, endpoint, undefined)).
-define(IS_SHORT_HOSTNAME, application:get_env(falcon, short_hostname, true)).
-define(FALCON_URL, application:get_env(falcon, falcon_url, "http://127.0.0.1:1988/v1/push")).
-define(FALCON_OPTION, application:get_env(falcon, falcon_option, [{timeout, 2000}, {connect_timeout, 1000}])).
-define(FALCON_RETRY, application:get_env(falcon, falcon_retry, 2)).
-define(FALCON_REDIS_RETRY, application:get_env(falcon, redis_retry, 2)).
%节点存储过期时间
-define(REDIS_NODE_EXPIRED, application:get_env(falcon, redis_node_expired, 30)).
%节点注册频率
-define(REG_TIMEVAL, application:get_env(falcon, node_reg_time, 20000)).
%统计指标过期时间
-define(KEY_EXPRIRED, application:get_env(falcon, key_expird, 300)).
