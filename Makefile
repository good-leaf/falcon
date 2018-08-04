PROJECT = falcon
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

DEPS = eredis_cluster eredis jsx chronos poolboy
dep_eredis_cluster = git https://github.com/adrienmo/eredis_cluster.git
dep_chronos = git https://github.com/lehoff/chronos.git

LOCAL_DEPS = inets crypto
include erlang.mk
