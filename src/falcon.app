{application, falcon, [
    {description, ""},
    {vsn, "1"},
    {registered, []},
    {applications, [
        kernel,
        stdlib,
        inets,
        poolboy,
        eredis_cluster
    ]},
    {mod, {falcon_app, []}},
    {env, []}
]}.