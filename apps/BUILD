cc_binary(
    name = "nodesrv",
    srcs = [
        "nodesrv.cpp",
        "//:registry.h",
    ],
    deps = [
        "@vrm//:vrm_headers",
        "//:node",
    ]
)

cc_binary(
    name = "nodecli",
    srcs = [
        "nodecli.cpp",
        "//:registry.h",
        "//:image.h",
    ],
    deps = [
        "@vrm//:vrm_headers",
    ]
)
