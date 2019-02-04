cc_binary(
    name = "nodecore",
    srcs = [
        "nodesrv.cpp",
        "//:registry.h",
    ],
    linkopts = ["-lrt", "-lpthread"],
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
    linkopts = ["-lrt", "-lpthread"],
    deps = [
        "@vrm//:vrm_headers",
    ]
)
