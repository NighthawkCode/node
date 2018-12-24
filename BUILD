package(default_visibility = ["//visibility:public"])

cc_library(
    name = "node",
    srcs = [
        "src/mytypes.h",
        "src/nodecore.h",
        "src/circular_buffer.h",
        "src/nodecore.cpp",
    ],
    hdrs = [
        "src/channel.h",
    ],
    includes = [
        "src",
    ]
)

genrule(
    name = "vrm_gen",
    srcs = [
        "vrm/image.vrm",
    ],
    outs = [
        "image.h",
    ],
    tools = [
        "@vrm//:vrm",
    ],
    cmd = "$(location @vrm//:vrm) $(location vrm/image.vrm) > $@",
)

cc_library(
    name = "vrm_messages",
    hdrs = [
        ":vrm_gen",
        "@vrm//:vrm_headers",
    ],
    includes=[
        "$(GENDIR)",
    ],
)