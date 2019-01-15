package(default_visibility = ["//visibility:public"])

cc_library(
    name = "node",
    srcs = [
        "src/mytypes.h",
        "src/nodecore.h",
        "src/circular_buffer.h",
        "src/nodecore.cpp",
        "src/node_registry.cpp",
    ],
    hdrs = [
        "src/channel.h",
        "src/node_registry.h",
    ],
    includes = [
        "src",
    ]
)


genrule(
    name = "image_gen",
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

genrule(
    name = "registry_gen",
    srcs = [
        "vrm/registry.vrm",
    ],
    outs = [
        "registry.h",
    ],
    tools = [
        "@vrm//:vrm",
    ],
    cmd = "$(location @vrm//:vrm) $(location vrm/registry.vrm) > $@",
)

cc_library(
    name = "vrm_messages",
    hdrs = [
        ":image.h",
        ":registry.h",
        "@vrm//:vrm_headers",
    ],
    includes=[
        "$(GENDIR)",
    ],
)