package(default_visibility = ["//visibility:public"])

cc_binary(
    name="nodecore",
    srcs=[
        "src/nodecore.cpp",
        "src/Array.h",
        "src/mytypes.h",
    ],
    linkopts = ["-lrt"],
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