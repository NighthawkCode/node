cc_binary(
    name="prod",
    srcs=[
        "producer.cpp",
        "//:image.h",
    ],
    linkopts = ["-lrt", "-lpthread"],
    deps = [
        "//:vrm_messages",
        "@vrm//:vrm_headers",
        "//:node",
    ],
)

cc_binary(
    name="cons",
    srcs=[
        "consumer.cpp",
        "//:image.h",
    ],
    linkopts = ["-lrt", "-lpthread"],
    deps = [
        "//:vrm_messages",
        "@vrm//:vrm_headers",
        "//:node",
    ],
)