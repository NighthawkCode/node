cc_binary(
    name="prod",
    srcs=[
        "producer.cpp",
        "//:image.h",
    ],
    linkopts = ["-lrt"],
    deps = [
        "//:vrm_messages",
        "@vrm//:vrm_headers",
    ],
)

cc_binary(
    name="cons",
    srcs=[
        "consumer.cpp",
    ],
    linkopts = ["-lrt"],
)