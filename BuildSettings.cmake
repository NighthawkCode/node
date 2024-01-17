
include(CheckLanguage)
check_language(CUDA)
set(CMAKE_CUDA_STANDARD 14)
if(CMAKE_CUDA_COMPILER)
  enable_language(CUDA)
  # Use a consistent CUDA runtime library throughout the build
  set(CMAKE_CUDA_RUNTIME_LIBRARY Shared)
endif()

if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
  if(${LSB_RELEASE} MATCHES "20.04")
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX,C>:-mcpu=cortex-a78>")
    # Orin
    set(CMAKE_CUDA_ARCHITECTURES 72 87)
  else()
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX,C>:-mcpu=carmel>")
    # Xavier
    set(CMAKE_CUDA_ARCHITECTURES 60 61 70)
  endif()
else()
  # x86
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX,C>:-march=x86-64-v3>")
  set(CMAKE_CUDA_ARCHITECTURES 60 61 70 80)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_LINK_DEPENDS_NO_SHARED TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Compiler agnostic flags common throughout
set(CMAKE_CXX_FLAGS
        "${CMAKE_CXX_FLAGS} \
      -Werror \
      -Wall   \
      -Wextra \
    ")

message("Compiler id: ${CMAKE_CXX_COMPILER_ID} version : ${CMAKE_CXX_COMPILER_VERSION}")

set(CUDA_HOST_COMPILER gcc)

#  clang/clang++ specific flags
set(CMAKE_CXX_FLAGS
        "${CMAKE_CXX_FLAGS} \
  -Werror=return-type \
  -Wframe-larger-than=8388608\
  -Wno-c++98-compat \
  -Wno-c++98-compat-pedantic \
  -Wno-global-constructors \
  -Wno-unknown-warning-option \
  -Wno-unsafe-buffer-usage \
  -Wno-exit-time-destructors \
  -Wno-ctad-maybe-unsupported \
  -Wno-double-promotion \
  -Wno-padded \
  -Wno-packed \
  -Wno-gnu-zero-variadic-macro-arguments")

# TODO: Fix these warnings
set(CMAKE_CXX_FLAGS
        "${CMAKE_CXX_FLAGS} \
  -Wno-cast-qual \
  -Wno-covered-switch-default \
  -Wno-deprecated \
  -Wno-deprecated-declarations \
  -Wno-disabled-macro-expansion \
  -Wno-documentation \
  -Wno-float-conversion \
  -Wno-float-equal \
  -Wno-header-hygiene \
  -Wno-implicit-float-conversion \
  -Wno-missing-noreturn \
  -Wno-newline-eof \
  -Wno-old-style-cast \
  -Wno-reorder \
  -Wno-reserved-id-macro \
  -Wno-sign-compare \
  -Wno-sign-conversion \
  -Wno-switch \
  -Wno-switch-enum \
  -Wno-undefined-func-template \
  -Wno-unreachable-code \
  -Wno-unreachable-code-break \
  -Wno-unreachable-code-return \
  -Wno-unused-function \
  -Wno-unused-macros \
  -Wno-unused-member-function \
  -Wno-unused-parameter \
  -Wno-unused-private-field \
  -Wno-unused-template \
  -Wno-unused-variable \
  -Wno-weak-template-vtables \
  -Wno-weak-vtables \
  -Wno-zero-as-null-pointer-constant \
  ")

# Add flag if we're doing a debug build
set(CMAKE_CXX_FLAGS_DEBUG
        "${CMAKE_CXX_FLAGS_DEBUG} \
  -DDEBUG_BUILD \
  -fstandalone-debug \
  -fno-omit-frame-pointer \
  -funwind-tables \
  -glldb \
  ")

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
        "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} \
  -fstandalone-debug \
  -fno-omit-frame-pointer \
  -funwind-tables \
  -glldb \
  ")
