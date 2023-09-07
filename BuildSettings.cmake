# BuildSettings.cmake
# Common settings shared across all builds in dev
# Only edit the root version of this file and run scripts/copy-build-settings.sh, don't edit this in third_party!

DEFINE_PROPERTY(GLOBAL PROPERTY INCLUDED_GLOBAL_VERDANT_SETTINGS)
get_property(INCLUDED_GLOBAL_VERDANT_SETTINGS GLOBAL PROPERTY INCLUDED_GLOBAL_VERDANT_SETTINGS)
if(INCLUDED_GLOBAL_VERDANT_SETTINGS)
  return()
endif()
set_property(GLOBAL PROPERTY INCLUDED_GLOBAL_VERDANT_SETTINGS TRUE)

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
    set(VERDANT_TABLET_BUILD ON)
  else()
    add_compile_options("$<$<COMPILE_LANGUAGE:CXX,C>:-mcpu=carmel>")
    # Xavier
    set(CMAKE_CUDA_ARCHITECTURES 60 61 70)
    set(VERDANT_TABLET_BUILD OFF)
  endif()
else()
  # x86
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX,C>:-march=x86-64-v3>")
  set(CMAKE_CUDA_ARCHITECTURES 60 61 70 80)
  set(VERDANT_TABLET_BUILD ON)
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

# TODO(jhurliman): Remove or change this
# kfranz probably this? - https://gitlab.kitware.com/cmake/cmake/-/issues/17323
set(CUDA_HOST_COMPILER gcc)

if (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
  #  GCC specific flags
  set(CMAKE_CXX_FLAGS
          "${CMAKE_CXX_FLAGS} \
      -pedantic \
    ")
  set(CMAKE_CXX_FLAGS_DEBUG
          "${CMAKE_CXX_FLAGS_DEBUG} \
    -DDEBUG_BUILD \
    -fno-omit-frame-pointer \
    -funwind-tables \
    ")

  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
          "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} \
    -fno-omit-frame-pointer \
    -funwind-tables \
    ")
else()
  #  clang/clang++ specific flags
  set(CMAKE_CXX_FLAGS
          "${CMAKE_CXX_FLAGS} \
    -Weverything \
    -Werror=return-type \
    -Wframe-larger-than=8388608\
    -Wno-c++98-compat \
    -Wno-c++98-compat-pedantic \
    -Wno-global-constructors \
    -Wno-exit-time-destructors \
    -Wno-ctad-maybe-unsupported \
    -Wno-double-promotion \
    -Wno-padded \
    -Wno-packed \
    -Wno-gnu-zero-variadic-macro-arguments")

  # TODO(jhurliman): Fix these warnings
  set(CMAKE_CXX_FLAGS
          "${CMAKE_CXX_FLAGS} \
    -Wno-cast-align \
    -Wno-cast-qual \
    -Wno-covered-switch-default \
    -Wno-delete-non-abstract-non-virtual-dtor \
    -Wno-deprecated \
    -Wno-deprecated-declarations \
    -Wno-disabled-macro-expansion \
    -Wno-documentation \
    -Wno-documentation-unknown-command \
    -Wno-float-conversion \
    -Wno-float-equal \
    -Wno-header-hygiene \
    -Wno-ignored-qualifiers \
    -Wno-implicit-float-conversion \
    -Wno-mismatched-tags \
    -Wno-missing-noreturn \
    -Wno-missing-prototypes \
    -Wno-missing-variable-declarations \
    -Wno-newline-eof \
    -Wno-non-virtual-dtor \
    -Wno-old-style-cast \
    -Wno-reorder \
    -Wno-reserved-id-macro \
    -Wno-shadow \
    -Wno-shorten-64-to-32 \
    -Wno-sign-compare \
    -Wno-sign-conversion \
    -Wno-sometimes-uninitialized \
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
endif()
