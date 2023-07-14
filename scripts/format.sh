#!/bin/bash

find include/*/*.h | xargs clang-format -i
find include/*.h | xargs clang-format -i
find src/*.cpp | xargs clang-format -i
find test/*.cpp | xargs clang-format -i
