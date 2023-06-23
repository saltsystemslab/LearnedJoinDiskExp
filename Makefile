CXX=g++
C_OPTIONS=-g -Ofast -pthread -std=c++17
INCLUDE=-Iinclude/algos -Iinclude/impl/int -Iinclude/impl/slice -Iinclude/impl/plr -Iinclude/interfaces -Iinclude/types -Iinclude/impl -I./PGM-index/include -Iinclude

USE_STRING_KEYS ?= 0
USE_INT_128 ?= 0

TRAIN_RESULT ?= 0
ERROR_BOUND ?= 10
USE_BULK_COPY ?= 1
TRUST_ERROR_BOUNDS ?= 0

LEARNED_MERGE= -DTRACK_STATS=0 -DTRACK_PLR_TRAIN_TIME=0 -DTRUST_ERROR_BOUNDS=${TRUST_ERROR_BOUNDS} -DPLR_POS_OFFSET=${ERROR_BOUND} -DPGM_ERROR_BOUND=1 -DUSE_STRING_KEYS=${USE_STRING_KEYS} -DUSE_INT_128=${USE_INT_128} -DUSE_BULK_COPY=${USE_BULK_COPY} -DMAX_KEYS_TO_BULK_COPY=1000000 -DTRAIN_RESULT=${TRAIN_RESULT}


OBJDIR=obj
_OBJ=plr.o
OBJ = $(patsubst %,$(OBJDIR)/%,$(_OBJ))

$(OBJDIR)/%.o: src/%.cpp
		mkdir -p $(OBJDIR)
		$(CXX) -c -o $@ $< $(C_OPTIONS) $(INCLUDE) $(LEARNED_MERGE)

benchmark: include/* src/*.cpp $(OBJ)
		mkdir -p bin/
		$(CXX) $(C_OPTIONS) $(OBJ) $(LEARNED_MERGE) src/benchmark.cpp -o bin/benchmark $(INCLUDE)

benchmark_pgm: include/* src/*.cpp $(OBJ)
		mkdir -p bin/
		$(CXX) $(C_OPTIONS) $(OBJ) $(LEARNED_MERGE) src/benchmark_pgm.cpp -o bin/benchmark_pgm $(INCLUDE)

benchmark_lookup: include/* src/*.cpp $(OBJ)
		mkdir -p bin/
		$(CXX) $(C_OPTIONS) $(OBJ) $(LEARNED_MERGE) src/benchmark_lookup.cpp -o bin/benchmark_lookup $(INCLUDE)

build_test: include/* tests/*.cpp $(OBJ) 
		mkdir -p bin/
		$(CXX) $(C_OPTIONS) $(OBJ) $(LEARNED_MERGE) tests/test_interfaces.cpp -o bin/test_interfaces $(INCLUDE)
		$(CXX) $(C_OPTIONS) $(OBJ) $(LEARNED_MERGE) tests/test_slice_comparator.cpp -o bin/test_slice_comparator $(INCLUDE)
		$(CXX) $(C_OPTIONS) $(OBJ) $(LEARNED_MERGE) tests/test_learned_merger.cpp -o bin/test_learned_merger $(INCLUDE)
		$(CXX) $(C_OPTIONS) $(OBJ) $(LEARNED_MERGE) tests/test_slice_file_iterator.cpp -o bin/test_slice_file_iterator $(INCLUDE)

test: build_test benchmark
		./bin/test_interfaces
		./bin/test_learned_merger
		./bin/test_slice_comparator
		./bin/test_slice_file_iterator
		./bin/benchmark

format:
		clang-format -i include/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
	  rm -rf DB/
		rm -rf obj
		rm -rf bin 
		rm -f a.out
		rm -f *.o

