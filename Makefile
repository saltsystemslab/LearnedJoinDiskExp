CXX=g++
C_OPTIONS=-g -fopenmp
INCLUDE=-Iinclude -I./PGM-index/include
LEARNED_MERGE=-DLEARNED_MERGE=1 -DTRACK_STATS=0 -DTRACK_PLR_TRAIN_TIME=0 -DTRUST_ERROR_BOUNDS=1 -DASSERT_SORT=0 -DPLR_POS_OFFSET=5 -DPLR_ERROR_BOUND=5 -DPGM_ERROR_BOUND=1

OBJDIR=obj
_OBJ=slice_file_iterator.o slice_array_iterator.o plr.o slice_iterator.o
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
		clang-format -i $(INCLUDE)/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
	  rm -rf DB/
		rm -rf obj
		rm -rf bin 
		rm -f a.out
		rm -f *.o

