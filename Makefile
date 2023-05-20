CXX=g++
C_OPTIONS=-g
INCLUDE=include
LEARNED_MERGE=-DLEARNED_MERGE=0 -DTRACK_STATS=0

OBJDIR=obj
_OBJ=slice_file_iterator.o slice_array_iterator.o plr.o slice_iterator.o
OBJ = $(patsubst %,$(OBJDIR)/%,$(_OBJ))

$(OBJDIR)/%.o: src/%.cpp
		mkdir -p $(OBJDIR)
		$(CXX) -c -o $@ $< $(C_OPTIONS) -I$(INCLUDE) $(LEARNED_MERGE)

benchmark: include/* src/*.cpp $(OBJ)
		mkdir -p bin/
		$(CXX) $(OPTIONS) $(OBJ) $(LEARNED_MERGE) src/benchmark.cpp -o bin/benchmark -Iinclude

build_test: include/* tests/*.cpp $(OBJ) 
		mkdir -p bin/
		$(CXX) $(OPTIONS) $(OBJ) $(LEARNED_MERGE) tests/test_interfaces.cpp -o bin/test_interfaces -Iinclude
		$(CXX) $(OPTIONS) $(OBJ) $(LEARNED_MERGE) tests/test_slice_comparator.cpp -o bin/test_slice_comparator -Iinclude
		$(CXX) $(OPTIONS) $(OBJ) $(LEARNED_MERGE) tests/test_learned_merger.cpp -o bin/test_learned_merger -Iinclude
		$(CXX) $(OPTIONS) $(OBJ) $(LEARNED_MERGE) tests/test_slice_file_iterator.cpp -o bin/test_slice_file_iterator -I$(INCLUDE)

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
		rm -rf obj
		rm -rf bin 
		rm -f a.out
		rm -f *.o

