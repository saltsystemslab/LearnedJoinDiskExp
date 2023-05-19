CXX=g++
C_OPTIONS=-g
INCLUDE=include

OBJDIR=obj
_OBJ=slice_file_iterator.o slice_array_iterator.o plr.o slice_iterator.o
OBJ = $(patsubst %,$(OBJDIR)/%,$(_OBJ))

$(OBJDIR)/%.o: src/%.cpp
		mkdir -p $(OBJDIR)
		$(CXX) -c -o $@ $< $(C_OPTIONS) -I$(INCLUDE)

build_benchmark: include/* src/*.cpp $(OBJ)
		mkdir -p test_bin/
		$(CXX) $(OPTIONS) $(OBJ) src/benchmark.cpp -o test_bin/benchmark -Iinclude

build_test: include/* tests/*.cpp $(OBJ) 
		mkdir -p test_bin/
		$(CXX) $(OPTIONS) $(OBJ) tests/test_interfaces.cpp -o test_bin/test_interfaces -Iinclude
		$(CXX) $(OPTIONS) $(OBJ) tests/test_slice_comparator.cpp -o test_bin/test_slice_comparator -Iinclude
		$(CXX) $(OPTIONS) $(OBJ) tests/test_learned_merger.cpp -o test_bin/test_learned_merger -Iinclude
		$(CXX) $(OPTIONS) $(OBJ) tests/test_slice_file_iterator.cpp -o test_bin/test_slice_file_iterator -I$(INCLUDE)

test: build_test build_benchmark
		./test_bin/test_interfaces
		./test_bin/test_learned_merger
		./test_bin/test_slice_comparator
		./test_bin/test_slice_file_iterator
		./test_bin/benchmark


format:
		clang-format -i $(INCLUDE)/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
		rm -rf obj
		rm -rf test_bin 
		rm -f a.out
		rm -f *.o

