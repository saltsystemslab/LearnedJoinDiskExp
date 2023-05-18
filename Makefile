CXX=g++
C_OPTIONS=-g
OBJECTS=slice_array.o
INCLUDE=include

slice_array.o : src/slice_array.cpp 
		mkdir -p build/
		g++ $(C_OPTIONS) -o slice_array.o -I$(INCLUDE) -c src/slice_array.cpp

build_test: include/* tests/*.cpp slice_array.o
		mkdir -p test_bin/
		$(CXX) $(OPTIONS) $(OBJECTS) tests/test_interfaces.cpp -o test_bin/test_interfaces -Iinclude
		$(CXX) $(OPTIONS) $(OBJECTS) tests/test_slice_comparator.cpp -o test_bin/test_slice_comparator -Iinclude
		$(CXX) $(OPTIONS) $(OBJECTS) tests/test_learned_merger.cpp -o test_bin/test_learned_merger -Iinclude

build_benchmark: include/* src/*.cpp slice_array.o
		mkdir -p test_bin/
		$(CXX) $(OPTIONS) $(OBJECTS) src/benchmark.cpp -o test_bin/benchmark -Iinclude
		

test: build_test
		./test_bin/test_interfaces
		./test_bin/test_learned_merger
		./test_bin/test_slice_comparator
		./test_bin/benchmark

format:
		clang-format -i include/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
		rm -rf test_bin 
		rm -f a.out
		rm -f *.o

