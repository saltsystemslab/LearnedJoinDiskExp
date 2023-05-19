CXX=g++
C_OPTIONS=-g
OBJECTS=slice_file_iterator.o slice_array.o
INCLUDE=include

slice_array.o : src/slice_array.cpp 
		mkdir -p build/
		g++ $(C_OPTIONS) -o slice_array.o -I$(INCLUDE) -c src/slice_array.cpp

slice_file_iterator.o : src/slice_file_iterator.cpp 
		mkdir -p build/
		g++ $(C_OPTIONS) -o slice_file_iterator.o -I$(INCLUDE) -c src/slice_file_iterator.cpp

build_benchmark: include/* src/*.cpp slice_array.o slice_file_iterator.o
		mkdir -p test_bin/
		$(CXX) $(OPTIONS) $(OBJECTS) src/benchmark.cpp -o test_bin/benchmark -Iinclude

build_test: include/* tests/*.cpp $(OBJECTS) 
		mkdir -p test_bin/
		$(CXX) $(OPTIONS) tests/test_interfaces.cpp -o test_bin/test_interfaces -Iinclude
		$(CXX) $(OPTIONS) $(OBJECTS) tests/test_slice_comparator.cpp -o test_bin/test_slice_comparator -Iinclude
		$(CXX) $(OPTIONS) tests/test_learned_merger.cpp -o test_bin/test_learned_merger -Iinclude
		$(CXX) $(OPTIONS) $(OBJECTS) tests/test_slice_file_iterator.cpp -o test_bin/test_slice_file_iterator -I$(INCLUDE)

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
		rm -rf test_bin 
		rm -f a.out
		rm -f *.o

