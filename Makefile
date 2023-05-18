CXX=g++
C_OPTIONS=-g
OBJECTS=slice_file_iterator.o
INCLUDE=include

slice_file_iterator.o : src/slice_file_iterator.cpp 
		mkdir -p build/
		g++ $(C_OPTIONS) -o slice_file_iterator.o -I$(INCLUDE) -c src/slice_file_iterator.cpp

build_test: $(INCLUDE)/* tests/*.cpp $(OBJECTS)
		mkdir -p test_bin/
		$(CXX) $(OPTIONS) tests/test_interfaces.cpp -o test_bin/test_interfaces -I$(INCLUDE)
		$(CXX) $(OPTIONS) tests/test_slice_comparator.cpp -o test_bin/test_slice_comparator -I$(INCLUDE)
		$(CXX) $(OPTIONS) tests/test_learned_merger.cpp -o test_bin/test_learned_merger -I$(INCLUDE)
		$(CXX) $(OPTIONS) $(OBJECTS) tests/test_slice_file_iterator.cpp -o test_bin/test_slice_file_iterator -I$(INCLUDE)

test: build_test
		./test_bin/test_interfaces
		./test_bin/test_learned_merger
		./test_bin/test_slice_comparator
		./test_bin/test_slice_file_iterator

format:
		clang-format -i $(INCLUDE)/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
		rm -rf test_bin 
		rm -f a.out
		rm -f *.o

