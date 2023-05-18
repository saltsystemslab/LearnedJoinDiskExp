CXX=g++
C_OPTIONS=-g


build_test: include/* tests/*.cpp
		mkdir -p test_bin/
		$(CXX) $(OPTIONS) tests/test_interfaces.cpp -o test_bin/test_interfaces -Iinclude
		$(CXX) $(OPTIONS) tests/test_slice_comparator.cpp -o test_bin/test_slice_comparator -Iinclude
		$(CXX) $(OPTIONS) tests/test_learned_merger.cpp -o test_bin/test_learned_merger -Iinclude

test: build_test
		./test_bin/test_interfaces
		./test_bin/test_learned_merger
		./test_bin/test_slice_comparator

format:
		clang-format -i include/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
		rm -rf test_bin 
		rm -f a.out
		rm -f *.o

