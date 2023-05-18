test_interfaces: include/* tests/test_interfaces.cpp
		mkdir -p build/
		g++ tests/test_interfaces.cpp -o build/test_interfaces -Iinclude
	
test_slice_comparator: include/* tests/test_slice_comparator.cpp
		mkdir -p build/
		g++ tests/test_slice_comparator.cpp -o build/test_slice_comparator -Iinclude

test_learned_merger: include/* tests/test_learned_merger.cpp
		mkdir -p build/
		g++ -g tests/test_learned_merger.cpp -o build/test_learned_merger -Iinclude

format:
		clang-format -i include/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
		rm -rf build
		rm -f a.out
		rm -f *.o

