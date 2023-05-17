test_interfaces: include/* tests/test_interfaces.cpp
		mkdir -p build/
		g++ tests/test_interfaces.cpp -o build/test_interfaces -Iinclude
	
format:
		clang-format -i include/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
		rm -rf build
		rm -f a.out
		rm -f *.o

