test_interfaces: include/* tests/test_interfaces.cpp
		g++ tests/test_interfaces.cpp -o tests/test_interfaces -Iinclude
	
format:
		clang-format -i include/*.h
		clang-format -i src/*.cpp
		clang-format -i tests/*.cpp
	
clean:
		rm -f tests/test_interfaces
		rm -f a.out
		rm -f *.o

