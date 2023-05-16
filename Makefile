test_interfaces: include/* tests/test_interfaces.cpp
	g++ tests/test_interfaces.cpp -o tests/test_interfaces -Iinclude
	
clean:
	rm -f tests/test_interfaces
	rm -f a.out
	rm -f *.o

