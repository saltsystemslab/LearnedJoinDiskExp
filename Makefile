test_interfaces: include/* test_interfaces.cpp
	g++ test_interfaces.cpp -o test_interfaces

clean:
	rm -f test_interfaces
	rm -f a.out
	rm -f *.o
