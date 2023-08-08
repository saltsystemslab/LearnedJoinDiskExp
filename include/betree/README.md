Betree: a small, simple implementation of a B^e-tree, as described in
the September 2015 ;login: article,
      "An Introduction to B^e-trees and Write-Optimization"
by Michael A. Bender, Martin Farach-Colton, William Jannen, Rob
Johnson, Bradley C. Kuszmaul, Donald E. Porter, Jun Yuan, and Yang
Zhan

A B^-e-tree is an on-disk data structure with an interface similar to
a B-tree.  It stores a mapping from keys to values, supporting
inserts, queries, deletes, updates, and efficient iteration.  The key
features of a B^e-tree are extremely I/O-efficient insertions,
updates, and iteration, with query performance comparable to a B-tree.
See the above-referenced article for more details.

This distribution includes
- the B^e-tree implementation
- a test program that checks correctness and demonstrates
  how to use the B^e-tree implementation


BUILDING AND RUNNING THE TEST PROGRAM
-------------------------------------

To build, run
  $ make
  $ mkdir tmpdir
  $ ./test -d tmpdir

The test takes about a minute to run and should print "Test PASSED".
The test performs a random sequence of operations on a betree and on
an STL map, verifying that it always gets the same result from each
data structure.  If it ever finds a discrepancy, it will abort with an
assertion failure, and will likely leave some files in tmpdir.  A
successful run should leave tmpdir empty.

The code has been tested on a Debian 8.2 Linux installation with
- g++ 4.9.2
- GNU make 4.0
- libstdc++ 6.0.20
- libc 2.19

GUIDE TO THE CODE
-----------------

test.cpp: The main test program.  Demonstrates how to construct and
          use a betree.

betree.hpp: The core of the betree implementation.  This class handles
            flushing messages down the tree, splitting nodes,
            performing inserts, queries, etc, and provides an iterator
            for scanning key/value pairs in the tree.

            The betree is written almost completely as an in-memory
	    data structure.  All I/O is handled transparently by
	    swap_space.

swap_space.{cpp,hpp}: Swaps objects to/from disk.  Maintains a cache
		      of in-memory objects.  When the cache becomes
		      too large the least-recently-used object is
		      written to disk and removed from the cache.
		      Automatically loads the object back into memory
		      when it is referenced.  Garbage collects objects
		      that are no longer referenced by any other
		      object.  Tracks when objects are modified in
		      memory so that it knows to write them back to
		      disk next time they get evicted.

backing_store.{cpp,hpp}: This defines a generic interface used by
                         swap_space to manage on-disk space.  It
                         supports allocating and deallocating on-disk
                         space. The file also defines a simple
                         implementation of the interface that stores
                         one object per file on disk.

