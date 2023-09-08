// Generic interface to the disk.  Used by swap_space to store
// objects.

#ifndef BACKING_STORE_HPP
#define BACKING_STORE_HPP

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <unistd.h>
#include <cassert>
#include <ext/stdio_filebuf.h>

class backing_store {
   public:
    virtual uint64_t allocate(size_t n) = 0;
    virtual void deallocate(uint64_t id) = 0;
    virtual std::iostream *get(uint64_t id) = 0;
    virtual void put(std::iostream *ios) = 0;
};

class one_file_per_object_backing_store : public backing_store {
   public:
    one_file_per_object_backing_store(std::string rt);
    uint64_t allocate(size_t n);
    void deallocate(uint64_t id);
    std::iostream *get(uint64_t id);
    void put(std::iostream *ios);

   private:
    std::string root;
    uint64_t nextid;
};

/////////////////////////////////////////////////////////////
// Implementation of the one_file_per_object_backing_store //
/////////////////////////////////////////////////////////////
one_file_per_object_backing_store::one_file_per_object_backing_store(
    std::string rt)
    : root(rt), nextid(1) {}

uint64_t one_file_per_object_backing_store::allocate(size_t n) {
    uint64_t id = nextid++;
    std::string filename = root + "/" + std::to_string(id);
    std::fstream dummy(filename, std::fstream::out);
    dummy.flush();
    assert(dummy.good());
    return id;
}

void one_file_per_object_backing_store::deallocate(uint64_t id) {
    std::string filename = root + "/" + std::to_string(id);
    assert(unlink(filename.c_str()) == 0);
}

std::iostream *one_file_per_object_backing_store::get(uint64_t id) {
    __gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>;
    std::string filename = root + "/" + std::to_string(id);
    fb->open(filename, std::fstream::in | std::fstream::out);
    std::fstream *ios = new std::fstream;
    ios->std::ios::rdbuf(fb);
    ios->exceptions(std::fstream::badbit | std::fstream::failbit |
                    std::fstream::eofbit);
    assert(ios->good());

    return ios;
}

void one_file_per_object_backing_store::put(std::iostream *ios) {
    ios->flush();
    __gnu_cxx::stdio_filebuf<char> *fb =
        (__gnu_cxx::stdio_filebuf<char> *)ios->rdbuf();
    fsync(fb->fd());
    delete ios;
    delete fb;
}

#endif  // BACKING_STORE_HPP
