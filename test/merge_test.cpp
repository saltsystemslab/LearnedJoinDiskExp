#include <gtest/gtest.h>

#include "merge.h"
#include "in_mem_sstable.h"
#include "disk_sstable.h"
#include "rbtree_index.h"
#include "btree_index.h"
#include "synthetic.h"
#include "runner.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace li_merge {

SSTable<KVSlice> *create_input_inmem(uint64_t *arr, int num_elems) {
    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    SSTableBuilder<KVSlice> *builder = 
        FixedSizeKVInMemSSTableBuilder::InMemMalloc(10, 8, 0, comparator);
    for (int i=0; i<num_elems; i++) {
        builder->add(KVSlice((char *)&arr[i], 8, 0));
    }
    return builder->build();
}

void check_output(SSTable<KVSlice> *result, uint64_t *expected, int num_elems) {
    auto iter = result->iterator();
    for (int i=0; i<num_elems; i++) {
        KVSlice kv = iter->key();
        uint64_t key = *(kv.data());
        ASSERT_EQ(key, expected[i]);
        iter->next();
    }
}

TEST(StandardMerge, StandardMerge) {
    uint64_t inner_data[10] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    uint64_t outer_data[5] = {25, 45, 65, 85, 105};
    uint64_t expected[15] = {10, 20, 25, 30, 40, 45, 50, 60, 65, 70, 80, 85, 90, 100, 105};

    SSTable<KVSlice> *inner = create_input_inmem(inner_data, 10);
    SSTable<KVSlice> *outer = create_input_inmem(outer_data, 5);
    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    SSTableBuilder<KVSlice> *resultBuilder = 
        FixedSizeKVInMemSSTableBuilder::InMemMalloc(15, 8, 0, comparator);
    json log;

    SSTable<KVSlice> *result = standardMerge(outer, inner, comparator, resultBuilder, &log);
    check_output(result, expected, 15);

}

TEST(StandardMerge, ParallelStandardMerge_RbTree_Disk) {
    SSTableBuilder<KVSlice> *inner_builder = new FixedSizeKVDiskSSTableBuilder("inner", 8, 0);
    SSTableBuilder<KVSlice> *outer_builder = new FixedSizeKVDiskSSTableBuilder("outer", 8, 0);

    auto inner = generate_uniform_random_distribution(105, 8, 0, new KVUint64Cmp(), inner_builder);
    auto outer = generate_uniform_random_distribution(95, 8, 0, new KVUint64Cmp(), outer_builder);

    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    IndexBuilder<KVSlice> * inner_index_builder 
        = new RbTreeIndexBuilder(comparator, 8);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    PSSTableBuilder<KVSlice> *pResultBuilder = new PFixedSizeKVDiskSSTableBuilder("p_result", 8, 0, 200);
    SSTableBuilder<KVSlice> *resultBuilder = new FixedSizeKVDiskSSTableBuilder("s_result", 8, 0);
    json log;

    SSTable<KVSlice> *resultParallel = parallelStandardMerge(outer, inner, inner_index, comparator, 4, pResultBuilder, &log);
    SSTable<KVSlice> *resultStandard = standardMerge(outer, inner, comparator, resultBuilder, &log);

    std::string standardMd5 = md5_checksum(resultParallel);
    std::string parallelMd5 = md5_checksum(resultStandard);
    ASSERT_EQ(standardMd5, parallelMd5);
}

TEST(StandardMerge, ParallelStandardMerge_RbTree) {
    uint64_t inner_data[10] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    uint64_t outer_data[5] = {25, 45, 65, 85, 105};
    uint64_t expected[15] = {10, 20, 25, 30, 40, 45, 50, 60, 65, 70, 80, 85, 90, 100, 105};

    SSTable<KVSlice> *inner = create_input_inmem(inner_data, 10);
    SSTable<KVSlice> *outer = create_input_inmem(outer_data, 5);
    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    IndexBuilder<KVSlice> * inner_index_builder 
        = new RbTreeIndexBuilder(comparator, 8);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    PSSTableBuilder<KVSlice> *resultBuilder =  new PFixedSizeKVInMemSSTableBuilder(15, 8, 0, comparator);
    json log;

    SSTable<KVSlice> *result = parallelStandardMerge(outer, inner, inner_index, comparator, 5, resultBuilder, &log);
    check_output(result, expected, 15);

}

TEST(StandardMerge, ParallelStandardMerge_bTree) {
    uint64_t inner_data[10] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    uint64_t outer_data[5] = {25, 45, 65, 85, 105};
    uint64_t expected[15] = {10, 20, 25, 30, 40, 45, 50, 60, 65, 70, 80, 85, 90, 100, 105};

    SSTable<KVSlice> *inner = create_input_inmem(inner_data, 10);
    SSTable<KVSlice> *outer = create_input_inmem(outer_data, 5);
    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    IndexBuilder<KVSlice> * inner_index_builder 
        = new BTreeIndexBuilder("btree_index", 8);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    PSSTableBuilder<KVSlice> *resultBuilder =  new PFixedSizeKVInMemSSTableBuilder(15, 8, 0, comparator);
    json log;

    SSTable<KVSlice> *result = parallelStandardMerge(outer, inner, inner_index, comparator, 5, resultBuilder, &log);
    check_output(result, expected, 15);
}

TEST(StandardMerge, ParallelStandardMerge_bTree_skewed1) {
    uint64_t inner_data[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    uint64_t outer_data[5] = {11, 12, 13, 14, 15};
    uint64_t expected[15] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    SSTable<KVSlice> *inner = create_input_inmem(inner_data, 10);
    SSTable<KVSlice> *outer = create_input_inmem(outer_data, 5);
    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    IndexBuilder<KVSlice> * inner_index_builder 
        = new BTreeIndexBuilder("btree_index", 8);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    PSSTableBuilder<KVSlice> *resultBuilder =  new PFixedSizeKVInMemSSTableBuilder(15, 8, 0, comparator);
    json log;

    SSTable<KVSlice> *result = parallelStandardMerge(outer, inner, inner_index, comparator, 5, resultBuilder, &log);
    check_output(result, expected, 15);
}

TEST(StandardMerge, ParallelStandardMerge_bTree_skewed2) {
    uint64_t outer_data[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    uint64_t inner_data[5] = {11, 12, 13, 14, 15};
    uint64_t expected[15] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    SSTable<KVSlice> *inner = create_input_inmem(inner_data, 5);
    SSTable<KVSlice> *outer = create_input_inmem(outer_data, 10);
    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    IndexBuilder<KVSlice> * inner_index_builder 
        = new BTreeIndexBuilder("btree_index", 8);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    PSSTableBuilder<KVSlice> *resultBuilder =  new PFixedSizeKVInMemSSTableBuilder(15, 8, 0, comparator);
    json log;

    SSTable<KVSlice> *result = parallelStandardMerge(outer, inner, inner_index, comparator, 5, resultBuilder, &log);
    check_output(result, expected, 15);

}



};