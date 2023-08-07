#include <gtest/gtest.h>

#include "merge.h"
#include "join.h"
#include "in_mem_sstable.h"
#include "disk_sstable.h"
#include "rbtree_index.h"
#include "btree_index.h"
#include "betree_index.h"
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
    IndexBuilder<KVSlice> * outer_index_builder 
        = new RbTreeIndexBuilder(comparator, 8);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    Index<KVSlice> *outer_index = build_index(outer, outer_index_builder);
    PSSTableBuilder<KVSlice> *p1ResultBuilder = new PFixedSizeKVDiskSSTableBuilder("p1_result", 8, 0);
    PSSTableBuilder<KVSlice> *p2ResultBuilder = new PFixedSizeKVDiskSSTableBuilder("p2_result", 8, 0);
    SSTableBuilder<KVSlice> *resultBuilder = new FixedSizeKVDiskSSTableBuilder("s_result", 8, 0);
    json log;

    SSTable<KVSlice> *resultParallelSM = parallelStandardMerge(outer, inner, inner_index, comparator, 4, p1ResultBuilder, &log);
    SSTable<KVSlice> *resultParallelLM = parallelLearnedMerge(outer, inner, outer_index, inner_index, comparator, 2, p2ResultBuilder, &log);
    SSTable<KVSlice> *resultStandard = standardMerge(outer, inner, comparator, resultBuilder, &log);

    std::string standardMd5 = md5_checksum(resultStandard);
    std::string parallelSMMd5 = md5_checksum(resultParallelSM);
    std::string parallelLMMd5 = md5_checksum(resultParallelLM);
    ASSERT_EQ(standardMd5, parallelSMMd5);
    ASSERT_EQ(standardMd5, parallelLMMd5);
}

TEST(StandardMerge, Merge_BeTree_Disk) {
    SSTableBuilder<KVSlice> *inner_builder = new FixedSizeKVDiskSSTableBuilder("inner", 8, 0);
    SSTableBuilder<KVSlice> *outer_builder = new FixedSizeKVDiskSSTableBuilder("outer", 8, 0);

    auto inner = generate_uniform_random_distribution(105, 8, 0, new KVUint64Cmp(), inner_builder);
    auto outer = generate_uniform_random_distribution(95, 8, 0, new KVUint64Cmp(), outer_builder);

    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    system("mkdir -p backing_dir_1");
    system("mkdir -p backing_dir_2");
    IndexBuilder<KVSlice> * inner_index_builder 
        = new BeTreeIndexBuilder("backing_dir_1", 1024, 64, 16);
    IndexBuilder<KVSlice> * outer_index_builder 
        = new BeTreeIndexBuilder("backing_dir_2", 1024, 64, 16);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    Index<KVSlice> *outer_index = build_index(outer, outer_index_builder);
    SSTableBuilder<KVSlice> *learnedResultBuilder_1 = new FixedSizeKVDiskSSTableBuilder("result_1", 8, 0);
    SSTableBuilder<KVSlice> *learnedResultBuilder_2 = new FixedSizeKVDiskSSTableBuilder("result_2", 8, 0);
    SSTableBuilder<KVSlice> *standardResultBuilder = new FixedSizeKVDiskSSTableBuilder("result_s", 8, 0);
    json log;

    SSTable<KVSlice> *resultStandard = standardMerge(outer, inner, comparator, standardResultBuilder, &log);
    SSTable<KVSlice> *resultLearned1 = mergeWithIndexes(outer, inner, outer_index, inner_index, comparator, learnedResultBuilder_1, &log);
    SSTable<KVSlice> *resultLearned2 = mergeWithIndexesThreshold(outer, inner, inner_index, 5, comparator, learnedResultBuilder_2, &log);

    std::string standardMd5 = md5_checksum(resultStandard);
    std::string learnedMd5 = md5_checksum(resultLearned1);
    std::string learnedThresholdMd5 = md5_checksum(resultLearned2);
    ASSERT_EQ(standardMd5, standardMd5);
}

TEST(StandardMerge, DISABLED_ParallelMerge_BeTree_Disk) {
    SSTableBuilder<KVSlice> *inner_builder = new FixedSizeKVDiskSSTableBuilder("inner", 8, 0);
    SSTableBuilder<KVSlice> *outer_builder = new FixedSizeKVDiskSSTableBuilder("outer", 8, 0);

    auto inner = generate_uniform_random_distribution(105, 8, 0, new KVUint64Cmp(), inner_builder);
    auto outer = generate_uniform_random_distribution(95, 8, 0, new KVUint64Cmp(), outer_builder);

    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    system("mkdir -p backing_dir_1");
    system("mkdir -p backing_dir_2");
    IndexBuilder<KVSlice> * inner_index_builder 
        = new BeTreeIndexBuilder("backing_dir_1", 1024, 64, 16);
    IndexBuilder<KVSlice> * outer_index_builder 
        = new BeTreeIndexBuilder("backing_dir_2", 1024, 64, 16);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    Index<KVSlice> *outer_index = build_index(outer, outer_index_builder);
    SSTableBuilder<KVSlice> *standardResultBuilder = new FixedSizeKVDiskSSTableBuilder("s_result", 8, 0);
    PSSTableBuilder<KVSlice> *pResultBuilder = new PFixedSizeKVDiskSSTableBuilder("p_result", 8, 0);
    json log;

    SSTable<KVSlice> *resultStandard = standardMerge(outer, inner, comparator, standardResultBuilder, &log);
    SSTable<KVSlice> *resultParallelLM = parallelLearnedMerge(outer, inner, outer_index, inner_index, comparator, 4, pResultBuilder, &log);

    std::string standardMd5 = md5_checksum(resultStandard);
    std::string learnedMd5 = md5_checksum(resultParallelLM);
    ASSERT_EQ(standardMd5, standardMd5);
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

TEST(Join, JoinAlgos) {
    uint64_t inner_data[10] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    uint64_t outer_data[5] = {20, 40, 60, 80, 100};
    uint64_t expected[5] = {20, 40, 60, 80, 100};

    SSTable<KVSlice> *inner = create_input_inmem(inner_data, 10);
    SSTable<KVSlice> *outer = create_input_inmem(outer_data, 5);
    Comparator<KVSlice> *comparator = new KVUint64Cmp();
    IndexBuilder<KVSlice> * inner_index_builder 
        = new RbTreeIndexBuilder(comparator, 8);
    Index<KVSlice> *inner_index = build_index(inner, inner_index_builder);
    PSSTableBuilder<KVSlice> *p1ResultBuilder = new PSplitFixedSizeKVDiskSSTableBuilder("p1_result", 8, 0);
    PSSTableBuilder<KVSlice> *p2ResultBuilder = new PSplitFixedSizeKVDiskSSTableBuilder("p1_result", 8, 0);

    SSTable<KVSlice> *result1 = parallel_presort_join(2, outer, inner, inner_index, comparator, p1ResultBuilder);
    SSTable<KVSlice> *result2 = parallel_indexed_nested_loop_join(2, outer, inner, inner_index, comparator, p2ResultBuilder);
    check_output(result1, expected, 5);
    check_output(result2, expected, 5);
}


};