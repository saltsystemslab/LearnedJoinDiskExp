#include <bits/stdc++.h>

#include <cassert>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "config.h"
#include "slice_array_iterator.h"
#include "slice_comparator.h"
#include "slice_file_iterator.h"
#include "standard_lookup.h"
#include "learned_lookup.h"

using namespace std;

#if USE_INT_128 && !USE_STRING_KEYS
static int FLAGS_key_size_bytes = 16;
#elif !USE_STRING_KEYS
static int FLAGS_key_size_bytes = 8;
#else
static int FLAGS_key_size_bytes = 20;
#endif

static const int BUFFER_SIZE = 1000;
static KEY_TYPE FLAGS_universe_size = 2000000000000000;
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
static const char *FLAGS_num_keys = "10000";
static const char *FLAGS_DB_dir = "./DB";

vector<KEY_TYPE> generate_keys(uint64_t num_keys, KEY_TYPE universe)
{
    std::random_device rd;  // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<KEY_TYPE> distrib(1, universe);
    vector<KEY_TYPE> keys;
    for (int i = 0; i < num_keys; i++)
    {
        KEY_TYPE key = distrib(gen);
        keys.push_back(key);
    }
    sort(keys.begin(), keys.end());
    return keys;
}

string to_str(KEY_TYPE k)
{
    std::string r;
    while (k)
    {
        r.push_back('0' + k % 10);
        k = k / 10;
    }
    reverse(r.begin(), r.end());
    return r;
}

string to_fixed_size_key(KEY_TYPE key_value, int key_size)
{
    string key = to_str(key_value);
    string result = string(key_size - key.length(), '0') + key;
    return std::move(result);
}

bool is_flag(const char *arg, const char *flag)
{
    return strncmp(arg, flag, strlen(flag)) == 0;
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++)
    {
        double m;
        long long n;
        char junk;
        if (is_flag(argv[i], "--num_keys="))
        {
            FLAGS_num_keys = argv[i] + strlen("--num_keys=");
        }
        else if (sscanf(argv[i], "--key_size_bytes=%lld%c", &n, &junk) == 1)
        {
            FLAGS_key_size_bytes = n;
        }
        else if (sscanf(argv[i], "--universe_log=%lld%c", &n, &junk) == 1)
        {
            FLAGS_universe_size = 1;
            for (int i = 0; i < n; i++)
            {
                FLAGS_universe_size = FLAGS_universe_size << 1;
            }
        }
        else if (sscanf(argv[i], "--use_disk=%lld%c", &n, &junk) == 1)
        {
            FLAGS_disk_backed = n;
        }
        else if (sscanf(argv[i], "--print_result=%lld%c", &n, &junk) == 1)
        {
            FLAGS_print_result = n;
        }
        else
        {
            printf("WARNING: unrecognized flag %s\n", argv[i]);
        }
    }

    if (FLAGS_disk_backed)
    {
        system("mkdir -p DB");
    }

#if !USE_STRING_KEYS
    if (FLAGS_key_size_bytes != sizeof(KEY_TYPE))
    {
        abort();
    }
#endif

    std::cout << "Universe Size: " << to_str(FLAGS_universe_size) << std::endl;

    Iterator<Slice> *iterator;
    IteratorBuilder<Slice> *builder;
    if (FLAGS_disk_backed)
    {
        std::string fileName = "./DB/" + to_str(1) + ".txt";
        std::cout << fileName << std::endl;
        builder = new FixedSizeSliceFileIteratorBuilder(
            fileName.c_str(), BUFFER_SIZE, FLAGS_key_size_bytes, 0);
    }
    else
    {
        builder = new SliceArrayBuilder(stoi(FLAGS_num_keys), FLAGS_key_size_bytes, 0);
    }
    auto keys = generate_keys(stoi(FLAGS_num_keys), FLAGS_universe_size);
    auto queries = keys;
    for (int j = 0; j < stoi(FLAGS_num_keys); j++)
    {

#if USE_STRING_KEYS
        std::string key = to_fixed_size_key(keys[j], FLAGS_key_size_bytes);
        builder->add(Slice(key.c_str(), FLAGS_key_size_bytes));
#else
        builder->add(Slice((char *)(&keys[j]), FLAGS_key_size_bytes));
#endif
    }

    iterator = builder->finish();
    Comparator<Slice> *c = new SliceComparator();
   // auto queries = generate_keys(100, FLAGS_universe_size);
    auto lookup_start = std::chrono::high_resolution_clock::now();
    int rightAnswer = 0;
    int wrongAnswer = 0;
    for (int i = 0; i < queries.size(); i++)
    {
#if USE_STRING_KEYS
        std::string key = to_fixed_size_key(queries[i], FLAGS_key_size_bytes);
        Slice query = Slice(key.c_str(), FLAGS_key_size_bytes);
#else
        Slice query = Slice((char *)(&queries[i]), FLAGS_key_size_bytes);
#endif
#if LEARNED_MERGE
        bool status = LearnedLookup::lookup(iterator, stoi(FLAGS_num_keys), c, query);
        if(status == true) {
            rightAnswer++;
        }
        else {
            std::cout<<"query:"<<query.toString()<<std::endl;
            wrongAnswer++;
        }
#else
        bool status = StandardLookup::lookup(iterator, stoi(FLAGS_num_keys), c, query);
        if(status == true) {
            rightAnswer++;
        }
        else {
            wrongAnswer++;
        }
#endif
    }

    std::cout << "right answer count:" << rightAnswer<<std::endl;
    std::cout << "wrong answer count:" << wrongAnswer<<std::endl;

    auto lookup_end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        lookup_end - lookup_start)
                        .count();
    std::cout << "Lookup duration:" << duration<<std::endl;
    std::cout << "Ok!" << std::endl;
}
