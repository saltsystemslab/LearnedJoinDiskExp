#ifndef LEARNEDLOOKUP_H
#define LEARNEDLOOKUP_H

#include "comparator.h"
#include "iterator.h"
#include "iterator_with_model.h"
#if TRACK_BINARY_SEARCH_STATS
static int binary_search_count_fd;
static int binary_search_count_offset;
#endif
class LearnedLookup
{
public:
        // static int binary_search_count_fd;
        // static int binary_search_count_offset;
#if TRACK_BINARY_SEARCH_STATS
        static void init() {
            binary_search_count_fd = open("./DB/binary_search_stats.txt", O_WRONLY | O_CREAT | O_TRUNC,
                            S_IRUSR | S_IWUSR);
            binary_search_count_offset=0;
        }
        static void teardown() {
            close(binary_search_count_fd);
        }

#endif
    template <class T>
    static float getApproxPos(IteratorWithModel<T> *iterator, T target_key) {
        float approx_pos = iterator->guessPositionUsingBinarySearch(target_key);
        return approx_pos;
    }
    template <class T>
    static bool lookupUsingApproxPos(IteratorWithModel<T> *iterator,
                       uint64_t num_keys,
                       Comparator<T> *comparator,
                        float approx_pos,
                        T target_key ) {
                        float start = approx_pos - PLR_ERROR_BOUND;
        float end = approx_pos + PLR_ERROR_BOUND;
        
        if (start < 0 || start >= num_keys)
        {
            start = 0;
        }
        if (end < 0 || end >= num_keys)
        {
            end = num_keys - 1;
        }
        while (comparator->compare(target_key, iterator->peek(start)) == -1)
        {
            start = start - PLR_ERROR_BOUND;
            if (start < 0)
            {
                start = 0;
                break;
            }
        }

        while (comparator->compare(target_key, iterator->peek(end)) == 1)
        {
            end = end + PLR_ERROR_BOUND;
            if (end >= num_keys)
            {
                end = num_keys - 1;
                break;
            }
        }
        #if TRACK_BINARY_SEARCH_STATS
        std::string start_end = std::to_string(start) + "," + std::to_string(end) + ",";
        binary_search_count_offset += pwrite(binary_search_count_fd, start_end.c_str(),
                                    start_end.size(), binary_search_count_offset);
        #endif
        bool status = binary_search(start, end, target_key, iterator, comparator);
        if (status == false)
        {
            //  KEY_TYPE sk = *((KEY_TYPE *)iterator->peek(start).data_);
            //  KEY_TYPE ek = *((KEY_TYPE *)iterator->peek(end).data_);
        }
        return status;
                       }
    template <class T> 
    static bool lookup(IteratorWithModel<T> *iterator,
                       uint64_t num_keys,
                       Comparator<T> *comparator,
                       T target_key)
    {
       
        float approx_pos = iterator->guessPositionUsingBinarySearch(target_key);
        float start = approx_pos - PLR_ERROR_BOUND;
        float end = approx_pos + PLR_ERROR_BOUND;
        
        if (start < 0 || start >= num_keys)
        {
            start = 0;
        }
        if (end < 0 || end >= num_keys)
        {
            end = num_keys - 1;
        }
        while (comparator->compare(target_key, iterator->peek(start)) == -1)
        {
            start = start - PLR_ERROR_BOUND;
            if (start < 0)
            {
                start = 0;
                break;
            }
        }

        while (comparator->compare(target_key, iterator->peek(end)) == 1)
        {
            end = end + PLR_ERROR_BOUND;
            if (end >= num_keys)
            {
                end = num_keys - 1;
                break;
            }
        }
        #if TRACK_BINARY_SEARCH_STATS
        std::string start_end = std::to_string(start) + "," + std::to_string(end) + ",";
        binary_search_count_offset += pwrite(binary_search_count_fd, start_end.c_str(),
                                    start_end.size(), binary_search_count_offset);
        #endif
        bool status = binary_search(start, end, target_key, iterator, comparator);
        if (status == false)
        {
            //  KEY_TYPE sk = *((KEY_TYPE *)iterator->peek(start).data_);
            //  KEY_TYPE ek = *((KEY_TYPE *)iterator->peek(end).data_);
        }
        return status;
    }

private:
    template <class T>
    static bool binary_search(int64_t start, int64_t end, T target_key, Iterator<T> *iterator, Comparator<T> *comparator)
    {
        #if TRACK_BINARY_SEARCH_STATS
        int count = 0;
        #endif
        while (start <= end)
        {
            #if TRACK_BINARY_SEARCH_STATS
            count+=1;
            #endif
            int64_t mid = (start + end) / 2;
            if (comparator->compare(iterator->peek(mid), target_key) == 0)
            {
                #if TRACK_BINARY_SEARCH_STATS
                std::string while_loop_cout = std::to_string(count)+"\n";
                binary_search_count_offset += pwrite(binary_search_count_fd, while_loop_cout.c_str(),
                                            while_loop_cout.size(), binary_search_count_offset);
                #endif
                return true;
            }
            else if (comparator->compare(iterator->peek(mid), target_key) < 0)
            {
                start = mid + 1;
            }
            else
            {
                end = mid - 1;
            }
        }
        #if TRACK_BINARY_SEARCH_STATS
        std::string while_loop_count = std::to_string(count)+"\n";
        binary_search_count_offset += pwrite(binary_search_count_fd, while_loop_count.c_str(),
                                    while_loop_count.size(), binary_search_count_offset);
        #endif
        return false;
    }


};

#endif