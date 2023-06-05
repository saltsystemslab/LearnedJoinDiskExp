#ifndef LEARNED_MERGE_H
#define LEARNED_MERGE_H

#include <fcntl.h>
#include <unistd.h>

#include <cmath>
#include <iostream>

#include "comparator.h"
#include "iterator.h"

#if TRACK_STATS
static int cluster_count_fd;
static int cluster_file_offset;
static int plr_error_fd;
static int plr_error_offset;
#endif

template <class T> class LearnedMerger {
public:
  static Iterator<T> *merge(Iterator<T> **iterators, int n,
                            Comparator<T> *comparator,
                            IteratorBuilder<T> *result) {
#if TRACK_STATS
    comparator = new CountingComparator<T>(comparator);
    cluster_count_fd = open("./DB/cluster.txt", O_WRONLY | O_CREAT | O_TRUNC,
                            S_IRUSR | S_IWUSR);
    plr_error_fd = open("./DB/plr_error.txt", O_WRONLY | O_CREAT | O_TRUNC,
                        S_IRUSR | S_IWUSR);
    cluster_file_offset = 0;
    plr_error_offset = 0;
#endif
    printf("LearnedMerge!\n");
    for (int i = 0; i < n; i++) {
      iterators[i]->seekToFirst();
    }
    Iterator<T> *smallest = nullptr;
    Iterator<T> *second_smallest = nullptr;
    findTwoSmallest(iterators, n, comparator, &smallest, &second_smallest);

    while (smallest != nullptr) {
      // There's only one list left, so let's fill up all the way.
      if (second_smallest == nullptr) {
        while (smallest->valid()) {
#if USE_BULK_COPY
          result->bulkAdd(smallest, 1);
#else
          result->add(smallest->key());
          smallest->next();
#endif
        }
        smallest = nullptr;
        continue;
      }
      addClusterToResult(smallest, second_smallest, comparator, result);
      smallest = second_smallest;
      second_smallest = nullptr;
      findSecondSmallest(iterators, n, comparator, smallest, &second_smallest);
    }
#if TRACK_STATS
    CountingComparator<T> *count_comp =
        dynamic_cast<CountingComparator<T> *>(comparator);
    std::cout << "Comparison Count: " << count_comp->get_count() << std::endl;
    close(cluster_count_fd);
    close(plr_error_fd);
#endif
    return result->finish();
  }

private:
  static void addClusterToResult(Iterator<T> *smallest,
                                 Iterator<T> *second_smallest,
                                 Comparator<T> *comparator,
                                 IteratorBuilder<T> *merge_result_builder) {
#if TRACK_STATS
    int cluster_length = 0;
#endif
    // approx_pos is always a valid position in iterator.
    uint64_t approx_pos = std::ceil(smallest->guessPositionMonotone(second_smallest->key()));
    approx_pos = std::max(approx_pos, smallest->current_pos());
    bool is_overshoot = false;
    if (comparator->compare(smallest->peek(approx_pos),
                            second_smallest->key()) > 0) {
      approx_pos--;
      is_overshoot = true;
    }
    // Correct the overshoot
    if (is_overshoot) {
#if TRACK_STATS
      int overshoot_error = 1; // 1 for the check to see if we overshot.
#endif

      while (comparator->compare(smallest->peek(approx_pos),
                                 second_smallest->key()) > 0) {
        approx_pos--;
#if TRACK_STATS
        overshoot_error++;
#endif
      }
#if TRACK_STATS
      overshoot_error++; // 1 for the comparison that exited.
      std::string entry =
          "overshoot," + std::to_string(overshoot_error) + ",\n";
      plr_error_offset +=
          pwrite(plr_error_fd, entry.c_str(), entry.size(), plr_error_offset);
#endif
    }
#if USE_BULK_COPY
    merge_result_builder->bulkAdd(smallest,
                                  approx_pos - smallest->current_pos() + 1);
#else
    // Blind copy until approx_pos
    while (smallest->current_pos() <= approx_pos) {
      merge_result_builder->add(smallest->key());
      smallest->next();
#if TRACK_STATS
      cluster_length++;
#endif
    }
#endif
    // If we overshot, we copied all the items we wanted for this cluster....
    if (is_overshoot) {
#if TRACK_STATS
      std::string entry =
          smallest->identifier() + "," + std::to_string(cluster_length) + ",\n";
      cluster_file_offset += pwrite(cluster_count_fd, entry.c_str(),
                                    entry.size(), cluster_file_offset);
#endif
      return;
    }
    // ....else, there might still be items to be taken from this list.
#if TRACK_STATS
    int undershoot_error = 1; // 1 for the overshot check.
#endif
    while (smallest->valid() &&
           comparator->compare(smallest->key(), second_smallest->key()) <= 0) {
#if USE_BULK_COPY
      merge_result_builder->bulkAdd(smallest, 1);
#else
      merge_result_builder->add(smallest->key());
      smallest->next();
#endif
#if TRACK_STATS
      cluster_length++;
      undershoot_error++;
#endif
    }
#if TRACK_STATS
    undershoot_error++; // 1 for the exit.
    std::string entry = "undershoot," + std::to_string(undershoot_error) + "\n";
    plr_error_offset +=
        pwrite(plr_error_fd, entry.c_str(), entry.size(), plr_error_offset);
    entry =
        smallest->identifier() + "," + std::to_string(cluster_length) + ",\n";
    cluster_file_offset += pwrite(cluster_count_fd, entry.c_str(), entry.size(),
                                  cluster_file_offset);
#endif
  }

  static void findTwoSmallest(Iterator<T> **iterators, int n,
                              Comparator<T> *comparator, Iterator<T> **smallest,
                              Iterator<T> **second_smallest) {
    for (int i = 0; i < n; i++) {
      if (!iterators[i]->valid()) {
        continue;
      }
      if (*smallest == nullptr) {
        *smallest = iterators[i];
        continue;
      }
      if (comparator->compare((*smallest)->key(), iterators[i]->key()) > 0) {
        *second_smallest = *smallest;
        *smallest = iterators[i];
        continue;
      }
      if (*second_smallest == nullptr) {
        *second_smallest = iterators[i];
        continue;
      }
      if (comparator->compare((*second_smallest)->key(), iterators[i]->key()) >
          0) {
        *second_smallest = iterators[i];
        continue;
      }
    }
  }

  static void findSecondSmallest(Iterator<T> **iterators, int n,
                                 Comparator<T> *comparator,
                                 const Iterator<T> *smallest,
                                 Iterator<T> **second_smallest) {
    for (int i = 0; i < n; i++) {
      if (!iterators[i]->valid()) {
        continue;
      }
      if (smallest == iterators[i]) {
        continue;
      }
      if (*second_smallest == nullptr) {
        *second_smallest = iterators[i];
        continue;
      }
      if (comparator->compare((*second_smallest)->key(), iterators[i]->key()) >
          0) {
        *second_smallest = iterators[i];
        continue;
      }
    }
  }
};
#endif
