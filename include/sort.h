#ifndef SORT_H
#define SORT_H

#include <mutex>
#include <thread>
#include <queue>
#include <vector>
#include "slice_comparator.h"

using namespace std;

char *merge(char *a, uint64_t a_count, char *b, uint64_t b_count, uint64_t key_len_bytes, Comparator<Slice> *c) {
  if (b_count == 0) {
    return a;
  }
  char *result = new char[(a_count + b_count) * key_len_bytes];
  SliceArrayIterator *it1 = new SliceArrayIterator(a, a_count, key_len_bytes, "a");
  SliceArrayIterator *it2 = new SliceArrayIterator(b, b_count, key_len_bytes, "b");
  SliceArrayBuilder *res = new SliceArrayBuilder(result, a_count + b_count, key_len_bytes, "c");
  Iterator<Slice> **iterators = new Iterator<Slice> *[2];
  iterators[0] = it1;
  iterators[1] = it2;
  StandardMerger<Slice>::merge(iterators, 2, c, res);
  delete a;
  return result;
}


std::mutex mtx;
void pivot_work(int t_id, std::queue<pair<int64_t, int64_t>> *q, std::queue<pair<int64_t, int64_t>> *nq, char *arr, int key_size_bytes, Comparator<Slice> *c) {
  uint64_t iters = 0;
  char c1;
  char c2;
  while (true) {
    mtx.lock();
    if (q->empty()) {
      mtx.unlock();
      break;
    }
    int64_t start = q->front().first;
    int64_t end = q->front().second;
    q->pop();
    mtx.unlock();

    int64_t mid = start + (end - start) / 2; // TODO: Pick a random key.
    iters++;
    if (start >= end) {
      continue;
    }
    int64_t pivot = end;
    for (int i = 0; i < key_size_bytes; i++) {
      c1 = arr[pivot * key_size_bytes + i];
      c2 = arr[mid * key_size_bytes + i];
      arr[mid * key_size_bytes + i] = c1;
      arr[pivot * key_size_bytes + i] = c2;
    }
    Slice pivot_slice(arr + pivot * key_size_bytes, key_size_bytes);
    int64_t temp_pivot = start - 1;
    int64_t idx;
    for (idx = start; idx < end; idx++) {
      Slice cur(arr + idx * key_size_bytes, key_size_bytes);
      if (c->compare(cur, pivot_slice) <= 0) {
        temp_pivot += 1;
        if (temp_pivot == idx)
          continue;
        for (int64_t i = 0; i < key_size_bytes; i++) {
          c1 = arr[temp_pivot * key_size_bytes + i];
          c2 = arr[idx * key_size_bytes + i];
          arr[idx * key_size_bytes + i] = c1;
          arr[temp_pivot * key_size_bytes + i] = c2;
        }
      }
    }
    temp_pivot += 1;
    for (int64_t i = 0; i < key_size_bytes; i++) {
      c1 = arr[temp_pivot * key_size_bytes + i];
      c2 = arr[pivot * key_size_bytes + i];
      arr[pivot * key_size_bytes + i] = c1;
      arr[temp_pivot * key_size_bytes + i] = c2;
    }
    mtx.lock();
    if (start < temp_pivot - 1)
      nq->push(std::pair<int64_t, int64_t>(start, temp_pivot - 1));
    if (temp_pivot + 1 < end)
      nq->push(std::pair<int64_t, int64_t>(temp_pivot + 1, end));
    mtx.unlock();
  }
}

void p_qsort(char *arr, uint64_t num_keys, int key_size_bytes, Comparator<Slice> *c) {
  std::queue<pair<int64_t, int64_t>> *q, *nq;
  q = new std::queue<pair<int64_t, int64_t>>();
  nq = new std::queue<pair<int64_t, int64_t>>();
  printf("%ld\n", num_keys-1);
  q->push(std::pair<int64_t, int64_t>(0, num_keys-1));

  std::thread t[NUM_SORT_THREADS];
  while (!q->empty()) {
    for (int i=0; i<NUM_SORT_THREADS; i++) {
      t[i] = std::thread(pivot_work, i, q, nq, arr, key_size_bytes, Comparator<Slice> *c);
    }
    for (int i=0; i<NUM_SORT_THREADS; i++) {
      t[i].join();
    }
    swap(q, nq);
  }
  delete q;
  delete nq;
}

#endif