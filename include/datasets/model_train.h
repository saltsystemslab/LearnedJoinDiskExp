#ifndef MODEL_TRAIN_H
#define MODEL_TRAIN_H

#include "slice_pgm_model.h"
#include "slice_plr_model.h"
#include "model.h"

Model<Slice> *train_model(Iterator<Slice> *it, BenchmarkInput *input) {
    ModelBuilder<Slice> *mb;
    if (input->index_type == PGM) {
        mb = new PGMModelBuilder(input->slice_to_point_converter);
    }
    else if (input->index_type == PLR) {
        mb = new SlicePLRModelBuilder(input->plr_error_bound, input->slice_to_point_converter);
    } else if (input->index_type == NO_MODEL) {
        mb = new DummyModelBuilder<Slice>();
    } else {
        printf("Unknown model\n");
        abort();
    }

    it->seekToFirst();
    auto model_build_start = std::chrono::high_resolution_clock::now();
    while (it->valid()) {
        mb->add(it->key());
        it->next();
    }
    Model<Slice> *m = mb->finish();
    auto model_build_end = std::chrono::high_resolution_clock::now();
    uint64_t iterator_build_duration_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            model_build_end - model_build_start)
            .count();
    float model_build_duration_sec = iterator_build_duration_ns / 1e9;
    printf("%s model creation duration: %.3lf sec\n", it->id().c_str(), model_build_duration_sec);
    printf("%s model size bytes: %lu\n", it->id().c_str(), m->size_bytes());
    return m;
}

#endif