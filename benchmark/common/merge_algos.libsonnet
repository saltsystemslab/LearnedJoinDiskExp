{
      get_merge_algos(key_type, key_size, value_size)::
      [
         {
            "algo": "standard_merge",
            "name": "standard_merge"
         },
         {
            "algo": "learned_merge_threshold",
            "index": {
               "type": "pgm64"
            },
            "name": "PGM_64",
            "threshold": 0
         },
         {
            "algo": "learned_merge_threshold",
            "index": {
               "type": "onelevel_pgm64"
            },
            "name": "ONELEVEL_PGM_64_THRESH",
            "threshold": 0
         },
         {
            "algo": "learned_merge_threshold",
            "index": {
               "type": "btree"
            },
            "name": "BTree",
            "threshold": 0
         },
      ]
}
