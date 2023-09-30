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
            "threshold": 5
         },
         {
            "algo": "learned_merge_threshold",
            "index": {
               "type": "onelevel_pgm64"
            },
            "name": "ONELEVEL_PGM_64_THRESH",
            "threshold": 5
         },
         {
            "algo": "learned_merge_threshold",
            "index": {
               "type": "rbtree"
            },
            "name": "RbTree",
            "threshold": 5
         },
      ]
}
