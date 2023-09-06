{
      get_join_algos(key_type, key_size, value_size)::
      [
         {
            "algo": "sort_join",
            "name": "sort_join"
         },
         {
            "algo": "hash_join",
            "name": "hash_join"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "onelevel_pgm64"
            },
            "name": "ONELEVEL_PGM_64"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "pgm64"
            },
            "name": "PGM_64"
         },
         {
            "algo": "inlj",
            "index": {
               "error_bound": 64,
               "type": "plr"
            },
            "name": "PLR_64"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "rbtree"
            },
            "name": "RbTree"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "btree"
            },
            "name": "Btree"
         }
      ]
}