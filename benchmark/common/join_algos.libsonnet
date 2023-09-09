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
            "algo": "inlj_btree",
            "name": "BTree"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "onelevel_pgm8"
            },
            "name": "ONELEVEL_PGM_8"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "pgm8"
            },
            "name": "PGM_8"
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
               "error_bound": 8,
               "type": "plr"
            },
            "name": "PLR_8"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "rbtree"
            },
            "name": "RbTree"
         },
      ]
}
