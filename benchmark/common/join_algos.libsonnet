{
      get_join_algos(key_type, key_size, value_size)::
      [
         {
            "algo": "sort_join",
            "name": "sort_join"
         },
         {
            "algo": "sort_join_exp",
            "name": "sort_join_exp"
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
            "algo": "inlj_pgm",
            "index": {
               "type": "onelevel_pgm8"
            },
            "name": "ONELEVEL_PGM_8"
         },
         {
            "algo": "inlj_pgm",
            "index": {
               "type": "pgm8"
            },
            "name": "PGM_8"
         },
         {
            "algo": "inlj_pgm",
            "index": {
               "type": "pgm64"
            },
            "name": "PGM_64"
         },
         {
            "algo": "inlj_pgm",
            "index": {
               "type": "pgm128"
            },
            "name": "PGM_128"
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
