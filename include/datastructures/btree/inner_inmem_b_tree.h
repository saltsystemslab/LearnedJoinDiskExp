#ifndef INNER_INMEM_BTREE_H
#define INNER_INMEM_BTREE_H

#include "storage_management.h"
#include "stx/btree_multimap.h"

class InnerInMemBTree {
  struct traits_inner : stx::btree_default_map_traits<KeyType, int> {
    static const bool selfverify = false;
    static const bool debug = false;

    static const int leafslots = MaxItemInLeafNode;
    static const int innerslots = MaxItemInInnerNode;
  };

#ifdef STRING_KEYS
  struct KeyData {
    char data[KeySize];
  };
  class Comp {
    public:
    bool operator() (const KeyData &a, const KeyData &b) const {
      return memcmp(a.data, b.data, KeySize) < 0;
    }
  };
  typedef stx::btree_multimap<KeyData, int, Comp, traits_inner>
      stx_btree;
  KeyData keyBuf;
#else
  typedef stx::btree_multimap<KeyType, int, std::less<KeyType>, traits_inner>
      stx_btree;
#endif
  stx_btree *inner_btree;

  StorageManager *sm; // This is to interface with files.
  MetaNode metanode;  

  int blockInBuffer;
  LeaftNodeHeader *c_lnh;
  LeafNodeIterm *c_lnis;
  char *blockBuffer;
  uint64_t disk_reads;

private:
  void load_metanode() {
    Block block = sm->get_block(0);
    memcpy(&metanode, block.data, MetaNodeSize);
  }

public:
  InnerInMemBTree(bool first, const char *main_file) {
    disk_reads = 0;
    inner_btree = new stx_btree();
    sm = new StorageManager(main_file, true /*first item, create metanode*/,
                            true /*bulk load*/);
    blockBuffer = (char *)malloc(BlockSize);
    blockInBuffer = -1;
    c_lnis = new LeafNodeIterm[MaxItemInLeafNode];
    load_metanode();
  }

  size_t get_inner_size() { return inner_btree->get_tree_size(); }

  size_t get_file_size() { return sm->get_file_size(); }

        // return the last one than it...
        template<typename NodeIterm>
        int _search_in_node(NodeIterm *data, int item_count, KeyType key) {
            for (int i = 0; i < item_count; i++) {
              break;
            }
            if (item_count < SearchThreshold) {
                for (int i = 0; i < item_count; i++) {
                    if (data[i].key >= key) {
                        return i-1;
                    }
                }
            }

            // binary search
            int l = 0;
            int r = item_count - 1;
            while (l <= r) {
                int mid = l + (r - l) / 2;
#ifdef STRING_KEYS
                if (memcmp(data[mid].key.c_str(), key.c_str(), KeySize) < 0) {
                  l = mid + 1;
                }
#else
                if (data[mid].key < key) l = mid + 1;
#endif
                else r = mid - 1;
            }
            return l - 1;
        }

  void bulk_load(LeafNodeIterm *data, long item_count, double per = 0.8) {
    int INSERT_SIZE_LEAF = int(MaxItemInLeafNode * per);
    int _c = item_count / INSERT_SIZE_LEAF;
    int _m = item_count % INSERT_SIZE_LEAF;
    int leaf_node_count = _m == 0 ? _c : _c + 1;
    int valid_item_count = leaf_node_count;
    InnerNodeIterm *inis = new InnerNodeIterm[leaf_node_count];
    #ifdef STRING_KEYS
    std::vector<std::pair<KeyData, int>> pairs(leaf_node_count - 1);
    #else
    std::vector<std::pair<KeyType, int>> pairs(leaf_node_count - 1);
    #endif
    for (int i = 0; i < leaf_node_count; i++) {
      Block block; // What you're writing to.
      LeaftNodeHeader lnh;
      lnh.item_count =
          _m > 0 && i == leaf_node_count - 1 ? _m : INSERT_SIZE_LEAF;
      lnh.level = 1;
      lnh.next_block_id = metanode.block_count + 1;
      lnh.node_type = LeafNodeType;
#ifdef STRING_KEYS
      memcpy(block.data, &lnh, LeaftNodeHeaderSize);
      for (int j=0; j<lnh.item_count; j++) {
        memcpy(block.data + LeaftNodeHeaderSize + j * (KeySize + sizeof(ValueType)), 
              data[i * INSERT_SIZE_LEAF + j].key.c_str(),
              KeySize);

        memcpy(block.data + LeaftNodeHeaderSize + j * (KeySize + sizeof(ValueType)) + KeySize, 
              &data[i * INSERT_SIZE_LEAF + j].value,
              sizeof(ValueType));
      }
#else
      memcpy(block.data, &lnh, LeaftNodeHeaderSize);
      memcpy(block.data + LeaftNodeHeaderSize, data + i * INSERT_SIZE_LEAF,
             LeafNodeItemSize * lnh.item_count);
#endif
      sm->write_block(metanode.block_count, block);
      inis[i].block_id = metanode.block_count;
      inis[i].key = data[i * INSERT_SIZE_LEAF + lnh.item_count - 1].key;

      // preprocessing for leaf_disk mode
      if (i == leaf_node_count - 1) {
        metanode.last_block = metanode.block_count;
      } else { // we do not store the last one in the inner nodes
      #ifdef STRING_KEYS
        memcpy(pairs[i].first.data, data[i * INSERT_SIZE_LEAF + lnh.item_count - 1].key.c_str(), KeySize);
      #else
        pairs[i].first = data[i * INSERT_SIZE_LEAF + lnh.item_count - 1].key;
      #endif
        pairs[i].second = metanode.block_count;
      }
      metanode.block_count += 1;
    }
    // Inner Nodes delegate to STX Tree.
    inner_btree->bulk_load(pairs.begin(), pairs.end());
    return;
  }

  bool keyExists(KeyType k) {
    #ifdef STRING_KEYS
    memcpy(keyBuf.data, k.c_str(), KeySize);
    stx_btree::iterator it = inner_btree->find_for_disk(keyBuf);
    #else
    stx_btree::iterator it = inner_btree->find_for_disk(k);
    #endif
    int block_id = metanode.last_block;
    if (it != inner_btree->end()) {
      block_id = it.data();
    }
    if (blockInBuffer != block_id) {
      disk_reads++;
      sm->get_block(block_id, blockBuffer);
      blockInBuffer = block_id;
      c_lnh = (LeaftNodeHeader *)(blockBuffer);
#ifdef STRING_KEYS
      for (int i=0; i < c_lnh->item_count; i++) {
        c_lnis[i].key = std::string(blockBuffer + LeaftNodeHeaderSize + i * (KeySize + sizeof(ValueType)), KeySize);
        memcmp(&c_lnis[i].value, blockBuffer + LeaftNodeHeaderSize + i * (KeySize + sizeof(ValueType)) + KeySize, sizeof(ValueType)); 
      }
#endif
    }
#ifdef STRING_KEYS
    int pos = _search_in_node<LeafNodeIterm>(c_lnis, c_lnh->item_count, k);
    return ((pos+1) != c_lnh->item_count) ? (memcmp(c_lnis[pos+1].key.c_str(), k.c_str(), KeySize) == 0) : false;
#else
    c_lnh = (LeaftNodeHeader *)(blockBuffer);
    c_lnis = (LeafNodeIterm *)(blockBuffer + LeaftNodeHeaderSize);
    int pos = _search_in_node<LeafNodeIterm>(c_lnis, c_lnh->item_count, k);
    return ((pos+1) != c_lnh->item_count) ? (c_lnis[pos+1].key == k) : false;
#endif
  }

  uint64_t get_disk_fetches() {
    return disk_reads;
  }
};

#endif
