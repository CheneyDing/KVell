#include "cpp-bplustree/bplustree.h"
#include "bplustree.h"

using namespace std;
using namespace bplustree;

extern "C"
{
   bplustree_t *bplustree_create(int worker_id) {
      char path[512];
      sprintf(path, BPLUSTREE_PATH, worker_id);
      bplustree_t *b = new bplus_tree(path, worker_id);
      return b;
   }

   int bplustree_find(bplustree_t *t, unsigned char* k, size_t len, struct index_entry *e) {
      uint64_t hash = *(uint64_t*)k;
      bplus_tree *b = static_cast<bplus_tree *>(t);
      leaf_index_t index;
      int res = b->search(hash, &index);
      if(res != -1) {
         e->slab_id = index.slab_id;
         e->slab_idx = index.slab_idx;
         return 1;
      } else {
         return 0;
      }
   }

   void bplustree_delete(btree_t *t, unsigned char*k, size_t len) {
      uint64_t hash = *(uint64_t*)k;
      bplus_tree *b = static_cast<bplus_tree *>(t);
      b->remove(hash);
   }

   void bplustree_insert(btree_t *t, unsigned char*k, size_t len, struct index_entry *e) {
      uint64_t hash = *(uint64_t*)k;
      bplus_tree *b = static_cast<bplus_tree *>(t);
      leaf_index_t value;
      value.hash = hash;
      value.slab_id = e->slab_id;
      value.slab_idx = e->slab_idx;
      b->insert(hash, &value);
   }

   struct index_scan bplustree_find_n(btree_t *t, unsigned char* k, size_t len, size_t n) {
      struct index_scan res;
      res.hashes = (uint64_t*) malloc(n*sizeof(*res.hashes));
      res.entries = (struct index_entry*) malloc(n*sizeof(*res.entries));
      res.nb_entries = 0;

      uint64_t hash = *(uint64_t*)k;
      bplus_tree *b = static_cast<bplus_tree *>(t);
      leaf_index_t *values = (leaf_index_t *)malloc(n*sizeof(leaf_index_t));
      int num = b->search_range(hash, values, n);
      int i = 0;
      while(res.nb_entries < num) {
         res.hashes[res.nb_entries] = values[i].hash;
         res.entries[res.nb_entries].slab_id = values[i].slab_id;
         res.entries[res.nb_entries].slab_idx = values[i].slab_idx;
         res.nb_entries++;
         i++;
      }

      return res;
   }

   void bplustree_forall_keys(btree_t *t, void (*cb)(uint64_t h, void *data), void *data) {
      bplus_tree *b = static_cast<bplus_tree *>(t);
      // Todo()
      return;
   }

   void bplustree_free(btree_t *t) {
      bplus_tree *b = static_cast<bplus_tree *>(t);
      delete b;
   }
}
