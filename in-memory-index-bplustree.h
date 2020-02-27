#ifndef IN_MEMORY_BPLUSTREE
#define IN_MEMORY_BPLUSTREE 1

#include "indexes/bplustree.h"

#define INDEX_TYPE "bplustree"
#define memory_index_items_init bplustree_items_init
#define memory_index_init bplustree_init
#define memory_index_add bplustree_index_add
#define memory_index_lookup bplustree_worker_lookup
#define memory_index_delete bplustree_worker_delete
#define memory_index_scan bplustree_init_scan

void bplustree_items_init(void);
void bplustree_init(size_t worker_id);
struct index_entry *bplustree_worker_lookup(int worker_id, void *item);
void bplustree_worker_delete(int worker_id, void *item);
struct index_scan bplustree_init_scan(void *item, size_t scan_size);
void bplustree_index_add(struct slab_callback *cb, void *item);

#endif

