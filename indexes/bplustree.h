#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "memory-item.h"
#include "../headers.h"
typedef void bplustree_t;

bplustree_t *bplustree_create(int worker_id);
int bplustree_find(bplustree_t *t, unsigned char*k, size_t len, struct index_entry *e);
void bplustree_delete(bplustree_t *t, unsigned char*k, size_t len);
void bplustree_insert(bplustree_t *t, unsigned char*k, size_t len, struct index_entry *e);
struct index_scan bplustree_find_n(bplustree_t *t, unsigned char* k, size_t len, size_t n);

void bplustree_forall_keys(bplustree_t *t, void (*cb)(uint64_t h, void *data), void *data);
void bplustree_free(bplustree_t *t);

#ifdef __cplusplus
}
#endif

#endif
