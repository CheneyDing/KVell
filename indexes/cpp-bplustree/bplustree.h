#ifndef BPT_H
#define BPT_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>

#include "../memory-item.h"
#include "../../utils.h"

#define BPLUSTREE_DEBUG

namespace bplustree {

/* offset */
#define OFFSET_META 0

/* path */
#define BPLUSTREE_PATH "/scratch/kvell/bplustree-%d"
#define BP_ORDER 20
#define FREE_LIST_NODE_MAX 128

/* meta information of B+ tree */
typedef struct {
    size_t order; /* `order` of B+ tree */
    size_t value_size; /* size of value */
    size_t key_size;   /* size of key */
    size_t internal_node_num; /* how many internal nodes */
    size_t leaf_node_num;     /* how many leafs */
    size_t height;            /* height of tree (exclude leafs) */
    size_t slot;        /* where to store new block */
    size_t free_list[FREE_LIST_NODE_MAX]; /* free node list */
    size_t free_count; /* how many free node in list */
    size_t root_offset; /* where is the root of internal nodes */
    size_t leaf_offset; /* where is the first leaf */
} meta_t;

/* internal nodes' index segment */
struct index_t {
    uint64_t hash;
    size_t child; /* child's offset */
};

/***
 * internal node block
 ***/
struct internal_node_t {
    typedef index_t * child_t;

    uint64_t parent; /* parent node offset */
    uint64_t next;
    uint64_t prev;
    uint16_t index_offset;
    size_t n; /* how many children */
    size_t size;
};

/* leaf node's index segment */
struct leaf_index_t {
    uint64_t hash;
    size_t slab_id;
    size_t slab_idx;
};

/* leaf node block */
struct leaf_node_t {
    typedef leaf_index_t * child_t;
    uint64_t parent;
    uint64_t next;
    uint64_t prev;
    uint16_t index_offset;
    size_t n; /* how many (k,v) pair */
    size_t size; /* node's size */
};

/* the encapulated B+ tree */
class bplus_tree {
public:
    bplus_tree(const char *path, int worker_id, bool force_empty = false);

    /* abstract operations */
    int search(uint64_t hash, struct leaf_index_t *value) const;
    int search_range(uint64_t hash, struct leaf_index_t *values, size_t n) const;
    int remove(uint64_t hash);
    int insert(uint64_t hash, struct leaf_index_t *value);
    int update(uint64_t hash, struct leaf_index_t *value);
    meta_t * get_meta() const {
        return meta;
    };

public:
    char path[512];
    meta_t *meta;
    int worker_id;

    /* init empty tree */
    void init_from_empty();

    /* find index */
    uint64_t search_index(uint64_t hash) const;

    /* find leaf */
    uint64_t search_leaf(uint64_t index, uint64_t hash) const;
    uint64_t search_leaf(uint64_t hash) const
    {
        return search_leaf(search_index(hash), hash);
    }

    /* find bag */
    uint64_t search_bag(uint64_t offset) const;

    /* remove internal node */
    void remove_from_index(size_t offset, internal_node_t *node,
                           uint64_t hash);

    /* borrow one key from other internal node */
    bool borrow_key(bool from_right, internal_node_t *borrower,
                    size_t offset);

    /* borrow one record from other leaf */
    bool borrow_key(bool from_right, leaf_node_t *borrower);

    /* change one's parent key to another key */
    void change_parent_child(size_t parent, uint64_t hash_o, uint64_t hash_n);

    /* merge right leaf to left leaf */
    void merge_leafs(leaf_node_t *left, leaf_node_t *right);

    void merge_keys(index_t *where, internal_node_t *left,
                    internal_node_t *right, bool change_where_key = false);

    /* insert into leaf without split */
    void insert_record_no_split(leaf_node_t *leaf,
                                uint64_t hash, struct leaf_index_t *value);

    /* add key to the internal node */
    void insert_key_to_index(size_t offset, uint64_t hash,
                             size_t value, size_t after);
    void insert_key_to_index_no_split(internal_node_t *node, uint64_t hash,
                                      size_t value);

    /* change children's parent */
    void reset_index_children_parent(index_t *begin, index_t *end,
                                     size_t parent);

    template<class T>
    void node_create(size_t offset, T *node, T *next, size_t next_offset);

    template<class T>
    void node_remove(T *prev, T *node);

    /* multi-level file open/close */
    mutable int fd;
    mutable int fd_level;
    //mutable FILE *fp;
    //mutable int fp_level;
    void open_file(const char *mode = "rb+") const
    {
        // `rb+` will make sure we can write everywhere without truncating file
        if (fd_level == 0) {
            fd = open(path, O_CREAT|O_RDWR, mode);
            if (fd == -1) {
                printf("fail to open file! %s\n", path);
                return;
            }
        }

        ++fd_level;
    }

    void close_file() const
    {
        if (fd_level == 1) {
            int res = close(fd);
            if (res == -1) {
                printf("fail to close file! %s\n", path);
                return;
            }
        }

        --fd_level;
    }

    /* alloc from disk */
    size_t alloc()
    {
        if (meta->free_count == 0) {
            size_t slot = meta->slot;
            meta->slot += PAGE_SIZE;
            return slot;
        }
        meta->free_count--;
        return meta->free_list[meta->free_count];
    }

    size_t alloc(leaf_node_t *leaf)
    {
        meta->leaf_node_num++;
        return alloc();
    }

    size_t alloc(internal_node_t *node)
    {
        meta->internal_node_num++;
        return alloc();
    }

    void unalloc(size_t offset) {
        assert(meta->free_count < FREE_LIST_NODE_MAX);
        meta->free_list[meta->free_count] = offset;
        meta->free_count++;
    }

    void unalloc(leaf_node_t *leaf, size_t offset)
    {
        --meta->leaf_node_num;
        unalloc(offset);
    }

    void unalloc(internal_node_t *node, size_t offset)
    {
        --meta->internal_node_num;
        unalloc(offset);
    }

    /* We need a unique hash for each page for the page cache */
    static uint64_t get_hash_for_page(int fd, uint64_t page_num) {
    return (((uint64_t)fd)<<40LU)+page_num; // Works for files less than 40EB
    }
    
    /* read block from disk */
    int map(void **block, size_t offset) const
    {
    #ifndef BPLUSTREE_DEBUG
        struct lru *lru;
        struct pagecache* p = get_pagecache(get_slab_context_by_worker_id(worker_id));
        uint64_t hash = get_hash_for_page(fd, offset / PAGE_SIZE);
        if (get_page(p, hash, block, &lru)) {
            return 1;
        }
    #else
        *block = malloc(PAGE_SIZE);
        printf("map! block:%p\n", *block);
    #endif
        open_file();
        size_t rd = pread(fd, *block, PAGE_SIZE, offset);
        close_file();
        return rd - 1;
    }

    template<class T>
    int map(T **block, size_t offset) const
    {
        return map((void **)block, offset);
    }

    /* write block to disk */
    int unmap(void **block, size_t offset) const
    {
        open_file();
        size_t wd = pwrite(fd, *block, PAGE_SIZE, offset);
        close_file();

        return wd - 1;
    }

    template<class T>
    int unmap(T **block, size_t offset) const
    {
        return unmap((void **)block, offset);
    }
};

}

#endif /* end of BPT_H */
