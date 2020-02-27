#include "bplustree.h"

#include <stdlib.h>

#include <list>
#include <algorithm>
using std::swap;
using std::binary_search;
using std::lower_bound;
using std::upper_bound;

namespace bplustree {

/* custom compare operator for STL algorithms */
OPERATOR_KEYCMP(index_t)
OPERATOR_KEYCMP(leaf_index_t)

/* helper iterating function */
template<class T>
inline typename T::child_t first_index(T *node) {
    return (typename T::child_t)((char *)node + node->index_offset);
}
template<class T>
inline typename T::child_t begin(T *node) {
    return first_index(node) + 0; 
}
template<class T>
inline typename T::child_t end(T *node) {
    return first_index(node) + node->n;
}

/* helper searching function */
inline index_t *find(internal_node_t *node, uint64_t hash) {
    if (hash) {
        return upper_bound(begin(node), end(node) - 1, hash);
    }
    // because the end of the index range is an empty string, so if we search the empty key(when merge internal nodes), we need to return the second last one
    if (node->n > 1) {
        return first_index(node) + node->n - 2;
    }
    return begin(node);
}
inline leaf_index_t *find(leaf_node_t *node, uint64_t hash) {
    return lower_bound(begin(node), end(node), hash);
}

bplus_tree::bplus_tree(const char *p, int worker_id, bool force_empty)
    : worker_id(worker_id)
{
    memset(path, 0, sizeof(path));
    strcpy(path, p);
       
    if (!force_empty) { 
        // read tree from file
        if (map(&meta, OFFSET_META) != 0) {
            force_empty = true;
        }
    }

    if (force_empty) {
        open_file("w+"); // truncate file

        // create empty tree if file doesn't exist
        init_from_empty();
        close_file();
    }
}

int bplus_tree::search(uint64_t hash, struct leaf_index_t *value) const
{
    leaf_node_t *leaf;
    map(&leaf, search_leaf(hash));

    // finding the record
    leaf_index_t *key_index = find(leaf, hash);
    if (key_index != first_index(leaf) + leaf->n) {
        // always return the lower bound
        *value = *key_index;

        return key_index->hash - hash;
    } else {
        return -1;
    }
}

int bplus_tree::search_range(uint64_t hash, struct leaf_index_t *values, size_t n) const
{
    uint64_t off_leaf = search_leaf(hash);
    uint64_t off = off_leaf;
    size_t i = 0;
    leaf_index_t *b, *e;

    leaf_node_t *leaf;
    while (off != 0 && i < n) {
        map(&leaf, off);

        // start point
        if (off_leaf == off) 
            b = find(leaf, hash);
        else
            b = begin(leaf);

        // copy
        e = end(leaf);
        for (; b != e && i < n; ++b, ++i) {
            values[i] = *b;
        }

        off = leaf->next;
    }

    return i;
}

int bplus_tree::remove(uint64_t hash)
{
    internal_node_t *parent;
    leaf_node_t *leaf;

    // find parent node
    uint64_t parent_off = search_index(hash);
    map(&parent, parent_off);

    // find current node
    index_t *where = find(parent, hash);
    uint64_t offset = where->child;
    map(&leaf, offset);

    // verify
    if (!binary_search(begin(leaf), end(leaf), hash))
        return -1;

    size_t min_n = meta->leaf_node_num == 1 ? 0 : meta->order / 2;
    assert(leaf->n >= min_n && leaf->n <= meta->order);

    // delete the key
    leaf_index_t *to_delete = find(leaf, hash);
    std::copy(to_delete + 1, end(leaf), to_delete);
    leaf->n--;
    leaf->size -= sizeof(leaf_index_t);
    assert(leaf->size < PAGE_SIZE);

    // merge or borrow
    if (leaf->n < min_n) {
        // first borrow from left
        bool borrowed = false;
        if (leaf->prev != 0)
            borrowed = borrow_key(false, leaf);

        // then borrow from right
        if (!borrowed && leaf->next != 0)
            borrowed = borrow_key(true, leaf);

        // finally we merge
        if (!borrowed) {
            assert(leaf->next != 0 || leaf->prev != 0);

            uint64_t index_hash;

            if (where == end(parent) - 1) {
                // if leaf is last element then merge | prev | leaf |
                assert(leaf->prev != 0);
                leaf_node_t *prev;
                map(&prev, leaf->prev);
                index_hash = begin(prev)->hash;

                merge_leafs(prev, leaf);
                node_remove(prev, leaf);
                unmap(&prev, leaf->prev);
            } else {
                // else merge | leaf | next |
                assert(leaf->next != 0);
                leaf_node_t *next;
                map(&next, leaf->next);
                index_hash = begin(leaf)->hash;

                merge_leafs(leaf, next);
                node_remove(leaf, next);
                unmap(&leaf, offset);
            }

            // remove parent's key
            remove_from_index(parent_off, parent, index_hash);
        } else {
            unmap(&leaf, offset);
        }
    } else {
        unmap(&leaf, offset);
    }

    return 0;
}

int bplus_tree::insert(uint64_t hash, struct leaf_index_t *value)
{
    uint64_t parent = search_index(hash);
    uint64_t offset = search_leaf(parent, hash);
    leaf_node_t *leaf;
    map(&leaf, offset);

    printf("insert --- leaf:%p, offset:%lu, hash:%lu\n", leaf, offset, hash);

    // check if we have the same key
    if (binary_search(begin(leaf), end(leaf), hash))
        return 1;

    if (leaf->n == meta->order) {
        // split when full

        // new sibling leaf
        leaf_node_t *new_leaf;
        size_t new_leaf_offset = alloc(new_leaf);
        map(&new_leaf, new_leaf_offset);
        new_leaf->n = 0;
        new_leaf->index_offset = sizeof(leaf_node_t);
        new_leaf->size = PAGE_SIZE - sizeof(leaf_node_t);
        node_create(offset, leaf, new_leaf, new_leaf_offset);

        printf("insert --- split, new_leaf_offset:%lu leaf->next:%lu\n", new_leaf_offset, leaf->next);

        // find even split point
        size_t point = leaf->n / 2;
        bool place_right = hash > (first_index(leaf) + point)->hash;
        if (place_right)
            ++point;

        // split
        std::copy(first_index(leaf) + point,  end(leaf),
                  first_index(new_leaf));
        new_leaf->n = leaf->n - point;
        leaf->n = point;

        // which part do we put the key
        if (place_right)
            insert_record_no_split(new_leaf, hash, value);
        else
            insert_record_no_split(leaf, hash, value);

        printf("place_right:%d leaf->next:%lu\n", place_right, leaf->next);

        // save leafs
        unmap(&leaf, offset);
        unmap(&new_leaf, leaf->next);

        // insert new index key
        insert_key_to_index(parent, first_index(new_leaf)->hash,
                            offset, leaf->next);
    } else {
        insert_record_no_split(leaf, hash, value);
        unmap(&leaf, offset);
    }

    return 0;
}

int bplus_tree::update(uint64_t hash, struct leaf_index_t *value)
{
    uint64_t offset = search_leaf(hash);
    leaf_node_t *leaf;
    map(&leaf, offset);

    leaf_index_t *record = find(leaf, hash);
    if (record != end(leaf))
        if (hash == record->hash) {
            record->slab_id = value->slab_id;
            record->slab_idx = value->slab_idx;
            unmap(&leaf, offset);

            return 0;
        } else {
            return 1;
        }
    else
        return -1;
}

void bplus_tree::remove_from_index(uint64_t offset, internal_node_t *node,
                                   uint64_t hash)
{
    size_t min_n = meta->root_offset == offset ? 1 : meta->order / 2;
    assert(node->n >= min_n && node->n <= meta->order);

    // remove key
    uint64_t index_hash = begin(node)->hash;
    index_t *to_delete = find(node, hash);
    if (to_delete != end(node)) {
        (to_delete + 1)->child = to_delete->child;
        std::copy(to_delete + 1, end(node), to_delete);
    }
    node->n--;

    // remove to only one key
    if (node->n == 1 && meta->root_offset == offset &&
                       meta->internal_node_num != 1)
    {
        unalloc(node, meta->root_offset);
        meta->height--;
        meta->root_offset = first_index(node)->child;
        unmap(&meta, OFFSET_META);
        return;
    }

    // merge or borrow
    if (node->n < min_n) {
        internal_node_t *parent;
        map(&parent, node->parent);

        // first borrow from left
        bool borrowed = false;
        if (offset != begin(parent)->child)
            borrowed = borrow_key(false, node, offset);

        // then borrow from right
        if (!borrowed && offset != (end(parent) - 1)->child)
            borrowed = borrow_key(true, node, offset);

        // finally we merge
        if (!borrowed) {
            assert(node->next != 0 || node->prev != 0);

            if (offset == (end(parent) - 1)->child) {
                // if leaf is last element then merge | prev | leaf |
                assert(node->prev != 0);
                internal_node_t *prev;
                map(&prev, node->prev);

                // merge
                index_t *where = find(parent, begin(prev)->hash);
                reset_index_children_parent(begin(node), end(node), node->prev);
                merge_keys(where, prev, node, true);
                unmap(&prev, node->prev);
            } else {
                // else merge | leaf | next |
                assert(node->next != 0);
                internal_node_t *next;
                map(&next, node->next);

                // merge
                index_t *where = find(parent, index_hash);
                reset_index_children_parent(begin(next), end(next), offset);
                merge_keys(where, node, next);
                unmap(&node, offset);
            }

            // remove parent's key
            remove_from_index(node->parent, parent, index_hash);
        } else {
            unmap(&node, offset);
        }
    } else {
        unmap(&node, offset);
    }
}

bool bplus_tree::borrow_key(bool from_right, internal_node_t *borrower,
                            uint64_t offset)
{
    typedef typename internal_node_t::child_t child_t;

    uint64_t lender_off = from_right ? borrower->next : borrower->prev;
    internal_node_t *lender;
    map(&lender, lender_off);

    assert(lender->n >= meta->order / 2);
    if (lender->n != meta->order / 2) {
        child_t where_to_lend, where_to_put;

        internal_node_t *parent;

        // swap keys, draw on paper to see why
        if (from_right) {
            where_to_lend = begin(lender);
            where_to_put = end(borrower);

            map(&parent, borrower->parent);
            child_t where = lower_bound(begin(parent), end(parent) - 1,
                                        (end(borrower) -1)->hash);
            where->hash = where_to_lend->hash;
            unmap(&parent, borrower->parent);
        } else {
            where_to_lend = end(lender) - 1;
            where_to_put = begin(borrower);

            map(&parent, lender->parent);
            child_t where = find(parent, begin(lender)->hash);
            // where_to_put->key = where->key;  // We shouldn't change where_to_put->key, because it just records the largest info but we only changes a new one which have been the smallest one
            where->hash = (where_to_lend - 1)->hash;
            unmap(&parent, lender->parent);
        }

        // store
        std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
        *where_to_put = *where_to_lend;
        borrower->n++;

        // erase
        reset_index_children_parent(where_to_lend, where_to_lend + 1, offset);
        std::copy(where_to_lend + 1, end(lender), where_to_lend);
        lender->n--;
        unmap(&lender, lender_off);
        return true;
    }

    return false;
}

bool bplus_tree::borrow_key(bool from_right, leaf_node_t *borrower)
{
    uint64_t lender_off = from_right ? borrower->next : borrower->prev;
    leaf_node_t *lender;
    map(&lender, lender_off);

    assert(lender->n >= meta->order / 2);
    if (lender->n != meta->order / 2) {
        typename leaf_node_t::child_t where_to_lend, where_to_put;

        // decide offset and update parent's index key
        if (from_right) {
            where_to_lend = begin(lender);
            where_to_put = end(borrower);
            change_parent_child(borrower->parent, begin(borrower)->hash,
                                (first_index(lender) + 1)->hash);
        } else {
            where_to_lend = end(lender) - 1;
            where_to_put = begin(borrower);
            change_parent_child(lender->parent, begin(lender)->hash,
                                where_to_lend->hash);
        }

        // store
        std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
        *where_to_put = *where_to_lend;
        borrower->n++;

        // erase
        std::copy(where_to_lend + 1, end(lender), where_to_lend);
        lender->n--;
        unmap(&lender, lender_off);
        return true;
    }

    return false;
}

void bplus_tree::change_parent_child(uint64_t parent, uint64_t o,
                                     uint64_t n)
{
    internal_node_t *node;
    map(&node, parent);

    index_t *w = find(node, o);
    assert(w != end(node)); 

    w->hash = n;
    unmap(&node, parent);
    if (w == first_index(node) + node->n - 1) {
        change_parent_child(node->parent, o, n);
    }
}

void bplus_tree::merge_leafs(leaf_node_t *left, leaf_node_t *right)
{
    std::copy(begin(right), end(right), end(left));
    left->n += right->n;
}

void bplus_tree::merge_keys(index_t *where,
                            internal_node_t *node, internal_node_t *next, bool change_where_key)
{
    //(end(node) - 1)->key = where->key;
    if (change_where_key) {
        where->hash = (end(next) - 1)->hash;
    }
    std::copy(begin(next), end(next), end(node));
    node->n += next->n;
    node_remove(node, next);
}

void print_leaf_info(leaf_node_t *leaf) {
    leaf_index_t *iter = begin(leaf);
    while(iter != end(leaf)) {
        printf("iter:%p hash:%lu\n", iter, iter->hash);
        iter++;
    }
}

void bplus_tree::insert_record_no_split(leaf_node_t *leaf,
                                        uint64_t hash, leaf_index_t *value)
{
    leaf_index_t *where = upper_bound(begin(leaf), end(leaf), hash);
    //print_leaf_info(leaf);
    std::copy_backward(where, end(leaf), end(leaf) + 1);
    //print_leaf_info(leaf);

    where->hash = hash;
    where->slab_id = value->slab_id;
    where->slab_idx = value->slab_idx;
    leaf->n++;
}

void bplus_tree::insert_key_to_index(uint64_t offset, uint64_t hash,
                                     uint64_t old, uint64_t after)
{
    if (offset == 0) {
        // create new root node
        internal_node_t *root;
        root->next = root->prev = root->parent = 0;
        meta->root_offset = alloc(root);
        root->n = 1;
        root->index_offset = sizeof(internal_node_t);
        root->size = PAGE_SIZE - sizeof(internal_node_t) - sizeof(index_t);
        meta->height++;

        // insert `old` and `after`
        root->n = 2;
        index_t *root_child = first_index(root);
        root_child[0].hash = hash;
        root_child[0].child = old;
        root_child[1].child = after;

        unmap(&meta, OFFSET_META);
        unmap(&root, meta->root_offset);

        // update children's parent
        reset_index_children_parent(begin(root), end(root),
                                    meta->root_offset);
        return;
    }

    internal_node_t *node;
    map(&node, offset);
    assert(node->n <= meta->order);

    if (node->n == meta->order) {
        // split when full

        internal_node_t *new_node;
        size_t new_node_offset = alloc(new_node);
        map(&new_node, new_node_offset);
        new_node->n = 1;
        new_node->index_offset = sizeof(internal_node_t);
        new_node->size = PAGE_SIZE - sizeof(internal_node_t) - sizeof(index_t);
        node_create(offset, node, new_node, new_node_offset);

        // find even split point
        size_t point = (node->n - 1) / 2;
        bool place_right = hash > (first_index(node) + point)->hash;
        if (place_right)
            ++point;

        // prevent the `key` being the right `middle_key`
        // example: insert 48 into |42|45| 6|  |
        if (place_right && hash < (first_index(node) + point)->hash)
            point--;

        uint64_t middle_key = (first_index(node) + point)->hash;

        // split
        std::copy(begin(node) + point + 1, end(node), begin(new_node));
        new_node->n = node->n - point - 1;
        node->n = point + 1;

        // put the new key
        if (place_right)
            insert_key_to_index_no_split(new_node, hash, after);
        else
            insert_key_to_index_no_split(node, hash, after);

        unmap(&node, offset);
        unmap(&new_node, node->next);

        // update children's parent
        reset_index_children_parent(begin(new_node), end(new_node), node->next);

        // give the middle key to the parent
        // note: middle key's child is reserved
        insert_key_to_index(node->parent, middle_key, offset, node->next);
    } else {
        insert_key_to_index_no_split(node, hash, after);
        unmap(&node, offset);
    }
}

void print_internal_info(internal_node_t *node) {
    index_t * iter = begin(node);
    while (iter != end(node)) {
        printf("iter:%p, hash:%lu, child:%lu\n", iter, iter->hash, iter->child);
        iter++;
    }
}

void bplus_tree::insert_key_to_index_no_split(internal_node_t *node,
                                              uint64_t hash, uint64_t value)
{
    index_t *where = upper_bound(begin(node), end(node) - 1, hash);

    //print_internal_info(node);
    // move later index forward
    std::copy_backward(where, end(node), end(node) + 1);

    // insert this key
    where->hash = hash;
    where->child = (where + 1)->child;
    (where + 1)->child = value;

    node->n++;
    //print_internal_info(node);
}

void bplus_tree::reset_index_children_parent(index_t *begin, index_t *end,
                                             uint64_t parent)
{
    // this function can change both internal_node_t and leaf_node_t's parent
    // field, but we should ensure that:
    // 1. sizeof(internal_node_t) <= sizeof(leaf_node_t)
    // 2. parent field is placed in the beginning and have same size
    internal_node_t *node;
    while (begin != end) {
        map(&node, begin->child);
        node->parent = parent;
        unmap(&node, begin->child);
        ++begin;
    }
}

size_t bplus_tree::search_index(uint64_t hash) const
{
    size_t org = meta->root_offset;
    int height = meta->height;
    while (height > 1) {
        internal_node_t *node;
        map(&node, org);    // read from file, org is the node's offset in file

        index_t *i = upper_bound(begin(node), end(node) - 1, hash);
        org = i->child;
        --height;
    }

    return org;
}

uint64_t bplus_tree::search_leaf(uint64_t index, uint64_t hash) const
{
    internal_node_t *node;
    map(&node, index);

    index_t *i = upper_bound(begin(node), end(node) - 1, hash);
    return i->child;
}

template<class T>
void bplus_tree::node_create(uint64_t offset, T *node, T *next, size_t next_offset)
{
    // new sibling node
    next->parent = node->parent;
    next->next = node->next;
    next->prev = offset;
    node->next = next_offset;
    // update next node's prev
    if (next->next != 0) {
        T *old_next;
        map(&old_next, next->next);
        old_next->prev = node->next;
        unmap(&old_next, next->next);
    }
    unmap(&meta, OFFSET_META);
}

template<class T>
void bplus_tree::node_remove(T *prev, T *node)
{
    unalloc(node, prev->next);
    prev->next = node->next;
    if (node->next != 0) {
        T *next;
        map(&next, node->next);
        next->prev = node->prev;
        unmap(&next, node->next);
    }
    unmap(&meta, OFFSET_META);
}

void bplus_tree::init_from_empty()
{
    printf("init_from_empty! meta:%p\n", meta);
    // init default meta
    memset(meta, 0, sizeof(meta_t));
    meta->order = BP_ORDER;
    meta->value_size = sizeof(leaf_index_t);
    meta->key_size = sizeof(uint64_t);
    meta->height = 1;
    meta->slot = PAGE_SIZE;
    meta->free_count = 0;

    // init root node
    internal_node_t *root;
    meta->root_offset = alloc(root);
    map(&root, meta->root_offset);
    root->n = 1;
    root->index_offset = sizeof(internal_node_t);
    root->size = PAGE_SIZE - sizeof(internal_node_t) - sizeof(index_t);
    root->next = root->prev = root->parent = 0;

    printf("root_offset:%lu\n", meta->root_offset);

    // init empty leaf
    leaf_node_t *leaf;
    meta->leaf_offset = first_index(root)->child = alloc(leaf);
    map(&leaf, meta->leaf_offset);
    leaf->n = 0;
    leaf->index_offset = sizeof(leaf_node_t);
    leaf->size = PAGE_SIZE - sizeof(leaf_node_t);
    leaf->next = leaf->prev = 0;
    leaf->parent = meta->root_offset;

    printf("leaf_offset:%lu\n", meta->leaf_offset);
    // save
    unmap(&meta, OFFSET_META);
    unmap(&root, meta->root_offset);
    unmap(&leaf, first_index(root)->child);
}

}
