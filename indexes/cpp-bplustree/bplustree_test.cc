#include "bplustree.h"
using namespace bplustree;

#include <string.h>

#define NB_INSERTS 1000LU

static unsigned long x=123456789, y=362436069, z=521288629;
unsigned long xorshf96(void) {          //period 2^96-1
   unsigned long t;
   x ^= x << 16;
   x ^= x >> 5;
   x ^= x << 1;

   t = x;
   x = y;
   y = z;
   z = t ^ x ^ y;

   return z;
}

int main(int argc, char *argv[])
{
   declare_timer;
   bplus_tree database(argv[1], 1);

   start_timer {
      for(size_t i = 0; i < NB_INSERTS; i++) {
         uint64_t hash = xorshf96()%NB_INSERTS;
         leaf_index_t value;
         database.insert(hash, &value);
      }
   } stop_timer("BTREE - Time for %lu inserts/replace (%lu inserts/s)", NB_INSERTS, NB_INSERTS*1000000LU/elapsed);

   start_timer {
      for(size_t i = 0; i < NB_INSERTS; i++) {
         uint64_t hash = xorshf96()%NB_INSERTS;
         leaf_index_t value;
         database.search(hash, &value);
      }
   } stop_timer("BTREE - Time for %lu finds (%lu finds/s)", NB_INSERTS, NB_INSERTS*1000000LU/elapsed);
    
   return 0;
}

