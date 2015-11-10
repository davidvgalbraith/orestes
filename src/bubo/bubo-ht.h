#pragma once

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "bubo-types.h"
#include "blob-store.h"
#include "utils.h"


#define DEFAULT_INIT_HASH_TABLE_SZ (4 << 10)
#define DEFAULT_MAX_HASH_TABLE_SZ (512 << 20)

#define RESIZE_THRESHOLD_PCT 97

/*
  BuboHashSet is a simple hash set in which one can insert any BYTE pointer except NULL, and do lookups.

  Internally, it is an array of 'struct Entry' type objects. Whenever there is a collision, a chain is
  started from the spine array.

      ^    |       |
      |    +-------+
      |    |Entry {|
           | val   |
      S    | next  |
      P    |}      |    <---- CHAIN -------->
      I    +-------+
      N    |Entry {|    +-------+    +-------+
      E    | val   |    |Entry {|    |Entry {|
           | next ----->| val   |    | val   |
      |    |}      |    | next ----> | next --->
      |    +-------+    |}      |    |}      |
      |    |Entry {|    +-------+    +-------+
      v    | val   |
           | next  |
           |}      |
           +-------+
           |       |

  Only disallowed value in the Bubo Hash Set is a NULL value for the BYTE pointer.

 */


struct BuboHashStat {
    uint64_t spine_len;         // Essentially, the size of the spine array.
    uint64_t spine_use;         // Current number spine entries being used.
    uint64_t entries;           // Total number of hash set entries that have been added.
    uint64_t ht_bytes;          // Current bytes used by the bubo hash set.
    uint64_t collision_slots;   // Number of spine slots that have collisions.
    uint64_t total_chain_len;   // Sum of all chain nodes. This also correspond to the number of collisions.
    uint64_t max_chain_len;     // Maximum chain length among all spine entries.
    uint64_t dist_1_2;          // Among the collision slots, number of chains with length [1,2]
    uint64_t dist_3_5;          // Among the collision slots, number of chains with length [3,5]
    uint64_t dist_6_9;          // Among the collision slots, number of chains with length [6,9]
    uint64_t dist_10_;          // Among the collision slots, number of chains with length [10 or more]
    double  avg_chain_len;      // The average chain length among collision slots.

    uint64_t blob_allocated_bytes; //blobstore allocated
    uint64_t blob_used_bytes;      //blobstore used

    uint64_t bytes;             // Total bytes of hash set plus blobstore.
};


template<typename H, typename E>
class BuboHashSet {

public:
    BuboHashSet() : BuboHashSet(DEFAULT_INIT_HASH_TABLE_SZ, DEFAULT_MAX_HASH_TABLE_SZ) {}

    BuboHashSet(uint32_t table_size, uint32_t max_table_size) : table_size_(table_size),
                                                                max_table_size_(max_table_size),
                                                                table_curr_use_(0),
                                                                table_collisions_(0),
                                                                num_entries_(0),
                                                                table_(new Entry[table_size_]()),
                                                                blob_store_(new BlobStore()) {}

    ~BuboHashSet() {
        clear();
        delete blob_store_;
        delete [] table_;
    }

    // Returns true if inserted val is a new entry. Else false.
    inline bool insert(const BYTE* entry_buf, int entry_len) {
        assert(entry_buf);
        uint32_t idx = hash(entry_buf, entry_len) % table_size_;

        bool found = find_at(&table_[idx], entry_buf, entry_len);

        if (!found) {
            BYTE* blob_ptr = blob_store_->add(entry_buf, entry_len);
            insert_value_into_table_at_index(blob_ptr, table_, idx);
        }

        maybe_resize();

        return !found;
    }

    inline void erase(BYTE* val) {
        int len = bubo_utils::get_entry_len(val);
        uint32_t idx = hash(val, len) % table_size_;

        Entry** erase_entry = NULL;
        bool is_spine_entry = false;
        bool found = find_at(&table_[idx], val, len, &erase_entry, &is_spine_entry);

        if (found) {
            assert(erase_entry);

            if (is_spine_entry) {
                // Indicates that value to be removed is in the spine.
                // The spine may or may not have a chain, which affects the counters.
                bool deleted = erase_spine_entry(&table_[idx]);
                if (!deleted) {
                    // spine did not have a chain. hence decrement table use.
                    table_curr_use_ --;
                }

            } else {
                // erase_entry holds the pointer to the next_ pointer which points to the
                // Entry to be deleted.
                erase_next(erase_entry);
            }
            num_entries_ --;
        }
    }


    inline void clear() {
        // clear() does not deallocate the spine.
        for (uint32_t idx = 0; idx < table_size_; idx++) {
            Entry* p = table_[idx].next_;
            while (p) {
               Entry* q = p->next_;
               delete p;
               p = q;
            }
            table_[idx].val_ = NULL;
            table_[idx].next_ = NULL;
        }
    }

    inline uint64_t size() const {
        return num_entries_;
    }


    void get_stats(BuboHashStat* stat) const {

        stat->spine_len = table_size_;
        stat->spine_use = table_curr_use_;
        stat->entries = num_entries_;
        stat->total_chain_len = 0;
        stat->collision_slots = 0;
        stat->max_chain_len = 0;
        stat->avg_chain_len = 0;
        stat->dist_1_2 = 0;
        stat->dist_3_5 = 0;
        stat->dist_6_9 = 0;
        stat->dist_10_ = 0;

        stat->ht_bytes = table_size_ * sizeof(Entry);

        for (uint32_t idx = 0; idx < table_size_; idx++) {
            Entry* p = &table_[idx];
            uint64_t chain_len = 0;

            p = p->next_;
            if (p) stat->collision_slots ++;

            while (p) {
                stat->ht_bytes += sizeof(Entry);
                chain_len ++;
                p = p->next_;
            }

            if (chain_len >=1 && chain_len <=2) stat->dist_1_2 ++;
            else if (chain_len >=3 && chain_len <=5) stat->dist_3_5 ++;
            else if (chain_len >=6 && chain_len <=9) stat->dist_6_9 ++;
            else if (chain_len >=10) stat->dist_10_ ++;

            stat->total_chain_len += chain_len;
            if (chain_len > stat->max_chain_len) stat->max_chain_len = chain_len;
        }

        if (stat->collision_slots > 0) {
            stat->avg_chain_len = (double) stat->total_chain_len/ (double)stat->collision_slots;
        } else {
            assert(stat->total_chain_len == 0);
        }

        uint64_t allocated_bytes = 0, used_bytes = 0;
        blob_store_->stats(&allocated_bytes, &used_bytes);
        stat->blob_allocated_bytes = allocated_bytes;
        stat->blob_used_bytes = used_bytes;

        stat->bytes = stat->ht_bytes + stat->blob_allocated_bytes;
    }

protected:
    struct Entry {
        const BYTE* val_;
        Entry* next_;
        Entry() : val_(NULL), next_(NULL) {}
        Entry(const BYTE* v, Entry* n) : val_(v), next_(n) {}
    };

    uint32_t table_size_;
    uint32_t max_table_size_;

    uint64_t table_curr_use_;
    uint64_t table_collisions_;
    uint64_t num_entries_;
    Entry* table_;

    BlobStore* blob_store_;

    H hash;
    E equals;

    void insert_value_into_table_at_index(const BYTE* value, Entry* table, uint32_t index) {
        if (table[index].val_ == NULL) {
            // not found, and spine doesn't have an entry.
            table[index].val_ = value;
            table[index].next_ = NULL;
            table_curr_use_ ++;
        } else {
            // not found, but spine has an entry. insert new item right next to spine.
            table[index].next_ = new Entry(value, table[index].next_);
            table_collisions_ ++;
        }
        num_entries_ ++;
    }

    /*
     * Checks if the val is present in the chain at spine_entry.
     * Returns true if found.
     *
     * Optionally, if valid erase_entry pointer is given, set it to the address of the
     * previous entry's next pointer (note that this pointer is invalid if it is a spine
     * entry. Therefore, caller should handle spine case separately).
     *
     * The optional boolean pointer is_spine_entry is set if the found value is a spine entry.
     */
    inline bool find_at(Entry* spine_entry, const BYTE* val, int len, Entry*** erase_entry = NULL, bool* is_spine_entry=NULL) {
        if (is_spine_entry) {
            *is_spine_entry = false;
        }

        bool head_entry = true;
        Entry** p = &spine_entry;
        for (; *p; p = &(*p)->next_) {
            if (equals((*p)->val_, val, len)) {

                if (erase_entry) {
                    *erase_entry = p;
                }

                if (is_spine_entry && head_entry) {
                    *is_spine_entry = true;
                }

                return true;
            }
            head_entry = false;
        }

        return false;
    }

    /*
     * Erases the entry pointed to by prev_next, the next_ pointer of the previous chain entry.
     * Only chain entries to be erased this way.
     */
    inline void erase_next(Entry** prev_next ) {
        assert(prev_next && *prev_next);

        Entry* tmp = (*prev_next)->next_;
        delete (*prev_next);
        *prev_next = tmp;
    }

    /*
     * Erase a spine entry. The head entry may or may not have chains.
     * Return value of true indicates a delete is performed.
     */
    inline bool erase_spine_entry(Entry* spine_entry) {
        Entry* p = spine_entry;
        if (!p->next_) { // no chain
            p->val_ = NULL;
            return false;
        }

        // there is chain. swap the first chain entry into spine entry, and delete that entry.
        Entry* tmp = p->next_;
        spine_entry->val_ = p->next_->val_;
        spine_entry->next_ = p->next_->next_;

        delete tmp;
        return true;
    }


    inline void maybe_resize() {
        if (100 * num_entries_ / table_size_ > RESIZE_THRESHOLD_PCT && table_size_ < max_table_size_) {
            uint32_t new_size = table_size_ * 2;
            table_curr_use_ = 0;
            num_entries_ = 0;
            table_collisions_ = 0;

            Entry* new_table = new Entry[new_size]();

            for (uint32_t idx = 0; idx < table_size_; idx++) {
                Entry* p = &table_[idx];
                while (p && p->val_) {
                    int len = bubo_utils::get_entry_len(p->val_);
                    uint32_t new_idx = hash(p->val_, len) % new_size;
                    insert_value_into_table_at_index(p->val_, new_table, new_idx);
                    p = p->next_;
                }
            }

            clear(); //clear() operates on table_
            Entry* tmp = table_;
            table_ = new_table;

            delete [] tmp;

            table_size_ = new_size;
        }
    }
};
