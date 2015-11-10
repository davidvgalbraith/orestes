#pragma once

#include <stdint.h>
#include <string.h>
#include <unordered_map>
#include "bubo-types.h"

struct EntryToken;

/*
 * StringsTable is a two-level map of tags and values.
 * +--------+------------------+
 * |        | tag_seqno        |
 * | char*  | +----------------------------------------------------------------------+
 * |        | |  values_map : [char*, val_seq], [char*, val_seq], [char*, val_seq].. |
 * |        | +----------------------------------------------------------------------+
 * |        | last_val_seq_no_ |
 * +--------+------------------+
 * |        | tag_seqno        |
 * | char*  | +----------------------------------------------------------------------+
 * |        | |  values_map : [char*, val_seq], [char*, val_seq], [char*, val_seq].. |
 * |        | +----------------------------------------------------------------------+
 * |        | last_val_seq_no_ |
 * +--------+------------------+
 * |        | tag_seqno        |
 * | char*  | +----------------------------------------------------------------------+
 * |        | |  values_map : [char*, val_seq], [char*, val_seq], [char*, val_seq].. |
 * |        | +----------------------------------------------------------------------+
 * |        | last_val_seq_no_ |
 * +--------+------------------+
 * |        | tag_seqno        |
 * | char*  | +----------------------------------------------------------------------+
 * |        | |  values_map : [char*, val_seq], [char*, val_seq], [char*, val_seq].. |
 * |        | +----------------------------------------------------------------------+
 * |        | last_val_seq_no_ |
 * +--------+------------------+
 */

class StringsTable {
public:
    StringsTable() : tags_(), last_tag_seq_no_(1), allocated_bytes_(0) {}
    virtual ~StringsTable();

    /* Checks for the presence of the given tag and val in the internal maps.
     * If found, fill up the corresponding sequnce numbers and char pointers into token.
     * If not found, add entry/entries in the map(s) (note: the 'char*' key gets
     * created in the heap) and fill up the corresponding sequnce numbers and char
     * pointers into token.
     *
     * Return value: true if both tag and tagname are found. False otherwise.
     */
    bool check_and_add(const char* tag, const char* val, EntryToken* token);

    /* returns number of tag entries in the internal map */
    size_t get_num_tags() const;
    /* returns number of tagname entries corresponding to the tag in the internal map */
    size_t get_num_vals(const char* tag) const;

    void stats(v8::Local<v8::Object>& stats) const;

protected:

    typedef std::unordered_map<const char*, uint64_t, CharPtrHash, CharPtrEqual> values_t;

    struct TagEntry {
        uint32_t tag_seq_no_;
        uint32_t last_val_seq_no_;
        values_t vals_;
        TagEntry(uint64_t s) : tag_seq_no_(s), last_val_seq_no_(1), vals_() {}
    };
    typedef std::unordered_map<const char*, TagEntry*, CharPtrHash, CharPtrEqual> tags_t;

    tags_t tags_;
    uint32_t last_tag_seq_no_;

    uint64_t allocated_bytes_;
};
