#include <assert.h>
#include <string.h>
#include <cstring>
#include "attrs-table.h"
#include "utils.h"
#include "strings-table.h"
#include "persistent-string.h"

static EntryToken* entryTokens[100];
static const int MAX_BUFFER_SIZE = 16 << 10;

AttributesTable::AttributesTable(StringsTable* strings_table)
        : attributes_hash_set_(),
          strings_table_(strings_table) {
              for (int i = 0; i < 100; i++) {
                  entryTokens[i] = new EntryToken();
              }
        }


/* Return true if the point corresponding to the tags/tagnames is found */
bool AttributesTable::lookup(const v8::Local<v8::Object>& pt,
                             v8::Local<v8::String>& attr_str, int* error) {
    int entrylen = 0;

    prepare_entry_buffer(pt, &entrylen, true, attr_str, error);

    if (*error) {
        return false;
    }

    return !attributes_hash_set_.insert(entry_buf_, entrylen);
}

void AttributesTable::remove(const v8::Local<v8::Object>& pt) {

    int entrylen = 0;
    v8::Local<v8::String> dummy;
    int* dummyInt = nullptr;
    prepare_entry_buffer(pt, &entrylen, false, dummy, dummyInt);

    attributes_hash_set_.erase(entry_buf_);
}

AttributesTable::~AttributesTable() {
    attributes_hash_set_.clear();
}

char* mystrcat( char* dest, const char* src, int* total_buffer_size );

// http://www.joelonsoftware.com/articles/fog0000000319.html
char* mystrcat( char* dest, const char* src, int* total_buffer_size ) {
     while (*dest) dest++;
     while ((*dest++ = *src++) && (*total_buffer_size)++ < MAX_BUFFER_SIZE);
     return --dest;
}

/* Returns true if all the tags and tag-names are found in the internal maps */
bool AttributesTable::prepare_entry_buffer(const v8::Local<v8::Object>& pt,
                                           int* entry_len,
                                           bool get_attr_str,
                                           v8::Local<v8::String>& attr_str,
                                           int* error) {
    int total_buffer_size = 0;
    static char g_attrstr_buf[MAX_BUFFER_SIZE] __attribute__ ((aligned (8)));
    g_attrstr_buf[0] = '\0';

    v8::Local<v8::Array> keys = pt->GetOwnPropertyNames();
    std::vector<EntryToken*> tokens;
    bool all_found = true;
    uint32_t length = keys->Length();

    for (u_int32_t i = 0; i < length; ++i) {
        v8::Local<v8::Value> key = keys->Get(i);
        v8::Local<v8::String> key_str(key->ToString());
        if (bubo_utils::is_ignored_attribute(key_str)) {
            continue;
        }
        v8::String::Utf8Value tag(key_str);
        v8::String::Utf8Value val(pt->Get(key_str));

        EntryToken* et = entryTokens[i];
        all_found = strings_table_->check_and_add(*tag, *val, et) && all_found;
        assert(et->tag_seq_no_ > 0 && et->val_seq_no_ > 0);
        tokens.push_back(et);
    }

    std::sort(tokens.begin(), tokens.end(), bubo_utils::cmp_entry_token);

    BYTE* entry_buf_ptr = entry_buf_;
    char* attr_buff_ptr = g_attrstr_buf;

    int encoded_len = 0;

    u_int32_t tags_count = tokens.size();
    bubo_utils::encode_packed(tags_count, entry_buf_ptr, &encoded_len);
    entry_buf_ptr += encoded_len;

    for (size_t i = 0; i < tags_count; i++) {

        EntryToken* et = tokens.at(i);

        encoded_len = 0;
        bubo_utils::encode_packed(et->tag_seq_no_, entry_buf_ptr, &encoded_len);
        entry_buf_ptr += encoded_len;

        encoded_len = 0;
        bubo_utils::encode_packed(et->val_seq_no_, entry_buf_ptr, &encoded_len);
        entry_buf_ptr += encoded_len;

        if (get_attr_str) {
            if (i != 0) {
                attr_buff_ptr = mystrcat(attr_buff_ptr, ",", &total_buffer_size);
            }
            attr_buff_ptr = mystrcat(attr_buff_ptr, et->tag_, &total_buffer_size);
            attr_buff_ptr = mystrcat(attr_buff_ptr, "=", &total_buffer_size);
            attr_buff_ptr = mystrcat(attr_buff_ptr, et->val_, &total_buffer_size);
        }
    }

    if (total_buffer_size >= MAX_BUFFER_SIZE) {
        *error = 1;
        return false;
    }

    if (get_attr_str) {
        attr_str = NanNew(g_attrstr_buf);
    }

    *entry_len = entry_buf_ptr - entry_buf_;

    // NOTE: "all_found == true" doesn't necessarily mean we have this entry.
    // This just means that each tag & tagname is known. But the order in which
    // they appear can vary within an entry.
    return all_found;
}


void AttributesTable::stats(v8::Local<v8::Object>& stats) const {

    static PersistentString attr_entries("attr_entries");

    static PersistentString blob_allocated_bytes("blob_allocated_bytes");
    static PersistentString blob_used_bytes("blob_used_bytes");

    static PersistentString ht_spine_len("ht_spine_len");
    static PersistentString ht_spine_use("ht_spine_use");
    static PersistentString ht_entries("ht_entries");
    static PersistentString ht_bytes("ht_bytes");
    static PersistentString ht_collision_slots("ht_collision_slots");
    static PersistentString ht_total_chain_len("ht_total_chain_len");
    static PersistentString ht_max_chain_len("ht_max_chain_len");
    static PersistentString ht_1_2("ht_dist_1_2");
    static PersistentString ht_3_5("ht_dist_3_5");
    static PersistentString ht_6_9("ht_dist_6_9");
    static PersistentString ht_10_("ht_dist_10_");
    static PersistentString ht_avg_chain_len("ht_avg_chain_len");

    static PersistentString ht_total_bytes("ht_total_bytes");

    stats->Set(attr_entries, NanNew<v8::Number>(attributes_hash_set_.size()));

    BuboHashStat bhs;
    memset(&bhs, 0, sizeof(BuboHashStat));

    attributes_hash_set_.get_stats(&bhs);

    stats->Set(ht_spine_len, NanNew<v8::Number>(bhs.spine_len));
    stats->Set(ht_spine_use, NanNew<v8::Number>(bhs.spine_use));
    stats->Set(ht_entries, NanNew<v8::Number>(bhs.entries));
    stats->Set(ht_bytes, NanNew<v8::Number>(bhs.ht_bytes));
    stats->Set(ht_collision_slots, NanNew<v8::Number>(bhs.collision_slots));
    stats->Set(ht_total_chain_len, NanNew<v8::Number>(bhs.total_chain_len));
    stats->Set(ht_max_chain_len, NanNew<v8::Number>(bhs.max_chain_len));
    stats->Set(ht_1_2, NanNew<v8::Number>(bhs.dist_1_2));
    stats->Set(ht_3_5, NanNew<v8::Number>(bhs.dist_3_5));
    stats->Set(ht_6_9, NanNew<v8::Number>(bhs.dist_6_9));
    stats->Set(ht_10_, NanNew<v8::Number>(bhs.dist_10_));
    stats->Set(ht_avg_chain_len, NanNew<v8::Number>(bhs.avg_chain_len));

    stats->Set(blob_allocated_bytes, NanNew<v8::Number>(bhs.blob_allocated_bytes));
    stats->Set(blob_used_bytes, NanNew<v8::Number>(bhs.blob_used_bytes));

    stats->Set(ht_total_bytes, NanNew<v8::Number>(bhs.bytes));

}
