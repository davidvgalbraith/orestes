#include <stdlib.h>
#include <assert.h>
#include "strings-table.h"
#include "utils.h"
#include "persistent-string.h"

StringsTable::~StringsTable() {
    for (tags_t::iterator t = tags_.begin(); t != tags_.end(); t++) {
        if (t->first) {
            free((void*)t->first);
        }
        if (t->second) {
            for (values_t::iterator n = t->second->vals_.begin(); n != t->second->vals_.end(); n++) {
                if (n->first) {
                    free((void*)n->first);
                }
            }
            t->second->vals_.clear();
        }
        delete t->second;
    }
    tags_.clear();
}

/* Return true if both the tag and tagname are found in the strings table */
bool StringsTable::check_and_add(const char* tag, const char* val, EntryToken* token) {

    bool found = true;

    const char* tagstr = NULL;
    TagEntry* te = NULL;

    tags_t::iterator ti = tags_.find(tag);
    if (ti == tags_.end()) {
        tagstr = strdup(tag);
        allocated_bytes_ += strlen(tagstr) + 1;
        te = new TagEntry(last_tag_seq_no_++);
        tags_.insert(std::make_pair(tagstr, te));
        found = false;
    } else {
        tagstr = ti->first;
        te = ti->second;
    }
    token->tag_ = tagstr;
    token->tag_seq_no_ = te->tag_seq_no_;

    const char* valstr = NULL;
    uint64_t valseq = 0;

    values_t::iterator vi = te->vals_.find(val);
    if (vi == te->vals_.end()) {
        valstr = strdup(val);
        allocated_bytes_ += strlen(valstr) + 1;
        valseq = te->last_val_seq_no_++;
        te->vals_.insert(std::make_pair(valstr, valseq));
        found = false;

    } else {
        valstr = vi->first;
        valseq = vi->second;
    }
    token->val_ = valstr;
    token->val_seq_no_ = valseq;

    return found;
}

size_t StringsTable::get_num_tags() const {
    return tags_.size();
}

size_t StringsTable::get_num_vals(const char* tag) const {
    tags_t::const_iterator it = tags_.find(tag);
    if (it != tags_.end()) {
        return it->second->vals_.size();
    }
    return 0;
}

void StringsTable::stats(v8::Local<v8::Object>& stats) const {
    static PersistentString allocated_bytes("allocated_bytes");
    static PersistentString num_tags("num_tags");
    static PersistentString num_vals_str("num_vals_all");


    stats->Set(allocated_bytes, NanNew<v8::Number>(allocated_bytes_));
    stats->Set(num_tags, NanNew<v8::Number>(tags_.size()));

    uint64_t num_vals_all = 0;
    for (tags_t::const_iterator t = tags_.begin(); t != tags_.end(); t++) {
        size_t num_vals = t->second->vals_.size();
        stats->Set(NanNew(t->first), NanNew<v8::Number>(num_vals));
        num_vals_all += num_vals;
    }
    stats->Set(num_vals_str, NanNew<v8::Number>(num_vals_all));
}
