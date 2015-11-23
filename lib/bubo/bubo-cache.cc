#include "bubo-cache.h"

#include "strings-table.h"
#include "blob-store.h"
#include "attrs-table.h"
#include "persistent-string.h"

BuboCache::Bucket::Bucket(const v8::Local<v8::String>& spaceBucket) {
    const v8::String::AsciiValue s(spaceBucket);
    spaceBucket_ = std::string(*s);
}

BuboCache::BuboCache()
    : bubo_cache_(),
      strings_table_(new StringsTable()) {}


BuboCache::~BuboCache() {
    for (bubo_cache_t::iterator it = bubo_cache_.begin(); it != bubo_cache_.end(); it++) {
        delete it->second;
    }
    bubo_cache_.clear();
    delete strings_table_;
}


bool BuboCache::lookup(const v8::Local<v8::String>& spaceBucket,
                       const v8::Local<v8::Object>& pt,
                       v8::Local<v8::String>& attr_str,
                       int* error) {
    Bucket key(spaceBucket);
    AttributesTable* at = NULL;
    bubo_cache_t::iterator it = bubo_cache_.find(key);
    if (it == bubo_cache_.end()) {
        at = new AttributesTable(strings_table_);
        bubo_cache_.insert(std::make_pair(key, at));
    } else {
        at = it->second;
    }
    return at->lookup(pt, attr_str, error);
}


void BuboCache::remove(const v8::Local<v8::String>& spaceBucket,
                       const v8::Local<v8::Object>& pt) {
    Bucket key(spaceBucket);

    bubo_cache_t::iterator it = bubo_cache_.find(key);
    if (it != bubo_cache_.end()) {
        it->second->remove(pt);
    }
}

long extractDay(const char* spaceBucket);
char* extractSpace(const char* spaceBucket);

long extractDay(const char* spaceBucket) {
    const char* p = spaceBucket;
    while(*(p++) != '@');
    return strtol(p, NULL, 10);
}

int spaceLength(const char* spaceBucket) {
    int i = 0;
    while (spaceBucket[i] != '@') {
        i++;
    }
    return i;
}

void BuboCache::remove_bucket(const v8::Local<v8::String>& spaceBucket) {
    // Note that the bucket is always a numeric value.
    // We remove all buckets with numeric value less than or equal to the requested bucket.
    const v8::String::AsciiValue s(spaceBucket);
    const char* deleteSpace = *s;
    int length = spaceLength(deleteSpace);
    long deleteBucket = extractDay(deleteSpace);

    for (bubo_cache_t::iterator it = bubo_cache_.begin(); it != bubo_cache_.end(); ) {
        const char* storedString = it->first.spaceBucket_.c_str();

        if (!strncmp(deleteSpace, storedString, length)) {
            long storedBucket = extractDay(storedString);

            if (storedBucket <= deleteBucket) {
                delete it->second;
                it = bubo_cache_.erase(it);
            } else {
                it++;
            }
        } else {
            it++;
        }
    }
}

void BuboCache::stats(v8::Local<v8::Object>& stats) const {

    static PersistentString strings_table("strings_table");

    v8::Local<v8::Object> strings_stats = NanNew<v8::Object>();
    strings_table_->stats(strings_stats);

    stats->Set(strings_table, strings_stats);

    for (bubo_cache_t::const_iterator it = bubo_cache_.begin(); it != bubo_cache_.end(); it++) {
        v8::Local<v8::Object> attr_stats = NanNew<v8::Object>();

        it->second->stats(attr_stats);
        stats->Set(NanNew(it->first.spaceBucket_.c_str()), attr_stats);
    }
}
