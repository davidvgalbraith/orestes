#pragma once


#include <stdlib.h>
#include <unordered_map>
#include <string>
#include "bubo-types.h"

class AttributesTable;
class StringsTable;

class BuboCache {
public:
    BuboCache();
    virtual ~BuboCache();

    bool lookup(const v8::Local<v8::String>& spaceBucket,
                const v8::Local<v8::Object>& pt,
                v8::Local<v8::String>& attr_str,
                int* error);

    void remove(const v8::Local<v8::String>& spaceBucket,
                const v8::Local<v8::Object>& pt);

    void remove_bucket(const v8::Local<v8::String>& spaceBucket);

    void stats(v8::Local<v8::Object>& stats) const;

protected:
    struct Bucket {
        std::string spaceBucket_;
        Bucket(const v8::Local<v8::String>& spaceBucket);
    };
    struct BucketHash {
        size_t operator()(const Bucket& bp) const {
            return std::hash<std::string>()(bp.spaceBucket_);
        }
    };
    struct BucketComparer {
        bool operator()(const Bucket& l, const Bucket& r) const {
            return l.spaceBucket_ == r.spaceBucket_;
        }
    };
    typedef std::unordered_map<Bucket, AttributesTable*, BucketHash, BucketComparer> bubo_cache_t;
    bubo_cache_t bubo_cache_;
    StringsTable* strings_table_;
};
