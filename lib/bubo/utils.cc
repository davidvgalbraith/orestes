#include "utils.h"
#include "persistent-string.h"
#include "strings-table.h"

using namespace v8;

namespace bubo_utils {
    v8::Persistent<v8::Object> ignoredAttrs_;

    void initialize(Local<Object> opts) {
        Local<Value> ignored = opts->Get(NanNew("ignoredAttributes"));
        NanAssignPersistent(ignoredAttrs_, ignored.As<Object>());
    }

void hex_out(const BYTE* data, int len, const char* hint) {
    printf("%s [%p][%d]\n", (hint ? hint : ""), data, len);

    for (int i = 0; i < len; i++) {
        if (i != 0 && i % 8 == 0) printf("\n");
        printf("%02X ", data[i]);
    }
    printf("\n\n");
}

bool is_ignored_attribute(const Local<String>& attr)
{
    return ignoredAttrs_->HasOwnProperty(attr);
}
}
