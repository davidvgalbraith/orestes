#include "bubo-types.h"
#include "utils.h"



std::size_t CharPtrHash::operator()(const char* b) const {
    int len = strlen(b);
    const unsigned char *p = (const unsigned char*)b;
    return bubo_utils::hash_byte_sequence(p, len);
}

bool CharPtrEqual::operator()(const char* a, const char* b) const {
    return !strcmp(a, b);
}


uint32_t BytePtrHash::operator()(const BYTE* p, int len) const {
    return bubo_utils::hash_byte_sequence(p, len);
}

bool BytePtrEqual::operator()(const BYTE* a, const BYTE* b, int blen) const {

	int alen = bubo_utils::get_entry_len(a);

    return (alen == blen) && !memcmp(a, b, alen);
}
