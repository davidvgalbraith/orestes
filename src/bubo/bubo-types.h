#pragma once

#include "node.h"
#include "nan.h"

typedef unsigned char BYTE;

struct CharPtrHash {
    std::size_t operator()(const char* b) const;
};

struct CharPtrEqual {
	bool operator()(const char* a, const char* b) const;
};



struct BytePtrHash {
    uint32_t operator()(const BYTE* b, int len) const;
};

struct BytePtrEqual {
    bool operator()(const BYTE* a, const BYTE* b, int blen) const;
};
