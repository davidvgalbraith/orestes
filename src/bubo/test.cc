#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unordered_set>
#include "bubo-types.h"
#include "utils.h"
#include "test.h"
#include "strings-table.h"
#include "attrs-table.h"
#include "bubo-ht.h"


static void test_hash_function_same_input() {
    BYTE a[5];
    a[0] = 2;
    a[1] = 3;
    a[2] = 4;
    a[3] = 5;
    a[4] = 6;
    const BYTE* x = a;

    BYTE b[5];
    b[0] = 2;
    b[1] = 3;
    b[2] = 4;
    b[3] = 5;
    b[4] = 6;
    const BYTE* y = b;

    size_t s1 = bubo_utils::hash_byte_sequence(x, 5);
    size_t s2 = bubo_utils::hash_byte_sequence(y, 5);

    assert(s1 == s2);
}

static void test_hash_function_diff_input() {
    BYTE a[5];
    a[0] = 0x30;
    a[1] = 0xF4;
    a[2] = 0xF2;
    a[3] = 0xF0;
    a[4] = 0x0E;
    const BYTE* x = a;

    BYTE b[5];
    b[0] = 0x30;
    b[1] = 0xF4;
    b[2] = 0xF6;
    b[3] = 0xF0;
    b[4] = 0x0E;
    const BYTE* y = b;

    size_t s1 = bubo_utils::hash_byte_sequence(x, 5);
    size_t s2 = bubo_utils::hash_byte_sequence(y, 5);


    assert(s1 != s2 || !memcmp(a, b, 5));
}

struct TestBytePtrHash {
    uint32_t operator()(const BYTE* b) const;
};

struct TestBytePtrEqual {
    bool operator()(const BYTE* a, const BYTE* b) const;
};

uint32_t TestBytePtrHash::operator()(const BYTE* p) const {
    int len = bubo_utils::get_entry_len(p);
    return bubo_utils::hash_byte_sequence(p, len);
}

bool TestBytePtrEqual::operator()(const BYTE* a, const BYTE* b) const {
	int alen = bubo_utils::get_entry_len(a);
    int blen = bubo_utils::get_entry_len(b);

    return (alen == blen) && !memcmp(a, b, alen);
}

void test_hash_function_use_in_set() {
    typedef std::unordered_set<const BYTE*, TestBytePtrHash, TestBytePtrEqual> entry_t;
    entry_t entryset;

    // deliberately add more bytes in a, b, and c. Only the 12 bytes after 0x06 woud be used.
    const BYTE aa[] = { 0x06, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0xab, 0xaa, 0xaa, 0xcc, 0xdd };
    const BYTE bb[] = { 0x06, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0xEE, 0x74, 0x70 };
    const BYTE cc[] = { 0x01, 0x12, 0x7F };
    BYTE *a = new BYTE[30],
         *b = new BYTE[30],
         *c = new BYTE[30];

    memcpy(a, aa, 18);
    memcpy(b, bb, 16);
    memcpy(c, cc, 3);

    entry_t::const_iterator it;
    it = entryset.find(a);
    assert(it == entryset.end());
    it = entryset.find(b);
    assert(it == entryset.end());

    entryset.insert(a);
    it = entryset.find(a);
    assert(it != entryset.end());

    it = entryset.find(b);
    assert(it != entryset.end());

    it = entryset.find(c);
    assert(it == entryset.end());

}

void test_entry_len() {
    BYTE test[20];

    test[0] = 0x0;  // <-- ends here
    test[1] = 0x81;
    test[2] = 0x7F;
    assert(1 == bubo_utils::get_entry_len(test));

    test[0] = 0x01;
    test[1] = 0x81;
    test[2] = 0x7F;
    test[3] = 0x7F; // <-- ends here
    test[4] = 0x7F;
    assert(4 == bubo_utils::get_entry_len(test));

    test[0] = 0x01;
    test[1] = 0x7F;
    test[2] = 0x7F;// <-- ends here
    test[3] = 0x8F;
    test[4] = 0x7F;
    assert(3 == bubo_utils::get_entry_len(test));

    test[0] = 0x02;
    test[1] = 0x7F;
    test[2] = 0x81;
    test[3] = 0x9E;
    test[4] = 0x81;
    test[5] = 0x44;
    test[6] = 0x7F;
    test[7] = 0x7F; // <-- ends here
    test[8] = 0x33;
    test[9] = 0x23;
    assert(8 == bubo_utils::get_entry_len(test));

    //TODO: add test with test[0] = 127+
}

static void test_encode_decode_result_match() {
    size_t vals[] = { 0x0, 0x1, 0x8F, 0xFF, 0xFF00, 0xFEEDBEEF, 0xFFFFFFFF };
    for (size_t i = 0; i < sizeof(vals)/sizeof(size_t); i++) {
        static BYTE buf[1<<10];
        int buflen = 0;
        bubo_utils::encode_packed(vals[i], buf, &buflen);
        size_t result = bubo_utils::decode_packed(buf);
        assert(vals[i] == result);
    }
}

static void test_entry_tokens_sorting() {
    std::vector<EntryToken*> tokens;
    EntryToken* et = NULL;

    const char* ordered_tags[] = { "t1", "t2", "t3", "t4", "t5", "t6" };
    const char* ordered_vals[] = { "v1", "v2", "v3", "v4", "v5", "v6" };

    // already ordered data
    for (int i = 0; i < 6; i ++) {
        et = new EntryToken();
        et->tag_ = strdup(ordered_tags[i]);
        et->val_ = strdup(ordered_vals[i]);
        et->tag_seq_no_ = i+1;
        et->val_seq_no_ = 1;
        tokens.push_back(et);
    }

    std::sort(tokens.begin(), tokens.end(), bubo_utils::cmp_entry_token);

    for (int i = 0; i < 3; i ++) {
        et = tokens.at(i);
        assert(!strcmp(et->tag_, ordered_tags[i]));
        assert(!strcmp(et->val_, ordered_vals[i]));
        assert(et->tag_seq_no_ == (uint64_t)i+1);
        assert(et->val_seq_no_ == 1);
    }

    tokens.clear();
    // unordered data
    const char* unordered_tags[] = { "t5", "t2", "t9", "t4" };
    const char* unordered_vals[] = { "v5", "v2", "v9", "v4" };
    uint64_t tagseqs[] = { 15, 16, 17, 18 };

    const char* exp_unordered_tags[] = { "t2", "t4", "t5", "t9" };
    const char* exp_unordered_vals[] = { "v2", "v4", "v5", "v9" };
    uint64_t exp_tagseqs[] = { 16, 18, 15, 17 };

    for (int i = 0; i < 4; i ++) {
        et = new EntryToken();
        et->tag_ = strdup(unordered_tags[i]);
        et->val_ = strdup(unordered_vals[i]);
        et->tag_seq_no_ = tagseqs[i];
        et->val_seq_no_ = 1;
        tokens.push_back(et);
    }

    std::sort(tokens.begin(), tokens.end(), bubo_utils::cmp_entry_token);

    for (int i = 0; i < 4; i ++) {
        et = tokens.at(i);
        assert(!strcmp(et->tag_, exp_unordered_tags[i]));
        assert(!strcmp(et->val_, exp_unordered_vals[i]));
        assert(et->tag_seq_no_ == exp_tagseqs[i]);
        assert(et->val_seq_no_ == 1);
    }

}

static void test_strings_table_sizes() {
    // Various checks to make sure that the strings table works as expected wrt # entries.
    StringsTable* st = new StringsTable();
    assert(st->get_num_tags() == 0);
    assert(st->get_num_vals("something") == 0);

    const char* tag = "";
    const char* val = "";
    EntryToken et;

    // [ tag1: [tag1name] ]
    tag = strdup("tag1");
    val = strdup("tag1name");
    assert(st->check_and_add(tag, val, &et) == false);
    assert(st->get_num_tags() == 1);
    assert(st->get_num_vals("tag1") == 1);
    assert(!strcmp(et.tag_, tag));
    assert(!strcmp(et.val_, val));
    assert(et.tag_seq_no_ == 1);
    assert(et.val_seq_no_ == 1);

    // [ tag1: [tag1name, blah] ]
    tag = strdup("tag1");
    val = strdup("blah");
    assert(st->check_and_add(tag, val, &et) == false);
    assert(st->get_num_tags() == 1);
    assert(st->get_num_vals("tag1") == 2);
    assert(!strcmp(et.tag_, tag));
    assert(!strcmp(et.val_, val));
    assert(et.tag_seq_no_ == 1);
    assert(et.val_seq_no_ == 2);

    // [ tag1: [tag1name, blah] ] again; check_and_add should return true for (tag1,blah).
    tag = strdup("tag1");
    val = strdup("blah");
    assert(st->check_and_add(tag, val, &et) == true);
    assert(st->get_num_tags() == 1);
    assert(st->get_num_vals("tag1") == 2);
    assert(!strcmp(et.tag_, tag));
    assert(!strcmp(et.val_, val));
    assert(et.tag_seq_no_ == 1);
    assert(et.val_seq_no_ == 2);

    // [ tag1: [tag1name, blah] ] again; check_and_add should return true for (tag1,tag1name).
    tag = strdup("tag1");
    val = strdup("tag1name");
    assert(st->check_and_add(tag, val, &et) == true);
    assert(st->get_num_tags() == 1);
    assert(st->get_num_vals("tag1") == 2);
    assert(!strcmp(et.tag_, tag));
    assert(!strcmp(et.val_, val));
    assert(et.tag_seq_no_ == 1);
    assert(et.val_seq_no_ == 1);

    // [ tag1: [tag1name, blah], blah: tag1 ]. Basically, reusing existing string, but in different order.
    tag = strdup("blah");
    val = strdup("tag1");
    assert(st->check_and_add(tag, val, &et) == false);
    assert(st->get_num_tags() == 2);
    assert(st->get_num_vals("tag1") == 2);
    assert(st->get_num_vals("blah") == 1);
    assert(!strcmp(et.tag_, tag));
    assert(!strcmp(et.val_, val));
    assert(et.tag_seq_no_ == 2);
    assert(et.val_seq_no_ == 1);

    delete st;
}

static void test_strings_table_entry_buf_basic() {
    // tests if basic functionality of prepare_entry_buffer() is allright.
    StringsTable* st = new StringsTable();
    AttributesTable* at = new AttributesTable(st);

    v8::Local<v8::Object> pt = NanNew<v8::Object>();
    pt->Set(NanNew("proxy"), NanNew("sfdc1"));              // tag seq 1 -> position 3 sorted.
    pt->Set(NanNew("ip"), NanNew("127.12.33.22"));          // 2 -> 2
    pt->Set(NanNew("host"), NanNew("myname.mydomain.com")); // 3 -> 1
    pt->Set(NanNew("rate"), NanNew("99"));                  // 4 -> 4

    v8::Local<v8::String> attrstr = NanNew("");

    int buflen = 0;
    int* error = nullptr;

    at->prepare_entry_buffer(pt, &buflen, true, attrstr, error);
    BYTE* buf = at->get_entry_buf();

    assert(buf != NULL);
    assert(buflen == 9);
    assert(buf[0] == 0x04);
    assert(buf[1] == 0x03);
    assert(buf[2] == 0x01);
    assert(buf[3] == 0x02);
    assert(buf[4] == 0x01);
    assert(buf[5] == 0x01);
    assert(buf[6] == 0x01);
    assert(buf[7] == 0x04);
    assert(buf[8] == 0x01);

    v8::String::AsciiValue k(attrstr);

    assert(!strcmp(*k, "host=myname.mydomain.com,ip=127.12.33.22,proxy=sfdc1,rate=99"));
}

static void test_strings_table_entry_buf_repeated() {
    // tests if functionality of prepare_entry_buffer() is allright when things repeat.
    StringsTable* st = new StringsTable();
    AttributesTable* at = new AttributesTable(st);

    int buflen = 0;

    v8::Local<v8::String> attrstr = NanNew("");
    v8::Local<v8::Object> pt = NanNew<v8::Object>();
    pt->Set(NanNew("ip"), NanNew("12.53.14.8"));
    pt->Set(NanNew("host"), NanNew("myname.mydomain.com"));

    int* error = nullptr;
    assert(at->prepare_entry_buffer(pt, &buflen, false, attrstr, error) == false);
    BYTE* buf = at->get_entry_buf();

    assert(buf != NULL);
    assert(buflen == 5);
    assert(buf[0] == 0x02);
    assert(buf[1] == 0x02);
    assert(buf[2] == 0x01);
    assert(buf[3] == 0x01);
    assert(buf[4] == 0x01);

    // create another entry for point with different ip.
    // some seqno would be reused (host part), and some not; return val should be false.
    pt = NanNew<v8::Object>();
    pt->Set(NanNew("ip"), NanNew("22.33.11.1"));
    pt->Set(NanNew("host"), NanNew("myname.mydomain.com"));

    assert(at->prepare_entry_buffer(pt, &buflen, true, attrstr, error) == false);
    buf = at->get_entry_buf();
    assert(buf != NULL);
    assert(buflen == 5);
    assert(buf[0] == 0x02);
    assert(buf[1] == 0x02);
    assert(buf[2] == 0x01);
    assert(buf[3] == 0x01);
    assert(buf[4] == 0x02);

    v8::String::AsciiValue k(attrstr);
    assert(!strcmp(*k, "host=myname.mydomain.com,ip=22.33.11.1"));

    delete st;
}

static void test_strings_table_entry_buf_large_seq() {
    // Test for sequence numbers larger than 127 (these will require more than 1 byte)
    StringsTable* st = new StringsTable();
    AttributesTable* at = new AttributesTable(st);

    v8::Local<v8::String> tbase = NanNew("mytag");
    v8::Local<v8::String> vbase = NanNew("myval");

    v8::Local<v8::Object> pt;
    v8::Local<v8::String> attrstr = NanNew("");
    BYTE* buf = NULL;
    int buflen = 0;

    int tcount = 1;
    int* error = nullptr;
    for (int i = 0; i < 10; i++) {
        v8::Local<v8::String> tag_suffix = NanNew(std::to_string(tcount++).c_str());
        int vcount = 1;

        for (int j = 0; j < 125 + (i % 5); j++) { //1-125, 2-126, 3-127, 4-128, 5-129, 6-125,..
            v8::Local<v8::String> val_suffix = NanNew(std::to_string(vcount++).c_str());

            pt = NanNew<v8::Object>();
            pt->Set(v8::String::Concat(tbase, tag_suffix), v8::String::Concat(vbase, val_suffix));
            // since each of this point has unique strings, the return value should be false.
            assert(at->prepare_entry_buffer(pt, &buflen, false, attrstr, error) == false);
        }
    }

    // Now we have 10 tags, with [125..129] vals each. Each tag has a unique seq no. Each val for a given tag has unique no.
    // Also [ 5: [129] ] is an entry.

    pt = NanNew<v8::Object>();
    pt->Set(NanNew("mytag5"), NanNew("myval129"));
    assert(at->prepare_entry_buffer(pt, &buflen, true, attrstr, error) == true);
    buf = at->get_entry_buf();

    v8::String::AsciiValue k(attrstr);
    assert(!strcmp("mytag5=myval129", *k));

    assert(buf != NULL);
    assert(buflen == 4);
    assert(buf[0] == 0x01);
    assert(buf[1] == 0x05);
    assert(buf[2] == 0x81);
    assert(buf[3] == 0x01);

    delete st;
}

void test_strings_table_wide_point() {
    // tests if prepare_entry_buffer() works with relatively wide inputs.
    StringsTable* st = new StringsTable();
    AttributesTable* at = new AttributesTable(st);

    BYTE* buf = NULL;
    int buflen = 0;

    v8::Local<v8::Object> pt = NanNew<v8::Object>();
    v8::Local<v8::String> attrstr = NanNew("");

    pt->Set(NanNew("name"), NanNew("cpu.system"));
    pt->Set(NanNew("pop"), NanNew("sf"));
    pt->Set(NanNew("time2"), NanNew("Tue May 26 2015 17:25:27 GMT-0700 (PDT)"));
    pt->Set(NanNew("value2"), NanNew("120"));
    pt->Set(NanNew("value3"), NanNew("22123"));
    pt->Set(NanNew("value4"), NanNew("22123"));
    pt->Set(NanNew("host"), NanNew("foo.com"));
    pt->Set(NanNew("value5"), NanNew("22123"));
    pt->Set(NanNew("value"), NanNew("333333"));
    pt->Set(NanNew("value6"), NanNew("22123"));
    pt->Set(NanNew("value7"), NanNew("22123"));
    pt->Set(NanNew("value8"), NanNew("22123"));
    pt->Set(NanNew("value9"), NanNew("22123"));
    pt->Set(NanNew("time"), NanNew("14044044"));

    int* error = nullptr;

    assert(at->prepare_entry_buffer(pt, &buflen, true, attrstr, error) == false);
    buf = at->get_entry_buf();

    v8::String::AsciiValue k(attrstr);
    //attr str should be ordered, and should not contain "value", and "time"
    assert(!memcmp(*k,
            "host=foo.com,name=cpu.system,pop=sf,time2=Tue May 26 2015 17:25:27 GMT-0700 (PDT),value2=120,value3=22123,value4=22123,value5=22123,value6=22123,value7=22123,value8=22123,value9=22123",
            strlen(*k)));

    delete st;
}

void test_hash_set() {

    BuboHashSet<BytePtrHash, BytePtrEqual> bubo_hash_set;

    BuboHashStat stat;

    // (1) test empty hash set.
    bubo_hash_set.get_stats(&stat);

    assert(stat.spine_len == 4096); // initial spine size
    assert(stat.spine_use == 0);
    assert(stat.entries == 0);
    assert(stat.ht_bytes == 4096 * 16);
    assert(stat.collision_slots == 0);
    assert(stat.total_chain_len == 0);
    assert(stat.max_chain_len == 0);
    assert(stat.blob_allocated_bytes == (20 << 20));
    assert(stat.blob_used_bytes == 0);

    BYTE test[20];

    test[0] = 0x02; // <-- len
    test[1] = 0x7F; // k[0] end
    test[2] = 0x81;
    test[3] = 0x9E;
    test[4] = 0x81;
    test[5] = 0x44; // v[0] end
    test[6] = 0x7F; // k[1] end
    test[7] = 0x7F; // v[0] end

    // (2) test single entry hash set.
    // add the entry to set. since its a new entry, it should return true.
    assert(true == bubo_hash_set.insert(test, 8));

    bubo_hash_set.get_stats(&stat);

    assert(stat.spine_len == 4096); // initial spine size
    assert(stat.spine_use == 1);
    assert(stat.entries == 1);
    assert(stat.ht_bytes == 4096 * 16);
    assert(stat.collision_slots == 0);
    assert(stat.total_chain_len == 0);
    assert(stat.max_chain_len == 0);
    assert(stat.blob_allocated_bytes == (20 << 20));
    assert(stat.blob_used_bytes == 8);

    // (3) test repeated addition.
    // add the entry to set the second time. since its not a new entry, it should return false.
    assert(false == bubo_hash_set.insert(test, 8));

    assert(stat.spine_len == 4096);
    assert(stat.spine_use == 1);
    assert(stat.entries == 1);
    assert(stat.ht_bytes == 4096 * 16);
    assert(stat.collision_slots == 0);
    assert(stat.total_chain_len == 0);
    assert(stat.max_chain_len == 0);
    assert(stat.blob_allocated_bytes == (20 << 20));
    assert(stat.blob_used_bytes == 8);

}

void test_hash_set_add_many_erase() {

    BuboHashSet<BytePtrHash, BytePtrEqual> bubo_hash_set(512, 2048);
    BuboHashStat stat;

    BYTE test[20];
    test[0] = 0x02; // <-- len
    test[1] = 0x7F; // k[0] end
    test[2] = 0x81;
    test[3] = 0x9E;
    test[4] = 0x81;
    test[5] = 0x44; // v[0] end
    test[6] = 0x7F; // k[1] end
    test[7] = 0x7F; // v[0] end

    // create 100 x 100 = 10K entries. There may be collisions.
    for (int i = 0; i < 100; i ++) {
        for (int j = 0; j < 100; j++) {
            // create a unique entry conforming to the above layout.
            test[2] = 0x80 | (i & 0x7F);
            test[4] = 0x80 | (j & 0x7F);
            bubo_hash_set.insert(test, 8);
        }
    }
    bubo_hash_set.get_stats(&stat);

    assert(stat.spine_len == 2048);
    assert(stat.entries == 10000);
    assert(stat.blob_allocated_bytes == (20 << 20));
    assert(stat.blob_used_bytes == 80000);

    // remove 30 * 30 = 900 values
    for (int i = 20; i < 50; i ++) {
        for (int j = 30; j < 60; j++) {
            // create a unique entry conforming to the above layout.
            test[2] = 0x80 | (i & 0x7F);
            test[4] = 0x80 | (j & 0x7F);
            bubo_hash_set.erase(test);
        }
    }
    bubo_hash_set.get_stats(&stat);
    assert(stat.spine_len == 2048);
    assert(stat.entries == 9100);
    assert(stat.blob_allocated_bytes == (20 << 20));
    assert(stat.blob_used_bytes == 80000);
}

void testall() {
    test_hash_function_same_input();
    test_hash_function_diff_input();
    test_hash_function_use_in_set();

    test_entry_len();
    test_entry_tokens_sorting();
    test_encode_decode_result_match();

    test_strings_table_sizes();
    test_strings_table_entry_buf_basic();
    test_strings_table_entry_buf_repeated();
    test_strings_table_entry_buf_large_seq();
    test_strings_table_wide_point();

    test_hash_set();
    test_hash_set_add_many_erase();

}
