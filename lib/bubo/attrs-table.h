#ifndef _ATTRS_TABLE_H_
#define _ATTRS_TABLE_H_

#include <stdint.h>
#include <vector>
#include <string>
#include "bubo-types.h"
#include "bubo-ht.h"

class StringsTable;

class AttributesTable {
public:
	AttributesTable(StringsTable* strings_table);
    virtual ~AttributesTable();

    bool lookup(const v8::Local<v8::Object>& pt, v8::Local<v8::String>& attr_str, int* error);
    void remove(const v8::Local<v8::Object>& pt);
    void stats(v8::Local<v8::Object>& stats) const;

    /*
     * An entry into the attributes_hash_set_ is a pointer to a byte sequence of the form:
     *    +-------------+---------+-----------+---------+-----------+--
     *    | num entries | tag1seq | value1seq | tag2seq | value2seq |..
     *    +-------------+---------+-----------+---------+-----------+--
     * where each value is in the packed encoding format.
     *
     * prepare_entry_buffer() obtains the sequence numbers corresponding to the tags and
     * tagnames from the strings table and creates the entry buffer in entry_buffer_.
	 *
     * Optionally, one can ask for the attr_str to be filled in with the tags and tag_names.
     *
     * @pt: The javascript object whose tag/val members are added to the entry buffer.
     *      (note: Certain attributes are ignored (based on bubo_utils::is_ignored_attribute()))
     * @entry_len: length of the buffer being prepared in bytes
     * @get_attr_str: boolean specifying whether we want attr_str to be filled in.
     * @attr_str: optional v8::String reference to be filled with 'tag1=tagname1,tag2=tagname2,..'
     *
     * Return value: true if all tags and tagnames are found. False otherwise.
     */
    bool prepare_entry_buffer(const v8::Local<v8::Object>& pt,
                              int* entry_len,
                              bool get_attr_str,
                              v8::Local<v8::String>& attr_str,
						      int* error);

    // For tests
    BYTE* get_entry_buf() { return entry_buf_; }

protected:
	BuboHashSet<BytePtrHash, BytePtrEqual> attributes_hash_set_;
	StringsTable* strings_table_;

	BYTE entry_buf_[16 << 10] __attribute__ ((aligned (8)));
};



#endif
