#pragma once

#include "die/core.hpp"

// *** Required Headers from the STL

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <istream>
#include <memory>
#include <ostream>
#include <utility>
#include <pthread.h>

namespace tlx {

// *** Debugging Macros

#ifdef TLX_BTREE_DEBUG

#include <iostream>

//! Print out debug information to std::cout if TLX_BTREE_DEBUG is defined.
#define TLX_BTREE_PRINT(x) \
    do { if (debug) (std::cout << x << std::endl); } while (0)

//! Assertion only if TLX_BTREE_DEBUG is defined. This is not used in verify().
#define TLX_BTREE_ASSERT(x) \
    do { assert(x); } while (0)

#else

//! Print out debug information to std::cout if TLX_BTREE_DEBUG is defined.
#define TLX_BTREE_PRINT(x)          do { } while (0)

//! Assertion only if TLX_BTREE_DEBUG is defined. This is not used in verify().
#define TLX_BTREE_ASSERT(x)         do { } while (0)

#endif

//! The maximum of a and b. Used in some compile-time formulas.
#define TLX_BTREE_MAX(a, b)          ((a) < (b) ? (b) : (a))

#ifndef TLX_BTREE_FRIENDS
//! The macro TLX_BTREE_FRIENDS can be used by outside class to access the B+
//! tree internals. This was added for wxBTreeDemo to be able to draw the
//! tree.
#define TLX_BTREE_FRIENDS           friend class btree_friend
#endif

const unsigned char DUP_MASK = 0x01;
const unsigned char DEL_MASK = 0x02;
const unsigned int MAX_UINT = std::numeric_limits<unsigned int>::max();

/*!
 * Generates default traits for a B+ tree used as a set or map. It estimates
 * leaf and inner node sizes by assuming a cache line multiple of 256 bytes.
*/
template <typename Key, typename Value>
struct btree_default_traits {
    //! If true, the tree will self verify its invariants after each insert() or
    //! erase(). The header must have been compiled with TLX_BTREE_DEBUG
    //! defined.
    static const bool self_verify = false;

    //! If true, the tree will print out debug information and a tree dump
    //! during insert() or erase() operation. The header must have been
    //! compiled with TLX_BTREE_DEBUG defined and key_type must be std::ostream
    //! printable.
    static const bool debug = false;

    //! Number of slots in each leaf of the tree. Estimated so that each node
    //! has a size of about 256 bytes.
    static const int leaf_slots =
        TLX_BTREE_MAX(8, 256 / (sizeof(Value)));

    //! Number of slots in each inner node of the tree. Estimated so that each
    //! node has a size of about 256 bytes.
    static const int inner_slots =
        TLX_BTREE_MAX(8, 256 / (sizeof(Key) + sizeof(void*)));

    //! As of stx-btree-0.9, the code does linear search in find_lower() and
    //! find_upper() instead of binary_search, unless the node size is larger
    //! than this threshold. See notes at
    //! http://panthema.net/2013/0504-STX-B+Tree-Binary-vs-Linear-Search
    static const size_t binsearch_threshold = 256;
};

class node {
public:
    //! Level in the b-tree, if level == 0 -> leaf node
    unsigned short level;

    //! Number of key slotuse use, so the number of valid children or data
    //! pointers
    unsigned short slotuse;

	unsigned char flags;
	pthread_spinlock_t dup_lock;

    inline bool is_dup() { return (flags & DUP_MASK) == DUP_MASK; }
	inline void set_dup() { flags ^= DUP_MASK; }
	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

    //! Delayed initialisation of constructed node.
    void initialize(const unsigned short l);

    //! True if this is a leaf node.
    bool is_leafnode() const;

    unsigned short get_level() const;

    unsigned short get_slotuse() const;

    void set_slotuse(unsigned short new_slotuse);
};

//! Extended structure of a inner node in-memory. Contains only keys and no
//! data items.
template <typename Key, typename Value>
class inner_node : public node {
public:
    //! Keys of children or data pointers
    Key slotkey[btree_default_traits<Key, Value>::inner_slots]; // NOLINT

    //! Pointers to children
    node * childid[btree_default_traits<Key, Value>::inner_slots + 1]; // NOLINT

    //! Set variables to initial values.
    void initialize(const unsigned short l);

    //! Return key in slot s
    const Key& key(size_t s) const;

    //! True if the node's slots are full.
    bool is_full() const;
    //! True if few used entries, less than half full.
    bool is_few() const {
        return (node::slotuse <= btree_default_traits<Key, Value>::inner_slots / 2);
    }

    //! True if node has too few entries.
    bool is_underflow() const;

    node * get_child(unsigned short slot) const;

    node ** get_childid_vec();

    void set_child(unsigned short slot, node * new_child);

    void copy_to_childid(node ** src_first, node ** src_last, unsigned short offset);

    void copy_backward_to_childid(node ** src_first, node ** src_last, unsigned short offset);

    Key get_slotkey(unsigned short slot) const;

    Key * get_slotkey_vec();

    void set_slotkey(unsigned short slot, Key new_key);

    void copy_to_slotkey(Key * src_first, Key * src_last, unsigned short offset);

    void copy_backward_to_slotkey(Key * src_first, Key * src_last, unsigned short offset);
};

//! Extended structure of a leaf node in memory. Contains pairs of keys and
//! data items. Key and data slots are kept together in value_type.
template <typename Key, typename Value>
class leaf_node : public node {
public:
    //! Double linked list pointers to traverse the leaves
    leaf_node* prev_leaf;

    //! Double linked list pointers to traverse the leaves
    leaf_node* next_leaf;

    //! Array of (key, data) pairs
    Value slotdata[btree_default_traits<Key, Value>::leaf_slots]; // NOLINT

    //! Set variables to initial values
    void initialize();

    //! Return key in slot s.
    const Key& key(size_t s) const;

    //! True if the node's slots are full.
    bool is_full() const;

    //! True if few used entries, less than half full.
    bool is_few() const;

    //! True if node has too few entries.
    bool is_underflow() const;

    Value get_slot(unsigned short slot) const;

    Value& get_slot(unsigned short slot);

    Value * get_slotdata_vec();

    //! Set the (key,data) pair in slot. Overloaded function used by
    //! bulk_load().
    void set_slot(unsigned short slot, const Value& value);

    void copy_to_slotdata(Value * src_first, Value * src_last, unsigned short offset);

    void copy_backward_to_slotdata(Value * src_first, Value * src_last, unsigned short offset);
};

class duplication_info_t
{
public:
	node* orig;
	node* dup;
	node* orig_parent;
	unsigned int orig_idx;
};

class path_info_t
{
public:
    node* child;
    node* parent;
    unsigned int child_idx;
}

thread_local std::vector<duplication_info_t>* duplications = nullptr;
thread_local std::vector<path_info_t>* path = nullptr;
thread_local bool in_writing_function = false;
thread_local bool dup_happened = false;
thread_local node* orig_root;
thread_local node* new_root;

template <typename Key, typename Value>
bool dup_open(node*& root)
{
	if (path)
		path->clear();
	else
		path = new std::vector<path_info_t>();

	if (duplications)
		duplications->clear();
	else
		duplications = new std::vector<duplication_info_t>();

	orig_root = root;
	new_root = nullptr;
	in_writing_function = true;
	dup_happened = false;
	return true;
}

void dup_unlock_duplications(
	std::vector<std::pair<node*, node*>>& locked, 
	bool release)
{
	for (auto& l : locked)
	{
		auto orig = l.first;
		auto orig_parent = l.second;

		pthread_spin_unlock(&orig_parent->dup_lock);
		if (release)
			orig->set_dup();
	}
}

bool dup_lock_duplications(
	std::vector<std::pair<node*, node*>>& locked)
{
	for (auto& d : *duplications)
	{
		auto orig = d.orig;
		auto orig_parent = d.orig_parent;
		if (orig_parent == nullptr)
			continue;

		if (!orig->is_dup() && !pthread_spin_trylock(&orig_parent->dup_lock))
		{
			locked.push_back(std::make_pair(orig, orig_parent));
		}
		else
		{
			dup_unlock_duplications(locked, false);
			return false;
		}
	}

	return true;
}

template <typename Key, typename Value>
bool dup_close(node*& root)
{
	in_writing_function = false;
	if (!dup_happened)
		return true;

	std::vector<std::pair<node*, node*>> locked;
	if (!dup_lock_duplications(locked))
		return false;

	for (auto& d : *duplications)
	{
		auto orig = d.orig;
		auto dup = d.dup;
		auto orig_parent = static_cast<inner_node<Key, Value>*>(d.orig_parent);
		auto orig_idx = d.orig_idx;

		if (orig_parent != nullptr)
		{
			if (orig_parent->childid[orig_idx] == orig)
			{
				orig_parent->childid[orig_idx] = dup;
				orig->set_dup();
			}
			else
			{
				dup_unlock_duplications(locked, true);
				return false;
			}
		}
		else
		{
			if (new_root != nullptr && root == orig_root)
				root = new_root;
			else
				return false;
		}
	}

	dup_unlock_duplications(locked, false);
	return true;
}

void node::initialize(const unsigned short l) {
    level = l;
    slotuse = 0;
    flags = 0;
    pthread_spin_init(&dup_lock, PTHREAD_PROCESS_PRIVATE);
}

bool node::is_leafnode() const {
    return (level == 0);
}

unsigned short node::get_level() const {
    return level;
}

unsigned short node::get_slotuse() const {
    return slotuse;
}

void node::set_slotuse(unsigned short new_slotuse) {
    slotuse = new_slotuse;
}



template <typename Key, typename Value>
void inner_node::initialize(const unsigned short l) {
    node::initialize(l);
}

template <typename Key, typename Value>
const Key& inner_node::key(size_t s) const {
    return slotkey[s];
}

template <typename Key, typename Value>
bool inner_node::is_full() const {
    return (node::slotuse == btree_default_traits<Key, Value>::inner_slots);
}

template <typename Key, typename Value>
bool inner_node::is_few() const {
    return (node::slotuse <= btree_default_traits<Key, Value>::inner_slots / 2);
}

template <typename Key, typename Value>
bool inner_node::is_underflow() const {
    return (node::slotuse < btree_default_traits<Key, Value>::inner_slots / 2);
}

template <typename Key, typename Value>
node * inner_node::get_child(unsigned short slot) const {
    node* child = childid[slot];
	if (in_writing_function && child != nullptr)
	{
		path->push_back({child, this, slot});
	}

    return childid[slot];
}

template <typename Key, typename Value>
node ** inner_node::get_childid_vec() {
    return childid;
}

template <typename Key, typename Value>
void inner_node::set_child(unsigned short slot, node * new_child) {
    childid[slot] = new_child;
}

template <typename Key, typename Value>
void inner_node::copy_to_childid(node ** src_first, node ** src_last, unsigned short offset) {
    // std::memcpy((void*)(childid + offset), (void *)src_first, (long long int)src_last - (long long int)src_first);
    std::copy(src_first, src_last, childid + offset);
}

template <typename Key, typename Value>
void inner_node::copy_backward_to_childid(node ** src_first, node ** src_last, unsigned short offset) {
    // std::memcpy((void*)(childid + offset), (void *)src_first, (long long int)src_last - (long long int)src_first);
    std::copy_backward(src_first, src_last, childid + offset);
}

template <typename Key, typename Value>
Key inner_node::get_slotkey(unsigned short slot) const {
    return slotkey[slot];
}

template <typename Key, typename Value>
Key * inner_node::get_slotkey_vec() {
    return &slotkey[0];
}

template <typename Key, typename Value>
void inner_node::set_slotkey(unsigned short slot, Key new_key) {
    slotkey[slot] = new_key;
}

template <typename Key, typename Value>
void inner_node::copy_to_slotkey(Key * src_first, Key * src_last, unsigned short offset) {
    std::copy(src_first, src_last, slotkey + offset);
}

template <typename Key, typename Value>
void inner_node::copy_backward_to_slotkey(Key * src_first, Key * src_last, unsigned short offset) {
    std::copy_backward(src_first, src_last, slotkey + offset);
}



template <typename Key, typename Value>
void leaf_node::initialize() {
    node::initialize(0);
    prev_leaf = next_leaf = nullptr;
}

template <typename Key, typename Value>
const Key& leaf_node::key(size_t s) const {
    // return KeyOfValue::get(slotdata[s]);
    return slotdata[s].first;
}

template <typename Key, typename Value>
bool leaf_node::is_full() const {
    return (node::slotuse == btree_default_traits<Key, Value>::leaf_slots);
}

template <typename Key, typename Value>
bool leaf_node::is_few() const {
    return (node::slotuse <= btree_default_traits<Key, Value>::leaf_slots / 2);
}

template <typename Key, typename Value>
bool leaf_node::is_underflow() const {
    return (node::slotuse < btree_default_traits<Key, Value>::leaf_slots / 2);
}

template <typename Key, typename Value>
Value leaf_node::get_slot(unsigned short slot) const {
    return slotdata[slot];
}

template <typename Key, typename Value>
Value& leaf_node::get_slot(unsigned short slot) {
    return slotdata[slot];
}

template <typename Key, typename Value>
Value * leaf_node::get_slotdata_vec() {
    return &slotdata[0];
}

template <typename Key, typename Value>
void leaf_node::set_slot(unsigned short slot, const Value& value) {
    TLX_BTREE_ASSERT(slot < node::slotuse);
    slotdata[slot] = value;
}

template <typename Key, typename Value>
void leaf_node::copy_to_slotdata(Value * src_first, Value * src_last, unsigned short offset) {
    std::copy(src_first, src_last, slotdata + offset);
}

template <typename Key, typename Value>
void leaf_node::copy_backward_to_slotdata(Value * src_first, Value * src_last, unsigned short offset) {
    std::copy_backward(src_first, src_last, slotdata + offset);
}

} // namespace tlx