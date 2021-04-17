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
#include <unordered_map>
#include <iostream>

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

#define Innernode inner_node<Key, Value>
#define Leafnode leaf_node<Key, Value>

const unsigned char DUP_MASK = 0x01;
const unsigned char DEL_MASK = 0x02;
const unsigned char COM_MASK = 0x04;
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
    node* my_dup;
    
    inline bool is_dup() { return (flags & DUP_MASK) == DUP_MASK; }
	inline void set_dup() { flags |= DUP_MASK; }
	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }
    inline bool is_com() { return (flags & COM_MASK) == COM_MASK; }
    inline void set_com() { flags |= COM_MASK; }

    node();

    //! Delayed initialisation of constructed node.
    void initialize(const unsigned short l);

    //! True if this is a leaf node.
    bool is_leafnode() const;

    unsigned short get_level() const;

    unsigned short get_slotuse() const;

    node * get_self() const;

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
    bool is_few() const ;

    //! True if node has too few entries.
    bool is_underflow() const;

    node * get_child(unsigned short slot) const;

    node ** get_childid_vec();

    void set_child(unsigned short slot, node * new_child);

    void copy_to_childid(node ** src_first, node ** src_last, node ** dst_last);

    void copy_backward_to_childid(node ** src_first, node ** src_last, node ** dst_last);

    Key get_slotkey(unsigned short slot) const;

    Key * get_slotkey_vec();

    void set_slotkey(unsigned short slot, Key new_key);

    void copy_to_slotkey(Key * src_first, Key * src_last, Key * dst_last);

    void copy_backward_to_slotkey(Key * src_first, Key * src_last, Key * dst_last);
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

    node** commit_point_addr;
    node* commit_point_content;

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

    leaf_node * get_next_leaf();

    leaf_node * get_prev_leaf();

    //! Set the (key,data) pair in slot. Overloaded function used by
    //! bulk_load().
    void set_slot(unsigned short slot, const Value& value);

    void copy_to_slotdata(Value * src_first, Value * src_last, Value * dst_last);

    void copy_backward_to_slotdata(Value * src_first, Value * src_last, Value * dst_last);

    void set_next_leaf(leaf_node * new_next);

    void set_prev_leaf(leaf_node * new_prev);
};


class duplication_info_t
{
public:
	node* dup;
	node* orig_parent;
	unsigned int orig_idx;
};

class path_info_t
{
public:
    node * self;
    node * parent;
    unsigned short index;
    unsigned short height;
};

thread_local std::unordered_map<node*, duplication_info_t>* duplications = nullptr;

thread_local std::vector<node*>* to_delete = nullptr;

thread_local std::unordered_map<node*, node*>* dup_orig_map = nullptr;

thread_local std::unordered_map<node*, bool>* locked = nullptr;

thread_local std::unordered_map<node*, path_info_t>* node_parent_map = nullptr;

thread_local std::unordered_map<node*, bool>* allocated = nullptr;

thread_local bool in_writing_function = false;

thread_local bool dup_happened = false;

thread_local node* orig_root;

thread_local node* new_root;

thread_local node* left_most_leaf;

thread_local node* right_most_leaf;

template <typename Key, typename Value>
bool dup_open(int tid, node** root)
{
	if (duplications)
		duplications->clear();
	else
		duplications = new std::unordered_map<node*, duplication_info_t>();

    if (to_delete)
        to_delete->clear();
    else
        to_delete = new std::vector<node*>();

    if (dup_orig_map)
		dup_orig_map->clear();
	else
		dup_orig_map = new std::unordered_map<node*, node*>();

    if (locked)
		locked->clear();
	else
        locked = new std::unordered_map<node*, bool>();

    if (node_parent_map)
		node_parent_map->clear();
	else
        node_parent_map = new std::unordered_map<node*, path_info_t>();

    if (allocated)
        allocated->clear();
    else
        allocated = new std::unordered_map<node*, bool>();

	orig_root = *root;
	new_root = *root;
	in_writing_function = true;
	dup_happened = false;

    left_most_leaf = nullptr;
    right_most_leaf = nullptr;

    if (orig_root)
        node_parent_map->insert({ orig_root, { orig_root, nullptr, 0, 0 }});

	return true;
}

template <typename Key, typename Value>
void dup_unlock_duplications(int tid, bool all)
{
	for (auto& l : *locked)
	{
		if (all || l.second)
        {
            locked->erase(l.first);
			pthread_spin_unlock(&l.first->dup_lock);
        }
	}
}

template <typename Key, typename Value>
bool dup_close(int tid, node** root)
{
    bool result = true;

	in_writing_function = false;
	if (!dup_happened)
    {
        result = true;
        goto end;
    }
    
    /* check that parent-child relations are as expected */
    node** com_pt_addr = nullptr;
    node* com_pt_content = nullptr;
    node* lml_dup = nullptr;
    node* rml_dup = nullptr;
	for (auto& d: *duplications)
	{
		auto orig = d.first;
        auto dup = d.second.dup;
		auto orig_parent = static_cast<Innernode*>(d.second.orig_parent);
		auto orig_idx = d.second.orig_idx;

        if (orig == left_most_leaf)
            lml_dup = dup;
        else if (orig == right_most_leaf)
            rml_dup = dup;

        if (duplications->find(orig_parent) != duplications->end() ||
            allocated->find(orig_parent) != allocated->end())
        {
            continue;
        }
        
		if (orig_parent != nullptr && orig_parent->childid[orig_idx] != orig) 
        {
			dup_unlock_duplications<Key, Value>(tid, true);
            result = false;
            goto end;
		}

        com_pt_addr = &orig_parent->childid[orig_idx];
        com_pt_content = dup;
	}

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    if (left_most_leaf)
    {
        Leafnode* llml = static_cast<Leafnode*>(left_most_leaf);
        if (llml->prev_leaf != nullptr)
        {
            if (!pthread_spin_trylock(&llml->prev_leaf->dup_lock))
            {
                locked->insert(std::make_pair(llml->prev_leaf, true));
            }
            else
            {
                dup_unlock_duplications<Key, Value>(tid, true);
                result = false;
                goto end;
            }
        }

        llml->set_com();
        llml->commit_point_addr = com_pt_addr;
        llml->commit_point_content = com_pt_content;
    }
    if (right_most_leaf)
    {
        Leafnode* lrml = static_cast<Leafnode*>(right_most_leaf);
        if (lrml->next_leaf != nullptr)
        {
            if (!pthread_spin_trylock(&lrml->next_leaf->dup_lock))
            {
                locked->insert(std::make_pair(lrml->next_leaf, true));
            }
            else
            {
                dup_unlock_duplications<Key, Value>(tid, true);
                result = false;
                goto end;
            }
        }

        lrml->set_com();
        lrml->commit_point_addr = com_pt_addr;
        lrml->commit_point_content = com_pt_content;
    }
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	for (auto& d : *duplications)
	{
		auto orig = d.first;
		auto dup = d.second.dup;
		auto orig_parent = static_cast<Innernode*>(d.second.orig_parent);
		auto orig_idx = d.second.orig_idx;

        if (duplications->find(orig_parent) != duplications->end() ||
            allocated->find(orig_parent) != allocated->end())
        {
            continue;
        }

		if (orig_parent != nullptr)
		{
            orig_parent->childid[orig_idx] = dup;
		}
		else
		{
            orig_root = new_root;
            *root = new_root;
		}
	}

    if (orig_root != new_root)
	{
		if (!__atomic_compare_exchange_n(root, &orig_root, new_root, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
		{
			dup_unlock_duplications<Key, Value>(tid, true);
			result = false;
			goto end;
		}
	}

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    if (left_most_leaf)
    {
        Leafnode* llml = static_cast<Leafnode*>(left_most_leaf);
        if (llml->prev_leaf != nullptr)
            llml->prev_leaf->next_leaf = lml_dup;
        llml->commit_point_addr = nullptr;
        llml->commit_point_content = nullptr;
    }
    if (left_most_leaf)
    {
        Leafnode* lrml = static_cast<Leafnode*>(right_most_leaf);
        if (lrml->prev_leaf != nullptr)
            lrml->prev_leaf->next_leaf = rml_dup;
        lrml->commit_point_addr = nullptr;
        lrml->commit_point_content = nullptr;
    }
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	dup_unlock_duplications<Key, Value>(tid, false);

end:
	return result;
}

node::node()
{
    allocated->insert({this, true});
}

void node::initialize(const unsigned short l) {
    level = l;
    slotuse = 0;
    flags = 0;
    my_dup = nullptr;
    pthread_spin_init(&dup_lock, PTHREAD_PROCESS_PRIVATE);
}

bool node::is_leafnode() const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return (found.dup->level == 0);
    }

    return (level == 0);
}

unsigned short node::get_level() const 
{
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return found.dup->level;
    }
    
    return level;
}

unsigned short node::get_slotuse() const 
{
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return found.dup->slotuse;
    }

    return slotuse;
}

void node::set_slotuse(unsigned short new_slotuse) {
    slotuse = new_slotuse;
}

node * node::get_self() const
{
    node * self = (node *)this;
    if (in_writing_function)
    {
		if (self != nullptr && duplications->find(self) != duplications->end()) {
			self = duplications->at(self).dup;
		}
    }

	return self;
}



template <typename Key, typename Value>
void Innernode::initialize(const unsigned short l) {
    node::initialize(l);
}

template <typename Key, typename Value>
const Key& Innernode::key(size_t s) const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return ((Innernode*)found.dup)->slotkey[s];
    }

    return slotkey[s];
}

template <typename Key, typename Value>
bool Innernode::is_full() const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return (found.dup->slotuse == btree_default_traits<Key, Value>::inner_slots);
    }

    return (node::slotuse == btree_default_traits<Key, Value>::inner_slots);
}

template <typename Key, typename Value>
bool Innernode::is_few() const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return (found.dup->slotuse <= btree_default_traits<Key, Value>::inner_slots / 2);
    }

    return (node::slotuse <= btree_default_traits<Key, Value>::inner_slots / 2);
}

template <typename Key, typename Value>
bool Innernode::is_underflow() const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return (found.dup->slotuse < btree_default_traits<Key, Value>::inner_slots / 2);
    }

    return (node::slotuse < btree_default_traits<Key, Value>::inner_slots / 2);
}

template <typename Key, typename Value>
node * Innernode::get_child(unsigned short slot) const 
{
    node* child = childid[slot];
    if (in_writing_function)
    {
        node * parent = (node*)this;
    
        if (duplications->find(parent) != duplications->end())
        {
            node * dup = (duplications->at(parent)).dup;
            parent = dup;
            child = ((Innernode*)parent)->childid[slot];
        }
    
        if (child != NULL && allocated->find(child) == allocated->end() && allocated->find(parent) == allocated->end())
        {
            if (node_parent_map->find(child) == node_parent_map->end())
            {
                unsigned short parent_height = 0;
				if (node_parent_map->find(parent) != node_parent_map->end())
					parent_height = node_parent_map->at(parent).height;
    
                node_parent_map->insert({
						child, { child, parent, (unsigned short)slot, (unsigned short)(parent_height + 1) }});
            }
        }
        else if (child != NULL)
        {
            auto ochild = child;
			if (dup_orig_map->find(child) != dup_orig_map->end())
				ochild = dup_orig_map->at(child);
    
            auto oparent = parent;
			if (dup_orig_map->find(parent) != dup_orig_map->end())
				oparent = dup_orig_map->at(parent);
    
            unsigned short oparent_height = 0;
			if (node_parent_map->find(oparent) != node_parent_map->end())
            	oparent_height = node_parent_map->at(oparent).height;
    
            if (((Innernode*)oparent)->childid[slot] == ochild) {
				node_parent_map->insert({
						ochild, { ochild, oparent, (unsigned short)slot, (unsigned short)(oparent_height + 1) }});
			}
        }
    }
    
    return child;
}

template <typename Key, typename Value>
node ** Innernode::get_childid_vec() 
{
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return ((Innernode*)found.dup)->childid;
    }

    return childid;
}

template <typename Key, typename Value>
void Innernode::set_child(unsigned short slot, node * new_child) {
    childid[slot] = new_child;
}

template <typename Key, typename Value>
void Innernode::copy_to_childid(node ** src_first, node ** src_last, node ** dst_last) {
    std::copy(src_first, src_last, dst_last);
}

template <typename Key, typename Value>
void Innernode::copy_backward_to_childid(node ** src_first, node ** src_last,  node ** dst_last) {
    // std::memcpy((void*)(childid + offset), (void *)src_first, (long long int)src_last - (long long int)src_first);
    std::copy_backward(src_first, src_last, dst_last);
}

template <typename Key, typename Value>
Key Innernode::get_slotkey(unsigned short slot) const 
{
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return ((Innernode*)found.dup)->slotkey[slot];
    }

    return slotkey[slot];
}

template <typename Key, typename Value>
Key * Innernode::get_slotkey_vec() 
{
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return &((Innernode*)found.dup)->slotkey[0];
    }

    return &slotkey[0];
}

template <typename Key, typename Value>
void Innernode::set_slotkey(unsigned short slot, Key new_key) {
    slotkey[slot] = new_key;
}

template <typename Key, typename Value>
void Innernode::copy_to_slotkey(Key * src_first, Key * src_last, Key * dst_last) {
    std::copy(src_first, src_last, dst_last);
}

template <typename Key, typename Value>
void Innernode::copy_backward_to_slotkey(Key * src_first, Key * src_last, Key * dst_last) {
    std::copy_backward(src_first, src_last, dst_last);
}



template <typename Key, typename Value>
void Leafnode::initialize() {
    node::initialize(0);
    prev_leaf = next_leaf = nullptr;
}

template <typename Key, typename Value>
const Key& Leafnode::key(size_t s) const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return ((Leafnode*)found.dup)->slotdata[s].first;
    }

    return slotdata[s].first;
}

template <typename Key, typename Value>
bool Leafnode::is_full() const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return (found.dup->slotuse == btree_default_traits<Key, Value>::leaf_slots);
    }

    return (node::slotuse == btree_default_traits<Key, Value>::leaf_slots);
}

template <typename Key, typename Value>
bool Leafnode::is_few() const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return (found.dup->slotuse <= btree_default_traits<Key, Value>::leaf_slots / 2);
    }

    return (node::slotuse <= btree_default_traits<Key, Value>::leaf_slots / 2);
}

template <typename Key, typename Value>
bool Leafnode::is_underflow() const {
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return (found.dup->slotuse < btree_default_traits<Key, Value>::leaf_slots / 2);
    }

    return (node::slotuse < btree_default_traits<Key, Value>::leaf_slots / 2);
}

template <typename Key, typename Value>
Value Leafnode::get_slot(unsigned short slot) const 
{
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return ((Leafnode*)found.dup)->slotdata[slot];
    }

    return slotdata[slot];
}

template <typename Key, typename Value>
Value& Leafnode::get_slot(unsigned short slot) 
{
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return ((Leafnode*)found.dup)->slotdata[slot];
    }

    return slotdata[slot];
}

template <typename Key, typename Value>
Value * Leafnode::get_slotdata_vec() 
{
    node * orig = (node*)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        return &((Leafnode*)found.dup)->slotdata[0];
    }

    return &slotdata[0];
}

template <typename Key, typename Value>
Leafnode * Leafnode::get_next_leaf()
{
    Leafnode * current = (Leafnode*)this;
    Leafnode * next = next_leaf;
    if (in_writing_function && duplications->find(current) != duplications->end())
    {
        auto found = duplications->at(current);
        next = ((Leafnode*)found.dup)->next_leaf;
    }
    else if (!in_writing_function)
    {
        if (next->commit_point_addr && *next->commit_point_addr == next->commit_point_content)
        {
            next = (Leafnode*)next->my_dup;
        }
    }

    return next;
}

template <typename Key, typename Value>
Leafnode * Leafnode::get_prev_leaf()
{
    Leafnode * orig = (Leafnode*)this;
    Leafnode * prev = prev_leaf;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        prev = ((Leafnode*)found.dup)->prev_leaf;
    }
    else if (!in_writing_function)
    {
        if (prev->commit_point_addr && *prev->commit_point_addr == prev->commit_point_content)
        {
            prev = (Leafnode*)prev->my_dup;
        }
    }

    return prev;
}

template <typename Key, typename Value>
void Leafnode::set_slot(unsigned short slot, const Value& value) {
    TLX_BTREE_ASSERT(slot < node::slotuse);
    slotdata[slot] = value;
}

template <typename Key, typename Value>
void Leafnode::copy_to_slotdata(Value * src_first, Value * src_last, Value * dst_last) {
    std::copy(src_first, src_last, dst_last);
}

template <typename Key, typename Value>
void Leafnode::copy_backward_to_slotdata(Value * src_first, Value * src_last, Value * dst_last) {
    std::copy_backward(src_first, src_last, dst_last);
}

template <typename Key, typename Value>
void Leafnode::set_next_leaf(Leafnode * new_next)
{
    next_leaf = new_next;
}

template <typename Key, typename Value>
void Leafnode::set_prev_leaf(Leafnode * new_prev)
{
    prev_leaf = new_prev;
}

} // namespace tlx