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

#include <mutex>
#include <thread>
#include <sstream>

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
const unsigned int MAX_UINT = std::numeric_limits<unsigned int>::max();

bool do_print = false;
std::mutex g_mutex;


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
    
	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

    node();

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

    void copy_to_slotdata(Value * src_first, Value * src_last, Value * dst_last);

    void copy_backward_to_slotdata(Value * src_first, Value * src_last, Value * dst_last);
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
    node* child;
    node* parent;
    unsigned int child_idx;
};

thread_local std::unordered_map<node*, duplication_info_t>* duplications = nullptr;

// thread_local std::vector<std::pair<node*, bool>>* locked = nullptr;
thread_local std::unordered_map<node*, bool>* locked = nullptr;

// thread_local std::vector<path_info_t>* path = nullptr;
thread_local std::unordered_map<node*, std::pair<node*, unsigned short>>* node_parent_map = nullptr;

thread_local std::unordered_map<node*, bool>* allocated = nullptr;

thread_local bool in_writing_function = false;

thread_local bool dup_happened = false;

thread_local node* orig_root;

thread_local node* new_root;

std::string bug_log[64];

std::stringstream my_tlog[64];
std::string current_log[64];
std::string last_log[64];
std::string last_log_2[64];
std::string last_log_3[64];
std::string last_log_4[64];

bool is_locked(node * n)
{
    if(pthread_spin_trylock(&n->dup_lock) == 0)
    {
        //lock was successful
        pthread_spin_unlock(&n->dup_lock);
        return false;
    }
    else
    {
        return true;
    }
}

template <typename Key, typename Value>
bool validate_children(node * n)
{
    if (n->is_leafnode())
        return true;
    
    Innernode * in = static_cast<Innernode *>(n);
    for (int i = 0; i <= in->slotuse; i++)
    {
        if ((in->childid[i])->is_del())
        {
            return false;
        }
    }

    return true;
}

template <typename Key, typename Value>
void pseudo_print_tree(node * n, int num_tabs, std::stringstream& total)
{
    if (!n)
        return;
    
    if (n->is_leafnode())
    {
        Leafnode * ln = static_cast<Leafnode *>(n);
        for (int j = 0; j < num_tabs; j++)
        {
            total << "\t";
        }
        total << ln << "\n";
        for (int i = 0; i < ln->slotuse; i++)
        {
            for (int j = 0; j < num_tabs + 1; j++)
            {
                total << "\t";
            }
            total << ln->slotdata[i].first << "\n";
        }
    }
    else
    {
        Innernode * in = static_cast<Innernode *>(n);
        for (int j = 0; j < num_tabs; j++)
        {
            total << "\t";
        }
        total << in << "\n";
        for (int i = 0; i < in->slotuse; i++)
        {
            for (int j = 0; j < num_tabs; j++)
            {
                total << "\t";
            }
            total << in->slotkey[i] << "\n";
        }
        for (int i = 0; i <= in->slotuse; i++)
        {
            pseudo_print_tree<Key, Value>(in->childid[i], num_tabs + 1, total);
        }
    }
}

template <typename Key, typename Value>
void print_tree(node * n, int num_tabs)
{
    if (!n)
        return;
    
    if (n->is_leafnode())
    {
        Leafnode * ln = static_cast<Leafnode *>(n);
        for (int j = 0; j < num_tabs; j++)
        {
            std::cout << "\t";
        }
        std::cout << ln << std::endl;
        for (int i = 0; i < ln->slotuse; i++)
        {
            for (int j = 0; j < num_tabs + 1; j++)
            {
                std::cout << "\t";
            }
            std::cout << ln->slotdata[i].first << std::endl;
        }
    }
    else
    {
        Innernode * in = static_cast<Innernode *>(n);
        for (int j = 0; j < num_tabs; j++)
        {
            std::cout << "\t";
        }
        std::cout << in << std::endl;
        for (int i = 0; i < in->slotuse; i++)
        {
            for (int j = 0; j < num_tabs; j++)
            {
                std::cout << "\t";
            }
            std::cout << in->slotkey[i] << std::endl;
        }
        for (int i = 0; i <= in->slotuse; i++)
        {
            print_tree<Key, Value>(in->childid[i], num_tabs + 1);
        }
    }
}

template <typename Key, typename Value>
void check_tree_3_helper(node * n, int * arr, int arr_size)
{
    if (!n)
        return;

    bool result = true;
    if (n->is_leafnode())
    {
        Leafnode * ln = static_cast<Leafnode *>(n);
        for (int i = 0; i < ln->slotuse; i++)
        {
            if (ln->slotdata[i].first > arr_size + 1)
            {
                std::cout << "error" << std::endl;
                exit(-1);
            }

            if (arr[ln->slotdata[i].first] > 0)
            {
                std::cout << "~" << ln->slotdata[i].first << "~" << i << "~" << ln->slotuse << std::endl;
            }

            arr[ln->slotdata[i].first]++;
        }
    }
    else
    {
        Innernode * in = static_cast<Innernode *>(n);
        for (int i = 0; i <= n->slotuse; i++)
        {
            check_tree_3_helper<Key, Value>(in->childid[i], arr, arr_size);
        }
    }
}

template <typename Key, typename Value>
bool check_tree_3(node * n)
{
    int arr[1001];
    
    for (int i = 0; i < 1001; i++)
        arr[i] = 0;

    check_tree_3_helper<Key, Value>(n, arr, 1000);

    for (int i = 0; i < 1000; i++)
    {
        if (arr[i] > 1)
        {
            std::cout << "check_tree_3 " << i << " " << arr[i] << std::endl;
            return false;
        }
    }

    return true;
}

template <typename Key, typename Value>
bool check_tree_2(node * n, int index)
{
    if (!n)
        return true;

    bool result = true;
    if (n->is_leafnode())
    {
        if (n->is_del()) {
            std::cout << "check_tree_2: leaf - " << n << " index " << index << std::endl;
            return false;
        }
    }
    else
    {
        Innernode * in = static_cast<Innernode *>(n);
        if (n->is_del()) {
            std::cout << "check_tree_2: inner - " << in << std::endl;
            result = false;
        }

        for (int i = 0; i <= n->slotuse; i++)
        {
            if (!check_tree_2<Key, Value>(in->childid[i], i))
            {
                std::cout << "check_tree_2: \tparent - " << in << std::endl;
                result = false;
            }
        }
    }

    return result;
}

unsigned long long history_time = 0;

template <typename Key, typename Value>
bool dup_open(int tid, node*& root)
{
    // my_tlog[tid] << "dup_open - " << history_time << "\n";
	if (duplications)
		duplications->clear();
	else
		duplications = new std::unordered_map<node*, duplication_info_t>();

    if (locked)
		locked->clear();
	else
        locked = new std::unordered_map<node*, bool>();

    if (node_parent_map)
		node_parent_map->clear();
	else
		node_parent_map = new std::unordered_map<node*, std::pair<node*, unsigned short>>();

    if (allocated)
        allocated->clear();
    else
        allocated = new std::unordered_map<node*, bool>();

    // if (!duplications->empty() || !locked->empty() || !node_parent_map->empty() || !allocated->empty())
    // {
    //     my_tlog[tid] << "error in dup_open\n";
    // }

	orig_root = root;
	new_root = root;
	in_writing_function = true;
	dup_happened = false;
	return true;
}

template <typename Key, typename Value>
void dup_unlock_duplications(int tid, bool all)
{
	for (auto& l : *locked)
	{
        // if (!is_locked(l.first))
        // {
        //     // my_tlog[tid] << "ERROR!!! - " << l.first << " is not locked!" << "\n";
        //     // std::cout << "ERROR!!! - " << l.first << " is not locked! - " << tid << "\n";
        //     exit(-1);
        // }
        
        // if (l.first != nullptr && !(l.first)->is_leafnode())
        // {
        //     my_tlog[tid] << l.first << "'s children when 'unlocked' are: ";
        //     Innernode * i_n = static_cast<Innernode*>(l.first);
        //     for (int i = 0; i <= i_n->slotuse; i++)
        //     {
        //         my_tlog[tid] << i_n->childid[i] << ", ";
        //     }
        //     my_tlog[tid] << "\n";
        // }
		if (all || l.second)
        {
            locked->erase(l.first);
			pthread_spin_unlock(&l.first->dup_lock);
            // my_tlog[tid] << "unlock " << l.first << "\n";
        }
	}
}

bool dup_lock_duplications()
{
	return true;
}

template <typename Key, typename Value>
bool dup_close(int tid, node*& root)
{
    bool result = true;

    // std::lock_guard<std::mutex> lck(g_mutex);

    // if (!check_tree_2<Key, Value>(root, 0))
    // {
    //     std::cout << "\tbefore dup_close - " << tid << std::endl;
    //     exit(-1);
    // }

    // std::stringstream ss;
    // pseudo_print_tree<Key, Value>(root, 0, ss);

	in_writing_function = false;
	if (!dup_happened)
    {
        result = true;
        goto end;
		// return true;
    }

	if (!dup_lock_duplications())
    {
        result = false;
        goto end;
		// return false;
    }
    
    /* check that parent-child relations are as expected */
	for (auto& d: *duplications)
	{
		auto orig = d.first;
        auto dup = d.second.dup;
		auto orig_parent = static_cast<Innernode*>(d.second.orig_parent);
		auto orig_idx = d.second.orig_idx;

        if (!validate_children<Key, Value>(dup))
        {
            std::cout << "fuck - " << tid << std::endl;
            exit(-1);
        }

        if (duplications->find(orig_parent) != duplications->end() ||
            allocated->find(orig_parent) != allocated->end())
        {
            auto bla = static_cast<Innernode*>((duplications->at(orig_parent)).dup);
            bool pr = false;
            for (int i = 0; i <= bla->slotuse; i++)
            {
                if (bla->childid[i] == orig)
                {
                    pr = true;
                }
            }
            if (pr)
            {
                std::cout << "@@@@@" << std::endl;
                exit(-1);
            }
            continue;
        }
        
		if (orig_parent != nullptr && orig_parent->childid[orig_idx] != orig) 
        {
			dup_unlock_duplications<Key, Value>(tid, true);
            result = false;
            goto end;
			// return false;
		}
	}

	for (auto& d : *duplications)
	{
		auto orig = d.first;
		auto dup = d.second.dup;
		auto orig_parent = static_cast<Innernode*>(d.second.orig_parent);
		auto orig_idx = d.second.orig_idx;

        if (duplications->find(orig_parent) != duplications->end() ||
            allocated->find(orig_parent) != allocated->end())
        {
            auto bla = static_cast<Innernode*>((duplications->at(orig_parent)).dup);
            bool pr = false;
            for (int i = 0; i <= bla->slotuse; i++)
            {
                if (bla->childid[i] == orig)
                {
                    pr = true;
                }
            }
            if (pr)
            {
                std::cout << "@@@@@" << std::endl;
                exit(-1);
            }
            continue;
        }

		if (orig_parent != nullptr)
		{
			// if (!__atomic_compare_exchange_n(
			// 		&orig_parent->childid[orig_idx], 
			// 		&orig, 
			// 		dup, 
			// 		true, 
			// 		__ATOMIC_RELAXED, 
			// 		__ATOMIC_RELAXED))
			// {
            //     //suicide
			// 	dup_unlock_duplications(tid, true);
            //     result = false;
            //     goto end;
			// 	// return false;
			// }

            orig_parent->childid[orig_idx] = dup;

            // my_tlog[tid] << "managed to replace orig " << orig << " with dup " << dup << "for parent " << orig_parent << "[" << orig_idx << "]" << "\n";
            // if (orig_parent->childid[orig_idx] != dup)
            //     my_tlog[tid] << "ERROR!!!\n";
            orig->set_del();
		}
		else
		{
			if (orig_root != orig || !__atomic_compare_exchange_n(
					&root, 
					&orig_root, 
					new_root, 
					true, 
					__ATOMIC_RELAXED, 
					__ATOMIC_RELAXED))
			{
				dup_unlock_duplications<Key, Value>(tid, true);
                result = false;
                goto end;
				// return false;
			}
            // my_tlog[tid] << "managed to replace orig_root " << orig_root << " with new_root" << new_root  << "\n";
		}
	}

	dup_unlock_duplications<Key, Value>(tid, false);
    // my_tlog[tid] << "dup_close\n";
end:
    // if (!check_tree_2<Key, Value>(root, 0))
    // {
    //     std::cout << "\tafter dup_close - " << tid << std::endl;
    //     std::cout << "\t\tresult - " << result << std::endl;
    //     std::cout << "\t\tduplications - " << std::endl;
    //     for (auto& d : *duplications)
    //     {
    //         std::cout << "\t\t\t" << d.first << " => " << d.second.dup << " (" << d.second.orig_parent << "[" << d.second.orig_idx << "])" << std::endl;
    //     }
    //     std::cout << "\t\tallocated - " << std::endl;
    //     for (auto& d : *allocated)
    //     {
    //         std::cout << "\t\t\t" << d.first << std::endl;
    //     }
    //     for (int i = 0; i < 64; i++)
    //         if (bug_log[i] != "")
    //             std::cout << "\t\tlog - " << i << "\n" << bug_log[i] << std::endl;
    //     for (int i = 0; i < 64; i++)
    //     {
    //         if (current_log[i] != "")
    //         {
    //             std::cout << "\t" << i <<" - current log" << std::endl;
    //             std::cout << "\t" << "###############" << std::endl;
    //             std::cout << my_tlog[i].str() << std::endl;
    //         }
    //         if (last_log[i] != "")
    //         {
    //             std::cout << "\t" << i <<" - last log" << std::endl;
    //             std::cout << "\t" << "###############" << std::endl;
    //             std::cout << current_log[i] << std::endl;
    //         }
    //         if (last_log_2[i] != "")
    //         {
    //             std::cout << "\t" << i <<" - older log" << std::endl;
    //             std::cout << "\t" << "###############" << std::endl;
    //             std::cout << last_log[i] << std::endl;
    //         }
    //         if (last_log_3[i] != "")
    //         {
    //             std::cout << "\t" << i <<" - older older log" << std::endl;
    //             std::cout << "\t" << "###############" << std::endl;
    //             std::cout << last_log_2[i] << std::endl;
    //         }
    //         if (last_log_3[i] != "")
    //         {
    //             std::cout << "\t" << i <<" - older older older log" << std::endl;
    //             std::cout << "\t" << "###############" << std::endl;
    //             std::cout << last_log_3[i] << std::endl;
    //         }
    //     }
    //     std::cout << "before: *******************************\n\n\n" << std::endl;
    //     std::cout << ss.str() << std::endl;
    //     std::cout << "after: *******************************\n\n\n" << std::endl;
    //     print_tree<Key, Value>(root, 0);
    //     exit(-1);
    // }
    // if (bug_log[tid].size() > 40)
    //     bug_log[tid] = bug_log[tid].substr(bug_log[tid].size() - 25);
    // history_times++;
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
        if (do_print)
            std::cout << "is_underflow " << found.dup << std::endl;
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
        node * orig = (node*)this;
        if (duplications->find(orig) != duplications->end())
        {
            node* dup = (duplications->at(orig)).dup;
            child = ((Innernode*)dup)->childid[slot];
            node_parent_map->insert(
                std::make_pair<node*, std::pair<node*, unsigned short>>(
                    (node*)child, std::make_pair<node*, unsigned short>(
                        (node*)dup, (unsigned short)slot)));
        }
        else
        {
            node_parent_map->insert(
                std::make_pair<node*, std::pair<node*, unsigned short>>(
                    (node*)child, std::make_pair<node*, unsigned short>(
                        (node*)this, (unsigned short)slot)));
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
    // std::memcpy((void*)(childid + offset), (void *)src_first, (long long int)src_last - (long long int)src_first);
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

} // namespace tlx