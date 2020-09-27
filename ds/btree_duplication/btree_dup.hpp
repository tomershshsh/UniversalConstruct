#pragma once

#include "btree.hpp"
#include "record_manager.h"

template <typename skey_t, typename sval_t, class RecMgr>
class btree_dup {
public:
    //! \name Template Parameter Types
    //! \{

    //! First template parameter: The key type of the btree. This is stored in
    //! inner nodes.
    typedef skey_t key_type;

    //! Second template parameter: The value type associated with each key.
    //! Stored in the B+ tree's leaves
    typedef sval_t data_type;

    //! Third template parameter: Key comparison function object
    typedef std::less<skey_t> key_compare;

    //! Fourth template parameter: Traits object used to define more parameters
    //! of the B+ tree
    typedef tlx::btree_default_traits<skey_t, std::pair<skey_t, skey_t> > traits;

    //! Fifth template parameter: STL allocator
    typedef std::allocator<std::pair<skey_t, skey_t> > allocator_type;

public:
    //! \name Constructed Types
    //! \{

    //! Typedef of our own type
    typedef btree_dup<key_type, data_type, RecMgr> self;

    //! Construct the STL-required value_type as a composition pair of key and
    //! data types
    typedef std::pair<key_type, data_type> value_type;

    //! Key Extractor Struct
    struct key_of_value {
        //! pull first out of pair
        static const key_type& get(const value_type& v) { return v.first; }
    };

    //! Implementation type of the btree_base
    typedef tlx::BTree<key_type, value_type, key_of_value, RecMgr, key_compare,
                  traits, false, allocator_type> btree_impl;

    //! Function class comparing two value_type pairs.
    typedef typename btree_impl::value_compare value_compare;

    //! Size type used to count keys
    typedef typename btree_impl::size_type size_type;

    //! Small structure containing statistics about the tree
    typedef typename btree_impl::tree_stats tree_stats;

    //! \}

public:
    //! \name Static Constant Options and Values of the B+ Tree
    //! \{

    //! Base B+ tree parameter: The number of key/data slots in each leaf
    static const unsigned short leaf_slotmax = btree_impl::leaf_slotmax;

    //! Base B+ tree parameter: The number of key slots in each inner node,
    //! this can differ from slots in each leaf.
    static const unsigned short inner_slotmax = btree_impl::inner_slotmax;

    //! Computed B+ tree parameter: The minimum number of key/data slots used
    //! in a leaf. If fewer slots are used, the leaf will be merged or slots
    //! shifted from it's siblings.
    static const unsigned short leaf_slotmin = btree_impl::leaf_slotmin;

    //! Computed B+ tree parameter: The minimum number of key slots used
    //! in an inner node. If fewer slots are used, the inner node will be
    //! merged or slots shifted from it's siblings.
    static const unsigned short inner_slotmin = btree_impl::inner_slotmin;

    //! Debug parameter: Enables expensive and thorough checking of the B+ tree
    //! invariants after each insert/erase operation.
    static const bool self_verify = btree_impl::self_verify;

    //! Debug parameter: Prints out lots of debug information about how the
    //! algorithms change the tree. Requires the header file to be compiled
    //! with TLX_BTREE_DEBUG and the key type must be std::ostream printable.
    static const bool debug = btree_impl::debug;

    //! Operational parameter: Allow duplicate keys in the btree.
    static const bool allow_duplicates = btree_impl::allow_duplicates;

    //! \}

public:
    //! \name Iterators and Reverse Iterators
    //! \{

    //! STL-like iterator object for B+ tree items. The iterator points to a
    //! specific slot number in a leaf.
    typedef typename btree_impl::iterator iterator;

    //! STL-like iterator object for B+ tree items. The iterator points to a
    //! specific slot number in a leaf.
    typedef typename btree_impl::const_iterator const_iterator;

    //! create mutable reverse iterator by using STL magic
    typedef typename btree_impl::reverse_iterator reverse_iterator;

    //! create constant reverse iterator by using STL magic
    typedef typename btree_impl::const_reverse_iterator const_reverse_iterator;

    //! \}

private:
    //! \name Tree Implementation Object
    //! \{

    //! The contained implementation object
    btree_impl tree_;

    const unsigned int idx_id;
    const skey_t KEY_MIN;
    const skey_t KEY_MAX;
    const sval_t NO_VALUE;
	int init[MAX_THREADS_POW2] = {0,};
    RecMgr* recmgr;

    //! \}

public:
    //! \name Constructors and Destructor
    //! \{

    //! Default constructor initializing an empty B+ tree with the standard key
    //! comparison function
    explicit btree_dup(
        const int _NUM_THREADS, 
        const skey_t& _KEY_MIN, 
        const skey_t& _KEY_MAX, 
        const sval_t& _VALUE_RESERVED, 
        unsigned int id) :
        idx_id(id), 
        KEY_MIN(_KEY_MIN), 
        KEY_MAX(_KEY_MAX), 
        NO_VALUE(_VALUE_RESERVED),  
        tree_(_NUM_THREADS, allocator_type())
    { 
        const int tid = 0;
        initThread(tid);
        tree_.recmgr->endOp(tid);
    }

    //! Frees up all used B+ tree memory pages
    ~btree_dup()
    {
        delete tree_.recmgr; 
    }

    RecMgr* debugGetRecMgr()
    {
        return tree_.recmgr;
    }

    void initThread(const int tid)
    {
        if (init[tid]) return;
        else init[tid] = !init[tid];
        tree_.recmgr->initThread(tid);
    }

    void deinitThread(const int tid)
    {
        if (!init[tid]) return;
        else init[tid] = !init[tid];
        tree_.recmgr->deinitThread(tid);
    }

    struct tlx::node* get_root()
    {
        return tree_.root_;
    }


public:
    //! \name Key and Value Comparison Function Objects
    //! \{

    //! Constant access to the key comparison object sorting the B+ tree
    key_compare key_comp() const {
        return tree_.key_comp();
    }

    //! Constant access to a constructed value_type comparison object. required
    //! by the STL
    value_compare value_comp() const {
        return tree_.value_comp();
    }

    //! \}

public:
    //! \name Allocators
    //! \{

    //! Return the base node allocator provided during construction.
    allocator_type get_allocator() const {
        return tree_.get_allocator();
    }

    //! \}

public:
    //! \name STL Access Functions Querying the Tree by Descending to a Leaf
    //! \{

    //! Tries to locate a key in the B+ tree and returns an iterator to the
    //! key/data slot if found. If unsuccessful it returns end().
    sval_t find(const int tid, const skey_t& key) 
    {
        auto guard = tree_.recmgr->getGuard(tid, true);
        auto it = tree_.find(key);
        if (it == tree_.end()) {
            return NO_VALUE;
        }
        else {
            return (*it).second;
        }
    }

public:
    //! \name Public Insertion Functions
    //! \{

    //! Attempt to insert a key/data pair into the B+ tree. Fails if the pair is
    //! already present.
    sval_t insert(const int tid, const skey_t& key, const sval_t& value) 
    {
        unsigned int counter = 0;
        while (1)
        {            
            auto guard = tree_.recmgr->getGuard(tid);
            tlx::dup_open<key_type, value_type>(tid, &tree_.root_);
            tlx::locking_res = true;
            auto insertion_res = tree_.insert(tid, std::make_pair(key, value));

            if (tlx::locking_res && tlx::dup_close<key_type, value_type>(tid, &tree_.root_))
            {
                for (auto& d : *tlx::duplications)
                {
                    if (d.first->is_leafnode()) {
                        tree_.recmgr->retire(tid, static_cast<tlx::leaf_node<key_type, value_type>*>(d.first));
                    }
                    else {
                        tree_.recmgr->retire(tid, static_cast<tlx::inner_node<key_type, value_type>*>(d.first));
                    }
                }
                
                if (insertion_res.second)
                    return NO_VALUE;
                else
                    return value;
            }
            else
            {
                for (auto& d : *tlx::allocated)
                {
                    if (d.first->is_leafnode()) {
                        tree_.recmgr->deallocate(tid, static_cast<tlx::leaf_node<key_type, value_type>*>(d.first));
                    }
                    else {
                        tree_.recmgr->deallocate(tid, static_cast<tlx::inner_node<key_type, value_type>*>(d.first));
                    }
                }
            }
        }
    }

    //! \}

public:
    //! \name Public Erase Functions
    //! \{

    //! Erases the key/data pairs associated with the given key. For this
    //! unique-associative map there is no difference to erase().
    sval_t erase(const int tid, const skey_t& key) 
    {
        unsigned int counter = 0;
        while (1)
        {
            auto guard = tree_.recmgr->getGuard(tid);
            tlx::dup_open<key_type, value_type>(tid, &tree_.root_);
            tlx::locking_res = true;
            auto removal_res = tree_.erase_one(tid, key);

            if (tlx::locking_res && tlx::dup_close<key_type, value_type>(tid, &tree_.root_))
            {
                for (auto& d : *tlx::duplications)
                {
                    if (d.first->is_leafnode()) {
                        tree_.recmgr->retire(tid, static_cast<tlx::leaf_node<key_type, value_type>*>(d.first));
                    }
                    else {
                        tree_.recmgr->retire(tid, static_cast<tlx::inner_node<key_type, value_type>*>(d.first));
                    }
                }

                if (removal_res)
                    return (sval_t)(&key);
                else
                    return NO_VALUE;
            }
            else
            {
                for (auto& d : *tlx::allocated)
                {
                    if (d.first->is_leafnode()) {
                        tree_.recmgr->deallocate(tid, static_cast<tlx::leaf_node<key_type, value_type>*>(d.first));
                    }
                    else {
                        tree_.recmgr->deallocate(tid, static_cast<tlx::inner_node<key_type, value_type>*>(d.first));
                    }
                }
            }
        }
    }

    //! \}
};