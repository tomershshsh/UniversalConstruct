/*******************************************************************************
 * tlx/container/btree.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2008-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/
#ifndef TLX_CONTAINER_BTREE_HEADER
#define TLX_CONTAINER_BTREE_HEADER

#include "btree_node.hpp"

// *** Required Headers from the STL

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <istream>
#include <memory>
#include <ostream>
#include <utility>

namespace tlx {

//! \addtogroup tlx_container
//! \{
//! \defgroup tlx_container_btree B+ Trees
//! B+ tree variants
//! \{

thread_local bool locking_res = true;

/*!
 * Basic class implementing a B+ tree data structure in memory.
 *
 * The base implementation of an in-memory B+ tree. It is based on the
 * implementation in Cormen's Introduction into Algorithms, Jan Jannink's paper
 * and other algorithm resources. Almost all STL-required function calls are
 * implemented. The asymptotic time requirements of the STL are not always
 * fulfilled in theory, however, in practice this B+ tree performs better than a
 * red-black tree and almost always uses less memory. The insertion function
 * splits the nodes on the recursion unroll. Erase is largely based on Jannink's
 * ideas.
 *
 * This class is specialized into btree_set, btree_multiset, btree_map and
 * btree_multimap using default template parameters and facade functions.
 */
template <typename Key, typename Value,
          typename KeyOfValue,
          typename RecMgr,
          typename Compare = std::less<Key>,
          typename Traits = btree_default_traits<Key, Value>,
          bool Duplicates = false,
          typename Allocator = std::allocator<Value> >
class BTree
{
public:
    //! \name Template Parameter Types
    //! \{

    //! First template parameter: The key type of the B+ tree. This is stored in
    //! inner nodes.
    typedef Key key_type;

    //! Second template parameter: Composition pair of key and data types, or
    //! just the key for set containers. This data type is stored in the leaves.
    typedef Value value_type;

    //! Third template: key extractor class to pull key_type from value_type.
    typedef KeyOfValue key_of_value;

    //! Fourth template parameter: key_type comparison function object
    typedef Compare key_compare;

    //! Fifth template parameter: Traits object used to define more parameters
    //! of the B+ tree
    typedef Traits traits;

    //! Sixth template parameter: Allow duplicate keys in the B+ tree. Used to
    //! implement multiset and multimap.
    static const bool allow_duplicates = Duplicates;

    //! Seventh template parameter: STL allocator for tree nodes
    typedef Allocator allocator_type;

    //! \}

    // The macro TLX_BTREE_FRIENDS can be used by outside class to access the B+
    // tree internals. This was added for wxBTreeDemo to be able to draw the
    // tree.
    TLX_BTREE_FRIENDS;

public:
    //! \name Constructed Types
    //! \{

    //! Typedef of our own type
    typedef BTree<key_type, value_type, key_of_value, RecMgr, key_compare,
                  traits, allow_duplicates, allocator_type> Self;

    typedef inner_node<key_type, value_type> InnerNode;
    typedef leaf_node<key_type, value_type> LeafNode;

    //! Size type used to count keys
    typedef size_t size_type;

    //! \}

public:
    //! \name Static Constant Options and Values of the B+ Tree
    //! \{

    //! Base B+ tree parameter: The number of key/data slots in each leaf
    static const unsigned short leaf_slotmax = traits::leaf_slots;

    //! Base B+ tree parameter: The number of key slots in each inner node,
    //! this can differ from slots in each leaf.
    static const unsigned short inner_slotmax = traits::inner_slots;

    //! Computed B+ tree parameter: The minimum number of key/data slots used
    //! in a leaf. If fewer slots are used, the leaf will be merged or slots
    //! shifted from it's siblings.
    static const unsigned short leaf_slotmin = (leaf_slotmax / 2);

    //! Computed B+ tree parameter: The minimum number of key slots used
    //! in an inner node. If fewer slots are used, the inner node will be
    //! merged or slots shifted from it's siblings.
    static const unsigned short inner_slotmin = (inner_slotmax / 2);

    //! Debug parameter: Enables expensive and thorough checking of the B+ tree
    //! invariants after each insert/erase operation.
    static const bool self_verify = traits::self_verify;

    //! Debug parameter: Prints out lots of debug information about how the
    //! algorithms change the tree. Requires the header file to be compiled
    //! with TLX_BTREE_DEBUG and the key type must be std::ostream printable.
    static const bool debug = traits::debug;

    //! \}

public:
    //! \name Iterators and Reverse Iterators
    //! \{

    class iterator;
    class const_iterator;
    class reverse_iterator;
    class const_reverse_iterator;

    //! STL-like iterator object for B+ tree items. The iterator points to a
    //! specific slot number in a leaf.
    class iterator
    {
    public:
        // *** Types

        //! The key type of the btree. Returned by key().
        typedef typename BTree::key_type key_type;

        //! The value type of the btree. Returned by operator*().
        typedef typename BTree::value_type value_type;

        //! Reference to the value_type. STL required.
        typedef value_type& reference;

        //! Pointer to the value_type. STL required.
        typedef value_type* pointer;

        //! STL-magic iterator category
        typedef std::bidirectional_iterator_tag iterator_category;

        //! STL-magic
        typedef ptrdiff_t difference_type;

        //! Our own type
        typedef iterator self;

    private:
        // *** Members

        //! The currently referenced leaf node of the tree
        typename BTree::LeafNode* curr_leaf;

        //! Current key/data slot referenced
        unsigned short curr_slot;

        //! Friendly to the const_iterator, so it may access the two data items
        //! directly.
        friend class const_iterator;

        //! Also friendly to the reverse_iterator, so it may access the two
        //! data items directly.
        friend class reverse_iterator;

        //! Also friendly to the const_reverse_iterator, so it may access the
        //! two data items directly.
        friend class const_reverse_iterator;

        //! Also friendly to the base btree class, because erase_iter() needs
        //! to read the curr_leaf and curr_slot values directly.
        friend class BTree<key_type, value_type, key_of_value, RecMgr, key_compare,
                           traits, allow_duplicates, allocator_type>;

        // The macro TLX_BTREE_FRIENDS can be used by outside class to access
        // the B+ tree internals. This was added for wxBTreeDemo to be able to
        // draw the tree.
        TLX_BTREE_FRIENDS;

    public:
        // *** Methods

        //! Default-Constructor of a mutable iterator
        iterator()
            : curr_leaf(nullptr), curr_slot(0)
        { }

        //! Initializing-Constructor of a mutable iterator
        iterator(typename BTree::LeafNode* l, unsigned short s)
            : curr_leaf(l), curr_slot(s)
        { }

        //! Copy-constructor from a reverse iterator
        iterator(const reverse_iterator& it) // NOLINT
            : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot)
        { }

        //! Dereference the iterator.
        reference operator * () const {
            auto bla = curr_leaf->get_slot(0);             
            return curr_leaf->get_slot(curr_slot);
        }

        //! Dereference the iterator.
        pointer operator -> () const {
            return &curr_leaf->get_slot(curr_slot);
        }

        //! Key of the current slot.
        const key_type& key() const {
            return curr_leaf->key(curr_slot);
        }

        //! Prefix++ advance the iterator to the next slot.
        iterator& operator ++ () {
            if (curr_slot + 1u < curr_leaf->get_slotuse()) {
                ++curr_slot;
            }
            else if (curr_leaf->next_leaf != nullptr) {
                curr_leaf = curr_leaf->next_leaf;
                curr_slot = 0;
            }
            else {
                // this is end()
                curr_slot = curr_leaf->get_slotuse();
            }

            return *this;
        }

        //! Postfix++ advance the iterator to the next slot.
        iterator operator ++ (int) {
            iterator tmp = *this;   // copy ourselves

            if (curr_slot + 1u < curr_leaf->get_slotuse()) {
                ++curr_slot;
            }
            else if (curr_leaf->next_leaf != nullptr) {
                curr_leaf = curr_leaf->next_leaf;
                curr_slot = 0;
            }
            else {
                // this is end()
                curr_slot = curr_leaf->get_slotuse();
            }

            return tmp;
        }

        //! Prefix-- backstep the iterator to the last slot.
        iterator& operator -- () {
            if (curr_slot > 0) {
                --curr_slot;
            }
            else if (curr_leaf->prev_leaf != nullptr) {
                curr_leaf = curr_leaf->prev_leaf;
                curr_slot = curr_leaf->get_slotuse() - 1;
            }
            else {
                // this is begin()
                curr_slot = 0;
            }

            return *this;
        }

        //! Postfix-- backstep the iterator to the last slot.
        iterator operator -- (int) {
            iterator tmp = *this;   // copy ourselves

            if (curr_slot > 0) {
                --curr_slot;
            }
            else if (curr_leaf->prev_leaf != nullptr) {
                curr_leaf = curr_leaf->prev_leaf;
                curr_slot = curr_leaf->get_slotuse() - 1;
            }
            else {
                // this is begin()
                curr_slot = 0;
            }

            return tmp;
        }

        //! Equality of iterators.
        bool operator == (const iterator& x) const {
            return (x.curr_leaf == curr_leaf) && (x.curr_slot == curr_slot);
        }

        //! Inequality of iterators.
        bool operator != (const iterator& x) const {
            return (x.curr_leaf != curr_leaf) || (x.curr_slot != curr_slot);
        }
    };

    //! STL-like read-only iterator object for B+ tree items. The iterator
    //! points to a specific slot number in a leaf.
    class const_iterator
    {
    public:
        // *** Types

        //! The key type of the btree. Returned by key().
        typedef typename BTree::key_type key_type;

        //! The value type of the btree. Returned by operator*().
        typedef typename BTree::value_type value_type;

        //! Reference to the value_type. STL required.
        typedef const value_type& reference;

        //! Pointer to the value_type. STL required.
        typedef const value_type* pointer;

        //! STL-magic iterator category
        typedef std::bidirectional_iterator_tag iterator_category;

        //! STL-magic
        typedef ptrdiff_t difference_type;

        //! Our own type
        typedef const_iterator self;

    private:
        // *** Members

        //! The currently referenced leaf node of the tree
        const typename BTree::LeafNode* curr_leaf;

        //! Current key/data slot referenced
        unsigned short curr_slot;

        //! Friendly to the reverse_const_iterator, so it may access the two
        //! data items directly
        friend class const_reverse_iterator;

        // The macro TLX_BTREE_FRIENDS can be used by outside class to access
        // the B+ tree internals. This was added for wxBTreeDemo to be able to
        // draw the tree.
        TLX_BTREE_FRIENDS;

    public:
        // *** Methods

        //! Default-Constructor of a const iterator
        const_iterator()
            : curr_leaf(nullptr), curr_slot(0)
        { }

        //! Initializing-Constructor of a const iterator
        const_iterator(const typename BTree::LeafNode* l, unsigned short s)
            : curr_leaf(l), curr_slot(s)
        { }

        //! Copy-constructor from a mutable iterator
        const_iterator(const iterator& it) // NOLINT
            : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot)
        { }

        //! Copy-constructor from a mutable reverse iterator
        const_iterator(const reverse_iterator& it) // NOLINT
            : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot)
        { }

        //! Copy-constructor from a const reverse iterator
        const_iterator(const const_reverse_iterator& it) // NOLINT
            : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot)
        { }

        //! Dereference the iterator.
        reference operator * () const {
            return curr_leaf->get_slot(curr_slot);
        }

        //! Dereference the iterator.
        pointer operator -> () const {
            return &curr_leaf->get_slot(curr_slot);
        }

        //! Key of the current slot.
        const key_type& key() const {
            return curr_leaf->key(curr_slot);
        }

        //! Prefix++ advance the iterator to the next slot.
        const_iterator& operator ++ () {
            if (curr_slot + 1u < curr_leaf->get_slotuse()) {
                ++curr_slot;
            }
            else if (curr_leaf->next_leaf != nullptr) {
                curr_leaf = curr_leaf->next_leaf;
                curr_slot = 0;
            }
            else {
                // this is end()
                curr_slot = curr_leaf->get_slotuse();
            }

            return *this;
        }

        //! Postfix++ advance the iterator to the next slot.
        const_iterator operator ++ (int) {
            const_iterator tmp = *this;   // copy ourselves

            if (curr_slot + 1u < curr_leaf->get_slotuse()) {
                ++curr_slot;
            }
            else if (curr_leaf->next_leaf != nullptr) {
                curr_leaf = curr_leaf->next_leaf;
                curr_slot = 0;
            }
            else {
                // this is end()
                curr_slot = curr_leaf->get_slotuse();
            }

            return tmp;
        }

        //! Prefix-- backstep the iterator to the last slot.
        const_iterator& operator -- () {
            if (curr_slot > 0) {
                --curr_slot;
            }
            else if (curr_leaf->prev_leaf != nullptr) {
                curr_leaf = curr_leaf->prev_leaf;
                curr_slot = curr_leaf->get_slotuse() - 1;
            }
            else {
                // this is begin()
                curr_slot = 0;
            }

            return *this;
        }

        //! Postfix-- backstep the iterator to the last slot.
        const_iterator operator -- (int) {
            const_iterator tmp = *this;   // copy ourselves

            if (curr_slot > 0) {
                --curr_slot;
            }
            else if (curr_leaf->prev_leaf != nullptr) {
                curr_leaf = curr_leaf->prev_leaf;
                curr_slot = curr_leaf->get_slotuse() - 1;
            }
            else {
                // this is begin()
                curr_slot = 0;
            }

            return tmp;
        }

        //! Equality of iterators.
        bool operator == (const const_iterator& x) const {
            return (x.curr_leaf == curr_leaf) && (x.curr_slot == curr_slot);
        }

        //! Inequality of iterators.
        bool operator != (const const_iterator& x) const {
            return (x.curr_leaf != curr_leaf) || (x.curr_slot != curr_slot);
        }
    };

    //! STL-like mutable reverse iterator object for B+ tree items. The
    //! iterator points to a specific slot number in a leaf.
    class reverse_iterator
    {
    public:
        // *** Types

        //! The key type of the btree. Returned by key().
        typedef typename BTree::key_type key_type;

        //! The value type of the btree. Returned by operator*().
        typedef typename BTree::value_type value_type;

        //! Reference to the value_type. STL required.
        typedef value_type& reference;

        //! Pointer to the value_type. STL required.
        typedef value_type* pointer;

        //! STL-magic iterator category
        typedef std::bidirectional_iterator_tag iterator_category;

        //! STL-magic
        typedef ptrdiff_t difference_type;

        //! Our own type
        typedef reverse_iterator self;

    private:
        // *** Members

        //! The currently referenced leaf node of the tree
        typename BTree::LeafNode* curr_leaf;

        //! One slot past the current key/data slot referenced.
        unsigned short curr_slot;

        //! Friendly to the const_iterator, so it may access the two data items
        //! directly
        friend class iterator;

        //! Also friendly to the const_iterator, so it may access the two data
        //! items directly
        friend class const_iterator;

        //! Also friendly to the const_iterator, so it may access the two data
        //! items directly
        friend class const_reverse_iterator;

        // The macro TLX_BTREE_FRIENDS can be used by outside class to access
        // the B+ tree internals. This was added for wxBTreeDemo to be able to
        // draw the tree.
        TLX_BTREE_FRIENDS;

    public:
        // *** Methods

        //! Default-Constructor of a reverse iterator
        reverse_iterator()
            : curr_leaf(nullptr), curr_slot(0)
        { }

        //! Initializing-Constructor of a mutable reverse iterator
        reverse_iterator(typename BTree::LeafNode* l, unsigned short s)
            : curr_leaf(l), curr_slot(s)
        { }

        //! Copy-constructor from a mutable iterator
        reverse_iterator(const iterator& it) // NOLINT
            : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot)
        { }

        //! Dereference the iterator.
        reference operator * () const {
            TLX_BTREE_ASSERT(curr_slot > 0);
            return curr_leaf->get_slot(curr_slot - 1);
        }

        //! Dereference the iterator.
        pointer operator -> () const {
            TLX_BTREE_ASSERT(curr_slot > 0);
            return &curr_leaf->get_slot(curr_slot - 1);
        }

        //! Key of the current slot.
        const key_type& key() const {
            TLX_BTREE_ASSERT(curr_slot > 0);
            return curr_leaf->key(curr_slot - 1);
        }

        //! Prefix++ advance the iterator to the next slot.
        reverse_iterator& operator ++ () {
            if (curr_slot > 1) {
                --curr_slot;
            }
            else if (curr_leaf->prev_leaf != nullptr) {
                curr_leaf = curr_leaf->prev_leaf;
                curr_slot = curr_leaf->get_slotuse();
            }
            else {
                // this is begin() == rend()
                curr_slot = 0;
            }

            return *this;
        }

        //! Postfix++ advance the iterator to the next slot.
        reverse_iterator operator ++ (int) {
            reverse_iterator tmp = *this;   // copy ourselves

            if (curr_slot > 1) {
                --curr_slot;
            }
            else if (curr_leaf->prev_leaf != nullptr) {
                curr_leaf = curr_leaf->prev_leaf;
                curr_slot = curr_leaf->get_slotuse();
            }
            else {
                // this is begin() == rend()
                curr_slot = 0;
            }

            return tmp;
        }

        //! Prefix-- backstep the iterator to the last slot.
        reverse_iterator& operator -- () {
            if (curr_slot < curr_leaf->get_slotuse()) {
                ++curr_slot;
            }
            else if (curr_leaf->next_leaf != nullptr) {
                curr_leaf = curr_leaf->next_leaf;
                curr_slot = 1;
            }
            else {
                // this is end() == rbegin()
                curr_slot = curr_leaf->get_slotuse();
            }

            return *this;
        }

        //! Postfix-- backstep the iterator to the last slot.
        reverse_iterator operator -- (int) {
            reverse_iterator tmp = *this;   // copy ourselves

            if (curr_slot < curr_leaf->get_slotuse()) {
                ++curr_slot;
            }
            else if (curr_leaf->next_leaf != nullptr) {
                curr_leaf = curr_leaf->next_leaf;
                curr_slot = 1;
            }
            else {
                // this is end() == rbegin()
                curr_slot = curr_leaf->get_slotuse();
            }

            return tmp;
        }

        //! Equality of iterators.
        bool operator == (const reverse_iterator& x) const {
            return (x.curr_leaf == curr_leaf) && (x.curr_slot == curr_slot);
        }

        //! Inequality of iterators.
        bool operator != (const reverse_iterator& x) const {
            return (x.curr_leaf != curr_leaf) || (x.curr_slot != curr_slot);
        }
    };

    //! STL-like read-only reverse iterator object for B+ tree items. The
    //! iterator points to a specific slot number in a leaf.
    class const_reverse_iterator
    {
    public:
        // *** Types

        //! The key type of the btree. Returned by key().
        typedef typename BTree::key_type key_type;

        //! The value type of the btree. Returned by operator*().
        typedef typename BTree::value_type value_type;

        //! Reference to the value_type. STL required.
        typedef const value_type& reference;

        //! Pointer to the value_type. STL required.
        typedef const value_type* pointer;

        //! STL-magic iterator category
        typedef std::bidirectional_iterator_tag iterator_category;

        //! STL-magic
        typedef ptrdiff_t difference_type;

        //! Our own type
        typedef const_reverse_iterator self;

    private:
        // *** Members

        //! The currently referenced leaf node of the tree
        const typename BTree::LeafNode* curr_leaf;

        //! One slot past the current key/data slot referenced.
        unsigned short curr_slot;

        //! Friendly to the const_iterator, so it may access the two data items
        //! directly.
        friend class reverse_iterator;

        // The macro TLX_BTREE_FRIENDS can be used by outside class to access
        // the B+ tree internals. This was added for wxBTreeDemo to be able to
        // draw the tree.
        TLX_BTREE_FRIENDS;

    public:
        // *** Methods

        //! Default-Constructor of a const reverse iterator.
        const_reverse_iterator()
            : curr_leaf(nullptr), curr_slot(0)
        { }

        //! Initializing-Constructor of a const reverse iterator.
        const_reverse_iterator(
            const typename BTree::LeafNode* l, unsigned short s)
            : curr_leaf(l), curr_slot(s)
        { }

        //! Copy-constructor from a mutable iterator.
        const_reverse_iterator(const iterator& it) // NOLINT
            : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot)
        { }

        //! Copy-constructor from a const iterator.
        const_reverse_iterator(const const_iterator& it) // NOLINT
            : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot)
        { }

        //! Copy-constructor from a mutable reverse iterator.
        const_reverse_iterator(const reverse_iterator& it) // NOLINT
            : curr_leaf(it.curr_leaf), curr_slot(it.curr_slot)
        { }

        //! Dereference the iterator.
        reference operator * () const {
            TLX_BTREE_ASSERT(curr_slot > 0);
            return curr_leaf->get_slot(curr_slot - 1);
        }

        //! Dereference the iterator.
        pointer operator -> () const {
            TLX_BTREE_ASSERT(curr_slot > 0);
            return &curr_leaf->get_slot(curr_slot - 1);
        }

        //! Key of the current slot.
        const key_type& key() const {
            TLX_BTREE_ASSERT(curr_slot > 0);
            return curr_leaf->key(curr_slot - 1);
        }

        //! Prefix++ advance the iterator to the previous slot.
        const_reverse_iterator& operator ++ () {
            if (curr_slot > 1) {
                --curr_slot;
            }
            else if (curr_leaf->prev_leaf != nullptr) {
                curr_leaf = curr_leaf->prev_leaf;
                curr_slot = curr_leaf->get_slotuse();
            }
            else {
                // this is begin() == rend()
                curr_slot = 0;
            }

            return *this;
        }

        //! Postfix++ advance the iterator to the previous slot.
        const_reverse_iterator operator ++ (int) {
            const_reverse_iterator tmp = *this;   // copy ourselves

            if (curr_slot > 1) {
                --curr_slot;
            }
            else if (curr_leaf->prev_leaf != nullptr) {
                curr_leaf = curr_leaf->prev_leaf;
                curr_slot = curr_leaf->get_slotuse();
            }
            else {
                // this is begin() == rend()
                curr_slot = 0;
            }

            return tmp;
        }

        //! Prefix-- backstep the iterator to the next slot.
        const_reverse_iterator& operator -- () {
            if (curr_slot < curr_leaf->get_slotuse()) {
                ++curr_slot;
            }
            else if (curr_leaf->next_leaf != nullptr) {
                curr_leaf = curr_leaf->next_leaf;
                curr_slot = 1;
            }
            else {
                // this is end() == rbegin()
                curr_slot = curr_leaf->get_slotuse();
            }

            return *this;
        }

        //! Postfix-- backstep the iterator to the next slot.
        const_reverse_iterator operator -- (int) {
            const_reverse_iterator tmp = *this;   // copy ourselves

            if (curr_slot < curr_leaf->get_slotuse()) {
                ++curr_slot;
            }
            else if (curr_leaf->next_leaf != nullptr) {
                curr_leaf = curr_leaf->next_leaf;
                curr_slot = 1;
            }
            else {
                // this is end() == rbegin()
                curr_slot = curr_leaf->get_slotuse();
            }

            return tmp;
        }

        //! Equality of iterators.
        bool operator == (const const_reverse_iterator& x) const {
            return (x.curr_leaf == curr_leaf) && (x.curr_slot == curr_slot);
        }

        //! Inequality of iterators.
        bool operator != (const const_reverse_iterator& x) const {
            return (x.curr_leaf != curr_leaf) || (x.curr_slot != curr_slot);
        }
    };

    //! \}

public:
    //! \name Small Statistics Structure
    //! \{

    /*!
     * A small struct containing basic statistics about the B+ tree. It can be
     * fetched using get_stats().
     */
    struct tree_stats {
        //! Number of items in the B+ tree
        size_type size;

        //! Number of leaves in the B+ tree
        size_type leaves;

        //! Number of inner nodes in the B+ tree
        size_type inner_nodes;

        //! Base B+ tree parameter: The number of key/data slots in each leaf
        static const unsigned short leaf_slots = Self::leaf_slotmax;

        //! Base B+ tree parameter: The number of key slots in each inner node.
        static const unsigned short inner_slots = Self::inner_slotmax;

        //! Zero initialized
        tree_stats()
            : size(0),
              leaves(0), inner_nodes(0)
        { }

        //! Return the total number of nodes
        size_type nodes() const {
            return inner_nodes + leaves;
        }

        //! Return the average fill of leaves
        double avgfill_leaves() const {
            return static_cast<double>(size) / (leaves * leaf_slots);
        }
    };

    //! \}

public:
    //! \name Tree Object Data Members
    //! \{

    //! Pointer to the B+ tree's root node, either leaf or inner node.
    node* root_;

    RecMgr * recmgr;

    //! Pointer to first leaf in the double linked leaf chain.
    LeafNode* head_leaf_;

    //! Pointer to last leaf in the double linked leaf chain.
    LeafNode* tail_leaf_;

    //! Other small statistics about the B+ tree.
    tree_stats stats_;

    //! Key comparison object. More comparison functions are generated from
    //! this < relation.
    key_compare key_less_;

    //! Memory allocator.
    allocator_type allocator_;

    //! \}

public:
    //! \name Constructors and Destructor
    //! \{

    //! Default constructor initializing an empty B+ tree with the standard key
    //! comparison function.
    explicit BTree(const int _NUM_THREADS, const allocator_type& alloc = allocator_type())
        : root_(nullptr), head_leaf_(nullptr), tail_leaf_(nullptr),
          allocator_(alloc),
          recmgr(new RecMgr(_NUM_THREADS))
    { }

    //! Constructor initializing an empty B+ tree with a special key
    //! comparison object.
    explicit BTree(const key_compare& kcf,
                   const allocator_type& alloc = allocator_type())
        : root_(nullptr), head_leaf_(nullptr), tail_leaf_(nullptr),
          key_less_(kcf), allocator_(alloc)
    { }

    //! Constructor initializing a B+ tree with the range [first,last). The
    //! range need not be sorted. To create a B+ tree from a sorted range, use
    //! bulk_load().
    template <class InputIterator>
    BTree(InputIterator first, InputIterator last,
          const allocator_type& alloc = allocator_type())
        : root_(nullptr), head_leaf_(nullptr), tail_leaf_(nullptr),
          allocator_(alloc) {
        insert(0, first, last);
    }

    //! Constructor initializing a B+ tree with the range [first,last) and a
    //! special key comparison object.  The range need not be sorted. To create
    //! a B+ tree from a sorted range, use bulk_load().
    template <class InputIterator>
    BTree(InputIterator first, InputIterator last, const key_compare& kcf,
          const allocator_type& alloc = allocator_type())
        : root_(nullptr), head_leaf_(nullptr), tail_leaf_(nullptr),
          key_less_(kcf), allocator_(alloc) {
        insert(0, first, last);
    }

    //! Frees up all used B+ tree memory pages
    ~BTree() {
        clear(0);
    }

    //! Fast swapping of two identical B+ tree objects.
    void swap(BTree& from) {
        std::swap(root_, from.root_);
        std::swap(head_leaf_, from.head_leaf_);
        std::swap(tail_leaf_, from.tail_leaf_);
        std::swap(stats_, from.stats_);
        std::swap(key_less_, from.key_less_);
        std::swap(allocator_, from.allocator_);
    }

    //! \}

public:
    //! \name Key and Value Comparison Function Objects
    //! \{

    //! Function class to compare value_type objects. Required by the STL
    class value_compare
    {
    protected:
        //! Key comparison function from the template parameter
        key_compare key_comp;

        //! Constructor called from BTree::value_comp()
        explicit value_compare(key_compare kc)
            : key_comp(kc)
        { }

        //! Friendly to the btree class so it may call the constructor
        friend class BTree<key_type, value_type, key_of_value, RecMgr, key_compare,
                           traits, allow_duplicates, allocator_type>;

    public:
        //! Function call "less"-operator resulting in true if x < y.
        bool operator () (const value_type& x, const value_type& y) const {
            return key_comp(x.first, y.first);
        }
    };

    //! Constant access to the key comparison object sorting the B+ tree.
    key_compare key_comp() const {
        return key_less_;
    }

    //! Constant access to a constructed value_type comparison object. Required
    //! by the STL.
    value_compare value_comp() const {
        return value_compare(key_less_);
    }

    //! \}

private:
    //! \name Convenient Key Comparison Functions Generated From key_less
    //! \{

    //! True if a < b ? "constructed" from key_less_()
    bool key_less(const key_type& a, const key_type& b) const {
        return key_less_(a, b);
    }

    //! True if a <= b ? constructed from key_less()
    bool key_lessequal(const key_type& a, const key_type& b) const {
        return !key_less_(b, a);
    }

    //! True if a > b ? constructed from key_less()
    bool key_greater(const key_type& a, const key_type& b) const {
        return key_less_(b, a);
    }

    //! True if a >= b ? constructed from key_less()
    bool key_greaterequal(const key_type& a, const key_type& b) const {
        return !key_less_(a, b);
    }

    //! True if a == b ? constructed from key_less(). This requires the <
    //! relation to be a total order, otherwise the B+ tree cannot be sorted.
    bool key_equal(const key_type& a, const key_type& b) const {
        return !key_less_(a, b) && !key_less_(b, a);
    }

    //! \}

public:
    //! \name Allocators
    //! \{

    //! Return the base node allocator provided during construction.
    allocator_type get_allocator() const {
        return allocator_;
    }

    //! \}

private:
    //! \name Node Object Allocation and Deallocation Functions
    //! \{

    //! Allocate and initialize a leaf node
    LeafNode * allocate_leaf(const int& tid) {
        LeafNode* n = (LeafNode*)recmgr->template allocate<LeafNode>(tid);
        n->initialize();
        stats_.leaves++;
        return n;
    }

    LeafNode * allocate_leaf(const int& tid, LeafNode * other) {
        LeafNode* n = (LeafNode*)recmgr->template allocate<LeafNode>(tid);
        std::memcpy((void *)n, (void *)other, sizeof(LeafNode));
        pthread_spin_init(&n->dup_lock, PTHREAD_PROCESS_PRIVATE);
        return n;
    }

    //! Allocate and initialize an inner node
    InnerNode * allocate_inner(const int& tid, unsigned short level) {
        InnerNode* n = (InnerNode*)recmgr->template allocate<InnerNode>(tid);
        n->initialize(level);
        stats_.inner_nodes++;
        return n;
    }

    InnerNode * allocate_inner(const int& tid, InnerNode * other) {
        InnerNode* n = (InnerNode*)recmgr->template allocate<InnerNode>(tid);
        std::memcpy((void *)n, (void *)other, sizeof(InnerNode));
        pthread_spin_init(&n->dup_lock, PTHREAD_PROCESS_PRIVATE);
        return n;
    }

    //! Correctly free either inner or leaf node, destructs all contained key
    //! and value objects.
    void free_node(const int& tid, node* n) {
        // if (n->is_leafnode()) {
        //     LeafNode* ln = static_cast<LeafNode*>(n);
        //     if (duplications->find(ln) != duplications->end())
        //     {
        //         auto found = duplications->at(ln);
        //         recmgr->deallocate(tid, static_cast<LeafNode*>(found.dup));
        //     }
        //     else
        //     {
        //         recmgr->retire(tid, ln);
        //     }
            
        //     stats_.leaves--;
        // }
        // else {
        //     InnerNode* in = static_cast<InnerNode*>(n);
        //     if (duplications->find(in) != duplications->end())
        //     {
        //         auto found = duplications->at(in);
        //         recmgr->deallocate(tid, static_cast<InnerNode*>(found.dup));
        //     }
        //     else
        //     {
        //         recmgr->retire(tid, in);
        //     }

        //     stats_.inner_nodes--;
        // }
    }

    //! \}

public:
    //! \name Duplication functions
    //! 
    //! \{

    node * dup_prologue(const int& tid, node * orig) 
    {
        if (locking_res == false)
        {
            dup_unlock_duplications<Key, Value>(tid, true);
            return nullptr;
        }

        if (allocated->find(orig) != allocated->end())
        {
            return orig;
        }

        if (duplications->find(orig) != duplications->end())
        {
            auto found = duplications->at(orig);
            return found.dup;
        }

        /* lock orig's parent */
        node * parent = nullptr;
        if (node_parent_map->find(orig) != node_parent_map->end())
        {
            auto found = node_parent_map->at(orig);
            // parent = found.first;
            parent = found.parent;
        }

        if (parent != nullptr && locked->find(parent) == locked->end())
        {
            if (!pthread_spin_trylock(&parent->dup_lock))
            {
                locked->insert(std::make_pair(parent, true));
            }
            else
            {
                dup_unlock_duplications<Key, Value>(tid, true);
                locking_res = false;
                return nullptr;
            }
        }

        /* lock orig */
        if (orig != nullptr && locked->find(orig) == locked->end()) 
        {
            if (!pthread_spin_trylock(&orig->dup_lock))
            {
                locked->insert(std::make_pair(orig, false));
            }
            else
            {
                dup_unlock_duplications<Key, Value>(tid, true);
                locking_res = false;
                return nullptr;
            }
        }
        else if (orig != nullptr)
        {
            (*locked)[orig] = false;
        }        

        if (orig->is_leafnode())
            return (node *)allocate_leaf(tid, static_cast<LeafNode*>(orig));
        else
            return (node *)allocate_inner(tid, static_cast<InnerNode*>(orig));
    }

    node * dup_epilogue(const int& tid, node * orig, node * dup)
    {
        node * parent = nullptr;
        unsigned int child_idx = MAX_UINT;	
        bool do_insert = false;

        if (orig != dup && duplications->find(orig) == duplications->end())
        {
            /* find duplication's parent */
            if (orig != orig_root)
            {
                if (node_parent_map->find(orig) != node_parent_map->end())
                {
                    auto found = node_parent_map->at(orig);
                    // parent = found.first;
                    // child_idx = found.second;
                    parent = found.parent;
                    child_idx = found.index;
                }
            }
            else
            {
                new_root = dup;
            }

            do_insert = true;
        }

        /* update if there is another duplication in the neighborhood */
        for (auto& d : *duplications)
        {
            if (d.first == parent && d.second.dup != nullptr)
            {
                InnerNode * i_dup = static_cast<InnerNode*>(d.second.dup);
                for (unsigned int idx = 0; idx <= i_dup->slotuse; idx++)
                {
                    if (i_dup->childid[idx] == orig)
                    {
                        i_dup->childid[idx] = dup;
                    }
                }
                continue;
            }

            if (dup != nullptr && !dup->is_leafnode())
            {
                InnerNode * i_dup = static_cast<InnerNode*>(dup);
                for (unsigned int idx = 0; idx <= i_dup->slotuse; idx++)
                {                  
                    if (i_dup->childid[idx] == d.first)
                    {
                        i_dup->childid[idx] = d.second.dup;
                    }
                }
            }
        }

        if (do_insert)
        {
            duplications->insert({orig, {dup, parent, child_idx}});
        }

        dup_happened = true;
        return dup;
    }

    node * dup_paths_to_lca_helper(const int& tid, node * first, node * second)
    {
        auto current_1 = node_parent_map->at(second);
        auto current_2 = node_parent_map->at(first);
        
        while (current_1.height > current_2.height)
        {
            auto temp = dup_prologue(tid, current_1.self);
            dup_epilogue(tid, current_1.self, temp);
            current_1 = node_parent_map->at(current_1.parent);
        }
        
        while (current_1.height < current_2.height)
        {
            auto temp = dup_prologue(tid, current_2.self);
            dup_epilogue(tid, current_2.self, temp);
            current_2 = node_parent_map->at(current_2.parent);
        }
        
        while (current_1.self != current_2.self)
        {
            auto temp_1 = dup_prologue(tid, current_1.self);
            dup_epilogue(tid, current_1.self, temp_1);

            auto temp_2 = dup_prologue(tid, current_1.self);
            dup_epilogue(tid, current_1.self, temp_2);

            current_1 = node_parent_map->at(current_1.parent);
            current_2 = node_parent_map->at(current_2.parent);
        }
        
        return current_1.self;
    }

    void dup_paths_to_lca(const int& tid)
    {
        if (duplications->size() == 0)
            return;

        auto first = duplications->begin()->first;
        
        for (auto it = ++(duplications->begin()); it != duplications->end(); ++it)
        {
            first = dup_paths_to_lca_helper(tid, first, it->first);
        }
    }

    //! \}

public:
    //! \name Fast Destruction of the B+ Tree
    //! \{

    //! Frees all key/data pairs and all nodes of the tree.
    void clear(const int& tid) {
        if (root_)
        {
            clear_recursive(tid, root_);
            free_node(tid, root_);

            root_ = nullptr;
            head_leaf_ = tail_leaf_ = nullptr;

            stats_ = tree_stats();
        }

        TLX_BTREE_ASSERT(stats_.size == 0);
    }

private:
    //! Recursively free up nodes.
    void clear_recursive(const int& tid, node* n) {
        if (n->is_leafnode())
        {
            LeafNode* leafnode = static_cast<LeafNode*>(n);

            for (unsigned short slot = 0; slot < leafnode->get_slotuse(); ++slot)
            {
                // data objects are deleted by LeafNode's destructor
            }
        }
        else
        {
            InnerNode* innernode = static_cast<InnerNode*>(n);

            for (unsigned short slot = 0; slot < innernode->get_slotuse() + 1; ++slot)
            {
                clear_recursive(tid, innernode->get_child(slot));
                free_node(tid, innernode->get_child(slot));
            }
        }
    }

    //! \}

public:
    //! \name STL Iterator Construction Functions
    //! \{

    //! Constructs a read/data-write iterator that points to the first slot in
    //! the first leaf of the B+ tree.
    iterator begin() {
        return iterator(head_leaf_, 0);
    }

    //! Constructs a read/data-write iterator that points to the first invalid
    //! slot in the last leaf of the B+ tree.
    iterator end() {
        return iterator(tail_leaf_, tail_leaf_ ? tail_leaf_->get_slotuse() : 0);
    }

    //! Constructs a read-only constant iterator that points to the first slot
    //! in the first leaf of the B+ tree.
    const_iterator begin() const {
        return const_iterator(head_leaf_, 0);
    }

    //! Constructs a read-only constant iterator that points to the first
    //! invalid slot in the last leaf of the B+ tree.
    const_iterator end() const {
        return const_iterator(tail_leaf_, tail_leaf_ ? tail_leaf_->get_slotuse() : 0);
    }

    //! Constructs a read/data-write reverse iterator that points to the first
    //! invalid slot in the last leaf of the B+ tree. Uses STL magic.
    reverse_iterator rbegin() {
        return reverse_iterator(end());
    }

    //! Constructs a read/data-write reverse iterator that points to the first
    //! slot in the first leaf of the B+ tree. Uses STL magic.
    reverse_iterator rend() {
        return reverse_iterator(begin());
    }

    //! Constructs a read-only reverse iterator that points to the first
    //! invalid slot in the last leaf of the B+ tree. Uses STL magic.
    const_reverse_iterator rbegin() const {
        return const_reverse_iterator(end());
    }

    //! Constructs a read-only reverse iterator that points to the first slot
    //! in the first leaf of the B+ tree. Uses STL magic.
    const_reverse_iterator rend() const {
        return const_reverse_iterator(begin());
    }

    //! \}

private:
    //! \name B+ Tree Node Binary Search Functions
    //! \{

    //! Searches for the first key in the node n greater or equal to key. Uses
    //! binary search with an optional linear self-verification. This is a
    //! template function, because the slotkey array is located at different
    //! places in LeafNode and InnerNode.
    template <typename node_type>
    unsigned short find_lower(const node_type* n, const key_type& key) const {
        if (sizeof(*n) > traits::binsearch_threshold)
        {
            if (n->get_slotuse() == 0) return 0;

            unsigned short lo = 0, hi = n->get_slotuse();

            while (lo < hi)
            {
                unsigned short mid = (lo + hi) >> 1;

                if (key_lessequal(key, n->key(mid))) {
                    hi = mid; // key <= mid
                }
                else {
                    lo = mid + 1; // key > mid
                }
            }

            TLX_BTREE_PRINT("BTree::find_lower: on " << n <<
                            " key " << key << " -> " << lo << " / " << hi);

            // verify result using simple linear search
            if (self_verify)
            {
                unsigned short i = 0;
                while (i < n->get_slotuse() && key_less(n->key(i), key)) ++i;

                TLX_BTREE_PRINT("BTree::find_lower: testfind: " << i);
                TLX_BTREE_ASSERT(i == lo);
            }

            return lo;
        }
        else // for nodes <= binsearch_threshold do linear search.
        {
            unsigned short lo = 0;
            while (lo < n->get_slotuse() && key_less(n->key(lo), key)) ++lo;
            return lo;
        }
    }

    //! Searches for the first key in the node n greater than key. Uses binary
    //! search with an optional linear self-verification. This is a template
    //! function, because the slotkey array is located at different places in
    //! LeafNode and InnerNode.
    template <typename node_type>
    unsigned short find_upper(const node_type* n, const key_type& key) const {
        if (sizeof(*n) > traits::binsearch_threshold)
        {
            if (n->get_slotuse() == 0) return 0;

            unsigned short lo = 0, hi = n->get_slotuse();

            while (lo < hi)
            {
                unsigned short mid = (lo + hi) >> 1;

                if (key_less(key, n->key(mid))) {
                    hi = mid; // key < mid
                }
                else {
                    lo = mid + 1; // key >= mid
                }
            }

            TLX_BTREE_PRINT("BTree::find_upper: on " << n <<
                            " key " << key << " -> " << lo << " / " << hi);

            // verify result using simple linear search
            if (self_verify)
            {
                unsigned short i = 0;
                while (i < n->get_slotuse() && key_lessequal(n->key(i), key)) ++i;

                TLX_BTREE_PRINT("BTree::find_upper testfind: " << i);
                TLX_BTREE_ASSERT(i == hi);
            }

            return lo;
        }
        else // for nodes <= binsearch_threshold do linear search.
        {
            unsigned short lo = 0;
            while (lo < n->get_slotuse() && key_lessequal(n->key(lo), key)) ++lo;
            return lo;
        }
    }

    //! \}

public:
    //! \name Access Functions to the Item Count
    //! \{

    //! Return the number of key/data pairs in the B+ tree
    size_type size() const {
        return stats_.size;
    }

    //! Returns true if there is at least one key/data pair in the B+ tree
    bool empty() const {
        return (size() == size_type(0));
    }

    //! Returns the largest possible size of the B+ Tree. This is just a
    //! function required by the STL standard, the B+ Tree can hold more items.
    size_type max_size() const {
        return size_type(-1);
    }

    //! Return a const reference to the current statistics.
    const struct tree_stats& get_stats() const {
        return stats_;
    }

    //! \}

public:
    //! \name STL Access Functions Querying the Tree by Descending to a Leaf
    //! \{

    //! Non-STL function checking whether a key is in the B+ tree. The same as
    //! (find(k) != end()) or (count() != 0).
    bool exists(const key_type& key) const {
        const node* n = root_;
        if (!n) return false;

        while (!n->is_leafnode())
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->get_child(slot);
        }

        const LeafNode* leaf = static_cast<const LeafNode*>(n);

        unsigned short slot = find_lower(leaf, key);
        return (slot < leaf->get_slotuse() && key_equal(key, leaf->key(slot)));
    }

    //! Tries to locate a key in the B+ tree and returns an iterator to the
    //! key/data slot if found. If unsuccessful it returns end().
    iterator find(const key_type& key) {
        node* n = root_;
        if (!n) return end();

        while (!n->is_leafnode())
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->get_child(slot);
        }

        LeafNode* leaf = static_cast<LeafNode*>(n);

        unsigned short slot = find_lower(leaf, key);
        return (slot < leaf->get_slotuse() && key_equal(key, leaf->key(slot)))
               ? iterator(leaf, slot) : end();
    }

    //! Tries to locate a key in the B+ tree and returns an constant iterator to
    //! the key/data slot if found. If unsuccessful it returns end().
    const_iterator find(const key_type& key) const {
        const node* n = root_;
        if (!n) return end();

        while (!n->is_leafnode())
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->get_child(slot);
        }

        const LeafNode* leaf = static_cast<const LeafNode*>(n);

        unsigned short slot = find_lower(leaf, key);
        return (slot < leaf->get_slotuse() && key_equal(key, leaf->key(slot)))
               ? const_iterator(leaf, slot) : end();
    }

    //! Tries to locate a key in the B+ tree and returns the number of identical
    //! key entries found.
    size_type count(const key_type& key) const {
        const node* n = root_;
        if (!n) return 0;

        while (!n->is_leafnode())
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->get_child(slot);
        }

        const LeafNode* leaf = static_cast<const LeafNode*>(n);

        unsigned short slot = find_lower(leaf, key);
        size_type num = 0;

        while (leaf && slot < leaf->get_slotuse() && key_equal(key, leaf->key(slot)))
        {
            ++num;
            if (++slot >= leaf->get_slotuse())
            {
                leaf = leaf->next_leaf;
                slot = 0;
            }
        }

        return num;
    }

    //! Searches the B+ tree and returns an iterator to the first pair equal to
    //! or greater than key, or end() if all keys are smaller.
    iterator lower_bound(const key_type& key) {
        node* n = root_;
        if (!n) return end();

        while (!n->is_leafnode())
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->get_child(slot);
        }

        LeafNode* leaf = static_cast<LeafNode*>(n);

        unsigned short slot = find_lower(leaf, key);
        return iterator(leaf, slot);
    }

    //! Searches the B+ tree and returns a constant iterator to the first pair
    //! equal to or greater than key, or end() if all keys are smaller.
    const_iterator lower_bound(const key_type& key) const {
        const node* n = root_;
        if (!n) return end();

        while (!n->is_leafnode())
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            unsigned short slot = find_lower(inner, key);

            n = inner->get_child(slot);
        }

        const LeafNode* leaf = static_cast<const LeafNode*>(n);

        unsigned short slot = find_lower(leaf, key);
        return const_iterator(leaf, slot);
    }

    //! Searches the B+ tree and returns an iterator to the first pair greater
    //! than key, or end() if all keys are smaller or equal.
    iterator upper_bound(const key_type& key) {
        node* n = root_;
        if (!n) return end();

        while (!n->is_leafnode())
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            unsigned short slot = find_upper(inner, key);

            n = inner->get_child(slot);
        }

        LeafNode* leaf = static_cast<LeafNode*>(n);

        unsigned short slot = find_upper(leaf, key);
        return iterator(leaf, slot);
    }

    //! Searches the B+ tree and returns a constant iterator to the first pair
    //! greater than key, or end() if all keys are smaller or equal.
    const_iterator upper_bound(const key_type& key) const {
        const node* n = root_;
        if (!n) return end();

        while (!n->is_leafnode())
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            unsigned short slot = find_upper(inner, key);

            n = inner->get_child(slot);
        }

        const LeafNode* leaf = static_cast<const LeafNode*>(n);

        unsigned short slot = find_upper(leaf, key);
        return const_iterator(leaf, slot);
    }

    //! Searches the B+ tree and returns both lower_bound() and upper_bound().
    std::pair<iterator, iterator> equal_range(const key_type& key) {
        return std::pair<iterator, iterator>(
            lower_bound(key), upper_bound(key));
    }

    //! Searches the B+ tree and returns both lower_bound() and upper_bound().
    std::pair<const_iterator, const_iterator>
    equal_range(const key_type& key) const {
        return std::pair<const_iterator, const_iterator>(
            lower_bound(key), upper_bound(key));
    }

    //! \}

public:
    //! \name B+ Tree Object Comparison Functions
    //! \{

    //! Equality relation of B+ trees of the same type. B+ trees of the same
    //! size and equal elements (both key and data) are considered equal. Beware
    //! of the random ordering of duplicate keys.
    bool operator == (const BTree& other) const {
        return (size() == other.size()) &&
               std::equal(begin(), end(), other.begin());
    }

    //! Inequality relation. Based on operator==.
    bool operator != (const BTree& other) const {
        return !(*this == other);
    }

    //! Total ordering relation of B+ trees of the same type. It uses
    //! std::lexicographical_compare() for the actual comparison of elements.
    bool operator < (const BTree& other) const {
        return std::lexicographical_compare(
            begin(), end(), other.begin(), other.end());
    }

    //! Greater relation. Based on operator<.
    bool operator > (const BTree& other) const {
        return other < *this;
    }

    //! Less-equal relation. Based on operator<.
    bool operator <= (const BTree& other) const {
        return !(other < *this);
    }

    //! Greater-equal relation. Based on operator<.
    bool operator >= (const BTree& other) const {
        return !(*this < other);
    }

    //! \}

public:
    //! \name Fast Copy: Assign Operator and Copy Constructors
    //! \{

    //! Assignment operator. All the key/data pairs are copied.
    BTree& operator = (const BTree& other) {
        if (this != &other)
        {
            clear(0);

            key_less_ = other.key_comp();
            allocator_ = other.get_allocator();

            if (other.size() != 0)
            {
                stats_.leaves = stats_.inner_nodes = 0;
                if (other.root_) {
                    root_ = copy_recursive(0, other.root_); // <=====
                }
                stats_ = other.stats_;
            }

            if (self_verify) verify();
        }
        return *this;
    }

    //! Copy constructor. The newly initialized B+ tree object will contain a
    //! copy of all key/data pairs.
    BTree(const BTree& other)
        : root_(nullptr), head_leaf_(nullptr), tail_leaf_(nullptr),
          stats_(other.stats_),
          key_less_(other.key_comp()),
          allocator_(other.get_allocator()) {
        if (size() > 0)
        {
            stats_.leaves = stats_.inner_nodes = 0;
            if (other.root_) {
                root_ = copy_recursive(0, other.root_); // <=====
            }
            if (self_verify) verify();
        }
    }

private:
    //! Recursively copy nodes from another B+ tree object
    struct node * copy_recursive(const int& tid, const node* n) {
        if (n->is_leafnode())
        {
            const LeafNode* leaf = static_cast<const LeafNode*>(n);
            LeafNode* newleaf = allocate_leaf(tid);

            newleaf->set_slotuse(leaf->get_slotuse());
            newleaf->copy_to_slotdata(
                    leaf->get_slotdata_vec(), 
                    leaf->get_slotdata_vec() + leaf->get_slotuse(), 
                    newleaf->get_slotdata_vec());

            if (head_leaf_ == nullptr)
            {
                head_leaf_ = tail_leaf_ = newleaf;
                newleaf->prev_leaf = newleaf->next_leaf = nullptr;
            }
            else
            {
                newleaf->prev_leaf = tail_leaf_;
                tail_leaf_->next_leaf = newleaf;
                tail_leaf_ = newleaf;
            }

            return newleaf;
        }
        else
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            InnerNode* newinner = allocate_inner(tid, inner->get_level());

            newinner->set_slotuse(inner->get_slotuse());
            newinner->copy_to_slotkey(
                inner->get_slotkey_vec(), 
                inner->get_slotkey_vec() + inner->get_slotuse(), 
                newinner->get_slotkey_vec());

            for (unsigned short slot = 0; slot <= inner->get_slotuse(); ++slot)
            {
                newinner->set_child(slot, copy_recursive(inner->get_child(slot)));
            }

            return newinner;
        }
    }

    //! \}

public:
    //! \name Public Insertion Functions
    //! \{

    //! Attempt to insert a key/data pair into the B+ tree. If the tree does not
    //! allow duplicate keys, then the insert may fail if it is already present.
    std::pair<iterator, bool> insert(const int& tid, const value_type& x) {
        return insert_start(tid, key_of_value::get(x), x);
    }

    //! Attempt to insert a key/data pair into the B+ tree. The iterator hint is
    //! currently ignored by the B+ tree insertion routine.
    iterator insert(const int& tid, iterator /* hint */, const value_type& x) {
        return insert_start(tid, key_of_value::get(x), x).first;
    }

    //! Attempt to insert the range [first,last) of value_type pairs into the B+
    //! tree. Each key/data pair is inserted individually; to bulk load the
    //! tree, use a constructor with range.
    template <typename InputIterator>
    void insert(const int& tid, InputIterator first, InputIterator last) {
        InputIterator iter = first;
        while (iter != last)
        {
            insert(tid, *iter);
            ++iter;
        }
    }

    //! \}

private:
    //! \name Private Insertion Functions
    //! \{

    //! Start the insertion descent at the current root and handle root splits.
    //! Returns true if the item was inserted
    std::pair<iterator, bool>
    insert_start(const int& tid, const key_type& key, const value_type& value) {
        node* newchild = nullptr;
        key_type newkey = key_type();

        if (root_ == nullptr) {
            root_ = orig_root = head_leaf_ = tail_leaf_ = allocate_leaf(tid); //TODO
        }
        
        std::pair<iterator, bool> r =
            insert_descend(tid, orig_root, key, value, &newkey, &newchild);

        if (newchild)
        {
            // this only occurs if insert_descend() could not insert the key
            // into the root node, this mean the root is full and a new root
            // needs to be created.
            InnerNode* newroot = allocate_inner(tid, orig_root->get_level() + 1);

            auto newroot_dup = static_cast<InnerNode*>(dup_prologue(tid, newroot));
            if (newroot_dup != nullptr) {
                newroot_dup->set_slotkey(0, newkey);

                newroot_dup->set_child(0, new_root);
                newroot_dup->set_child(1, newchild);

                newroot_dup->set_slotuse(1);
                dup_epilogue(tid, newroot, newroot_dup);
            }

            /* TOMER CHANGE - TODO */
            auto root_dup = dup_prologue(tid, orig_root);
            if (root_dup != nullptr)
            {
                root_dup = newroot;
                dup_epilogue(tid, orig_root, root_dup);
            }

            new_root = root_dup;
            // root_ = newroot;
        }

        // increment size if the item was inserted
        if (r.second) ++stats_.size;

#ifdef TLX_BTREE_DEBUG
        if (debug) print(std::cout);
#endif

        if (self_verify) {
            verify();
            TLX_BTREE_ASSERT(exists(key));
        }
        return r;
    }

    /*!
     * Insert an item into the B+ tree.
     *
     * Descend down the nodes to a leaf, insert the key/data pair in a free
     * slot. If the node overflows, then it must be split and the new split node
     * inserted into the parent. Unroll / this splitting up to the root.
    */
    std::pair<iterator, bool> insert_descend(
        const int& tid, node* n, const key_type& key, const value_type& value,
        key_type* splitkey, node** splitnode) {
        
        if (!n->is_leafnode())
        {
            InnerNode* inner = static_cast<InnerNode*>(n);

            key_type newkey = key_type();
            node* newchild = nullptr;
            
            unsigned short slot = find_lower(inner, key);

            TLX_BTREE_PRINT(
                "BTree::insert_descend into " << inner->get_child(slot));

            std::pair<iterator, bool> r =
                insert_descend(tid, inner->get_child(slot),
                               key, value, &newkey, &newchild);

            if (newchild)
            {
                TLX_BTREE_PRINT("BTree::insert_descend newchild" <<
                                " with key " << newkey <<
                                " node " << newchild << " at slot " << slot);

                if (inner->is_full())
                {
                    split_inner_node(tid, inner, splitkey, splitnode, slot);

                    TLX_BTREE_PRINT("BTree::insert_descend done split_inner:" <<
                                    " putslot: " << slot <<
                                    " putkey: " << newkey <<
                                    " upkey: " << *splitkey);

#ifdef TLX_BTREE_DEBUG
                    if (debug)
                    {
                        print_node(std::cout, inner);
                        print_node(std::cout, *splitnode);
                    }
#endif

                    // check if insert slot is in the split sibling node
                    TLX_BTREE_PRINT("BTree::insert_descend switch: "
                                    << slot << " > " << inner->get_slotuse() + 1);

                    if (slot == inner->get_slotuse() + 1 &&
                        inner->get_slotuse() < (*splitnode)->get_slotuse())
                    {
                        // special case when the insert slot matches the split
                        // place between the two nodes, then the insert key
                        // becomes the split key.

                        TLX_BTREE_ASSERT(inner->get_slotuse() + 1 < inner_slotmax);

                        InnerNode* split = static_cast<InnerNode*>(*splitnode);

                        // move the split key and it's datum into the left node
                        
                        auto inner_dup = static_cast<InnerNode*>(dup_prologue(tid, inner));
                        if (inner_dup != nullptr) {
                            inner_dup->set_slotkey(inner_dup->get_slotuse(), *splitkey);
                            inner_dup->set_child(inner_dup->get_slotuse() + 1, split->get_child(0));
                            inner_dup->set_slotuse(inner_dup->get_slotuse() + 1);
                            dup_epilogue(tid, inner, inner_dup);
                        }

                        // set new split key and move corresponding datum into
                        // right node
                        auto split_dup = static_cast<InnerNode*>(dup_prologue(tid, split));
                        if (split_dup != nullptr) {
                            split_dup->set_child(0, newchild);
                            dup_epilogue(tid, split, split_dup);
                        }

                        *splitkey = newkey;

                        return r;
                    }
                    else if (slot >= inner->get_slotuse() + 1)
                    {
                        // in case the insert slot is in the newly create split
                        // node, we reuse the code below.
                        
                        slot -= inner->get_slotuse() + 1;
                        inner = static_cast<InnerNode*>(*splitnode);
                        TLX_BTREE_PRINT(
                            "BTree::insert_descend switching to "
                            "splitted node " << inner << " slot " << slot);
                    }
                }

                // move items and put pointer to child node into correct slot
                TLX_BTREE_ASSERT(slot >= 0 && slot <= inner->get_slotuse());

                auto inner_dup = static_cast<InnerNode*>(dup_prologue(tid, inner));
                if (inner_dup != nullptr) {
                    inner_dup->copy_backward_to_slotkey(
                        inner_dup->get_slotkey_vec() + slot, 
                        inner_dup->get_slotkey_vec() + inner_dup->get_slotuse(),
                        inner_dup->get_slotkey_vec() + inner_dup->get_slotuse() + 1);
                    inner_dup->copy_backward_to_childid(
                        inner_dup->get_childid_vec() + slot, 
                        inner_dup->get_childid_vec() + inner_dup->get_slotuse() + 1,
                        inner_dup->get_childid_vec() + inner_dup->get_slotuse() + 2);

                    inner_dup->set_slotkey(slot, newkey);
                    inner_dup->set_child(slot + 1, newchild);
                    inner_dup->set_slotuse(inner_dup->get_slotuse() + 1);
                    dup_epilogue(tid, inner, inner_dup);
                }
            }

            return r;
        }
        else // n->is_leafnode() == true
        {
            LeafNode* leaf = static_cast<LeafNode*>(n);

            unsigned short slot = find_lower(leaf, key);

            if (!allow_duplicates &&
                slot < leaf->get_slotuse() && key_equal(key, leaf->key(slot))) 
            {
                return std::pair<iterator, bool>(iterator(leaf, slot), false);
            }

            if (leaf->is_full())
            {
                split_leaf_node(tid, leaf, splitkey, splitnode);

                // check if insert slot is in the split sibling node
                if (slot >= leaf->get_slotuse())
                {
                    slot -= leaf->get_slotuse();
                    leaf = static_cast<LeafNode*>(*splitnode);
                }
            }

            // move items and put data item into correct data slot
            TLX_BTREE_ASSERT(slot >= 0 && slot <= leaf->get_slotuse());

            auto leaf_dup = static_cast<LeafNode*>(dup_prologue(tid, leaf));
            if (leaf_dup != nullptr) {
                leaf_dup->copy_backward_to_slotdata(
                    leaf_dup->get_slotdata_vec() + slot, 
                    leaf_dup->get_slotdata_vec() + leaf_dup->get_slotuse(),
                    leaf_dup->get_slotdata_vec() + leaf_dup->get_slotuse() + 1);

                leaf_dup->set_slot(slot, value);
                leaf_dup->set_slotuse(leaf_dup->get_slotuse() + 1);
                dup_epilogue(tid, leaf, leaf_dup);
            }
            
            if (splitnode && leaf != *splitnode && slot == leaf->get_slotuse() - 1)
            {
                // special case: the node was split, and the insert is at the
                // last slot of the old node. then the splitkey must be updated.
                *splitkey = key;
            }

            return std::pair<iterator, bool>(iterator(leaf, slot), true);
        }
    }

    //! Split up a leaf node into two equally-filled sibling leaves. Returns the
    //! new nodes and it's insertion key in the two parameters.
    void split_leaf_node(const int& tid, LeafNode* leaf,
                         key_type* out_newkey, node** out_newleaf) {
        TLX_BTREE_ASSERT(leaf->is_full());

        unsigned short mid = (leaf->get_slotuse() >> 1);

        TLX_BTREE_PRINT("BTree::split_leaf_node on " << leaf);

        LeafNode* newleaf = allocate_leaf(tid);

        auto newleaf_dup = static_cast<LeafNode*>(dup_prologue(tid, newleaf));
        if (newleaf_dup != nullptr) {
            newleaf_dup->set_slotuse(leaf->get_slotuse() - mid);

            /* ITERATOR ISSUE - TODO */
            // newleaf->next_leaf = leaf->next_leaf;
            // if (newleaf->next_leaf == nullptr) {
            //     TLX_BTREE_ASSERT(leaf == tail_leaf_);
            //     tail_leaf_ = newleaf;
            // }
            // else {
            //     newleaf->next_leaf->prev_leaf = newleaf;
            // }

            newleaf_dup->copy_to_slotdata(
                    leaf->get_slotdata_vec() + mid, 
                    leaf->get_slotdata_vec() + leaf->get_slotuse(), 
                    newleaf_dup->get_slotdata_vec());
            dup_epilogue(tid, newleaf, newleaf_dup);
        }

        auto leaf_dup = static_cast<LeafNode*>(dup_prologue(tid, leaf));
        if (leaf_dup != nullptr) {
            leaf_dup->set_slotuse(mid);
            dup_epilogue(tid, leaf, leaf_dup);
        }

        /* ITERATOR ISSUE - TODO */
        // leaf->next_leaf = newleaf;
        // newleaf->prev_leaf = leaf;

        *out_newkey = leaf->key(leaf->get_slotuse() - 1);
        *out_newleaf = newleaf;
    }

    //! Split up an inner node into two equally-filled sibling nodes. Returns
    //! the new nodes and it's insertion key in the two parameters. Requires the
    //! slot of the item will be inserted, so the nodes will be the same size
    //! after the insert.
    void split_inner_node(const int& tid, InnerNode* inner, key_type* out_newkey,
                          node** out_newinner, unsigned int addslot) {
        TLX_BTREE_ASSERT(inner->is_full());

        unsigned short mid = (inner->get_slotuse() >> 1);

        TLX_BTREE_PRINT("BTree::split_inner: mid " << mid <<
                        " addslot " << addslot);

        // if the split is uneven and the overflowing item will be put into the
        // larger node, then the smaller split node may underflow
        if (addslot <= mid && mid > inner->get_slotuse() - (mid + 1))
            mid--;

        TLX_BTREE_PRINT("BTree::split_inner: mid " << mid <<
                        " addslot " << addslot);

        TLX_BTREE_PRINT("BTree::split_inner_node on " << inner <<
                        " into two nodes " << mid << " and " <<
                        inner->get_slotuse() - (mid + 1) << " sized");

        InnerNode* newinner = allocate_inner(tid, inner->get_level());

        auto newinner_dup = static_cast<InnerNode*>(dup_prologue(tid, newinner));
        if (newinner_dup != nullptr) {
            newinner_dup->set_slotuse(inner->get_slotuse() - (mid + 1));

            newinner_dup->copy_to_slotkey(
                inner->get_slotkey_vec() + mid + 1, 
                inner->get_slotkey_vec() + inner->get_slotuse(), 
                newinner_dup->get_slotkey_vec());
            newinner_dup->copy_to_childid(
                inner->get_childid_vec() + mid + 1, 
                inner->get_childid_vec() + inner->get_slotuse() + 1, 
                newinner_dup->get_childid_vec());
            dup_epilogue(tid, newinner, newinner_dup);
        }

        auto inner_dup = static_cast<InnerNode*>(dup_prologue(tid, inner));
        if (inner_dup != nullptr) {
            inner_dup->set_slotuse(mid);
            dup_epilogue(tid, inner, inner_dup);
        }

        *out_newkey = inner->key(mid);
        *out_newinner = newinner;
    }

    //! \}

public:
    //! \name Bulk Loader - Construct Tree from Sorted Sequence
    //! \{

    //! Bulk load a sorted range. Loads items into leaves and constructs a
    //! B-tree above them. The tree must be empty when calling this function.
    template <typename Iterator>
    void bulk_load(const int& tid, Iterator ibegin, Iterator iend) {
        TLX_BTREE_ASSERT(empty());

        stats_.size = iend - ibegin;

        // calculate number of leaves needed, round up.
        size_t num_items = iend - ibegin;
        size_t num_leaves = (num_items + leaf_slotmax - 1) / leaf_slotmax;

        TLX_BTREE_PRINT("BTree::bulk_load, level 0: " << stats_.size <<
                        " items into " << num_leaves <<
                        " leaves with up to " <<
                        ((iend - ibegin + num_leaves - 1) / num_leaves) <<
                        " items per leaf.");

        Iterator it = ibegin;
        for (size_t i = 0; i < num_leaves; ++i)
        {
            // allocate new leaf node
            LeafNode* leaf = allocate_leaf(tid);

            // copy keys or (key,value) pairs into leaf nodes, uses template
            // switch leaf->set_slot().
            leaf->set_slotuse(static_cast<int>(num_items / (num_leaves - i)));
            for (size_t s = 0; s < leaf->get_slotuse(); ++s, ++it)
                leaf->set_slot(s, *it);

            if (tail_leaf_ != nullptr) {
                tail_leaf_->next_leaf = leaf;
                leaf->prev_leaf = tail_leaf_;
            }
            else {
                head_leaf_ = leaf;
            }
            tail_leaf_ = leaf;

            num_items -= leaf->get_slotuse();
        }

        TLX_BTREE_ASSERT(it == iend && num_items == 0);

        // if the btree is so small to fit into one leaf, then we're done.
        if (head_leaf_ == tail_leaf_) {
            root_ = head_leaf_;
            return;
        }

        TLX_BTREE_ASSERT(stats_.leaves == num_leaves);

        // create first level of inner nodes, pointing to the leaves.
        size_t num_parents =
            (num_leaves + (inner_slotmax + 1) - 1) / (inner_slotmax + 1);

        TLX_BTREE_PRINT("BTree::bulk_load, level 1: " <<
                        num_leaves << " leaves in " <<
                        num_parents << " inner nodes with up to " <<
                        ((num_leaves + num_parents - 1) / num_parents) <<
                        " leaves per inner node.");

        // save inner nodes and maxkey for next level.
        typedef std::pair<InnerNode*, const key_type*> nextlevel_type;
        nextlevel_type* nextlevel = new nextlevel_type[num_parents];

        LeafNode* leaf = head_leaf_;
        for (size_t i = 0; i < num_parents; ++i)
        {
            // allocate new inner node at level 1
            InnerNode* n = allocate_inner(tid, 1);

            n->set_slotuse(static_cast<int>(num_leaves / (num_parents - i)));
            TLX_BTREE_ASSERT(n->get_slotuse() > 0);
            // this counts keys, but an inner node has keys+1 children.
            n->set_slotuse(n->get_slotuse() - 1);

            // copy last key from each leaf and set child
            for (unsigned short s = 0; s < n->get_slotuse(); ++s)
            {
                n->set_slotkey(s, leaf->key(leaf->get_slotuse() - 1));
                n->set_child(s, leaf);
                leaf = leaf->next_leaf;
            }
            n->set_child(n->get_slotuse(), leaf);

            // track max key of any descendant.
            nextlevel[i].first = n;
            nextlevel[i].second = &leaf->key(leaf->get_slotuse() - 1);

            leaf = leaf->next_leaf;
            num_leaves -= n->get_slotuse() + 1;
        }

        TLX_BTREE_ASSERT(leaf == nullptr && num_leaves == 0);

        // recursively build inner nodes pointing to inner nodes.
        for (int level = 2; num_parents != 1; ++level)
        {
            size_t num_children = num_parents;
            num_parents =
                (num_children + (inner_slotmax + 1) - 1) / (inner_slotmax + 1);

            TLX_BTREE_PRINT(
                "BTree::bulk_load, level " << level <<
                    ": " << num_children << " children in " <<
                    num_parents << " inner nodes with up to " <<
                ((num_children + num_parents - 1) / num_parents) <<
                    " children per inner node.");

            size_t inner_index = 0;
            for (size_t i = 0; i < num_parents; ++i)
            {
                // allocate new inner node at level
                InnerNode* n = allocate_inner(tid, level);

                n->set_slotuse(static_cast<int>(num_children / (num_parents - i)));
                TLX_BTREE_ASSERT(n->get_slotuse() > 0);
                // this counts keys, but an inner node has keys+1 children.
                n->set_slotuse(n->get_slotuse() - 1);

                // copy children and maxkeys from nextlevel
                for (unsigned short s = 0; s < n->get_slotuse(); ++s)
                {
                    n->set_slotkey(s, *nextlevel[inner_index].second);
                    n->set_child(s, nextlevel[inner_index].first);
                    ++inner_index;
                }
                n->set_child(n->get_slotuse(), nextlevel[inner_index].first);

                // reuse nextlevel array for parents, because we can overwrite
                // slots we've already consumed.
                nextlevel[i].first = n;
                nextlevel[i].second = nextlevel[inner_index].second;

                ++inner_index;
                num_children -= n->get_slotuse() + 1;
            }

            TLX_BTREE_ASSERT(num_children == 0);
        }

        root_ = nextlevel[0].first;
        delete[] nextlevel;

        if (self_verify) verify();
    }

    //! \}

private:
    //! \name Support Class Encapsulating Deletion Results
    //! \{

    //! Result flags of recursive deletion.
    enum result_flags_t {
        //! Deletion successful and no fix-ups necessary.
        btree_ok = 0,

        //! Deletion not successful because key was not found.
        btree_not_found = 1,

        //! Deletion successful, the last key was updated so parent slotkeys
        //! need updates.
        btree_update_lastkey = 2,

        //! Deletion successful, children nodes were merged and the parent needs
        //! to remove the empty node.
        btree_fixmerge = 4
    };

    //! B+ tree recursive deletion has much information which is needs to be
    //! passed upward.
    struct result_t {
        //! Merged result flags
        result_flags_t flags;

        //! The key to be updated at the parent's slot
        key_type lastkey;

        //! Constructor of a result with a specific flag, this can also be used
        //! as for implicit conversion.
        result_t(result_flags_t f = btree_ok) // NOLINT
            : flags(f), lastkey()
        { }

        //! Constructor with a lastkey value.
        result_t(result_flags_t f, const key_type& k)
            : flags(f), lastkey(k)
        { }

        //! Test if this result object has a given flag set.
        bool has(result_flags_t f) const {
            return (flags & f) != 0;
        }

        //! Merge two results OR-ing the result flags and overwriting lastkeys.
        result_t& operator |= (const result_t& other) {
            flags = result_flags_t(flags | other.flags);

            // we overwrite existing lastkeys on purpose
            if (other.has(btree_update_lastkey))
                lastkey = other.lastkey;

            return *this;
        }
    };

    //! \}

public:
    //! \name Public Erase Functions
    //! \{

    //! Erases one (the first) of the key/data pairs associated with the given
    //! key.
    bool erase_one(const int& tid, const key_type& key) {
        TLX_BTREE_PRINT("BTree::erase_one(" << key <<
                        ") on btree size " << size());
        if (self_verify) verify();

        if (!orig_root) return false;
        
        result_t result = erase_one_descend(
            tid, key, orig_root, nullptr, nullptr, nullptr, nullptr, nullptr, 0);

        if (!result.has(btree_not_found))
            --stats_.size;

#ifdef TLX_BTREE_DEBUG
        if (debug) print(std::cout);
#endif
        if (self_verify) verify();
        
        return !result.has(btree_not_found);
    }

    //! Erases all the key/data pairs associated with the given key. This is
    //! implemented using erase_one().
    size_type erase(const int& tid, const key_type& key) {
        size_type c = 0;

        while (erase_one(tid, key))
        {
            ++c;
            if (!allow_duplicates) break;
        }

        return c;
    }

    //! Erase the key/data pair referenced by the iterator.
    void erase(const int& tid, iterator iter) {
        TLX_BTREE_PRINT("BTree::erase_iter(" << iter.curr_leaf <<
                        "," << iter.curr_slot << ") on btree size " << size());
        
        if (self_verify) verify();

        if (!root_) return;

        result_t result = erase_iter_descend(
            tid, iter, root_, nullptr, nullptr, nullptr, nullptr, nullptr, 0);

        if (!result.has(btree_not_found))
            --stats_.size;

#ifdef TLX_BTREE_DEBUG
        if (debug) print(std::cout);
#endif
        if (self_verify) verify();
    }

#ifdef BTREE_TODO
    //! Erase all key/data pairs in the range [first,last). This function is
    //! currently not implemented by the B+ Tree.
    void erase(iterator /* first */, iterator /* last */) {
        abort();
    }
#endif

    //! \}

private:
    //! \name Private Erase Functions
    //! \{

    /*!
     * Erase one (the first) key/data pair in the B+ tree matching key.
     *
     * Descends down the tree in search of key. During the descent the parent,
     * left and right siblings and their parents are computed and passed
     * down. Once the key/data pair is found, it is removed from the leaf. If
     * the leaf underflows 6 different cases are handled. These cases resolve
     * the underflow by shifting key/data pairs from adjacent sibling nodes,
     * merging two sibling nodes or trimming the tree.
     */
    result_t erase_one_descend(const int& tid, 
                               const key_type& key,
                               node* curr,
                               node* left, node* right,
                               InnerNode* left_parent, InnerNode* right_parent,
                               InnerNode* parent, unsigned int parentslot) {                                   
        
        if (curr->is_leafnode())
        {
            LeafNode* leaf = static_cast<LeafNode*>(curr);
            LeafNode* left_leaf = static_cast<LeafNode*>(left);
            LeafNode* right_leaf = static_cast<LeafNode*>(right);

            unsigned short slot = find_lower(leaf, key);
            
            if (slot >= leaf->get_slotuse() || !key_equal(key, leaf->key(slot)))
            {
                TLX_BTREE_PRINT("Could not find key " << key << " to erase.");

                return btree_not_found;
            }

            TLX_BTREE_PRINT(
                "Found key in leaf " << curr << " at slot " << slot);
            
            auto leaf_dup = static_cast<LeafNode*>(dup_prologue(tid, leaf));
            if (leaf_dup != nullptr) {
                leaf_dup->copy_to_slotdata(
                        leaf_dup->get_slotdata_vec() + slot + 1, 
                        leaf_dup->get_slotdata_vec() + leaf_dup->get_slotuse(),
                        leaf_dup->get_slotdata_vec() + slot);
                leaf_dup->set_slotuse(leaf_dup->get_slotuse() - 1);
                dup_epilogue(tid, leaf, leaf_dup);
            }

            result_t myres = btree_ok;

            // if the last key of the leaf was changed, the parent is notified
            // and updates the key of this leaf
            if (slot == leaf->get_slotuse())
            {
                if (parent && parentslot < parent->get_slotuse())
                {
                    TLX_BTREE_ASSERT(parent->get_child(parentslot) == curr);
                    auto parent_dup = static_cast<InnerNode*>(dup_prologue(tid, parent));
                    if (parent_dup != nullptr) {
                        parent_dup->set_slotkey(parentslot, leaf->key(leaf->get_slotuse() - 1));
                        dup_epilogue(tid, parent, parent_dup);
                    }
                }
                else
                {
                    if (leaf->get_slotuse() >= 1)
                    {
                        TLX_BTREE_PRINT("Scheduling lastkeyupdate: key " <<
                                        leaf->key(leaf->get_slotuse() - 1));
                        myres |= result_t(
                            btree_update_lastkey, leaf->key(leaf->get_slotuse() - 1));
                    }
                    else
                    {
                        TLX_BTREE_ASSERT(leaf == root_);
                    }
                }
            }
            
            if (leaf->is_underflow() && !(leaf == root_ && leaf->get_slotuse() >= 1))
            {
                // determine what to do about the underflow

                // case : if this empty leaf is the root, then delete all nodes
                // and set root to nullptr.
                if (left_leaf == nullptr && right_leaf == nullptr)
                {
                    TLX_BTREE_ASSERT(leaf == root_);
                    TLX_BTREE_ASSERT(leaf->get_slotuse() == 0);

                    free_node(tid, root_);

                    // root_ = leaf = nullptr; // TODO
                    auto root_dup = dup_prologue(tid, orig_root);
                    if (root_dup != nullptr)
                    {
                        root_dup = nullptr;
                        dup_epilogue(tid, orig_root, root_dup);
                    }

                    new_root = root_dup;

                    auto leaf_dup = static_cast<LeafNode*>(dup_prologue(tid, leaf));
                    if (leaf_dup != nullptr) {
                        leaf_dup = nullptr;
                        dup_epilogue(tid, leaf, leaf_dup);
                    }
                    
                    // head_leaf_ = tail_leaf_ = nullptr; // TODO

                    // will be decremented soon by insert_start()
                    TLX_BTREE_ASSERT(stats_.size == 1);
                    TLX_BTREE_ASSERT(stats_.leaves == 0);
                    TLX_BTREE_ASSERT(stats_.inner_nodes == 0);
                    
                    return btree_ok;
                }
                // case : if both left and right leaves would underflow in case
                // of a shift, then merging is necessary. choose the more local
                // merger with our parent
                else if ((left_leaf == nullptr || left_leaf->is_few()) &&
                         (right_leaf == nullptr || right_leaf->is_few()))
                {
                    if (left_parent == parent)
                        myres |= merge_leaves(tid, left_leaf, leaf, left_parent);
                    else
                        myres |= merge_leaves(tid, leaf, right_leaf, right_parent);
                }
                // case : the right leaf has extra data, so balance right with
                // current
                else if ((left_leaf != nullptr && left_leaf->is_few()) &&
                         (right_leaf != nullptr && !right_leaf->is_few()))
                {
                    if (right_parent == parent)
                        myres |= shift_left_leaf(
                            tid, leaf, right_leaf, right_parent, parentslot);
                    else
                        myres |= merge_leaves(tid, left_leaf, leaf, left_parent);
                }
                // case : the left leaf has extra data, so balance left with
                // current
                else if ((left_leaf != nullptr && !left_leaf->is_few()) &&
                         (right_leaf != nullptr && right_leaf->is_few()))
                {
                    if (left_parent == parent)
                        shift_right_leaf(
                            tid, left_leaf, leaf, left_parent, parentslot - 1);
                    else
                        myres |= merge_leaves(tid, leaf, right_leaf, right_parent);
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (left_parent == right_parent)
                {
                    if (left_leaf->get_slotuse() <= right_leaf->get_slotuse())
                        myres |= shift_left_leaf(
                            tid, leaf, right_leaf, right_parent, parentslot);
                    else
                        shift_right_leaf(
                            tid, left_leaf, leaf, left_parent, parentslot - 1);
                }
                else
                {
                    if (left_parent == parent)
                        shift_right_leaf(
                            tid, left_leaf, leaf, left_parent, parentslot - 1);
                    else
                        myres |= shift_left_leaf(
                            tid, leaf, right_leaf, right_parent, parentslot);
                }
            }
            
            return myres;
        }
        else // !curr->is_leafnode()
        {   
            InnerNode* inner = static_cast<InnerNode*>(curr);
            InnerNode* left_inner = static_cast<InnerNode*>(left);
            InnerNode* right_inner = static_cast<InnerNode*>(right);

            node* myleft, * myright;
            InnerNode* myleft_parent, * myright_parent;

            unsigned short slot = find_lower(inner, key);

            if (slot == 0) {
                myleft =
                    (left == nullptr) ? nullptr :
                    static_cast<InnerNode*>(left)->get_child(left->get_slotuse() - 1);
                myleft_parent = left_parent;
            }
            else {
                myleft = inner->get_child(slot - 1);
                myleft_parent = inner;
            }

            if (slot == inner->get_slotuse()) {
                myright =
                    (right == nullptr) ? nullptr :
                    static_cast<InnerNode*>(right)->get_child(0);
                myright_parent = right_parent;
            }
            else {
                myright = inner->get_child(slot + 1);
                myright_parent = inner;
            }

            TLX_BTREE_PRINT("erase_one_descend into " << inner->get_child(slot));

            result_t result = erase_one_descend(
                tid,
                key,
                inner->get_child(slot),
                myleft, myright,
                myleft_parent, myright_parent,
                inner, slot);

            result_t myres = btree_ok;

            if (result.has(btree_not_found))
            {
                return result;
            }

            if (result.has(btree_update_lastkey))
            {
                if (parent && parentslot < parent->get_slotuse())
                {
                    TLX_BTREE_PRINT("Fixing lastkeyupdate: key " <<
                                    result.lastkey << " into parent " <<
                                    parent << " at parentslot " <<
                                    parentslot);

                    TLX_BTREE_ASSERT(parent->get_child(parentslot) == curr);
                    auto parent_dup = static_cast<InnerNode*>(dup_prologue(tid, parent));
                    if (parent_dup != nullptr) {
                        parent_dup->set_slotkey(parentslot, result.lastkey);
                        dup_epilogue(tid, parent, parent_dup);
                    }
                }
                else
                {
                    TLX_BTREE_PRINT(
                        "Forwarding lastkeyupdate: key " << result.lastkey);
                    myres |= result_t(btree_update_lastkey, result.lastkey);
                }
            }

            if (result.has(btree_fixmerge))
            {
                // either the current node or the next is empty and should be
                // removed
                if (inner->get_child(slot)->get_slotuse() != 0)
                    slot++;

                // this is the child slot invalidated by the merge
                TLX_BTREE_ASSERT(inner->get_child(slot)->get_slotuse() == 0);

                free_node(tid, inner->get_child(slot));

                auto inner_dup = static_cast<InnerNode*>(dup_prologue(tid, inner));
                if (inner_dup != nullptr) {
                    inner_dup->copy_to_slotkey(
                        inner_dup->get_slotkey_vec() + slot, 
                        inner_dup->get_slotkey_vec() + inner_dup->get_slotuse(),
                        inner_dup->get_slotkey_vec() + slot - 1);
                    inner_dup->copy_to_childid(
                        inner_dup->get_childid_vec() + slot + 1,
                        inner_dup->get_childid_vec() + inner_dup->get_slotuse() + 1,
                        inner_dup->get_childid_vec() + slot);

                    inner_dup->set_slotuse(inner_dup->get_slotuse() - 1);
                    dup_epilogue(tid, inner, inner_dup);
                }
                
                if (inner->get_level() == 1)
                {
                    // fix split key for children leaves
                    slot--;
                    LeafNode* child =
                        static_cast<LeafNode*>(inner->get_child(slot));
                    auto inner_dup = static_cast<InnerNode*>(dup_prologue(tid, inner));
                    if (inner_dup != nullptr) {
                        inner_dup->set_slotkey(slot, child->key(child->get_slotuse() - 1));
                        dup_epilogue(tid, inner, inner_dup);
                    }
                }
            }

            if (inner->is_underflow() &&
                !(inner == root_ && inner->get_slotuse() >= 1))
            {
                // case: the inner node is the root and has just one child. that
                // child becomes the new root
                if (left_inner == nullptr && right_inner == nullptr)
                {
                    TLX_BTREE_ASSERT(inner == root_);
                    TLX_BTREE_ASSERT(inner->get_slotuse() == 0);
                    
                    // root_ = inner->get_child(0); // TODO

                    auto root_dup = dup_prologue(tid, orig_root);
                    if (root_dup != nullptr) {
                        root_dup = inner->get_child(0);
                        dup_epilogue(tid, orig_root, root_dup);
                    }

                    new_root = root_dup;

                    auto inner_dup = static_cast<InnerNode*>(dup_prologue(tid, inner));
                    if (inner_dup != nullptr) {
                        inner_dup->set_slotuse(0);
                        dup_epilogue(tid, inner, inner_dup);
                    }

                    free_node(tid, inner);
                    
                    return btree_ok;
                }
                // case : if both left and right leaves would underflow in case
                // of a shift, then merging is necessary. choose the more local
                // merger with our parent
                else if ((left_inner == nullptr || left_inner->is_few()) &&
                         (right_inner == nullptr || right_inner->is_few()))
                {
                    if (left_parent == parent)
                        myres |= merge_inner(
                            tid, left_inner, inner, left_parent, parentslot - 1);
                    else
                        myres |= merge_inner(
                            tid, inner, right_inner, right_parent, parentslot);
                }
                // case : the right leaf has extra data, so balance right with
                // current
                else if ((left_inner != nullptr && left_inner->is_few()) &&
                         (right_inner != nullptr && !right_inner->is_few()))
                {
                    if (right_parent == parent)
                        shift_left_inner(
                            tid, inner, right_inner, right_parent, parentslot);
                    else
                        myres |= merge_inner(
                            tid, left_inner, inner, left_parent, parentslot - 1);
                }
                // case : the left leaf has extra data, so balance left with
                // current
                else if ((left_inner != nullptr && !left_inner->is_few()) &&
                         (right_inner != nullptr && right_inner->is_few()))
                {
                    if (left_parent == parent) {
                        shift_right_inner(
                            tid, left_inner, inner, left_parent, parentslot - 1);
                    }
                    else {
                        myres |= merge_inner(
                            tid, inner, right_inner, right_parent, parentslot);
                    }
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (left_parent == right_parent)
                {
                    if (left_inner->get_slotuse() <= right_inner->get_slotuse()) {
                        shift_left_inner(
                            tid, inner, right_inner, right_parent, parentslot);
                    }
                    else {
                        shift_right_inner(
                            tid, left_inner, inner, left_parent, parentslot - 1);
                    }
                }
                else
                {
                    if (left_parent == parent) {
                        shift_right_inner(
                            tid, left_inner, inner, left_parent, parentslot - 1);
                    }
                    else {
                        shift_left_inner(
                            tid, inner, right_inner, right_parent, parentslot);
                    }
                }
            }
            
            return myres;
        }
    }

    /*!
     * Erase one key/data pair referenced by an iterator in the B+ tree.
     *
     * Descends down the tree in search of an iterator. During the descent the
     * parent, left and right siblings and their parents are computed and passed
     * down. The difficulty is that the iterator contains only a pointer to a
     * LeafNode, which means that this function must do a recursive depth first
     * search for that leaf node in the subtree containing all pairs of the same
     * key. This subtree can be very large, even the whole tree, though in
     * practice it would not make sense to have so many duplicate keys.
     *
     * Once the referenced key/data pair is found, it is removed from the leaf
     * and the same underflow cases are handled as in erase_one_descend.
     */
    result_t erase_iter_descend(const int& tid, 
                                const iterator& iter,
                                node* curr,
                                node* left, node* right,
                                InnerNode* left_parent, InnerNode* right_parent,
                                InnerNode* parent, unsigned int parentslot) {
        if (curr->is_leafnode())
        {
            LeafNode* leaf = static_cast<LeafNode*>(curr);
            LeafNode* left_leaf = static_cast<LeafNode*>(left);
            LeafNode* right_leaf = static_cast<LeafNode*>(right);

            // if this is not the correct leaf, get next step in recursive
            // search
            if (leaf != iter.curr_leaf)
            {
                return btree_not_found;
            }

            if (iter.curr_slot >= leaf->get_slotuse())
            {
                TLX_BTREE_PRINT("Could not find iterator (" <<
                                iter.curr_leaf << "," << iter.curr_slot <<
                                ") to erase. Invalid leaf node?");

                return btree_not_found;
            }

            unsigned short slot = iter.curr_slot;

            TLX_BTREE_PRINT("Found iterator in leaf " <<
                            curr << " at slot " << slot);

            leaf->copy_to_slotdata(
                    leaf->get_slotdata_vec() + slot + 1, 
                    leaf->get_slotdata_vec() + leaf->get_slotuse(),
                    leaf->get_slotdata_vec() + slot);

            leaf->set_slotuse(leaf->get_slotuse() - 1);

            result_t myres = btree_ok;

            // if the last key of the leaf was changed, the parent is notified
            // and updates the key of this leaf
            if (slot == leaf->get_slotuse())
            {
                if (parent && parentslot < parent->get_slotuse())
                {
                    TLX_BTREE_ASSERT(parent->get_child(parentslot) == curr);
                    parent->set_slotkey(parentslot, leaf->key(leaf->get_slotuse() - 1));
                }
                else
                {
                    if (leaf->get_slotuse() >= 1)
                    {
                        TLX_BTREE_PRINT("Scheduling lastkeyupdate: key " <<
                                        leaf->key(leaf->get_slotuse() - 1));
                        myres |= result_t(
                            btree_update_lastkey, leaf->key(leaf->get_slotuse() - 1));
                    }
                    else
                    {
                        TLX_BTREE_ASSERT(leaf == root_);
                    }
                }
            }

            if (leaf->is_underflow() && !(leaf == root_ && leaf->get_slotuse() >= 1))
            {
                // determine what to do about the underflow

                // case : if this empty leaf is the root, then delete all nodes
                // and set root to nullptr.
                if (left_leaf == nullptr && right_leaf == nullptr)
                {
                    TLX_BTREE_ASSERT(leaf == root_);
                    TLX_BTREE_ASSERT(leaf->get_slotuse() == 0);

                    free_node(tid, root_);

                    root_ = leaf = nullptr;
                    head_leaf_ = tail_leaf_ = nullptr;

                    // will be decremented soon by insert_start()
                    TLX_BTREE_ASSERT(stats_.size == 1);
                    TLX_BTREE_ASSERT(stats_.leaves == 0);
                    TLX_BTREE_ASSERT(stats_.inner_nodes == 0);

                    return btree_ok;
                }
                // case : if both left and right leaves would underflow in case
                // of a shift, then merging is necessary. choose the more local
                // merger with our parent
                else if ((left_leaf == nullptr || left_leaf->is_few()) &&
                         (right_leaf == nullptr || right_leaf->is_few()))
                {
                    if (left_parent == parent)
                        myres |= merge_leaves(left_leaf, leaf, left_parent);
                    else
                        myres |= merge_leaves(leaf, right_leaf, right_parent);
                }
                // case : the right leaf has extra data, so balance right with
                // current
                else if ((left_leaf != nullptr && left_leaf->is_few()) &&
                         (right_leaf != nullptr && !right_leaf->is_few()))
                {
                    if (right_parent == parent) {
                        myres |= shift_left_leaf(
                            tid, leaf, right_leaf, right_parent, parentslot);
                    }
                    else {
                        myres |= merge_leaves(left_leaf, leaf, left_parent);
                    }
                }
                // case : the left leaf has extra data, so balance left with
                // current
                else if ((left_leaf != nullptr && !left_leaf->is_few()) &&
                         (right_leaf != nullptr && right_leaf->is_few()))
                {
                    if (left_parent == parent) {
                        shift_right_leaf(
                            tid, left_leaf, leaf, left_parent, parentslot - 1);
                    }
                    else {
                        myres |= merge_leaves(leaf, right_leaf, right_parent);
                    }
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (left_parent == right_parent)
                {
                    if (left_leaf->get_slotuse() <= right_leaf->get_slotuse()) {
                        myres |= shift_left_leaf(
                            tid, leaf, right_leaf, right_parent, parentslot);
                    }
                    else {
                        shift_right_leaf(
                            tid, left_leaf, leaf, left_parent, parentslot - 1);
                    }
                }
                else
                {
                    if (left_parent == parent) {
                        shift_right_leaf(
                            tid, left_leaf, leaf, left_parent, parentslot - 1);
                    }
                    else {
                        myres |= shift_left_leaf(
                            tid, leaf, right_leaf, right_parent, parentslot);
                    }
                }
            }

            return myres;
        }
        else // !curr->is_leafnode()
        {
            InnerNode* inner = static_cast<InnerNode*>(curr);
            InnerNode* left_inner = static_cast<InnerNode*>(left);
            InnerNode* right_inner = static_cast<InnerNode*>(right);

            // find first slot below which the searched iterator might be
            // located.

            result_t result;
            unsigned short slot = find_lower(inner, iter.key());

            while (slot <= inner->get_slotuse())
            {
                node* myleft, * myright;
                InnerNode* myleft_parent, * myright_parent;

                if (slot == 0) {
                    myleft = (left == nullptr) ? nullptr
                             : static_cast<InnerNode*>(left)->get_child(
                        left->get_slotuse() - 1);
                    myleft_parent = left_parent;
                }
                else {
                    myleft = inner->get_child(slot - 1);
                    myleft_parent = inner;
                }

                if (slot == inner->get_slotuse()) {
                    myright = (right == nullptr) ? nullptr
                              : static_cast<InnerNode*>(right)->get_child(0);
                    myright_parent = right_parent;
                }
                else {
                    myright = inner->get_child(slot + 1);
                    myright_parent = inner;
                }

                TLX_BTREE_PRINT("erase_iter_descend into " <<
                                inner->get_child(slot));

                result = erase_iter_descend(tid, 
                                            iter,
                                            inner->get_child(slot),
                                            myleft, myright,
                                            myleft_parent, myright_parent,
                                            inner, slot);

                if (!result.has(btree_not_found))
                    break;

                // continue recursive search for leaf on next slot

                if (slot < inner->get_slotuse() &&
                    key_less(inner->get_slotkey(slot), iter.key()))
                    return btree_not_found;

                ++slot;
            }

            if (slot > inner->get_slotuse())
                return btree_not_found;

            result_t myres = btree_ok;

            if (result.has(btree_update_lastkey))
            {
                if (parent && parentslot < parent->get_slotuse())
                {
                    TLX_BTREE_PRINT("Fixing lastkeyupdate: key " <<
                                    result.lastkey << " into parent " <<
                                    parent << " at parentslot " << parentslot);

                    TLX_BTREE_ASSERT(parent->get_child(parentslot) == curr);
                    parent->set_slotkey(parentslot, result.lastkey);
                }
                else
                {
                    TLX_BTREE_PRINT(
                        "Forwarding lastkeyupdate: key " << result.lastkey);
                    myres |= result_t(btree_update_lastkey, result.lastkey);
                }
            }

            if (result.has(btree_fixmerge))
            {
                // either the current node or the next is empty and should be
                // removed
                if (inner->get_child(slot)->get_slotuse() != 0)
                    slot++;

                // this is the child slot invalidated by the merge
                TLX_BTREE_ASSERT(inner->get_child(slot)->get_slotuse() == 0);

                free_node(tid, inner->get_child(slot));

                inner->copy_to_slotkey(
                    inner->get_slotkey_vec() + slot, 
                    inner->get_slotkey_vec() + inner->get_slotuse(),
                    inner->get_slotkey_vec() + slot - 1);
                inner->copy_to_childid(
                    inner->get_childid_vec() + slot + 1,
                    inner->get_childid_vec() + inner->get_slotuse() + 1,
                    inner->get_childid_vec() + slot);

                inner->set_slotuse(inner->get_slotuse() - 1);

                if (inner->get_level() == 1)
                {
                    // fix split key for children leaves
                    slot--;
                    LeafNode* child =
                        static_cast<LeafNode*>(inner->get_child(slot));
                    inner->set_slotkey(slot, child->key(child->get_slotuse() - 1));
                }
            }

            if (inner->is_underflow() &&
                !(inner == root_ && inner->get_slotuse() >= 1))
            {
                // case: the inner node is the root and has just one
                // child. that child becomes the new root
                if (left_inner == nullptr && right_inner == nullptr)
                {
                    TLX_BTREE_ASSERT(inner == root_);
                    TLX_BTREE_ASSERT(inner->get_slotuse() == 0);

                    root_ = inner->get_child(0);

                    inner->set_slotuse(0);
                    free_node(tid, inner);

                    return btree_ok;
                }
                // case : if both left and right leaves would underflow in case
                // of a shift, then merging is necessary. choose the more local
                // merger with our parent
                else if ((left_inner == nullptr || left_inner->is_few()) &&
                         (right_inner == nullptr || right_inner->is_few()))
                {
                    if (left_parent == parent) {
                        myres |= merge_inner(
                            left_inner, inner, left_parent, parentslot - 1);
                    }
                    else {
                        myres |= merge_inner(
                            inner, right_inner, right_parent, parentslot);
                    }
                }
                // case : the right leaf has extra data, so balance right with
                // current
                else if ((left_inner != nullptr && left_inner->is_few()) &&
                         (right_inner != nullptr && !right_inner->is_few()))
                {
                    if (right_parent == parent) {
                        shift_left_inner(
                            tid, inner, right_inner, right_parent, parentslot);
                    }
                    else {
                        myres |= merge_inner(
                            left_inner, inner, left_parent, parentslot - 1);
                    }
                }
                // case : the left leaf has extra data, so balance left with
                // current
                else if ((left_inner != nullptr && !left_inner->is_few()) &&
                         (right_inner != nullptr && right_inner->is_few()))
                {
                    if (left_parent == parent) {
                        shift_right_inner(
                            left_inner, inner, left_parent, parentslot - 1);
                    }
                    else {
                        myres |= merge_inner(
                            inner, right_inner, right_parent, parentslot);
                    }
                }
                // case : both the leaf and right leaves have extra data and our
                // parent, choose the leaf with more data
                else if (left_parent == right_parent)
                {
                    if (left_inner->get_slotuse() <= right_inner->get_slotuse()) {
                        shift_left_inner(
                            tid, inner, right_inner, right_parent, parentslot);
                    }
                    else {
                        shift_right_inner(
                            left_inner, inner, left_parent, parentslot - 1);
                    }
                }
                else
                {
                    if (left_parent == parent) {
                        shift_right_inner(
                            left_inner, inner, left_parent, parentslot - 1);
                    }
                    else {
                        shift_left_inner(
                            tid, inner, right_inner, right_parent, parentslot);
                    }
                }
            }

            return myres;
        }
    }

    //! Merge two leaf nodes. The function moves all key/data pairs from right
    //! to left and sets right's slotuse to zero. The right slot is then removed
    //! by the calling parent node.
    result_t merge_leaves(const int& tid, 
                          LeafNode* left, 
                          LeafNode* right,
                          InnerNode* parent) {
        TLX_BTREE_PRINT("Merge leaf nodes " << left << " and " << right <<
                        " with common parent " << parent << ".");
        (void)parent;

        TLX_BTREE_ASSERT(left->is_leafnode() && right->is_leafnode());
        TLX_BTREE_ASSERT(parent->get_level() == 1);

        TLX_BTREE_ASSERT(left->get_slotuse() + right->get_slotuse() < leaf_slotmax);

        auto right_dup = static_cast<LeafNode*>(dup_prologue(tid, right));
        auto left_dup = static_cast<LeafNode*>(dup_prologue(tid, left));
        if (left_dup != nullptr) {
            left_dup->copy_to_slotdata(
                    right->get_slotdata_vec(), 
                    right->get_slotdata_vec() + right->get_slotuse(),
                    left_dup->get_slotdata_vec() + left_dup->get_slotuse());

            left_dup->set_slotuse(left_dup->get_slotuse() + right->get_slotuse());
            dup_epilogue(tid, left, left_dup);
        }     

        //TODO - iterator stuff
        left->next_leaf = right->next_leaf;
        if (left->next_leaf)
            left->next_leaf->prev_leaf = left;
        else
            tail_leaf_ = left;

        // auto right_dup = static_cast<LeafNode*>(dup_prologue(tid, right));
        if (right_dup != nullptr) {
            right_dup->set_slotuse(0);
            dup_epilogue(tid, right, right_dup);
        }

        return btree_fixmerge;
    }

    //! Merge two inner nodes. The function moves all key/childid pairs from
    //! right to left and sets right's slotuse to zero. The right slot is then
    //! removed by the calling parent node.
    result_t merge_inner(const int& tid, 
                                InnerNode* left, 
                                InnerNode* right,
                                InnerNode* parent, unsigned int parentslot) {
        TLX_BTREE_PRINT("Merge inner nodes " << left << " and " << right <<
                        " with common parent " << parent << ".");

        TLX_BTREE_ASSERT(left->get_level() == right->get_level());
        TLX_BTREE_ASSERT(parent->get_level() == left->get_level() + 1);

        TLX_BTREE_ASSERT(parent->get_child(parentslot) == left);

        TLX_BTREE_ASSERT(left->get_slotuse() + right->get_slotuse() < inner_slotmax);

        if (self_verify)
        {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while (leftslot <= parent->get_slotuse() &&
                   parent->get_child(leftslot) != left)
                ++leftslot;

            TLX_BTREE_ASSERT(leftslot < parent->get_slotuse());
            TLX_BTREE_ASSERT(parent->get_child(leftslot) == left);
            TLX_BTREE_ASSERT(parent->get_child(leftslot + 1) == right);

            TLX_BTREE_ASSERT(parentslot == leftslot);
        }

        // retrieve the decision key from parent
        auto right_dup = static_cast<InnerNode*>(dup_prologue(tid, right));
        auto left_dup = static_cast<InnerNode*>(dup_prologue(tid, left));
        if (left_dup != nullptr) {
            left_dup->set_slotkey(left_dup->get_slotuse(), parent->get_slotkey(parentslot));
            left_dup->set_slotuse(left_dup->get_slotuse() + 1);

            // copy over keys and children from right
            left_dup->copy_to_slotkey(
                    right->get_slotkey_vec(),
                    right->get_slotkey_vec() + right->get_slotuse(),
                    left_dup->get_slotkey_vec() + left_dup->get_slotuse());
            left_dup->copy_to_childid(
                    right->get_childid_vec(), 
                    right->get_childid_vec() + right->get_slotuse() + 1,
                    left_dup->get_childid_vec() + left_dup->get_slotuse());

            left_dup->set_slotuse(left_dup->get_slotuse() + right->get_slotuse());
            dup_epilogue(tid, left, left_dup);
        }

        // auto right_dup = static_cast<InnerNode*>(dup_prologue(tid, right));
        if (right_dup != nullptr) {
            right_dup->set_slotuse(0);
            dup_epilogue(tid, right, right_dup);
        }

        return btree_fixmerge;
    }

    //! Balance two leaf nodes. The function moves key/data pairs from right to
    //! left so that both nodes are equally filled. The parent node is updated
    //! if possible.
    result_t shift_left_leaf(
        const int& tid, LeafNode* left, LeafNode* right,
        InnerNode* parent, unsigned int parentslot) {

        TLX_BTREE_ASSERT(left->is_leafnode() && right->is_leafnode());
        TLX_BTREE_ASSERT(parent->get_level() == 1);

        // TLX_BTREE_ASSERT(left->next_leaf == right);
        // TLX_BTREE_ASSERT(left == right->prev_leaf);

        TLX_BTREE_ASSERT(left->get_slotuse() < right->get_slotuse());
        TLX_BTREE_ASSERT(parent->get_child(parentslot) == left);

        unsigned int shiftnum = (right->get_slotuse() - left->get_slotuse()) >> 1;

        TLX_BTREE_PRINT("Shifting (leaf) " << shiftnum << " entries to left " <<
                        left << " from right " << right <<
                        " with common parent " << parent << ".");

        TLX_BTREE_ASSERT(left->get_slotuse() + shiftnum < leaf_slotmax);

        // copy the first items from the right node to the last slot in the left
        // node.
        auto right_dup = static_cast<LeafNode*>(dup_prologue(tid, right));
        auto left_dup = static_cast<LeafNode*>(dup_prologue(tid, left));
        if (left_dup != nullptr) {
            left_dup->copy_to_slotdata(
                    right->get_slotdata_vec(), 
                    right->get_slotdata_vec() + shiftnum,
                    left_dup->get_slotdata_vec() + left_dup->get_slotuse());

            left_dup->set_slotuse(left_dup->get_slotuse() + shiftnum);
            dup_epilogue(tid, left, left_dup);
        }

        // shift all slots in the right node to the left
        // auto right_dup = static_cast<LeafNode*>(dup_prologue(tid, right));
        if (right_dup != nullptr) {
            right_dup->copy_to_slotdata(
                    right_dup->get_slotdata_vec() + shiftnum, 
                    right_dup->get_slotdata_vec() + right_dup->get_slotuse(),
                    right_dup->get_slotdata_vec());

            right_dup->set_slotuse(right_dup->get_slotuse() - shiftnum);
            dup_epilogue(tid, right, right_dup);
        }

        // fixup parent
        if (parentslot < parent->get_slotuse()) {
            auto parent_dup = static_cast<InnerNode*>(dup_prologue(tid, parent));
            if (parent_dup != nullptr) {
                parent_dup->set_slotkey(parentslot, left->key(left->get_slotuse() - 1));
                dup_epilogue(tid, parent, parent_dup);
            }
            
            return btree_ok;
        }
        else {  // the update is further up the tree
            return result_t(btree_update_lastkey, left->key(left->get_slotuse() - 1));
        }
    }

    //! Balance two inner nodes. The function moves key/data pairs from right to
    //! left so that both nodes are equally filled. The parent node is updated
    //! if possible.
    void shift_left_inner(
                const int& tid, InnerNode* left, InnerNode* right,
                InnerNode* parent, unsigned int parentslot) {
        TLX_BTREE_ASSERT(left->get_level() == right->get_level());
        TLX_BTREE_ASSERT(parent->get_level() == left->get_level() + 1);

        TLX_BTREE_ASSERT(left->get_slotuse() < right->get_slotuse());
        TLX_BTREE_ASSERT(parent->get_child(parentslot) == left);

        unsigned int shiftnum = (right->get_slotuse() - left->get_slotuse()) >> 1;

        TLX_BTREE_PRINT("Shifting (inner) " << shiftnum <<
                        " entries to left " << left <<
                        " from right " << right <<
                        " with common parent " << parent << ".");

        TLX_BTREE_ASSERT(left->get_slotuse() + shiftnum < inner_slotmax);

        if (self_verify)
        {
            // find the left node's slot in the parent's children and compare to
            // parentslot

            unsigned int leftslot = 0;
            while (leftslot <= parent->get_slotuse() &&
                   parent->get_child(leftslot) != left)
                ++leftslot;

            TLX_BTREE_ASSERT(leftslot < parent->get_slotuse());
            TLX_BTREE_ASSERT(parent->get_child(leftslot) == left);
            TLX_BTREE_ASSERT(parent->get_child(leftslot + 1) == right);

            TLX_BTREE_ASSERT(leftslot == parentslot);
        }

        // copy the parent's decision slotkey and childid to the first new key
        // on the left
        auto right_dup = static_cast<InnerNode*>(dup_prologue(tid, right));
        auto left_dup = static_cast<InnerNode*>(dup_prologue(tid, left));
        if (left_dup != nullptr) {
            left_dup->set_slotkey(left_dup->get_slotuse(), parent->get_slotkey(parentslot));
            left_dup->set_slotuse(left_dup->get_slotuse() + 1);

            // copy the other items from the right node to the last slots in the
            // left node.
            left_dup->copy_to_slotkey(
                    right->get_slotkey_vec(), 
                    right->get_slotkey_vec() + shiftnum - 1,
                    left_dup->get_slotkey_vec() + left_dup->get_slotuse());
            left_dup->copy_to_childid(
                    right->get_childid_vec(), 
                    right->get_childid_vec() + shiftnum,
                    left_dup->get_childid_vec() + left_dup->get_slotuse());

            left_dup->set_slotuse(left_dup->get_slotuse() + (shiftnum - 1));
            dup_epilogue(tid, left, left_dup);
        }

        // fixup parent
        auto parent_dup = static_cast<InnerNode*>(dup_prologue(tid, parent));
        if (parent_dup != nullptr) {
            parent_dup->set_slotkey(parentslot, right->get_slotkey(shiftnum - 1));
            dup_epilogue(tid, parent, parent_dup);
        }

        // shift all slots in the right node
        // auto right_dup = static_cast<InnerNode*>(dup_prologue(tid, right));
        if (right_dup != nullptr) {
            right_dup->copy_to_slotkey(
                right_dup->get_slotkey_vec() + shiftnum, 
                right_dup->get_slotkey_vec() + right_dup->get_slotuse(),
                right_dup->get_slotkey_vec());
            right_dup->copy_to_childid(
                right_dup->get_childid_vec() + shiftnum, 
                right_dup->get_childid_vec() + right_dup->get_slotuse() + 1,
                right_dup->get_childid_vec());

            right_dup->set_slotuse(right_dup->get_slotuse() - shiftnum);
            dup_epilogue(tid, right, right_dup);
        }
    }

    //! Balance two leaf nodes. The function moves key/data pairs from left to
    //! right so that both nodes are equally filled. The parent node is updated
    //! if possible.
    void shift_right_leaf(
                const int& tid, LeafNode* left, LeafNode* right,
                InnerNode* parent, unsigned int parentslot) {
        TLX_BTREE_ASSERT(left->is_leafnode() && right->is_leafnode());
        TLX_BTREE_ASSERT(parent->get_level() == 1);

        // TLX_BTREE_ASSERT(left->next_leaf == right);
        // TLX_BTREE_ASSERT(left == right->prev_leaf);
        TLX_BTREE_ASSERT(parent->get_child(parentslot) == left);

        TLX_BTREE_ASSERT(left->get_slotuse() > right->get_slotuse());

        unsigned int shiftnum = (left->get_slotuse() - right->get_slotuse()) >> 1;

        TLX_BTREE_PRINT("Shifting (leaf) " << shiftnum <<
                        " entries to right " << right <<
                        " from left " << left <<
                        " with common parent " << parent << ".");

        if (self_verify)
        {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while (leftslot <= parent->get_slotuse() &&
                   parent->get_child(leftslot) != left)
                ++leftslot;

            TLX_BTREE_ASSERT(leftslot < parent->get_slotuse());
            TLX_BTREE_ASSERT(parent->get_child(leftslot) == left);
            TLX_BTREE_ASSERT(parent->get_child(leftslot + 1) == right);

            TLX_BTREE_ASSERT(leftslot == parentslot);
        }

        // shift all slots in the right node

        TLX_BTREE_ASSERT(right->get_slotuse() + shiftnum < leaf_slotmax);

        auto left_dup = static_cast<LeafNode*>(dup_prologue(tid, left));
        auto right_dup = static_cast<LeafNode*>(dup_prologue(tid, right));
        if (right_dup != nullptr) {
            right_dup->copy_backward_to_slotdata(
                    right_dup->get_slotdata_vec(), 
                    right_dup->get_slotdata_vec() + right_dup->get_slotuse(),
                    right_dup->get_slotdata_vec() + right_dup->get_slotuse() + shiftnum);

            right_dup->set_slotuse(right_dup->get_slotuse() + shiftnum);

            // copy the last items from the left node to the first slot in the right
            // node.
            right_dup->copy_to_slotdata(
                    left->get_slotdata_vec() + left->get_slotuse() - shiftnum,
                    left->get_slotdata_vec() + left->get_slotuse(), 
                    right_dup->get_slotdata_vec());
            dup_epilogue(tid, right, right_dup);
        }

        // auto left_dup = static_cast<LeafNode*>(dup_prologue(tid, left));
        if (left_dup != nullptr) {
            left_dup->set_slotuse(left_dup->get_slotuse() - shiftnum);
            dup_epilogue(tid, left, left_dup);
        }

        auto parent_dup = static_cast<InnerNode*>(dup_prologue(tid, parent));
        if (parent_dup != nullptr) {
            parent_dup->set_slotkey(parentslot, left->key(left->get_slotuse() - 1));
            dup_epilogue(tid, parent, parent_dup);
        }
    }

    //! Balance two inner nodes. The function moves key/data pairs from left to
    //! right so that both nodes are equally filled. The parent node is updated
    //! if possible.
    void shift_right_inner(
                const int& tid, InnerNode* left, InnerNode* right,
                InnerNode* parent, unsigned int parentslot) {
        
        TLX_BTREE_ASSERT(left->get_level() == right->get_level());
        TLX_BTREE_ASSERT(parent->get_level() == left->get_level() + 1);

        TLX_BTREE_ASSERT(left->get_slotuse() > right->get_slotuse());
        TLX_BTREE_ASSERT(parent->get_child(parentslot) == left);

        unsigned int shiftnum = (left->get_slotuse() - right->get_slotuse()) >> 1;

        TLX_BTREE_PRINT("Shifting (leaf) " << shiftnum <<
                        " entries to right " << right <<
                        " from left " << left <<
                        " with common parent " << parent << ".");

        if (self_verify)
        {
            // find the left node's slot in the parent's children
            unsigned int leftslot = 0;
            while (leftslot <= parent->get_slotuse() &&
                   parent->get_child(leftslot) != left)
                ++leftslot;

            TLX_BTREE_ASSERT(leftslot < parent->get_slotuse());
            TLX_BTREE_ASSERT(parent->get_child(leftslot) == left);
            TLX_BTREE_ASSERT(parent->get_child(leftslot + 1) == right);

            TLX_BTREE_ASSERT(leftslot == parentslot);
        }

        // shift all slots in the right node

        TLX_BTREE_ASSERT(right->get_slotuse() + shiftnum < inner_slotmax);
        
        auto left_dup = static_cast<InnerNode*>(dup_prologue(tid, left));
        auto right_dup = static_cast<InnerNode*>(dup_prologue(tid, right));
        if (right_dup != nullptr) {
            right_dup->copy_backward_to_slotkey(
                right_dup->get_slotkey_vec(), 
                right_dup->get_slotkey_vec() + right_dup->get_slotuse(),
                right_dup->get_slotkey_vec() + right_dup->get_slotuse() + shiftnum);
            right_dup->copy_backward_to_childid(
                right_dup->get_childid_vec(), 
                right_dup->get_childid_vec() + right_dup->get_slotuse() + 1,
                right_dup->get_childid_vec() + right_dup->get_slotuse() + 1 + shiftnum);

            right_dup->set_slotuse(right_dup->get_slotuse() + shiftnum);

            // copy the parent's decision slotkey and childid to the last new key on
            // the right
            right_dup->set_slotkey(shiftnum - 1, parent->get_slotkey(parentslot));

            // copy the remaining last items from the left node to the first slot in
            // the right node.
            right_dup->copy_to_slotkey(
                    left->get_slotkey_vec() + left->get_slotuse() - shiftnum + 1,
                    left->get_slotkey_vec() + left->get_slotuse(), 
                    right_dup->get_slotkey_vec());
            right_dup->copy_to_childid(
                    left->get_childid_vec() + left->get_slotuse() - shiftnum + 1,
                    left->get_childid_vec() + left->get_slotuse() + 1, 
                    right_dup->get_childid_vec());
            dup_epilogue(tid, right, right_dup);
        }
        
        // copy the first to-be-removed key from the left node to the parent's
        // decision slot
        auto parent_dup = static_cast<InnerNode*>(dup_prologue(tid, parent));
        if (parent_dup != nullptr) {
            parent_dup->set_slotkey(parentslot, left->get_slotkey(left->get_slotuse() - shiftnum));
            dup_epilogue(tid, parent, parent_dup);
        }
        
        // auto left_dup = static_cast<InnerNode*>(dup_prologue(tid, left));
        if (left_dup != nullptr) {
            left_dup->set_slotuse(left_dup->get_slotuse() - shiftnum);
            dup_epilogue(tid, left, left_dup);
        }
    }

    //! \}

#ifdef TLX_BTREE_DEBUG

public:
    //! \name Debug Printing
    //! \{

    //! Print out the B+ tree structure with keys onto the given ostream. This
    //! function requires that the header is compiled with TLX_BTREE_DEBUG and
    //! that key_type is printable via std::ostream.
    void print(std::ostream& os) const {
        if (root_) {
            print_node(os, root_, 0, true);
        }
    }

    //! Print out only the leaves via the double linked list.
    void print_leaves(std::ostream& os) const {
        os << "leaves:" << std::endl;

        const LeafNode* n = head_leaf_;

        while (n)
        {
            os << "  " << n << std::endl;

            n = n->next_leaf;
        }
    }

private:
    //! Recursively descend down the tree and print out nodes.
    static void print_node(std::ostream& os, const node* node,
                           unsigned int depth = 0, bool recursive = false) {
        for (unsigned int i = 0; i < depth; i++) os << "  ";

        os << "node " << node << " level " << node->get_level() <<
            " slotuse " << node->get_slotuse() << std::endl;

        if (node->is_leafnode())
        {
            const LeafNode* leafnode = static_cast<const LeafNode*>(node);

            for (unsigned int i = 0; i < depth; i++) os << "  ";
            os << "  leaf prev " << leafnode->prev_leaf <<
                " next " << leafnode->next_leaf << std::endl;

            for (unsigned int i = 0; i < depth; i++) os << "  ";

            for (unsigned short slot = 0; slot < leafnode->get_slotuse(); ++slot)
            {
                // os << leafnode->key(slot) << " "
                //    << "(data: " << leafnode->get_slot(slot) << ") ";
                os << leafnode->key(slot) << "  ";
            }
            os << std::endl;
        }
        else
        {
            const InnerNode* innernode = static_cast<const InnerNode*>(node);

            for (unsigned int i = 0; i < depth; i++) os << "  ";

            for (unsigned short slot = 0; slot < innernode->get_slotuse(); ++slot)
            {
                os << "(" << innernode->get_child(slot) << ") "
                   << innernode->get_slotkey(slot) << " ";
            }
            os << "(" << innernode->get_child(innernode->get_slotuse()) << ")"
               << std::endl;

            if (recursive)
            {
                for (unsigned short slot = 0;
                     slot < innernode->get_slotuse() + 1; ++slot)
                {
                    print_node(
                        os, innernode->get_child(slot), depth + 1, recursive);
                }
            }
        }
    }

    //! \}
#endif

public:
    //! \name Verification of B+ Tree Invariants
    //! \{

    //! Run a thorough verification of all B+ tree invariants. The program
    //! aborts via tlx_die_unless() if something is wrong.
    void verify() const {
        key_type minkey, maxkey;
        tree_stats vstats;

        if (root_)
        {
            verify_node(root_, &minkey, &maxkey, vstats);

            tlx_die_unless(vstats.size == stats_.size);
            tlx_die_unless(vstats.leaves == stats_.leaves);
            tlx_die_unless(vstats.inner_nodes == stats_.inner_nodes);

            verify_leaflinks();
        }
    }

private:
    //! Recursively descend down the tree and verify each node
    void verify_node(const node* n, key_type* minkey, key_type* maxkey,
                     tree_stats& vstats) const {
        TLX_BTREE_PRINT("verifynode " << n);

        if (n->is_leafnode())
        {
            const LeafNode* leaf = static_cast<const LeafNode*>(n);

            tlx_die_unless(leaf == root_ || !leaf->is_underflow());
            tlx_die_unless(leaf->get_slotuse() > 0);

            for (unsigned short slot = 0; slot < leaf->get_slotuse() - 1; ++slot)
            {
                tlx_die_unless(
                    key_lessequal(leaf->key(slot), leaf->key(slot + 1)));
            }

            *minkey = leaf->key(0);
            *maxkey = leaf->key(leaf->get_slotuse() - 1);

            vstats.leaves++;
            vstats.size += leaf->get_slotuse();
        }
        else // !n->is_leafnode()
        {
            const InnerNode* inner = static_cast<const InnerNode*>(n);
            vstats.inner_nodes++;

            tlx_die_unless(inner == root_ || !inner->is_underflow());
            tlx_die_unless(inner->get_slotuse() > 0);

            for (unsigned short slot = 0; slot < inner->get_slotuse() - 1; ++slot)
            {
                tlx_die_unless(
                    key_lessequal(inner->key(slot), inner->key(slot + 1)));
            }

            for (unsigned short slot = 0; slot <= inner->get_slotuse(); ++slot)
            {
                const node* subnode = inner->get_child(slot);
                key_type subminkey = key_type();
                key_type submaxkey = key_type();

                tlx_die_unless(subnode->get_level() + 1 == inner->get_level());
                verify_node(subnode, &subminkey, &submaxkey, vstats);

                TLX_BTREE_PRINT("verify subnode " << subnode <<
                                ": " << subminkey <<
                                " - " << submaxkey);

                if (slot == 0)
                    *minkey = subminkey;
                else
                    tlx_die_unless(
                        key_greaterequal(subminkey, inner->key(slot - 1)));

                if (slot == inner->get_slotuse())
                    *maxkey = submaxkey;
                else
                    tlx_die_unless(key_equal(inner->key(slot), submaxkey));

                if (inner->get_level() == 1 && slot < inner->get_slotuse())
                {
                    // children are leaves and must be linked together in the
                    // correct order
                    const LeafNode* leafa = static_cast<const LeafNode*>(
                        inner->get_child(slot));
                    const LeafNode* leafb = static_cast<const LeafNode*>(
                        inner->get_child(slot + 1));

                    tlx_die_unless(leafa->next_leaf == leafb);
                    tlx_die_unless(leafa == leafb->prev_leaf);
                }
                if (inner->get_level() == 2 && slot < inner->get_slotuse())
                {
                    // verify leaf links between the adjacent inner nodes
                    const InnerNode* parenta = static_cast<const InnerNode*>(
                        inner->get_child(slot));
                    const InnerNode* parentb = static_cast<const InnerNode*>(
                        inner->get_child(slot + 1));

                    const LeafNode* leafa = static_cast<const LeafNode*>(
                        parenta->get_child(parenta->get_slotuse()));
                    const LeafNode* leafb = static_cast<const LeafNode*>(
                        parentb->get_child(0));

                    tlx_die_unless(leafa->next_leaf == leafb);
                    tlx_die_unless(leafa == leafb->prev_leaf);
                }
            }
        }
    }

    //! Verify the double linked list of leaves.
    void verify_leaflinks() const {
        const LeafNode* n = head_leaf_;

        tlx_die_unless(n->get_level() == 0);
        tlx_die_unless(!n || n->prev_leaf == nullptr);

        unsigned int testcount = 0;

        while (n)
        {
            tlx_die_unless(n->get_level() == 0);
            tlx_die_unless(n->get_slotuse() > 0);

            for (unsigned short slot = 0; slot < n->get_slotuse() - 1; ++slot)
            {
                tlx_die_unless(key_lessequal(n->key(slot), n->key(slot + 1)));
            }

            testcount += n->get_slotuse();

            if (n->next_leaf)
            {
                tlx_die_unless(key_lessequal(n->key(n->get_slotuse() - 1),
                                             n->next_leaf->key(0)));

                tlx_die_unless(n == n->next_leaf->prev_leaf);
            }
            else
            {
                tlx_die_unless(tail_leaf_ == n);
            }

            n = n->next_leaf;
        }

        tlx_die_unless(testcount == size());
    }

    //! \}
};

//! \}
//! \}

} // namespace tlx

#endif // !TLX_CONTAINER_BTREE_HEADER

/******************************************************************************/