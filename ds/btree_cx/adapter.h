/**
 * Implementation of the lock-free external BST of Ellen, Fatourou, Ruppert and van Breugel.
 * This is a heavily modified version of the ASCYLIB implementation (see copyright in ellen.h).
 * The modifications are copyrighted (consistent with the original license)
 *   by Maya Arbel-Raviv and Trevor Brown, 2018.
 */

#ifndef BST_ADAPTER_H
#define BST_ADAPTER_H

#include <iostream>
#include <csignal>
#include "errors.h"
#include "random_fnv1a.h"
#ifdef USE_TREE_STATS
#   define TREE_STATS_BYTES_AT_DEPTH
#   include "tree_stats.h"
#endif
#include "btree_cx.hpp"
#include <iostream>

#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, tlx::inner_node<K, std::pair<K,V>>, tlx::leaf_node<K, std::pair<K,V>>>
#define DATA_STRUCTURE_T btree_ser<K, V, RECORD_MANAGER_T>

template <typename K, typename V, class Reclaim = reclaimer_debra<K>, class Alloc = allocator_new<K>, class Pool = pool_none<K>>
class ds_adapter {
private:
    const V NO_VALUE;
    DATA_STRUCTURE_T * const ds;

public:
    ds_adapter(const int NUM_THREADS,
               const K& KEY_MIN,
               const K& KEY_MAX,
               const V& VALUE_RESERVED,
               RandomFNV1A * const unused2)
    : NO_VALUE(VALUE_RESERVED)
    , ds(new DATA_STRUCTURE_T(NUM_THREADS, KEY_MIN, KEY_MAX, NO_VALUE, 0 /* unused */))
    {}
    ~ds_adapter() {
        delete ds;
    }
    
    V getNoValue() {
        return NO_VALUE;
    }
    
    void initThread(const int tid) {
        ds->initThread(tid);
    }
    void deinitThread(const int tid) {
        ds->deinitThread(tid);
    }

    V insert(const int tid, const K& key, const V& val) {
        setbench_error("insert-replace functionality not implemented for this data structure");
    }
    V insertIfAbsent(const int tid, const K& key, const V& val) {
        return ds->insert(tid, key, val);
    }
    V erase(const int tid, const K& key) {
        return ds->erase(tid, key);
    }
    V find(const int tid, const K& key) {
        return ds->find(tid, key);
    }
    bool contains(const int tid, const K& key) {
        return find(tid, key) != getNoValue();
    }
    int rangeQuery(const int tid, const K& lo, const K& hi, K * const resultKeys, V * const resultValues) {
        setbench_error("not implemented");
    }
    void printSummary() {
        // ds->printTree();
        auto recmgr = ds->debugGetRecMgr();
        recmgr->printStatus();
    }
    bool validateStructure() {
        return true;
    }
    void printObjectSizes() {
        std::cout<<"sizes: node="
                 <<(sizeof(int))
                 <<std::endl;
    }

#ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef struct tlx::node * NodePtrType;
        K minKey;
        K maxKey;
        
        NodeHandler(const K& _minKey, const K& _maxKey) {
            minKey = _minKey;
            maxKey = _maxKey;
        }
        
        class ChildIterator {
        private:
            bool child_slots[TLX_BTREE_MAX(DATA_STRUCTURE_T::btree_impl::inner_slotmax, 
                DATA_STRUCTURE_T::btree_impl::leaf_slotmax) + 1];
            NodePtrType node; // node being iterated over

        public:
            ChildIterator(NodePtrType _node) {
                node = _node;
                for (int i = 0; i <= node->slotuse; i++)
                    child_slots[i] = true;
            }
            bool hasNext() {
                if (node->is_leafnode())
                    return false;

                bool res = false;
                for (int i = 0; i <= node->slotuse; i++)
                    if (child_slots[i])
                        res = true;
                return res;
            }
            NodePtrType next() {
                if (node->is_leafnode())
                    setbench_error("ERROR: it is suspected that you are calling ChildIterator::next() without first verifying that it hasNext()");
                
                struct DATA_STRUCTURE_T::btree_impl::InnerNode* in = 
                    static_cast<struct DATA_STRUCTURE_T::btree_impl::InnerNode*>(node);
                for (int i = 0; i <= node->slotuse; i++)
                {
                    if (child_slots[i])
                    {
                        child_slots[i] = false;
                        return in->childid[i];
                    }
                }
                setbench_error("ERROR: it is suspected that you are calling ChildIterator::next() without first verifying that it hasNext()");
            }
        };
        
        bool isLeaf(NodePtrType node) {
            return node->is_leafnode();
        }
        size_t getNumChildren(NodePtrType node) {
            if (isLeaf(node)) return 0;
            return node->slotuse + 1;
        }
        size_t getNumKeys(NodePtrType node) {
            if (!node->is_leafnode()) return 0;
            return node->slotuse;
        }
        size_t getSumOfKeys(NodePtrType node) {
            int sum_keys = 0;

            if (node->is_leafnode()) {
                struct DATA_STRUCTURE_T::btree_impl::LeafNode* ln = 
                    static_cast<struct DATA_STRUCTURE_T::btree_impl::LeafNode*>(node);
                for (int i = 0; i < node->slotuse; i++) {
                    sum_keys += ln->key(i);
                }
            }
            return sum_keys;
        }
        ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }
        static size_t getSizeInBytes(NodePtrType node) { return sizeof(*node); }
    };

    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), ds->get_root(), false);
    }
#endif 
};

#endif