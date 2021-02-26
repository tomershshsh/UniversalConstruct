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
#include "rb_tree.h"

#define RECORD_MANAGER_T record_manager<Reclaim, Alloc, Pool, rb_node<K,V>>
#define DATA_STRUCTURE_T rb_tree<K, V, RECORD_MANAGER_T>

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
        return ds->rb_dup_insert(tid, key, val);
    }
    V erase(const int tid, const K& key) {
        return ds->rb_dup_delete(tid, key);
    }
    V find(const int tid, const K& key) {
        return ds->rb_contains(tid, key);
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
                 <<(sizeof(rb_node<K,V>))
                 <<std::endl;
    }

#ifdef USE_TREE_STATS
    class NodeHandler {
    public:
        typedef rb_node<K,V> * NodePtrType;
        K minKey;
        K maxKey;
        
        NodeHandler(const K& _minKey, const K& _maxKey) {
            minKey = _minKey;
            maxKey = _maxKey;
        }
        
        class ChildIterator {
        private:
            bool leftDone;
            bool rightDone;
            NodePtrType node; // node being iterated over
        public:
            ChildIterator(NodePtrType _node) {
                node = _node;
                leftDone = (node->get_child(LEFT) == NULL);
                rightDone = (node->get_child(RIGHT) == NULL);
            }
            bool hasNext() {
                return !(leftDone && rightDone);
            }
            NodePtrType next() {
                if (!leftDone) {
                    leftDone = true;
                    return node->get_child(LEFT);
                }
                if (!rightDone) {
                    rightDone = true;
                    return node->get_child(RIGHT);
                }
                setbench_error("ERROR: it is suspected that you are calling ChildIterator::next() without first verifying that it hasNext()");
            }
        };
        
        bool isLeaf(NodePtrType node) {
            return (node->get_child(LEFT) == NULL && node->get_child(RIGHT) == NULL);
        }
        size_t getNumChildren(NodePtrType node) {
            if (isLeaf(node)) return 0;
            return (node->get_child(LEFT) != NULL) + (node->get_child(RIGHT) != NULL);
        }
        size_t getNumKeys(NodePtrType node) {
            if (node == NULL) return 0;
            if (node->get_key() == minKey || node->get_key() == maxKey) return 0;
            return 1;
        }
        size_t getSumOfKeys(NodePtrType node) {
            if (getNumKeys(node) == 0) return 0;
            return (size_t) node->get_key();
        }
        ChildIterator getChildIterator(NodePtrType node) {
            return ChildIterator(node);
        }
        static size_t getSizeInBytes(NodePtrType node) { 
            return sizeof(*node); 
        }
    };
    TreeStats<NodeHandler> * createTreeStats(const K& _minKey, const K& _maxKey) {
        return new TreeStats<NodeHandler>(new NodeHandler(_minKey, _maxKey), ds->get_root(), false);
    }
#endif
};

#endif
