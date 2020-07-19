#pragma once

#include "pc_par_node.h"
#include <atomic>
#include <iostream>
#include <string>

const unsigned int LEFT = 0;
const unsigned int RIGHT = 1;
const unsigned int MAX_CHILDREN = 2;

#define bst	BST<skey_t, sval_t, RecMgr>

template <typename skey_t, typename sval_t, class RecMgr>
class BST {
private:
	Node* root;
	const unsigned int idx_id;
	const int NUM_THREADS;
    const skey_t KEY_MIN;
    const skey_t KEY_MAX;
    const sval_t NO_VALUE;
	int init[MAX_THREADS_POW2] = {0,};
	RecMgr* recmgr;

	void make_empty(Node* t);

	Node* find(const skey_t& key, Node*& parent);

	Node* create_node(const int& tid, const skey_t& key, const sval_t& value, unsigned int max_num_children);

	Node* create_node(const int& tid, const Node& node);

	Node* path_copy(const int& tid, Node* start);

public:
	BST(
		const int _NUM_THREADS, 
		const skey_t& _KEY_MIN, 
		const skey_t& _KEY_MAX, 
		const sval_t& _VALUE_RESERVED, 
		unsigned int id);

	virtual ~BST();

	void initThread(const int tid);

	void deinitThread(const int tid);

	RecMgr* debugGetRecMgr();

	Node* get_root();

	sval_t insert(const int tid, const skey_t& key, const sval_t& value);

	sval_t insert_wrapper(const int tid, const skey_t& key, const sval_t& value);

	sval_t remove(const int tid, const skey_t& key);

	sval_t remove_wrapper(const int tid, const skey_t& key);

	sval_t search(const int tid, const skey_t& key);

	sval_t search_wrapper(const int tid, const skey_t& key);
};

template <typename skey_t, typename sval_t>
thread_local Node* tl_root;
#define tl_root	tl_root<skey_t, sval_t>

template <typename skey_t, typename sval_t, class RecMgr>
void bst::make_empty(Node* t) {
	if (t == nullptr)
		return;

	make_empty(t->get_child(LEFT));
	make_empty(t->get_child(RIGHT));
	delete t;
}

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::find(
	const skey_t& key,
	Node*& parent)
{
	auto curr = root;

	while (curr != nullptr && (curr->key != key || curr->is_del()))
	{
		parent = curr;
		curr = (key < curr->key) ? curr->get_child(LEFT) : curr->get_child(RIGHT);
	}

	return curr;
}

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::create_node(const int& tid, const skey_t& key, const sval_t& value, unsigned int max_num_children)
{
	Node* result = (Node*)recmgr->template allocate<Node>(tid);
	result->key = key;
	result->value = value;
	result->children.resize(max_num_children, nullptr);
	result->flags = 0;
	return result;
}

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::create_node(const int& tid, const Node& node)
{
	Node* result = (Node*)recmgr->template allocate<Node>(tid);
	result->key = node.key;
	result->value = node.value;
	result->children = node.children;
	result->flags = node.flags;
	return result;
}

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::path_copy(const int& tid, Node* start)
{
	Node* duplication = create_node(tid, *start);
	duplications->insert({ start, duplication });

	Node* current = start;
	Node* current_dup = duplication;
	Node* parent;
	Node* parent_dup;

	bool reached_root = false;
	std::pair<Node*, unsigned int> pair;
	while (!(reached_root = (node_parent_map->find(current) == node_parent_map->end())) &&
		(pair = node_parent_map->at(current), duplications->find(pair.first) == duplications->end()))
	{
		parent = pair.first;
		auto child_idx = pair.second;
		parent_dup = create_node(tid, *parent);
		parent_dup->children[child_idx] = current_dup;
		duplications->insert({ parent, parent_dup });

		current = parent;
		current_dup = parent_dup;
	}

	if (reached_root)
	{
		new_root = current_dup;
	}
	else // reached a duplicated parent
	{
		auto parent = node_parent_map->at(current).first;
		auto child_idx = node_parent_map->at(current).second;
		auto to_update = duplications->at(parent);
		to_update->children[child_idx] = current_dup;
	}

	pc_happened = true;
	return duplication;
}

template <typename skey_t, typename sval_t, class RecMgr>
bst::BST(
	const int _NUM_THREADS, 
	const skey_t& _KEY_MIN, 
	const skey_t& _KEY_MAX, 
	const sval_t& _VALUE_RESERVED, 
	unsigned int id) : 
	idx_id(id), 
	NUM_THREADS(_NUM_THREADS), 
	KEY_MIN(_KEY_MIN), 
	KEY_MAX(_KEY_MAX), 
	NO_VALUE(_VALUE_RESERVED), 
	root(nullptr),
	recmgr(new RecMgr(NUM_THREADS))
{
	const int tid = 0;
    initThread(tid);
	recmgr->endOp(tid);
}

template <typename skey_t, typename sval_t, class RecMgr>
bst::~BST() {
	make_empty(root);
	delete recmgr;
}

template <typename skey_t, typename sval_t, class RecMgr>
void bst::initThread(const int tid)
{
	if (init[tid]) return;
    else init[tid] = !init[tid];
    recmgr->initThread(tid);
}

template <typename skey_t, typename sval_t, class RecMgr>
void bst::deinitThread(const int tid)
{
	if (!init[tid]) return;
    else init[tid] = !init[tid];
    recmgr->deinitThread(tid);
}

template <typename skey_t, typename sval_t, class RecMgr>
RecMgr* bst::debugGetRecMgr()
{
	return recmgr;
}

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::get_root()
{
	return root;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::insert(const int tid, const skey_t& key, const sval_t& value)
{
	if (orig_root == nullptr)
	{
		pc_happened = true;
		new_root = create_node(tid, key, value, MAX_CHILDREN);
		return NO_VALUE;
	}

	Node* parent = nullptr;
	auto found = find(key, parent);

	if (found != nullptr || parent == nullptr)
		return value;

	if (key < parent->get_key())
	{
		auto parent_dup = path_copy(tid, parent);
		parent_dup->set_child(LEFT, create_node(tid, key, value, MAX_CHILDREN));
		// parent->set_child(LEFT, create_node(tid, key, value, MAX_CHILDREN));
	}
	else
	{
		auto parent_dup = path_copy(tid, parent);
		parent_dup->set_child(RIGHT, create_node(tid, key, value, MAX_CHILDREN));
		// parent->set_child(RIGHT, create_node(tid, key, value, MAX_CHILDREN));
	}

	return NO_VALUE;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::insert_wrapper(const int tid, const skey_t& key, const sval_t& value)
{
	sval_t insertion_res;

	while (1)
	{
		auto guard = recmgr->getGuard(tid);
		Node::open(root);
		insertion_res = insert(tid, key, value);
		if (Node::close(root))
		{
			for (auto& d : *duplications)
			{
				recmgr->retire(tid, d.first);
			}
			return insertion_res;
		}
		else
		{
			for (auto& d : *duplications)
			{
				recmgr->deallocate(tid, d.second);
			}
		}
	}

	return insertion_res;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::remove(const int tid, const skey_t& key)
{
	Node* parent = nullptr;
	auto found = find(key, parent);

	if (found == nullptr)
		return NO_VALUE;

	sval_t res = found->get_value();
	if (found->children[LEFT] == nullptr && found->children[RIGHT] == nullptr)
	{
		if (parent == nullptr)
		{
			found->delete_node();
		}
		else if (parent->get_key() <= found->get_key())
		{
			auto parent_dup = path_copy(tid, parent);
			parent_dup->set_child(RIGHT, nullptr);
		}
		else
		{
			auto parent_dup = path_copy(tid, parent);
			parent_dup->set_child(LEFT, nullptr);
		}
	}
	else
	{
		auto found_dup = path_copy(tid, found);
		found_dup->delete_node();
	}

	return res;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::remove_wrapper(const int tid, const skey_t& key)
{
	auto guard = recmgr->getGuard(tid);
	sval_t removal_res;

	do
	{
		Node::open(root);
		removal_res = remove(tid, key);
	} while (!Node::close(root));

	for (auto& d : *duplications)
		recmgr->retire(tid, d.first);

	return removal_res;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::search(const int tid, const skey_t& key) {
	auto guard = recmgr->getGuard(tid, true);
	auto curr = root;

	while (curr != nullptr && (curr->key != key || curr->is_del()))
	{
		curr = (key < curr->key) ? curr->children[LEFT] : curr->children[RIGHT];
	}

	if (curr != nullptr)
		return curr->value;
	else
		return NO_VALUE;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::search_wrapper(const int tid, const skey_t& key)
{
	return search(tid, key);
}