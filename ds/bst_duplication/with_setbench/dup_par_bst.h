#pragma once

#include "dup_par_node.h"
#include <iostream>

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

	dinfo* create_dinfo(Node*& dup, Node*& parent, unsigned int orig_idx);

	Node* dup_prologue(const int& tid, Node* orig);

	Node* dup_epilogue(const int& tid, Node* orig, Node* dup);

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

template <typename skey_t, typename sval_t, class RecMgr>
void bst::make_empty(Node* t) {
	if (t == nullptr)
		return;

	make_empty(t->get_child(LEFT));
	make_empty(t->get_child(RIGHT));
	delete t;
}

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::find(const skey_t& key, Node*& parent)
{
	auto curr = root;

	while (curr != nullptr && curr->key != key)
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
	pthread_spin_init(&result->dup_lock, PTHREAD_PROCESS_PRIVATE);
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
	pthread_spin_init(&result->dup_lock, PTHREAD_PROCESS_PRIVATE);
	return result;
}

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::dup_prologue(const int& tid, Node* orig)
{
	return create_node(tid, *orig);
}

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::dup_epilogue(const int& tid, Node* orig, Node* dup)
{
	Node* parent = nullptr;
	unsigned int child_idx = MAX_UINT;	
	// if (orig != orig_root)//if (node_parent_map->find(orig) != node_parent_map->end())
	// {
	// 	auto pair = node_parent_map->at(orig);
	// 	parent = pair.first;
	// 	child_idx = pair.second;
	// }
	// else
	// {
	// 	new_root = dup;
	// 	parent = nullptr;
	// }

	if (orig != orig_root)
	{
		for (auto it = path->rbegin(); it != path->rend(); ++it)
		{
			unsigned int ch_idx = 0;
			for (auto& ch : (*it)->children)
			{
				if (ch != nullptr && ch->key == orig->key)
				{
					parent = *it;
					child_idx = ch_idx;
					goto FOUND;
				}
				ch_idx++;
			}
		}
	}
	else
	{
		new_root = dup;
	}

FOUND:
	for (auto& d : *duplications)
	{
		if (parent != nullptr && d.first->key == dup->key)
		{
			d.second.dup->children[child_idx] = dup;
			continue;
		}

		unsigned int ch_idx = 0;
		for (auto& ch : dup->children)
		{
			if (ch != nullptr && d.first->key == ch->key)
				dup->children[ch_idx] = d.second.dup;
			ch_idx++;
		}
	}

	// if (parent != nullptr && 
	// 	duplications->find(parent) != duplications->end())
	// {
	// 	Node* parent_dup = duplications->at(parent).dup;
	// 	parent_dup->children[child_idx] = dup;
	// }

	// unsigned int ch_idx = 0;
	// for (auto& ch : dup->children)
	// {
	// 	if (ch != nullptr && 
	// 		duplications->find(ch) != duplications->end())
	// 	{
	// 		Node* child_dup = duplications->at(ch).dup;
	// 		dup->children[ch_idx] = child_dup;
	// 	}
	// 	ch_idx++;
	// }

	duplications->insert({ orig, {dup, parent, child_idx} });
	dup_happened = true;
	return dup;
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
	if (root == nullptr)
	{
		root = create_node(tid, key, value, MAX_CHILDREN);
		return NO_VALUE;
	}

	Node* parent = nullptr;
	auto found = find(key, parent);

	if (found != nullptr || parent == nullptr)
		return value;

	if (key < parent->get_key())
	{
		auto parent_dup = dup_prologue(tid, parent);
		// recmgr->deallocate(tid, parent_dup);
		parent_dup->set_child(LEFT, create_node(tid, key, value, MAX_CHILDREN));
		dup_epilogue(tid, parent, parent_dup);
		// parent->set_child(LEFT, create_node(tid, key, value, MAX_CHILDREN));
	}
	else
	{
		auto parent_dup = dup_prologue(tid, parent);
		// recmgr->deallocate(tid, parent_dup);
		parent_dup->set_child(RIGHT, create_node(tid, key, value, MAX_CHILDREN));
		dup_epilogue(tid, parent, parent_dup);
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
				recmgr->deallocate(tid, d.second.dup);
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
	if (found->get_child(LEFT) == nullptr && found->get_child(RIGHT) == nullptr)
	{
		if (parent == nullptr)
			found->delete_node();
		else if (parent->get_key() <= found->get_key())
		{
			auto parent_dup = dup_prologue(tid, parent);
			parent_dup->set_child(RIGHT, nullptr);
			dup_epilogue(tid, parent, parent_dup);
		}
		else
		{
			auto parent_dup = dup_prologue(tid, parent);
			parent_dup->set_child(LEFT, nullptr);
			dup_epilogue(tid, parent, parent_dup);
		}
	}
	else
	{
		auto found_dup = dup_prologue(tid, found);
		found_dup->delete_node();
		dup_epilogue(tid, found, found_dup);
	}

	return res;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::remove_wrapper(const int tid, const skey_t& key)
{
	sval_t removal_res;

	do
	{
		auto guard = recmgr->getGuard(tid);
		Node::open(root);
		removal_res = remove(tid, key);
	} while (!Node::close(root));

	return removal_res;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::search(const int tid, const skey_t& key) {
	auto guard = recmgr->getGuard(tid, true);
	auto curr = root;

	while (curr != nullptr && curr->key != key)
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