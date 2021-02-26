#pragma once

#include "rb_node.h"

#define rb_tree	rb_tree<skey_t, sval_t, RecMgr>

template <typename skey_t, typename sval_t, class RecMgr>
class rb_tree {
private:
	rb_node * root;

	const int NUM_THREADS;
    const skey_t KEY_MIN;
    const skey_t KEY_MAX;
    const sval_t NO_VALUE;
	int init[MAX_THREADS_POW2] = {0,};
	RecMgr* recmgr;

	rb_node * find(const skey_t & key)
	{
		auto curr = root;

		while (curr != nullptr && curr->key != key)
		{
			curr = (key < curr->key) ? curr->_M_left : curr->_M_right;
		}
		
		return curr;
	}

	Node* create_node(const int & tid, const skey_t & key, const sval_t & value)
	{
		rb_node * result = (rb_node *)recmgr->template allocate<Node>(tid);
		result->_M_key = key;
		result->_M_value = value;
		result->_M_left = nullptr;
		result->_M_right = nullptr;
		result->_M_color = rb_color::_S_red;
		return result;
	}

	Node* create_node(const int & tid, const rb_node * node)
	{
		rb_node * result = (rb_node *)recmgr->template allocate<Node>(tid);
		result->_M_key = node->_M_key;
		result->_M_value = node->_M_value;
		result->_M_left = node->_M_left;
		result->_M_right = node->_M_right;
		result->_M_color = node->_M_color;
		return result;
	}

public:
	rb_node(
		const int _NUM_THREADS, 
		const skey_t& _KEY_MIN, 
		const skey_t& _KEY_MAX, 
		const sval_t& _VALUE_RESERVED, 
		unsigned int id): 
		NUM_THREADS(_NUM_THREADS), 
		KEY_MIN(_KEY_MIN), 
		KEY_MAX(_KEY_MAX), 
		NO_VALUE(_VALUE_RESERVED), 
		root(nullptr),
		recmgr(new RecMgr(NUM_THREADS))
	{
		const int & tid = 0;
		initThread(tid);
		recmgr->endOp(tid);
	}

	virtual ~rb_node()
	{
		make_empty(root);
		delete recmgr;
	}

	void initThread(const int & tid)
	{
		if (init[tid]) return;
		else init[tid] = !init[tid];
		recmgr->initThread(tid);
	}

	void deinitThread(const int & tid)
	{
		if (!init[tid]) return;
		else init[tid] = !init[tid];
		recmgr->deinitThread(tid);
	}

	void make_empty(rb_node * t) 
	{
		if (t == nullptr)
			return;

		make_empty(t->get_child(LEFT));
		make_empty(t->get_child(RIGHT));
		delete t;
	}

	RecMgr * debugGetRecMgr();

	rb_node * get_root();

	sval_t insert(const int & tid, const skey_t & key, const sval_t & value);

	sval_t insert_wrapper(const int & tid, const skey_t & key, const sval_t & value);

	sval_t remove(const int & tid, const skey_t& key);

	sval_t remove_wrapper(const int & tid, const skey_t& key);

	sval_t search(const int & tid, const skey_t& key);

	sval_t search_wrapper(const int & tid, const skey_t& key);
};

template <typename skey_t, typename sval_t, class RecMgr>
Node* bst::find(const skey_t& key, Node*& parent)
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
sval_t bst::insert(const int & tid, const skey_t& key, const sval_t& value)
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
		parent->set_child(LEFT, create_node(tid, key, value, MAX_CHILDREN));
	}
	else
	{
		parent->set_child(RIGHT, create_node(tid, key, value, MAX_CHILDREN));
	}

	return NO_VALUE;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::insert_wrapper(const int & tid, const skey_t& key, const sval_t& value)
{
	sval_t insertion_res;

	while (1)
	{
		auto guard = recmgr->getGuard(tid);
		Node::open(root);
		insertion_res = insert(tid, key, value);
		if (Node::close(root))
		{
			return insertion_res;
		}
	}

	return insertion_res;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::remove(const int & tid, const skey_t& key)
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
			parent->set_child(RIGHT, nullptr);
		}
		else
		{
			parent->set_child(LEFT, nullptr);
		}
	}
	else
	{
		found->delete_node();
	}

	return res;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::remove_wrapper(const int & tid, const skey_t& key)
{
	sval_t removal_res;

	while (1)
	{
		auto guard = recmgr->getGuard(tid);
		Node::open(root);
		removal_res = remove(tid, key);
		if (Node::close(root))
		{
			return removal_res;
		}
	}

	return removal_res;
}

template <typename skey_t, typename sval_t, class RecMgr>
sval_t bst::search(const int & tid, const skey_t& key) {
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
sval_t bst::search_wrapper(const int & tid, const skey_t& key)
{
	return search(tid, key);
}