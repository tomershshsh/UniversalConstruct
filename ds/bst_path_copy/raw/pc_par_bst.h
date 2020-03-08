#pragma once

#include "pc_par_node.h"
#include <atomic>

const unsigned int LEFT = 0;
const unsigned int RIGHT = 1;
const unsigned int MAX_CHILDREN = 2;

template <typename skey_t>
class BST {
private:
	node_t<skey_t>* root;

	void make_empty(node_t<skey_t>* t);
	
	node_t<skey_t>* find(
		node_t<skey_t>* start,
		const skey_t& key,
		node_t<skey_t>*& parent);

public:
	BST();

	virtual ~BST();

	bool insert(const skey_t& key);

	bool insert_wrapper(const skey_t& key);

	bool remove(const skey_t& key);

	bool remove_wrapper(const skey_t& key);

	bool search(const skey_t& key);

	bool search_wrapper(const skey_t& key);
};

template<typename skey_t>
thread_local node_t<skey_t>* tl_root;

template <typename skey_t>
void BST<skey_t>::make_empty(node_t<skey_t>* t) {
	if (t == nullptr)
		return;

	make_empty(t->get_child(LEFT));
	make_empty(t->get_child(RIGHT));
	delete t;
}

template <typename skey_t>
node_t<skey_t>* BST<skey_t>::find(
	node_t<skey_t>* start,
	const skey_t& key,
	node_t<skey_t>*& parent)
{
	auto curr = start;

	while (curr != nullptr)
	{
		if (key < curr->get_key())
		{
			parent = curr;
			curr = curr->get_child(LEFT);
		}
		else if (key > curr->get_key())
		{
			parent = curr;
			curr = curr->get_child(RIGHT);
		}
		else
		{
			if (!curr->is_deleted())
			{
				return curr;
			}
			else
			{
				parent = curr;
				if (curr->get_child(RIGHT))
					curr = curr->get_child(RIGHT);
				else
					curr = curr->get_child(LEFT);
			}
		}
	}

	return nullptr;
}

template <typename skey_t>
BST<skey_t>::BST() :
	root(nullptr) {};

template <typename skey_t>
BST<skey_t>::~BST() {
	make_empty(root);
}

template <typename skey_t>
bool BST<skey_t>::insert(const skey_t& key)
{
	if (tl_root<skey_t> == nullptr)
	{
		tl_root<skey_t> = new node_t<skey_t>(key, MAX_CHILDREN);
		return true;
	}

	node_t<skey_t>* parent = nullptr;
	auto found = find(tl_root<skey_t>, key, parent);

	if (found != nullptr || parent == nullptr)
		return false;

	if (key < parent->get_key())
		parent->set_child(LEFT, new node_t<skey_t>(key, MAX_CHILDREN));
	else
		parent->set_child(RIGHT, new node_t<skey_t>(key, MAX_CHILDREN));

	return true;
}

template <typename skey_t>
bool BST<skey_t>::insert_wrapper(const skey_t& key)
{
	bool insertion_res;

	do
	{
		node_t<skey_t>::open(root);
		tl_root<skey_t> = root;
		insertion_res = insert(key);
	} while (!node_t<skey_t>::close(root));

	return insertion_res;
}

template <typename skey_t>
bool BST<skey_t>::remove(const skey_t& key)
{
	node_t<skey_t>* parent = nullptr;
	auto found = find(tl_root<skey_t>, key, parent);

	if (found == nullptr)
		return false;

	if (found->get_child(LEFT) == nullptr && found->get_child(RIGHT) == nullptr)
	{
		if (parent == nullptr)
			found->delete_node();
		else if (parent->get_key() <= found->get_key())
			parent->set_child(RIGHT, nullptr);
		else
			parent->set_child(LEFT, nullptr);
	}
	else
	{
		found->delete_node();
	}

	return true;
}

template <typename skey_t>
bool BST<skey_t>::remove_wrapper(const skey_t& key)
{
	bool removal_res;

	do
	{
		node_t<skey_t>::open(root);
		tl_root<skey_t> = root;
		removal_res = remove(key);
	} while (!node_t<skey_t>::close(root));

	return removal_res;
}

template <typename skey_t>
bool BST<skey_t>::search(const skey_t& key) {
	node_t<skey_t>* parent = nullptr;
	return (find(tl_root<skey_t>, key, parent) != nullptr);
}

template <typename skey_t>
bool BST<skey_t>::search_wrapper(const skey_t& key)
{
	tl_root<skey_t> = root;
	return search(key);
}