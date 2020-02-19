#pragma once

#include "dup_par_node.h"

const unsigned int LEFT = 0;
const unsigned int RIGHT = 1;
const unsigned int MAX_CHILDREN = 2;

template <typename skey_t>
class BST {
private:
	using node = node_t<skey_t>;

	node* root;

	void make_empty(node* t) {
		if (t == nullptr)
			return;

		make_empty(t->get_child(LEFT));
		make_empty(t->get_child(RIGHT));
		delete t;
	}

	node* find(
		node* start,
		const skey_t& key,
		node*& parent)
	{
		auto curr = start;

		while (curr != nullptr)
		{
			parent = curr;
			if (key < curr->get_key())
				curr = curr->get_child(LEFT);
			else if (key > curr->get_key())
				curr = curr->get_child(RIGHT);
			else
				return curr;
		}

		return nullptr;
	}

public:
	BST() :
		root(nullptr) {};

	virtual ~BST() {
		make_empty(root);
	}

	bool insert(const skey_t& key)
	{
		std::map<node*, node*> dups; // 

		if (root == nullptr)
		{
			root = new node(key, MAX_CHILDREN);
			return true;
		}

		node* parent = nullptr;
		auto found = find(root, key, parent);

		if (found != nullptr || parent == nullptr)
			return false;

		node* res;
		if (key < parent->get_key())
			res = parent->set_child(LEFT, new node(key, MAX_CHILDREN), dups);
		else
			res = parent->set_child(RIGHT, new node(key, MAX_CHILDREN), dups);

		node_t<skey_t>::closure({ parent }, dups);
		return true;
	}

	bool search(const skey_t& key) {
		node_t<skey_t>* parent = nullptr;
		return (find(root, key, parent) != nullptr);
	}
};
