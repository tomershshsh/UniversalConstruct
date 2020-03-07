#pragma once

#include "pc_par_node.h"
#include <atomic>

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

public:
	BST() :
		root(nullptr) {};

	virtual ~BST() {
		make_empty(root);
	}

	bool insert(const skey_t& key)
	{
		if (root == nullptr)
		{
			root = new node(key, MAX_CHILDREN);
			return true;
		}

		node* parent = nullptr;
		auto found = find(root, key, parent);

		if (found != nullptr || parent == nullptr)
			return false;

		if (key < parent->get_key())
			parent->set_child(LEFT, new node(key, MAX_CHILDREN));
		else
			parent->set_child(RIGHT, new node(key, MAX_CHILDREN));

		return true;
	}

	bool insert_wrapper(const skey_t& key)
	{
		bool insertion_res;

		do
		{
			node_t<skey_t>::open(root);
			insertion_res = insert(key);
		} while (!node_t<skey_t>::close(root));

		return insertion_res;
	}

	bool remove(const skey_t& key)
	{
		node_t<skey_t>* parent = nullptr;
		auto found = find(root, key, parent);

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

	bool remove_wrapper(const skey_t& key)
	{
		bool removal_res;

		do
		{
			node_t<skey_t>::open(root);
			removal_res = remove(key);
		} while (!node_t<skey_t>::close(root));

		return removal_res;
	}

	bool search(const skey_t& key) {
		node* parent = nullptr;
		return (find(root, key, parent) != nullptr);
	}
};
