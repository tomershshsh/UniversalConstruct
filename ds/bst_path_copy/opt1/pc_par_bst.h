#pragma once

#include "pc_par_node.h"

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
		node*& parent,
		std::vector<node*>& path)
	{
		auto curr = start;

		while (curr != nullptr)
		{
			if (key < curr->get_key())
			{
				parent = curr;
				path.push_back(parent);
				curr = curr->get_child(LEFT);
			}
			else if (key > curr->get_key())
			{
				parent = curr;
				path.push_back(parent);
				curr = curr->get_child(RIGHT);
			}
			else
			{
				if (!curr->is_deleted())
				{
					path.push_back(curr);
					return curr;
				}
				else
				{
					if (curr->get_child(RIGHT))
					{
						parent = curr;
						path.push_back(parent);
						curr = curr->get_child(RIGHT);
					}
					else
					{
						parent = curr;
						path.push_back(parent);
						curr = curr->get_child(LEFT);
					}
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
		std::map<node*, node*> dups; // 

		if (root == nullptr)
		{
			root = new node(key, MAX_CHILDREN);
			return true;
		}

		node* parent = nullptr;
		std::vector<node*> path;
		auto found = find(root, key, parent, path);

		if (found != nullptr || parent == nullptr)
			return false;

		node* new_root;
		if (key < parent->get_key())
			parent->set_child(LEFT, new node(key, MAX_CHILDREN), path, dups, new_root);
		else
			parent->set_child(RIGHT, new node(key, MAX_CHILDREN), path, dups, new_root);

		root = new_root;
		return true;
	}

	bool remove(const skey_t& key)
	{
		std::map<node*, node*> dups; // 

		node_t<skey_t>* parent = nullptr;
		std::vector<node*> path;
		auto found = find(root, key, parent, path);

		if (found == nullptr)
			return false;

		node* new_root;
		if (found->get_child(LEFT) == nullptr && found->get_child(RIGHT) == nullptr)
		{
			if (parent == nullptr)
				found->delete_node(path, dups, new_root);
			else if (parent->get_key() <= found->get_key())
				parent->set_child(RIGHT, nullptr, path, dups, new_root);
			else
				parent->set_child(LEFT, nullptr, path, dups, new_root);
		}
		else
		{
			found->delete_node(path, dups, new_root);
		}

		root = new_root;
		return true;
	}

	bool search(const skey_t& key) {
		node* parent = nullptr;
		std::vector<node*> path;
		return (find(root, key, parent, path) != nullptr);
	}
};
