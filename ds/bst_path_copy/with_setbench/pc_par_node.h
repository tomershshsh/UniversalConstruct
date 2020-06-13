#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include "record_manager.h"

#define Node node_t<skey_t, sval_t>

const unsigned char DEL_MASK = 0x02;
static std::mutex g_mutex;

template <typename skey_t, typename sval_t>
class node_t
{
public:
// private:
	skey_t key;
	sval_t value;
	unsigned char flags;
	std::vector<Node*> children;

	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

	skey_t get_key();
	sval_t get_value();
	Node* get_child(unsigned int child_idx);
	bool is_deleted();
	
	Node* set_key(const skey_t& new_key);
	Node* set_child(unsigned int child_idx, Node* new_child);
	Node* delete_node();

	static bool open(Node*& root);
	static bool close(Node*& root);
};

template <typename skey_t, typename sval_t>
thread_local std::unordered_map<Node*, Node*>* duplications = nullptr;
#define duplications	duplications<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local std::unordered_map<Node*, 
	std::pair<Node*, unsigned int>>* node_parent_map = nullptr;
#define node_parent_map	node_parent_map<skey_t, sval_t>

thread_local bool in_writing_function = false;
thread_local bool pc_happened = false;

template <typename skey_t, typename sval_t>
thread_local Node* orig_root;
#define orig_root	orig_root<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local Node* new_root;
#define	new_root	new_root<skey_t, sval_t>

template <typename skey_t, typename sval_t>
bool Node::open(Node*& root)
{
	if (node_parent_map)
		node_parent_map->clear();
	else
		node_parent_map = new std::unordered_map<Node*, std::pair<Node*, unsigned int>>();

	if (duplications)
		duplications->clear();
	else
		duplications = new std::unordered_map<Node*, Node*>();
		
	orig_root = root;
	in_writing_function = true;
	pc_happened = false;
	return true;
}

template <typename skey_t, typename sval_t>
bool Node::close(Node*& root)
{	
	in_writing_function = false;

	if (pc_happened)
	{
		std::lock_guard<std::mutex> lock(g_mutex);
		if (root == orig_root)
		{
			root = new_root;
			return true;
		}

		return false;
	}
	else
	{
		return true;
	}
}

template <typename skey_t, typename sval_t>
skey_t Node::get_key() 
{ 
	return key; 
}

template <typename skey_t, typename sval_t>
sval_t Node::get_value()
{
	return value;
}

template <typename skey_t, typename sval_t>
Node* Node::get_child(unsigned int child_idx)
{
	if (child_idx >= children.size())
		return nullptr;

	Node* child = children.at(child_idx);
	if (in_writing_function && child != nullptr)
		node_parent_map->insert({ child, std::make_pair(this, child_idx) });

	return child;
}

template <typename skey_t, typename sval_t>
bool Node::is_deleted() 
{ 
	return is_del(); 
}

template <typename skey_t, typename sval_t>
Node* Node::set_key(const skey_t& new_key)
{
	this->key = new_key;
	return this;
}

template <typename skey_t, typename sval_t>
Node* Node::set_child(unsigned int child_idx, Node* new_child)
{
	this->children[child_idx] = new_child;
	return this;
}

template <typename skey_t, typename sval_t>
Node* Node::delete_node()
{
	this->set_del();
	return this;
}