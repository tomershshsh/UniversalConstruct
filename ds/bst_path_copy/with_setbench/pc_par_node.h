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

	// Node* path_copy(const int& tid);
	
// public:
	// node_t(const skey_t& key, const sval_t& value, unsigned int max_num_children);
	// node_t(const Node& node);

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

// template <typename skey_t, typename sval_t, class RecMgr>
// Node* Node::path_copy(const int& tid)
// {
// 	// node_t<skey_t>* duplication = new node_t<skey_t>(*this);
// 	Node* duplication = (Node*)recmgr<RecMgr>->template allocate<Node>(tid, *this);
// 	duplications.insert({ this, duplication });
// 	recmgr<RecMgr>->retire(tid, this);
//
// 	Node* current = this;
// 	Node* current_dup = duplication;
// 	Node* parent;
// 	Node* parent_dup;
//
// 	bool reached_root = false;
// 	while (!(reached_root = (node_parent_map.find(current) == node_parent_map.end())) &&
// 		duplications.find(node_parent_map[current].first) == duplications.end())
// 	{
// 		parent = node_parent_map[current].first;
// 		auto child_idx = node_parent_map[current].second;
// 		// parent_dup = new node_t<skey_t>(*parent);
// 		parent_dup = (Node*)recmgr<RecMgr>->template allocate<Node>(tid, *parent);
// 		parent_dup->children[child_idx] = current_dup;
//
// 		duplications.insert({ parent, parent_dup });
// 		recmgr<RecMgr>->retire(tid, parent);
//
// 		current = parent;
// 		current_dup = parent_dup;
// 	}
//
// 	if (reached_root)
// 	{
// 		new_root = current_dup;
// 	}
// 	else // reached a duplicated parent
// 	{
// 		auto parent = node_parent_map[current].first;
// 		auto child_idx = node_parent_map[current].second;
// 		auto to_update = duplications[parent];
// 		to_update->children[child_idx] = current_dup;
// 	}
//
// 	pc_happened = true;
// 	return duplication;
// }

// template <typename skey_t, typename sval_t>
// Node::node_t(const skey_t& key, const sval_t& value, unsigned int max_num_children) :
// 	key(key),
// 	value(value),
// 	children(max_num_children, nullptr),
// 	flags(0)
// {}

// template <typename skey_t, typename sval_t>
// Node::node_t(const Node& node) :
// 	key(node.key),
// 	value(node.value),
// 	children(node.children),
// 	flags(node.flags)
// {}

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