#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include "record_manager.h"

#define Node node_t<skey_t, sval_t>

const unsigned char DUP_MASK = 0x01;
const unsigned char DEL_MASK = 0x02;
const unsigned int MAX_UINT = std::numeric_limits<unsigned int>::max();
static std::mutex g_mutex;

enum class node_field
{
	KEY,
	CHILD,
	DELETE
};

struct write_params_t
{
	node_field field_indicator;
	unsigned int specifier;
	void* replacement;
};

template <typename skey_t, typename sval_t>
class node_t
{
public:
// private:
	skey_t key;
	sval_t value;
	unsigned char flags;
	// std::vector<Node*> children; 
	Node* left;
	Node* right;

	inline bool is_dup() { return (flags & DUP_MASK) == DUP_MASK; }
	inline void set_dup() { flags ^= DUP_MASK; }
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

template<typename skey_t, typename sval_t>
struct duplication_info_t
{
	Node* dup;
	Node* orig_parent;
	unsigned int orig_idx;
};

template <typename skey_t, typename sval_t>
bool Node::open(Node*& root)
{
	return true;
}

template<typename skey_t, typename sval_t>
bool Node::close(Node*& root)
{
	return true;
}

template<typename skey_t, typename sval_t>
skey_t Node::get_key() 
{
	return key; 
}

template<typename skey_t, typename sval_t>
sval_t Node::get_value()
{
	return value;
}

template<typename skey_t, typename sval_t>
Node* Node::get_child(unsigned int child_idx)
{
	// if (child_idx >= children.size())
	// 	return nullptr;

	// Node* child = children.at(child_idx);
	// return child;

	if (child_idx == 0)
		return left;
	else
		return right;
}

template<typename skey_t, typename sval_t>
bool Node::is_deleted()
{ 
	return is_del(); 
}

template<typename skey_t, typename sval_t>
Node* Node::set_key(const skey_t& new_key)
{
	this->key = new_key;
	return this;
}

template<typename skey_t, typename sval_t>
Node* Node::set_child(unsigned int child_idx, Node* new_child)
{
	// this->children[child_idx] = new_child;
	// return this;

	if (child_idx == 0)
		left = new_child;
	else
		right = new_child;

	return this;
}

template<typename skey_t, typename sval_t>
Node* Node::delete_node()
{
	this->set_del();
	return this;
}