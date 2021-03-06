#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include <pthread.h>
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
	skey_t key;
	sval_t value;
	unsigned char flags;
	std::vector<Node*> children; 
	pthread_spinlock_t dup_lock;

	inline bool is_dup() { return (flags & DUP_MASK) == DUP_MASK; }
	inline void set_dup() { flags ^= DUP_MASK; }
	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }
	
	static bool lock_duplications();
	static void unlock_duplications(bool all);

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
class duplication_info_t
{
public:
	Node* orig;
	Node* dup;
	Node* orig_parent;
	unsigned int orig_idx;
};

#define dinfo duplication_info_t<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local std::vector<dinfo>* duplications = nullptr;
#define duplications	duplications<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local std::vector<Node*>* path = nullptr;
#define path path<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local std::vector<std::pair<Node*, bool>>* locked = nullptr;
#define locked locked<skey_t, sval_t>

thread_local bool in_writing_function = false;
thread_local bool dup_happened = false;

template <typename skey_t, typename sval_t>
thread_local Node* orig_root;
#define orig_root	orig_root<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local Node* new_root;
#define	new_root	new_root<skey_t, sval_t>

template <typename skey_t, typename sval_t>
bool Node::open(Node*& root)
{
	if (path)
		path->clear();
	else
		path = new std::vector<Node*>();

	if (duplications)
		duplications->clear();
	else
		duplications = new std::vector<dinfo>();

	if (locked)
		locked->clear();
	else
		locked = new std::vector<std::pair<Node*, bool>>();

	orig_root = root;
	new_root = nullptr;
	in_writing_function = true;
	dup_happened = false;
	return true;
}

template <typename skey_t, typename sval_t>
bool Node::lock_duplications()
{
	return true;
}

template<typename skey_t, typename sval_t>
void Node::unlock_duplications(bool all)
{
	for (auto& l : *locked)
	{
		if (all || l.second)
			pthread_spin_unlock(&l.first->dup_lock);
	}
}

template<typename skey_t, typename sval_t>
bool Node::close(Node*& root)
{
	in_writing_function = false;
	if (!dup_happened)
		return true;

	if (!lock_duplications())
		return false;

	/* check that parent-child relations are as expected */
	for (auto& d: *duplications)
	{
		auto orig = d.orig;
		auto orig_parent = d.orig_parent;
		auto orig_idx = d.orig_idx;

		if (orig_parent != nullptr && orig_parent->children[orig_idx] != orig) {
			unlock_duplications(true);
			// pthread_spin_unlock(&orig->dup_lock);
			return false;
		}
	}

	/* connect duplications to tree */
	for (auto& d : *duplications)
	{
		auto orig = d.orig;
		auto dup = d.dup;
		auto orig_parent = d.orig_parent;
		auto orig_idx = d.orig_idx;

		if (orig_parent != nullptr)
		{
			if (!__atomic_compare_exchange_n(
					&orig_parent->children[orig_idx], 
					&orig, 
					dup, 
					true, 
					__ATOMIC_RELAXED, 
					__ATOMIC_RELAXED))
			{
				unlock_duplications(true);
				// pthread_spin_unlock(&orig->dup_lock);
				return false;
			}
		}
		else
		{
			if (orig_root != orig || new_root != dup || !__atomic_compare_exchange_n(
					&root, 
					&orig_root, 
					new_root, 
					true, 
					__ATOMIC_RELAXED, 
					__ATOMIC_RELAXED))
			{
				unlock_duplications(true);
				// pthread_spin_unlock(&orig->dup_lock);
				return false;
			}
		}
	}

	unlock_duplications(false);
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
	if (child_idx >= children.size())
		return nullptr;

	Node* child = children.at(child_idx);
	if (in_writing_function && child != nullptr)
	{
		path->push_back(this);
	}

	return child;
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
	this->children[child_idx] = new_child;
	return this;
}

template<typename skey_t, typename sval_t>
Node* Node::delete_node()
{
	this->set_del();
	return this;
}