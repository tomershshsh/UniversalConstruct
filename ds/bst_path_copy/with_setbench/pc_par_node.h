#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>

const unsigned char DEL_MASK = 0x02;
static std::mutex g_mutex;

enum class node_field
{
	KEY,
	CHILD,
	DELETE
};

template <typename skey_t>
class node_t
{
private:
	skey_t key;
	unsigned char flags;
	std::vector<node_t<skey_t>*> children;

	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

	node_t<skey_t>* path_copy();
	
public:
	node_t(const skey_t& key, unsigned int max_num_children);
	node_t(const node_t<skey_t>& node);

	skey_t get_key();
	node_t<skey_t>* get_child(unsigned int child_idx);
	bool is_deleted();
	
	node_t<skey_t>* set_key(const skey_t& new_key);
	node_t<skey_t>* set_child(unsigned int child_idx, node_t<skey_t>* new_child);
	node_t<skey_t>* delete_node();

	static bool open(node_t<skey_t>*& root);
	static bool close(node_t<skey_t>*& root);
};

template<typename skey_t>
thread_local std::unordered_map<node_t<skey_t>*, node_t<skey_t>*> duplications;

template<typename skey_t>
thread_local std::unordered_map<node_t<skey_t>*, 
	std::pair<node_t<skey_t>*, unsigned int>> node_parent_map;

thread_local bool in_writing_function = false;
thread_local bool pc_happened = false;

template<typename skey_t>
thread_local node_t<skey_t>* orig_root;

template<typename skey_t>
thread_local node_t<skey_t>* new_root;

template<typename skey_t>
bool node_t<skey_t>::open(node_t<skey_t>*& root)
{
	duplications<skey_t>.clear();
	node_parent_map<skey_t>.clear();
	orig_root<skey_t> = root;
	in_writing_function = true;
	pc_happened = false;
	return true;
}

template<typename skey_t>
bool node_t<skey_t>::close(node_t<skey_t>*& root)
{	
	if (pc_happened)
	{
		std::lock_guard<std::mutex> lock(g_mutex);
		in_writing_function = false;
		if (root == orig_root<skey_t>)
		{
			root = new_root<skey_t>;
			return true;
		}

		return false;
	}
	else
	{
		return true;
	}
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::path_copy()
{
	node_t<skey_t>* duplication = new node_t<skey_t>(*this);
	duplications<skey_t>.insert({ this, duplication });

	node_t<skey_t>* current = this;
	node_t<skey_t>* current_dup = duplication;
	node_t<skey_t>* parent;
	node_t<skey_t>* parent_dup;

	bool reached_root = false;
	while (!(reached_root = (node_parent_map<skey_t>.find(current) == node_parent_map<skey_t>.end())) &&
		duplications<skey_t>.find(node_parent_map<skey_t>[current].first) == duplications<skey_t>.end())
	{
		parent = node_parent_map<skey_t>[current].first;
		auto child_idx = node_parent_map<skey_t>[current].second;
		parent_dup = new node_t<skey_t>(*parent);
		parent_dup->children[child_idx] = current_dup;

		duplications<skey_t>.insert({ parent, parent_dup });

		current = parent;
		current_dup = parent_dup;
	}

	if (reached_root)
	{
		new_root<skey_t> = current_dup;
	}
	else // reached a duplicated parent
	{
		auto parent = node_parent_map<skey_t>[current].first;
		auto child_idx = node_parent_map<skey_t>[current].second;
		auto to_update = duplications<skey_t>[parent];
		to_update->children[child_idx] = current_dup;
	}

	pc_happened = true;
	return duplication;
}

template<typename skey_t>
node_t<skey_t>::node_t(const skey_t& key, unsigned int max_num_children) :
	key(key),
	children(max_num_children, nullptr),
	flags(0)
{}

template<typename skey_t>
node_t<skey_t>::node_t(const node_t<skey_t>& node) :
	key(node.key),
	children(node.children),
	flags(node.flags)
{}

template<typename skey_t>
skey_t node_t<skey_t>::get_key() 
{ 
	return key; 
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::get_child(unsigned int child_idx)
{
	if (child_idx >= children.size())
		return nullptr;

	node_t<skey_t>* child = children.at(child_idx);
	if (in_writing_function && child != nullptr)
		node_parent_map<skey_t>.insert({ child, std::make_pair(this, child_idx) });

	return child;
}

template<typename skey_t>
bool node_t<skey_t>::is_deleted() 
{ 
	return is_del(); 
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::set_key(const skey_t& new_key)
{
	auto dup = path_copy();
	dup->key = new_key;
	return dup;
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::set_child(unsigned int child_idx, node_t<skey_t>* new_child)
{
	auto dup = path_copy();
	dup->children[child_idx] = new_child;
	return dup;
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::delete_node()
{
	auto dup = path_copy();
	dup->set_del();
	return dup;
}