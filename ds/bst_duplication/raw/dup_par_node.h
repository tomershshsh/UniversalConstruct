#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>

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

template <typename skey_t>
class node_t
{
private:
	skey_t key;
	unsigned char flags;
	std::vector<node_t<skey_t>*> children; 

	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }
	
	void connect_dup_to_anc_dup(node_t<skey_t>* duplication);
	node_t<skey_t>* write(write_params_t&& params);

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
struct duplication_info_t
{
	node_t<skey_t>* dup;
	node_t<skey_t>* orig_parent;
	unsigned int orig_idx;
};

template<typename skey_t>
thread_local std::unordered_map<node_t<skey_t>*, 
	duplication_info_t<skey_t>> duplications;

template<typename skey_t>
thread_local std::unordered_map<node_t<skey_t>*, 
	std::pair<node_t<skey_t>*, unsigned int>> node_parent_map;

thread_local bool in_writing_function = false;
thread_local bool dup_happened = false;

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
	new_root<skey_t> = nullptr;
	in_writing_function = true;
	dup_happened = false;
	return true;
}

template<typename skey_t>
bool node_t<skey_t>::close(node_t<skey_t>*& root)
{
	if (dup_happened)
	{
		std::lock_guard<std::mutex> lock(g_mutex);
		in_writing_function = false;
		for (auto& d : duplications<skey_t>)
		{
			auto orig = d.first;
			auto dup = d.second.dup;
			auto orig_parent = d.second.orig_parent;
			auto orig_idx = d.second.orig_idx;

			if (orig_parent != nullptr)
			{
				if (orig_parent->children[orig_idx] == orig)
					orig_parent->children[orig_idx] = dup;
				else
					return false;
			}
			else
			{
				if (new_root<skey_t> != nullptr && root == orig_root<skey_t>)
					root = new_root<skey_t>;
				else
					return false;
			}
		}

		return true;
	}
	else
	{
		return true;
	}
}

template<typename skey_t>
void node_t<skey_t>::connect_dup_to_anc_dup(node_t<skey_t>* duplication)
{
	auto start = this;
	auto current = start;

	bool reached_root = false;
	while (!(reached_root = (node_parent_map<skey_t>.find(current) == node_parent_map<skey_t>.end())) &&
		duplications<skey_t>.find(node_parent_map<skey_t>[current].first) == duplications<skey_t>.end())
	{
		current = node_parent_map<skey_t>[current].first;
	}

	if (!reached_root)
	{ /* there is a duplication of a predecessor */
		node_t<skey_t>* end = current;
		node_t<skey_t>* current = this;
		node_t<skey_t>* current_dup = duplication;
		node_t<skey_t>* parent;
		node_t<skey_t>* parent_dup;

		while (current != end)
		{
			parent = node_parent_map<skey_t>[current].first;
			auto child_idx = node_parent_map<skey_t>[current].second;
			if (duplications<skey_t>.find(parent) == duplications<skey_t>.end())
				parent_dup = new node_t<skey_t>(*parent);
			else
				parent_dup = duplications<skey_t>[parent].dup;
			parent_dup->children[child_idx] = current_dup;

			duplications<skey_t>.insert({ parent, {parent_dup, nullptr, MAX_UINT} });

			current = parent;
			current_dup = parent_dup;
		}
	}
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::write(write_params_t&& params)
{
	node_t<skey_t>* dup = new node_t<skey_t>(*this);
	node_t<skey_t>* parent;
	unsigned int child_idx = MAX_UINT;
	if (node_parent_map<skey_t>.find(this) != node_parent_map<skey_t>.end())
	{
		parent = node_parent_map<skey_t>[this].first;
		child_idx = node_parent_map<skey_t>[this].second;
	}
	else
	{
		new_root<skey_t> = dup;
		parent = nullptr;
	}

	switch (params.field_indicator)
	{
	case node_field::KEY:
		dup->key = *(skey_t*)params.replacement;
		break;
	case node_field::CHILD:
		if (params.specifier < dup->children.size())
			dup->children[params.specifier] = (node_t<skey_t>*)params.replacement;
		break;
	case node_field::DELETE:
		dup->set_del();
		break;
	default:
		break;
	}

	if (parent != nullptr && 
		duplications<skey_t>.find(parent) != duplications<skey_t>.end())
	{
		node_t<skey_t>* parent_dup = duplications<skey_t>[parent].dup;
		parent_dup->children[child_idx] = dup;
	}

	unsigned int ch_idx = 0;
	for (auto& ch : dup->children)
	{
		if (ch != nullptr && 
			duplications<skey_t>.find(ch) != duplications<skey_t>.end())
		{
			node_t<skey_t>* child_dup = duplications<skey_t>[ch].dup;
			dup->children[ch_idx] = child_dup;
			ch_idx++;
		}
	}

	duplications<skey_t>.insert({ this, {dup, parent, child_idx} });
	dup_happened = true;
	return dup;
}

template<typename skey_t>
node_t<skey_t>::node_t(const skey_t& key, unsigned int max_num_children) :
	key(key),
	children(max_num_children, nullptr),
	flags(0)
{}

template<typename skey_t>
node_t<skey_t>::node_t(const node_t& node) :
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
	return write({ node_field::KEY, 0, (void*)& new_key });
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::set_child(unsigned int child_idx, node_t<skey_t>* new_child)
{
	return write({ node_field::CHILD, child_idx, (void*)new_child });
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::delete_node()
{
	return write({ node_field::DELETE, 0, nullptr });
}