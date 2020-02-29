#pragma once

#include <vector>
#include <unordered_map>

const unsigned char DUP_MASK = 0x01;
const unsigned char DEL_MASK = 0x02;

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

	inline bool is_dup() { return (flags & DUP_MASK) == DUP_MASK; }
	inline void set_dup() { flags |= DUP_MASK; }
	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

	inline unsigned int get_child_idx(node_t<skey_t>* self, node_t<skey_t>* parent)
	{
		unsigned int idx = 0;
		for (auto& ch : parent->children)
		{
			if (ch != nullptr && self->key == ch->key)
				return idx;
			idx++;
		}

		return std::numeric_limits<unsigned int>::max();
	}

	node_t<skey_t>* path_copy();
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
thread_local std::unordered_map<node_t<skey_t>*, node_t<skey_t>*> duplications;

template<typename skey_t>
thread_local std::unordered_map<node_t<skey_t>*, node_t<skey_t>*> node_parent_map;

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
	return true;
}

template<typename skey_t>
bool node_t<skey_t>::close(node_t<skey_t>*& root)
{
	if (root == orig_root<skey_t>)
	{
		root = new_root<skey_t>;
		return true;
	}

	return false;
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
		!node_parent_map<skey_t>[current]->is_dup())
	{
		parent = node_parent_map<skey_t>[current];
		parent_dup = new node_t<skey_t>(*parent);
		parent_dup->children[get_child_idx(current, parent)] = current_dup;

		duplications<skey_t>.insert({ parent, parent_dup });
		parent->set_dup();

		current = parent;
		current_dup = parent_dup;
	}

	if (reached_root)
	{
		new_root<skey_t> = current_dup;
	}
	else // reached a duplicated parent
	{
		auto to_update = duplications<skey_t>[node_parent_map<skey_t>[current]];
		to_update->children[get_child_idx(current, to_update)] = current_dup;
	}

	return duplication;
}

template<typename skey_t>
node_t<skey_t>* node_t<skey_t>::write(write_params_t&& params)
{
	auto dup = path_copy();

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

	return dup;
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
	node_parent_map<skey_t>.insert({ child, this });

	return children.at(child_idx);
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