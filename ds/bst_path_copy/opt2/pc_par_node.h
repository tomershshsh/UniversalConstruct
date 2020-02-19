#pragma once

#include <vector>
#include <map>

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
	using node = node_t<skey_t>;

	skey_t key;
	node* parent;
	unsigned int idx_in_parent_children;
	std::vector<node*> children;
	unsigned char flags;
	node* root;

	inline bool is_dup() { return (flags & DUP_MASK) == DUP_MASK; }
	inline void set_dup() { flags |= DUP_MASK; }
	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

	inline node* path_copy(std::map<node*, node*>& dups)
	{
		node* duplication = new node(*this);
		dups.insert({ this, duplication });

		auto current = duplication;
		while (current->parent != nullptr && !current->parent->is_dup()) {
			auto temp = current;
			current = new node(*(current->parent));

			dups.insert({ temp->parent, current });
			temp->parent->set_dup();

			temp->parent = current;
			current->children[temp->idx_in_parent_children] = temp;
		}

		if (current->parent == nullptr)
		{
			duplication->root = current;
		}
		else
		{
			current->parent = dups.at(current->parent);
			current->parent->children[current->idx_in_parent_children] = current;
			duplication->root = current->parent->root;
		}

		return duplication;
	}

	node* write(write_params_t&& params, std::map<node*, node*>& dups)
	{
		auto dup = path_copy(dups);

		switch (params.field_indicator)
		{
		case node_field::KEY:
			dup->key = *(skey_t*)params.replacement;
			break;
		case node_field::CHILD:
			if (params.specifier < dup->children.size())
				dup->children[params.specifier] = (node*)params.replacement;
			break;
		case node_field::DELETE:
			dup->set_del();
			break;
		default:
			break;
		}

		return dup;
	}

public:
	node_t(const skey_t& key, unsigned int max_num_children) :
		key(key),
		parent(nullptr),
		idx_in_parent_children(std::numeric_limits<unsigned int>::max()),
		children(max_num_children, nullptr),
		flags(0),
		root(nullptr)
	{}

	node_t(const node& node) :
		key(node.key), 
		parent(node.parent), 
		idx_in_parent_children(node.idx_in_parent_children),
		children(node.children),
		flags(node.flags),
		root(node.root)
	{}

	virtual ~node_t() = default;

	skey_t get_key() { return key; }

	node* get_child(unsigned int child_idx)
	{
		if (child_idx >= children.size())
			return nullptr;

		return children.at(child_idx);
	}

	bool is_deleted() { return is_del(); }

	node* get_root() { return root; }

	node* set_key(const skey_t& new_key, std::map<node*, node*>& dups)
	{
		return write({ node_field::KEY, 0, (void*)& new_key }, dups);
	}

	node* set_child(unsigned int child_idx, node* new_child, std::map<node*, node*>& dups)
	{
		auto res = write({ node_field::CHILD, child_idx, (void*)new_child }, dups);
		new_child->parent = res;
		new_child->idx_in_parent_children = child_idx;
		new_child->root = res->root;
		return res;
	}

	node* delete_node(std::map<node*, node*>& dups)
	{
		return write({ node_field::DELETE, 0, nullptr }, dups);
	}
};
