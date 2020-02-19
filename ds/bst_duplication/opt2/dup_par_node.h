#pragma once

#include <vector>
#include <map>

enum class node_field
{
	KEY,
	CHILD
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

	node* write(write_params_t&& params, std::map<node*, node*>& dups)
	{
		node* dup;
		if (parent != nullptr)
			dup = new node(*this);
		else
			dup = this;

		switch (params.field_indicator)
		{
		case node_field::KEY:
			dup->key = *(skey_t*)params.replacement;
			break;
		case node_field::CHILD:
			if (params.specifier < dup->children.size())
				dup->children[params.specifier] = (node_t<skey_t>*)params.replacement;
			break;
		default:
			break;
		}

		if (dup->parent != nullptr && dups.find(dup->parent) != dups.end())
		{
			auto parent_dup = dups.at(dup->parent);
			dup->parent = parent_dup;
			parent_dup->children[dup->idx_in_parent_children] = dup;
		}

		for (auto& ch : dup->children)
		{
			if (ch != nullptr && dups.find(ch) != dups.end())
			{
				auto child_dup = dups.at(ch);
				dup->children[ch->idx_in_parent_children] = child_dup;
				child_dup->parent = dup;
			}
		}

		if (dup != this)
			dups.insert({ this, dup });

		return dup;
	}

public:
	node_t(const skey_t& key, unsigned int max_num_children) :
		key(key),
		parent(nullptr),
		idx_in_parent_children(std::numeric_limits<unsigned int>::max()),
		children(max_num_children, nullptr)
	{}

	node_t(const node_t& node) :
		key(node.key),
		parent(node.parent),
		idx_in_parent_children(node.idx_in_parent_children),
		children(node.children)
	{}

	virtual ~node_t() = default;

	skey_t get_key() { return key; }

	node* get_child(unsigned int child_idx)
	{
		if (child_idx >= children.size())
			return nullptr;

		return children.at(child_idx);
	}

	node* set_key(const skey_t& new_key, std::map<node*, node*>& dups)
	{
		return write({ node_field::KEY, 0, (void*)& new_key }, dups);
	}

	node* set_child(unsigned int child_idx, node* new_child, std::map<node*, node*>& dups)
	{
		auto res = write({ node_field::CHILD, child_idx, (void*)new_child }, dups);
		new_child->parent = res;
		new_child->idx_in_parent_children = child_idx;
		return res;
	}

	static void closure(std::vector<node*>&& set_nodes, std::map<node*, node*>& dups)
	{
		for (auto& n : set_nodes)
		{
			if (n->parent != nullptr && dups.find(n) != dups.end())
				n->parent->children[n->idx_in_parent_children] = dups.at(n);
		}
	}
};
