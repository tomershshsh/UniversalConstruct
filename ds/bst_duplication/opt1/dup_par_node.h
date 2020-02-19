#pragma once

#include <vector>
#include <map>
#include <mutex>

const unsigned char DUP_MASK = 0x01;
const unsigned char DEL_MASK = 0x02;
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
	using node = node_t<skey_t>;

	skey_t key;
	std::vector<node*> children;
	unsigned char flags;

	inline bool is_dup() { return (flags & DUP_MASK) == DUP_MASK; }
	inline void set_dup() { flags |= DUP_MASK; }
	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

	inline static unsigned int get_child_idx(node* self, node* parent)
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
	
	node* write(write_params_t&& params, node*& parent, std::map<node*, node*>& dups)
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
		case node_field::DELETE:
			dup->set_del();
			break;
		default:
			break;
		}

		if (parent != nullptr && dups.find(parent) != dups.end())
		{
			auto parent_dup = dups.at(parent);
			parent_dup->children[get_child_idx(dup, parent_dup)] = dup;
		}

		unsigned int ch_idx = 0;
		for (auto& ch : dup->children)
		{
			if (ch != nullptr && dups.find(ch) != dups.end())
			{
				auto child_dup = dups.at(ch);
				dup->children[ch_idx] = child_dup;
				ch_idx++;
			}
		}

		if (dup != this)
			dups.insert({ this, dup });

		return dup;
	}

public:
	node_t(const skey_t& key, unsigned int max_num_children) :
		key(key),
		children(max_num_children, nullptr),
		flags(0)
	{}

	node_t(const node_t& node) :
		key(node.key),
		children(node.children),
		flags(node.flags)
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

	node* set_key(const skey_t& new_key, node*& parent, std::map<node*, node*>& dups)
	{
		return write({ node_field::KEY, 0, (void*)& new_key }, parent, dups);
	}

	node* set_child(unsigned int child_idx, node* new_child, node*& parent, std::map<node*, node*>& dups)
	{
		return write({ node_field::CHILD, child_idx, (void*)new_child }, parent, dups);
	}

	node* delete_node(node*& parent, std::map<node*, node*>& dups)
	{
		return write({ node_field::DELETE, 0, nullptr }, parent, dups);
	}

	static void closure(std::map<node*, node*>& set_nodes, std::map<node*, node*>& dups)
	{
		const std::lock_guard<std::mutex> lock(g_mutex);
		for (auto& n : set_nodes)
		{
			if (n.second != nullptr && dups.find(n.first) != dups.end())
				n.second->children[get_child_idx(n.first, n.second)] = dups.at(n.first);
		}
	}
};
