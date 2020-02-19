#pragma once

#include <vector>

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
	std::vector<node*> children;
	unsigned char flags;

	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

	node* write(write_params_t&& params)
	{
		switch (params.field_indicator)
		{
		case node_field::KEY:
			key = *(skey_t*)params.replacement;
			break;
		case node_field::CHILD:
			if (params.specifier < children.size())
				children[params.specifier] = (node*)params.replacement;
			break;
		case node_field::DELETE:
			set_del();
			break;
		default:
			break;
		}

		return this;
	}

public:
	node_t(const skey_t& key, unsigned int max_num_children) :
		key(key), children(max_num_children, nullptr), flags(0) {}
	node_t(const node_t& node) :
		key(node.key), children(node.children), flags(node.flags) {}
	virtual ~node_t() = default;

	skey_t get_key() { return key; }

	node* get_child(unsigned int child_idx)
	{
		if (child_idx >= children.size())
			return nullptr;

		return children.at(child_idx);
	}

	bool is_deleted() { return is_del(); }

	node* set_key(const skey_t& new_key)
	{
		return write({ node_field::KEY, 0, (void*)& new_key });
	}

	node* set_child(unsigned int child_idx, node* new_child)
	{
		return write({ node_field::CHILD, child_idx, (void*)new_child });
	}

	node* delete_node()
	{
		return write({ node_field::DELETE, 0, nullptr });
	}
};
