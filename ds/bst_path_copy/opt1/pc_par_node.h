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
	std::vector<node*> children;
	unsigned char flags;

	inline bool is_dup() { return (flags & DUP_MASK) == DUP_MASK; }
	inline void set_dup() { flags |= DUP_MASK; }
	inline bool is_del() { return (flags & DEL_MASK) == DEL_MASK; }
	inline void set_del() { flags |= DEL_MASK; }

	inline unsigned int get_child_idx(node* self, node* parent)
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

	inline node* path_copy(std::vector<node*>& path, std::map<node*, node*>& dups, node*& new_root)
	{
		node* duplication = new node(*this);
		dups.insert({ this, duplication });

		auto current = duplication;
		auto rit = path.rbegin() + 1;
		for (; rit != path.rend(); ++rit) {
			auto parent = (*rit);
			if (parent->is_dup())
			{
				parent->children[get_child_idx(current, parent)] = current;
				break;
			}

			auto temp = current;
			current = new node(*(*rit));

			dups.insert({ *rit, current });
			(*rit)->set_dup();

			current->children[get_child_idx(temp, current)] = temp;
		}

		if (rit == path.rend())
			new_root = current;
		else
			new_root = path[0];

		return duplication;
	}

	node* write(write_params_t&& params, std::vector<node*>& path, std::map<node*, node*>& dups, node*& new_root)
	{
		auto dup = path_copy(path, dups, new_root);

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
		children(max_num_children, nullptr),
		flags(0)
	{}

	node_t(const node& node) :
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
	
	node* set_key(const skey_t& new_key, std::vector<node*>& path, std::map<node*, node*>& dups, node*& new_root)
	{
		return write({ node_field::KEY, 0, (void*)& new_key }, path, dups, new_root);
	}

	node* set_child(unsigned int child_idx, node* new_child, std::vector<node*>& path, std::map<node*, node*>& dups, node*& new_root)
	{
		return write({ node_field::CHILD, child_idx, (void*)new_child }, path, dups, new_root);
	}

	node* delete_node(std::vector<node*>& path, std::map<node*, node*>& dups, node*& new_root)
	{
		return write({ node_field::DELETE, 0, nullptr }, path, dups, new_root);
	}
};
