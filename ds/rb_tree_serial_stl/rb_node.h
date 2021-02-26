#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include "record_manager.h"

const unsigned int LEFT = 0;
const unsigned int RIGHT = 1;
const unsigned int MAX_CHILDREN = 2;

enum class rb_color { _S_red = false, _S_black = true };

template <typename skey_t, typename sval_t>
class rb_node
{
public:
	skey_t 			_M_key;
	sval_t 			_M_value;
	rb_color		_M_color;
	rb_node *		_M_parent;
	rb_node *		_M_left;
	rb_node *		_M_right;

	rb_color get_color()
	{
		return _M_color;
	}

	rb_node * get_parent()
	{
		return _M_parent;
	}

	rb_node * get_child(unsigned int index)
	{
		if (index == LEFT)
			return _M_left;
		else if (index == RIGHT)
			return _M_right;
		else return nullptr;
	}

	skey_t get_key()
	{
		return _M_key;
	}

	sval_t get_value()
	{
		return _M_value;
	}

	void set_color(rb_color color)
	{
		_M_color = color;
	}

	void set_parent(rb_node * parent)
	{
		_M_parent = parent;
	}

	void set_child(unsigned int index, rb_node * child)
	{
		if (index == LEFT)
			_M_left = child;
		else if (index == RIGHT)
			_M_right = child;
	}

	void set_key(skey_t key)
	{
		_M_key = key;
	}

	void set_value(sval_t value)
	{
		_M_value = value;
	}
};