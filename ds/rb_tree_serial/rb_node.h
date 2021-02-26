#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include "record_manager.h"

typedef enum { RED=0, BLACK=1 } Colors ; 

const unsigned int LEFT = 0;
const unsigned int RIGHT = 1;

template <typename skey_t, typename sval_t>
class rb_node
{
public:
	skey_t k;
	rb_node<skey_t, sval_t> * p;
	rb_node<skey_t, sval_t> * l; 
    rb_node<skey_t, sval_t> * r; 
    intptr_t c ; 
    sval_t v;

	skey_t get_key()
	{
		return k;
	}

	void set_key(const skey_t & key)
	{
		k = key;
	}

	sval_t get_value()
	{
		return v;
	}

	void set_value(const sval_t & value)
	{
		v = value;
	}

	intptr_t get_color()
	{
		return c;
	}

	void set_color(const intptr_t & color)
	{
		c = color;
	}

	rb_node<skey_t, sval_t> * get_parent()
	{
		return p;
	}

	void set_parent(rb_node<skey_t, sval_t> * parent)
	{
		p = parent;
	} 

	rb_node<skey_t, sval_t> * get_child(const unsigned int & index)
	{
		if (LEFT == index)
			return l;
		else if(RIGHT == index)
			return r;
		else
			return nullptr;
	}

	void set_child(const unsigned int & index, rb_node<skey_t, sval_t> * child)
	{
		if (LEFT == index)
			l = child;
		else if(RIGHT == index)
			r = child;
	}
};