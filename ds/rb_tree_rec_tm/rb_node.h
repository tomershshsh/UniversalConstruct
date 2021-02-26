#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include <pthread.h>
#include "record_manager.h"

thread_local bool locking_res = true;

typedef enum { RED=0, BLACK=1 } Colors ; 

const unsigned int LEFT = 0;
const unsigned int RIGHT = 1;
const unsigned int MAX_UINT = std::numeric_limits<unsigned int>::max();

bool do_print;

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
	pthread_spinlock_t dup_lock;

	rb_node();

	skey_t get_key();

	void set_key(const skey_t & key);

	sval_t get_value();

	void set_value(const sval_t & value);

	intptr_t get_color();

	void set_color(const intptr_t & color);

	rb_node<skey_t, sval_t> * get_parent();

	void set_parent(rb_node<skey_t, sval_t> * parent);

	rb_node<skey_t, sval_t> * get_child(const unsigned int & index);

	void set_child(const unsigned int & index, rb_node<skey_t, sval_t> * child);

	rb_node<skey_t, sval_t> * get_self();
};

#define node_t rb_node<skey_t, sval_t>

template <typename skey_t, typename sval_t>
rb_node<skey_t, sval_t>::rb_node() {}

template <typename skey_t, typename sval_t>
skey_t rb_node<skey_t, sval_t>::get_key()
{
	skey_t key = k;
	return key;
}

template <typename skey_t, typename sval_t>
void rb_node<skey_t, sval_t>::set_key(const skey_t & key)
{
	k = key;
}

template <typename skey_t, typename sval_t>
sval_t rb_node<skey_t, sval_t>::get_value()
{
	sval_t val = v;
	return val;
}

template <typename skey_t, typename sval_t>
void rb_node<skey_t, sval_t>::set_value(const sval_t & value)
{
	v = value;
}

template <typename skey_t, typename sval_t>
intptr_t rb_node<skey_t, sval_t>::get_color()
{
	intptr_t col = c;
	return col;
}

template <typename skey_t, typename sval_t>
void rb_node<skey_t, sval_t>::set_color(const intptr_t & color)
{
	c = color;
}

template <typename skey_t, typename sval_t>
rb_node<skey_t, sval_t> * rb_node<skey_t, sval_t>::get_parent()
{
	std::cout << "x" << std::endl;
	exit(-1);
	node_t * parent = p;
	return parent;
}

template <typename skey_t, typename sval_t>
void rb_node<skey_t, sval_t>::set_parent(rb_node<skey_t, sval_t> * parent)
{
	p = parent;
} 

template <typename skey_t, typename sval_t>
rb_node<skey_t, sval_t> * rb_node<skey_t, sval_t>::get_child(const unsigned int & index)
{
	node_t * child;
	if (LEFT == index)
		child = l;
	else if (RIGHT == index)
		child = r;
	else
		child = NULL;

    return child;
}

template <typename skey_t, typename sval_t>
void rb_node<skey_t, sval_t>::set_child(const unsigned int & index, rb_node<skey_t, sval_t> * child)
{
	if (LEFT == index)
		l = child;
	else if(RIGHT == index)
		r = child;
}

template <typename skey_t, typename sval_t>
rb_node<skey_t, sval_t> * rb_node<skey_t, sval_t>::get_self()
{
	node_t * self = (node_t *)this;
    return self;
}