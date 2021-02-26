#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include <pthread.h>
#include "record_manager.h"

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
class duplication_info_t
{
public:
	node_t * dup;
	node_t * orig_parent;
	unsigned int orig_idx;
};

#define dinfo_t duplication_info_t<skey_t, sval_t>

template <typename skey_t, typename sval_t>
class path_info_t
{
public:
    node_t * self;
    node_t * parent;
    unsigned short index;
    unsigned short height;
};

#define pinfo_t path_info_t<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local std::unordered_map<node_t*, node_t*>* duplications = nullptr;
#define duplications duplications<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local std::unordered_map<node_t*, node_t*>* dup_orig_map = nullptr;
#define dup_orig_map dup_orig_map<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local std::unordered_map<node_t*, std::pair<node_t*, unsigned short>>* node_parent_map = nullptr;
#define node_parent_map node_parent_map<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local std::unordered_map<node_t*, bool>* allocated = nullptr;
#define allocated allocated<skey_t, sval_t>

thread_local bool in_writing_function = false;

thread_local bool pc_happened = false;

template <typename skey_t, typename sval_t>
thread_local node_t* orig_root;
#define orig_root orig_root<skey_t, sval_t>

template <typename skey_t, typename sval_t>
thread_local node_t* new_root;
#define new_root new_root<skey_t, sval_t>

template <typename skey_t, typename sval_t>
bool pc_open(const int& tid, node_t** root)
{
	if (duplications)
		duplications->clear();
	else
		duplications = new std::unordered_map<node_t*, node_t*>();

	if (dup_orig_map)
		dup_orig_map->clear();
	else
		dup_orig_map = new std::unordered_map<node_t *, node_t *>();

    if (node_parent_map)
		node_parent_map->clear();
	else
		node_parent_map = new std::unordered_map<node_t*, std::pair<node_t*, unsigned short>>();

    if (allocated)
        allocated->clear();
    else
        allocated = new std::unordered_map<node_t*, bool>();


	orig_root = *root;
	new_root = *root;
	in_writing_function = true;
	pc_happened = false;
	return true;
}

template <typename skey_t, typename sval_t>
bool pc_close(const int& tid, node_t** root)
{
	in_writing_function = false;

	if (pc_happened)
	{
		return __atomic_compare_exchange_n(
            root, 
            &orig_root, 
            new_root, 
            true, 
            __ATOMIC_RELAXED, 
            __ATOMIC_RELAXED);
	}
	else
	{
		return true;
	}
}

template <typename skey_t, typename sval_t>
rb_node<skey_t, sval_t>::rb_node()
{
	allocated->insert({this, true});
}

template <typename skey_t, typename sval_t>
skey_t rb_node<skey_t, sval_t>::get_key()
{
	skey_t key = k;
	node_t * orig = (node_t *)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        key = found->k;
    }

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
	node_t * orig = (node_t *)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        val = found->v;
    }

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
	node_t * orig = (node_t *)this;
    if (in_writing_function && duplications->find(orig) != duplications->end())
    {
        auto found = duplications->at(orig);
        col = found->c;
    }

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
	node_t * child = (node_t *)this;
    if (in_writing_function)
    {
		if (duplications->find(child) != duplications->end()) {
			child = duplications->at(child);
			parent = child->p;
		}
    }

	if (duplications->find(parent) != duplications->end())
		parent = duplications->at(parent);

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

    if (in_writing_function)
    {
        node_t * parent = (node_t *)this;

        // unsigned short parent_height = 0;
        // if (node_parent_map->find(parent) != node_parent_map->end())
        //     parent_height = node_parent_map->at(parent).height;
		
        if (duplications->find(parent) != duplications->end())
        {
			auto bla = duplications->at(parent);
            parent = bla;
			if (LEFT == index)
				child = parent->l;
			else if (RIGHT == index)
				child = parent->r;
			else
				child = NULL;
        }
		
        
		if (child != NULL && allocated->find(child) == allocated->end() && allocated->find(parent) == allocated->end())
		{
			if (node_parent_map->find(child) == node_parent_map->end())
			{
				// unsigned short parent_height = 0;
				// if (node_parent_map->find(parent) != node_parent_map->end())
				// 	parent_height = node_parent_map->at(parent).height;

				node_parent_map->insert(
					std::make_pair<node_t*, std::pair<node_t*, unsigned short>>(
						(node_t*)child, std::make_pair<node_t*, unsigned short>(
							(node_t*)parent, (unsigned int)index)));
			}
			
		}
		else if (child != NULL)
		{
			auto ochild = child;
			if (dup_orig_map->find(child) != dup_orig_map->end())
				ochild = dup_orig_map->at(child);

			auto oparent = parent;
			if (dup_orig_map->find(parent) != dup_orig_map->end())
				oparent = dup_orig_map->at(parent);

			// unsigned short oparent_height = 0;
			// if (node_parent_map->find(oparent) != node_parent_map->end())
            // 	oparent_height = node_parent_map->at(oparent).height;

			if ((index == LEFT && oparent->l == ochild) || (index == RIGHT && oparent->r == ochild)) {
				node_parent_map->insert(
					std::make_pair<node_t*, std::pair<node_t*, unsigned short>>(
						(node_t*)ochild, std::make_pair<node_t*, unsigned short>(
							(node_t*)oparent, (unsigned int)index)));
			}
		}
    }

end:
	
	return child;
}

// template <typename skey_t, typename sval_t>
// rb_node<skey_t, sval_t> * rb_node<skey_t, sval_t>::get_child(const unsigned int & index)
// {
// 	node_t * child;
// 	if (LEFT == index)
// 		child = l;
// 	else if (RIGHT == index)
// 		child = r;
// 	else
// 		child = NULL;

//     if (in_writing_function)
//     {
//         node_t * parent = (node_t *)this;
// 		node_t * orig_parent = parent;
		
//         if (duplications->find(parent) != duplications->end())
//         {
// 			auto bla = duplications->at(parent);
//             node_t * parent = bla;
// 			if (LEFT == index)
// 				child = parent->l;
// 			else if (RIGHT == index)
// 				child = parent->r;
// 			else
// 				child = NULL;
//         }
        
// 		if (child != NULL && allocated->find(child) == allocated->end() && allocated->find(orig_parent) == allocated->end())
// 		{
// 			if (node_parent_map->find(child) == node_parent_map->end())
// 			{
// 				unsigned short idx;
// 				if (orig_parent->l == child)
// 					idx = LEFT;
// 				else if (orig_parent->r == child)
// 					idx = RIGHT;
// 				else
// 				{
// 					//TODO: check
// 					goto end;
// 				}
				
// 				node_parent_map->insert(
// 					std::make_pair<node_t*, std::pair<node_t*, unsigned short>>(
// 						(node_t*)child, std::make_pair<node_t*, unsigned short>(
// 							(node_t*)orig_parent, (unsigned int)idx)));
// 			}
// 		}
//     }

// end:
	
// 	return child;
// }

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
    if (in_writing_function)
    {
		if (duplications->find(self) != duplications->end()) {
			self = duplications->at(self);
		}
    }

	return self;
}