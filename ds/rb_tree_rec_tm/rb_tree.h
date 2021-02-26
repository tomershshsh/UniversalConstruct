#pragma once

#include "rb_node.h" 
#include <mutex>

std::mutex g_mutex;
unsigned int recursive_counter;

template <typename skey_t, typename sval_t, class RecMgr>
class rb_tree {
private:
	rb_node<skey_t, sval_t> * root;

	const int NUM_THREADS;
    const skey_t KEY_MIN;
    const skey_t KEY_MAX;
    const sval_t NO_VALUE;
	int init[MAX_THREADS_POW2] = {0,};
	RecMgr* recmgr;

	rb_node<skey_t, sval_t> * _lookup (skey_t k) {
		rb_node<skey_t, sval_t> * p = root; 
		while (p != NULL) {
			skey_t cmp = k - p->get_key(); 
			if (cmp == 0) return p; 
			p = (cmp < 0) ? p->get_child(LEFT) : p->get_child(RIGHT);
		}
		return NULL ; 
	}

	// DUP operations

	rb_node<skey_t, sval_t> * dup_prologue(const int& tid, rb_node<skey_t, sval_t> * orig) 
    {
        return orig;
    }

    rb_node<skey_t, sval_t> * dup_epilogue(const int& tid, rb_node<skey_t, sval_t> * orig, rb_node<skey_t, sval_t> * dup)
    {
        return orig;
    }

	//  Balancing operations.
	// 
	//  Implementations of rebalancings during insertion and deletion are
	//  slightly different than the CLR version.  Rather than using dummy
	//  nilnodes, we use a set of accessors that deal properly with null.  They
	//  are used to avoid messiness surrounding nullness checks in the main
	//  algorithms.
	// 
	// From CLR 

	void rotateLeft (const int & tid, rb_node<skey_t, sval_t> * xp, rb_node<skey_t, sval_t> * x)
	{
		rb_node<skey_t, sval_t> * r = rightOf(x);
		rb_node<skey_t, sval_t> * rl = leftOf(r);

		auto x_dup = dup_prologue(tid, x);
		if (x_dup != nullptr) {
			x_dup->set_child(RIGHT, rl);
			dup_epilogue(tid, x, x_dup);
		}

		auto r_dup = dup_prologue(tid, r);	//
		if (r_dup != nullptr) {				//
			r_dup->set_child(LEFT, x); 		//
			dup_epilogue(tid, r, r_dup);	//
		}									//

		if (xp == NULL) {
			root = r;
		}

		else if (selfOf(xp->get_child(LEFT)) == selfOf(x)) {
			auto xp_dup = dup_prologue(tid, xp);
			if (xp_dup != nullptr) {
				xp_dup->set_child(LEFT, r); 
				dup_epilogue(tid, xp, xp_dup);
			}
		}
		else {
			auto xp_dup = dup_prologue(tid, xp);
			if (xp_dup != nullptr) {
				xp_dup->set_child(RIGHT, r);
				dup_epilogue(tid, xp, xp_dup);
			} 
		}
	}

	void rotateRight (const int & tid, rb_node<skey_t, sval_t> * xp, rb_node<skey_t, sval_t> * x)
	{
		rb_node<skey_t, sval_t> * l = leftOf(x);
		rb_node<skey_t, sval_t> * lr = rightOf(l);

		auto x_dup = dup_prologue(tid, x);
		if (x_dup != nullptr) {
			x_dup->set_child(LEFT, lr);
			dup_epilogue(tid, x, x_dup);
		}

		auto l_dup = dup_prologue(tid, l);	//
		if (l_dup != nullptr) {				//
			l_dup->set_child(RIGHT, x);		//
			dup_epilogue(tid, l, l_dup);	//
		}									//
		
		auto rxp = (xp != NULL) ? xp->get_child(RIGHT) : NULL;
		if (xp == NULL) {
			root = l;
		}
		else if (selfOf(rxp) == selfOf(x)) {
			auto xp_dup = dup_prologue(tid, xp);
			if (xp_dup != nullptr) {
				xp_dup->set_child(RIGHT, l);
				dup_epilogue(tid, xp, xp_dup);
			}
		}
		else { 
			auto xp_dup = dup_prologue(tid, xp);
			if (xp_dup != nullptr) {
				xp_dup->set_child(LEFT, l);
				dup_epilogue(tid, xp, xp_dup);
			}
		}
	}

	inline rb_node<skey_t, sval_t> * parentOf (rb_node<skey_t, sval_t> * n) {
	return n ? n->get_parent() : NULL ; 
	}

	inline rb_node<skey_t, sval_t> * leftOf (rb_node<skey_t, sval_t> * n) {
	return n ? n->get_child(LEFT) : NULL ; 
	}

	inline rb_node<skey_t, sval_t> * rightOf (rb_node<skey_t, sval_t> * n) { 
		return n ? n->get_child(RIGHT) : NULL ; 
	}

	inline rb_node<skey_t, sval_t> * selfOf (rb_node<skey_t, sval_t> * n) { 
		return n ? n->get_self() : NULL ; 
	}

	inline int colorOf (rb_node<skey_t, sval_t> * n) { 
		return n ? (int)(n->get_color()) : BLACK ; 
	}

	inline void setColor (const int & tid, rb_node<skey_t, sval_t> * n, int c) { 
		if (n != NULL) {
			auto n_dup = dup_prologue(tid, n);
			if (n_dup != nullptr) {
				n_dup->set_color(c); 
				dup_epilogue(tid, n, n_dup);
			}
		}
	}

	void print_tree_helper2(rb_node<skey_t, sval_t> * bla, int n, bool left)
	{
		if (bla == NULL)
			return;
		// if (left)
		// {
		// 	if (!pthread_spin_trylock(&bla->dup_lock)) {
		// 		std::cout << "l: " << bla << "(" << bla->c << ")" << " [" << bla->k << "]" << std::endl;
		// 		pthread_spin_unlock(&bla->dup_lock);
		// 	}
		// 	else
		// 	{
		// 		std::cout << "l: " << bla << "(" << bla->c << ")" << " [" << bla->k << "] LOCKED!!!" << std::endl;
		// 	}
			
		// }
		// else
		// {
		// 	if (!pthread_spin_trylock(&bla->dup_lock)) {
		// 		std::cout << "r: " << bla << "(" << bla->c << ")" << " [" << bla->k << "]" << std::endl;
		// 		pthread_spin_unlock(&bla->dup_lock);
		// 	}
		// 	else
		// 	{
		// 		std::cout << "r: " << bla << "(" << bla->c << ")" << " [" << bla->k << "] LOCKED!!!" << std::endl;
		// 	}
		// }
		if (bla->l != NULL)
			print_tree_helper2(bla->l, n+1, true);
		if (bla->r != NULL)
			print_tree_helper2(bla->r, n+1, false);
	}

	void print_tree_helper(rb_node<skey_t, sval_t> * bla, int n, bool left)
	{
		if (bla == NULL)
			return;
		for (int i = 0; i < n; i++)
		{
			std::cout << "\t";
		}
		if (left)
		{
			if (!pthread_spin_trylock(&bla->dup_lock)) {
				std::cout << "l: " << bla << "(" << bla->c << ")" << " [" << bla->k << "]" << std::endl;
				pthread_spin_unlock(&bla->dup_lock);
			}
			else
			{
				std::cout << "l: " << bla << "(" << bla->c << ")" << " [" << bla->k << "] LOCKED!!!" << std::endl;
			}
			
		}
		else
		{
			if (!pthread_spin_trylock(&bla->dup_lock)) {
				std::cout << "r: " << bla << "(" << bla->c << ")" << " [" << bla->k << "]" << std::endl;
				pthread_spin_unlock(&bla->dup_lock);
			}
			else
			{
				std::cout << "r: " << bla << "(" << bla->c << ")" << " [" << bla->k << "] LOCKED!!!" << std::endl;
			}
		}
		if (bla->l != NULL)
			print_tree_helper(bla->l, n+1, true);
		if (bla->r != NULL)
			print_tree_helper(bla->r, n+1, false);
	}

	void print_tree()
	{
		print_tree_helper(root, 0, true);
	}

	int checkConsequentReds(rb_node<skey_t, sval_t>* currNode)
	{
		if (currNode == NULL)
			return 0;

		int add = currNode->get_color() == RED ? 1 : 0;

		int lres = checkConsequentReds(currNode->get_child(LEFT));
		int rres = checkConsequentReds(currNode->get_child(RIGHT));

		if (lres == -1 || rres == -1 || lres + add == 2 || rres + add == 2)
			return -1;
		else
			return add;
	}

	int computeBlackHeight(rb_node<skey_t, sval_t>* currNode) {
		if (currNode == NULL)
			return 0;
		
		int leftHeight = computeBlackHeight(currNode->get_child(LEFT));
		int rightHeight = computeBlackHeight(currNode->get_child(RIGHT));
		int add = currNode->get_color() == BLACK ? 1 : 0;
		
		if (leftHeight == -1 || rightHeight == -1 || leftHeight != rightHeight)
			return -1;
		else
			return leftHeight + add;
	}

	int count_nodes(rb_node<skey_t, sval_t>* currNode) {
		if (currNode == NULL)
			return 0;

		int l = count_nodes(currNode->l);
		int r = count_nodes(currNode->r);

		return 1 + l + r;
	}

	int insert_rec(const int & tid, skey_t k, sval_t v, rb_node<skey_t, sval_t>* n)
	{
		if (root == NULL) {
			if (n == NULL) return 0;
			// Note: the following STs don't really need to be transactional.  

			auto n_dup = dup_prologue(tid, n);
			if (n_dup != nullptr) {
				n_dup->set_child(LEFT, NULL);
				n_dup->set_child(RIGHT, NULL);
				n_dup->set_parent(NULL);
				n_dup->set_key(k); 
				n_dup->set_value(v); 
				n_dup->set_color(BLACK);
				dup_epilogue(tid, n, n_dup);
			}			

			root = n_dup;
			return 0;
		}

		int res = insert_recursive(tid, NULL, root, k, v, n);
		rb_node<skey_t, sval_t>* ro = root;
		if (colorOf(ro) != BLACK) {
		// if (ro->get_color() != BLACK) {
			auto ro_dup = dup_prologue(tid, ro);
			if (ro_dup != nullptr) {
				ro_dup->set_color(BLACK);
				dup_epilogue(tid, ro, ro_dup);
			}
			root = ro_dup;
		}
		return res;
	}

	int fix_rec_insert(
		const int & tid, 
		rb_node<skey_t, sval_t>* xppp,
		rb_node<skey_t, sval_t>* xpp,
		rb_node<skey_t, sval_t>* xp,
		rb_node<skey_t, sval_t>* x)
	{
		if (colorOf(xp) != RED) {
			return 1337;
		}

		if (xppp) {
			auto bla = dup_prologue(tid, xppp);
			if (bla)
				dup_epilogue(tid, xppp, bla);
		}
		if (xpp) {
			auto bla = dup_prologue(tid, xpp);
			if (bla)
				dup_epilogue(tid, xpp, bla);
		}
		if (xp) {
			auto bla = dup_prologue(tid, xp);
			if (bla)
				dup_epilogue(tid, xp, bla);
		}
		if (x) {
			auto bla = dup_prologue(tid, x);
			if (bla)
				dup_epilogue(tid, x, bla);
		}

		if (selfOf(xp) == selfOf(leftOf(xpp))) {
			rb_node<skey_t, sval_t>* y = rightOf(xpp);
			if (colorOf(y) == RED) {
				setColor(tid, xp, BLACK);
				setColor(tid, y, BLACK);
				setColor(tid, xpp, RED);
				return 2;
			}
			else {
				if (selfOf(x) == selfOf(rightOf(xp))) {
					auto temp = selfOf(x);
					x = selfOf(xp);
					xp = temp;
					rotateLeft(tid, xpp, x);
				}
				setColor(tid, xp, BLACK);
				setColor(tid, xpp, RED);
				if (xpp != NULL)
				{
					rotateRight(tid, xppp, xpp);
				}
					
				return 1337;
			}
		}
		else {
			rb_node<skey_t, sval_t>* y = leftOf(xpp);
			if (colorOf(y) == RED) {
				setColor(tid, xp, BLACK);
				setColor(tid, y, BLACK);
				setColor(tid, xpp, RED);
				return 2;
			}
			else {
				if (selfOf(x) == selfOf(leftOf(xp))) {
					auto temp = selfOf(x);
					x = selfOf(xp);
					xp = temp;
					rotateRight(tid, xpp, x);
				}
				setColor(tid, xp, BLACK);
				setColor(tid, xpp, RED);
				if (xpp != NULL)
				{
					rotateLeft(tid, xppp, xpp);
				}
				
				return 1337;
			}
		}
	}

	int insert_recursive(
		const int & tid, 
		rb_node<skey_t, sval_t>* tp,
		rb_node<skey_t, sval_t>* t, 
		skey_t k, 
		sval_t v, 
		rb_node<skey_t, sval_t>* n)
	{
		skey_t cmp = k - t->get_key();

		if (cmp == 0)
		{
			return 1338;
		}
		else if (cmp < 0)
		{
			rb_node<skey_t, sval_t>* tl = t->get_child(LEFT);
			if (tl != NULL)
			{
				int res = insert_recursive(tid, t, tl, k, v, n);
				if (res == 1337 || res == 1338)
				{
					return res;
				}
				else if (res - 1 > 0)
				{
					return res - 1;
				}
				else
				{
					rb_node<skey_t, sval_t>* xppp = tp;
					rb_node<skey_t, sval_t>* xpp = t;
					rb_node<skey_t, sval_t>* xp = tl;
					rb_node<skey_t, sval_t>* x = k - tl->get_key() < 0 ? tl->get_child(LEFT) : tl->get_child(RIGHT);

					return fix_rec_insert(tid, xppp, xpp, xp, x);
				}
			}
			else
			{
				auto n_dup = dup_prologue(tid, n);
				if (n_dup != nullptr) {
					n_dup->set_child(LEFT, NULL);
					n_dup->set_child(RIGHT, NULL);
					n_dup->set_key(k); 
					n_dup->set_value(v); 
					n_dup->set_color(RED);
					dup_epilogue(tid, n, n_dup);
				}

				auto t_dup = dup_prologue(tid, t);
				if (t_dup != nullptr) {
					t_dup->set_child(LEFT, n); 
					dup_epilogue(tid, t, t_dup);
				}

				return 1;
			}
		}
		else
		{
			rb_node<skey_t, sval_t>* tr = t->get_child(RIGHT);
			if (tr != NULL)
			{
				int res = insert_recursive(tid, t, tr, k, v, n);
				if (res == 1337 || res == 1338)
				{
					return res;
				}
				else if (res - 1 > 0)
				{
					return res - 1;
				}
				else
				{
					rb_node<skey_t, sval_t>* xppp = tp;
					rb_node<skey_t, sval_t>* xpp = t;
					rb_node<skey_t, sval_t>* xp = tr;
					rb_node<skey_t, sval_t>* x = k - tr->get_key() < 0 ? tr->get_child(LEFT) : tr->get_child(RIGHT);

					return fix_rec_insert(tid, xppp, xpp, xp, x);
				}
			}
			else
			{
				auto n_dup = dup_prologue(tid, n);
				if (n_dup != nullptr) {
					n_dup->set_child(LEFT, NULL);
					n_dup->set_child(RIGHT, NULL);
					n_dup->set_key(k); 
					n_dup->set_value(v); 
					n_dup->set_color(RED);
					dup_epilogue(tid, n, n_dup);
				}

				auto t_dup = dup_prologue(tid, t);
				if (t_dup != nullptr) {
					t_dup->set_child(RIGHT, n); 
					dup_epilogue(tid, t, t_dup);
				}

				return 1;
			}
		}
	}

	void fixAfterInsertion(const int & tid, rb_node<skey_t, sval_t> * x) {
		auto x_dup = dup_prologue(tid, x);
		if (x_dup != nullptr) {
			x_dup->set_color(RED); 
			dup_epilogue(tid, x, x_dup);
		}
		
		while (x != NULL && x != root) { 
			rb_node<skey_t, sval_t> * xp = x->get_parent(); 
			if (xp->get_color() != RED) break ; 

			auto px = parentOf(x);
			auto ppx = parentOf(parentOf(x));
			auto lppx = leftOf(ppx);

			// TODO: cache g = ppx = parentOf(parentOf(x))
			// if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
			if (px == lppx) {
				rb_node<skey_t, sval_t> *  y = rightOf(parentOf(parentOf(x)));
				if (colorOf(y) == RED) {
					setColor(tid, parentOf(x), BLACK);
					setColor(tid, y, BLACK);
					setColor(tid, parentOf(parentOf(x)), RED);
					x = parentOf(parentOf(x));
				} else {
					auto rpx = rightOf(parentOf(x));
					if (x->get_self() == rpx) {
						x = parentOf(x);
						rotateLeft(tid, parentOf(x), x);
					}
					setColor(tid, parentOf(x), BLACK);
					setColor(tid, parentOf(parentOf(x)), RED);
					if (parentOf(parentOf(x)) != NULL)
						rotateRight(tid, parentOf(parentOf(parentOf(x))), parentOf(parentOf(x)));
				}
			} else {
				rb_node<skey_t, sval_t> * y = leftOf(parentOf(parentOf(x)));
				if (colorOf(y) == RED) {
					setColor(tid, parentOf(x), BLACK);
					setColor(tid, y, BLACK);
					setColor(tid, parentOf(parentOf(x)), RED);
					x = parentOf(parentOf(x));
				} else {
					if (x->get_self() == leftOf(parentOf(x))) {
						x = parentOf(x);
						rotateRight(tid, parentOf(x), x);
					}
					setColor(tid, parentOf(x),  BLACK);
					setColor(tid, parentOf(parentOf(x)), RED);
					if (parentOf(parentOf(x)) != NULL)
						rotateLeft(tid, parentOf(parentOf(parentOf(x))), parentOf(parentOf(x)));
				}
			}
		}
		rb_node<skey_t, sval_t> * ro = root;
		if (ro->get_color() != BLACK) {
			auto ro_dup = dup_prologue(tid, ro);
			if (ro_dup != nullptr) {
				ro_dup->set_color(BLACK); 
				dup_epilogue(tid, ro, ro_dup);
			}
		}
	}

	// _insert() has putIfAbsent() semantics
	// If the key already exists in the tree _insert() returns a pointer to the 
	// node bearing that key and does not modify the tree, otherwise if the key
	// is not in the tree it inserts (k,v) into the tree using node n.

	rb_node<skey_t, sval_t> *  _insert (const int & tid, skey_t k, sval_t v, rb_node<skey_t, sval_t> * n) { 
		rb_node<skey_t, sval_t> * t = root; 
		if (t == NULL) {
			if (n == NULL) return NULL ; 
			// Note: the following STs don't really need to be transactional.
			auto n_dup = dup_prologue(tid, n);
			if (n_dup != nullptr) {
				n_dup->set_child(LEFT, NULL);
				n_dup->set_child(RIGHT, NULL);
				n_dup->set_parent(NULL);
				n_dup->set_key(k); 
				n_dup->set_value(v); 
				n_dup->set_color(BLACK);
				dup_epilogue(tid, n, n_dup);
			}			

			root = n_dup;
			return NULL ; 
		}

		for (;;) {
			skey_t cmp = k - t->get_key() ; 
			if (cmp == 0) {
				return t ; 
			} else if (cmp < 0) {
				rb_node<skey_t, sval_t> * tl = t->get_child(LEFT); 
				if (tl != NULL) {
					t = tl ; 
				} else {
					auto n_dup = dup_prologue(tid, n);
					if (n_dup != nullptr) {
						n_dup->set_child(LEFT, NULL);
						n_dup->set_child(RIGHT, NULL);
						n_dup->set_key(k); 
						n_dup->set_value(v); 
						n_dup->set_parent(t);
						dup_epilogue(tid, n, n_dup);
					}

					auto t_dup = dup_prologue(tid, t);
					if (t_dup != nullptr) {
						t_dup->set_child(LEFT, n); 
						dup_epilogue(tid, t, t_dup);
					}

					fixAfterInsertion(tid, n);
					return NULL ;
				}
			} else { // cmp > 0
				rb_node<skey_t, sval_t> * tr = t->get_child(RIGHT); 
				if (tr != NULL) {
					t = tr;
				} else {
					auto n_dup = dup_prologue(tid, n);
					if (n_dup != nullptr) {
						n_dup->set_child(LEFT, NULL);
						n_dup->set_child(RIGHT, NULL);
						n_dup->set_key(k); 
						n_dup->set_value(v); 
						n_dup->set_parent(t); 
						dup_epilogue(tid, n, n_dup);
					}

					auto t_dup = dup_prologue(tid, t);
					if (t_dup != nullptr) {
						t_dup->set_child(RIGHT, n); 
						dup_epilogue(tid, t, t_dup);
					}

					fixAfterInsertion(tid, n);
					return NULL;
				}
			}
		}
	}

	// Return the given node's successor node---the node which has the
	// next key in the the left to right ordering. If the node has
	// no successor, a null pointer is returned rather than a pointer to
	// the nil node.

	static rb_node<skey_t, sval_t> * _successor(rb_node<skey_t, sval_t> * t) {
		if (t == NULL)
			return NULL;
		else if (t->get_child(RIGHT) != NULL) {
			rb_node<skey_t, sval_t> * p = t->get_child(RIGHT) ; 
			while (p->get_child(LEFT) != NULL) { 
				p = p->get_child(LEFT) ; 
			}
			return p;
		} else {
			rb_node<skey_t, sval_t> * p = t->get_parent();
			rb_node<skey_t, sval_t> * ch = t;
			while (p != NULL && ch == p->get_child(RIGHT)) {
				ch = p;
				p = p->get_parent(); 
			}
			return p;
		}
	}

	void just_duplicate_node(const int & tid, rb_node<skey_t, sval_t>* n)
	{
		if (n)
		{
			auto bla = dup_prologue(tid, n);
			if (bla)
				dup_epilogue(tid, n, bla);
		}
	}

	int successor_recursive(
		const int & tid, 
		rb_node<skey_t, sval_t>* tpp,
		rb_node<skey_t, sval_t>* tp,
		rb_node<skey_t, sval_t>* t,
		rb_node<skey_t, sval_t>** deleted)
	{
		rb_node<skey_t, sval_t>* tl = t->get_child(LEFT);
		if (tl == NULL)
		{
			*deleted = t;

			rb_node<skey_t, sval_t>* xppp = tpp;
			rb_node<skey_t, sval_t>* xpp = tp;
			rb_node<skey_t, sval_t>* xp = t;
			rb_node<skey_t, sval_t>* x = t->get_child(RIGHT);

			just_duplicate_node(tid, xppp);
			just_duplicate_node(tid, xpp);
			just_duplicate_node(tid, xp);
			just_duplicate_node(tid, x);

			if (x != NULL) {
				if (xpp == NULL)
				{
					auto root_dup = dup_prologue(tid, root);
					if (root_dup != nullptr) {
						root_dup = selfOf(x);
						dup_epilogue(tid, root, root_dup);
					}
					
					root = root_dup;
				}
				else if (selfOf(xp) == selfOf(xpp->get_child(LEFT)))
				{
					auto xpp_dup = dup_prologue(tid, xpp);
					if (xpp_dup != nullptr) {
						xpp_dup->set_child(LEFT, x);
						dup_epilogue(tid, xpp, xpp_dup);
					}
				}
				else
				{
					auto xpp_dup = dup_prologue(tid, xpp);
					if (xpp_dup != nullptr) {
						xpp_dup->set_child(RIGHT, x);
						dup_epilogue(tid, xpp, xpp_dup);
					}
				}

				auto xp_dup = dup_prologue(tid, xp);
				if (xp_dup != nullptr) {
					xp_dup->set_child(LEFT, NULL);
					xp_dup->set_child(RIGHT, NULL);
					xp_dup->set_parent(NULL);
					dup_epilogue(tid, xp, xp_dup);
				}

				// Fix replacement
				if (xp->get_color() == BLACK)
				{
					return fix_rec_delete(tid, xppp, xpp, x);
				}
				else
					return 1337;
			}
			else {
				int res = 1337;
				if (xp->get_color() == BLACK)
				{
					res = fix_rec_delete(tid, xppp, xpp, xp);
				}

				if (xpp != NULL) {
					if (selfOf(xp) == selfOf(xpp->get_child(LEFT)))
					{
						auto xpp_dup = dup_prologue(tid, xpp);
						if (xpp_dup != nullptr) {
							xpp_dup->set_child(LEFT, NULL);
							dup_epilogue(tid, xpp, xpp_dup);
						}
					}
					else if (selfOf(xp) == selfOf(xpp->get_child(RIGHT)))
					{
						auto xpp_dup = dup_prologue(tid, xpp);
						if (xpp_dup != nullptr) {
							xpp_dup->set_child(RIGHT, NULL);
							dup_epilogue(tid, xpp, xpp_dup);
						}
					}
				}

				return res;
			}
		}
		else
		{
			int res = successor_recursive(tid, tp, t, tl, deleted);
			if (res == 1337 || res == 1338)
			{
				return res;
			}
			else if (res - 1 > 0)
			{
				return res - 1;
			}
			else
			{
				rb_node<skey_t, sval_t>* xppp = tpp;
				rb_node<skey_t, sval_t>* xpp = tp;
				rb_node<skey_t, sval_t>* xp = t;
				rb_node<skey_t, sval_t>* x = tl;

				just_duplicate_node(tid, xppp);
				just_duplicate_node(tid, xpp);
				just_duplicate_node(tid, xp);
				just_duplicate_node(tid, x);

				if (xp->get_color() == BLACK)
				{
					return fix_rec_delete(tid, xppp, xpp, xp);
				}
				else
				{
					setColor(tid, xp, BLACK);
					return 1337;
				}
			}
		}
	}

	rb_node<skey_t, sval_t>* delete_rec(const int & tid, skey_t k)
	{
		rb_node<skey_t, sval_t>* t = root;

		if (t == NULL)
			return NULL;

		rb_node<skey_t, sval_t>* deleted = NULL;

		int res = delete_recursive(tid, NULL, NULL, root, k, &deleted);
		if (res != 1338)
			return deleted;
		else
			return NULL;
	}

	int fix_rec_delete(
		const int & tid, 
		rb_node<skey_t, sval_t>* xpp,
		rb_node<skey_t, sval_t>* xp,
		rb_node<skey_t, sval_t>* x)
	{
		if (selfOf(x) == selfOf(leftOf(xp))) {
			rb_node<skey_t, sval_t>* sib = rightOf(xp);
			if (colorOf(sib) == RED) {
				setColor(tid, sib, BLACK);
				setColor(tid, xp, RED);
				rotateLeft(tid, xpp, xp);

				xpp = selfOf(sib);

				sib = selfOf(rightOf(xp));
			}

			if (colorOf(leftOf(sib)) == BLACK &&
				colorOf(rightOf(sib)) == BLACK) {
				setColor(tid, sib, RED);
				return 1;
			}
			else {
				if (colorOf(rightOf(sib)) == BLACK) {
					setColor(tid, leftOf(sib), BLACK);
					setColor(tid, sib, RED);
					rotateRight(tid, xp, sib);

					sib = selfOf(rightOf(xp));
				}
				setColor(tid, sib, colorOf(xp));
				setColor(tid, xp, BLACK);
				setColor(tid, rightOf(sib), BLACK);
				rotateLeft(tid, xpp, xp);
				// TODO: consider break ...
				return 1337;
			}
		}
		else { // symmetric
			rb_node<skey_t, sval_t>* sib = leftOf(xp);

			if (colorOf(sib) == RED) {
				setColor(tid, sib, BLACK);
				setColor(tid, xp, RED);
				rotateRight(tid, xpp, xp);

				xpp = selfOf(sib);

				sib = selfOf(leftOf(xp));
			}

			if (colorOf(rightOf(sib)) == BLACK &&
				colorOf(leftOf(sib)) == BLACK) {
				setColor(tid, sib, RED);
				return 1;
			}
			else {
				if (colorOf(leftOf(sib)) == BLACK) {
					setColor(tid, rightOf(sib), BLACK);
					setColor(tid, sib, RED);
					rotateLeft(tid, xp, sib);

					sib = selfOf(leftOf(xp));
				}
				setColor(tid, sib, colorOf(xp));
				setColor(tid, xp, BLACK);
				setColor(tid, leftOf(sib), BLACK);
				rotateRight(tid, xpp, xp);
				// TODO: consider break ...
				return 1337;
			}
		}
	}

	int delete_recursive(
		const int & tid, 
		rb_node<skey_t, sval_t>* tpp,
		rb_node<skey_t, sval_t>* tp, 
		rb_node<skey_t, sval_t>* t, 
		skey_t k,
		rb_node<skey_t, sval_t>** deleted)
	{
		skey_t cmp = k - t->get_key();

		if (cmp == 0)
		{
			just_duplicate_node(tid, t);
			auto deleted_key = t->get_key();
			auto deleted_val = t->get_value();
			if (t->get_child(LEFT) != NULL && t->get_child(RIGHT) != NULL)
			{
				rb_node<skey_t, sval_t>* tr = t->get_child(RIGHT);
				int res = successor_recursive(tid, tp, t, tr, deleted);

				// if (*deleted == NULL)
				// {
				// 	std::cout << "error!!!" << std::endl;
				// 	// exit(-1);
				// }
				
				just_duplicate_node(tid, *deleted);
				auto t_dup = dup_prologue(tid, t);
				if (t_dup != nullptr) {
					t_dup->set_key((*deleted)->get_key());
					t_dup->set_value((*deleted)->get_value());
					dup_epilogue(tid, t, t_dup);
				}
				
				// (*deleted)->k = deleted_key;
				// (*deleted)->v = deleted_val;

				if (res == 1337 || res == 1338)
				{
					return res;
				}
				else if (res - 1 > 0)
				{
					return res - 1;
				}
				else
				{
					rb_node<skey_t, sval_t>* xppp = tpp;
					rb_node<skey_t, sval_t>* xpp = tp;
					rb_node<skey_t, sval_t>* xp = t;
					rb_node<skey_t, sval_t>* x = tr;
				
					just_duplicate_node(tid, xppp);
					just_duplicate_node(tid, xpp);
					just_duplicate_node(tid, xp);
					just_duplicate_node(tid, x);
				
					if (xp->get_color() == BLACK)
					{
						return fix_rec_delete(tid, xppp, xpp, x);
					}
					else
					{
						setColor(tid, xp, BLACK);
						return 1337;
					}
				}
			}
			else
			{
				*deleted = t;
			
				rb_node<skey_t, sval_t>* xppp = tpp;
				rb_node<skey_t, sval_t>* xpp = tp;
				rb_node<skey_t, sval_t>* xp = t;
				rb_node<skey_t, sval_t>* x = (t->get_child(LEFT) != NULL) ? t->get_child(LEFT) : t->get_child(RIGHT);
			
				just_duplicate_node(tid, xppp);
				just_duplicate_node(tid, xpp);
				just_duplicate_node(tid, xp);
				just_duplicate_node(tid, x);
			
				if (x != NULL) {
					if (xpp == NULL)
					{
						auto root_dup = dup_prologue(tid, root);
						if (root_dup != nullptr) {
							root_dup = selfOf(x);
							dup_epilogue(tid, root, root_dup);
						}
			
						root = root_dup;
					}
					else if (selfOf(xp) == selfOf(xpp->get_child(LEFT)))
					{
						auto xpp_dup = dup_prologue(tid, xpp);
						if (xpp_dup != nullptr) {
							xpp_dup->set_child(LEFT, x);
							dup_epilogue(tid, xpp, xpp_dup);
						}
					}
					else
					{
						auto xpp_dup = dup_prologue(tid, xpp);
						if (xpp_dup != nullptr) {
							xpp_dup->set_child(RIGHT, x);
							dup_epilogue(tid, xpp, xpp_dup);
						}
					}
					
					auto xp_dup = dup_prologue(tid, xp);
					if (xp_dup != nullptr) {
						xp_dup->set_child(LEFT, NULL);
						xp_dup->set_child(RIGHT, NULL);
						xp_dup->set_parent(NULL);
						dup_epilogue(tid, xp, xp_dup);
					}
			
					// Fix replacement
					if (xp->get_color() == BLACK)
					{
						return fix_rec_delete(tid, xppp, xpp, x);
					}
					else
					{
						return 1337;
					}
				}
				else {
					int res = 1337;
					if (xp->get_color() == BLACK)
					{
						res = fix_rec_delete(tid, xppp, xpp, xp);
					}
				
					if (xpp != NULL) {
						if (selfOf(xp) == selfOf(xpp->get_child(LEFT)))
						{
							auto xpp_dup = dup_prologue(tid, xpp);
							if (xpp_dup != nullptr) {
								xpp_dup->set_child(LEFT, NULL);
								dup_epilogue(tid, xpp, xpp_dup);
							}
						}
						else if (selfOf(xp) == selfOf(xpp->get_child(RIGHT)))
						{
							auto xpp_dup = dup_prologue(tid, xpp);
							if (xpp_dup != nullptr) {
								xpp_dup->set_child(RIGHT, NULL);
								dup_epilogue(tid, xpp, xpp_dup);
							}
						}
					}
				
					return res;
				}
			}
		}
		else if (cmp < 0)
		{
			rb_node<skey_t, sval_t>* tl = t->get_child(LEFT);
			if (tl != NULL)
			{
				int res = delete_recursive(tid, tp, t, tl, k, deleted);
				if (res == 1337 || res == 1338)
				{
					return res;
				}
				else if (res - 1 > 0)
				{
					return res - 1;
				}
				else
				{
					rb_node<skey_t, sval_t>* xppp = tpp;
					rb_node<skey_t, sval_t>* xpp = tp;
					rb_node<skey_t, sval_t>* xp = t;
					rb_node<skey_t, sval_t>* x = tl;
		
					just_duplicate_node(tid, xppp);
					just_duplicate_node(tid, xpp);
					just_duplicate_node(tid, xp);
					just_duplicate_node(tid, x);
					
					if (xp->get_color() == BLACK)
					{
						return fix_rec_delete(tid, xppp, xpp, xp);
					}
					else
					{
						setColor(tid, xp, BLACK);
						return 1337;
					}
				}
			}
			else
			{
				return 1338;
			}
		}
		else
		{
			rb_node<skey_t, sval_t>* tr = t->get_child(RIGHT);
			if (tr != NULL)
			{
				int res = delete_recursive(tid, tp, t, tr, k, deleted);
				if (res == 1337 || res == 1338)
				{
					return res;
				}
				else if (res - 1 > 0)
				{
					return res - 1;
				}
				else
				{
					rb_node<skey_t, sval_t>* xppp = tpp;
					rb_node<skey_t, sval_t>* xpp = tp;
					rb_node<skey_t, sval_t>* xp = t;
					rb_node<skey_t, sval_t>* x = tr;
		
					just_duplicate_node(tid, xppp);
					just_duplicate_node(tid, xpp);
					just_duplicate_node(tid, xp);
					just_duplicate_node(tid, x);
		
					if (xp->get_color() == BLACK)
					{
						return fix_rec_delete(tid, xppp, xpp, xp);
					}
					else
					{
						setColor(tid, xp, BLACK);
						return 1337;
					}
				}
			}
			else
			{
				return 1338;
			}
		}
	}

	void fixAfterDeletion(const int & tid, rb_node<skey_t, sval_t> *  x) {
		while (x != root && colorOf(x) == BLACK) {
			if (x->get_self() == leftOf(parentOf(x))) {
				rb_node<skey_t, sval_t> * sib = rightOf(parentOf(x));
				if (colorOf(sib) == RED) {
					setColor(tid, sib, BLACK);
					setColor(tid, parentOf(x), RED);
					rotateLeft(tid, parentOf(parentOf(x)), parentOf(x));
					sib = rightOf(parentOf(x));
				}

				if (colorOf(leftOf(sib))  == BLACK &&
					colorOf(rightOf(sib)) == BLACK) {
					setColor(tid, sib,  RED);
					x = parentOf(x);
				} else {
					if (colorOf(rightOf(sib)) == BLACK) {
						setColor(tid, leftOf(sib), BLACK);
						setColor(tid, sib, RED);
						rotateRight(tid, parentOf(sib), sib);
						sib = rightOf(parentOf(x));
					}
					setColor(tid, sib, colorOf(parentOf(x)));
					setColor(tid, parentOf(x), BLACK);
					setColor(tid, rightOf(sib), BLACK);
					rotateLeft(tid, parentOf(parentOf(x)), parentOf(x));
					// TODO: consider break ...
					x = root; 
				}
			} else { // symmetric
				rb_node<skey_t, sval_t> * sib = leftOf(parentOf(x));

				if (colorOf(sib) == RED) {
					setColor(tid, sib, BLACK);
					setColor(tid, parentOf(x), RED);
					rotateRight(tid, parentOf(parentOf(x)), parentOf(x));
					sib = leftOf(parentOf(x));
				}

				if (colorOf(rightOf(sib)) == BLACK &&
					colorOf(leftOf(sib)) == BLACK) {
					setColor(tid, sib,  RED);
					x = parentOf(x);
				} else {
					if (colorOf(leftOf(sib)) == BLACK) {
						setColor(tid, rightOf(sib), BLACK);
						setColor(tid, sib, RED);
						rotateLeft(tid, parentOf(sib), sib);
						sib = leftOf(parentOf(x));
					}
					setColor(tid, sib, colorOf(parentOf(x)));
					setColor(tid, parentOf(x), BLACK);
					setColor(tid, leftOf(sib), BLACK);
					rotateRight(tid, parentOf(parentOf(x)), parentOf(x));
					// TODO: consider break ...
					x = root; 
				}
			}
		}

		if (x != NULL && x->get_color() != BLACK) {
			auto x_dup = dup_prologue(tid, x);
			if (x_dup != nullptr) {
				x_dup->set_color(BLACK); 
				dup_epilogue(tid, x, x_dup);
			}
		}
	}

	rb_node<skey_t, sval_t> * _delete (const int & tid, rb_node<skey_t, sval_t> * p) { 
		// If strictly internal, copy successor's element to p and then make p
		// point to successor.
		if (p->get_child(LEFT) != NULL && p->get_child(RIGHT) != NULL) {
			rb_node<skey_t, sval_t> * s = _successor (p);
			auto p_dup = dup_prologue(tid, p);
			if (p_dup != nullptr) {
				p_dup->set_key(s->get_key());
				p_dup->set_value(s->get_value());
				dup_epilogue(tid, p, p_dup);
			}
			p = s;
		} // p has 2 children

		// Start fixup at replacement node, if it exists.
		rb_node<skey_t, sval_t> * replacement = (p->get_child(LEFT) != NULL) ? p->get_child(LEFT) : p->get_child(RIGHT);

		if (replacement != NULL) {
			// Link replacement to parent
			// TODO: precompute pp = p->p and substitute below ...
			auto replacement_dup = dup_prologue(tid, replacement);
			if (replacement_dup != nullptr) {
				replacement_dup->set_parent(p->get_parent()); 
				dup_epilogue(tid, replacement, replacement_dup);
			}

			rb_node<skey_t, sval_t> * pp = p->get_parent(); 
			if (pp == NULL) {
				auto root_dup = dup_prologue(tid, root);
				if (root_dup != nullptr) {
					root_dup = replacement_dup;
					dup_epilogue(tid, root, root_dup);
				}

				root = root_dup;
			}
			else if (p == pp->get_child(LEFT)) {
				auto pp_dup = dup_prologue(tid, pp);
				if (pp_dup != nullptr) {
					pp_dup->set_child(LEFT, replacement);
					dup_epilogue(tid, pp, pp_dup);
				}
			} 
			else {
				auto pp_dup = dup_prologue(tid, pp);
				if (pp_dup != nullptr) {
					pp_dup->set_child(RIGHT, replacement); 
					dup_epilogue(tid, pp, pp_dup);
				}
			}

			// Null out links so they are OK to use by fixAfterDeletion.
			auto p_dup = dup_prologue(tid, p);
			if (p_dup != nullptr) {
				p_dup->set_child(LEFT, NULL); 
				p_dup->set_child(RIGHT, NULL); 
				p_dup->set_parent(NULL); 
				dup_epilogue(tid, p, p_dup);
			}

			// Fix replacement
			if (p->get_color() == BLACK)
				fixAfterDeletion(tid, replacement);
			} else if (p->get_parent() == NULL) { // return if we are the only node.
				auto root_dup = dup_prologue(tid, root);
				if (root_dup != nullptr) {
					root_dup = NULL;
					dup_epilogue(tid, root, root_dup);
				}

				root = root_dup;
			} else { //  No children. Use self as phantom replacement and unlink.
				if (p->get_color() == BLACK)
					fixAfterDeletion(tid, p);

			rb_node<skey_t, sval_t> * pp = p->get_parent(); 
			if (pp != NULL) {
				if (p == pp->get_child(LEFT)) {
					auto pp_dup = dup_prologue(tid, pp);
					if (pp_dup != nullptr) {
						pp_dup->set_child(LEFT, NULL); 
						dup_epilogue(tid, pp, pp_dup);
					}
				}
				else if (p == pp->get_child(RIGHT)) {
					auto pp_dup = dup_prologue(tid, pp);
					if (pp_dup != nullptr) {
						pp_dup->set_child(RIGHT, NULL); 
						dup_epilogue(tid, pp, pp_dup);
					}
				}

				auto p_dup = dup_prologue(tid, p);
				if (p_dup != nullptr) {
					p_dup->set_parent(NULL); 
					dup_epilogue(tid, p, p_dup);
				}
			}
		}
		return p ; 
	}

	rb_node<skey_t, sval_t> * GetNode (const int & tid) {
		rb_node<skey_t, sval_t> * result = 
			(rb_node<skey_t, sval_t> *)recmgr->template allocate<rb_node<skey_t, sval_t>>(tid);
		pthread_spin_init(&result->dup_lock, PTHREAD_PROCESS_PRIVATE);
		return result; 
	}

	rb_node<skey_t, sval_t> * GetNode (const int & tid, rb_node<skey_t, sval_t> * node) {
		rb_node<skey_t, sval_t> * result = 
			(rb_node<skey_t, sval_t> *)recmgr->template allocate<rb_node<skey_t, sval_t>>(tid);
		std::memcpy((void *)result, (void *)node, sizeof(rb_node<skey_t, sval_t>));
		pthread_spin_init(&result->dup_lock, PTHREAD_PROCESS_PRIVATE);
		return result; 
	}

	void ReleaseNode (const int & tid, rb_node<skey_t, sval_t> * n) { 
		recmgr->deallocate(tid, n);
	}

public:
	rb_tree(
		const int _NUM_THREADS, 
		const skey_t& _KEY_MIN, 
		const skey_t& _KEY_MAX, 
		const sval_t& _VALUE_RESERVED, 
		unsigned int id): 
		NUM_THREADS(_NUM_THREADS), 
		KEY_MIN(_KEY_MIN), 
		KEY_MAX(_KEY_MAX), 
		NO_VALUE(_VALUE_RESERVED), 
		root(nullptr),
		recmgr(new RecMgr(NUM_THREADS))
	{
		const int & tid = 0;
		initThread(tid);
		recmgr->endOp(tid);
	}

	virtual ~rb_tree()
	{
		make_empty(root);
		delete recmgr;
	}

	void initThread(const int & tid)
	{
		if (init[tid]) return;
		else init[tid] = !init[tid];
		recmgr->initThread(tid);
	}

	void deinitThread(const int & tid)
	{
		if (!init[tid]) return;
		else init[tid] = !init[tid];
		recmgr->deinitThread(tid);
	}

	void make_empty(rb_node<skey_t, sval_t> * t) 
	{
		if (t == nullptr)
			return;

		make_empty(t->get_child(LEFT));
		make_empty(t->get_child(RIGHT));
		delete t;
	}

	RecMgr * debugGetRecMgr()
	{
		return recmgr;
	}

	rb_node<skey_t, sval_t> * get_root()
	{
		return root;
	}

	sval_t rb_insert(const int & tid, skey_t Key, sval_t Val) {
		rb_node<skey_t, sval_t> * node = GetNode(tid); 
		__transaction_atomic { int res = insert_rec(tid, Key, Val, node);

		if (res == 1338) {
			ReleaseNode (tid, node);
			return Val;
		}
		else {
			return NO_VALUE;
		} }
	}

	sval_t rb_tm_insert(const int & tid, skey_t Key, sval_t Val) {
		auto guard = recmgr->getGuard(tid);
		auto insertion_res = rb_insert(tid, Key, Val);
		return insertion_res;
	}

	sval_t rb_delete(const int & tid, skey_t Key) {
		rb_node<skey_t, sval_t> * node = NULL;

		__transaction_atomic { if (node == NULL) { 
			node = delete_rec(tid, Key);
		}

		if (node != NULL) {
			ReleaseNode(tid, node);
			return node->v;
		}
		else {
			return NO_VALUE;
		} }
	}

	sval_t rb_tm_delete(const int & tid, skey_t Key) {
		auto guard = recmgr->getGuard(tid);
		auto removal_res = rb_delete(tid, Key);
		return removal_res;
	}

	sval_t rb_contains (const int & tid, skey_t Key) {
		rb_node<skey_t, sval_t> * n = _lookup(Key);
		if (n != NULL) {
			return n->v;
		}
		else {
			return NO_VALUE;
		}
	}

	sval_t rb_tm_contains (const int & tid, skey_t Key) {
		auto guard = recmgr->getGuard(tid, true);
		__transaction_atomic { return rb_contains(tid, Key); }
	}
};