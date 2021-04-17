#pragma once

#include "rb_node.h" 
#include <mutex>

std::mutex g_mutex;

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

	// PC operations
	rb_node<skey_t, sval_t> * path_copy(const int& tid, rb_node<skey_t, sval_t> * orig)
    {
        if (allocated->find(orig) != allocated->end())
        {
            return orig;
        }

        if (duplications->find(orig) != duplications->end())
        {
            return duplications->at(orig);
        }

        rb_node<skey_t, sval_t> * duplication = GetNode(tid, orig);
        duplications->insert({orig, duplication});
		dup_orig_map->insert({duplication, orig});

        rb_node<skey_t, sval_t> * current = orig;
        rb_node<skey_t, sval_t> * current_dup = duplication;
        rb_node<skey_t, sval_t> * parent;
        rb_node<skey_t, sval_t> * parent_dup;

        bool reached_root = false;
        std::pair<rb_node<skey_t, sval_t>*, unsigned int> pair;
        while (!(reached_root = (node_parent_map->find(current) == node_parent_map->end())) &&
            (pair = node_parent_map->at(current), duplications->find(pair.first) == duplications->end()))
        {
            parent = pair.first;
            auto child_idx = pair.second;
            parent_dup = GetNode(tid, parent);
			if (child_idx == LEFT)
				parent_dup->l = current_dup;
			else
				parent_dup->r = current_dup;
            
            duplications->insert({ (rb_node<skey_t, sval_t> *)parent, (rb_node<skey_t, sval_t> *)parent_dup });
			dup_orig_map->insert({(rb_node<skey_t, sval_t> *)parent_dup, (rb_node<skey_t, sval_t> *)parent});

            current = (rb_node<skey_t, sval_t> *)parent;
            current_dup = (rb_node<skey_t, sval_t> *)parent_dup;
        }

        if (reached_root)
        {
            new_root = current_dup;
        }
        else // reached a duplicated parent
        {
            rb_node<skey_t, sval_t> * parent = node_parent_map->at(current).first;
            auto child_idx = node_parent_map->at(current).second;
            rb_node<skey_t, sval_t> * to_update = duplications->at(parent);
			if (child_idx == LEFT && to_update && to_update->l == current)
				to_update->l = current_dup;
			else if (child_idx == RIGHT && to_update && to_update->r == current)
				to_update->r = current_dup;
        }

        pc_happened = true;
        return duplication;
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

		just_duplicate_node(tid, r);
		just_duplicate_node(tid, rl);
		just_duplicate_node(tid, x);

		auto x_dup = path_copy(tid, x);
		if (x_dup != nullptr) {
			x_dup->set_child(RIGHT, selfOf(rl));
		}

		auto r_dup = path_copy(tid, r);		//
		if (r_dup != nullptr) {				//
			r_dup->set_child(LEFT, selfOf(x)); 		//
		}									//

		if (xp == NULL) {
			auto root_dup = path_copy(tid, orig_root);
			if (root_dup != nullptr) {
				if (duplications->find(r) == duplications->end())
					root_dup = r;
				else
					root_dup = duplications->at(r);
			}

			new_root = root_dup;
		}

		else if (selfOf(xp->get_child(LEFT)) == selfOf(x)) {
			auto xp_dup = path_copy(tid, xp);
			if (xp_dup != nullptr) {
				xp_dup->set_child(LEFT, selfOf(r));
			}
		}
		else {
			auto xp_dup = path_copy(tid, xp);
			if (xp_dup != nullptr) {
				xp_dup->set_child(RIGHT, selfOf(r));
			} 
		}
	}

	void rotateRight (const int & tid, rb_node<skey_t, sval_t> * xp, rb_node<skey_t, sval_t> * x)
	{
		rb_node<skey_t, sval_t> * l = leftOf(x);
		rb_node<skey_t, sval_t> * lr = rightOf(l);

		just_duplicate_node(tid, l);
		just_duplicate_node(tid, lr);
		just_duplicate_node(tid, x);

		auto x_dup = path_copy(tid, x);
		if (x_dup != nullptr) {
			x_dup->set_child(LEFT, selfOf(lr));
		}

		auto l_dup = path_copy(tid, l);		//
		if (l_dup != nullptr) {				//
			l_dup->set_child(RIGHT, selfOf(x));		//
		}									//
		
		auto rxp = (xp != NULL) ? xp->get_child(RIGHT) : NULL;
		if (xp == NULL) {
			auto root_dup = path_copy(tid, orig_root);
			if (root_dup != nullptr) {
				if (duplications->find(l) == duplications->end())
					root_dup = l;
				else
					root_dup = duplications->at(l);
			}

			new_root = root_dup;
		}
		else if (selfOf(rxp) == selfOf(x)) {
			auto xp_dup = path_copy(tid, xp);
			if (xp_dup != nullptr) {
				xp_dup->set_child(RIGHT, selfOf(l));
			}
		}
		else { 
			auto xp_dup = path_copy(tid, xp);
			if (xp_dup != nullptr) {
				xp_dup->set_child(LEFT, selfOf(l));
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
			auto n_dup = path_copy(tid, n);
			if (n_dup != nullptr) {
				n_dup->set_color(c); 
			}
		}
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
		print_tree_helper(new_root, 0, true);
	}

	int count_keys(rb_node<skey_t, sval_t>* currNode)
	{
		if (currNode == NULL)
			return 0;
		
		int lres = count_keys(currNode->get_child(LEFT));
		int rres = count_keys(currNode->get_child(RIGHT));

		return lres + rres + 1;
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

	int insert_rec(const int & tid, skey_t k, sval_t v, rb_node<skey_t, sval_t>* n)
	{
		if (orig_root == NULL) {
			if (n == NULL) return 0;
			// Note: the following STs don't really need to be transactional.  

			auto n_dup = n;//path_copy(tid, n);
			if (n_dup != nullptr) {
				n_dup->set_child(LEFT, NULL);
				n_dup->set_child(RIGHT, NULL);
				n_dup->set_parent(NULL);
				n_dup->set_key(k); 
				n_dup->set_value(v); 
				n_dup->set_color(BLACK);
			}			

			// new_root = n_dup;
			root = n_dup;
			return 0;
		}

		int res = insert_recursive(tid, NULL, orig_root, k, v, n);
		rb_node<skey_t, sval_t>* ro = new_root;
		if (colorOf(ro) != BLACK) {
		// if (ro->get_color() != BLACK) {
			auto ro_dup = path_copy(tid, ro);
			if (ro_dup != nullptr) {
				ro_dup->set_color(BLACK);
			}
			new_root = ro_dup;
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

		// if (xppp) {
		// 	auto bla = path_copy(tid, xppp);
		// }
		// if (xpp) {
		// 	auto bla = path_copy(tid, xpp);
		// }
		// if (xp) {
		// 	auto bla = path_copy(tid, xp);
		// }
		// if (x) {
		// 	auto bla = path_copy(tid, x);
		// }

		if (selfOf(xp) == selfOf(leftOf(xpp))) {
			rb_node<skey_t, sval_t>* y = rightOf(xpp);
			if (colorOf(y) == RED) {
				setColor(tid, xp, BLACK);
				setColor(tid, y, BLACK);
				setColor(tid, xpp, RED);
				if (xppp == NULL) {
					new_root = selfOf(xpp);
				}
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
				else
				{
					new_root = selfOf(xp);
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
				if (xppp == NULL) {
					new_root = selfOf(xpp);
				}
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
				else
				{
					new_root = selfOf(xp);
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
				auto n_dup = path_copy(tid, n);
				if (n_dup != nullptr) {
					n_dup->set_child(LEFT, NULL);
					n_dup->set_child(RIGHT, NULL);
					n_dup->set_key(k); 
					n_dup->set_value(v); 
					n_dup->set_color(RED);
				}

				auto t_dup = path_copy(tid, t);
				if (t_dup != nullptr) {
					t_dup->set_child(LEFT, selfOf(n));
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
				auto n_dup = path_copy(tid, n);
				if (n_dup != nullptr) {
					n_dup->set_child(LEFT, NULL);
					n_dup->set_child(RIGHT, NULL);
					n_dup->set_key(k); 
					n_dup->set_value(v); 
					n_dup->set_color(RED);
				}

				auto t_dup = path_copy(tid, t);
				if (t_dup != nullptr) {
					t_dup->set_child(RIGHT, selfOf(n));
				}

				return 1;
			}
		}
	}

	void fixAfterInsertion(const int & tid, rb_node<skey_t, sval_t> * x) {
		auto x_dup = path_copy(tid, x);
		if (x_dup != nullptr) {
			x_dup->set_color(RED);
		}
		
		while (x != NULL && x != new_root) { 
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
		rb_node<skey_t, sval_t> * ro = new_root;//orig_root; 
		if (ro->get_color() != BLACK) {
			auto ro_dup = path_copy(tid, ro);
			if (ro_dup != nullptr) {
				ro_dup->set_color(BLACK);
			}
		}
	}

	// _insert() has putIfAbsent() semantics
	// If the key already exists in the tree _insert() returns a pointer to the 
	// node bearing that key and does not modify the tree, otherwise if the key
	// is not in the tree it inserts (k,v) into the tree using node n.

	rb_node<skey_t, sval_t> *  _insert (const int & tid, skey_t k, sval_t v, rb_node<skey_t, sval_t> * n) { 
		rb_node<skey_t, sval_t> * t = orig_root; 
		if (t == NULL) {
			if (n == NULL) return NULL ; 
			// Note: the following STs don't really need to be transactional.
			auto n_dup = path_copy(tid, n);
			if (n_dup != nullptr) {
				n_dup->set_child(LEFT, NULL);
				n_dup->set_child(RIGHT, NULL);
				n_dup->set_parent(NULL);
				n_dup->set_key(k); 
				n_dup->set_value(v); 
				n_dup->set_color(BLACK);
			}			

			new_root = n_dup;
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
					auto n_dup = path_copy(tid, n);
					if (n_dup != nullptr) {
						n_dup->set_child(LEFT, NULL);
						n_dup->set_child(RIGHT, NULL);
						n_dup->set_key(k); 
						n_dup->set_value(v); 
						n_dup->set_parent(t);
					}

					auto t_dup = path_copy(tid, t);
					if (t_dup != nullptr) {
						t_dup->set_child(LEFT, selfOf(n));
					}

					fixAfterInsertion(tid, n);
					return NULL ;
				}
			} else { // cmp > 0
				rb_node<skey_t, sval_t> * tr = t->get_child(RIGHT); 
				if (tr != NULL) {
					t = tr;
				} else {
					auto n_dup = path_copy(tid, n);
					if (n_dup != nullptr) {
						n_dup->set_child(LEFT, NULL);
						n_dup->set_child(RIGHT, NULL);
						n_dup->set_key(k); 
						n_dup->set_value(v); 
						n_dup->set_parent(t);
					}

					auto t_dup = path_copy(tid, t);
					if (t_dup != nullptr) {
						t_dup->set_child(RIGHT, selfOf(n));
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
			auto bla = path_copy(tid, n);
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
					auto root_dup = path_copy(tid, orig_root);
					if (root_dup != nullptr) {
						root_dup = selfOf(x);
					}
					
					new_root = root_dup;
				}
				else if (selfOf(xp) == selfOf(xpp->get_child(LEFT)))
				{
					auto xpp_dup = path_copy(tid, xpp);
					if (xpp_dup != nullptr) {
						xpp_dup->set_child(LEFT, selfOf(x));
					}
					if (xppp == NULL)
						new_root = selfOf(xpp);
				}
				else
				{
					auto xpp_dup = path_copy(tid, xpp);
					if (xpp_dup != nullptr) {
						xpp_dup->set_child(RIGHT, selfOf(x));
					}
					if (xppp == NULL)
						new_root = selfOf(xpp);
				}

				auto xp_dup = path_copy(tid, xp);
				if (xp_dup != nullptr) {
					xp_dup->set_child(LEFT, NULL);
					xp_dup->set_child(RIGHT, NULL);
					xp_dup->set_parent(NULL);
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
						auto xpp_dup = path_copy(tid, xpp);
						if (xpp_dup != nullptr) {
							xpp_dup->set_child(LEFT, NULL);
						}
						if (xppp == NULL)
							new_root = selfOf(xpp);
					}
					else if (selfOf(xp) == selfOf(xpp->get_child(RIGHT)))
					{
						auto xpp_dup = path_copy(tid, xpp);
						if (xpp_dup != nullptr) {
							xpp_dup->set_child(RIGHT, NULL);
						}
						if (xppp == NULL)
							new_root = selfOf(xpp);
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
					if (xpp == NULL)
						new_root = selfOf(xp);
					return 1337;
				}
			}
		}
	}

	rb_node<skey_t, sval_t>* delete_rec(const int & tid, skey_t k)
	{
		rb_node<skey_t, sval_t>* t = orig_root;

		if (t == NULL)
			return NULL;

		rb_node<skey_t, sval_t>* deleted = NULL;

		int res = delete_recursive(tid, NULL, NULL, orig_root, k, &deleted);
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
			rb_node<skey_t, sval_t>* sib = selfOf(rightOf(xp));
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
			rb_node<skey_t, sval_t>* sib = selfOf(leftOf(xp));
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

				if (*deleted == NULL)
				{
					std::cout << "error!!!" << std::endl;
					exit(-1);
				}

				just_duplicate_node(tid, *deleted);
				auto t_dup = path_copy(tid, t);
				if (t_dup != nullptr) {
					t_dup->set_key((*deleted)->get_key());
					t_dup->set_value((*deleted)->get_value());
				}
				if (tp == NULL)
					new_root = selfOf(t);

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
						auto root_dup = path_copy(tid, orig_root);
						if (root_dup != nullptr) {
							root_dup = selfOf(x);
						}

						new_root = root_dup;
					}
					else if (selfOf(xp) == selfOf(xpp->get_child(LEFT)))
					{
						auto xpp_dup = path_copy(tid, xpp);
						if (xpp_dup != nullptr) {
							xpp_dup->set_child(LEFT, selfOf(x));
						}
						if (xppp == NULL)
							new_root = selfOf(xpp);
					}
					else
					{
						auto xpp_dup = path_copy(tid, xpp);
						if (xpp_dup != nullptr) {
							xpp_dup->set_child(RIGHT, selfOf(x));
						}
						if (xppp == NULL)
							new_root = selfOf(xpp);
					}

					auto xp_dup = path_copy(tid, xp);
					if (xp_dup != nullptr) {
						xp_dup->set_child(LEFT, NULL);
						xp_dup->set_child(RIGHT, NULL);
						xp_dup->set_parent(NULL);
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
							auto xpp_dup = path_copy(tid, xpp);
							if (xpp_dup != nullptr) {
								xpp_dup->set_child(LEFT, NULL);
							}
							if (xppp == NULL)
								new_root = selfOf(xpp);
						}
						else if (selfOf(xp) == selfOf(xpp->get_child(RIGHT)))
						{
							auto xpp_dup = path_copy(tid, xpp);
							if (xpp_dup != nullptr) {
								xpp_dup->set_child(RIGHT, NULL);
							}
							if (xppp == NULL)
								new_root = selfOf(xpp);
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
						if (xpp == NULL)
							new_root = selfOf(xp);
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
						if (xpp == NULL)
							new_root = selfOf(xp);
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
		while (x != orig_root && colorOf(x) == BLACK) {
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
					x = orig_root; 
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
					x = orig_root; 
				}
			}
		}

		if (x != NULL && x->get_color() != BLACK) {
			auto x_dup = path_copy(tid, x);
			if (x_dup != nullptr) {
				x_dup->set_color(BLACK);
			}
		}
	}

	rb_node<skey_t, sval_t> * _delete (const int & tid, rb_node<skey_t, sval_t> * p) { 
		// If strictly internal, copy successor's element to p and then make p
		// point to successor.
		if (p->get_child(LEFT) != NULL && p->get_child(RIGHT) != NULL) {
			rb_node<skey_t, sval_t> * s = _successor (p);
			auto p_dup = path_copy(tid, p);
			if (p_dup != nullptr) {
				p_dup->set_key(s->get_key());
				p_dup->set_value(s->get_value());
			}
			p = s;
		} // p has 2 children

		// Start fixup at replacement node, if it exists.
		rb_node<skey_t, sval_t> * replacement = (p->get_child(LEFT) != NULL) ? p->get_child(LEFT) : p->get_child(RIGHT);

		if (replacement != NULL) {
			// Link replacement to parent
			// TODO: precompute pp = p->p and substitute below ...
			auto replacement_dup = path_copy(tid, replacement);
			if (replacement_dup != nullptr) {
				replacement_dup->set_parent(p->get_parent()); 
			}

			rb_node<skey_t, sval_t> * pp = p->get_parent(); 
			if (pp == NULL) {
				auto root_dup = path_copy(tid, orig_root);
				if (root_dup != nullptr) {
					root_dup = replacement_dup;
				}

				new_root = root_dup;
			}
			else if (p == pp->get_child(LEFT)) {
				auto pp_dup = path_copy(tid, pp);
				if (pp_dup != nullptr) {
					pp_dup->set_child(LEFT, selfOf(replacement));
				}
			} 
			else {
				auto pp_dup = path_copy(tid, pp);
				if (pp_dup != nullptr) {
					pp_dup->set_child(RIGHT, selfOf(replacement));
				}
			}

			// Null out links so they are OK to use by fixAfterDeletion.
			auto p_dup = path_copy(tid, p);
			if (p_dup != nullptr) {
				p_dup->set_child(LEFT, NULL); 
				p_dup->set_child(RIGHT, NULL); 
				p_dup->set_parent(NULL);
			}

			// Fix replacement
			if (p->get_color() == BLACK)
				fixAfterDeletion(tid, replacement);
			} else if (p->get_parent() == NULL) { // return if we are the only node.
				auto root_dup = path_copy(tid, orig_root);
				if (root_dup != nullptr) {
					root_dup = NULL;
				}

				new_root = root_dup;
			} else { //  No children. Use self as phantom replacement and unlink.
				if (p->get_color() == BLACK)
					fixAfterDeletion(tid, p);

			rb_node<skey_t, sval_t> * pp = p->get_parent(); 
			if (pp != NULL) {
				if (p == pp->get_child(LEFT)) {
					auto pp_dup = path_copy(tid, pp);
					if (pp_dup != nullptr) {
						pp_dup->set_child(LEFT, NULL);
					}
				}
				else if (p == pp->get_child(RIGHT)) {
					auto pp_dup = path_copy(tid, pp);
					if (pp_dup != nullptr) {
						pp_dup->set_child(RIGHT, NULL);
					}
				}

				auto p_dup = path_copy(tid, p);
				if (p_dup != nullptr) {
					p_dup->set_parent(NULL);
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
		// recmgr->deallocate(tid, n);
		duplications->insert({n, nullptr});
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
		// rb_node<skey_t, sval_t> * ex; 
		// ex = _insert(tid, Key, Val, node);
		int res = insert_rec(tid, Key, Val, node);

		// if (ex != NULL) {
		if (res == 1338) {
			ReleaseNode (tid, node); 
			// return ex->v;
			return Val;
		}
		else {
			return NO_VALUE;
		}
	}

	unsigned int tries = 0;
	unsigned int successfuls = 0;
	sval_t rb_pc_insert(const int & tid, skey_t Key, sval_t Val) {
		// unsigned int temp = 0;
		while (1)
        {
			// temp++;
            auto guard = recmgr->getGuard(tid);
            pc_open<skey_t, sval_t>(tid, &root);
            auto insertion_res = rb_insert(tid, Key, Val);
			
            if (pc_close<skey_t, sval_t>(tid, &root))
            {
                for (auto& d : *duplications)
                {
					recmgr->retire(tid, d.first);
                }

				// if (insertion_res == NO_VALUE)
				// {
				// 	int ttries = __atomic_fetch_add(&tries, temp, __ATOMIC_RELAXED);
				// 	int tsucc = __atomic_fetch_add(&successfuls, 1, __ATOMIC_RELAXED);
				// 	if (tsucc % 10000 == 1)
				// 	{
				// 		std::cout << ttries << "\t\t" << (float)ttries / tsucc << std::endl;
				// 	}
				// }
                
                return insertion_res;
            }
            else
            {
                for (auto& d : *allocated)
                {
					recmgr->deallocate(tid, d.first);
                }
            }
        }
	}

	sval_t rb_delete(const int & tid, skey_t Key) {
		rb_node<skey_t, sval_t> * node = NULL ;
		if (node == NULL) {
			node = delete_rec(tid, Key);
		}

		if (node != NULL) {
			ReleaseNode(tid, node);
			return node->v;
		}
		else {
			return NO_VALUE;
		}
	}
	
	sval_t rb_pc_delete(const int & tid, skey_t Key) {
		while (1)
		{
			auto guard = recmgr->getGuard(tid);
			pc_open<skey_t, sval_t>(tid, &root);
			auto removal_res = rb_delete(tid, Key);

			if (pc_close<skey_t, sval_t>(tid, &root))
			{
				for (auto& d : *duplications)
				{
					recmgr->retire(tid, d.first);
				}

				return removal_res;
			}
			else
			{
				for (auto& d : *allocated)
				{
					recmgr->deallocate(tid, d.first);
				}
			}
		}
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

	sval_t rb_pc_contains (const int & tid, skey_t Key)
	{
		auto guard = recmgr->getGuard(tid, true);
		return rb_contains(tid, Key);
	}
};