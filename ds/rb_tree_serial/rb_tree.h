#pragma once

#include "rb_node.h" 

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

	//  Balancing operations.
	// 
	//  Implementations of rebalancings during insertion and deletion are
	//  slightly different than the CLR version.  Rather than using dummy
	//  nilnodes, we use a set of accessors that deal properly with null.  They
	//  are used to avoid messiness surrounding nullness checks in the main
	//  algorithms.
	// 
	// From CLR 

	void rotateLeft (rb_node<skey_t, sval_t> * x) 
	{
		rb_node<skey_t, sval_t> * r = x->get_child(RIGHT);      // AKA r, y
		rb_node<skey_t, sval_t> * rl = r->get_child(LEFT); 
		x->set_child(RIGHT, rl);
		if (rl != NULL) { 
			rl->set_parent(x); 
		} 
		// TODO: compute p = xp = x->p.  Use xp for R-Values in following
		rb_node<skey_t, sval_t> * xp = x->get_parent(); 
		r->set_parent(xp); 
		if (xp == NULL)
			root = r;
		else if (xp->get_child(LEFT) == x)
			xp->set_child(LEFT, r); 
		else
			xp->set_child(RIGHT, r); 
		r->set_child(LEFT, x); 
		x->set_parent(r); 
	}

	void rotateRight (rb_node<skey_t, sval_t> * x) 
	{
		rb_node<skey_t, sval_t> * l = x->get_child(LEFT);      // AKA l,y
		rb_node<skey_t, sval_t> * lr = l->get_child(RIGHT) ; 
		x->set_child(LEFT, lr); 
		if (lr != NULL) {
			lr->set_parent(x);
		}
		// TODO: compute xp or p = x->p
		rb_node<skey_t, sval_t> * xp = x->get_parent(); 
		l->set_parent(xp); 
		if (xp == NULL)
			root = l;
		else if (xp->get_child(RIGHT) == x)
			xp->set_child(RIGHT, l);
		else 
			xp->set_child(LEFT, l);
		l->set_child(RIGHT, x);
		x->set_parent(l);
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

	inline int colorOf (rb_node<skey_t, sval_t> * n) { 
		return n ? (int)(n->get_color()) : BLACK ; 
	}

	inline void setColor (rb_node<skey_t, sval_t> * n, int c) { 
		if (n != NULL) n->set_color(c); 
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
			std::cout << "l: " << bla << "(" << bla->c << ")" << " [" << bla->k << "]" << std::endl;
		else
			std::cout << "r: " << bla << "(" << bla->c << ")" << " [" << bla->k << "]" << std::endl;
		if (bla->l != NULL)
			print_tree_helper(bla->l, n+1, true);
		if (bla->r != NULL)
			print_tree_helper(bla->r, n+1, false);
	}

	void print_tree()
	{
		print_tree_helper(root, 0, true);
	}

	void fixAfterInsertion(rb_node<skey_t, sval_t> * x) {
		x->set_color(RED); 

		while (x != NULL && x != root) { 
			rb_node<skey_t, sval_t> * xp = x->get_parent(); 
			if (xp->get_color() != RED) break ; 

			// TODO: cache g = ppx = parentOf(parentOf(x))
			if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
				rb_node<skey_t, sval_t> *  y = rightOf(parentOf(parentOf(x)));
				if (colorOf(y) == RED) {
					setColor(parentOf(x), BLACK);
					setColor(y, BLACK);
					setColor(parentOf(parentOf(x)), RED);
					x = parentOf(parentOf(x));
				} else {
					if (x == rightOf(parentOf(x))) {
						x = parentOf(x);
						rotateLeft(x);
					}
					setColor(parentOf(x), BLACK);
					setColor(parentOf(parentOf(x)), RED);
					if (parentOf(parentOf(x)) != NULL)
						rotateRight(parentOf(parentOf(x)));
				}
			} else {
				rb_node<skey_t, sval_t> * y = leftOf(parentOf(parentOf(x)));
				if (colorOf(y) == RED) {
					setColor(parentOf(x), BLACK);
					setColor(y, BLACK);
					setColor(parentOf(parentOf(x)), RED);
					x = parentOf(parentOf(x));
				} else {
					if (x == leftOf(parentOf(x))) {
						x = parentOf(x);
						rotateRight(x);
					}
					setColor(parentOf(x),  BLACK);
					setColor(parentOf(parentOf(x)), RED);
					if (parentOf(parentOf(x)) != NULL)
						rotateLeft(parentOf(parentOf(x)));
				}
			}
		}
		rb_node<skey_t, sval_t> * ro = root; 
		if (ro->get_color() != BLACK) {
		ro->set_color(BLACK); 
		}
	}

	// _insert() has putIfAbsent() semantics
	// If the key already exists in the tree _insert() returns a pointer to the 
	// node bearing that key and does not modify the tree, otherwise if the key
	// is not in the tree it inserts (k,v) into the tree using node n.

	rb_node<skey_t, sval_t> *  _insert (skey_t k, sval_t v, rb_node<skey_t, sval_t> * n) { 
		rb_node<skey_t, sval_t> * t  = root; 
		if (t == NULL) {
		if (n == NULL) return NULL ; 
		// Note: the following STs don't really need to be transactional.  
		n->set_child(LEFT, NULL);
		n->set_child(RIGHT, NULL);
		n->set_parent(NULL);
		n->set_key(k); 
		n->set_value(v); 
		n->set_color(BLACK); 
		root = n;
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
					n->set_child(LEFT, NULL);
					n->set_child(RIGHT, NULL);
					n->set_key(k); 
					n->set_value(v); 
					n->set_parent(t); 
					t->set_child(LEFT, n); 
					fixAfterInsertion(n);
					return NULL ;
				}
			} else { // cmp > 0
				rb_node<skey_t, sval_t> * tr = t->get_child(RIGHT); 
				if (tr != NULL) {
					t = tr;
				} else {
					n->set_child(LEFT, NULL);
					n->set_child(RIGHT, NULL);
					n->set_key(k); 
					n->set_value(v); 
					n->set_parent(t); 
					t->set_child(RIGHT, n); 
					fixAfterInsertion(n);
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

	void fixAfterDeletion(rb_node<skey_t, sval_t> *  x) {
		while (x != root && colorOf(x) == BLACK) {
			if (x == leftOf(parentOf(x))) {
				rb_node<skey_t, sval_t> * sib = rightOf(parentOf(x));
				if (colorOf(sib) == RED) {
					setColor(sib, BLACK);
					setColor(parentOf(x), RED);
					rotateLeft(parentOf(x));
					sib = rightOf(parentOf(x));
				}

				if (colorOf(leftOf(sib))  == BLACK &&
					colorOf(rightOf(sib)) == BLACK) {
					setColor(sib,  RED);
					x = parentOf(x);
				} else {
					if (colorOf(rightOf(sib)) == BLACK) {
						setColor(leftOf(sib), BLACK);
						setColor(sib, RED);
						rotateRight(sib);
						sib = rightOf(parentOf(x));
					}
					setColor(sib, colorOf(parentOf(x)));
					setColor(parentOf(x), BLACK);
					setColor(rightOf(sib), BLACK);
					rotateLeft(parentOf(x));
					// TODO: consider break ...
					x = root; 
				}
			} else { // symmetric
				rb_node<skey_t, sval_t> * sib = leftOf(parentOf(x));

				if (colorOf(sib) == RED) {
					setColor(sib, BLACK);
					setColor(parentOf(x), RED);
					rotateRight(parentOf(x));
					sib = leftOf(parentOf(x));
				}

				if (colorOf(rightOf(sib)) == BLACK &&
					colorOf(leftOf(sib)) == BLACK) {
					setColor(sib,  RED);
					x = parentOf(x);
				} else {
					if (colorOf(leftOf(sib)) == BLACK) {
						setColor(rightOf(sib), BLACK);
						setColor(sib, RED);
						rotateLeft(sib);
						sib = leftOf(parentOf(x));
					}
					setColor(sib, colorOf(parentOf(x)));
					setColor(parentOf(x), BLACK);
					setColor(leftOf(sib), BLACK);
					rotateRight(parentOf(x));
					// TODO: consider break ...
					x = root; 
				}
			}
		}

		if (x != NULL && x->get_color() != BLACK) {
		x->set_color(BLACK); 
		}
	}

	rb_node<skey_t, sval_t> * _delete (rb_node<skey_t, sval_t> * p) { 
		// If strictly internal, copy successor's element to p and then make p
		// point to successor.
		if (p->get_child(LEFT) != NULL && p->get_child(RIGHT) != NULL) {
			rb_node<skey_t, sval_t> * s = _successor (p);
			p->set_key(s->get_key());
			p->set_value(s->get_value());
			p = s;
		} // p has 2 children

		// Start fixup at replacement node, if it exists.
		rb_node<skey_t, sval_t> * replacement = (p->get_child(LEFT) != NULL) ? p->get_child(LEFT) : p->get_child(RIGHT);

		if (replacement != NULL) {
			// Link replacement to parent
			// TODO: precompute pp = p->p and substitute below ...
			replacement->set_parent(p->get_parent()); 
			rb_node<skey_t, sval_t> * pp = p->get_parent(); 
			if (pp == NULL)
				root = replacement; 
			else if (p == pp->get_child(LEFT)) 
				pp->set_child(LEFT, replacement); 
			else
				pp->set_child(RIGHT, replacement); 

			// Null out links so they are OK to use by fixAfterDeletion.
			p->set_child(LEFT, NULL); 
			p->set_child(RIGHT, NULL); 
			p->set_parent(NULL); 

			// Fix replacement
			if (p->get_color() == BLACK)
				fixAfterDeletion(replacement);
		} else if (p->get_parent() == NULL) { // return if we are the only node.
			root = NULL; 
		} else { //  No children. Use self as phantom replacement and unlink.
			if (p->get_color() == BLACK)
				fixAfterDeletion(p);

			rb_node<skey_t, sval_t> * pp = p->get_parent(); 
			if (pp != NULL) {
				if (p == pp->get_child(LEFT))
					pp->set_child(LEFT, NULL); 
				else if (p == pp->get_child(RIGHT))
					pp->set_child(RIGHT, NULL); 
				p->set_parent(NULL); 
			}
		}
		return p ; 
	}

	rb_node<skey_t, sval_t>  * GetNode (const int & tid) {
		rb_node<skey_t, sval_t>* result = 
			(rb_node<skey_t, sval_t>*)recmgr->template allocate<rb_node<skey_t, sval_t>>(tid);
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
		rb_node<skey_t, sval_t> * ex; 
		std::cout << "==================" << std::endl;
		print_tree();
		ex = _insert(Key, Val, node);
		print_tree();

		if (ex != NULL) {
			ReleaseNode (tid, node); 
			return ex->v;
		}
		else {
			return NO_VALUE;
		}
	}

	sval_t rb_delete(const int & tid, skey_t Key) {
		rb_node<skey_t, sval_t> * node = NULL ; 
		node = _lookup(Key);
		if (node != NULL) { 
			node = _delete(node);
		}

		if (node != NULL) {
			ReleaseNode(tid, node);
			return node->v;
		}
		else {
			return NO_VALUE;
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
};