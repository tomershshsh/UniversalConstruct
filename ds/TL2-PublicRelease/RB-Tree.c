// %W% - %E%

#include "TL.c" 
#include "RB-Tree.h"

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

// Red-Black Tree implementation


static int Alloc = 1 ; 

#define TXLDA(a)      (*((intptr_t *)(a)))
#define TXSTA(a,v)    (*((intptr_t *)(a)) = (v))
#define TXLDF(o,f)    ((o)->f)
#define TXLDFIW(o,f)  ((o)->f)
#define TXSTF(o,f,v)  ((o)->f = (v))
#define TXLDV(v)      TXLDA(&(v))
#define TXSTV(v,s)    TXSTA(&(v),(s))

#define LDNODE(o,f) ((node_t *) (TXLDF((o),f)))


// See also:
// * Doug Lea's j.u.TreeMap
// * Keir Fraser's rb_stm.c and rb_lock_serialisedwriters.c in libLtx.  
//
// Following Doug Lea's TreeMap example, we avoid the use of the magic
// "nil" sentinel pointers.  The sentinel is simply a convenience and 
// is not fundamental to the algorithm.  We forgo the sentinel as
// it is a source of false+ data conflicts in transactions.  Relatedly,
// even with locks, use of a nil sentil can result in considerable
// cache coherency traffic on traditional SMPs.  

static node_t * _lookup (Thread * Self, KVMap * s, int k) {
    node_t * p = LDNODE(s,root); 
    while (p != NULL) {
      int cmp = k - TXLDF(p,k) ; 
      if (cmp == 0) return p ; 
      p = (cmp < 0) ? LDNODE(p,l) : LDNODE(p,r) ; 
      if (!TxValid(Self)) return NULL ; 
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


static void rotateLeft (Thread * Self, set_t * s, node_t * x) {
    node_t * r = LDNODE(x,r);      // AKA r, y
    node_t * rl = LDNODE(r,l) ; 
    TXSTF(x,r,rl);
    if (rl != NULL) { 
        TXSTF(rl,p,x) ; 
    } 
    // TODO: compute p = xp = x->p.  Use xp for R-Values in following
    node_t * xp = LDNODE(x,p) ; 
    TXSTF(r,p,xp) ; 
    if (xp == NULL)
        TXSTF(s,root,r);
    else if (LDNODE(xp,l) == x)
        TXSTF(xp,l,r) ; 
    else
        TXSTF(xp,r,r) ; 
    TXSTF(r,l,x) ; 
    TXSTF(x,p,r) ; 
}


static void rotateRight (Thread * Self, set_t * s, node_t * x) {
    node_t * l = LDNODE(x,l);      // AKA l,y
    node_t * lr = LDNODE(l,r) ; 
    TXSTF(x,l,lr) ; 
    if (lr != NULL) {
        TXSTF(lr,p,x);
    }
    // TODO: compute xp or p = x->p
    node_t * xp = LDNODE(x,p) ; 
    TXSTF(l,p,xp) ; 
    if (xp == NULL)
        TXSTF(s,root,l);
    else if (LDNODE(xp,r) == x)
        TXSTF(xp,r,l);
    else 
        TXSTF(xp,l,l);
    TXSTF(l,r,x);
    TXSTF(x,p,l);
}

static inline node_t * _parentOf (Thread * Self, node_t * n) {
   return n ? LDNODE(n,p) : NULL ; 
}

#define parentOf(n) _parentOf(Self,(n))

static inline node_t * _leftOf (Thread * Self, node_t * n) {
   return n ? LDNODE(n,l) : NULL ; 
}

#define leftOf(n) _leftOf(Self,(n))

static inline node_t * _rightOf (Thread * Self, node_t * n) { 
    return n ? LDNODE(n,r) : NULL ; 
}

#define rightOf(n) _rightOf(Self, (n))

static inline int _colorOf (Thread * Self, node_t * n) { 
    // TODO: TXLDF instead of LDNODE
    return n ? (int)(LDNODE(n,c)) : BLACK ; 
}

#define colorOf(n) _colorOf(Self, (n)) 

static inline void _setColor (Thread * Self, node_t * n, int c) { 
    if (n != NULL) TXSTF(n,c,c) ; 
}

#define setColor(n,c) _setColor(Self, (n),(c))

static void fixAfterInsertion(Thread * Self, set_t * s, node_t * x) {
    TXSTF(x,c,RED) ; 

    while (x != NULL && x != LDNODE(s,root)) { 
        node_t * xp = LDNODE(x,p) ; 
        if (TXLDF(xp,c) != RED) break ; 

        if (!TxValid(Self)) return ; 
        // TODO: cache g = ppx = parentOf(parentOf(x))
        if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
            node_t *  y = rightOf(parentOf(parentOf(x)));
            if (colorOf(y) == RED) {
                setColor(parentOf(x), BLACK);
                setColor(y, BLACK);
                setColor(parentOf(parentOf(x)), RED);
                x = parentOf(parentOf(x));
            } else {
                if (x == rightOf(parentOf(x))) {
                    x = parentOf(x);
                    rotateLeft(Self, s, x);
                }
                setColor(parentOf(x), BLACK);
                setColor(parentOf(parentOf(x)), RED);
                if (parentOf(parentOf(x)) != NULL)
                    rotateRight(Self, s, parentOf(parentOf(x)));
            }
        } else {
            node_t * y = leftOf(parentOf(parentOf(x)));
            if (colorOf(y) == RED) {
                setColor(parentOf(x), BLACK);
                setColor(y, BLACK);
                setColor(parentOf(parentOf(x)), RED);
                x = parentOf(parentOf(x));
            } else {
                if (x == leftOf(parentOf(x))) {
                    x = parentOf(x);
                    rotateRight(Self, s,x);
                }
                setColor(parentOf(x),  BLACK);
                setColor(parentOf(parentOf(x)), RED);
                if (parentOf(parentOf(x)) != NULL)
                    rotateLeft(Self, s, parentOf(parentOf(x)));
            }
        }
    }
    node_t * ro = LDNODE(s,root) ; 
    if (TXLDF(ro,c) != BLACK) {
      TXSTF(ro,c,BLACK) ; 
    }
}

// _insert() has putIfAbsent() semantics
// If the key already exists in the tree _insert() returns a pointer to the 
// node bearing that key and does not modify the tree, otherwise if the key
// is not in the tree it inserts (k,v) into the tree using node n.

static node_t *  _insert (Thread * Self, set_t * s, int k, int v, node_t * n) { 
    node_t * t  = LDNODE(s,root) ; 
    if (t == NULL) {
       if (n == NULL) return NULL ; 
       // Note: the following STs don't really need to be transactional.  
       TXSTF(n,l, NULL) ; 
       TXSTF(n,r, NULL) ; 
       TXSTF(n,p, NULL) ; 
       TXSTF(n,k, k) ; 
       TXSTF(n,v, v) ; 
       TXSTF(n,c, BLACK) ; 
       TXSTF(s,root, n) ; 
       return NULL ; 
    }

    for (;;) { 
        if (!TxValid(Self)) return NULL ; 
        intptr_t cmp = k - TXLDF(t,k) ; 
        if (cmp == 0) {
            return t ; 
        } else if (cmp < 0) {
            node_t * tl = LDNODE(t,l) ; 
            if (tl != NULL) {
                t = tl ; 
            } else {
                TXSTF(n,l, NULL) ; 
                TXSTF(n,r, NULL) ; 
                TXSTF(n,k, k) ; 
                TXSTF(n,v, v) ; 
#if 0
                TXSTF(n,c, BLACK) ; // fixAfterInsertion() will set RED
#endif
                TXSTF(n,p, t) ; 
                TXSTF(t,l, n) ; 
                fixAfterInsertion(Self, s,n);
                return NULL ;
            }
        } else { // cmp > 0
            node_t * tr = LDNODE(t,r) ; 
            if (tr != NULL) {
                t = tr;
            } else {
                TXSTF(n,l, NULL) ; 
                TXSTF(n,r, NULL) ; 
                TXSTF(n,k, k) ; 
                TXSTF(n,v, v) ; 
#if 0
                TXSTF(n,c, BLACK) ; // fixAfterInsertion() will set RED
#endif
                TXSTF(n,p, t); 
                TXSTF(t,r, n) ; 
                fixAfterInsertion(Self, s,n);
                return NULL;
            }
        }
    }
}

// Return the given node's successor node---the node which has the
// next key in the the left to right ordering. If the node has
// no successor, a null pointer is returned rather than a pointer to
// the nil node.

static node_t * _successor(Thread * Self, node_t * t) {
    if (t == NULL)
        return NULL;
    else if (LDNODE(t,r) != NULL) {
        node_t * p = LDNODE(t,r) ; 
        while (LDNODE(p,l) != NULL) { 
            p = LDNODE(p,l) ; 
            if (!TxValid(Self)) return NULL ; 
        }
        return p;
    } else {
        node_t * p = LDNODE(t,p);
        node_t * ch = t;
        while (p != NULL && ch == LDNODE(p,r)) {
            ch = p;
            p = LDNODE(p,p) ; 
            if (!TxValid(Self)) return NULL ; 
        }
        return p;
    }
}

static void fixAfterDeletion(Thread * Self, set_t * s, node_t *  x) {
    while (x != LDNODE(s,root) && colorOf(x) == BLACK) {
        if (!TxValid(Self)) return ; 
        if (x == leftOf(parentOf(x))) {
            node_t * sib = rightOf(parentOf(x));
            if (colorOf(sib) == RED) {
                setColor(sib, BLACK);
                setColor(parentOf(x), RED);
                rotateLeft(Self, s, parentOf(x));
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
                    rotateRight(Self, s, sib);
                    sib = rightOf(parentOf(x));
                }
                setColor(sib, colorOf(parentOf(x)));
                setColor(parentOf(x), BLACK);
                setColor(rightOf(sib), BLACK);
                rotateLeft(Self, s, parentOf(x));
                // TODO: consider break ...
                x = LDNODE(s,root) ; 
            }
        } else { // symmetric
            node_t * sib = leftOf(parentOf(x));

            if (colorOf(sib) == RED) {
                setColor(sib, BLACK);
                setColor(parentOf(x), RED);
                rotateRight(Self, s, parentOf(x));
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
                    rotateLeft(Self, s, sib);
                    sib = leftOf(parentOf(x));
                }
                setColor(sib, colorOf(parentOf(x)));
                setColor(parentOf(x), BLACK);
                setColor(leftOf(sib), BLACK);
                rotateRight(Self, s, parentOf(x));
                // TODO: consider break ...
                x = LDNODE(s,root) ; 
            }
        }
    }

    if (x != NULL && TXLDF(x,c) != BLACK) {
       TXSTF(x,c, BLACK) ; 
    }
}

static node_t * _delete (Thread * Self, set_t * s, node_t * p) { 
    // If strictly internal, copy successor's element to p and then make p
    // point to successor.
    if (LDNODE(p,l) != NULL && LDNODE(p,r) != NULL) {
        node_t * s = _successor (Self, p);
        TXSTF(p,k, LDNODE(s,k)) ; 
        TXSTF(p,v, LDNODE(s,v)) ; 
        p = s;
    } // p has 2 children

    // Start fixup at replacement node, if it exists.
    node_t * replacement = (LDNODE(p,l) != NULL) ? LDNODE(p,l) : LDNODE(p,r);

    if (replacement != NULL) {
        // Link replacement to parent
        // TODO: precompute pp = p->p and substitute below ...
        TXSTF (replacement, p, LDNODE(p,p)) ; 
        node_t * pp = LDNODE(p,p) ; 
        if (pp == NULL)
            TXSTF(s, root, replacement) ; 
        else if (p == LDNODE(pp,l)) 
            TXSTF(pp,l, replacement) ; 
        else
            TXSTF(pp,r, replacement) ; 

        // Null out links so they are OK to use by fixAfterDeletion.
        TXSTF (p,l, NULL) ; 
        TXSTF (p,r, NULL) ; 
        TXSTF (p,p, NULL) ; 

        // Fix replacement
        if (TXLDF(p,c) == BLACK)
            fixAfterDeletion(Self, s, replacement);
    } else if (LDNODE(p,p) == NULL) { // return if we are the only node.
        TXSTF (s, root, NULL) ; 
    } else { //  No children. Use self as phantom replacement and unlink.
        if (TXLDF(p,c) == BLACK)
            fixAfterDeletion(Self, s,p);

        node_t * pp = LDNODE(p,p) ; 
        if (pp != NULL) {
            if (p == LDNODE(pp,l))
                TXSTF(pp,l, NULL) ; 
            else if (p == LDNODE(pp,r))
                TXSTF(pp,r, NULL) ; 
            TXSTF(p,p, NULL) ; 
        }
    }
    return p ; 
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
// Diagnostic section
// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

static node_t * FirstEntry (set_t * s) {
    node_t * p = s->root ; 
    if (p != NULL) {
        while (p->l != NULL)
            p = p->l;
    }
    return p;
}

static node_t * successor(node_t * t) {
    if (t == NULL)
        return NULL;
    else if (t->r != NULL) {
        node_t * p = t->r;
        while (p->l != NULL)
            p = p->l ; 
        return p;
    } else {
        node_t * p = t->p;
        node_t * ch = t;
        while (p != NULL && ch == p->r) {
            ch = p;
            p = p->p ; 
        }
        return p;
    }
}


static node_t * predecessor (node_t * t) {
    if (t == NULL)
        return NULL;
    else if (t->l != NULL) {
        node_t * p = t->l ; 
        while (p->r != NULL) {
            p = p->r;
        }
        return p;
    } else {
        node_t * p = t->p;
        node_t * ch = t;
        while (p != NULL && ch == p->l) {
            ch = p;
            p = p->p ; 
        }
        return p;
    }
}

// Post-validate the RB-Tree structure - structural integrity check.
// Assumes no concurrent access.
//
// Compute the BH (BlackHeight) and validate the tree.
//
// This function recursively verifies that the given binary subtree satisfies
// three of the red black properties. It checks that every red node has only
// black children. It makes sure that each node is either red or black. And it
// checks that every path has the same count of black nodes from root to leaf.
// It returns the blackheight of the given subtree; this allows blackheights to
// be computed recursively and compared for left and right siblings for
// mismatches. It does _not check for every nil node being black.
// The return value of this function is the
// black height of the subtree rooted at the node ``root'', or zero if the
// subtree is not red-black.
// 

static int verify_redblack (node_t * root, int depth) { 
    int  height_left, height_right;

    if (root == NULL) return 1 ; 
    height_left  = verify_redblack(root->l, depth+1);
    height_right = verify_redblack(root->r, depth+1);
    if (height_left == 0 || height_right == 0) {
        return 0;
    }
    if (height_left != height_right) { 
        printf ("[INTEGRITY] Imbalance @depth=%d : %d %d\n", depth, height_left, height_right) ; 
        if (0) return 0;
    }

    if (root->l != NULL && root->l->p != root) { 
       printf ("[INTEGRITY] lineage\n") ; 
    }
    if (root->r != NULL && root->r->p != root) { 
       printf ("[INTEGRITY] lineage\n") ; 
    }

    // Red-Black alternation
    if (root->c == RED) {
        if (root->l != NULL && root->l->c != BLACK) {
          printf ("[INTEGRITY] VERIFY %d\n", __LINE__) ; 
          return 0;
        }
        if (root->r != NULL && root->r->c != BLACK) {
          printf ("[INTEGRITY] VERIFY %d\n", __LINE__) ; 
          return 0;
        }
        return height_left;
    }
    if (root->c != BLACK) {
        printf ("[INTEGRITY] VERIFY %d\n", __LINE__) ; 
        return 0;
    }
    return height_left + 1;
}

// Verify or validate the RB tree.  

int kv_verify (set_t * s, int Verbose) { 
    node_t * root = s->root ; 
    if (root == NULL) return 1 ; 
    if (Verbose) {
       printf ("Structural integrity check: ") ; 
    }
    if (0) { 
      printf ("  root=%X: key=%d color=%d\n", root, root->k, root->c) ; 
    }

    if (root->p != NULL) {
        printf ("  [INTEGRITY] root %X parent=%X\n", root, root->p) ; 
        return -1 ; 
    }
    if (root->c != BLACK) { 
        printf ("  [INTEGRITY] root %X color=%X\n", root, root->c) ; 
    }

    // Weak check of binary-tree property
    int ctr = 0 ; 
    node_t * its = FirstEntry(s) ; 
    while (its != NULL) { 
        ctr ++ ; 
        node_t * child = its->l ; 
        if (child != NULL && child->p != its) { 
            printf ("[INTEGRITY] Bad parent\n") ; 
        }
        child = its->r ; 
        if (child != NULL && child->p != its) { 
            printf ("[INTEGRITY] Bad parent\n") ; 
        }
        node_t * nxt = successor (its) ; 
        if (nxt == NULL) break ; 
        if (its->k >= nxt->k) { 
           printf ("[INTEGRITY] Key order %X (%d %d) %X (%d %d)\n", 
            its, its->k, its->v, nxt, nxt->k, nxt->v) ; 
           return -3;  
        }
        its = nxt ; 
    }

    int vfy = verify_redblack (root, 0) ; 
    if (Verbose) { 
      printf (" Nodes=%d Depth=%d\n", ctr, vfy) ; 
    }
    return vfy ; 
}


// ========================[API and Accessors]========================


KVMap *kv_create(int maxcount, void * cmp) {
    KVMap * n = (KVMap * ) malloc (sizeof(*n)) ; 
    n->root = NULL ; 
    return n ; 
}
    

static node_t  * GetNode (Thread * Self) {
    static int step = 0 ; 
    node_t * n = NULL ; 

    n = Self->NodeCache ; 
    if (n != NULL) {
       Self->NodeCache = n->NextFree ; 
       return n; 
    }

    if (n == NULL) { 
       n = (node_t *) malloc (sizeof(*n)) ; 
       n->NextFree = NULL ; 
       if (0) memset (n, 0, sizeof(*n)) ; 
    }
    return n ; 
}

static void ReleaseNode (Thread * Self, node_t * n) { 
    // TSM and immortal -- Thread local caches are bounded
    if (Alloc == 2) { 
       n->NextFree = Self->NodeCache ; 
       Self->NodeCache = n ; 
       return ; 
    }

    // Per-thread cache limited to at most one node.
    if (Self->NodeCache == NULL) {
        Self->NodeCache = n ; 
        return ; 
    }

    TxSterilize (Self, n, sizeof(*n)) ; 
    // In diagnostic mode we clobber the node to assist
    // in detecting use-after-free errors.
    if (NEVER) memset (n, 0xFF, sizeof(*n)) ; 
    free (n) ; 
}

// ld_insert AKA putIfAbsent

int kv_insert(Thread * Self, KVMap *dict, int Key, int Val) {
    Self->InFunc = "insert" ;       
    node_t * node = GetNode(Self) ; 
    node_t * ex ; 
    for (;;) {
       ex = NULL ; 
       static int ROFlag = 1 ; 
       TxStart (Self, &ROFlag) ; 
       ex = _insert(Self, dict, Key, Val, node);
       if (TxCommit(Self)) break ; 
    }

    if (ex != NULL) ReleaseNode (Self, node) ; 
    return (ex == NULL) ; 
}

int kv_delete(Thread * Self, KVMap *dict, int Key) {
    Self->InFunc = "delete:lookup";     
    node_t * node = NULL ; 
    for (;;) { 
        static int ROFlag = 1 ; 
        TxStart (Self, &ROFlag) ; 
        node = _lookup(Self, dict, Key);
        if (!TxValid(Self))  continue;  
        if (node != NULL) { 
           Self->InFunc = "delete:unlink";     
           node = _delete(Self, dict, node);
        }
        if (TxCommit(Self)) break ; 
    }
    if (node != NULL) ReleaseNode(Self, node) ; 
    return (node != NULL) ; 
}


// ld_put AKA ld_set

int kv_put (Thread * Self, KVMap * dict, int Key, int Val) { 
    Self->InFunc = "put" ; 
    node_t * nn = GetNode(Self) ; 
    for (;;) {
        static int ROFlag = 1 ; 
        TxStart (Self, &ROFlag) ; 
        node_t * ex = _insert (Self, dict, Key, Val, nn) ; 
        if (ex != NULL) { 
          TXSTF(ex,v,Val) ; 
          if (TxCommit(Self)) {
            ReleaseNode (Self, nn);
            return 0  ;
          }
          continue ; 
        }
        if (TxCommit(Self)) return 1 ; 
    }
}

int kv_get (Thread * Self, KVMap  * dict, int Key) {
    Self->InFunc = "get" ; 
    for (;;) { 
      static int ROFlag = 1 ; 
      TxStart (Self, &ROFlag) ; 
      node_t * n = _lookup(Self, dict, Key) ; 
      if (n != NULL) { 
          int val = TXLDF(n,v) ; 
          if (TxCommit(Self)) return val ; 
          continue ; 
      }
      if (TxCommit(Self)) return 0 ; 
    }
}

// ld_contains AKA ld_lookup
        
int kv_contains (Thread * Self, KVMap * dict, int Key) {
    Self->InFunc = "contains" ; 
    for (;;) { 
      static int ROFlag = 1 ; 
      TxStart (Self, &ROFlag) ; 
      node_t * n = _lookup(Self, dict, Key) ; 
      if (!TxCommit(Self)) continue ; 
      return n != NULL ; 
    }
}
        
char * kv_init() {
    char * p = getenv ("ALLOC") ; 
    Alloc = p ? strtol (p, NULL, 0) : Alloc ; 
    printf ("RedBlack (Alloc=%d)\n", Alloc) ; 
    return "" ; 
}





