// --------------------[ Red-Black Tree ]---------------------------

typedef enum { RED=0, BLACK=1 } Colors ; 

// For runs like "rb D10 u100 d0 s1000 r2000 t64" the tree sees little structural mutation
// and the k,p,l,r fields tend to be stable, but the v field is hot.  
// Depending on the stripe width (8W vs 1W for instance), this can result in lots of false+ aborts.
// Sequestering the value _away from the other fields is reasonable remedy.  
// Of course this increases D$ and TLB pressure.  
//
// See the related comments in objectmonitor.hpp : [EXCERPT] : 
//
//   Field placement to minimize coherency misses: 
//   Generally fields that are accessed closed together in time should be
//   placed proximally in space to promote D$ locality.  That is, temporal
//   locality should condition spatial locality.  That having been said,
//   we have to be careful to avoid false sharing and excessive invalidation
//   from coherence traffic.  As such, we try to cluster fields that tend to
//   _written_ at approximately the same time on the same $line.
//   Note that there's a tension at play: if we try too hard to minimize 1T 
//   capacity misses then we can end up with excessive coherency misses
//   running in a parallel environment.  
//


typedef struct _node {
    intptr_t k ; 
    // Typically we'd place k,v adjacent, but to avoid 
    // the issue mentioned above we instead place v on what 
    // will likely be its own stripe.
    // k is effectively final.
    // p,l,r,c show moderation mutation.
    // v can be highly mutable.  
    struct _node * p ; 
    struct _node * l ; 
    struct _node * r ; 
    intptr_t c ; 
    struct _node * NextFree ; 

    // padding to avoid false+ aborts and false sharing.  
    intptr_t __Sequester [8] ;      
    intptr_t v ; 

    // Avoid false-sharing but at the expense of DTLB and D$ pressure. 
    intptr_t __Pad [6] ;     
} node_t ; 
    
typedef struct {        // KVMap
    double padA [16] ; 
    node_t * root ;
    double pad [16] ; 
} set_t ;


typedef set_t KVMap ; 

// Key-Value "map" interface to underlying implementation:

extern int kv_insert(Thread * Self, KVMap * sk, int Key, int Valu);
extern int kv_delete(Thread * Self, KVMap * sk, int key);
extern int kv_put (Thread * Self, KVMap * sk, int Key, int Valu) ; 
extern int kv_set (Thread * Self, KVMap * sk, int Key, int Valu) ; 
extern int kv_get (Thread * Self, KVMap * sk, int Key) ; 
extern int kv_contains (Thread * Self, KVMap * sk, int Key) ; 
extern char * kv_init();
extern int kv_verify (KVMap * sk, int Verbose) ; 
extern KVMap * kv_create () ; 


