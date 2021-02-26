// Copyright (C) Sun Microsystems Inc., 2006-2008.  All Rights Reserved.  
// Authors: Dave Dice, Nir Shavit, Ori Shalev.  
//
// TL2: Transactional Locking for Disjoint Access Parallelism
// 
// Transactional Locking II,
// Dave Dice, Ori Shalev, Nir Shavit
// DISC 2006, Sept 2006, Stockholm, Sweden.  
// 

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <inttypes.h>
#include <stddef.h>
#include <libgen.h>
#include <assert.h>
#include <poll.h>
#include <pthread.h>
#include <errno.h>
#include <sys/utsname.h>
#include <sys/systeminfo.h>
#include <schedctl.h>
#include "if.h"
#include "TL.h"

#if !(__sparc)
#error currently only works on SPARC
#endif

#define CRX const restrict

typedef unsigned long long u64t ; 

// =====================> Platform-specific bindings

extern intptr_t sysCAS (intptr_t o0, intptr_t o1, intptr_t * o2);
#define CAS(m,c,s)     sysCAS((intptr_t)(s),(intptr_t)(c),(intptr_t *)(m))
extern void            membarstoreload() ;
extern void            membarsync() ; 
extern void            SequencePoint() ; 
extern u64t            rdtick() ;
extern u64t            rdstick() ;
extern u64t            Non1CAS (intptr_t cmp, intptr_t set, intptr_t * adr) ; 
extern u64t            Non2CAS (intptr_t cmp, intptr_t set, intptr_t * adr) ; 

#define NONATOMIC 0

#if NONATOMIC
// Useful for experiments to determine the impact of and sensitivity to local CAS latency.
#define sysCAS(a,b,c) Non2CAS(a,b,c)
#endif


// SPARC TSO memory model 
// Note that LDLD isn't strictly correct as SPARC allows LDs to reorder
// with respect to other LDS, but respects data and control dependencies. 
#define MEMBARLDLD()   (0)
#define MEMBARSTST()   (0)
#define MEMBARSTLD()   (membarstoreload())

// Non-faulting LD.  Can easily be emulated with trap handlers
// on SIGSEGV, SIGBUS, SIGILL.  
extern intptr_t LDNF   (intptr_t * a) ; 

// We use PrefetchW in LD...CAS and LD...ST circumstances to
// force the $line directly into M-state, avoiding RTS->RTO upgrade txns.  
extern void PrefetchW  (void * a) ; 


// PAUSE() - MP-polite spinning
// Ideally we'd like to drop the priority of our CMT strand.
// consider:  wr %g0,%asi | MEMBAR #sync | MEMBAR #halt
#define PAUSE() (0)  

// =====================> Generic Infrastructure

enum { NEVER = 0, ALWAYS=1 } ;
static volatile int * const _BAD = (volatile int *) 0xBAD ; 

#define DIM(A)      (sizeof(A)/sizeof((A)[0]))
#define UNS(a)      ((uintptr_t)(a))
#define CTASSERT(x) { int tag[1-(2*!(x))]; printf ("Tag @%X\n", tag); } 
#define TRACE(nom)  (0)
#define CONFIGURED    1
#define NOTCONFIGURED (!CONFIGURED)

#if 0
#define ASSERT(x)   assert(x)
#define ASSERT(x)   { if (!(x)) *((volatile int *) 0xBAD) = __FILE__##__LINE__ ; }
#define ASSERT(x)   ((x) || __afail(__FILE__,__LINE__,#x))
#define ASSERT(x)   (0)
#endif
#define ASSERT(x)   (0)

static volatile int AssertStall = 1 ; 
static int AssertRV = 0 ; 

static int __afail (const char * file, int Line, const char * expr) {
  int v = AssertRV ;
  if (v) { AssertRV = 0 ; return v ; }
  printf ("\nAssertion failure: %s:%d '%s'\n", file, Line, expr) ;
  if (AssertStall) {
    for (;;) ;
  }
  return 0 ;
}

// Atomic Fetch-and-Add()

static intptr_t Adjust (volatile intptr_t * addr, int dx) { 
  intptr_t v ;
  for (v = *addr ; CAS (addr, v, v+dx) != v; v = *addr) ; 
  return v ; 
}

// Advance *addr to at least mx

static intptr_t SetMax (volatile intptr_t * addr, intptr_t mx ) { 
  for (;;) {
    intptr_t cv = *addr ; 
    if (cv >= mx) return cv ; 
    if (CAS (addr, cv, mx) == cv) return cv ; 
  }
}


static int AdjustFF (volatile int * addr, int dx) { 
  int v = *addr ; 
  for (;;) { 
     int vfy = CAS (addr, v, v+dx) ; 
     if (vfy == v) return v ; 
     v = vfy ; 
     // Feed-forward value from failing CAS to next iteration
  }
}

static inline int FenceLDLD (const int rv) { 
  MEMBARLDLD() ; 
  return rv ; 
}

// Simplistic low-quality Marsaglia shift-xor PRNG.
// Bijective except for the final masking operation.
// Cycle length for non-zero values is 4G-1.
// 0 is absorbing and should be avoided -- fixed point.
// We currently seed/reseed with 1.

static inline int MarsagliaXORV (int x) { 
  if (x == 0) x = 1 ; 
  x ^= x << 6;
  x ^= ((unsigned)x) >> 21;
  x ^= x << 7 ; 
  return x ;        // use either x or x & 0x7FFFFFFF
}

static inline int MarsagliaXOR (int * seed) {
  int x = MarsagliaXORV(*seed);
  *seed = x ; 
  return x & 0x7FFFFFFF;
}

static int TSRandom (Thread * CRX Self) { 
  return MarsagliaXOR (&Self->rng) ; 
}


static int UseSchedctl = 0 ; 
static const int CachePad = 64 ; 
static int OverflowTally = 0 ; 
static volatile intptr_t ThreadUniqID = 0 ;           // Thread sequence #
static volatile int _stats [4096] ; 

// ====================> Support for TL2Stick and TL2StickTA 

// Avoid false-sharing - ensure that TAdjust and basis are the sole occupants of a $Line.
// Sequester TAdjust and basis. 
// Basis should be RO except for initialization.
// Hopefully, TAdjust will be mostly R with little W traffic.
// TODO-FIXME: collapse TAdjust and basis into a single variable containing (TAdjust-basis).  
static u64t basis                     = 0 ;     // time base : epoch
static volatile intptr_t TAdjust      = 0 ;

#define _RDTICK() ((rdtick() & ~1LL) - basis)
#define _RDTICK() ((rdtick() - basis) << 1)

// Coherent hardware clock ...
#define HWCLOCK (rdtick())
#define HWCLOCK (rdstick())
#define MAXCLOCKSKEW 2          // Maximum clock skew for coherent clock source

// =====================>  Stripe locks

// Consider 4M alignment for LockTab so we can use large-page support.
// Alternately, we could mmap() the region with anonymous DZF pages.  
// Consider aligning LockTab on 64KB or 4M virtual address boundary,
// and then offseting the base by 64 bytes to achieve coloring. 
// Critically, we want to minimize cases where the lock and the datum
// appear in the same $-way.
// extern volatile vwLock LockTab [_TABSZ]; 
#define _TABSZ (1<<20)
#pragma align 128 (LockTab)
static volatile vwLock LockTab [_TABSZ] ;    // PS : PS1M

// PSLOCK: maps variable address to lock address.
// COLOR attempts to place the lock(metadata) and the data on
// different D$ indexes.  

#define TABMSK        (_TABSZ-1)
#define COLOR         (128)     // alternatives: 128, (256-16), 0
#define DCLINESIZEB   64
#define DCLINESIZEW   (DCSIZEB/sizeof(intptr_t))
#define LOCKSPERLINE  (DCLINESIZEB/sizeof(LockTab[0]))

// The most sensible stripe widths are between 1 full word and 
// 1 native D$ line.  The D$ line is likely the best choice as
// a cache line is the unit of coherence.
// ILP32 vs LP64.  STRIPESHFT >= Log2(sizeof(intptr_t)).
#define STRIPESHIFT_MIN   ((sizeof(void *) == 4) ? 2 : 3)
#if !defined(STRIPESHIFT)
#define STRIPESHIFT STRIPESHIFT_MIN
#endif

#define PSLOCK(a)      (LockTab + (((UNS(a)+COLOR) >> STRIPESHIFT) & TABMSK))   // PS1M

// =====================>  Bloom filter

// We use a degenerate Bloom filter with only one hash function generating
// a single bit.  A traditional Bloom filter use multiple hash functions and 
// multiple bits.  Relatedly, the size our filter is small, so it can saturate 
// and become useless with a rather small write-set.  
// The filter gives us a very cheap in-the-set test, returning NO or MAYBE. 
// A better solution might be small per-thread hash tables keyed by address that
// point into the write-set.  
// Beware that 0x1F == (MIN(sizeof(int),sizeof(intptr_t))*8)-1

#define FILTERHASH(a)   ((UNS(a) >> 2) ^ (UNS(a) >> 5)) 
#define FILTERBITS(a)   (1 << (FILTERHASH(a) & 0x1F))

// =====================>  Global Version-clock management 

// We use GClock[32] as the global counter.  It must be the sole occupant
// of its cache line to avoid false sharing.  Even so, accesses to 
// GCLock will cause significant cache coherence & communication costs
// as it is multi-read multi-write.  
static volatile vwLock GClock [64]; 

#define _GCLOCK  GClock[32] 


// Configuration control - select one of: GV4, GV5, GV6, Stick, StickTA
// TODO: Change names: GETRV(), GETWV() or GVRV(), GVWV().  

#if !defined(_GVCONFIGURATION)
#define _GVCONFIGURATION 4
#endif

#if _GVCONFIGURATION == 4
#define _GVFLAVOR    "GV4"
#define GVGenerateWV GVGenerateWV_GV4
#define GVRead       GVReadCommon
#define _SIMPLEABORT 1
#endif

#if _GVCONFIGURATION == 5
#define _GVFLAVOR    "GV5"
#define GVGenerateWV GVGenerateWV_GV5
#define GVRead       GVReadCommon
#define _SIMPLEABORT 0
#endif

#if _GVCONFIGURATION == 6
#define _GVFLAVOR    "GV6"
#define GVGenerateWV GVGenerateWV_GV6
#define GVRead       GVReadCommon
#define _SIMPLEABORT 0
#endif

#if _GVCONFIGURATION == 9
#define _GVFLAVOR    "Stick"
#define GVGenerateWV GVGenerateWV_Stick
#define GVRead       GVReadStick
#define _SIMPLEABORT 1
#endif

#if _GVCONFIGURATION == 10
#define _GVFLAVOR   "StickTA"
#define GVGenerateWV GVGenerateWV_StickTA
#define GVRead       GVReadStickTA
#define _SIMPLEABORT 1
#endif

#if !defined(_SIMPLEABORT)
#error "Misconfigured"
#endif

static void GVInit () { _GCLOCK = 0 ; basis = HWCLOCK & ~LOCKBIT ; } 

// GVReadCommon() for GV4,GV5,GV6.  

static vwLock GVReadCommon (Thread * CRX Self) { 
#if CONFIGURED
  return _GCLOCK ; 
#else
  // Optional optimization:
  // Avoid self-induced aborts -- Use with GV5 or GV6
  vwLock gc = _GCLOCK ; 
  vwLock wv = Self->wv ; 
  if (wv > gc) {
    CAS (&_GCLOCK, gc, wv);
    return _GCLOCK ; 
  }
  return gc ; 
#endif
}


// GVGenerateWV(): compute the next linearization number.
//
// Conceptually, we'd like to fetch-and-add _GCLOCK.  In practice, however,
// that naive approach, while safe and correct, results in CAS contention
// and SMP cache coherency-related performance penalties.  As such, we
// use either various schemes (GV4,GV5 or GV6) to reduce traffic on _GCLOCK.  
//
// Global Version-Clock invariant:
// I1: The generated WV must be > any previously observed (read) RV
// More accurately the WV must be > the RV used by any _prior thread.  


// Regarding GV4:
// The GV4 form of GVGenerateWV() does not have a CAS retry loop.
// If the CAS fails then we have 2 writers that are racing, trying to bump 
// the global clock.  One increment succeeded and one failed.  Because the 2 writers 
// hold locks at the time we bump, we know that their write-sets don't intersect.  
// If the write-set of one thread intersects the read-set of the other then we 
// know that one will subsequently fail validation (either because the lock 
// associated with the read-set entry is held by the other thread, or 
// because the other thread already made the update and dropped the lock, 
// installing the new version #).  In this particular case it's safe if
// two threads call GVGenerateWV() concurrently and they both generate the same
// (duplicate) WV.  That is, if we have writers that concurrently try to increment
// the clock-version and then we allow them both to use the same wv.  The failing
// thread can "borrow" the wv of the successful thread.  

static inline vwLock GVGenerateWV_GV4 (Thread * CRX Self, vwLock maxv) { 
  // CONSIDER PrefetchW (&_GCLOCK) to avoid RTS->RTO cache coherent upgrade bus txn
  const vwLock gv = _GCLOCK ; 
  vwLock wv = gv + VER1 ; 
  // if (maxv > wv) wv = maxv + VER1 ; 
  const vwLock k = CAS (&_GCLOCK, gv, wv) ;
  if (k != gv) {
    ASSERT (k >= wv); 
    wv = k ; 
  }
  ASSERT ((wv & LOCKBIT) == 0) ; 
  if (wv == 0) printf ("GV:OVERFLOW\n") ;  
  // Asserts to check for retrograde clock
  ASSERT (wv > Self->rv) ; 
  ASSERT (wv > Self->wv) ; 
  ASSERT (wv > maxv) ; 
  Self->wv = wv ; 
  return wv ; 
}


// GV5:

static inline vwLock GVGenerateWV_GV5 (Thread * CRX Self, vwLock maxv) { 
  // Simply compute WV = GCLOCK + VER1.
  // This increases the false+ abort-rate but reduces cache-coherence traffic.  
  // We only increment _GCLOCK at abort-time and perhaps TxStart()-time.
  // The rate at which _GCLOCK advances controls performance and abort-rate.
  // That is, the rate at which _GCLOCK advances is really a performance
  // concern -- related to false+ abort rates -- rather than a correctness issue.  
  // CONSIDER: use MAX(_GCLOCK, Self->rv, Self->wv, maxv, VERSION(Self->abv))+2.  
  vwLock wv = _GCLOCK + 2 ; 
  if (maxv > wv) wv = maxv + VER1 ; 
  if (wv == 0) printf ("GV:OVERFLOW\n") ;  
  ASSERT (wv > Self->rv) ; 
  ASSERT (wv >= Self->wv) ; 
  Self->wv = wv; 
  return wv ; 
}

// GV6: composite of GV4 and GV5

static inline vwLock GVGenerateWV_GV6 (Thread * CRX Self, vwLock maxv) { 
  // Trade-off -- abort-rate vs SMP cache-coherence costs.  
  // TODO: make the frequence mask adaptive at runtime.
  // let the abort-rate or abort:success ratio drive the mask.
  int rnd = MarsagliaXOR (Self->xorrng) ; 
  if ((rnd & 0x1F) == 0) {          // TUNABLE!
    // Inlined version of GVGenerateWV_GV4 with relaxed asserts
    const vwLock gv = _GCLOCK ; 
    vwLock wv = gv + VER1 ; 
    // if (maxv > wv) wv = maxv + VER1 ; 
    const vwLock k = CAS (&_GCLOCK, gv, wv) ;
    if (k != gv) wv = k ; 
    ASSERT ((wv & LOCKBIT) == 0) ; 
    if (wv == 0) printf ("GV:OVERFLOW\n") ;  
    Self->wv = wv ; 
    return wv ; 
  } else { 
    return GVGenerateWV_GV5(Self, maxv) ; 
  }
}

// StickTA: uses %STICK with TAdjust 

static vwLock GVReadStickTA (Thread * CRX Self) { 
  vwLock rv = ((HWCLOCK - basis) << 1) + TAdjust ;
  ASSERT ((Self->wv  & LOCKBIT) == 0) ;
  vwLock abv = Self->abv ;
  if (abv & LOCKBIT) abv = 0 ;
  if (rv < Self->wv || rv < abv) {
    // If rv is less then either then thread's previous WV (Self->wv)
    // or the version# that caused the previous abort (Self->AbortV) 
    // then we advance TAdjust accordingly.
    // We want to minimize the # of writes to TAdjust to avoid
    // coherence traffic.
    // If we find WV > rv at TxStart|GVRead-time we can:
    // (a) preemptively try to advance TAdjust,  [OR] 
    // (b) we can be lazy and try to run the txn, risking self-abort.
    //     If we happen to abort we can advance TAdjust either at
    //     abort-time, or, even more lazily, at the next TxStart|GVRead-time
    //     when we check ABV > rv.  The lazy approaches have the advantage 
    //     that we might entirely avoid the need to advance TAdjust.
    vwLock mx = Self->wv ;
    if (abv > mx) mx = abv ;
    // mx = MAX (abv, Self->wv) ;
    ASSERT ((mx-rv) > 0) ;
    ASSERT (((mx-rv) & LOCKBIT) == 0) ;
    Adjust (&TAdjust, mx - rv) ;
    // ALTERNATE: SetMax (&TAdjust, mx - ((HWCLOCK-basis) << 1)) ; 
    rv = mx ;
    ASSERT (rv <= (((HWCLOCK - basis) << VERSHIFT) + TAdjust)) ; 
  }
  ASSERT ((rv & LOCKBIT) == 0) ;
  return rv ;
}

static inline vwLock GVGenerateWV_StickTA (Thread * CRX Self, vwLock maxv) {
  vwLock wv = ((HWCLOCK - basis) << VERSHIFT) + TAdjust + ((1+MAXCLOCKSKEW)*2) ;
  // consider MEMBAR #sync here
  Self->wv = wv ;
  return wv ;
}

// Stick: uses %STICK only

static vwLock GVReadStick (Thread * CRX Self) { 
  return (HWCLOCK - basis) <<VERSHIFT ;
}

static inline vwLock GVGenerateWV_Stick (Thread * CRX Self, vwLock maxv) {
  vwLock wv = ((HWCLOCK - basis) << VERSHIFT) + ((1+MAXCLOCKSKEW)*2) ;
  // consider MEMBAR #sync here
  Self->wv = wv ;
  return wv ;
}


// GV5 and GV6 admit single-threaded false+ aborts. 
// Consider the following scenario:
// GCLOCK is initially 10.  TxStart() fetches GCLOCK, observing 10, and 
// sets RV accordingly.  The thread calls TXST().  At commit-time the thread 
// computes WV = 12 in GVComputeWV().  T1 stores WV (12) in various versioned 
// lock words covered by the write-set.  The transaction commits successfully.
// The thread then runs a 2nd tnx. TxStart() fetches _GCLOCK == 12 and sets RV 
// accordingly.  The thread the calls TXLD() to fetch a variable written in the 
// 1st txn and observes Version# == 12, which is > RV.  The thread aborts.  
// This is false+ abort as there is no actual interference.
// We can recover by incrementing _GCLOCK at abort-time if we find 
// that RV == GCLOCK and Self->Abv > GCLOCK.  
// Alternately we can attempt to avoid the false+ abort by advancing
// _GCLOCK at GVRead()-time if we find that the thread's previous WV is >
// than the current _GCLOCK value.  

static int GVAbort (Thread * CRX Self) { 
#if _SIMPLEABORT
  // Used for GV4,Stick,StickTA
  return 0 ; 
#else
  // Used for GV5,GV6
  // If we find abv > rv then can either 
  // A. advance _GCLOCK here and now, [OR]
  // B. defer advancing _GCLOCK until the next TxStart|GVRead.  
  //    (B) is likely to be better as we might find some other
  //    other thread has bumped GCLOCK in the interim, obviating the 
  //    need for this thread to write to GCLOCK.
  vwLock abv = Self->abv ; 
  if (abv & LOCKBIT) return 0 ;       // normal interference
  vwLock gv = _GCLOCK ; 
  if (Self->rv == gv && abv > gv) { 
     CAS (&_GCLOCK, gv, abv) ;        // advance to either (gv+VER1) or abv
     // If this was a GV5/GV6-specific false+ abort then we don't want to delay.
     return 1 ;     // false+ abort
  }
  return 0 ;        // normal abort from interference
#endif
}


// =====================>  TL/TL2 Infrastructure

void TxOnce () { 
  // Note: placing LockTab on large page(s) greatly reduces DTLB miss rates.  
  // Use memctl()'s MC_HAT_ADVISE.  

  printf ("TX system ready: ") ; 
  GVInit () ; 

  char * p = getenv ("SCHEDCTL") ; 
  UseSchedctl = p ? strtol (p, NULL, 0) : UseSchedctl ; 
  printf ("SCHEDCTL=%d; ", UseSchedctl) ; 

  char sibuf [256] ; 
  sibuf[0] = 0 ; 
  sysinfo (SI_PLATFORM, sibuf, sizeof(sibuf)) ; 
  printf ("%s; ", sibuf) ; 

  struct utsname un ; 
  uname (&un) ; 
  printf ("%s; ", un.nodename) ; 

  AssertRV = 1 ; 
  if (ASSERT(0) == 1) { 
     printf ("+ASSERTS ") ; 
  }
  if (INLINED) { 
     printf ("+INLINED ") ; 
  }
  printf ("TL2-Ref4-%s STRIPEWIDTH=%db\n", _GVFLAVOR, (1<<STRIPESHIFT)) ; 
    
  // Sanity check the coherent hardware clock
  u64t epoch = HWCLOCK ;
  u64t sample [32] ;
  int i ; 
  for (i = 0 ; i < 32 ; i++ ) {
     sample[i] = HWCLOCK - epoch ;
  }
  printf ("HWCLOCK: (MAXCLOCKSKEW=%d) ", MAXCLOCKSKEW) ;
  for (i = 0 ; i < 32 ; i++) {
     printf ("%llX ", sample[i]) ;
  }
  printf ("\n") ;

}

void TxShutdown () { 
  printf ("Shutdown: Overflows=%d ", OverflowTally) ; 
  if (_GCLOCK != 0) printf (" GCLOCK=%llX\n"  , _GCLOCK) ; 
  if (TAdjust != 0) printf (" TAdjust=%llX\n" , TAdjust) ; 
  printf ("\n") ; 
  for (int i = 0 ; i < DIM(_stats); i++) { 
    if (_stats[i] != 0) printf ("  %d: %d\n", i, _stats[i]) ; 
  }
}

char * TxDescribe() { 
  static char buf [128] ; 
  char * preload = getenv ("LD_PRELOAD") ;     // libumem ? 
  if (preload == NULL) preload = "" ; 
  preload = basename (preload) ; 
  sprintf (buf, "TL2-Ref4-+%s+%dW (%s)", _GVFLAVOR, (1<<STRIPESHIFT)/sizeof(xword), preload) ; 
  return buf ; 
}

// Allocate the primary list as a large chunk so we can guarantee 
// ascending & adjacent addresses through the list.
// This improves D$ and DTLB behaviour.  

static AVPair * MakeList (int sz, Thread * CRX Self) { 
  ASSERT (sz > 0) ; 
  // Use CachePad to reduce the odds of false-sharing.
  AVPair * ap = (AVPair *) malloc((sizeof(*ap) * sz) + CachePad) ; 
  memset (ap, 0, sizeof(*ap) * sz) ; 
  AVPair * List = ap ; 
  AVPair * Tail = NULL ;
  int i ; 
  for ( i = 0 ; i < sz; i++) { 
     AVPair * e = ap++ ; 
     e->Next    = ap ; 
     e->Prev    = Tail ; 
     Tail = e ; 
  }
  Tail->Next = NULL ; 
  return List ; 
}

// Postpend at the tail.  We want the front of the list, which sees 
// the most traffic, to remains contiguous.  

static AVPair * ExtendList (AVPair * List) {
  ASSERT (List != NULL) ;
  AVPair * Tail = List ;
  while (Tail->Next != NULL) Tail = Tail->Next ;
  AVPair * e = (AVPair *) malloc (sizeof(*e)) ;
  memset (e, 0, sizeof(*e)) ;
  Tail->Next = e ;
  e->Prev    = Tail ; 
  e->Next    = NULL ;
  return List ;
}

Thread * TxNewThread () { 
  Thread * const t = (Thread *) memalign (64, sizeof(*t) + CachePad) ; 
  memset (t, 0, sizeof(*t)) ; 
  int id = Adjust (&ThreadUniqID, 1) ; 
  t->UniqID = id ; 
  t->xorrng[0] = t->rng = (gethrtime() ^ id) | 1 ; 
  t->wrSet.put = t->wrSet.List = MakeList (WSSIZE, t) ; 
  t->LocalUndo.put = t->LocalUndo.List = MakeList (LSSIZE, t) ; 
  t->sc_self = UseSchedctl ? schedctl_init() : NULL ; 

  // Largely redundant with TxReset()
  t->SpinBudget = SPINBUDGET ; 
  t->rsp        = t->rsv ; 
  t->rsExtent   = t->rsv + RSSIZE ; 
  t->rsFilter   = 0 ; 
  return t ; 
}

// Log operators
// ~~~~~~~~~~~~~
// Consider: instead of a count we could make the AVList NULL-terminated
// with AVList[].addr == NULL.  See also CloseLog()


// Transfer the data in the log its ultimate location and
// then mark the log as empty.  

static void WriteBackF (Log * k) {   
  AVPair * e ; 
  AVPair * End = k->put ; 
  for (e = k->List ; e != End ; e = e->Next) { 
    ASSERT (e->Addr != NULL) ; 
    *(e->Addr) = e->Valu ; 
  }
  // Note: WriteBackF explicitly avoids resetting the "put" list.
  // k->put = k->List ; 
}

static void WriteBackR (Log * k) {  
  AVPair * e ; 
  for (e = k->put->Prev; e != NULL ; e = e->Prev) {  
    ASSERT (e->Addr != NULL) ; 
    *(e->Addr) = e->Valu ; 
    e->Addr = NULL ;     // diagnostic hygiene
    e->Valu = 0 ; 
  }
  k->put = k->List ; 
}

static void SaveForRollBack (Log * k, volatile intptr_t * Addr, intptr_t Valu) {
  AVPair * e = k->put ; 
  k->put     = e->Next ; 
  e->Addr    = Addr ; 
  e->Valu    = Valu ; 
  e->LockFor = NULL ;        
}

static inline void TrackLoad (Thread * CRX Self, volatile vwLock * CRX LockFor) {
  // Add an entry to the read-set.
  ASSERT (LockFor != NULL) ; 

  // The current implementation allows duplicate in the read-set.
  // This is completely benign but possibily inefficient.
  // An interesting experiment is to see if there's any use in 
  // filtering out duplicates to minimize the length of the RS.
#if 0
  BitMap msk = FILTERBITS(LockFor) ; 
  if ((Self->rsFilter & msk) == msk) { 
    rsEntry * const endv = Self->rsp ;
    for (rsEntry * s = Self->rsv ; s != endv ; s++) { 
      if (s->LockFor == LockFor) return ; 
    }
  }
  Self->rsFilter |= msk ; 
#endif

  (Self->rsp++)->LockFor = (vwLock *) LockFor ;    // append

  // Check for overflow.  Options:
  // (a) report and die  -- the implementation uses this policy.
  // (b) extend RS and continue 
  // (c) abort, resize in TxStart(), and retry
  if (Self->rsp >= (Self->rsv + RSSIZE)) {
    fprintf (stderr, "\nRead-Set overflow : adjust RSSIZE\n") ;
    exit (1) ; 
  }
}

static inline rsEntry * FindInReadSet (Thread * CRX Self, vwLock * LockFor) {
  ASSERT (LockFor != NULL) ;
#if 0
  // The Bloom filter gives us a fast-path null return.
  BitMap msk = FILTERBITS(LockFor) ; 
  if ((Self->rsFilter & msk) != msk) { 
    return NULL ; 
  }
#endif
  rsEntry * const endv = Self->rsp ;
  for (rsEntry * e = Self->rsv ; e != endv ; e++) {
    if (e->LockFor == LockFor) return e ;
  }
  return NULL ;
}

static void TxReset (Thread * CRX Self) { 
  // Reset to ground state
  Self->Mode                = 0 ; 
  Self->wrSet.BloomFilter   = 0 ; 
  Self->wrSet.put = Self->wrSet.List ; 
  Self->LocalUndo.put = Self->LocalUndo.List ; 

  Self->SpinBudget = 1000 ; 
  Self->rsp        = Self->rsv ; 
  Self->rsFilter   = 0 ; 
}

// Our mechanism admits mutual abort with no progress - livelock. 
// Consider the following scenario where T1 and T2 execute concurrently:
// Thread T1:  WriteLock A; Read B LockWord; detect locked, abort, retry
// Thread T2:  WriteLock B; Read A LockWord; detect locked, abort, retry
//
// Possible solutions:
//
// * try backoff (random and/or exponential), with some mixture
//   of yield or spinning. 
//
// * Use a form of deadlock detection and arbitration.
//
// In practice it's likely that a semi-random semi-exponential back-off
// would be best.
//

static void TxAbort (Thread * CRX Self, int Line, intptr_t hint) {
  ASSERT (Self->Mode == TTXN); 

  // Clean up after an abort.  
  // Restore any modified locals
  if (Self->LocalUndo.put != Self->LocalUndo.List) {
    WriteBackR (&Self->LocalUndo) ; 
  }

  Self->Retries ++ ; 
  Self->Aborts ++ ; 

  TxReset(Self) ;       
  Self->Mode = TABORTED ; 

  // CONSIDER: longjmp() back to TxStart().  
  // This gives us an implicit loop.  

  if (GVAbort (Self)) {     // possibly advance _GCLOCK for GV5 or GV6
    TRACE(FALSEPOS-ABORT) ; 
    return ; 
  }

  TRACE(ABORT) ; 

  // Beware: back-off is useful for highly contended environments
  // where N threads shows negative scalability over 1 thread.
  // Extreme back-off restricts parallelism and, in the extreme,
  // is tantamount to allowing the N parallel threads to run serially
  // 1 at-a-time in succession.  
  //
  // Consider: make the back-off duration a function of:
  // + a random #
  // + the # of previous retries
  // + the size of the previous read-set
  // + the size of the previous write-set
  // 
  // Consider:
  // - using true CSMA-CD MAC style random exponential backoff. 
  // - classic truncated binary exponential backoff -- TBEB
  // - time-based gethrtime() loops can impede performance of other strands
  //   that share compute resources with the looping strand.
  //   It might be better to use rdtick or rdstick instead of gethrtime().
  //   In addition, gethrtime() can quantize time values, possibly causing
  //   cliques or cohorts of threads to fall out of contention management in groups.
  //   As such, an Marsaglia xor-shift RNG might be useful:
  //   +    for (int j = next() & MASK; --i >= 0 ; ) Pause() ; 
  //   +    for (int j = next() & MASK; --i >= 0 ; ) next() ; 
  //   Where Pause() is one of the usual long-latency low-impact "CMT-polite" 
  //   instructions such as "rd %ccr,%g0" or "rd %y,%g0". 
  // - Generally: time-based loop vs iteration-based loop
  // - See also: SUN080760-TLEContentionManagement

  if (Self->Retries > 3 ) {         // TUNABLE
    int stall = TSRandom (Self) & 0xF ; 
    stall += Self->Retries >> 2 ; 
#if CONFIGURED
    hrtime_t expiry = gethrtime() + (stall * 1000LL) ; 
    while (gethrtime() < expiry) PAUSE() ; 
#else
    int shft = Self->Retries >> 2 ; 
    if (shft > 31) shft = 31 ; 
    int its = ((1 << shft) - 1) & TSRandom(Self) ; 
    while (--its >= 0) TSRandom(Self) ;
#endif
    TRACE(BackOff) ; 
  }
}

static int TxEndSuccess (Thread * CRX Self, int v) {
  TxReset (Self) ; 
  Self->Retries = 0 ; 
  Self->ovf = 0 ; 
  return v ; 
}

static int TxEndFailure (Thread * CRX Self, int v) { 
  TxAbort (Self, __LINE__, 0) ; 
  return v ; 
}


// Remarks on deadlock:
// Indefinite spinning in the lock acquisition phase admits deadlock.
// We can avoid deadlock by any of the following means:
//
// 1. Bounded spin with back-off and retry.
//    If the we fail to acquire the lock within the interval we drop all 
//    held locks, delay (back-off - either random or exponential), and retry 
//    the entire txn.  
//
// 2. Deadlock detection - detect and recover.  
//    Use a simple waits-for graph to detect deadlock.  We can recovery
//    either by aborting *all* the participant threads, or we can arbitrate.
//    One thread becomes the winner and is allowed to proceed or continue
//    spinning.  All other threads are losers and must abort and retry.
//
// 3. Prevent or avoid deadlock by acquiring locks in some order.  
//    Address order using LockFor as the key is the most natural.  
//    Unfortunately this requires sorting -- See the LockRecord structure.     
//


// Encode locked state as:
// A.  (Thread *) | LOCKBIT
// B.  (AVPair *) | LOCKBIT

static Thread * OwnerOf (vwLock v) { 
  return (v & LOCKBIT) ? ((Thread *) (v ^ LOCKBIT)) : NULL ; 
}

// With PS the versioned lock words (the LockTab array) are table stable and
// references will never fault.  Under PO, however, fetches by a doomed
// zombie txn can fault if the referent is free()ed and unmapped.  

#define LDLOCK(a)    *(a)           /* for PS */

// Is the read-set mutually consistent?  
// ReadSetCoherent() can be called at any time - 
// before the caller acquires locks or after. 

static int ReadSetCoherent (Thread * CRX Self) {
  intptr_t dx = 0 ; 
  vwLock const rv = Self->rv ; 
  ASSERT ((rv & LOCKBIT) == 0) ; 
  rsEntry * const End = Self->rsp ; 
  rsEntry * e ; 
  for (e = Self->rsv ; e != End; e++) { 
     ASSERT (e->LockFor != NULL) ; 
     const vwLock v = LDLOCK(e->LockFor) ; 
     if (v & LOCKBIT) { 
       dx |= (UNS(v) & ~LOCKBIT) ^ UNS(Self) ;  
     } else { 
       dx |= (v > rv) ; 
     }
  }
  return (dx == 0) ; 
}

static void RestoreLocks (Thread * CRX Self) { 
  AVPair * p ; 
  AVPair * CRX End = Self->wrSet.put ; 
  Self->wrSet.put = Self->wrSet.List ; 
  for (p = Self->wrSet.List ; p != End ; p = p->Next) { 
     ASSERT (p->Addr != NULL) ;
     ASSERT (p->LockFor != NULL) ;
     if (p->Held == 0) continue ; 
     ASSERT (OwnerOf(*(p->LockFor)) == Self) ;
     ASSERT ((p->rdv & LOCKBIT) == 0) ;
     p->Held = 0 ; 
     *(p->LockFor) = p->rdv ; 
     // Conceptually: assert p->rdv <= _GCLOCK
  }
}

static void DropLocks (Thread * CRX Self, vwLock wv) { 
  ASSERT ((wv & LOCKBIT) == 0) ; 
#if _GVCONFIGURATION == 4
  ASSERT (wv <= GVReadCommon(Self)) ; 
#endif
  AVPair * p ; 
  AVPair * CRX End = Self->wrSet.put ; 
  Self->wrSet.put = Self->wrSet.List ; 
  for (p = Self->wrSet.List ; p != End ; p = p->Next) { 
     ASSERT (p->Addr != NULL) ;
     ASSERT (p->LockFor != NULL) ;
     if (p->Held == 0) continue ; 
     p->Held = 0 ; 
#if _GVCONFIGURATION == 4
     ASSERT (wv > p->rdv) ; 
#else
     // GV5 and GV6 admit wv == p->rdv
     ASSERT (wv >= p->rdv) ; 
#endif
     ASSERT (OwnerOf(*(p->LockFor)) == Self) ;
     *(p->LockFor) = wv ;  
     // Conceptually: assert wv <= _GCLOCK
  }
}

// TryFastUpdate() is conservative with respect to privatization models
// and holds _all write locks until the final write is completed.
// This requires a total of 3 passes over the write-set: one to
// acquire, one to write-back, and a final pass to release the locks.
// If we're willing to forgo and relax ordering properties we can
// alternatively use a single "epilog" pass over the write set, writing 
// values and dropping locks.  This approach yields a timeline of write;drop;write;drop 
// instead of write;write;drop;drop.  This approach reduces lock hold-times
// and is friendly to the local cache regarding the traversal of the write-set.
// 
// It's also possible to reduce the passes by speculatively writing values
// immediately after having acquired a write-lock and before validation,
// but we'd then have to record original value to provide for possible roll-back 
// in the event of subsequent abort.  The write-set would be part-undo and part-redo.

static inline int TryFastUpdate (Thread * CRX Self) {
  ASSERT (Self->Mode == TTXN) ; 
  AVPair * const Start = Self->wrSet.List ;   // Least-recently added ws element
  AVPair * const End   = Self->wrSet.put ;    // 1 past most-recently added ws element

  // Optionally optimization -- pre-validate and vet the read-set.
  // Consider: Call ReadSetCoherent() before grabbing write-locks.
  // Validate that the set of values we've fetched from pure READ objects
  // remain coherent.  This avoids the situation where a doomed transaction 
  // grabs write locks and impedes or causes other potentially successful
  // transactions to spin or abort.  
  //
  // A smarter tactic might be to only call ReadSetCoherent() and pre-validated
  // the read-set only when Self->Retries > NN.  
  if (ALWAYS && !ReadSetCoherent(Self)) {
    return 0 ; 
  }

  schedctl_t * const sc = Self->sc_self ; 
  if (sc) schedctl_start(sc) ; 

  // Consider: if the write-set is long or Self->Retries is high we
  // could run a pre-pass and sort the write-locks by LockFor address.
  // We could either use a separate LockRecord list (sorted) or 
  // link the write-set entries via SortedNext.  

  // Lock-acquisition phase ...
  //
  // CONSIDER: While iterating over the locks that cover the write-set
  // track the maximum observed version# in maxv.  
  // In GV4:   wv = GVComputeWV(); ASSERT wv > maxv
  // In GV5|6: wv = GVComputeWV(); if (maxv >= wv) wv = maxv + VER1
  // This is strictly an optimization.
  // maxv isn't required for algorithmic correctness.  

  vwLock maxv = 0 ; 
  const vwLock srv = Self->rv ; 

  // traverse write-set in reverse order.
  // reverse-order is cache-benign as the most recently added
  // elements are the most likely to be $-resident
  for (AVPair * p = End->Prev ; p != NULL  ; p = p->Prev) { 
     ASSERT (p->Addr != NULL) ; 
     ASSERT (p->LockFor != NULL) ; 
     ASSERT (p->Held == 0) ; 
     volatile vwLock * const LockFor = p->LockFor ; 
     // Consider prefetching only when Self->Retries == 0
     if (NEVER) PrefetchW ((void *)LockFor) ; 
     vwLock cv = LDLOCK(LockFor) ; 
     if (cv == (UNS(Self)|LOCKBIT)) { 
        // Already locked by an earlier iteration.
        continue ;      
     }

     // Acquire the write-lock, conceptually:
     //   READ-WRITE stripe - validate version# and acquire
     //   WRITE-ONLY stripe - acquire
     // In practice we use the same validation for both WO and RW stripes. 
     // This admits false+ aborts (increasing the abort rate) but avoids the lookaside 
     // into the RS to distinguish RW from WO.  This is safe but overly conservative. 
     // 
     // Spinning on a LOCKED stripe makes little sense for READ-WRITE elements.
     // In theory we could spin if the read-version is the same but
     // the lock is held in the faint hope that the owner might
     // abort and revert the lock.  

     if ((cv & LOCKBIT) == 0 && cv <= srv && CAS(LockFor, cv, UNS(Self)|LOCKBIT) == cv ) { 
       if (cv > maxv) maxv = cv ; 
       p->rdv  = cv ;        // Save version# so we can revert on abort - roll-back
       p->Held = 1 ; 
       continue ;            // acquire success - advance to next element in WS
     }

     // The stripe is either locked or failed RV validation or the CAS failed.
     // CONSIDER: lookaside into RS to distinguish WO vs RW.
     // Optimial optimization to try to keep the txn alive and avoid false+ abort.
     // RW stripes need to validate the version #, but for pure WO stripes
     // we only need to check that the lock isn't held.  
     // For a WO stripe we could relax and avoid the (cv <= srv) above and try again.  
#if 0
     if ((cv & LOCKBIT) == 0 && cv > srv && FindInReadSet (Self, LockFor) == NULL) { 
       if (CAS (LockFor, cv, UNS(Self)|LOCKBIT) == cv) { 
         if (cv > maxv) maxv = cv ; 
         p->rdv  = cv ;        // Save version# so we can revert on abort - roll-back
         p->Held = 1 ; 
         continue ;            // acquire success - advance to next element in WS
       }
     }
#endif

     // For the most part spinning only makes sense if the lock is held and there's
     // some reasonable chance the owner will abort and roll-back the version #s.  
     // Consider: while spinning we might periodically validate
     // the read-set by calling ReadSetCoherent(). 
     Self->abv = cv ; 
     RestoreLocks (Self) ;
     TRACE (TryFastUpdate - Abort - ACQUIRE:RW) ; 
     if (sc) schedctl_stop(sc) ;  
     return 0 ; 
  }

  // We now hold all the locks for RW and W objects.
  // Next we validate that the values we've fetched from pure READ objects
  // remain coherent.  
  //
  // If GVGenerateWV() is implemented as a simplistic atomic fetch-and-add then
  // we can optimize by skipping read-set validation in the common-case.
  // Namely, 
  //   if (Self->rv != (wv-VER1) && !ReadSetCoherent(Self)) { ... abort ... }
  // That is, we could elide read-set validation for pure READ objects if 
  // there were no intervening write txns between the fetch of _GCLOCK into 
  // Self->rv in TxStart() and the increment of _GCLOCK in GVGenerateWV().  

  if (!ReadSetCoherent(Self)) {
    // The read-set is inconsistent.  
    // The transaction is spoiled as the read-set is stale.
    // The candidate results produced by the txn and held in 
    // the write-set are a function of the read-set, and thus invalid.
    RestoreLocks (Self) ; 
    if (sc) schedctl_stop(sc) ;  
    TRACE (TryFastUpdate - Abort - READ version) ; 
    return 0 ; 
  }

  // Generate next linearization number.
  // Invariant: any generated WV must be > any RV previously observed
  // by any thread.
  const vwLock wv = GVGenerateWV (Self, maxv);

  // We're now committed - this txn is successful.  
  // Write-back the deferred stores to their ultimate object locations.
  WriteBackF (&Self->wrSet) ;
    
  // Ensure the stores, above, are visible before we drop the locks.  
  MEMBARSTST() ; 
       
  // Release all the held write-locks, incrementing the version
  DropLocks (Self, wv) ; 

  if (sc) schedctl_stop(sc) ;  

  // Ensure that all the prior STs have drained before starting the next
  // txn.  We want to avoid the scenario where STs from "this" txn 
  // languish in the write-buffer and inadvertently satisfy LDs in
  // a subsequent txn via look-aside into the write-buffer.  
  MEMBARSTLD() ;

  return 1 ;        // return success indication
}

API void TxStore (Thread * CRX Self, volatile intptr_t * addr, intptr_t valu) { 
  // Consider: in GVTL/TL2 mode we can forgo the following check.  
  int m = Self->Mode ; 
  if (m == TABORTED) goto AReturn ;  
  ASSERT (m == TTXN) ; 

  if (Self->IsRO) goto ROTrap ; 
  // Consider -- colloct stats : Self->TxST ++ ; 

  volatile vwLock * const LockFor = PSLOCK(addr) ; 
  Log * const wr = &Self->wrSet ; 

  // CONSIDER: prefetch both the lock and the data
  if (NEVER && Self->Retries == 0) { 
    PrefetchW ((void *) addr) ; 
    PrefetchW ((void *) LockFor) ; 
  }

  // CONSIDER: spin briefly (bounded) while the object is locked,
  // periodically calling ReadSetCoherent(Self).  

  // Convert a redundant "idempotent" store to a tracked load.
  // This helps minimize the wrSet size and reduces false+ aborts.
  // Conceptually, "a = x" is equivalent to "if (a != x) a = x" 
  // This is entirely optional
  if (ALWAYS && LDNF((intptr_t *) addr) == valu) {
    BitMap msk = FILTERBITS(addr) ; 
    if ((wr->BloomFilter & msk) == msk) {
      AVPair * e ; 
      for (e = wr->put->Prev ; e != NULL ; e = e->Prev) { 
        ASSERT (e->Addr != NULL) ; 
        if (e->Addr == addr) {
           ASSERT (LockFor == e->LockFor) ; 
           e->Valu = valu ; 
           TRACE(SQUASHED-STORE-1) ; 
           return ; 
        }
      }
    }

    const vwLock rdv = LDLOCK(LockFor) ; 
    MEMBARLDLD() ; 
    // Ratify previously observed load
    if ((rdv & LOCKBIT) == 0 && rdv <= Self->rv && 
       LDNF((intptr_t *) addr) == valu && FenceLDLD(1) && LDLOCK(LockFor) == rdv) { 
       TrackLoad (Self, LockFor) ; 
       TRACE(SQUASHED-STORE-2) ; 
       return ; 
    }
  }

  // Each Address is protected by a single Lock.
  // The Address::Lock relation is stable and persistent.
  // We currently use a speculative store buffer.
  // Stores are recorded in chronological order - append only.
  // Maintain FIFO order to avoid WAW hazards.
  // Save the (Address,Value) pair in the speculative store buffer. 
  // As an optimization we could squash multiple stores to the same location.

  wr->BloomFilter |= FILTERBITS(addr)  ; 
  AVPair * e = wr->put ; 
  if (e == NULL) { 
    fprintf (stderr, "\nWrite-Set overflow -- adjust WSSIZE\n") ; 
    exit (1) ; 
  }
  wr->put    = e->Next ; 
  e->Addr    = addr ; 
  e->Valu    = valu ; 
  e->LockFor = LockFor ;        
  e->Held    = 0 ; 
#if !CONFIGURED
  e->rdv     = LOCKBIT ;        // use either 0 or LOCKBIT 
#endif
  return ; 

 ROTrap:
  *(Self->ROFlag) = 0 ; 
  TxAbort (Self, __LINE__, 0) ; 
  TRACE (Upgrade txn block RO to RW) ; 
 AReturn:
  return ; 
}

API intptr_t TxLoad (Thread * CRX Self, volatile intptr_t * Addr) { 

  // Consider: in GVTL/TL2 mode we can forgo the following check.  
  const int m = Self->Mode ; 
  if (m == TABORTED) {
    goto ARet ; 
  }

  ASSERT (m == TTXN) ; 
  // Consider -- collect stats: Self->TxLD ++ ; 

  volatile vwLock * const LockFor = PSLOCK(Addr) ; 

  // Preserve the illusion of processor consistency in run-ahead mode.
  // Look-aside: check the wrSet for RAW hazards.  
  // Bloom filter is an optional optimzation.  It give us a likely
  // fast-path for the common case where Addr doesn't appear in the write-set.
  Log * const wr = &Self->wrSet ; 
  BitMap const msk = FILTERBITS(Addr); 
  if ((wr->BloomFilter & msk) == msk) { 
    AVPair * e ;
    for (e = wr->put->Prev ; e != NULL ; e = e->Prev) { 
      ASSERT (e->Addr != NULL) ; 
      if (e->Addr == Addr) {
        ASSERT (LockFor == e->LockFor) ; 
        return e->Valu ; 
      }
    }
  }

  // Currently we set Self->rv in TxStart().
  // We might be better served to defer reading Self->rv
  // until the 1st transactional load unstead of in TxStart().
  // if (Self->rv == 0) Self->rv = _GCLOCK ; 

  // Fetch tentative value 
  // Use either SPARC non-fault loads or complicit signal handlers.
  // If the LD fails we'd like to call TxAbort()
  // TL2 does not permit zombie/doomed txns to run
  // Alternatives to reduce the # of conditional branches:
  // BR1:   rdv = LDLOCK(LockFor);
  //        rdv |= -(rdv & 1) ;      // unsigned
  //        LDLD; fetch value; LDLD
  //        if (rdv <= Self->rv && LDLOCK(LockFor) == rdv) ...
  // BR2:   rdv = LDLOCK(LockFor)
  //        LDLD; fetch value; LDLD
  //        if (rdv <= Self->rv && ((LDLOCK(LockFor)^rdv)|(rdv & 1)) == 0) ...
  // BR3:   rdv = LDLOCK(LockFor)
  //        rdv |= -(rdv & 1)
  //        LDLD; fetch value; LDLD
  //        vfy = LDLOCK(LockFor)
  //        rdv |= -Normalize(vfy^rdv)
  //        if (rdv <= Self->rv) ...
  //        WHERE: Normalize(x) is inline asm : movrnz R,1,R
  // BR4:   rdv = LDLOCK(LockFor)
  //        LDLD; fetch value; LDLD
  //        vfy = LDLOCK(LockFor)
  //        rdv |= -(Normalize(vfy^rdv) | (rdv & 1))
  //        if (rdv <= Self->rv) ...
  vwLock const rdv = LDLOCK(LockFor)  ; 
  ASSERT (OwnerOf(rdv) != Self) ; 
  MEMBARLDLD() ; 
  intptr_t const Valu = LDNF((intptr_t *) Addr) ;     // potentially inconsistent or faulting load
  MEMBARLDLD() ;      
  vwLock const vfy = LDLOCK(LockFor) ;              
  if ((rdv & LOCKBIT) == 0 && rdv <= Self->rv && vfy == rdv) { 
     if (!Self->IsRO) { 
       TrackLoad (Self, LockFor) ; 
     }
     return Valu ; 
  }

  // The location is either currently locked or has been
  // updated since this txn started.  In the later case if
  // the read-set is otherwise empty we could simply re-load
  // Self->rv = _GCLOCK and try again.  If the location is 
  // locked it's fairly likely that the owner will release
  // the lock by writing a versioned write-lock value that
  // is > Self->rv, so spinning provides little profit.  
  if (vfy & LOCKBIT) { 
    TRACE (Abort - TxLoad - LOCKED) ; 
  } else { 
    TRACE (Abort - TxLoad - Updated) ; 
    Self->abv = vfy ; 
  }
  Self->CfAddr = (intptr_t) Addr ; 
  Self->CfLock = (vwLock *) LockFor ; 
  
  // Intentional fall-thru into Abort ...

 Abort:
  TxAbort (Self, __LINE__, vfy) ; 
 ARet:
  return 0 ; 
}
  

// Localization, Privatization and Isolation.
// Prevent lifecycle pathologies (latent txl stores) by quiescing the region.
// Use TxSterilize() any time an object passes out of the transactional domain
// and will be accessed solely with normal non-transactional load and store
// operations. TxSterilize() allows latent pending txn STs to drain before allowing 
// the object to escape.  This avoids use-after-free errors, for instance.
// Note that we need to know or track the length of the malloc()ed objects.
// In practice, however, most malloc() subsystems can compute the object length
// in a very efficient manner, so a simple extension to the malloc()-free() 
// interface would suffice.  
//
// [CUNHA] See "Testing Patterns for Software Transactional Memory Engines".
// Lourenco and Cunha.  PADTAD'07
//
// To be more precise, it's not sufficient to allow existing writers to drain,
// but rather we must actually perform dummy write txn on the buffer.
// We must wait for existing writers to drain _AND ensure that any readers
// will abort. 

API void TxSterilize (Thread * CRX Self, void * Base, size_t Length) {
  const vwLock wv = GVGenerateWV (Self, 0) ; 
  intptr_t * Addr = (intptr_t *) Base ;
  intptr_t * const End =  (intptr_t *) (UNS(Base)+Length) ;
  volatile vwLock * PrevLockFor = NULL ; 
  // For each lock covering [Base,Base+Length) ...
  ASSERT (Addr <= End) ;
  while (Addr < End) {
    volatile vwLock * const Lock = PSLOCK(Addr) ;
    Addr ++ ;               // TODO: advance by stripe width/sizeof(intptr_t)
    if (Lock == PrevLockFor) continue ; 
    PrevLockFor = Lock ; 

    // [CUNHA]
    // Wait for any pending writes to drain and then advance the version#.
    // This is tantamount to a dummy write txn.
    //
    // The following scenario illustrates that simply waiting for 
    // the LOCKBIT to clear is insufficient and flawed.
    // We have a linked list X->Y->Z.
    // Thread 1 runs txn T1 to read Y.key.
    // Thread 2 runs txn T2 to unlink Y and then free(Y).
    // 1. T1 txly reads X.Next and then traverses to Y.
    // 2. T2 traverses the list and txly sets X.Next = Z.
    // 3. T2 commits, calls TxSterilize(Y) and then calls free(Y)
    //    For the purposes of explication, we'll assume none of the 
    //    stripes underying Y are locked, so the TxSterilize() method
    //    returns immediately.
    // 4. T1 txly reads Y.Key and can observe junk. 
    //    We have use-after-free error.
    // 5. T1 is a pure-reader txn and will commit successfully.
    //    T1 observed (fetched) and invalid location but still committed!
    //
    // A slightly more simple scenario is:
    // Thread T1 runs:  txn { v = buf->Field; } 
    // Thread T2 runs:  n = new(); txn { tmp=buf; buf=n;} Sterilize(tmp); free (tmp); 
    //
    // As an optimization, we could escape the following loop if
    // some other thread managed to increment the version# while we
    // were waiting.  Beware of roll-back from abort, however, where
    // a lock can transition from (V:0) to (ID:1) back to (V:0). 
    //
    // It's tempting to simply bump the lock version numbers in a simplistic fashion
    // such as the following:
    //  | for (;;) { 
    //  |   vwLock vw = *Lock ; 
    //  |   if (vw & LOCKBIT) continue ; 
    //  |   if (CAS (Lock, vw, vw+VER1) == vw) break ; 
    //  |  }
    // This is flawed, however, as it can leave a stripe version# > GV.
    // To avoid this problem we can:
    //   -- Modify TxAbort() to advance GV up to the largest observed value on-demand.
    //   -- Generate a new GV.  We use this approach.  Unfortunately this
    //      means we call GVGenerateWV() which is a global coherence operation.
    
    for (;;) { 
      const vwLock ver = *Lock ; 
      if (ver & LOCKBIT) continue ; 
      if (ver > wv) break ; 
      if (CAS (Lock, ver, wv) == ver) break ; 
    }
  }
}

void TxStoreLocal (Thread * CRX Self, volatile intptr_t * Addr, intptr_t Valu) { 
  // Update in-place, saving the original contents in the undo log
  SaveForRollBack (&Self->LocalUndo, Addr, *Addr) ; 
  *Addr = Valu ; 
}

// If TxValid() returns FALSE the caller is expected to unwind and restart the
// transactional attempt.  An alternative would be to call setjmp()
// in TxStart() and have TxValid() check and optionally longjmp() to
// restart and retry an operation that failed because of conflicts. 

API int TxValid (Thread * CRX Self) {
  if (Self->Mode == TABORTED) {
     return 0 ; 
  }
  return 1 ; 
}

int TxValidateAndAbort (Thread * CRX Self) {
  if (TxValid(Self)) return 1 ;
  TxAbort (Self, __LINE__, 0) ; 
  return 0 ; 
}

void * TxStart (Thread * CRX Self, int * ROFlag) { 
  if (Self->Mode == TABORTED) {
    Self->Mode = TIDLE ;
  }

  ASSERT (Self->Mode == TIDLE) ; 
  TxReset (Self) ; 
  ASSERT (Self->LocalUndo.put == Self->LocalUndo.List) ; 
  ASSERT (Self->wrSet.put == Self->wrSet.List) ; 
  Self->ROFlag = ROFlag ;       // txn site-specific flag
  Self->IsRO   = ROFlag ? *ROFlag : 0 ; 
  Self->Mode   = TTXN ; 
  Self->rv     = GVRead (Self) ; 
  ASSERT ((Self->rv & LOCKBIT) == 0) ; 
  MEMBARLDLD() ; 
  
  // CONSIDER: setjmp (Self->OnFailure) 
  return NULL ; 
}

int TxCommit (Thread * CRX Self) { 
  if (Self->Mode == TABORTED) { 
     return 0 ; 
  }

  ASSERT (Self->Mode == TTXN) ; 

  // Fast-path: Optional optimization for pure-readers
  if (Self->wrSet.put == Self->wrSet.List) { 
    // Given TL2 the read-set is already known to be coherent.
    TxReset (Self) ; 
    Self->Retries = 0; 
    Self->ovf = 0 ; 
    return 1 ; 
  }

  if (TryFastUpdate (Self)) {
    TxReset (Self) ; 
    Self->Retries = 0 ; 
    Self->ovf = 0 ; 
    return 1 ; 
  }

  TxAbort (Self, __LINE__, 0) ; 
  return 0 ; 
}

// Compile-time assertions -- establish critical invariants
// It's always better to fail at compile-time than at run-time.  

void CTAsserts () { 
  // Ensure we're LP64
  CTASSERT (sizeof(intptr_t) == sizeof(long)) ;  
  CTASSERT (sizeof(long) == 8) ;
  // _TABSZ length must be power-of-two
  CTASSERT ((_TABSZ & (_TABSZ-1)) == 0) ; 
  CTASSERT ((1<<STRIPESHIFT) >= sizeof(xword)) ; 
  CTASSERT (STRIPESHIFT >= STRIPESHIFT_MIN) ; 
}

#define TXLDA(a)      TxLoad   (Self, (intptr_t *)(a))  
#define TXSTA(a,v)    TxStore  (Self, (intptr_t *)(a),(v)) 
#define TXLDV(a)      TxLoad   (Self, (intptr_t *) &(a))  
#define TXSTV(a,v)    TxStore  (Self, (intptr_t *) &(a), (v))  
#define TXLDF(o,f)    TxLoad   (Self, (intptr_t *)(&((o)->f)))  
#define TXSTF(o,f,v)  TxStore  (Self, (intptr_t *)(&((o)->f)), (intptr_t)(v))


intptr_t TxStatsLDS (Thread * t) { return 0 ; }
intptr_t TxStatsSTS (Thread * t) { return 0 ; }

