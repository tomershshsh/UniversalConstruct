// Copyright (C) Sun Microsystems Inc., 2007.  All Rights Reserved.  
// Authors: Dave Dice, Nir Shavit
//
// TLRW-redo
// *  Always consistent - no zombies - always valid
// *  Acquires read locks during speculative execution phase and write locks during
//    commit phase.  
// *  No need for LDNF - no traps
// *  LockWord = (WriterID:16; RC:16) 
//    -- writers can set Owner to Non-zero while ReadCount is > 0
//       and then wait for ReadCount to drain to 0.
//    -- readers can not increment the ReadCount when Owner != 0
// *  Uses lazy versioning (AKA deferred writes, redo log, speculative store buffer,
//    etc in the literature).  
//    Reads must look-aside into write-set to avoid RAW hazards.  
// *  Suitable for 32-bit environments
// *  Consider reducing LockT size from 64-bits or 32-bits, even for 64-bit environment
// *  Visible readers
// *  Provides implicit privatization
//    Critically, we hold the read-set read-locks over the writes during the commit.
// *  We detect and recover from deadlock by way of bounded spinning leading to abort.
//    That is, deadlock detection is integral to contention managment.  (conflated with...)
//    Contention management also provides escape from unbounded deadlock.
// *  Stripe lock = RW lock; no version #s
// *  Acquire read-locks at encounter-time during txn
// *  Acquire write-locks at commit-time
// *  Assumes "PS" - stripe locks.
//    Lock metadata is (a) immortal, (b) type-stable, and (c) and separate from data.
//    Stripe locks
// *  The sole source of abort is deadlock avoidance.  
// *  Suffers from CAS latency, CAS interference, and cache-coherent 
//    communication costs for mostly reader "triangular" data structures.
//    The topmost nodes in a red-black tree, for instance, see considerable traffic
//    a readers traverse.  


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

typedef unsigned long long u64t ; 

// =====================> Platform-specific bindings

extern intptr_t sysCAS (intptr_t o0, intptr_t o1, intptr_t * o2);
#define CAS(m,c,s)     sysCAS((intptr_t)(s),(intptr_t)(c),(intptr_t *)(m))
extern void            membarstoreload() ;
extern void            membarsync() ; 
extern void            SequencePoint() ; 
extern u64t            rdtick() ;
extern u64t            rdstick() ;

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
extern void FetchL1 (void * a) ; 

#define PrefetchW(x) (0)    // disable prefetch on N1, use on traditional SMP

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
#define CFG0        0

#if 0
#define ASSERT(x)   assert(x)
#define ASSERT(x)   { if (!(x)) *((volatile int *) 0xBAD) = __FILE__##__LINE__ ; }
#define ASSERT(x)   ((x) || __afail(__FILE__,__LINE__,#x))
#define ASSERT(x)   (0)
#endif
#define ASSERT(x)   (0)

#define EVT(n) { static volatile int _ctr = 0 ; int x = ++_ctr ; if ((x & (x-1)) == 0) printf ("%d: %s\n", x, #n); }

static volatile int AssertStall = 1 ; 
static int AssertRV = 1 ; 

static int __afail (const char * file, int Line, const char * expr) {
  int v = AssertRV ; 
  if (v) { AssertRV = 0 ; return v; } 
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

// Simplistic low-quality Marsaglia SHIFT-XOR RNG.
// Bijective except for the trailing mask operation.  

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

static int TSRandom (Thread * Self) { 
  return MarsagliaXOR (&Self->rng) ; 
}


static int UseSchedctl = 0 ; 
static const int CachePad = 64 ; 
static int OverflowTally = 0 ; 
static volatile intptr_t ThreadUniqID = 0 ;           // Thread sequence #

static volatile int _stats [4096] ; 

// =====================> Stripe locks : immortal and type-stable

// Consider 4M alignment for LockTab so we can use large-page support.
// Alternately, we could mmap() the region with anonymous DZF pages.  
// extern volatile LockT LockTab [_TABSZ]; 
#define _TABSZ (1<<20)
#pragma align 128 (LockTab)

static volatile LockT LockTab [_TABSZ] ;             // PS : PS1M

// PSLOCK: maps variable address to lock address.
// For PW the mapping is simply (UNS(addr)+sizeof(int))
// COLOR attempts to place the lock(metadata) and the data on
// different D$ indexes.  

#define TABMSK        (_TABSZ-1)
#define COLOR         (128)     // CONSIDER: 128, (256-16),0
// ILP32 vs LP64.  PSSHIFT >= Log2(sizeof(intptr_t)).
// Sensible stripe widths are either 1 fullword or a D$.  
// A D$ makes sense as that's the unit of coherency.  
#define PSSHIFT_MIN      ((sizeof(void *) == 4) ? 2 : 3)
#define PSSHIFT  6
#define PSLOCK(a)      (LockTab + (((UNS(a)+COLOR) >> PSSHIFT) & TABMSK))   // PS1M

// =====================>  Bloom filter infrastructure

// We use a degenerate Bloom filter with only one hash function generating
// a single bit.  A traditional Bloom filter use multiple hash functions and 
// multiple bits.  Relatedly, the size our filter is small, so it can saturate 
// and become useless with a rather small write-set.  
// A better solution might be small per-thread hash tables keyed by address that
// point into the write-set.  
// Beware that 0x1F == (MIN(sizeof(int),sizeof(intptr_t))*8)-1

#define FILTERHASH(a)   ((UNS(a) >> 2) ^ (UNS(a) >> 5)) 
#define FILTERBITS(a)   (1 << (FILTERHASH(a) & 0x1F))

// =====================>  TL/TL2 Infrastructure

void TxOnce () { 
  // Note: placing LockTab on large page(s) greatly reduces DTLB miss rates.  
  // Use memctl()'s MC_HAT_ADVISE.  

  printf ("TX system ready: ") ; 

  char * p = getenv ("SCHEDCTL") ; 
  UseSchedctl = p ? strtol (p, NULL, 0) : UseSchedctl ; 
  printf ("SCHEDCTL=%d; ", UseSchedctl) ; 

  AssertRV = 1;
  if (ASSERT(0)) { 
    printf ("+ASSERTS ") ; 
  }
  if (INLINED) printf ("+INLINED ") ; 

  char sibuf [256] ; 
  sibuf[0] = 0 ; 
  sysinfo (SI_PLATFORM, sibuf, sizeof(sibuf)) ; 
  printf ("%s; ", sibuf) ; 
    
  struct utsname un ; 
  uname (&un) ; 
  printf ("%s; ", un.nodename) ; 
  printf ("TLRWx-redo STRIPEWIDTH=%db\n", (1<<PSSHIFT)) ; 
}

void TxShutdown () { 
  printf ("Shutdown: Overflows=%d ", OverflowTally) ; 
  for (int i = 0 ; i < DIM(_stats); i++) { 
    if (_stats[i] != 0) printf ("  %d: %d\n", i, _stats[i]) ; 
  }
}

char * TxDescribe() { 
  static char buf [128] ; 
  sprintf (buf, "TLRWx-redo WIDTH=%db ", (1<<PSSHIFT)) ; 
  return buf ; 
}

// Allocate the primary list as a large chunk so we can guarantee 
// ascending & adjacent addresses through the list.
// This improves D$ and DTLB behaviour.  

static AVPair * MakeList (int sz, Thread * Self) { 
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
  Thread * const t = (Thread *) memalign (64, sizeof(*t)) ; 
  memset (t, 0, sizeof(*t)) ; 
  int id = Adjust (&ThreadUniqID, 1) + 1 ; 
  ASSERT (id != 0) ; 
  t->UniqID      = id << OWNERSHIFT ; 
  t->sc_self     = UseSchedctl ? schedctl_init() : NULL ; 
  t->xorrng[0]   = t->rng = (gethrtime() ^ id) | 1 ; 
  t->wrSet.put   = t->wrSet.List = MakeList (256, t) ; 

  // Redundant with TxReset() ...
  t->rsp         = t->rsv ; 
  t->rsFilter    = 0 ; 
  t->LocalUndo.put = t->LocalUndo.List = MakeList (200, t) ; 
  t->SpinBudget  = 1000 ;        
  return t ; 
}

// Log operators
// ~~~~~~~~~~~~~
// Consider: instead of a count we could make the AVList NULL-terminated
// with AVList[].addr == NULL.  See also CloseLog()


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

// Each Address is protected by a single Lock.  
// The Address::Lock relation is stable and persistent.
// We currently use a speculative store buffer.
// Stores are recorded in chronological order - append only.  
// Maintain FIFO order to avoid WAW hazards.


static void SaveForRollBack (Log * k, volatile intptr_t * Addr, intptr_t Valu) {
  AVPair * e = k->put ; 
  k->put     = e->Next ; 
  e->Addr    = Addr ; 
  e->Valu    = Valu ; 
  e->LockFor = NULL ;        
}

static void TxReset (Thread * Self) { 
  // Reset to ground state
  Self->SpinBudget          = 2000 ;        // TODO F(#cpus, recentabortrate)
  Self->SpinBudget          = 300 ;         // TUNABLE !
  Self->Mode                = 0 ; 
  Self->rsp                 = Self->rsv ;   // mark read-set as empty
  Self->rsFilter            = 0 ; 
  Self->wrSet.BloomFilter   = 0 ; 
  Self->wrSet.put = Self->wrSet.List ; 
  Self->LocalUndo.put = Self->LocalUndo.List ; 
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

static void TxAbort (Thread * Self) {
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
  // Consider using true CSMA-CD MAC style random exponential backoff. 

  if (Self->Retries > 0 ) {         // TUNABLE
    int stall = TSRandom (Self) & 0xF ; 
    stall += Self->Retries >> 2 ; 
    hrtime_t expiry = gethrtime() + (stall * 1000LL) ; 
    while (gethrtime() < expiry) PAUSE() ; 
    TRACE(BackOff) ; 
  }
}

static int IOwn (Thread * Self, LockT v) { 
  ASSERT (Self->UniqID != 0) ; 
  return (UNS(Self->UniqID) ^ (v & OWNERMSK)) == 0 ;
}

static void DropReadLocks (Thread * const Self) { 
  RSEntry * const endv = Self->rsp ; 
  for (RSEntry * e = Self->rsv ; e != endv ; e++) { 
    volatile LockT * const LockFor = e->LockFor ; 
    e->LockFor = NULL ;     // diagnostic hygiene - optional
    // If we have a stripe that's RW, the commit operator will
    // null out the read-set element
    if (LockFor != NULL) { 
       // Decrement the RC field
       for (;;) {
         LockT rw = *LockFor ;
         ASSERT ((rw & RCMSK) > 0) ; 
         if (CAS (LockFor, rw, rw-RC1) == rw) break ; 
       }
    }
  }
  Self->rsp = Self->rsv ;   // diagnostic hygiene - optional
  Self->rsFilter = 0 ; 
}

static inline RSEntry * FindInReadSet (Thread * const Self, volatile LockT * const LockFor) { 
  ASSERT (LockFor != NULL) ;
  // The Bloom filter gives us a fast-path NULL return
  BitMap const msk = FILTERBITS(LockFor) ; 
  if ((Self->rsFilter & msk) != msk) return NULL ; 

  // Hits are relatively common in this test, so, given
  // spatial and temporal locality, working from the most-recently
  // added RS entries toward the oldest entries makes good sense.
  // It's also more $ friendly as the most recently added items
  // are more likely to be $-resident.
  RSEntry * const endv = Self->rsv - 1 ; 
  for (RSEntry * e = Self->rsp - 1 ; e != endv ; e--) { 
    if (e->LockFor == LockFor) return e ; 
  }

  return NULL ; 
}

static inline void AddToReadSet (Thread * Self, volatile LockT * LockFor) { 
  ASSERT (LockFor != NULL) ; 
  // TODO-FIXME: check uniqueness of LockFor in read-set
  // A given LockFor address should appear at most one in the read-set.
  ASSERT (FindInReadSet (Self, LockFor) == NULL) ; 
  Self->rsFilter |= FILTERBITS(LockFor) ;
  (Self->rsp++)->LockFor = (LockT *) LockFor ;    // append read-set entry
  // Check for read-set overflow.  Options:
  // 1. report and die
  // 2. extend read-set and continue
  // 3. abort, resize read-set in next TxStart() and rerun. 
  if (Self->rsp >= (Self->rsv + RSSIZE)) { 
    fprintf (stderr, "\nRead-set overflow: rebuild with larger RSSIZE\n") ; 
    exit (1) ; 
  }
}

// Only used at abort-time where only a subset of the write-set has been locked.
// The caller should have locked write-set elements in the region (UptoHint,End]
// We could iterate between (UptoHint,End] clearly just those locks.  
// Alternately, we could simply iterate over the entire write-set.
// If we find a lock-word set by the current thread will simply drop that lock.
// In this particular case we could find (Owner=Self:RC>0) as we may have aborted
// after having acquired a write-lock but while draining.  
 

static void DropWriteLocks (Thread * const Self, AVPair * const UptoHint) {
  AVPair * const Start = UptoHint ? UptoHint->Next : Self->wrSet.List ; 
  AVPair * const End = Self->wrSet.put ; 
  Self->wrSet.put = Self->wrSet.List ; 
  for (AVPair * e = Start ; e != End ; e = e->Next) { 
    ASSERT (e->Addr != NULL) ;
    ASSERT ((*PSLOCK(e->Addr) & OWNERMSK) == Self->UniqID) ;     // Expect (Owner=Self;RC=*)
    volatile LockT * const LockFor = e->LockFor ; 
    if (LockFor != NULL) { 
      ASSERT (LockFor == PSLOCK(e->Addr)) ; 
      // We can release the lock either with a fullword CAS
      // or a partial-word ST to the owner subfield
      *((volatile short *)(UNS(LockFor)+OWNEROFF)) = 0 ; 
    } 
    e->Addr    = NULL ;             // diagnostic hygiene - optional
    e->LockFor = NULL ; 
  }
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

API void TxStore (Thread * Self, volatile intptr_t * const addr, intptr_t valu) { 

  // Consider: in GVTL/TL2 mode we can forgo the following check.  
  int m = Self->Mode ; 
  if (m == TABORTED) goto AReturn ;  
  ASSERT (m == TTXN) ; 

  // Possible optimizations:
  // 1. Consider checking the lock-word in TxStore().  
  //    There may be a correlation between seeing a non-writable 
  //    lockword and subsequent commit-failure.  
  // 2. Merge or collapse redundant stores:  X=3;X=4;X=7
  // 3. Try to convert idempotent stores to tracked loads.  
  // 4. PrefetchW

  // CONSIDER: prefetch both the lock and the data
  if (NEVER && Self->Retries == 0) { 
    PrefetchW (addr) ; 
    PrefetchW (LockFor) ; 
  }


  // Convert a redundant "idempotent" store to a tracked load.
  // This helps minimize the wrSet size and reduces false+ aborts.
  // Conceptually, "a = x" is equivalent to "if (a != x) a = x" 
  // This is entirely optional

  // Issue: cheaper to cache/memoize LockFor or simply regenerate it
  // at commit-time ?
  volatile LockT * const LockFor = PSLOCK(addr) ; 
  Log * const wr = &Self->wrSet ; 

#if CFG0
  // Try to squash redundant writes
  if (ALWAYS && *addr == valu) {
    // First, look aside into the store buffer for a redundant stores
    // Avoid WAW hazards
    BitMap const msk = FILTERBITS(addr) ; 
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

    // Addr does not appear in the write-set.  
    // Try to convert the apparently idempotent TXST to a TXLD.
    // Try to acquire the read-lock.  If the CAS fails instead
    // of looping we simply fall thru into the normal ST path.  
    MEMBARLDLD() ; 
    // Materialize LockFor if we haven't already ...
    LockT rw = *LockFor ; 
    if (rw == 0 && CAS (LockFor, rw, rw+RC1) == rw) { 
       AddToReadSet (Self, LockFor) ; 
       // Now that we hold the read-lock recheck the value - ratify
       if (*addr == valu) return ; 
       goto RecordStore; 
    }
    if (FindInReadSet (Self, LockFor) != NULL) {
       return ; 
    }
    if ((rw & OWNERMSK) == 0 && CAS (LockFor, rw, rw+RC1) == rw) {
       AddToReadSet (Self, LockFor) ; 
       // Now that we hold the read-lock recheck the value - ratify
       if (*addr == valu) return ; 
       goto RecordStore; 
    }
  }
 RecordStore: (0); 
#endif

  // Enter the (Address,Value) pair into the speculative store buffer
  // As an optimization we could squash multiple stores to the same location.
  wr->BloomFilter |= FILTERBITS(addr)  ; 
  AVPair * e = wr->put ;         // advance the cursor
  if (e == NULL) { 
    fprintf (stderr, "\nWrite-Set overflow\n") ; 
    exit (1) ; 
  }
  wr->put    = e->Next ; 
  e->Addr    = addr ; 
  e->Valu    = valu ; 
  e->LockFor = LockFor ;        
 AReturn:
  return ; 
}

API intptr_t TxLoad (Thread * Self, volatile intptr_t * Addr) { 
  const int m = Self->Mode ; 
  if (m == TABORTED) return 0 ; 
  ASSERT (m == TTXN) ; 

  // Preserve the illusion of processor consistency in run-ahead mode.
  // Look-aside: check the wrSet for RAW hazards.  
  // The Bloom filter gives us a fast-path for the common-case where
  // the address doesn't appear in the write-set.
  Log * const wr = &Self->wrSet ; 
  BitMap const msk = FILTERBITS(Addr); 
  if ((wr->BloomFilter & msk) == msk) { 
    AVPair * e ;
    for (e = wr->put->Prev ; e != NULL ; e = e->Prev) { 
      ASSERT (e->Addr != NULL) ; 
      if (e->Addr == Addr) {
        ASSERT (PSLOCK(Addr) == e->LockFor) ; 
        return e->Valu ; 
      }
    }
  }

  // The address::lock relationship is stable. 
  volatile LockT * const LockFor = PSLOCK(Addr) ; 

  // Avoid RTS->RTO bus upgrades
  // Ideally, readers would not write (metadata)
  // But with RW locks, readers write shared stripe locks.
  PrefetchW ((void *)LockFor) ; 

  // Attempt to acquire the read-lock by incrementing the RC field.
  LockT rw = *LockFor ; 
  ASSERT ((rw & OWNERMSK) != Self->UniqID) ; 
  if (rw == 0 && CAS (LockFor, rw, rw+RC1) == rw) {
    // Already in read-set _IMPLIES_ RC > 1 and unlocked
    // RC = 0 IMPLIES not in read-set
    ASSERT (FindInReadSet (Self, LockFor) == NULL) ;
    AddToReadSet (Self, LockFor) ; 
    goto FinishRead ; 
  } 
  
  RSEntry * const r = FindInReadSet (Self, LockFor) ;
  if (r != NULL) {
     ASSERT ((rw & RCMSK) > 0) ;
     // Optional optimization - yield to stalled writer.  
     // writer is in commit waiting for readers to drain.
     if (NEVER && (rw & OWNERMSK) != 0) { 
        DropReadLocks (Self) ; 
        TxAbort(Self) ; 
        return 0 ; 
     }
     goto FinishRead ; 
  }
  
  for (;; rw = *LockFor) {
     if ((rw & OWNERMSK) == 0 && CAS (LockFor, rw, rw+RC1) == rw) {
       AddToReadSet (Self, LockFor) ;
       goto FinishRead ;
     }
     // CASES:
     // 1. reader-vs-writer contention -- writer exists
     // 2. CAS failure: reader interfered with this reader
     // 3. CAS failure: writer interfered with this reader
     // Contention management to reduce CAS failure rate on SPARC.  
     // CONSIDER: use a back-off if we keep failing the CAS
     // CONSIDER: truncated binary random exponential backoff.  
     // CONSIDER: make failed CAS return value available to distinguish (2) vs (3).
     // Observation: gethrtime() works well as a "pause" unit. 
     // TUNABLES !!!
     PAUSE() ;
     if ((rw & OWNERMSK) == 0) {         
       // CAS failure : most likely some other reader adjusted the RC field.
       // consider Bernoulli trials to escape
       for (int v = TSRandom(Self) & 0x1F; --v >= 0 ; TSRandom(Self)) ;
       if (ALWAYS) continue ; 
     }

     // Either the CAS failed or it's owned.  
     if (--Self->SpinBudget >= 0) continue ;
     DropReadLocks (Self) ;
     TxAbort (Self) ;
     return 0 ;
  }

 FinishRead:
  return *Addr ; 
}
  
API void TxSterilize (Thread * Self, void * Base, size_t Length) {
}

void TxStoreLocal (Thread * Self, volatile intptr_t * Addr, intptr_t Valu) { 
  // Update in-place, saving the original contents in the undo log
  SaveForRollBack (&Self->LocalUndo, Addr, *Addr) ; 
  *Addr = Valu ; 
}

// If TxValid() returns FALSE the caller is expected to unwind and restart the
// transactional attempt.  An alternative would be to call setjmp()
// in TxStart() and have TxValid() check and optionally longjmp() to
// restart and retry an operation that failed because of conflicts. 

API int TxValid (Thread * Self) {
  if (Self->Mode == TABORTED) {
     return 0 ; 
  }
  return 1 ; 
}

void * TxStart (Thread * Self, int * ROFlag) { 
  int ix ; 

 if (Self->Mode == TABORTED) {
    Self->Mode = TIDLE ;
  }

  ASSERT (Self->Mode == TIDLE) ; 
  TxReset (Self) ; 
  Self->Mode = TTXN ; 
  MEMBARLDLD() ; 
  
  ASSERT (Self->LocalUndo.put == Self->LocalUndo.List) ; 
  ASSERT (Self->wrSet.put == Self->wrSet.List) ; 
  // CONSIDER: setjmp (Self->OnFailure) 
  return NULL ; 
}

// Consider alternative return value:
// -1 : failure
//  0 : acquired write lock, no readers
//  1 : acquired write lock, readers require draining (skipped)

static inline int AcquireForWrite (Thread * Self, AVPair * e) { 
  volatile LockT * const LockFor = e->LockFor ; 
  ASSERT (e->LockFor != NULL) ; 
  ASSERT (e->Addr    != NULL) ; 
  e->Draining = 0 ; 
  intptr_t const selfid = Self->UniqID ; 

  // Consider prefetching only when Self->Retries == 0
  PrefetchW ((void *)LockFor) ; 
  LockT rw = *LockFor ; 
  if ((rw & OWNERMSK) == selfid) { 
    // Already locked by an earlier iteration
    e->LockFor = NULL ; 
    return 1 ; 
  }

  // Try fast-path
  if (rw == 0 && CAS (LockFor, rw, rw|selfid) == rw) {
    ASSERT (FindInReadSet (Self, LockFor) == NULL) ; 
    return 1 ; 
  }

  RSEntry * const rse = FindInReadSet (Self, LockFor) ; 
  for (;;rw = *LockFor) { 
    ASSERT ((rw & OWNERMSK) != selfid) ; 
    if (rw == RC1 && rse != NULL) { 
      // READ-WRITE element - try to upgrade from R to W.  
      // This thread already holds a read-lock on the stripe.
      // Convert from (Owner=null:RC=1) to (Owner=Self:RC=0)
      ASSERT (rse->LockFor == LockFor) ; 
      if (CAS (LockFor, rw, selfid) == rw) { 
        rse->LockFor = 0 ;  // remove LockFor from the read-set
                            // consider using a distinguished DEFUNCT encoding.
        return 1 ; 
      }
      continue ; 
    }
    if (rw == 0) { 
      ASSERT (rse == NULL) ; 
      if (CAS (LockFor, rw, selfid) == rw) {
        return 1 ; 
      }
      continue ; 
    }
    if ((rw & OWNERMSK) == 0 && (rw & RCMSK) > 0) {
      LockT const rc = (rw - ((rse == NULL) ? 0 : RC1)) & RCMSK ;
      if (CAS (LockFor, rw, selfid|rc) == rw) {
        if (rc != 0) e->Draining = 1 ; 
        if (rse != NULL) rse->LockFor = 0 ; 
        Self->Skipped ++ ; 
        return 1 ;
      }
    }

    if ((rw & OWNERMSK) != 0 && rse != NULL) { 
      ASSERT ((rw & RCMSK) > 0) ; 
      // Some other thread holds the write lock while thread holds a read-lock 
      // and is trying to upgrade from R to RW.
      // The other thread -- the owner -- is waiting for this thread to drain off
      // and release its R lock and this thread is waiting for the owner to 
      // release W.   At least one thread must ultimately abort.
      // Since this thread can identify the condition, by our policy it graciously
      // aborts to allow the other thread to make progress.
      // This is an optional optimization
      return 0 ; 
    }

    // We have either W-W or R-W contention.
    // This could be honest contention or deadlock.
    // Use a bounded spin.  Failing that, abort.
    if (--Self->SpinBudget < 0) return 0 ; 
    PAUSE() ;   // consider civilized back-off
  }
}

int TxCommit (Thread * Self) { 
  if (Self->Mode == TABORTED) { 
     return 0 ; 
  }

  ASSERT (Self->Mode == TTXN) ; 

  // Fast-path: Optional optimization for pure-readers
  // If the write-set is empty we're done.
  if (Self->wrSet.put == Self->wrSet.List) { 
    DropReadLocks (Self) ; 
    TxReset (Self) ; 
    Self->Retries = 0; 
    return 1 ; 
  }

  // The read-set is always valid in TLRW so there's no
  // need to validate.
  ASSERT (Self->Mode == TTXN) ; 
  AVPair * e ; 
  AVPair * const End   = Self->wrSet.put ;      // one beyond the most-recently addded
  AVPair * const Start = Self->wrSet.List ;     // least-recently added

  schedctl_t * sc = Self->sc_self ; 
  if (sc) schedctl_start(sc) ; 

  Self->Skipped = 0 ; 

  // Write-lock acquistion phase - RW and WO stripes.
  // Read-locks were already acquired during speculative execution
  // Iterate over the write-set in reverse chronological order. 
  // This is benign for performance as the most-recently stored
  // write-set elements are most likely to be resident in D$.  
  // We use reverse order to ensure that if multiple write-set elements
  // refer to the same lock that only the most-recent recent element
  // (that nearest the tail) is "canonical" and has e->LockFor != null 
  // and all the other older elements have e->LockFor == null.
  // This property allows the use of a one-pass write & release loop. 
  // (That loop *MUST* operate in head to tail order to avoid WAW). 

  // First, we acquire write locks, even if there are still readers present.
  // 2nd, after having acquired all write locks we wait for any conflicting
  // readers to drain off.  This approach {acquire A; acquire B; waitdrain A; waitdrain B}
  // is superior to the simplistic and obvious {acquire A; waitdrain A; acquire B; 
  // waitdrain B;} as, in a sense, it allows this thread to wait concurrently for
  // for multiple readers.  That is, it admits more potential overlap and ||ism.    
  // As usual we use a bounded wait.  The only source of abort is deadlock.  

  for (e = End->Prev ; e != NULL ; e = e->Prev) { 
    if (!AcquireForWrite (Self, e)) {
      // Abort the txn
      DropWriteLocks (Self, e) ; 
      DropReadLocks (Self) ;  
      if (sc) schedctl_stop(sc) ;  
      TxAbort (Self) ;
      return 0 ; 
    }
  }
  if (Self->Skipped != 0) {
     Self->SpinBudget = 100 ;    //  TUNABLE!
     for (;;) { 
       int drg = 0 ; 
       for (e = End->Prev ; e != NULL ; e = e->Prev) { 
         ASSERT (IOwn(Self, *PSLOCK(e->Addr))) ; 
         if (e->Draining != 0) {
            volatile LockT * const LockFor = e->LockFor ; 
            ASSERT (LockFor != NULL) ; 
            LockT w = *LockFor ; 
            if ((w & RCMSK) == 0) {
              e->Draining = 0 ; 
            } else { 
              drg ++ ; 
              // CONSIDER: Self->SpinBudget -- ; 
            }
         }
       }
       if (drg == 0) break ; 
       if (--Self->SpinBudget <= 0) { 
          DropWriteLocks (Self, NULL) ; 
          DropReadLocks (Self) ; 
          if (sc) schedctl_stop(sc) ;  
          TxAbort (Self) ;
          return 0 ; 
       }
    }
  }

  // We're now committed - write-back and drop write-set locks
  // Spill deferred writes from the speculative store buffer to their 
  // ultimate locations. 
  // Must write-back in chronological order to avoid WAW hazards. 
  // For implicit privatization safety must hold all write-locks
  // until all writes are completed.  The usual 1-pass write-and-drop
  // loop breaks that invariant.  Instead, for IP-safety we must use
  // a 2-pass scheme with a write-back loop followed by an unlock loop.
  for (e = Start ; e != End ; e = e->Next) { 
    ASSERT (e->Draining == 0) ;
    ASSERT (e->Addr != NULL) ; 
    ASSERT (*PSLOCK(e->Addr) == Self->UniqID) ;  // expect (Owner=Self;RC=0)
    *(e->Addr) = e->Valu ;              // write-back
  }
  MEMBARSTST() ; 

  // Order independence: drop read locks, drop write locks
  for (e = Start ; e != End ; e = e->Next) { 
    ASSERT (e->Addr != NULL) ; 
    volatile LockT * const LockFor = e->LockFor ; 
    if (LockFor != NULL) {
      ASSERT (PSLOCK(e->Addr) == LockFor) ; 
      ASSERT (IOwn (Self, *LockFor)) ; 
      ASSERT (*LockFor == Self->UniqID) ;   // Expect(Owner=Self;RC=0)
      // Clear the Owner field, releasing the lock
      // 1. CAS of fullword, with precautionary loop
      // 2. partial-word ST into Owner subfield
      // CONSIDER: *((volatile short *)(UNS(LockFor) + OWNEROFF)) = 0 ; 
      *LockFor = 0 ; 
    }
    e->Addr    = NULL ;
    e->LockFor = NULL ;       // diagnostic hygiene - optional
  }

  DropReadLocks (Self) ; 
  if (sc) schedctl_stop(sc) ;  
  TxReset (Self) ; 
  Self->Retries = 0; 
  return 1 ;        // return success indication
}

// Compile-time assertions -- establish critical invariants
// It's always better to fail at compile-time than at run-time.  

void CTAsserts () { 
  // Ensure we're LP64
  CTASSERT (sizeof(intptr_t) == sizeof(long)) ;  
  CTASSERT (sizeof(long) == 8) ;
  // _TABSZ must be power-of-two
  CTASSERT ((_TABSZ & (_TABSZ-1)) == 0) ; 
  CTASSERT (PSSHIFT >= PSSHIFT_MIN) ; 
}

#define TXLDA(a)      TxLoad   (Self, (intptr_t *)(a))  
#define TXSTA(a,v)    TxStore  (Self, (intptr_t *)(a),(v)) 
#define TXLDV(a)      TxLoad   (Self, (intptr_t *) &(a))  
#define TXSTV(a,v)    TxStore  (Self, (intptr_t *) &(a), (v))  
#define TXLDF(o,f)    TxLoad   (Self, (intptr_t *)(&((o)->f)))  
#define TXSTF(o,f,v)  TxStore  (Self, (intptr_t *)(&((o)->f)), (intptr_t)(v))

intptr_t TxStatsLDS (Thread * t) { return 0 ; }
intptr_t TxStatsSTS (Thread * t) { return 0 ; }

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
