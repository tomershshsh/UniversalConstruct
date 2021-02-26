// %W% - %E%
// Copyright 2006, Sun Microsystems, Inc.  All Rights Reserved.
// Test harness for concurrent Key-Value maps.
// Supports the following key-value JUC "collection"-style maps:
// -- red-black trees
// -- skiplists
// -- hash tables, etc.
//
// Remarks:
// -- Provides lightweight content integrity checks.
//    The current implementation only covers the keys, but we should someday
//    enhance it track values as well.  Still, it's sufficiently low-cost
//    that we can leave it permanently enabled.
//
// TODO: 
// --   Add think-time parallel component : advance thread-local PRNG
// --   Reduce use of high-latency div and mod.  
//      These can also impact scalability on certain CMT platforms where
//      the DIVIDE unit might be shared between strands.
// --   Many apps demonstrate inter-transaction key locality.
//      Consider using a non-uniform PRNG with "memory" to generate a thread's next key.
//      That is, we'd attempt to approximate inter-operation spatial locality with a 
//      non-uniform PRNG to generate keys.  We're assuming "key locality" reflects as 
//      in spatial locality, which is reasonable.
// --   Consider having the primordial thread completely pre-allocate the full complement of 
//      nodes that the test will ever require (or at least some large N).
//      Presumably the nodes will be near each other spatially, and the TLB span of the
//      nodes (the covering) will be smaller than if we use libumem.so, which reduces
//      contention but can end up populating the tree with nodes from very distant TLBs.

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/lwp.h>
#include <pthread.h>
#include <unistd.h>
#include <poll.h>
#include <alloca.h>
#include <ctype.h>
#include <sys/processor.h>
#include <sys/procset.h>

#include "TL.h"
#include "RB-Tree.h"

enum { MAX_THREADS = 512 } ; 
#define DOUBLE(v) ((double)(v))

static int Verbose = 0 ; 
static volatile int threads_alive = 0;
static volatile int nDead = 0 ; 
static pthread_mutex_t StartGate [1];
static volatile int can_start = 0;
static volatile int stop_now  = 0 ;         // AKA: Halt
static volatile int DogWarn   = 0 ; 
static KVMap * ht;

static int arg_inserts = 0;
static int arg_deletes = 0;
static int arg_updates = 0 ; 
static int arg_thinks  = 0 ; 
static int arg_initial_size = 10000;
static int Determinism = 0 ; 
static int uniq ;           // Starting population
static int PreSum = 0 ; 
static int arg_opgroup = 1;
static int _nThreads = 8;
static int _Duration = 10 ;
static char * ExecutableName = NULL ; 
static char * Comment = "" ; 
static int arg_range = 1000000;
volatile char * Observable [256]  ; 

typedef struct {
    int pid;
    Thread * Self ; 
    pthread_t pthread_self ; 
    volatile int State ; 

    volatile int kSum ; 
    volatile int vSum ; 

    volatile int nUpdates ; 
    volatile int nDeletes ; 
    volatile int nInserts ; 
    volatile int nLookups ; 
    volatile int nMisses ; 
    volatile long TXAborts ;        // failed
    volatile long TXCompleted ;     // successful
    double pad [64] ; 
} thread_data_t;

static thread_data_t thread_data[MAX_THREADS];

static int BindSpan    = 0 ; 
static char * BindMapFile = NULL ; 
static int nCpu     = 0 ; 
static int nConfig  = 0 ; 
static int * CpuMap = NULL ; 

static int CpuBind () { 
  static volatile int ThreadSeq = 0 ;
  static int CpuBase = 0 ; 
  static pthread_mutex_t IncLock [1] ; 
  int CpuID = -1 ;
  if (nCpu > 1 && BindSpan != 0) {
    if (BindSpan > nCpu || BindSpan <= 0) {
      BindSpan = nCpu ;
    }
    // READ: int ix = Adjust (&ThreadSeq, 1) ;
    pthread_mutex_lock (IncLock) ; 
    int ix = ThreadSeq ++ ; 
    pthread_mutex_unlock (IncLock) ; 
    CpuID = CpuMap [CpuBase + (ix % BindSpan) ] ;
    int rslt = processor_bind (P_LWPID, P_MYID, CpuID, NULL) ;
    if (rslt != 0) {
      printf ("processor_bind (%d) failed\n", CpuID) ;
    } else
    if (Verbose) { 
      printf ("LWP %d bound to %d\n", _lwp_self(), CpuID) ;
    }
  }
  return CpuID ;
}

// Allow simplistic encoding B@<specification>
// Compact description of mapping between logical LWP ordinals and logical CPUIDs.  
// Logical CPUIDs in turn map to (Die,Core,Exu,Strand) physical resources.  
// Ultimately, we want direct control over LWP to physical resource mapping.
// 
// Examples
// +  Batoka: 4 chips; 8 cores/chip; 2 pipelines/core; 4 strands/pipeline
//    Apparent Batoka logical CPUID to physical mapping on snv_78:
//      cpuid = (chipid:2, coreid:3, exeuid:1, strandid:2)
//    See also Solaris source kit -- lwp_create() calls lgrp_move_thread (lgrp_choose()) 
//    to place a newly created LWP
//    Batoka and N2 specifics
//      Pipelines share ifetch and instruction picker.
//      cores share one FPU
//      EXU AKA Pipeline AKA "TG" or thread-group
// +  N1 (T1) : 8 cores/chip; 4 strands/core
// +  N2 (T2) : 8 cores/chip; 2 pipelines/core; 4 strands/pipeline
//
// Usage:
// --   B@D[0,64,128,196]S[0,1,2,3]X[0,8]C[0,8,16,24,32,40,48,56]
//      D=Die,S=Strand,X=P=Pipeline,C=Core

static int * CpuBuildMap (char * BindMapFile) {
  nCpu    = sysconf (_SC_NPROCESSORS_ONLN) ;
  nConfig = sysconf (_SC_NPROCESSORS_CONF) ;
  int * CpuMap  = (int *) malloc (sizeof(CpuMap[0]) * (nConfig+1)) ;
  if (BindMapFile == NULL) { 
    memset (CpuMap, 0, sizeof (CpuMap[0]) * nConfig) ;
    int j, found ; 
    for (j = found = 0 ; found < nCpu ; j++ ) {
      int rslt = p_online (j, P_STATUS) ;
      if (rslt == P_ONLINE) {
        CpuMap[found++] = j ;
      }
    }
  } else { 
    FILE * map = fopen (BindMapFile, "r") ; 
    if (map == NULL) { 
      printf ("Couldn't open Bind map file: %s\n", BindMapFile) ; 
      exit (1) ; 
    }
    int putIndex = 0 ; 
    for (;;) {
      if (putIndex > nConfig) {
        printf ("Warning: more entries in %s than are needed - ignored\n", BindMapFile) ; 
        break ; 
      }
      // Read the next CPUID # from the file -- currently one per line
      char buf [128];
      memset (buf, 0, sizeof(buf)) ; 
      if (fgets (buf, sizeof(buf)-1, map) == NULL) break ;
      if (buf[0] == '#') continue ;     // skip comment line
      // TODO: allow thread#:cpuid pairs in any order
      // TODO: add an inner loop -- allow multiple numbers per line
      // TODO: consider sscanf()
      // TODO: skip blank lines
      int id = strtol (buf, NULL, 0) ; 
      // Validate CPUID
      if (id < 0 || p_online(id, P_STATUS) != P_ONLINE) {
        printf ("Invalid CPUID %d in Bind Map file: %s\n", id, BindMapFile) ; 
        exit (1) ;
      }
      // Check for duplicates
      int d ; 
      for (d = 0 ; d < putIndex && CpuMap[d] != id; d++) ; 
      if (d != putIndex) { 
        printf ("Warning: duplicate entries in Bind map file: %s %d\n", BindMapFile, id) ; 
      }
      // Insert into vector
      if (Verbose) printf ("%d ", id) ; 
      CpuMap[putIndex++] = id ; 
    }
    fclose (map) ; 
    if (putIndex < nConfig) {
      printf ("Needed %d from %s but only got %d\n", nConfig, BindMapFile, putIndex) ; 
      exit (1) ; 
    }
  }
  return CpuMap ;
}

// Simplistic low-quality Marsaglia shift-xor PRNG.
// Bijective except for the final masking operation.
// Cycle length for non-zero values is 4G-1.
// 0 is absorbing and should be avoided -- fixed point.
// Currently we seed/reseed with gethrtime() assuming that the various threadis
// will typically be seeded with distinct and different values.  
// Beware that if gethrtime() is "coarse" and the threads happen
// to seed at nearly the same time we could end up with common
// seeds and entrainment problems.    
 
static inline int MarsagliaNext (int v) {
  if (v == 0) v = 1 ;       // gethrtime()|1
  v ^= v << 6;
  v ^= ((unsigned)v) >> 21;
  v ^= v >> 7 ; 
  return v ;
}

static inline int MarsagliaXOR (int * seed) {
  int x = *seed;
  if (x == 0) x = 1 ;       // gethrtime()|1
  x ^= x << 6;
  x ^= ((unsigned)x) >> 21;
  *seed = x ^= x << 7;
  return x & 0x7FFFFFFF;
}

static inline int MarsagliaG (int * seed) {
  static int gseed = 0 ; 
  if (seed == NULL) seed = &gseed ; 
  int x = *seed;
  if (x == 0) x = 1 ;       // gethrtime()|1
  x ^= x << 6;
  x ^= ((unsigned)x) >> 21;
  *seed = x ^= x << 7;
  return x & 0x7FFFFFFF;
}

static inline int _MarsagliaXOR (int x) {
  if (x == 0) x = gethrtime()|1 ;  
  x ^= x << 6;
  x ^= ((unsigned)x) >> 21;
  x ^= x << 7;
  return x ;    // CONSIDER:  return x & 0x7FFFFFFF;
}

static inline int NextRandom (int * x) { 
   *x = _MarsagliaXOR (*x) ; 
   return (*x) & 0x7FFFFFFF ; 
}

static int ParkMillerRNG(int *seed0) {
  const int a =      16807;
  const int m = 2147483647;
  const int q =     127773;  /* m div a */
  const int r =       2836;  /* m mod a */
  int seed = *seed0;
  int hi   = seed / q;
  int lo   = seed % q;
  int test = a * lo - r * hi;
  if (test > 0)
    seed = test;
  else
    seed = test + m;
  *seed0 = seed;
  return seed;
}

#define TLRand(sa) MarsagliaXOR(sa)

void * Worker (void *arg) { 
  CpuBind () ; 
  int TallyMisses  = 0 ; 
  int TallyUpdates = 0; 
  int TallyInserts = 0 ; 
  int TallyDeletes = 0 ; 
  int TallyLookups = 0 ; 
  thread_data_t * const pdata = (thread_data_t *)arg;
  Thread * const Self = TxNewThread () ; 
  pdata->Self = Self ; 
  // Color the stack offsets
  Observable[128] = alloca ((pdata->pid * 7297) & 0xFFFF) ; 
  int seed = (((int) &seed) + gethrtime()) | 1 ; 
  if (Determinism) { 
    seed = pdata->pid ; 
    if (seed == 0) seed = 0xD1CE ; 
  }
  pthread_mutex_lock(StartGate);
  if (pdata->pid == 0) {
    printf ("Initializing ...") ; 
    for (int i = 0; i < arg_initial_size; i++) {
      int key = TLRand(&seed) % arg_range ; 
      if (!kv_contains (Self, ht, key)) {  ; 
        kv_put(Self, ht, key, key);
        ++uniq ; 
        PreSum += key ; 
      }
    }
    printf ("Initialized %d unique of %d\n", uniq, arg_initial_size) ; 
  }
  ++threads_alive;
  pthread_mutex_unlock(StartGate );

  // Hoist globals into local immutables 
  const int _grp  = arg_opgroup ; 
  const int _ups  = arg_updates ;
  const int _ins  = arg_inserts ; 
  const int _dels = arg_deletes ; 
  const int _rng  = arg_range ; 
  KVMap * const _ht = ht ; 

  int keysum = 0 ;      // key integrity check - thread-specific component

  // Consider using a civilized and proper consensus barrier.  
  // Using poll(NULL,0,1) is tempting, but the implementation currently
  // spins intentionall to keep the threads ONPROC and avoid migration.
  while (!can_start) ; 
  if (Verbose) printf ("[%d] ", getcpuid()) ; 

  while (!stop_now) {
     int op = TLRand (&seed) % 100 ; 

     // Consider using something like:
     //  int opgroup = 1;
     //  if (_grp != 1) opgroup = TLRand (&seed) % _grp;
     // This would cut down unnecessary loop overhead and
     // further reduce use of DIV and MOD.
     int opgroup = 1 ; 
     if (_grp != 1) opgroup = TLRand (&seed) % _grp ; 

     if (op < _ins) {
       // Case: Insert
       int i ; 
	   for (i = 0; i < opgroup; i++) { 
         int key = TLRand(&seed) % _rng ; 
	     if (kv_insert(Self, _ht, key, key )) keysum += key ; 
           TallyInserts ++ ; 
	     }
     } else if (op >= _ins && op < (_ins+_ups)) { 
       // Case: Put AKA Update
       int i ; 
	   for (i = 0; i < opgroup; i++) { 
         int key = TLRand(&seed) % _rng ; 
         if (kv_put (Self, _ht, key, TLRand(&seed))) keysum += key ; 
         TallyUpdates ++ ; 
	   }
	 } else if (op >= (100 - _dels)) {
       // Case: Delete
       int i ; 
	   for (i = 0; i < opgroup; i++) {
         int key = TLRand(&seed) % _rng ; 
	     if (kv_delete(Self, _ht, key)) keysum -= key ; 
           TallyDeletes ++ ; 
	     }
     } else {
       int i ; 
	   for (i = 0; i < opgroup; i++) { 
         int key = TLRand(&seed) % _rng ; 
	     int hit = kv_get(Self, _ht, key);
         TallyLookups ++ ; 
         if (!hit) TallyMisses ++ ; 
	   }
	}
  }

  pdata->nMisses  = TallyMisses ; 
  pdata->nUpdates = TallyUpdates ; 
  pdata->nDeletes = TallyDeletes ; 
  pdata->nLookups = TallyLookups ; 
  pdata->nInserts = TallyInserts ; 
  pdata->kSum     = keysum ; 

  pthread_mutex_lock (StartGate) ;
  ++nDead ;
  -- threads_alive ;
  // XXXX ASSERT (threads_alive >= 0) ; 
  pthread_mutex_unlock (StartGate) ;

  return NULL;
}



static int SelfTest (Thread * Self, KVMap * ht) { 
  enum { RANGE = 5000} ; 
  static int map [RANGE] ;    // Tracks the ht - key:value map
  int seed = gethrtime() ;  
  printf ("(1) Single-threaded self-test\n") ; 
  int i ; 
  for (i = 0 ; i < 2000000 ; i++ ) { 
     int op = TLRand(&seed) % 100 ; 
     if (op < 20) { 
        int key = TLRand (&seed) % RANGE ; 
        kv_delete (Self, ht, key) ; 
        map [key] = 0 ; 
        if (kv_get (Self, ht, key) != 0) { 
           printf ("%d: delete read-back %d\n", key, kv_get (Self, ht, key)) ; 
        }
     } else 
     if (op >= 20 && op < 50) { 
       int key = TLRand (&seed) % RANGE ; 
       int val = TLRand (&seed) ; 
       kv_put (Self, ht, key, val) ; 
       map [key] = val ; 
     } else { 
       int key = TLRand (&seed) % RANGE ; 
       if (kv_get(Self, ht, key) != map[key]) { 
         printf ("%d: map=%d ht=%d\n", key, map[key], kv_get(Self, ht, key)) ; 
       }
     }
  }
  printf ("(2) read-back\n") ; 
  for (i = 0 ; i < RANGE ; i++) { 
     if (kv_get (Self, ht, i) != map[i]) { 
       printf ("%d: map=%d ht=%d\n", i, map[i], kv_get(Self, ht, i)) ; 
     }
  }
  printf ("(3) integrity check\n") ; 
  kv_verify (ht, 1) ; 
  printf ("(4) completed\n") ; 
  return 0 ; 
}

static pthread_t LaunchThread (void * (*func)(void *), void * arg) { 
  int rc ; 
  pthread_t newid [1] ;
  pthread_attr_t hx [1];
  pthread_attr_init (hx);
  newid[0] = 0 ; 
  if (1) {
      pthread_attr_setschedpolicy(hx, SCHED_RR);
      pthread_attr_setscope(hx, PTHREAD_SCOPE_SYSTEM) ;
      // Beware: DETACHED -> not join()able
      pthread_attr_setdetachstate (hx, PTHREAD_CREATE_DETACHED) ;
   }
   rc = pthread_create(newid, hx, func, arg);
   if (rc != 0) {
      printf("error creating thread, error: %s\n", strerror(rc));
      return 0 ;
   }
   return newid[0] ; 
}

typedef struct {
   void * (*func)() ; 
   void * arg ; 
   int Color ; 
} Trampoline ; 


static void * Thunk (void * tmp) { 
   Observable[128]  = alloca ((gethrtime() * 7297) & 0xFFFF) ;     // color stack
   void * (*func)() = ((Trampoline *)tmp)->func ; 
   void * arg       = ((Trampoline *)tmp)->arg ; 
   free (tmp) ; 
   return (*func)(arg) ; 
}

static pthread_t LaunchThreadC (void * (*func)(void *), void * arg) { 
  int rc ; 
  pthread_t newid [1] ;
  pthread_attr_t hx [1];
  pthread_attr_init (hx);
  newid[0] = 0 ; 
  if (1) {
      pthread_attr_setschedpolicy(hx, SCHED_RR);
      pthread_attr_setscope(hx, PTHREAD_SCOPE_SYSTEM) ;
      // Beware: DETACHED -> not join()able
      pthread_attr_setdetachstate (hx, PTHREAD_CREATE_DETACHED) ;
   }
   Trampoline * tmp = (Trampoline *) malloc (sizeof(Trampoline)) ; 
   tmp->func = func ; 
   tmp->arg  = arg ; 
   rc = pthread_create(newid, hx, Thunk, tmp);
   if (rc != 0) {
      free (tmp) ; 
      printf("error creating thread, error: %s\n", strerror(rc));
      return 0 ;
   }
   return newid[0] ; 
}

static void * WatchDog (void * arg) {
  printf ("WatchDog running\n") ;
  poll (NULL, 0, _Duration * 2 * 1000) ;
  DogWarn = 1 ; 
  printf ("WATCHDOG WARNING\n") ;
  poll (NULL, 0, _Duration * 4 * 1000) ;
  printf ("WATCHDOG WARNING\n") ;
  poll (NULL, 0, 3 * 1000) ;
  printf ("WATCHDOG TIMEOUT!\n") ;
  _exit (1) ; 
  return NULL ; 
}

static int ParseInt (char * str) {
   char * p = strdup (str) ;
   char * const m = p ;
   int mulf = 1 ;
   while (isxdigit(*p) || *p == 'x' || *p == 'X') ++p ;
   if (*p == 'k' || *p == 'K') {
      mulf = 1024 ;
      *p = 0 ;
   } else
   if (*p == 'm' || *p == 'M') {
      mulf = 1024*1024 ;
      *p = 0 ;
   }
   int v = strtol (m, NULL, 0) * mulf ;
   free (m) ;
   return v ;
}

int main(int argc, char *argv[]) { 
    int i, k;
    setbuf (stdout, NULL) ; 
    ExecutableName = argv[0] ; 
    TxOnce () ; 
    kv_init();

    --argc ; 
    ++argv ; 
    while (argc > 0) {
       char * p = *(argv++)  ; 
       if (*p == '-') ++p ; 
       --argc ; 
       switch (*(p++)) { 
       case 'B':
          BindSpan = -1 ;
          BindMapFile = NULL ; 
          if (*p == ':') { 
            BindMapFile = p+1 ; 
            printf ("Binding: %s\n", BindMapFile) ; 
          } else { 
            printf ("Binding 1:1\n") ; 
          }
          CpuMap = CpuBuildMap(BindMapFile) ; 
          break ; 
       case 'D':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         _Duration = ParseInt (p) ; 
         break ; 
       case 's':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         arg_initial_size = ParseInt (p) ; 
         break ; 
       case 'r':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         arg_range = ParseInt (p) ; 
         break ; 
       case 'c':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         arg_thinks = ParseInt (p) ; 
         break ; 
       case 'n':
       case 'T':
       case 't':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         _nThreads = ParseInt (p) ; 
         if (_nThreads < 0 || _nThreads > MAX_THREADS) { 
           printf ("nThreads=%d\n", _nThreads) ; 
           exit (1) ; 
         }
         break ;    
       case 'u':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         arg_updates = ParseInt (p) ; 
         break ;    
       case 'i':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         arg_inserts = ParseInt (p) ; 
         break ; 
       case 'd':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         arg_deletes = ParseInt (p) ; 
         break ; 
       case 'g':
         if (*p == 0 && argc > 0) { 
            p = *(argv++) ; --argc ; 
         }
         arg_opgroup = ParseInt (p) ; 
         break ; 
       case 'C':
         Comment = p ;     // consider strdup+strcat
         break ; 
       case 'V':
         ++Verbose ; 
         break ; 
       case 'R':            // reproducibility
         Determinism ++ ; 
         printf ("Determinism=%d\n", Determinism) ; 
         break ; 
       default:
         printf ("UNKNOWN SWITCH: %s\n", p-1) ; 
	   }
    }
    if (arg_deletes > arg_updates) {
      printf ("WARNING: deletes >> updates -- tree will converge to empty\n") ; 
    }

    ht = kv_create(-1, NULL);

    // Run a quick smoke-test
    Thread * const Self = TxNewThread () ; 
    if (getenv ("SELFTEST") != NULL) { 
        SelfTest (Self, ht) ; 
    }

    if (Verbose & 2) { 
       printf ("  ") ; 
       printf ("V%d ",             kv_verify (ht, 0)) ; 
       printf ("INSERT(55)=%d "  , kv_insert (Self, ht, 55, 55)) ; 
       printf ("INSERT(55)=%d "  , kv_insert (Self, ht, 55, 55)) ; 
       printf ("GET(55)=%d "     , kv_get    (Self, ht, 55)) ;
       printf ("CONTAINS(55)=%d ", kv_contains (Self, ht, 55)) ; 
       printf ("DELETE(55)=%d "  , kv_delete (Self, ht, 55)) ; 
       printf ("DELETE(55)=%d "  , kv_delete (Self, ht, 55)) ; 
       printf ("GET(55)=%d "     , kv_get    (Self, ht, 55)) ;
       printf ("CONTAINS(55)=%d ", kv_contains (Self, ht, 55)) ; 
       printf ("V%d\n",            kv_verify (ht, 0)) ; 
       printf ("  ") ; 
       printf ("Insert ") ; 
       int i ; 
       for (i = 0 ; i < 20 ; i++) kv_insert (Self, ht, i ^ 0x5A, i ^ 0x5A) ; 
       int fail = 0 ;
       for (i = 0 ; i < 20 ; i++) {
          kv_delete (Self, ht, i ^ 0x5A) ; 
          if (fail == 0 && !kv_verify (ht, 0)) {
             ++fail ;
             printf ("fail >> %d (%d)\n", i, (i ^ 0x5A)) ; 
          }
       }
       if (kv_verify (ht, 0) <= 0) printf ("VERIFY FAILURE!\n") ; 

       printf (" " ) ; 
       printf ("GET(1000)=%d "   , kv_get(Self, ht, 1000)) ; 
       printf ("SET(1000,1)=%d " , kv_put(Self, ht, 1000, 1)) ; 
       printf ("GET(1000)=%d "   , kv_get(Self, ht, 1000)) ; 
       printf ("SET(1000,2)=%d " , kv_put(Self, ht, 1000, 2)) ; 
       printf ("GET(1000)=%d "   , kv_get(Self, ht, 1000)) ; 
       printf ("V%d\n", kv_verify (ht, 0)) ; 
       printf ("\n") ; 
    }

    pthread_mutex_init(StartGate , NULL);
    LaunchThread (WatchDog, NULL) ; 

    printf ("Launching...") ; 
    for (i = 0; i < _nThreads ; i++) {
	   thread_data[i].pid = i;
       thread_data[i].pthread_self = LaunchThread (Worker, (void *)(thread_data + i));
    }
    poll (NULL, 0, 100) ; 
    printf ("Launched...") ; 

    // Wait for all the threads to be running. 
    int dw = 0 ; 
    while (threads_alive < _nThreads) { 
       poll (NULL,0,10); 
       if (DogWarn && dw == 0) { 
         dw = 1 ; 
         printf ("Waiting %d %d %d\n", threads_alive, nDead, _nThreads) ; 
       }
    }

    // Quiesce
    poll (NULL, 0, 10) ; 
    
    printf ("Starting...") ; 
    hrtime_t start_time = gethrtime();
    can_start = 1;
    poll (NULL, 0, _Duration*1000) ; 
    stop_now  = 1;
    hrtime_t end_time = gethrtime();
    printf ("shutdown...") ; 

    // Wait for all the threads to shut down.
    // Note that they're not necessarily joinable.  
    while (threads_alive != 0) { poll (NULL, 0, 20); } 

    // TODO: report min, miax, stddev, median, average, spread for (I,D,L,U)

    printf("results:\n");
    int all_inserts = 0 ; 
    int all_misses  = 0 ; 
    int all_updates = 0 ; 
    int all_deletes = 0 ; 
    int all_lookups = 0 ; 
    int all_aborts  = 0 ; 
    int AggSum      = 0 ; 
    int64_t MaxCompleted = 0 ; 
    int64_t MinCompleted = -1 ; 
    int nOps = 0 ; 
    int64_t lds, sts ;
    lds = sts = 0 ; 
    for (i = 0; i < _nThreads; i++) {
       int64_t Completed = 
          thread_data[i].nInserts + 
          thread_data[i].nDeletes +
          thread_data[i].nUpdates + 
          thread_data[i].nLookups ; 
       if (Completed > MaxCompleted) MaxCompleted = Completed ; 
       if (Completed < MinCompleted || MinCompleted < 0) MinCompleted = Completed ; 
       if (Verbose) { 
	     printf("(%d, %d, %d, %d) ", 
	   	   thread_data[i].nInserts, 
	   	   thread_data[i].nDeletes, 
	   	   thread_data[i].nUpdates,
           thread_data[i].nLookups) ; 
       }
       lds         += thread_data[i].Self->TxLD ; 
       sts         += thread_data[i].Self->TxST ; 
       all_aborts  += thread_data[i].Self->Aborts ; 
       all_misses  += thread_data[i].nMisses ; 

       all_inserts += thread_data[i].nInserts ; 
       all_updates += thread_data[i].nUpdates ; 
       all_lookups += thread_data[i].nLookups ; 
       all_deletes += thread_data[i].nDeletes ; 
       AggSum      += thread_data[i].kSum ; 
        
       if (Verbose && (i % 3) == 2) printf ("\n") ; 
    }
    if (Verbose) printf ("\n") ;
    nOps = all_inserts + all_updates + all_deletes + all_lookups ; 

    printf ("Post validation : ") ; 
    // Crude - iterate over the key space to determine the population (cardinality)
    // of the RB-tree.  A better way would be to augment the recursive _verify()
    // routine to return the population.  
    int upop = 0 ; 
    int PostSum = 0 ; 
    for (i = 0 ; i < arg_range ; i++) { 
       upop += kv_contains (Self, ht, i) != 0 ; 
       if (kv_contains (Self, ht, i)) PostSum += i ; 
    }
    if ((PreSum + AggSum) != PostSum) { 
       printf ("ERROR!: Lightweight key integrity check failure %X+%X != %X\n",
         PreSum, AggSum, PostSum) ; 
       exit (1) ; 
    } else { 
      printf ("[pass] Content Integrity check: %X+%X=%X\n", PreSum, AggSum, PostSum) ; 
    }

    printf ("TEST: %s %dT %d msecs ins=%%%d del=%%%d ups=%%%d isize=%d (initpop=%d) range=%d\n", 
      TxDescribe(),
      _nThreads, _Duration*1000, arg_inserts, arg_deletes, arg_updates, arg_initial_size, 
      uniq, arg_range) ; 
    

    // uniq is the initial population.  
    // TODO: We should report the final population, too.  

    printf ("RESULTS: Dur=%d pop=%d U=%d I=%d D=%d L=%d (Misses=%d) SPREAD=%g TOTAL=%d\n", 
	    (int)((end_time - start_time) / 1000000),
        upop,
        all_updates, all_inserts, all_deletes, all_lookups, all_misses, 
        DOUBLE(MaxCompleted)/DOUBLE(MinCompleted+1),
        nOps) ; 
    printf ("RESULTS: TxLDs=%lld TxSTs=%lld\n", lds, sts) ; 
    printf ("SUMMARY: %s %s %s %dT I%d D%d U%d L%d (%d,%d) ABO=%d pop=%d -> %d Ops\n", 
        ExecutableName,
        Comment,
        TxDescribe(), 
        _nThreads, arg_inserts, arg_deletes, arg_updates,
        100 - (arg_inserts + arg_deletes + arg_updates),
        arg_initial_size, arg_range, all_aborts, upop, nOps) ;

    // Run a post-mortem verification pass.
    // There are no concurrent operations on the ht.
    // TODO: implement a non-transactional verify we can run when
    // the ht is stable and queisced. 
    // Verify post-conditions.
    int vfy = kv_verify (ht, 1) ; 
    printf ("VERIFY=%d ", vfy) ; 
    if (vfy <= 0) { 
      printf ("ERROR! - Structural Integrity Failure\n") ; 
      exit (1) ; 
    }
    printf ("\n") ; 
    TxShutdown() ; 

    // consider: enable MSA? 
    struct rusage ru ; 
    getrusage (RUSAGE_SELF, &ru) ;
    printf ("User=%ld System=%ld msecs\n", 
        ((ru.ru_utime.tv_sec * 1000000L) + ru.ru_utime.tv_usec)/1000L, 
        ((ru.ru_stime.tv_sec * 1000000L) + ru.ru_stime.tv_usec)/1000L) ; 

    return 0;
}

