// Copyright 2006 Sun Microsystems, Inc.  All Rights Reserved.

#include <limits.h>
#include <setjmp.h>
#include <schedctl.h>

// For inclusion into C++ compiliation units

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  TIDLE           = 0,        // Non-transactional
  TTXN            = 1,        // Transactional mode
  TABORTING       = 3,        // aborting - abort pending
  TABORTED        = 5,        // defunct - moribund txn
  TCOMMITING      = 6,
  TULTIMATE  
} Modes ; 

typedef enum {
  LOCKBIT         = 1,
  VER1            = 2,
  VERMSK          = ~LOCKBIT,
  VERSHIFT        = 1,
  NULLVER         = (int) 0xFFFFFFF0,     // CONSIDER 0x1
} LockWordLayoutAndEncoding ; 

typedef enum {
  RSSIZE          = 1024,
  LSSIZE          = 128,
  WSSIZE          = 512,
  SPINBUDGET      = 100,
} Tunables ; 

typedef enum { ONE=1, } ManifestConstants ; 

typedef int BitMap ;
typedef uintptr_t xword ;        // transactional word size
typedef uintptr_t vwLock ;       // (Version,LOCKBIT) - Lockword : TODO: uint64t .  
typedef vwLock LockT ;           // RW lock
typedef intptr_t IntPtr ; 
typedef unsigned char byte ; 

// AVPair  : write-set element
// rsEntry : read-set element

typedef struct _AVPair {         // read-set and write-set log entry
  struct _AVPair * Next ;        // Next element in log
  struct _AVPair * Prev ;        // Prev element in log
    
  volatile intptr_t * Addr ;     // (Address,Value) pair
  intptr_t Valu ; 
  volatile vwLock * LockFor ;    // LockFor points to the vwLock covering Addr
  vwLock rdv ;                   // read-version @ time of 1st read - observed
  byte Held ; 
  byte IsRW ;                    // W vs RW
  byte Size ;                    // 0,1,2,4,8 bytes

  // Position rarely used elements at the tail
} AVPair ; 

typedef struct {                 // read-set element
  vwLock * LockFor ; 
} rsEntry ; 

typedef struct _Log { 
  AVPair * List ; 
  AVPair * put ;                 // Insert position - cursor
  int ovf ;                      // Overflow - request to grow 
  int CurrentLength ;            // # AVPairs on List
  BitMap BloomFilter ;           // Address exclusion fast-path test
  volatile int State ; 
  struct _Thread * Assoc ;
  double InitialBlock [1] ; 
} Log ; 

typedef struct _Thread { 
  int UniqID ; 
  volatile int ctx ; 
  volatile int Mode ; 
  volatile int Periodic ; 
  volatile int Retries ; 
  volatile int ProgressCount ;
  schedctl_t * sc_self ;          // effective final field : write once
  BitMap rdSig ;                  // read-set signature  (summary)
  BitMap wrSig ;                  // write-set signature (summary)
  vwLock rv ; 
  vwLock wv ; 
  vwLock abv ; 
  vwLock * CfLock ;               // Last conflicting Lock ref
  intptr_t CfAddr ;               // Last conflicting Address
  int * ROFlag ; 
  int IsRO ; 
  int SpinBudget ;                // Credits -- Allowance
  int Aborts ;                    // Tally of # of aborts
  int rng ; 
  int xorrng [1] ; 
  int ovf ; 
  void * volatile CurrentTxn ; 
  void * NodeCache ; 
  int CachePopulation ; 
  int Color ; 
  void * volatile WaitsFor ;      // deadlock graph
  rsEntry * rsp ; 
  BitMap rsFilter ; 
  rsEntry * rsExtent ; 
  rsEntry * rsBase ; 
  Log wrSet ; 
  Log LocalUndo ; 
  rsEntry rsv [RSSIZE] ; 

  jmp_buf OnFailure ; 

  // Diagnostics and statistics
  int stats [12] ; 
  char * InFunc ; 
  int TxST, TxLD ; 
} Thread ; 

typedef struct {
  void * sx ; 
} TxSession ; 


extern Thread * TxNewThread () ; 
extern char   * TxDescribe  () ; 
extern void   * TxStart     (Thread * Self, int * ROFlag) ; 
extern int      TxCommit    (Thread * Self) ; 
extern int      TxValid     (Thread * Self) ; 
extern void     TxOnce      () ; 
extern void     TxShutdown  () ; 
extern void     TxStore     (Thread * Self, volatile intptr_t * addr, intptr_t v) ; 
extern intptr_t TxLoad      (Thread * Self, volatile intptr_t * addr) ; 
extern intptr_t TxStatsLDS  (Thread * Self) ; 
extern intptr_t TxStatsSTS  (Thread * Self) ; 



