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
  TULTIMATE  
} Modes ; 

typedef enum {
  OWNERMSK       = 0xFFFF0000,
  OWNEROFF       = 4,
  RCMSK          = 0x0000FFFF,
  RC1            = 1,
  OWNERSHIFT     = 16
} LockWordLayout ; 

typedef enum { 
  NULLVER         = (int) 0xFFFFFFF0,     // CONSIDER 0x1
  RSSIZE          = 1024,
  NADA
} ManifestContants ; 

typedef int BitMap ;
typedef uintptr_t LockT ;        // RW lock (Owner,RC)
typedef uintptr_t vwLock ; 
typedef intptr_t IntPtr ; 
typedef unsigned char byte ; 

// AVPair  : write-set element
// RSEntry : read-set element


typedef struct { 
  LockT * LockFor ; 
} RSEntry ; 

typedef struct _AVPair {         // read-set and write-set log entry
    struct _AVPair * Next ;      // Next element in log
    struct _AVPair * Prev ;      // Prev element in log
    
    volatile intptr_t * Addr ;   // (Address,Value) pair
    intptr_t Valu ; 
    volatile LockT * LockFor ;   // LockFor points to the LockT covering Addr

    byte Size ; 
    byte Held ; 
    byte Draining ; 
} AVPair ; 

typedef struct _Log { 
    AVPair * List ; 
    AVPair * put ;               // Insert position - cursor
    int ovf ;                    // Overflow - request to grow 
    int CurrentLength ;          // # AVPairs on List
    BitMap BloomFilter ;         // Address exclusion fast-path test
    volatile int State ; 
    struct _Thread * Assoc ;
    double InitialBlock [1] ; 
} Log ; 

typedef struct _Thread { 
    int UniqID ;                // effectively final - write once
    int txseq ; 
    int Skipped ; 
    volatile int ctx ; 
    volatile int Mode ; 
    volatile int Periodic ; 
    volatile int Retries ; 
    volatile int ProgressCount ;
    schedctl_t * sc_self ; 
    BitMap rdSig ;                  // read-set signature - summary
    BitMap wrSig ;                  // write-set signature - summary
    int IsRO ; 
    int SpinBudget ;                // Credits -- Allowance
    int Aborts ;                    // Tally of # of aborts
    int rng ; 
    int xorrng [1] ; 
    int ovf ; 
    void * volatile CurrentTxn ; 
    void * NodeCache ; 
    int CachePopulation ; 
    RSEntry * rsp ; 
    BitMap rsFilter ; 
    void * volatile WaitsFor ;      // deadlock graf
    Log wrSet ; 
    Log LocalUndo ; 
    jmp_buf OnFailure ; 

    // Diagnostics and statistics
    int stats [12] ; 
    char * InFunc ; 
    int TxST, TxLD ; 

    RSEntry rsv [RSSIZE] ; 
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





