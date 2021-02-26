
2008-9-5

*   Attached is our "technology demonstrator" source release.  It's made 
    available under a very liberal BSD/OSI open source license.  The code is rather 
    primitive, but implements and illustrates the fundamentals of TL2.  It's currently
    SPARC-specific, but you can get the intel port from Stanford (http://stamp.stanford.edu). 
    The stamp project also provides a divserse set of benchmarks written directly to
    the low-level TL2 programming interface.  

    The TL2 sources are library-based in that the developer must explicitly label all 
    transactional reads and writes.  Ultimately, of course, we believe that STM
    support should be integrated into compilers.

*   The release contains two STMs, TLRW-redo and TL2-Ref, both of which
    support the existing TL* programming interface (TxLoad, etc).  
    TLRW-redo is provided only for comparison.  
    
    TL2-Ref4 is the latest incarnation of TL2 as described in my DISC 2006 paper.
    The code has evolved slightly since the original source release and Stanford
    Stamp fork point.  The makefiles build two distinct TL2 targets, -GV4 and -GV6.
    Refer to the TL2-Ref4/RELEASENOTES.txt file for details.  GV4 has a slightly
    lower abort rate but generates more read-write traffic on the centralized
    global version number.  GV6 admits more false aborts than GV4 but 
    also generates much less coherence traffic advancing the global clock.
    Which one is better depends on the aggregate mutation rate (more
    precisely the rate of txns that try to commit that are writers),
    the degree of concurrency, and the communication costs of the platform.
    GV6 tends to do much better under high mutation, on large systems
    such as a Maramba, or on systems where communication costs are high.
    GV4 tends to be better on simple N1 or N2 systems or low-order MP platforms.

    TLRW-redo is a simple lock-based STM that uses per-stripe read-write
    locks instead of versions numbers as found in TL2.  It provides implicit
    privatization but unfortunately readers must write, and can generate
    considerable cache coherence traffic.  Relatedly, we can encounter CAS
    interference trying to increment or decrement the reader count subfield
    of the lock-words.  Finally, CAS itself incurs considerable local latency
    particularly on Niagara-family machines where the implementation currently
    causes invalidation of the memory operand from the L1$.  
    (See http://blogs.sun.com/dave/entry/cas_and_cache_trivia_invalidate).  
    The TLRW* family tends to have very low abort rates, as the sole source of abort 
    is deadlock avoidance.  Generally you can expect TLRW-redo to perform 
    worse than TL2.  As the name would suggest TLRW-redo uses a redo
    log with deferred write-lock acquisition and deferred writes in order
    to minimize the write-lock hold times.  

    TLRW-redo tends to perform relatively worse on platforms where CAS is
    expensive or communication costs are high. 

    Both TLRW-redo and TL2-Ref4 use 8-xword (64-byte) stripes.  That is,
    data stripes coincide with $ lines.  (The makefile build target names
    include a -8W suffix indication an 8-xword stripe width).

*   I recommend a quick skim of the TL2 RELEASENOTES.txt file to familiarize
    yourself with some of the configuration options and switches.
 
*   I recommend running with libumem.so (setenv LD_PRELOAD libumem.so)
    to prevent malloc/free scalability issues from dominating performance.
    Beware, however, that libumem.so can result in strange performance
    anomalies.  See the extended commentary in RELEASENOTES.txt

*   To simplify the configuration I've specialized the code and makefiles
    to target Solaris/SPARC/v9/suncc.

*   Source delivery directory layout

    ./TL2-Ref4                      : TL2 subdirectory
    ./TL2-Ref4/RELEASENOTES.txt     : TL2 release notes
    ./TL2-Ref4/Makefile             : gmake-style makefile
                                      generates rb-GV4-8W and rb-GV6-8W
    ./TL2-Ref4/TL.c                 : TL2 STM body, exposes TL* interface
    ./TL2-Ref4/TL.h
    ./TL2-Ref4/rb-GV6-8W            : make target - red-black tree; TL2-GV6 with an 8-xword stripe
    ./TL2-Ref4/rb-GV4-8W            : make target - red-black tree; TL2-GV4 with an 8-xword stripe
                                      These binaries include the RB-Harness code which makes
                                      calls to the RB-Tree implementation, which in turn
                                      invoke the TL.c STM operators.  
    ./TLRW-redo                     : TLRW-redo STM subdirectory
    ./TLRW-redo/Makefile            : gmake-style makefile
    ./TLRW-redo/TL.c                : TLRW-redo STM body, exposes TL* interface
    ./TLRW-redo/TL.h
    ./TLRW-redo/rb-8W               : make target - red-black tree; TLRW-undo with an 8-xword stripe
    ./if.h                          : standard interface
    ./cas64.il                      : SunCC .il for CAS, MEMBAR, etc
    ./RB-Tree.c                     : red-black tree concurrent key-value 
                                      map written to the TL* interface
    ./RB-Tree.h                        
    ./RB-Harness.c                  : Concurrent key-value map test harness.
                                      Makes calls to entry points in RB-Tree.c
    ./README.txt                    : this file
    ./SOFTWARELICENSE.txt           : TL2 release boilerplate
    ./LEGALNOTICE.txt               : TL2 release boilerplate

*   The STM code can be used as a binary library or included directly
    into the application code, allowing higher performance through 
    compiler-based inlining.  The source above are configured for the latter.  
    RB-Tree.c includes <STM>/TL.c directly.  The test harness, RB-Harness.c calls entry
    points in RB-Tree.c.  
    
*   The read-set and write-set have fixed limits in the current code.
    If you overflow the set you'll see a "Read-Set Overflow" or "Write-Set Overflow"
    message on stderr and the program will exit.  You can easily increase
    the default set sizes at compile-time, or augment the code to automatically
    resize the sets at runt-time.

*   To run the benchmark you could start with command line of the form:
      rb-GV4-8W D10 d30 u30 s1000 r2000 t8
    This gives a 10 second measurement interval (D10) with 30% deletes,
    30% updates with an initial population of 1000 entries in a key-space of
    2000 entries and running 8 threads.  Results are reported in aggregate
    operations completed during the measurement interval.  Higher is better.
    Note that an update operation installs a new key-value pair in the
    map (structural update) if the key isn't already present, while if the 
    key is present the update only modifies the existing value (non-structural
    update).  

    The benchmark also makes a good test of STM correctness.  The harness automatically
    performs lightweight content integrity tests on the keys, as well as post-run
    structural integrity checks of the red-black tree.  We've found that the red-black
    tree is amazingly sensitive to STM concurrency bugs.  Errors tend to accumulate
    and are "sticky".  If you notice the integrity checks failing, hangs, traps, 
    or other errors you should be concerned.  And of course red-black trees in
    general are a nice example of the application of transactional memory as they're
    a reasonably useful data structure but notoriously hard to convert correctly
    to fine-grain locking. 

    Output from the harness is somewhat noisy:

    ./TL2-Ref4/rb-GV6-8W D10 d30 u30 s1000 r2000 t16
    TX system ready: SCHEDCTL=0; SUNW,Sun-Fire-V890; graham; +INLINED TL2-Ref4-GV6 STRIPEWIDTH=64b
    HWCLOCK: (MAXCLOCKSKEW=2) 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3 3 3 3 3 3 3 
    RedBlack (Alloc=1)
    Launching...WatchDog running
    Initializing ...Initialized 773 unique of 1000
    Launched...Starting...shutdown...results:
    Post validation : [pass] Content Integrity check: BA8AC+437DD=FE089
    TEST: TL2-Ref4-+GV6+8W (libumem.so) 16T 10000 msecs ins=%0 del=%30 ups=%30 isize=1000 (initpop=773) range=2000
    RESULTS: Dur=10000 pop=1036 U=14151104 I=0 D=14159312 L=18867066 (Misses=9433844) SPREAD=1.02062 TOTAL=47177482
    RESULTS: TxLDs=0 TxSTs=0
    SUMMARY: TL2-Ref4/rb-GV6-8W  TL2-Ref4-+GV6+8W (libumem.so) 16T I0 D30 U30 L40 (1000,2000) ABO=6378109 pop=1036 -> 47177482 Ops
    Structural integrity check:  Nodes=1036 Depth=8
    VERIFY=8 
    Shutdown: Overflows=0  GCLOCK=45CC02
    User=161546 System=33 msecs

    The key performance metrics are 47177482 ops in the 10 sec interval with 6378109 aborts.
    As musicologists say, the rest is just noise. 
    

Regards
Dave and Nir


