## Copyright (C) Sun Microsystems, Inc. 2006.  All Rights Reserved. 
## We'd like to write CASN,STN,LDN, but the synthetic forms are not
## available in the .il processing phase.  So we're stuck with either using
## cas-32.il and cas-64.il -- with CAS and CASX, respectively -- or providing 
## a single cas.il with both cas32() and cas64() and then using source level
## wrappers to bind cas() to cas32 or cas64.  

    .inline		sysCAS,3
    .volatile
    casx [%o2],%o1,%o0
    .nonvolatile
    .end

    ## CASX with post-fetch
    ## See also: http://blogs.sun.com/dave/entry/cas_and_cache_trivia_invalidate
    ## standard prefetch is not effective in this case. 
    .inline		sysCASpf,3
    .volatile
    casx [%o2],%o1,%o0
    lduw [%o2], %g0
    .nonvolatile
    .end

    ## See also: rock.il _DCAS()

    .inline Non1CAS,3
    .volatile
    mov %o0,%o3
    ldx [%o2],%o0
    xor %o0,%o1,%o1
    brz,a,pt %o1,1f
    stx  %o3,[%o2]
  1: nop
    .nonvolatile
    .end

    .inline Non2CAS,3
    .volatile
    mov %o0,%o3
    ldx [%o2],%o0
    xor %o0,%o1,%o1
    movrnz %o1,%o0,%o3
    stx %o3,[%o2]
    .nonvolatile
    .end

    .inline Norm1,1
    .volatile
    movrnz %o0,1,%o0
    .nonvolatile
    .end

    .inline NormN1,1
    .volatile
    movrnz %o0,-1,%o0
    .nonvolatile
    .end

    .inline NormV,1
    .volatile
    movrnz %o0,%o1,%o0
    .nonvolatile
    .end

    .inline		sysCAS32,3
    .volatile
    cas [%o2],%o1,%o0
    .nonvolatile
    .end

    ## CAS with post-fetch
    ## See also: http://blogs.sun.com/dave/entry/cas_and_cache_trivia_invalidate
    ## standard prefetch is not effective in this case. 
    .inline		sysCAS32pf,3
    .volatile
    cas [%o2],%o1,%o0
    lduw [%o2], %g0
    .nonvolatile
    .end

   .inline  membarstoreload,0
   .volatile
   membar #StoreLoad
   .nonvolatile
   .end

   .inline membarsync,0
   .volatile
   membar #Sync
   .nonvolatile
   .end

   .inline membarldld,0
   .volatile
   membar #LoadLoad
   .nonvolatile
   .end

   .inline rdtick,0
   .volatile
   rd %tick, %o0
   .nonvolatile
   .end

   .inline rdstick,0
   .volatile
    rd %asr24, %o0
   .nonvolatile
   .end

    .inline Pause,0
    .volatile
    rd %ccr, %g0
    .nonvolatile
    .end


   .inline SequencePoint,0
   .volatile
   .nonvolatile
   .end

   .inline LDNF,1
   .volatile
   ldxa [%o0]0x82, %o0
   .nonvolatile
   .end

   .inline _LDNF,1
   .volatile
   ldxa [%o0]0x82, %o0
   .nonvolatile
   .end

   .inline STNF,2
   .volatile
   stxa %o1, [%o0]0x82
   .nonvolatile
   .end

   .inline sysCASNF,3
   .volatile
   casxa [%o2]0x82,%o1,%o0
   .nonvolatile
   .end

   .inline PrefetchW,1
    .volatile
    prefetch [%o0],2
    .nonvolatile
    .end

    .inline _PrefetchR, 1
    .volatile
    prefetch [%o0], 0
    .nonvolatile 
    .end

   .inline FetchL1,1
   .volatile
   ldub [%o0],%g0
   .nonvolatile
   .end

    .inline _rdpc,1
    .volatile
    rd %pc, %o0
    .nonvolatile
    .end

    .inline __Align1K,0
    .volatile
    ba  1f
    nop
    .align  1024
   1:nop
    .nonvolatile
    .end






