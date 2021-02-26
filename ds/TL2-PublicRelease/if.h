// TL2 can be used in either an "embedded" form that allows inlining of 
// performance critical transactional operations such as TxLoad and TxSTore,
// or in a library-based mode where TxLoad and TxSore are exposed as 
// normal entry points.

#if defined(INLINED)
#define API static inline
#else
#define API
#endif

// TODO-FIXME: move the interfaces definitions into this file, allowing us
// to separate the TL implementation private aspects and the interface
// definitions.  Ideally, we'd have 
// A. an interface file, if.h for clients of TL
// B. a common file for all TL implementations: ICommon.h
//    The contents are specific to TL internals and are
//    implementation-private, but are shared by all the
//    various TL implementations.  
// C. an implementation-specific file: ISpecific.h 




