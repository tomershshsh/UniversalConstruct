CFLAGS  := -DNDEBUG -DSPARC=1 -DINLINED=1 -mt -xarch=v9 -xO5 ../cas64.il -I. -I..
COMMON  := Makefile ../cas64.il ../if.h
RBCFILES := ../RB-Harness.c ../RB-Tree.c 
RACFILES := ../RA-Harness.c
CC      := cc
LDFLAGS := -lpthread

all: rb-8W

rb: $(RBCFILES) $(COMMON) ../RB-Tree.h TL.c TL.h
	$(CC) $(CFLAGS) $(RBCFILES) -o rb

rb-GV6: $(RBCFILES) $(COMMON) ../RB-Tree.h TL.c TL.h
	$(CC) $(CFLAGS) $(RBCFILES) -D_GVCONFIGURATION=6 -o rb-GV6

rb-8W: $(RBCFILES) $(COMMON) ../RB-Tree.h TL.c TL.h
	$(CC) $(CFLAGS) $(RBCFILES) -DSTRIPESHIFT=6 -o rb-8W

ra: $(RACFILES) $(COMMON) TL.c TL.h
	$(CC) $(CFLAGS) $(RACFILES) -o ra

ra-GV6: $(RACFILES) $(COMMON) TL.c TL.h
	$(CC) $(CFLAGS) $(RACFILES) -D_GVCONFIGURATION=6 -o ra-GV6

ra-8W: $(RACFILES) $(COMMON) TL.c TL.h
	$(CC) $(CFLAGS) $(RACFILES) -DSTRIPESHIFT=6 -o ra-8W
