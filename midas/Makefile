# Compiler Flags

CC=g++
CFLAGS=-std=c++1z -Wall -O2

# Inputs

MIDAS_HOME=lib/midas
INCLUDE=-I $(MIDAS_HOME)/include -I lib/libcuckoo -I lib/pmdk/src/include
SRC_DIR=src

LIB_DIR=-L $(MIDAS_HOME)/bin -Llib/pmdk/src/nondebug/
STATIC_LIBS=-lmidas -lpmemobj -lpmem
DYNAMIC_LIBS=-ldl -lstdc++fs -pthread
LDFLAGS=$(LIB_DIR) -Wl,-Bstatic $(STATIC_LIBS) -Wl,-Bdynamic $(DYNAMIC_LIBS)

# Artifacts

BIN_DIR=bin

# Targets

all : baseline

baseline :
	$(CC) -c $(CFLAGS) $(INCLUDE) $(SRC_DIR)/$@.cpp -o $(BIN_DIR)/$@.o
	$(CC) $(CFLAGS) $(BIN_DIR)/$@.o $(LDFLAGS) -o $(BIN_DIR)/$@

clean :
	rm -rf bin/*
