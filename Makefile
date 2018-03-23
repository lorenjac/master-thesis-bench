# Compiler Flags

CXX=g++
CXXFLAGS=-std=c++1z -Wall -O2

# Dependencies

# MIDAS_HOME=lib/midas
# ECHO_HOME=lib/echo/echo
#
# MIDAS_INCLUDE=-I $(MIDAS_HOME)/include -I lib/libcuckoo -I lib/pmdk/src/include
# MIDAS_LIB_DIR=-L $(MIDAS_HOME)/bin -L lib/pmdk/src/nondebug/
# MIDAS_STATIC_LIBS=-lmidas -lpmemobj -lpmem
# MIDAS_DYNAMIC_LIBS=-ldl -lstdc++fs -pthread
# MIDAS_LDFLAGS=$(MIDAS_LIB_DIR) -Wl,-Bstatic $(MIDAS_STATIC_LIBS) -Wl,-Bdynamic $(MIDAS_DYNAMIC_LIBS)
#
# ECHO_INCLUDE=-I $(ECHO_HOME)/include -I $(ECHO_HOME)/src
# ECHO_LIB_DIR=-L $(ECHO_HOME)/lib
# ECHO_STATIC_LIBS=-lkp_kvstore
# ECHO_DYNAMIC_LIBS=-pthread
# ECHO_LDFLAGS=$(ECHO_LIB_DIR) -Wl,-Bstatic $(ECHO_STATIC_LIBS) -Wl,-Bdynamic $(ECHO_DYNAMIC_LIBS)

INCLUDE=-I include
SRC=src

# Artifacts

BIN=bin

# Targets

workload-gen : opcode tx-profile workload jsoncpp
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/tools/workload-gen.cpp -o $(BIN)/workload-gen.o
	$(CXX) $(CXXFLAGS) $(BIN)/workload-gen.o $(BIN)/tx-profile.o $(BIN)/workload.o $(BIN)/opcode.o $(BIN)/jsoncpp.o -o $(BIN)/$@

kv-gen :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/tools/kv-gen.cpp -o $(BIN)/kv-gen.o
	$(CXX) $(CXXFLAGS) $(BIN)/kv-gen.o -o $(BIN)/$@

opcode :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/utils/$@.cpp -o $(BIN)/$@.o

workload :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/utils/$@.cpp -o $(BIN)/$@.o

tx-profile :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/utils/$@.cpp -o $(BIN)/$@.o

jsoncpp :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/utils/$@.cpp -o $(BIN)/$@.o

clean :
	rm -rf $(BIN)/*
