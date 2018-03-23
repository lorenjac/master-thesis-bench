CXX=g++-7
CXXFLAGS=-std=c++1z -Wall -O2
INCLUDE=-I include
SRC=src
BIN=bin

workload-gen : opcode tx-profile workload jsoncpp
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/workload-gen.cpp -o $(BIN)/workload-gen.o
	$(CXX) $(CXXFLAGS) $(BIN)/workload-gen.o $(BIN)/tx-profile.o $(BIN)/workload.o $(BIN)/opcode.o $(BIN)/jsoncpp.o -o $(BIN)/$@

tx-profile-test : opcode tx-profile jsoncpp
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/$@.cpp -o $(BIN)/$@.o
	$(CXX) $(CXXFLAGS) $(BIN)/$@.o $(BIN)/tx-profile.o $(BIN)/opcode.o $(BIN)/jsoncpp.o -o $(BIN)/$@

workload-test : opcode workload jsoncpp
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/$@.cpp -o $(BIN)/$@.o
	$(CXX) $(CXXFLAGS) $(BIN)/$@.o $(BIN)/workload.o $(BIN)/opcode.o $(BIN)/jsoncpp.o -o $(BIN)/$@

opcode :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/$@.cpp -o $(BIN)/$@.o

workload :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/$@.cpp -o $(BIN)/$@.o

tx-profile :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/$@.cpp -o $(BIN)/$@.o

jsoncpp :
	$(CXX) -c $(CXXFLAGS) $(INCLUDE) $(SRC)/$@.cpp -o $(BIN)/$@.o

clean :
	rm -rf $(BIN)/*
