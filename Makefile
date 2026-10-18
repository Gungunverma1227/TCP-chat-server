CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra

all: server client

server: server.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

client: client.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f server client

.PHONY: all clean
