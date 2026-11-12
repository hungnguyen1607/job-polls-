CXX ?= g++
CXXFLAGS ?= -O3 -std=c++17 -fPIC -Wall -Wextra
LDFLAGS ?= -shared
INCLUDE := -Iinclude
SRC := src/native.cpp
LIB := build/libjobpolls_native.so

.PHONY: all clean test bench

all: $(LIB)

$(LIB): $(SRC) include/jobpolls.h
	@mkdir -p build
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(INCLUDE) $(SRC) -o $(LIB)

test: $(LIB)
	@mkdir -p build
	$(CXX) -O2 -std=c++17 $(INCLUDE) tests/test_native.cpp -Lbuild -ljobpolls_native \
		-Wl,-rpath,'$$ORIGIN' -o build/test_native
	./build/test_native

bench: $(LIB)
	@mkdir -p build
	$(CXX) -O2 -std=c++17 $(INCLUDE) tests/bench_native.cpp -Lbuild -ljobpolls_native \
		-Wl,-rpath,'$$ORIGIN' -o build/bench_native
	./build/bench_native

clean:
	rm -rf build
