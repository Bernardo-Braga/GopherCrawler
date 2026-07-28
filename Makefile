# Makefile for GopherCrawler
#
# Single-file C project. Pure POSIX sockets (socket/connect/select/getaddrinfo)
# from libc — no external libraries and no pthread linking required.
# The source defines _POSIX_C_SOURCE 200112L itself, so no extra defines here.

CC      ?= cc
CFLAGS  ?= -std=c11 -O2 -Wall -Wextra
LDFLAGS ?=
LDLIBS  ?=

TARGET  := gopher_client
SRC     := gopher_client.c

# Default host:port used by `make run`; override on the command line:
#   make run HOST=gopher.floodgap.com:70
HOST    ?= comp3310.ddns.net:70

.PHONY: all run clean debug

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(SRC) $(LDLIBS)

# Build with debug symbols and no optimization.
debug: CFLAGS := -std=c11 -g -O0 -Wall -Wextra
debug: clean $(TARGET)

# Build (if needed) and crawl HOST.
run: $(TARGET)
	./$(TARGET) $(HOST)

clean:
	rm -f $(TARGET)
