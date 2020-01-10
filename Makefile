SRC = ./src/
BIN = ./bin/

CC = g++
CCFLAGS = -std=c++17 -Wall
MPICC = mpic++

INCLUDE = -I/home/mandres/install/include -I./inc
LIBS = -L/home/mandres/install/lib
LDLIBS = -lzmq -lpthread


BINARIES = buffer client stat_srv
BIN_FILES = $(addprefix $(BIN), $(BINARIES))
BUFFER_OBJECTS = buffer.o buff_workers.o memalloc.o
CLIENT_OBJECTS = client.o imss.o policies.o crc.o
METADATA_SRV_OBJECTS = stat.o stat_server.o stat_workers.o memalloc.o
BUFFER_LIST = $(addprefix $(BIN), $(BUFFER_OBJECTS))
CLIENT_LIST = $(addprefix $(BIN), $(CLIENT_OBJECTS))
METADATA_SRV_LIST = $(addprefix $(BIN), $(METADATA_SRV_OBJECTS))


all: $(BIN_FILES)
.PHONY : all
	
$(BIN)buffer: $(BUFFER_LIST)
	$(MPICC) $(CCFLAGS) $(INCLUDE) $(LIBS) $^ $(LDLIBS) -o $@

$(BIN)client: $(CLIENT_LIST)
	$(MPICC) $(CCFLAGS) $(INCLUDE) $(LIBS) $^ $(LDLIBS) -o $@

$(BIN)stat_srv: $(METADATA_SRV_LIST)
	$(MPICC) $(CCFLAGS) $(INCLUDE) $(LIBS) $^ $(LDLIBS) -o $@

$(BIN)%.o: $(SRC)%.c
	$(MPICC) $(CCFLAGS) $(INCLUDE) $(LIBS) $(LDLIBS) -c $< -o $@

clean:
	@rm -rf bin/*
.PHONY : clean
