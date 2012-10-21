CC		= g++
FLAGS	= -O2 -c -D_FILE_OFFSET_BITS=64
OBJS	= $(patsubst %.cpp, %.o, $(wildcard *.cpp))

all : $(OBJS)

%.o : %.cpp
	$(CC) $(FLAGS) -o $@ $<

.PHONY : clean

clean :
	rm -f *.o



