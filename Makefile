CC		= g++
FLAGS	= -O2 -D_FILE_OFFSET_BITS=64
TARGET = run
OBJS = client/*.o client/tool/*.o client/db/build_unix/*.o

.PHONY : all clean tar

all : $(TARGET)
	$(MAKE) --directory=client
	$(CC) $(FLAGS) -o run main.cpp $(OBJS) -lpthread 

clean :
	$(MAKE) --directory=client clean
	rm -f run *.tar.gz data/*

tar :
	tar zcvf submission.tar.gz client


