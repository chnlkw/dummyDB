CC		= g++
FLAGS	= -O2 -D_FILE_OFFSET_BITS=64 -g -std=c++11

.PHONY : all clean tar run

all :
	$(MAKE) --directory=client
	$(CC) $(FLAGS) -o run main.cpp hash.cpp client/*.o workload.cpp -lpthread 
	#client/db/build_unix/*.o

clean :
	$(MAKE) --directory=client clean
	rm -f run *.tar.gz data/*

tar :
	tar zcvf submission.tar.gz client

run :
	./run nnn

