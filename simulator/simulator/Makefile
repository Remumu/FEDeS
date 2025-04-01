# 
CC = g++
# CFLAGS = -Wall -g3
CFLAGS = -Wall -g -std=c++17
#
main : main.o duration.o cluster.o util.o
	g++ -o main main.o duration.o util.o 

main.o : main.cpp util.h config.h action.h job.h cluster.h
	g++ -c $(CFLAGS) main.cpp

duration.o : duration.hpp duration.cpp util.h
	g++ -c $(CFLAGS) duration.cpp

#cluster.o : cluster.h util.h location.h switch.h node.h
cluster.o : cluster.h util.h location.h
	g++ -c $(CFLAGS) cluster.h

util.o : util.h util.cpp
	g++ -c $(CFLAGS) util.cpp

.PHONY: clean

clean :
	rm *.o *.gch main
