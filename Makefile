CPP = g++
CPPFLAGS = -Wall -g -ansi -c

all: 
		bison HW4-sql.ypp
		flex --c++ HW4-sql.x
		g++ professorParser.cpp -o professorParser -std=c++11

clean:
		rm -rf *o professorParser

