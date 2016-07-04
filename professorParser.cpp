#include <stdio.h>
#include <iostream>
// #include <string.h>
#include <string>
using namespace std;

/**********************************************************************/
/* Stuff to make the yacc and lex stuff compile in C++ without errors */
/**********************************************************************/
#define MAXNODE 20000
int freen = 0;
#include "HW4-expr.h"
#include "hw3.cpp"

expr cvt_itoe(int);
int yylex();
int yyerror(char const *);
int makestrexpr(char *), makename(int, char*), setname(int, int), makenum(int);
// expr compile(expr), evalexpr(expr), optimize(expr);
// void print_relation(expr);


/**********************************************************************/
/* Parser *************************************************************/
/**********************************************************************/
int linenum;

#include "HW4-sql.tab.cpp"
#include "lex.yy.cc"

yyFlexLexer *t = new yyFlexLexer;

int main(int argc, char* argv[])
{
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i],"-d") == 0) {
            debug = true;
        }
    }

    LoadDatabase(".tbl");
    while (true) {
        if (!yyparse())  {
            break;
        }
	cout << "Syntax error, resyncing...\n";
    }
    cout << "QUITting...\n";
    return 0;
}

int
yylex()
{
	return t->yylex();
}


/* yyparse wants integers and CSIF machines have larger pointers than ints */
/* So need conversion routines between ints and node pointers **************/
expression ebuf[MAXNODE];

expr
cvt_itoe(int i)
{
	expr e;

	//printf("::cvt_itoe called with %d\n", i);

	if(!i) return 0;
	if(i<MAXNODE) {
		printf("Messed up index - too low\n");
		return 0;
	}
	if(i > MAXNODE+MAXNODE) {
		printf("Messed up index - too high\n");
		return 0;
	}
	e = (expr)(ebuf + (i-MAXNODE));
	return e;
}

/* Utility to convert a list into an array **********************************/
expr *
makearray(expr e)
{
	expression **epp, *ep = (expression *)e;
	int i, size = listlen(e);

	if(size==0) return 0;
	if(size < 0) {
		printf("::Bad list structure\n");
		exit(0);
	}
	epp = (expression **)malloc(size * sizeof (struct expression *));

	for(i=size-1; i>=0; i--) {
		if(!ep || ep->func != OP_RLIST) {
			printf("::Not a list element\n");
			return 0;
		}
		epp[i] = ep->values[0].ep;
		ep = ep->values[1].ep;
	}

	return (expr *)epp;
}

/* yyparse wants an int (YYSTYPE) and supplies ints, so this has to be ******/
int
makeexpr(int op, int cnt, int arg1, int arg2)
{
	expression *ep;

	//printf(":make_expr called with %d %d %d %d\n", op, cnt, arg1, arg2);

	/* yyparse wants integers not pointers, and on CSIF machines they are incompatible */
	/* So allocate from an array, and return a modified index */
	if(freen<MAXNODE) {
		ep = ebuf + (freen++);
	} else {
		printf("Out of expression nodes\n");
		return 0;
	}

	ep->func = op;
	ep->count = cnt;
	ep->values[0].ep = (expression *)cvt_itoe(arg1);
	switch(ep->func) {
	default:	ep->values[1].ep = (expression *)cvt_itoe(arg2);
			break;
	case OP_COLUMNDEF:
			ep->values[1].num = arg2;
			break;
	}

	//printf("::returning %d\n", (ep-ebuf)+MAXNODE);
	return (ep-ebuf)+MAXNODE;
}

int
makenum(int v)
{
	int i = makeexpr(OP_NUMBER,1,0,0);
	ebuf[i-MAXNODE].count = 1;
	ebuf[i-MAXNODE].values[0].num = v;
	return i;
}

int
makestrexpr(char *str)
{
	int i = makeexpr(OP_STRING,1,0,0);
	ebuf[i-MAXNODE].count = 1;
	ebuf[i-MAXNODE].values[0].data = str;
	return i;
}

int
makename(int op, char*str)
{
	int i = makeexpr(op,1,0,0);

	//printf("makename called with %d %s\n", op, str);
	ebuf[i-MAXNODE].count = 1;
	ebuf[i-MAXNODE].values[0].name = str;
	//printf("::makename: returning %d\n", i);
	return i;
}

int
setname(int op, int ei)
{
	expression *ep;
	//printf("::setname called with %d %d\n", op, ei);
	ep = (expression *)cvt_itoe(ei);
	if(!ep) return 0;
	//printf("::Setname: name=%s\n", ep->values[0].name);
	ep->func=op;
	return ei;
}

int
listlen(expr e)
{
	expression *ep = (expression *)e;
	int i;

	for(i=0; ep; i++) {
		if(ep->func != OP_RLIST) return -1;		/* Not a list element */
		ep = ep->values[1].ep;
	}
	return i;
}