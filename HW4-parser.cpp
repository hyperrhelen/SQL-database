#include <stdio.h>
#include <iostream>
#include <string.h>
#include <string>
using namespace std;

/**********************************************************************/
/* Stuff to make the yacc and lex stuff compile in C++ without errors */
/**********************************************************************/
#define MAXNODE 20000
int freen = 0;
#include "HW4-expr.h"
expr cvt_itoe(int);
int yylex();
int yyerror(char const *);
int makestrexpr(char *), makename(int, char*), setname(int, int), makenum(int);
expr compile(expr), evalexpr(expr), optimize(expr);
void print_relation(expr);

/**********************************************************************/
/* Parser *************************************************************/
/**********************************************************************/
int linenum;

#include "HW4-sql.tab.cpp"
#include "lex.yy.cc"
yyFlexLexer *t = new yyFlexLexer;

int main()
{
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

/**********************************************************************/
/* Expression generation code *****************************************/
/**********************************************************************/
typedef
struct expression {
	int func;
	int count;
	union values {
		char *name;
		char *data;
		int num;
		struct expression *ep;
	} values[2];
} expression;
 
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
	int i, size;

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

void
print_e(expr e, int lev)
{
	expression *ep = (expression *)e;
	register int i, slev=lev;

	if(!ep) { printf("() "); return; }
	switch(ep->func) {

	/* Literals */
	case OP_NUMBER:	printf("%d ", ep->values[0].num); return;
	case OP_STRING:	printf("%s ", ep->values[0].data); return;
	case OP_NULL:	printf("NULL "); return;

	/* Names */
	case OP_COLNAME:
			printf("COLUMN:%s ", ep->values[0].name); return;
	case OP_TABLENAME:
			printf("TABLE:%s ", ep->values[0].name); return;
	case OP_FNAME:
			printf("FUNC:%s ", ep->values[0].name); return;
	case OP_COLUMNDEF:
			printf("(COLSPEC ");
			printf("%s ", ep->values[1].num==1?"KEY":ep->values[1].num==3?"PRIMARY":"");
			print_e(ep->values[0].ep, lev+2);
			putchar(')');
			return;


	/* Relational operators */
	case OP_PROJECTION:
			printf("(PROJECT \n"); break;
	case OP_SELECTION:
			printf("(SELECT \n"); break;
	case OP_PRODUCT:
			printf("(PRODUCT \n"); break;
	case OP_SORT:
			printf("(SORT \n"); break;
	case OP_GROUP:
			printf("(GROUP \n"); break;
	case OP_DELTA:
			printf("(DELTA \n"); break;
	case OP_CREATETABLE:
			printf("(CREATETABLE \n"); break;
	case OP_INSERTROW:
			printf("(INSERTROW \n"); break;
	
	case OP_PLUS:	printf("(+ \n"); break;
	case OP_BMINUS:	printf("(- \n"); break;
	case OP_TIMES:	printf("(* \n"); break;
	case OP_DIVIDE:	printf("(/ \n"); break;

	case OP_AND:	printf("(AND \n"); break;
	case OP_OR:	printf("(OR \n"); break;
	case OP_NOT:	printf("(! \n"); break;
	case OP_GT:	printf("(> \n"); break;
	case OP_LT:	printf("(< \n"); break;
	case OP_EQUAL:	printf("(== \n"); break;
	case OP_NOTEQ:	printf("(<> \n"); break;
	case OP_GEQ:	printf("(>= \n"); break;
	case OP_LEQ:	printf("(<= \n"); break;

	case OP_SORTSPEC:
			printf("(SORTSPEC \n"); break;

	case OP_OUTCOLNAME:
			printf("(AS \n"); break;

	case OP_RLIST:	printf("(RLIST \n"); break;
	default:	printf("(%d \n", ep->func); break;
	}
	lev += 2;
	for(i=0; i<lev; i++) putchar(' ');
	print_e(ep->values[0].ep, lev+2); putchar(' ');
	print_e(ep->values[1].ep, lev+2); 
	putchar('\n');
	for(i=0; i<slev; i++) putchar(' ');
	putchar(')');
}


/**********************************************************************/
/* Dummy routines that need to be filled out for HW4 ******************/
/* Move to another module and fully define there **********************/
/**********************************************************************/
expr compile(expr e)
{
	return e;
}

expr evalexpr(expr e)
{
	print_e(e, 0);
	return e;
}

expr optimize(expr e)
{
	return e;
}

void print_relation(expr e)
{
}
