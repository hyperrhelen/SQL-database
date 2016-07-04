// Written by: Helen Chac & Dehowe Feng

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <numeric>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <math.h>

// Get the directory structory.
#include <dirent.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "HW4-expr.h"

// Btree header files.
#include "btree/btree.h"
#include "btree/btree_set.h"
#include "btree/btree_map.h"

using namespace std;
const string dir_name = "data/";
const string file_extension = ".tbl";
const string index_extension = ".idx";
bool debug = false;

vector<string> ParseComma(string line);

struct WordIndex{
    string word;
    int index;
    WordIndex(string w, int n) {
        word = w;
        index = n;
    }
};

int global_index_for_sort = 0;

/**********************************************************************/
/* Expression generation code *****************************************/
/**********************************************************************/
typedef struct expression {
    int func;
    int count;
    union values {
        char *name;
        char *data;
        int num;
        struct expression *ep;
    } values[2];
} expression;

enum Type {none, create_op, insert_op, select_op };
struct Evaluator{
    Type type;
    string table_name;
    vector<string> values;
    // string table_names;
    vector<string> keys;
    vector<pair<expression*, string>> projections;
    expression *filter;
    bool ColumnsExist = true;
    bool ProjectionsExist = true;
};

class Table {
private:
    string name;
    vector<string> headers;
    // int count = 0;
    unordered_map<string, btree::btree_map<string, int>> key_strings;
    // unordered_map<string, btree::btree_map<int, int>> key_ints;
public:
    Table(vector<string> tHeaders, string n) {
        headers = tHeaders;
        name = n;
    }
    Table() {}
    int CountColumns() { return headers.size(); }
    string GetName() { return name; }

    vector<string> GetKeys() {
        vector<string> keys;
        for (auto key : key_strings) {
            keys.push_back(key.first);
        }
        return keys;
    }

    bool UpdateBtreeString(string col_name, string str) {
        auto lookup = key_strings.find(col_name);
        if (lookup != key_strings.end()) {
            if (lookup->second.find(str) != lookup->second.end()) {
                cout << "DEBUG: Key already exists." << endl;
                return false;
            }
            lookup->second.insert(make_pair(str, GetRows().size()));
        }
        ofstream file;
        string file_name = dir_name+name+'.'+col_name+index_extension;
        file.open(file_name, std::fstream::trunc);

        for (auto b_map : lookup->second) {
            file << b_map.first << "\t" << b_map.second << endl;
        }
        file.close();
        if (debug) {
            cout << "DEBUG: " << name << "." << col_name << ".\n";
        }
        return true;
    }

    void LoadBtreeIndex(string col_name, string file_name) {
        string f = dir_name + file_name;
        fstream file(f.c_str());
        btree::btree_map<string, int> tree;
        string line;
        while(getline(file, line)) {
            vector<string> list = ParseComma(line);
            if ((int)list.size() == 2) {
                tree.insert(make_pair(list[0], atoi(list[1].c_str())));    
            }
        }
        key_strings.emplace(col_name, tree);
    }

    bool DoesKeyExists(string col_name) {
        auto found = key_strings.find(col_name);
        if (found == key_strings.end()) {
            return false;
        } 
        return true;
    }

    btree::btree_map<string, int> GetTree(string col_name) {
        auto found = key_strings.find(col_name);
        return found->second;
    }


    void AddBtreeString(string col_name) {
        btree::btree_map<string, int> tree;
        if (key_strings.find(col_name) == key_strings.end()) {
            key_strings.emplace(col_name, tree);
            if (debug) {
                cout << "DEBUG: " << name << "." << col_name << "has a B-Tree index.\n";                
            }
        }
    }

    // Returns a list of rows.
    vector<vector<string>> GetRows() {
        vector<vector<string>> listRows;
        string file_name = dir_name + name + file_extension;
        fstream file(file_name.c_str());
        bool header = false;
        string line;
        while(getline(file, line)) {
            vector<string> list = ParseComma(line);
            if(!header) {
                header = true;
            } else {
                listRows.push_back(list);
            }
        }
        file.close();
        return listRows;
    }

    vector<string> GetHeaders() { return headers; }
};

bool CheckTableNameEntityName(string &table_name, vector<string> &column_names,
    unordered_map<string, Table> db);
int define_table(string& table_name, int num_columns,
    vector<string>& column_names, vector<string>& keys, unordered_map<string, Table> &db);
bool CheckEntities(string entityName);
int insert_row(string &table_name, int num_values,
     vector<string> &values, unordered_map<string, Table> &db);
bool CheckTableExists(vector<string> table_names,
    unordered_map<string, Table> &db);
bool CheckAllEntityExists(vector<string> entities,
    unordered_map<string, Table> &db);
void combination(vector<string> table_name, unsigned int index,
    vector<string> sofar, vector<vector<string>> &output,
    unordered_map<string, Table> &db);
int fetching_rows(vector<string> &table_names, vector<pair<expression*,string>> &projection,
        expression* condition, unordered_map<string, Table> &db);
void print_err();
bool checkValidWord(string s);
WordIndex findNextWord( string s, int index);
bool CheckSelectProjection(string s);
WordIndex findNextWordSpace(string s, int index);
bool CheckSelectCondition(string s);
WordIndex findSelectCondition( string &s, int index);
int findSymbol( string &s,  char symbol, int index) ;
void makeLowerCase(string* s);
void parse_command( string command, unordered_map<string, Table> &db);
void CheckDB(unordered_map<string, Table> &database);
bool CheckSuffix(const string db_ext, string file_name);

string GetFileName(string file_name);
pair<string, Table> LoadSchema(string dir, string file_name);
void LoadDatabase(const string db_ext);

string ConvertToString(char* str);
expr compile(expr e);
void Clear();
expr evalexpr(expr e);
expr optimize(expr e);
void print_relation(expr e);

// New functions:
int create_or_insert(expr e, bool create);
int projection(expr e, vector<vector<string>>& output, vector<string> &headers);
int selection(expression* e, vector<vector<string>>& output, vector<string> &headers);
int product(expression *e, vector<vector<string>> &output, vector<string> &headers);
int GroupBy(expression *ep, unordered_map<string, vector<vector<string>>> &output, 
    vector<string>& table_headers);
int FuncCall(expression *ep, vector<vector<string>>& input, vector<string> &headers,
    int &output);
int SortProjection(expression *ep, vector<vector<string>> &table_rows,
    vector<string> &table_headers);
vector<vector<string>> CartesianProduct(vector<vector<string>> &A,
    vector<vector<string>> &B);
void print_table(vector<string> &headers, vector<vector<string>> &rows);



void print_e(expr e, int lev);
void drop_table(expr e);
bool ParseFileName(string file_name, string *tbl_name, string *col_name);
// Database that stores in the values. Global variables.
unordered_map<string, Table> database;
Evaluator eval;


void print_table(vector<string> &headers, vector<vector<string>> &rows) {
    for (string & head: headers ) {
        cout << head << "\t";
    }
    cout << endl;
    cout << "----" << endl;
    for (vector<string> & row: rows) {
        for (string & entry : row ) {
            cout << entry << "\t";
        }
        cout << endl;
    }
    cout << rows.size() << " rows fetched.\n";
}

// Checks that the table Name does not conflict with the entity names.
// Returns true for valid entity names.
// Returns false if there are overlapping entity names
bool CheckTableNameEntityName(string &table_name, vector<string> &column_names,
    unordered_map<string, Table> db) {
    unordered_set<string> set;
    for (unsigned int i = 0; i < column_names.size(); ++i) {
        auto found = set.find(column_names[i]);
        if (table_name == column_names[i] ||
             db.find(column_names[i]) != db.end()) {
            return false;
        }
        if (found == set.end()) {
            set.insert(column_names[i]);
        } else {
            return false;
        }
    }
    return true;
}

// Creates table in the database.
// Returns 0 if successful.
// Returns -1 if unsuccessful.
int create_or_insert(expr e, bool create) {
    // Make a stack that goes through the list of tables
    // or something like that.
    // YOU KNOW that the left side is the table name
    // the right side is the list of values.
    // expression *ep = (expression *) e;
    expression* ep = (expression *) e;
    expression *ep_left = (expression* ) ep->values[0].ep;
    expression *ep_right = (expression*) ep->values[1].ep;
    string table_name;
    vector<string> col_names;
    vector<string> keys;
    if (ep_left->func == OP_TABLENAME) {
        table_name = ConvertToString(ep_left->values[0].name);
    } else {
        cout << "Invalid OP_CODE in the expression tree -- need table_name.\n";
        return -1;
    }
    // Get a list of column names.
    if (ep_right->func == OP_RLIST) {
        while(ep_right->func == OP_RLIST) {
            // Check the left side.
            // Recurse through the right.
            expression *left = ep_right->values[0].ep;
            if(left->func == OP_COLUMNDEF && left->values[0].ep->func == OP_COLNAME) {
                string col_name = ConvertToString(left->values[0].ep->values[0].name);
                col_names.push_back(col_name);
                if (ep_right->values[1].num == 1 || ep_right->values[1].num == 3) {
                    keys.push_back(col_name);
                    // Create an index file.
                }
            }
            else if (create == false && (left->func == OP_STRING || left->func == OP_NULL 
                    || left->func == OP_NUMBER)) {
                if (left->func == OP_NUMBER) {
                   col_names.push_back(to_string(left->values[0].num)); 
                } else if ( left->func == OP_STRING) {
                    col_names.push_back(ConvertToString(left->values[0].data));
                } else if (left->func == OP_NULL) {
                    col_names.push_back("NULL");
                }
            }
            else {
                cout << "Invalid OP_CODE in the expression tree -- create/insert.\n";
                return -1;
                // die.
            }
            if (ep_right->values[1].ep == NULL) {
                break;
            }
            ep_right = ep_right->values[1].ep;
        }
    }
    for (unsigned int i = 0; i < col_names.size(); ++i) {
        cout << col_names[i] << endl;
    }
    if (create == true) {
        if (define_table(table_name, col_names.size(), col_names, keys, database) != 0) {
            return -1;
        }
    } else {
        if (insert_row(table_name, col_names.size(), col_names, database) != 0) {
            return -1;
        }
    }
    
    return 0;
}

int define_table(string& table_name, int num_columns,
    vector<string>& column_names, vector<string> &keys, unordered_map<string, Table> &db) {
    // Check to see if the file exists. If the file exists, then
    // you want to return NULL. Print out that you can't create the
    // table.
    if (!checkValidWord(table_name)) {
        cout << "Not valid Table name" << endl;
        return -1;
    }
    string file_name = dir_name+table_name + file_extension;
    if (ifstream(file_name.c_str())) {
        cout << "Error: Table already exists.\n";
        return -1;
    }
    if (!CheckTableNameEntityName(table_name, column_names, db)) {
        cout << "Error: Invalid column name.\n";
        return -1;
    }

    ofstream outfile(file_name.c_str());
    if (outfile.fail()) {
        cout << "Error: Cannot create table. Not enough memory.\n";
        return -1;
    }
    outfile.close();
    ofstream file;
    file.open(file_name);

    string insert;
    for (int i = 0; i < num_columns; ++i) {
        insert += column_names[i];
        if (i == num_columns - 1) {
        } else {
            insert += "\t";
        }
    }
    // cout << ")\n;\n";
    file << insert << endl;
    if (!file.good()) {
        cout << "Error: Unknown server error.\n";
        file.close();
        return -1;
    }
    // db.insert(make_pair(table_name,Table(values, table_name)));
    Table table = Table(column_names, table_name);
    file.close();

    for (string key: keys) {
        string f_name = dir_name+table_name+'.'+key+index_extension;
        ofstream outfile(f_name.c_str());
        outfile.close();
        table.AddBtreeString(key);
    }
    db.emplace(table_name, table);
    return 0;
}

// Checks to see if the entity is a valid entity:
// This includes:
//          - 'string'
//          - [0-9]*
// It cannot be a column_name or the name of another table.
bool CheckEntities(string entityName) {
    // Check if the string is NULL
    bool period = false;
    if (entityName.length() ==0) {
        return true;
    }
    //cout << entityName << endl;
    // Check for null string.
    if ((entityName[0] == '\'' && entityName[entityName.length()-1] == '\'') ||
        entityName == "null") {
        return true;
    }
    for (unsigned int i = 0; i < entityName.length(); ++i) {
        if (!((entityName[i] >= '0' && entityName[i] <= '9')
            || ((entityName[i] == '.') && (period == false)))) {
            return false;
        }
        if (entityName[i] == '.') {
            period = true;
        }
    }
    return true;
}

// Inserts the rows into the database.
// Returns 0 when it's a successful update.
// Returns -1 when unsuccessful.
// This can be due to insufficient amount of columns.
// This can also because the .tbl does not exist.
int insert_row(string &table_name, int num_values,
     vector<string> &values, unordered_map<string, Table> &db) {
    // Check to see if the file exists.
    string file_name = dir_name+table_name + file_extension;
    auto found = db.find(table_name);

    // if the file does not exist of we can't find it in our map.
    if (!ifstream(file_name.c_str()) || found == db.end()) {
        cout << "Error: Table does not exist.\n";
        return -1;
    }

    // Check to make sure there are the correct amount of variables.
    if (num_values < (found->second).CountColumns()) {
        cout << "Error: Too few values.\n";
        return -1;
    }
    if (num_values > (found->second).CountColumns()) {
        cout << "Error: Too many values.\n";
        return -1;
    }

    // Checks to see if all the values that are being inserted are valid.
    for (int i = 0; i < num_values; ++i) {
        if(!CheckEntities(values[i])) {
            cout << "Error: Invalid Data\n";
            return -1;
        }
    }
    vector<string> headers = found->second.GetHeaders();

    // the variable found has the table.
    // check the corresponding headers to figure out which values to insert.
    vector<string> keys = found->second.GetKeys();
    for (string key: keys) {
        for (int i = 0; i < (int) headers.size(); ++i) {
            if (key == headers[i]) {
                // Then we want to insert this into the index file.
                if(!found->second.UpdateBtreeString(key, values[i])) {

                    return -1;
                }
                break;
            }
            if (i == (int) headers.size() - 1) {
                // It should have been able to find it.
                cout << "Error: Server error" << endl;
                return -1;
            }
        }
    }

    // Insert into our memory map.
    fstream file;
    // (found->second).InsertRow(values);
    string insert;
    file.open(file_name,std::ios_base::app);
    // cout << "INSERT INTO " << table_name << " VALUES\n(";
    for (int i = 0; i < num_values; ++i) {
        if (values[i] == "") {
            insert += "NULL";
        } else {
            insert += values[i];
        }
        if(i == num_values-1) {
            // cout << values[i] << "";
        } else {
            insert += "\t";
            // cout << values[i] << " , ";
        }
    }
    // cout << ")\n;\n";
    file << insert << endl;
    if (!file.good()) {
        cout << "Error: Unknown Server Error.\n";
        file.close();
        return -1;
    }
    file.close();

    return 0;
}

// Loops through the list of tables and checks if it exists
// in the current database. If exists, then return true.
bool CheckTableExists(vector<string> table_names,
    unordered_map<string, Table> &db) {
    for (string table : table_names) {
        auto found = db.find(table);
        if (found == db.end()) {
            return false;
        }
    }
    return true;
}

// Loops through the database and checks whether the entity exists
// by going through all the tables's entities.
bool CheckEntityExists(string ent_name, unordered_map<string, Table> &db) {
    int found = false;
    for (auto table: db) {
        for (string table_ent : table.second.GetHeaders()) {
            if (ent_name == table_ent || ent_name == "*") {
                found = true;
            }
        }
    }
    found == false? found=false : found=true;
    return found;
}

bool CheckAllEntityExists(vector<pair<string,string>> entities,
    unordered_map<string, Table> &db) {
    for (const pair<string,string> ent_name : entities) {
        if (!CheckEntityExists(ent_name.second, db)) {
            return false;
        }
    }
    return true;
}

vector<vector<string>> CartesianProduct(vector<vector<string>>& A, vector<vector<string>>& B) {

    vector<vector<string>> answer;

    for (unsigned int i = 0; i < A.size(); ++i) {
        for (unsigned int j = 0; j < B.size(); ++j) {
            vector<string> temp = A[i];
            temp.insert(temp.end(), B[j].begin(), B[j].end());    
            answer.push_back(temp);
        }
    }
    return answer;
}

void combination(vector<string> table_name, unsigned int index,
    vector<string> sofar, vector<vector<string>> &output,
    unordered_map<string, Table> &db) {
    // cout << "ERR" << endl;
    if (table_name.size() == 0) {
        output.push_back(sofar);
        return;
    }
    vector<vector<string>> rows = db.find(table_name[index])->second.GetRows();
    for (vector<string> row: rows) {
        vector<string> curr = sofar;
        for(string entity: row) {
            curr.push_back(entity);
        }
        if(index == table_name.size() - 1) {
            output.push_back(curr);
        } else {
            combination(table_name, index+1, curr, output, db);
        }
    }
}

void print_err() {
    cout << "Invalid command.\n";
}

bool checkValidWord(string s) {
    if (s == "select" || s == "from" || s == "where" || s == "insert" ||
        s == "into" || s == "create" || s == "table" || s == "as" ||
        s == "natural" || s == "join" || s == "delete" || s == "alter" ||
        s == "transaction"){
        return false;
    }
    for (unsigned int i = 0; i < s.length(); ++i) {
        if (i == 0) {
            if (!(s[i] >= 'a' && s[i] <= 'z') && !(s[i] >= 'A' || s[i] <= 'Z') && (s[i] != '\'')) {
                return false;
            }
        } else {
            if ((!(s[i] >= 'a' && s[i] <= 'z') && !(s[i] >= 'A' && s[i] <= 'Z') &&
                !(s[i] >='0' && s[i] <= '9')) && s[i] != '.' && s[i] != '\'' && s[i] != '.' && (s[i] != '_')) {
                return false;
            }
        }
    }
    return true;
}

WordIndex findNextWord( string s, int index) {
    string output;
    if ((unsigned int)index > s.length() || index < 0) {
        return WordIndex("", -1);
    }
    for (unsigned int i = index; i < s.length(); ++i) {
        if ((s[i] == '\n' ||s[i] == '\t'|| s[i] == ' '||s[i] == ',' || s[i] == '(' || s[i] == ')'|| s[i] == ';')
            && output.length() != 0 ) {
                return WordIndex(output, i);
        } else if (i == s.length() - 1 ) {
            output += s[i];
            return WordIndex(output, -1);
        } else if (s[i] == ',') {
            return WordIndex(output, i);
        }
        else if (s[i] != ' ') {
            output += s[i];
        }
    }
    return WordIndex("", -1);
}

bool CheckSelectProjection(string s) {
    bool first = false;
    for (unsigned int i = 0; i < s.length(); ++i) {
        if (first == false) {
            if (!(s[i] >= 'A' && s[i] <= 'Z') && !(s[i] >= 'a' && s[i]<='z')) {
                return false;
            }
            first = true;
        } else if (s[i] == ',') {
            // Need to validate the first character.
            first = false;
        } else {
            if (!(s[i] >= 'A' && s[i] <= 'Z') && !(s[i] >= 'a' && s[i] <= 'z') &&
                !(s[i] == '_' && !(s[i] >= '0' && s[i] <= '9'))) {
                return false;
            }
        }
    }
    if (first == false) {
        return false;
    } else {
        return true;
    }
}

WordIndex findNextWordSpace(string s, int index) {
    string output;
    if ((unsigned int)index > s.length() || index < 0) {
        return WordIndex("", -1);
    }
    for (unsigned int i = index; i < s.length(); ++i) {
        if (s[i] == ' ' && output.length() != 0) {
            if (CheckSelectProjection(output)) {
                return WordIndex(output, i);
            }
            break;
        } else if (s[i] != ' ') {
            output += s[i];
        }
    }
    return WordIndex("", -1);
}


int findSymbol( string &s,  char symbol, int index) {
    if ((unsigned int) index > s.length()) {
        return -1;
    }
    for (unsigned int i = index; i < s.length(); ++i) {
        if (s[i] == ' ') {
            continue;
        } else if (s[i] == symbol) {
            return i + 1;
        } else {
            return -1;
        }
    }
    return -1;
}


void CheckDB(unordered_map<string, Table> &database) {
    for (auto x : database) {
        cout << "Table: "<<x.first << endl;
        vector<vector<string>> rows = x.second.GetRows();
        for (auto row : rows) {
            for (auto col : row ) {
                cout << col << " ";
            }
            cout << endl;
        }
    }
}

bool CheckSuffix(const string db_ext, string file_name) {
    if (file_name.size() < db_ext.size()) {
        return false;
    }
    for (unsigned int i = 1; i < db_ext.size(); ++i) {
        if (file_name[file_name.size()-i] != db_ext[db_ext.size()-i]) {
            return false;
        }
    }
    return true;
}

vector<string> ParseComma(string line) {
    vector<string> words;
    int i = 0;
    while(i != -1) {
        WordIndex word = findNextWord(line, i);
        words.push_back(word.word);
        // cout << "ParseComma's word: " << word.word << endl;
        i = findSymbol(line,'\t', word.index);
        // cout << "I: " << i << endl;
    }
    return words;
}

string GetFileName(string file_name) {
    string file;
    for (int i = 0; i < (int)file_name.size(); ++i) {
        if (file_name[i] == '.') {
            break;
        }
        file += file_name[i];
    }
    return file;
}

pair<string, Table> LoadSchema(string dir, string file_name) {
    pair<string, Table> table;
    string line;
    table.first = GetFileName(file_name);
    fstream file((dir+"/"+file_name).c_str());
    while(getline(file,line)) {
        table.second = Table(ParseComma(line), table.first);
        break;
    }
    file.close();
    return table;
}


bool ParseFileName(string file_name, string *tbl_name, string *col_name) {
    int index = 0;
    for (index = 0; index < (int) file_name.length(); ++index) {
        if (file_name[index] == '.') {
            index++;
            break;
        }
        *tbl_name += file_name[index];
    }
    if (index >= (int)file_name.length()) {
        return false;
    }
    for (index = index; index < (int) file_name.length(); ++index) {
        if (file_name[index] == '.') {
            break;
        }
        *col_name += file_name[index];
    }

    if (!(*tbl_name == "" && *col_name == "")) {
        return true;
    } else {
        return false;
    }
}


void LoadDatabase(const string db_ext) {
    DIR *dir = opendir("data");
    if (!dir) {
        int status = mkdir("data", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        if (status != 0) {
            cout << "Could not create directory.\n";
        }
    } else {
        dirent *entry;
        while ((entry = readdir(dir))) {
            string file_name = (string)entry->d_name;
            if (CheckSuffix(db_ext, file_name)) {
                database.insert(LoadSchema("data", file_name));
                if (debug == true) {
                   cout << "DEBUG: Loading in " << file_name << " into the database.\n";
                }
            }
        }
        dir = opendir("data");
        string row = "";
        while ((entry = readdir(dir))) {
            string file_name = (string)entry->d_name;
            if(CheckSuffix(file_extension, file_name)) {
                //Search for the table

                ifstream myfile; //open file
                string myline;
                string file_location = "data/" + file_name;
                myfile.open(file_location.c_str());
              //cout << file_name << " opened" << endl;

                int rowcount = 0;

                while(getline(myfile, myline)) {
                    //cout << "line get " << endl;
                    rowcount++; //rowcount
                }
                //cout << rowcount << endl;

                struct stat st;
                stat(file_location.c_str(), &st);
                double fileSize =  st.st_size;

                int numberofblocks = ceil(fileSize/(16000)); //# of blocks

                myfile.close();

                row = row + file_name + "\t\t" + to_string(rowcount) + "\t\t" + to_string(numberofblocks) + "\n";


            }

            else if (CheckSuffix(index_extension, file_name)) {
                // Search for the table.



                string tbl_name, col_name;
                if (ParseFileName(file_name, &tbl_name, &col_name)) {
                    // Then successfuly parsed the file_names
                    auto found = database.find(tbl_name);
                    found->second.LoadBtreeIndex(col_name, file_name);//Loads in BTree Stuff here





                    if (debug == true) {
                        cout << "DEBUG: Loading in " << tbl_name << "." << col_name << " into memory, it has a B-tree index.\n";
                    }
                }
            }

        }

        ofstream outfile;
        outfile.open("data/tables.stat");
        outfile << "table name\t\trowcount\t#blocks\n";
        outfile << row; 

        outfile.close();
    }
}

string ConvertToString(char* str) {
    string output;
    while(*str != '\0') {
        output += *str;
        str++;
    }
    return output;
}

// Check if the row contains all the correct names.
// Returns 1 for correct.
// Returns 0 for failure.
int CheckRow(expression* condition, unordered_map<string, string>& row) {
    if (!condition) {
        return 1;
    }
    switch(condition->func) {
        case OP_IN:
        {
            expression* column = condition->values[0].ep;
            expression* select = condition->values[1].ep;
            if (column->func != OP_COLNAME) {
                cout << "OP CODE IS NOT COLUMN NAME -- Checking IN.\n";
                return -1;
            }
            vector<vector<string>> table_rows;
            vector<string> headers;
            string col_name = column->values[0].name;
            projection(select, table_rows, headers);
            if (headers.size() != 1) {
                cout << "Invalid subquery in IN.\n";    
                return -1;
            }
            auto found = row.find(col_name);
            if (found == row.end()) {
                cout << "Column name does not exist in IN.\n";
                return -1;
            }
            for (vector<string> row : table_rows) {
                if (row[0] == found->second) {
                    // it exists.
                    return 1;
                }
            }
            return 0;
        }
        case OP_COLNAME:
            if (row.find(ConvertToString(condition->values[0].name)) != row.end()) {
                auto found = row.find(ConvertToString(condition->values[0].name));
                return atoi((found->second).c_str());
            }
            eval.ColumnsExist = false;
            return 0;

        case OP_NUMBER:
            return condition->values[0].num;
        case OP_STRING:
            eval.ColumnsExist = false;
            return 0;
        case OP_NULL:
            return 0;

        case OP_PLUS:
            return CheckRow(condition->values[0].ep, row) + CheckRow(condition->values[1].ep, row);
            break;
        case OP_BMINUS:
            return CheckRow(condition->values[0].ep, row) - CheckRow(condition->values[1].ep, row);
            break;
        case OP_TIMES:
            return CheckRow(condition->values[0].ep, row) * CheckRow(condition->values[1].ep, row);
            break;
        case OP_DIVIDE:
            return CheckRow(condition->values[0].ep, row) / (CheckRow(condition->values[1].ep, row)+0.0);
            break;

        case OP_AND:
            if (CheckRow(condition->values[0].ep, row) && CheckRow(condition->values[1].ep, row)) {
                return 1;
            }
            return 0;
        case OP_OR:
            if (CheckRow(condition->values[0].ep, row) || CheckRow(condition->values[1].ep, row)) {
                return 1;
            }
            return 0;
        case OP_NOT:
            if (CheckRow(condition->values[0].ep, row) != 0) {
                return 1;
            } else {
                return 0;
            }
            break;
        case OP_GT:
            if (CheckRow(condition->values[0].ep, row) > CheckRow(condition->values[1].ep, row)) {
                return 1;
            }
            return 0;
        case OP_LT:
            if (CheckRow(condition->values[0].ep, row) < CheckRow(condition->values[1].ep, row)) {
                return 1;
            }
            return 0;
        case OP_EQUAL:
            if(condition->values[0].ep->func == OP_STRING && condition->values[1].ep->func == OP_STRING) {
                if (condition->values[0].ep->values[0].data == condition->values[1].ep->values[0].data) {
                    return 1;
                }
            }
            else if ((condition->values[0].ep->func == OP_COLNAME) && condition->values[1].ep->func == OP_COLNAME) {
                auto find1 = row.find(ConvertToString(condition->values[0].ep->values[0].name));
                auto find2 = row.find(ConvertToString(condition->values[1].ep->values[0].name));
                cout << "???" << endl;
                if (find1 != row.end() && find2 != row.end()) {
                    if (find1->second == find2->second) {
                        return 1;
                    }
                } else {
                    eval.ColumnsExist = false;
                    return 0;
                }
            }
            else if (condition->values[0].ep->func == OP_COLNAME && condition->values[1].ep->func == OP_STRING) {
                auto find = row.find(ConvertToString(condition->values[0].ep->values[0].name));
                if (find->second == ConvertToString(condition->values[1].ep->values[0].data)) {
                    return 1;
                } else {
                    eval.ColumnsExist = false;
                }
            }
            else if (condition->values[1].ep->func == OP_COLNAME && condition->values[0].ep->func == OP_STRING) {
                auto find = row.find(ConvertToString(condition->values[1].ep->values[0].name));
                if (find->second == condition->values[0].ep->values[0].data) {
                    return 1;
                } else {
                    eval.ColumnsExist = false;
                }
            }
            else {

                if (CheckRow(condition->values[0].ep, row) == CheckRow(condition->values[1].ep, row)) {
                    return 1;
                }
            }
            return 0;
        case OP_NOTEQ:
            if(condition->values[0].ep->func == OP_STRING && condition->values[1].ep->func == OP_STRING) {
                if (condition->values[0].ep->values[0].data != condition->values[1].ep->values[0].data) {
                    return 1;
                }
            }
            else if ((condition->values[0].ep->func == OP_COLNAME) && condition->values[1].ep->func == OP_COLNAME) {
                auto find1 = row.find(condition->values[0].ep->values[0].name);
                auto find2 = row.find(condition->values[1].ep->values[0].name);
                if (find1 != row.end() && find2 != row.end()) {
                    if (find1->second != find2->second) {
                        return 1;
                    }
                } else {
                    eval.ColumnsExist = false;
                }
            }
            else if (condition->values[0].ep->func == OP_COLNAME && condition->values[1].ep->func == OP_STRING) {
                auto find = row.find(condition->values[0].ep->values[0].name);
                if (find->second != ConvertToString(condition->values[1].ep->values[0].data)) {
                    return 1;
                } else {
                    eval.ColumnsExist = false;
                }
            }
            else if (condition->values[1].ep->func == OP_COLNAME && condition->values[0].ep->func == OP_STRING) {
                auto find = row.find(condition->values[1].ep->values[0].name);
                if (find->second != condition->values[0].ep->values[0].data) {
                    return 1;
                } else {
                    eval.ColumnsExist = false;
                }
            }
            else {
                if (CheckRow(condition->values[0].ep, row) != CheckRow(condition->values[1].ep, row)) {
                    return 1;
                } else {
                    eval.ColumnsExist = false;
                }
            }
            return 0;
        case OP_GEQ:
            if (CheckRow(condition->values[0].ep, row) >= CheckRow(condition->values[1].ep, row)) {
                return 1;
            }
            return 0;
        case OP_LEQ:
            if (CheckRow(condition->values[0].ep, row) <= CheckRow(condition->values[1].ep, row)) {
                return 1;
            }
            return 0;
    }

    return 0;
}


int evaluate(expression* ep, unordered_map<string, string>& row) {
   switch(ep->func) {
        case OP_NUMBER:
            return ep->values[0].num;
        case OP_STRING:
            return 0;
        case OP_NULL:
            return 0;
        case OP_COLNAME:
            if (row.find(ConvertToString(ep->values[0].name)) != row.end()) {
                auto found = row.find(ConvertToString(ep->values[0].name));
                return atoi((found->second).c_str());
            }
            eval.ProjectionsExist = false;

            return 1;

        case OP_PLUS:
            return CheckRow(ep->values[0].ep, row) + CheckRow(ep->values[1].ep, row);
        case OP_BMINUS:
            return CheckRow(ep->values[0].ep, row) - CheckRow(ep->values[1].ep, row);
            break;
        case OP_TIMES:
            return CheckRow(ep->values[0].ep, row) * CheckRow(ep->values[1].ep, row);
            break;
        case OP_DIVIDE:
            return CheckRow(ep->values[0].ep, row) / (CheckRow(ep->values[1].ep, row)+0.0);
     }
     return 1;
}

int CheckSelectIndex(vector<string> &table_names, vector<string> &projection, 
    expression* condition, unordered_map<string, Table> &db, int level) {
    if (!condition) {
        return 0;
    }

    switch(level) {
        case 0: {
            int comparison = 0;
            string col_name;
            bool col_name_on_left = false;
            if (condition->func >= 20 && condition->func <= 25) {
                if (condition->values[0].ep->func == OP_COLNAME && condition->values[1].ep->func != OP_COLNAME) {
                    // Calculate the right side of the tree.
                    comparison = CheckSelectIndex(table_names, projection, condition->values[1].ep, db, level + 1);
                    col_name = condition->values[0].ep->values[0].name;
                } else if (condition->values[1].ep->func == OP_COLNAME && condition->values[0].ep->func != OP_COLNAME) {
                    // Calculate the left side of the tree.
                    comparison = CheckSelectIndex(table_names, projection, condition->values[0].ep, db, level + 1);
                    col_name = condition->values[1].ep->values[0].name;
                    col_name_on_left = true;
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
            // Find the table that the column corresponds to.
            string table_name;
            Table t;
            btree::btree_map<string, int> tree;
            for (int i = 0; i < (int) table_names.size(); ++i) {
                auto table = db.find(table_names[i]);
                if (table->second.DoesKeyExists(col_name)) {
                    table_name = table_names[i];
                    tree = table->second.GetTree(col_name);
                    table_names.erase(table_names.begin()+i);
                    t = table->second;
                    break;
                }
            }
            if (table_name == "") {
                if (debug) {
                    cout << "No index file for " << col_name << endl;
                }
                return 0;
            }
            switch(condition->func) {
                case OP_EQUAL: {
                    string comparison_string = to_string(comparison);
                    auto found_node = tree.find(comparison_string);
                    if (found_node == tree.end()) {
                        cout << "0 fetched.\n";
                        if (debug) {
                            cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                        }
                        // This means that none were found, so we don't need to output anything.
                        return 1;
                    }
                    vector<string> headers = t.GetHeaders();
                    for (string t_name : table_names) {
                        for (auto finder : db.find(t_name)->second.GetHeaders()) {
                            headers.push_back(finder);
                        }
                    }
                    vector<vector<string>> ans;
                    combination(table_names, 0, t.GetRows()[found_node->second], ans, db);
                    vector<vector<string>> finalOutput;
                    for (auto row : ans) {
                        vector<string> makeRow;
                        for (string proj : projection) {
                            for (int i = 0; i < (int) headers.size(); ++i) {
                                if (headers[i] == proj) {
                                    makeRow.push_back(row[i]);
                                    break;
                                }
                            }
                        }
                        finalOutput.push_back(makeRow);
                    }
                    for (auto oneRow : finalOutput) {
                        for (string str : oneRow) {
                            cout << str << "\t";
                        }
                        cout << endl;
                    }
                    cout << ans.size() << " fetched.\n";

                }
                return 1;   
                break;
                case OP_GT: {
                    if (col_name_on_left == false) {
                        string comparison_string = to_string(comparison);
                        auto found_node = tree.find(comparison_string);
                        if (found_node == tree.end()) {
                            cout << "0 fetched.\n";
                            if (debug) {
                                cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                            }
                            // This means that none were found, so we don't need to output anything.
                            return 1;
                        }
                        vector<vector<string>> rows_temp = t.GetRows();
                        vector<vector<string>> ans;
                        found_node++;
                        while(found_node != tree.end()) {
                            ans.push_back(rows_temp[found_node->second]);
                            // cout << found_node->first << endl; 
                            found_node++;
                        }

                        vector<string> headers = t.GetHeaders();
                        for (string t_name : table_names) {
                            for (auto finder : db.find(t_name)->second.GetHeaders()) {
                                headers.push_back(finder);
                            }
                        }
                        vector<vector<string>> finalOutput;
                        for (auto row : ans) {
                            vector<string> makeRow;
                            for (string proj : projection) {
                                for (int i = 0; i < (int) headers.size(); ++i) {
                                    if (headers[i] == proj) {
                                        makeRow.push_back(row[i]);
                                        break;
                                    }
                                }
                            }
                            finalOutput.push_back(makeRow);
                        }
                        for (auto oneRow : finalOutput) {
                            for (string str : oneRow) {
                                cout << str << "\t";
                            }
                            cout << endl;
                        }
                        cout << ans.size() << " fetched.\n";
                        if (debug) {
                            cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                        }
                        return 1;

                    }
                }
                    
                    // left or right side.
                    return 0;
                    break;
                case OP_LT:  {
                    if (col_name_on_left == false) {
                        string comparison_string = to_string(comparison);
                        auto found_node = tree.find(comparison_string);
                        if (found_node == tree.end()) {
                            cout << "0 fetched.\n";
                            if (debug) {
                                cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                            }
                            // This means that none were found, so we don't need to output anything.
                            return 1;
                        }
                        vector<vector<string>> rows_temp = t.GetRows();
                        vector<vector<string>> ans;
                        found_node--;
                        while(found_node != tree.begin()) {
                            ans.push_back(rows_temp[found_node->second]);
                            found_node--;
                            if (found_node == tree.begin()) {
                                ans.push_back(rows_temp[found_node->second]);
                            }
                        }

                        vector<string> headers = t.GetHeaders();
                        for (string t_name : table_names) {
                            for (auto finder : db.find(t_name)->second.GetHeaders()) {
                                headers.push_back(finder);
                            }
                        }
                        vector<vector<string>> finalOutput;
                        for (auto row : ans) {
                            vector<string> makeRow;
                            for (string proj : projection) {
                                for (int i = 0; i < (int) headers.size(); ++i) {
                                    if (headers[i] == proj) {
                                        makeRow.push_back(row[i]);
                                        break;
                                    }
                                }
                            }
                            finalOutput.push_back(makeRow);
                        }
                        for (auto oneRow : finalOutput) {
                            for (string str : oneRow) {
                                cout << str << "\t";
                            }
                            cout << endl;
                        }
                        cout << ans.size() << " fetched.\n";
                        if (debug) {
                            cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                        }
                        return 1;

                    }
                }
                    // left or right side.
                    return 0;
                    break;
                case OP_NOTEQ:{
                    string comparison_string = to_string(comparison);
                    auto found_node = tree.find(comparison_string);
                    vector<vector<string>> ans = t.GetRows();
                    // then it's just the whole file.
                    if (found_node != tree.end()) {
                        ans.erase(ans.begin()+found_node->second);
                        // This means that none were found, so we don't need to output anything.
                    } 
                    vector<string> headers = t.GetHeaders();
                    for (string t_name : table_names) {
                        for (auto finder : db.find(t_name)->second.GetHeaders()) {
                            headers.push_back(finder);
                        }
                    }

                    vector<vector<string>> finalOutput;
                    for (auto row : ans) {
                        vector<string> makeRow;
                        for (string proj : projection) {
                            for (int i = 0; i < (int) headers.size(); ++i) {
                                if (headers[i] == proj) {
                                    makeRow.push_back(row[i]);
                                    break;
                                }
                            }
                        }
                        finalOutput.push_back(makeRow);
                    }
                    for (auto oneRow : finalOutput) {
                        for (string str : oneRow) {
                            cout << str << "\t";
                        }
                        cout << endl;
                    }
                    cout << ans.size() << " fetched.\n";
                    if (debug) {
                        cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                    }
                    return 1;
                }
                    break;
                case OP_GEQ: {
                    if (col_name_on_left == false) {
                        string comparison_string = to_string(comparison);
                        auto found_node = tree.find(comparison_string);
                        if (found_node == tree.end()) {
                            cout << "0 fetched.\n";
                            if (debug) {
                                cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                            }
                            // This means that none were found, so we don't need to output anything.
                            return 1;
                        }
                        vector<vector<string>> rows_temp = t.GetRows();
                        vector<vector<string>> ans;
                        while(found_node != tree.end()) {
                            ans.push_back(rows_temp[found_node->second]);
                            // cout << found_node->first << endl; 
                            found_node++;
                        }

                        vector<string> headers = t.GetHeaders();
                        for (string t_name : table_names) {
                            for (auto finder : db.find(t_name)->second.GetHeaders()) {
                                headers.push_back(finder);
                            }
                        }
                        vector<vector<string>> finalOutput;
                        for (auto row : ans) {
                            vector<string> makeRow;
                            for (string proj : projection) {
                                for (int i = 0; i < (int) headers.size(); ++i) {
                                    if (headers[i] == proj) {
                                        makeRow.push_back(row[i]);
                                        break;
                                    }
                                }
                            }
                            finalOutput.push_back(makeRow);
                        }
                        for (auto oneRow : finalOutput) {
                            for (string str : oneRow) {
                                cout << str << "\t";
                            }
                            cout << endl;
                        }
                        cout << ans.size() << " fetched.\n";
                        if (debug) {
                            cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                        }
                        return 1;

                    }
                }
                return 0;
                case OP_LEQ: {
                    if (col_name_on_left == false) {
                        string comparison_string = to_string(comparison);
                        auto found_node = tree.find(comparison_string);
                        if (found_node == tree.end()) {
                            cout << "0 fetched.\n";
                            if (debug) {
                                cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                            }
                            // This means that none were found, so we don't need to output anything.
                            return 1;
                        }
                        vector<vector<string>> rows_temp = t.GetRows();
                        vector<vector<string>> ans;
                        auto start = tree.begin();
                        while(start != found_node) {
                            ans.push_back(rows_temp[start->second]);
                            // cout << found_node->first << endl; 
                            start++;
                            if (start == found_node) {
                                ans.push_back(rows_temp[start->second]);
                            }
                        }

                        vector<string> headers = t.GetHeaders();
                        for (string t_name : table_names) {
                            for (auto finder : db.find(t_name)->second.GetHeaders()) {
                                headers.push_back(finder);
                            }
                        }
                        vector<vector<string>> finalOutput;
                        for (auto row : ans) {
                            vector<string> makeRow;
                            for (string proj : projection) {
                                for (int i = 0; i < (int) headers.size(); ++i) {
                                    if (headers[i] == proj) {
                                        makeRow.push_back(row[i]);
                                        break;
                                    }
                                }
                            }
                            finalOutput.push_back(makeRow);
                        }
                        for (auto oneRow : finalOutput) {
                            for (string str : oneRow) {
                                cout << str << "\t";
                            }
                            cout << endl;
                        }
                        cout << ans.size() << " fetched.\n";
                        if (debug) {
                            cout << "DEBUG: " << table_name << "." << col_name << " has B-Tree index\n";
                        }
                        return 1;

                    }
                }
                return 0;
                default:
                    return 0;
            }
            // Made it all the way back, thus print out the combination stuff.
        }
        // check the LT, GT ...etc.
        default: {
            switch(condition->func) {
                case OP_NUMBER:
                    return condition->values[0].num;
                case OP_PLUS:
                case OP_BMINUS:
                case OP_DIVIDE:
                case OP_TIMES:
                default:
                    return 0;
            }
        }
    }
    return 1;
}


// Return 0 for success
int product(expression *ep, vector<vector<string>> &output, 
    vector<string>& headers) {
    if (ep->func != OP_PRODUCT) {
        cout << "Went into the product function when op code is not product.\n";
        return -1;
    }
    // cout << "Enters here\n";
    // Example query: 
    // select a from B,C where x=10;
    if(ep->values[0].ep->func == OP_PRODUCT && ep->values[1].ep->func == OP_TABLENAME) {
        if (product(ep->values[0].ep, output, headers) != 0) {
            // then it successfully returned something in output and headers.
            cout << "Error in calculating JOIN-ing.\n";
            return -1;
        }
        string table1 = ep->values[1].ep->values[0].name;
        auto find = database.find(table1);
        if (find == database.end()) {
            cout << "Database could not find table " << table1 << endl;
            return -1;
        }
        vector<vector<string>> table_rows = find->second.GetRows();
        vector<string> table_headers = find->second.GetHeaders();
        output = CartesianProduct(output, table_rows);
        headers.insert(headers.end(), table_headers.begin(), table_headers.end());
        // headers = CartesianProduct(table_headers, headers);

    } else {
        if (ep->values[0].ep->func != OP_TABLENAME || ep->values[1].ep->func != OP_TABLENAME) {
            cout << "OP CODE's in Product is not tablename\n" << endl;    
            return -1;
        }
        string table_name1 = ep->values[0].ep->values[0].name;
        string table_name2 = ep->values[1].ep->values[0].name;
        auto find = database.find(table_name1);
        if(find == database.end()) {
            cout << "Database could not find table " << table_name1 << endl;
            
            return -1;
        }
        vector<vector<string>> tab1 = find->second.GetRows();
        vector<string> head1 = find->second.GetHeaders();
        
        find = database.find(table_name2);
        if(find == database.end()) {
            cout << "Database could not find table " << table_name2 << endl;
            return -1;
        }
        vector<vector<string>> tab2 = find->second.GetRows();
        vector<string> head2 = find->second.GetHeaders();
    
        output = CartesianProduct(tab1, tab2);
        head1.insert(head1.end(), head2.begin(), head2.end());
        headers = head1;
        // headers = CartesianProduct(head1, head2);
    
    }

    return 0;
}


// Returns 0 for success
// Else it's an error.
int selection(expression* ep, vector<vector<string>>& output, vector<string> &table_headers) {
    if (ep->func != OP_SELECTION) {
        cout << "Function code does not match -- SELECTION\n";
        return -1;
    }
    expression *tables = ep->values[0].ep;
    expression *where_clause = ep->values[1].ep;
    vector<vector<string>> table_information;
    // vector<string> table_headers;

    // Gather the table information first.
    if(tables->func == OP_PRODUCT) {
        if (product(tables, table_information, table_headers) != 0) {
            return -1;
        }
        // Calls a function that prints the table.
    } else {
        // then it should just be a table.
        if (tables->func == OP_TABLENAME) {
            // if it's just a table_name, then we just get the entries
            // from that table.
            string table_name = tables->values[0].name;
            
            auto found = database.find(table_name);
            if (found != database.end()) {
                table_information = found->second.GetRows();
                table_headers = found->second.GetHeaders();
            } else {
                cout << "Table does not exist.\n";   
                return -1; 
            }
        } else {
            cout << "Missing table in the expression tree.\n";
            return -1;
        }
    }

    if (where_clause->func == OP_RLIST) {
        output = table_information;
        return 0;
    }

    for (vector<string> row : table_information) {
        // create a hashmap; 
        unordered_map<string, string> row_map; 
        for (unsigned int i = 0; i < row.size(); ++i) {
            row_map.emplace(table_headers[i], row[i]);
        }
        if (CheckRow(where_clause, row_map) == 1) {
            output.push_back(row);
        }
    }

    return 0;
}

int FuncCall(expression *ep, vector<vector<string>>& input, 
    vector<string>& headers, int& output) {
    if (ep->func != OP_FCALL) {
        cout << "OP CODE does not match -- FUNC.\n";
        return -1;
    }
    expression* fcall = ep->values[0].ep;
    string func_type;
    if (fcall != NULL && fcall->func == OP_FNAME) {
        func_type = fcall->values[0].name;
    } else {
        cerr << "Operation code is not valid -- FuncCall.\n";
        return -1;
    }
    expression *fcall_var = ep->values[1].ep;
    if (fcall_var != NULL && fcall_var->func != OP_RLIST 
        && fcall_var->values[0].ep->func != OP_COLNAME) {
        cerr << "OPCODE is not correct -- FuncCall aggregate functions.\n";
        return -1;
    }

    int index = -1;
    // cout << "Can't do this\n";
    // cout<< fcall_var->values[0].ep->func << endl;
    string col_name = fcall_var->values[0].ep->values[0].name;
    for (unsigned int i = 0; i < headers.size(); ++i) {
        if (headers[i] == col_name) {
            index = i;
            break;
        }
    }
    if (index == -1) {
        cout << "Column name in aggregate function does not exist.\n";
        return -1;
    }


    if (func_type == "sum" || func_type == "avg") {
        int sum = 0;
        for (vector<string> row : input) {
            sum += stoi(row[index]);
        }
        output = sum;
        if (func_type == "avg") {
            output = sum/(input.size() + 0.0);
        }
    } else if (func_type == "count") {
        output = input.size();
    } else if (func_type == "min") {
        int min = numeric_limits<int>::max();
        for (vector<string> row : input) {
            if (stoi(row[index]) < min) {
                min = stoi(row[index]);
            }
        }
        output = min;
    } else if (func_type == "max") {
        int max = -numeric_limits<int>::max();
        for (vector<string> row : input) {
            if (stoi(row[index]) > max) {
                max = stoi(row[index]);
            }
        }
        output = max;
    } else {
        cout << "Did not implement "<< func_type << " this aggregate function.\n";
        return -1;
    }

    return 0;
}


int GroupBy(expression* ep, unordered_map<string, vector<vector<string>>>& output, 
    vector<string>& table_headers) {
    if (ep->func != OP_GROUP) {
        cout << "Function code does not match - GROUP BY.\n";
        return -1;
    }

    vector<vector<string>> table_rows;

    expression *first = ep->values[0].ep;
    if (first->func == OP_TABLENAME) {
        string table_name = first->values[0].name;
        auto find = database.find(table_name);
        if (find == database.end()) {
            cout << "Could not find table in Group by\n";
            return -1;  
        }
        table_headers = find->second.GetHeaders();
        table_rows = find->second.GetRows();
    } else if (first->func == OP_PRODUCT) {
        if(product(first, table_rows, table_headers) != 0) {
            return -1;
        }
    } else {
        cout << "Foreign OP_CODE in Groupby.\n";
        return -1;
    }

    expression* groupby = ep->values[1].ep;
    vector<int> group;
    while(groupby != NULL && groupby->func == OP_RLIST) {
        if (groupby->values[0].ep->func != OP_COLNAME) {
            cout << "Function code does not match -- GROUP BY -- TABLENAME.\n";
            return -1;
        }
        string col_name = groupby->values[0].ep->values[0].name;
        // Checks to see if these column names are valid.
        bool found = false;
        for (unsigned int i = 0; i < table_headers.size(); ++i) {
            if (table_headers[i] == col_name) {
                group.push_back(i);
                found = true;
                break;
            }
        }
        if (found == false) {
            cout << "Could not find column name.\n";
            return -1;
        }
        groupby = groupby->values[1].ep;
    }

    if (group.size() == 0) {
        cout << "There was an error in parsing out the group names.\n";
        return -1;
    }

    for (vector<string> row : table_rows) {
        string key;
        for (int i : group) {
            key += row[i];
        }
        auto found = output.find(key);
        if (found == output.end()) {
            vector<vector<string>> ins;
            ins.push_back(row);
            output.emplace(key, ins);
        } else {
            found->second.push_back(row);
        }
    }
    // Sort the groups.
    return 0;
}

// returns 0 for success
// returns -1 for failure
int projection(expr e, vector<vector<string>>& output, vector<string>& headers) {
    expression *ep = (expression*) e;

    expression* select = ep->values[0].ep;
    expression* project = ep->values[1].ep;

    vector<string> table_headers;
    vector<vector<string>> table_rows;
    if (select->func == OP_SELECTION) {
        if (selection(select, table_rows, table_headers) != 0) {
            return -1;
        }    
    } else if (select->func == OP_GROUP) {
        unordered_map<string, vector<vector<string>>> groupby_output;
        if (GroupBy(select, groupby_output, table_headers) != 0) {
            return -1;
        }
        bool flag_headers = false;
        expression *copy_project = project;
        for (auto single_group : groupby_output) {
            vector<string> temp_out;
            project = copy_project;
            // Loop through the projection and find the aggregate functions.
            while(project != NULL && project->func == OP_RLIST) {
                expression* as = project->values[0].ep;
                // AS opcode is OP_OUTCOLNAME
                if (as ->func != OP_OUTCOLNAME) {
                    cout << "Missing AS in the expression tree.\n";
                    return -1;
                }
                if (as->values[0].ep->func == OP_FCALL) {
                    int ans;
                    if (FuncCall(as->values[0].ep, single_group.second, table_headers, ans) != 0) {
                        return -1;
                    }
                    temp_out.push_back(to_string(ans));
                } else if (as->values[0].ep->func == OP_COLNAME) {
                    string original_name = as->values[0].ep->values[0].name;
                    for (unsigned int i = 0; i < table_headers.size(); ++i) {
                        if (original_name == table_headers[i]) {
                            auto x = single_group.second[0][i];
                            temp_out.push_back(x);
                            break;
                        }
                        if (i == table_headers.size() -1 ) {
                            cout << "Column name does not exist.\n";
                            return -1;
                        }
                    }
                } else {
                    cout << "Incorrect OP Code in Projection.\n";
                    return -1;
                }
                if (as->values[1].ep->func == OP_COLNAME && flag_headers != true) {
                    headers.insert(headers.begin(), as->values[1].ep->values[0].name);
                }
                project = project->values[1].ep;
            } // while
            if (headers.size() != 0) {
                flag_headers = true;
            }
            output.insert(output.begin(), temp_out);
        }
        return 0;
    } else if (select->func == OP_PRODUCT) {
        if (product(select, table_rows, table_headers) != 0) {
            return -1;
        }
    } else if (select->func == OP_TABLENAME) {
        string table_name = select->values[0].name;
        auto find = database.find(table_name);
        if (find == database.end()) {
            cout << "Could not find the table." << endl;
            return -1;
        }
        table_headers = find->second.GetHeaders();
        table_rows = find->second.GetRows();
    } else {
        cout << "Function codes do not match.\n";
        return -1;
    }

    vector<int> index;

    while(project != NULL && project->func == OP_RLIST) {
        // Arithmetic does not work on the projection.
        expression* as = project->values[0].ep;
        if (as->func != OP_OUTCOLNAME) {
            cout << "Missing AS in the expression tree.\n";
            return -1;
        }
        if (as->values[0].ep->func != OP_COLNAME || as->values[1].ep->func != OP_COLNAME) {
            cout << "Missing column names in the AS clause in expression tree.\n";
            return -1;
        }
        // cout << "AS: " << as->values[0].ep->values[0].name << endl;
        // cout << "AS: " << as->values[1].ep->values[0].name << endl;
        headers.insert(headers.begin(), as->values[1].ep->values[0].name);
        string original_name = as->values[0].ep->values[0].name;
        // look for it and insert it index.
        for (unsigned int i = 0; i < table_headers.size(); ++i) {
            if (original_name == table_headers[i]) {
                index.insert(index.begin(), i);
                break;
            }
        }

        project = project->values[1].ep;
    }

    for (vector<string> row : table_rows) {
        vector<string> new_output;
        for (int i : index) {
            new_output.push_back(row[i]);
        }
        output.push_back(new_output);
    }

    return 0;
}

/**********************************************************************/
/* Dummy routines that need to be filled out for HW4 ******************/
/* Move to another module and fully define there **********************/
/**********************************************************************/
expr compile(expr e)
{
    return e;
}

void Clear() {
    eval.table_name = "";
    eval.values.clear();
    // eval.table_names.clear();
    eval.projections.clear();
    eval.keys.clear();
    eval.ProjectionsExist = true;
    eval.ColumnsExist = true;
    eval.filter = NULL;
}

bool SortComparison(vector<string> a, vector<string> b) {
    return a[global_index_for_sort] < b[global_index_for_sort];
}


int SortProjection(expression* ep, vector<vector<string>>& table_rows, 
    vector<string> &table_headers) {
    if(projection(ep->values[0].ep, table_rows, table_headers) != 0) {
        return -1;
    }

    // All the error checking.
    expression* rlist = ep->values[1].ep;
    expression* ordering = rlist->values[0].ep;
    if (rlist == NULL) {
        if(debug) {
            cout << "Ordering not specified. Node in expression tree is NULL.\n";
        }
        return -1;
    }
    if (rlist->func!= OP_RLIST) {
        cout << "OP CODE IS NOT OF RLIST.\n";
        return -1;
    }
    if (ordering == NULL) {
        cout << "Ordering not specified. Node in expression tree is NULL.\n";
        return -1;
    }
    if (ordering->func != OP_SORTSPEC) {
        cout << "OP CODE IS NOT OF OP_SORTPSEC.\n";
        return -1;
    }
    if (ordering->values[0].ep->func != OP_COLNAME) {
        cout << "OP CODE IS NOT OP COLNAME.\n";
        return -1;
    }

    string sort_col_name = ordering->values[0].ep->values[0].name;
    // check if the column exists in the table.
    for (size_t i = 0; i < table_headers.size(); ++i) {
        if (table_headers[i] == sort_col_name) {
            global_index_for_sort = i;
            break;
        }        
        if (i == table_headers.size()-1) {
            cout << "Could not find the column name in the table.\n";
            return -1;
        }
    }

    if (global_index_for_sort == -1) {
        cout << "Could not find the column name in the table.\n";
        return -1;
    }

    sort(table_rows.begin(), table_rows.end(), SortComparison);
    // TODO : SORT BY THAT INDEX (ordering_index).
    global_index_for_sort = -1;
    return 0;
}


expr evalexpr(expr e) {
    expression *ep = (expression *) e;
    switch(ep->func) {
        case OP_SORT:
        {
            vector<vector<string>> output;
            vector<string> headers;
            if (SortProjection(ep, output, headers) == 0) {
                print_table(headers, output);
            }
        }
        // call the sort function, which calls the projection function.
        // seelct a from b where x=10 order by a;
        // this call is made before projections.
        break;
        case OP_PROJECTION:
        {
            // Output will be a vector of vectors of strings.        
            vector<vector<string>> output;
            vector<string> headers;
            if (projection(e, output, headers) == 0) {
                print_table(headers, output);
            }
            break;
        }
        case OP_INSERTROW:
            create_or_insert(e, false);
            // Call the insertion function
            break;
        case OP_CREATETABLE:
            create_or_insert(e, true);
            // Call the create table function.
            break;
        default:
            cout << "Not yet implemented.\n";
            break;
    }

    print_e(e, 0);
    return e;
}

expr optimize(expr e) { return e;}

void print_relation(expr e) { }

void drop_table(expr e) {
    expression *ep = (expression*) e;
    switch(ep->func) {
        case OP_TABLENAME:
            {
                string table_name = (string)ep->values[0].name;
                string file_name = dir_name + table_name + file_extension;
                if (!remove(file_name.c_str())) {
                    printf("Successfully removed file.\n");
                } else {
                    printf("Error in removing file.\n");
                }
            }
            break;
        default:
            printf("No");
            break;
    }
}


void print_e(expr e, int lev)
{
    expression *ep = (expression *)e;
    register int i, slev=lev;

    if(!ep) { printf("() "); return; }
    switch(ep->func) {

    /* Literals */
    case OP_NUMBER: printf("%d ", ep->values[0].num); return;
    case OP_STRING: printf("%s ", ep->values[0].data); return;
    case OP_NULL:   printf("NULL "); return;

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

    case OP_PLUS:   printf("(+ \n"); break;
    case OP_BMINUS: printf("(- \n"); break;
    case OP_TIMES:  printf("(* \n"); break;
    case OP_DIVIDE: printf("(/ \n"); break;

    case OP_AND:    printf("(AND \n"); break;
    case OP_OR: printf("(OR \n"); break;
    case OP_NOT:    printf("(! \n"); break;
    case OP_GT: printf("(> \n"); break;
    case OP_LT: printf("(< \n"); break;
    case OP_EQUAL:  printf("(== \n"); break;
    case OP_NOTEQ:  printf("(<> \n"); break;
    case OP_GEQ:    printf("(>= \n"); break;
    case OP_LEQ:    printf("(<= \n"); break;

    case OP_SORTSPEC:
            printf("(SORTSPEC \n"); break;

    case OP_OUTCOLNAME:
            printf("(AS \n"); break;

    case OP_RLIST:  printf("(RLIST \n"); break;
    default:    printf("(%d \n", ep->func); break;
    }
    lev += 2;
    for(i=0; i<lev; i++) putchar(' ');
    print_e(ep->values[0].ep, lev+2); putchar(' ');
    print_e(ep->values[1].ep, lev+2);
    putchar('\n');
    for(i=0; i<slev; i++) putchar(' ');
    putchar(')');
}
