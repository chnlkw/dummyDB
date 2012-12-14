#include "../include/client.h"
#include "../tool/tokenize.h"
#include "../tool/split_csv.h"

#include "dummydb.h"

using namespace std;

map<string, vector<string> > table2name;
map<string, vector<string> > table2type;
map<string, vector<string> > table2pkey;
map<string, DummyQuery> table2query;
vector<string> result;
DummyDB dummyDB;

void done(const vector<string>& table, const vector<string>& output,
	int depth, vector<string>& row)
{
	string str;
	if (depth == table.size()) {
		str = row[0];
		for (int i = 1; i < row.size(); i++)
			str += "," + row[i];
		result.push_back(str);
		return;
	}

	vector<DummyItem> ret = dummyDB.tables[table[depth]].Get(table2query[table[depth]]);
	for (int j = 0; j < ret.size(); j++) {
		for (int z = 0; z < output.size(); z++) {
			auto it1 = dummyDB.tables[table[depth]].col2intIdx.find(output[z]);
			if (it1 != dummyDB.tables[table[depth]].col2intIdx.end()) {
				char* num = itoa(ret[j].intdata[it1->second]);
				row[z] = num;
				delete num;
			}
			auto it2 = dummyDB.tables[table[depth]].col2strIdx.find(output[z]);
			if (it2 != dummyDB.tables[table[depth]].col2strIdx.end()) {
				row[z] = ret[j].strdata[it2->second];
			}
		}
		done(table, output, depth + 1, row);
	}
}

void create(const string& table, const vector<string>& column,
	const vector<string>& type, const vector<string>& key)
{
	table2name[table] = column;
	table2type[table] = type;
	table2pkey[table] = key;
	table2query[table] = DummyQuery();
	DummyTable dummyTable;
	//key process reserved
	for (int i = 0; i < column.size(); i++) {
		if (type[i] == "INTEGER") {
			dummyTable.col2intIdx[column[i]] = dummyTable.nInt;
			dummyTable.nInt++;
		} else {
			dummyTable.col2strIdx[column[i]] = dummyTable.nStr;
			dummyTable.nStr++;
			const char* str = type[i].c_str();
			int s = 8, p = 8;
			while(str[p] > 47 && str[p] < 58) {
				 p++;
			}
			string len = type[i].substr(s, p-s);
			dummyTable.StringTypeLen.push_back(atoi(len.c_str()));
		}
	}
	// be careful that here, we assume, are only int keys
	dummyTable.nIntKey = key.size();
	for (int i = 0; i < key.size(); i++) {
		dummyTable.IntKey.push_back(multimap<int, DummyItem>());
	}
	dummyDB.CreateTable(dummyTable, table);
}

void train(const vector<string>& query, const vector<double>& weight)
{
	// I am too clever; I don't need it.
}

void load(const string& tableName, const vector<string>& row)
{
	vector<string>& column = table2name[tableName];
	vector<string>& type = table2type[tableName];
	// key processing reserved
	for (int i = 0; i < row.size(); i++) {
		DummyItem dummyItem;
		vector<string> token;
		split_csv(row[i].c_str(), token);
		for (int j = 0; j < token.size(); j++) {
			if (type[j] == "INTEGER") {
				dummyItem.intdata.push_back(atoi(token[j].c_str()));
			} else {
				dummyItem.strdata.push_back(token[j]);
			}
		}
		dummyDB.tables[tableName].Insert(dummyItem);
	}	
}

void preprocess()
{
	// I am too clever; I don't need it.
}

void execute(const string& sql)
{
	vector<string> token, output, table, row;
	map<string, int> m;
	int i;

	result.clear();

	if (strstr(sql.c_str(), "INSERT") != NULL) {
		fprintf(stderr, "Sorry, I give up.\n");
		exit(1);
	}

	output.clear();
	table.clear();
	tokenize(sql.c_str(), token);
	for (i = 0; i < token.size(); i++) {
		if (token[i] == "SELECT" || token[i] == ",")
			continue;
		if (token[i] == "FROM")
			break;
		output.push_back(token[i]);
	}
	for (i++; i < token.size(); i++) {
		if (token[i] == "," || token[i] == ";")
			continue;
		if (token[i] == "WHERE")
			break;
		table.push_back(token[i]);
	}
	for (i++; i < token.size(); i++) {
		for(int j = 0; j < table.size(); j++) {
			auto it1 = dummyDB.tables[table[j]].col2intIdx.find(token[i]);
			if (it1 != dummyDB.tables[table[j]].col2intIdx.end()) {
				if (token[i+1] == "=") {
					table2query[table[j]].create(it1->second, atoi(token[i+2].c_str()));
				} else if (token[i+1] == "<") {
					table2query[table[j]].create(it1->second, INT_MIN, atoi(token[i+2].c_str())-1);
				} else {
					table2query[table[j]].create(it1->second, atoi(token[i+2].c_str())+1, INT_MAX);
				}
			}
			auto it2 = dummyDB.tables[table[j]].col2strIdx.find(token[i]);
			if(it2 != dummyDB.tables[table[j]].col2strIdx.end()) {
				table2query[table[j]].create(it2->second, token[i+2]);
			}
		}
		i = i+3;
	}
	m.clear();
	for (i = 0; i < output.size(); i++)
		m[output[i]] = i;

	row.clear();
	row.resize(output.size(), "");

	done(table, output, 0, row);
}

int next(char *row)
{
	if (result.size() == 0)
		return (0);
	strcpy(row, result.back().c_str());
	result.pop_back();

	/*
	 * This is for debug only. You should avoid unnecessary output
	 * in your submission, which will hurt the performance.
	 */
	printf("%s\n", row);

	return (1);
}

void close()
{
	// I have nothing to do.
}


