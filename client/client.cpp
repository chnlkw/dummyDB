#include "../include/client.h"
#include "../tool/tokenize.h"
#include "../tool/split_csv.h"

#include "dummydb.h"

using namespace std;

map<string, vector<string> > table2name;
map<string, vector<string> > table2type;
map<string, vector<string> > table2pkey;
vector<string> result;
map<string, DummyQuery> table2query;

void done(const vector<string>& table, const map<string, int>& m,
	int depth, vector<string>& row)
{
	FILE *fin;
	char buf[65536];
	vector<string> column_name, token;
	string str;
	int i;

	if (depth == table.size()) {
		str = row[0];
		for (i = 1; i < row.size(); i++)
			str += "," + row[i];
		result.push_back(str);
		return;
	}

	assert(table2name.find(table[depth]) != table2name.end());
	column_name = table2name[table[depth]];

	fin = fopen(((string) "data/" + table[depth]).c_str(), "r");
	assert(fin != NULL);

	while (fgets(buf, 65536, fin) != NULL) {
		int len = strlen(buf);
		if (len > 0 && buf[len - 1] == '\n') {
			buf[len - 1] = '\0';
			len--;
		}
		if (len == 0)
			continue;

		split_csv(buf, token);
		assert(token.size() == column_name.size());

		for (i = 0; i < column_name.size(); i++)
			if (m.find(column_name[i]) != m.end())
				row[m.find(column_name[i]) -> second] = token[i];

		done(table, m, depth + 1, row);
	}

	fclose(fin);
}

void create(const string& table, const vector<string>& column,
	const vector<string>& type, const vector<string>& key)
{
	table2name[table] = column;
	table2type[table] = type;
	table2pkey[table] = key;
	table2query[table] = *(new DummyQuery);
}

void train(const vector<string>& query, const vector<double>& weight)
{
	// I am too clever; I don't need it.
}

void load(const string& table, const vector<string>& row)
{
	FILE *fout;
	int i;

	fout = fopen(((string) "data/" + table).c_str(), "w");
	assert(fout != NULL);

	for (i = 0; i < row.size(); i++)
		fprintf(fout, "%s\n", row[i].c_str());

	fclose(fout);
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
			vector<string>& name = table2name[table[j]];
			vector<string>& type = table2type[table[j]];
			int cInt = 0, cStr = 0, fType = -1;//caution: initial cInt = 1?
			for (int z = 0; z < name.size(); z++) {
				if (name[z] == token[i]) {
					if (type[z] == "INTEGER") fType = 0;
					else fType = 1; // type == "STRING"
				}
				if (type[z] == "INTEGER") cInt ++;
				else cStr ++; // type == "STRING"
			}
			if (fType == 0) {
				if (token[i+1] == "=") {
					table2query[table[j]].create(cInt, atoi(token[i+2].c_str()));
				} else if (token[i+1] == "<") {
					table2query[table[j]].create(cInt, INT_MIN, atoi(token[i+2].c_str())-1);
				} else {
					table2query[table[j]].create(cInt, atoi(token[i+2].c_str())+1, INT_MAX);
				}
			} else if (fType == 1 && token[i+1] == "=") {
				table2query[table[j]].create(cStr, token[i+2]);
			}
		}
		i = i+3;
	}
	m.clear();
	for (i = 0; i < output.size(); i++)
		m[output[i]] = i;

	row.clear();
	row.resize(output.size(), "");

	done(table, m, 0, row);
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


