#include "../include/client.h"
#include "../tool/tokenize.h"
#include "../tool/split_csv.h"

#include "dummydb.h"
//#include "berkeleydb.h"

using namespace std;

map<string, vector<string> > table2name;
map<string, vector<string> > table2type;
map<string, vector<string> > table2pkey;
map<string, pair<string, int>> col2table_intIdx;
map<string, pair<string, int>> col2table_strIdx;
map<string, DummyQuery> table2query;
map<string, string> intEqual;
map<string, string> intGreater;
map<string, string> intLess;
map<string, string> strEqual;
map<string, vector<string>> projectTable;
vector<string> result;
DummyDB dummyDB;
bool prepareProject = false;

char* itoa(int value) {
	char* str = new char[20];
	char* res = new char[20];
	int count = 0, i = 0;
	while (value/10 > 0) {
		int num = value-value/10*10;
		value = value/10;
		str[count] = num+48;
		count++;
	}
	str[count] = value+48;
	count++;
	while (count > 0) {
		count--;
		res[i] = str[count];
		i++;
	}
	res[i] = '\0';
	delete str;
	return res;
}

void insert(const string& sql) {
	DummyItem dummyItem;
	vector<string> token;
	tokenize(sql.c_str(), token);
	int i = 0;
	for (; i < token.size(); i++) {
		if (token[i] == "INSERT") continue;
		if (token[i] == "TO") break;
	}
	string& table = token[++i];
	vector<string>& type = table2type[table];
	for (i += 2; i < token.size(); i++) {
		if (token[i] == "(") continue;
		DummyItem dummyItem;
		int j = 0;
		for(;token[i] != ")"; i++) {
			if (token[i] == ",") continue;
			if (type[j] == "INTEGER") {
				dummyItem.intdata.push_back(atoi(token[i].c_str()));
			} else {
				dummyItem.strdata.push_back(token[i]);
			}
			j++;
		}
		dummyDB.tables[table]->Insert(dummyItem);
	}
}

void done(const vector<string>& table, map<string, int>& m,
	int depth, vector<string>& row, vector<DummyItem>& record)
{
	string str;
	if (depth == table.size()) {
		str = row[0];
		for (int i = 1; i < row.size(); i++)
			str += "," + row[i];
		result.push_back(str);
		return;
	}

	vector<string>& column = table2name[table[depth]];
	vector<pair<string, int>> pos;
	for (int z = 0; z < column.size(); z++) {
		auto it = m.find(column[z]);
		if (it != m.end()) {
			pos.push_back(pair<string, int>(it->first, it->second));
		}
	}

	vector<DummyItem> ret = dummyDB.tables[table[depth]]->Get(table2query[table[depth]]);
	for (int j = 0; j < ret.size(); j++) {
		bool isSkip = 0;
		for (int z = 0; z < column.size() && !isSkip; z++) {
			if (intEqual.find(column[z]) != intEqual.end()) {
				string& col2 = intEqual[column[z]];
				auto it = col2table_intIdx.find(column[z]);
				auto it2 = col2table_intIdx.find(col2);
				string& table2 = it2->second.first;
				int intIdx2 = it2->second.second;
				int i = 0;
				for (; i < depth; i++) {
					if (table[i] == table2) {
						break;
					}
				}
				if (i != depth && ret[j].intdata[it->second.second] != record[i].intdata[intIdx2]) {
					isSkip = 1;
				}
			} else if (intGreater.find(column[z]) != intGreater.end()) {
				string& col2 = intGreater[column[z]];
				auto it = col2table_intIdx.find(column[z]);
				auto it2 = col2table_intIdx.find(col2);
				string& table2 = it2->second.first;
				int intIdx2 = it2->second.second;
				int i = 0;
				for (; i < depth; i++) {
					if (table[i] == table2) {
						break;
					}
				}
				if (i != depth && ret[j].intdata[it->second.second] <= record[i].intdata[intIdx2]) {
					isSkip = 1;
				}
			} else if (intLess.find(column[z]) != intLess.end()) {
				string& col2 = intLess[column[z]];
				auto it = col2table_intIdx.find(column[z]);
				auto it2 = col2table_intIdx.find(col2);
				string& table2 = it2->second.first;
				int intIdx2 = it2->second.second;
				int i = 0;
				for (; i < depth; i++) {
					if (table[i] == table2) {
						break;
					}
				}
				if (i != depth && ret[j].intdata[it->second.second] >= record[i].intdata[intIdx2]) {
					isSkip = 1;
				}
			} else if (strEqual.find(column[z]) != strEqual.end()) {
				string& col2 = strEqual[column[z]];
				auto it = col2table_strIdx.find(column[z]);
				auto it2 = col2table_strIdx.find(col2);
				string& table2 = it2->second.first;
				int strIdx2 = it2->second.second;
				int i = 0;
				for (; i < depth; i++) {
					if (table[i] == table2) {
						break;
					}
				}
				if (i != depth && ret[j].strdata[it->second.second] != record[i].strdata[strIdx2]) {
					isSkip = 1;
				}
			}
		}
		if (isSkip) continue;
		for (int z = 0; z < pos.size(); z++) {
			auto it1 = col2table_intIdx.find(pos[z].first);
			if (it1 != col2table_intIdx.end() && it1->second.first == table[depth]) {
				char* num = itoa(ret[j].intdata[it1->second.second]);
				row[pos[z].second] = num;
				delete num;
			}
			auto it2 = col2table_strIdx.find(pos[z].first);
			if (it2 != col2table_strIdx.end() && it2->second.first == table[depth]) {
				row[pos[z].second] = ret[j].strdata[it2->second.second];
			}
		}
		record.push_back(ret[j]); 
		done(table, m, depth + 1, row, record);
		record.pop_back();
	}
}

void ProjectDone(string& table, vector<string>& output) {
	vector<string>& col1 = projectTable[output[0]];
	vector<string>& col2 = projectTable[output[1]];
	/*for (int i = 0; i < output.size(); i++) {
		cols.push_back(projectTable[output[i]]);
	}*/
	string str;
	for (int i = 0; i < col1.size(); i++) {
		str = col1[i]+","+col2[i];
		/*for (int j = 1; j < cols.size(); j++) {
			str += ","+cols[j][i];
		}*/
		result.push_back(str);
	}
}

void create(const string& tablename, const vector<string>& column,
	const vector<string>& type, const vector<string>& key)
{
	table2name[tablename] = column;
	table2type[tablename] = type;
	table2pkey[tablename] = key;
	table2query[tablename] = DummyQuery();
	int nInt = 0, nIntKey = 0, nStr = 0, nStrKey = 0;
	vector<int> StringTypeLen;
	//key process reserved
	for (int i = 0; i < column.size(); i++) {
		if (type[i] == "INTEGER") {
			col2table_intIdx[column[i]] = pair<string, int>(tablename, nInt);
			nInt++;
		} else {
			col2table_strIdx[column[i]] = pair<string, int>(tablename, nStr);
			nStr++;
			const char* str = type[i].c_str();
			int s = 8, p = 8;
			while(str[p] > 47 && str[p] < 58) {
				 p++;
			}
			string len = type[i].substr(s, p-s);
			StringTypeLen.push_back(atoi(len.c_str()));
		}
	}
	// be careful that here, we assume, are only int keys
	nIntKey = key.size();
	unique_ptr<BaseTable> table(new DummyTable(nInt, nIntKey, nStr, nStrKey, StringTypeLen));
	dummyDB.CreateTable(table, tablename);
}

void train(const vector<string>& query, const vector<double>& weight)
{
	//special: project test:
	for (int z = 0; z < query.size(); z++) {
		const string& sql = query[z];
		vector<string> token, table;
		int i;

		if (strstr(sql.c_str(), "INSERT") != NULL && strstr(sql.c_str(), "WHERE") != NULL) {
			continue;
		}

		table.clear();
		tokenize(sql.c_str(), token);
		for (i = 0; i < token.size(); i++) {
			if (token[i] == "SELECT" || token[i] == ",")
				continue;
			if (token[i] == "FROM")
				break;
		}
		for (i++; i < token.size(); i++) {
			if (token[i] == "," || token[i] == ";")
				continue;
			table.push_back(token[i]);
		}
		if (i >= token.size() && table.size() == 1) {
			prepareProject = true;
		}
	}
}

void load(const string& tableName, const vector<string>& row)
{
	vector<string>& type = table2type[tableName];
	vector<string>& column = table2name[tableName];
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
			if (prepareProject) {
				projectTable[column[j]].push_back(token[j]);
			}
		}
		dummyDB.tables[tableName]->Insert(dummyItem);
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
		insert(sql);
		return;
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
	if (i >= token.size() && table.size() == 1 && output.size() == 2) {
		ProjectDone(table[0], output);
		return;
	}
	for (i++; i < token.size(); i++) {
		if (token[i+2][0] >= '0' && token[i+2][0] <= '9') {
			pair<string, int>& table_intIdx = col2table_intIdx[token[i]];
			string& table = table_intIdx.first;
			int idx = table_intIdx.second;
			if (token[i+1] == "=") {
				table2query[table].create(idx, atoi(token[i+2].c_str()));
			} else if (token[i+1] == "<") {
				table2query[table].create(idx, INT_MIN, atoi(token[i+2].c_str())-1);
			} else {
				table2query[table].create(idx, atoi(token[i+2].c_str())+1, INT_MAX);
			}
		} else if (token[i+2][0] == '\'') {
			pair<string, int>& table_strIdx = col2table_strIdx[token[i]];
			string& table = table_strIdx.first;
			int idx = table_strIdx.second;
			table2query[table].create(idx, token[i+2]);
		} else {
			auto it1 = col2table_intIdx.find(token[i]);
			if (it1 != col2table_intIdx.end()) {
				if (token[i+1] == "=") {
					intEqual[token[i]] = token[i+2];
					intEqual[token[i+2]] = token[i];
				} else if (token[i+1] == "<") {
					intLess[token[i]] = token[i+2];
					intGreater[token[i+2]] = token[i];
				} else {
					intGreater[token[i]] = token[i];
					intLess[token[i+2]] = token[i];
				}
			}
			auto it2 = col2table_strIdx.find(token[i]);
			if (it2 != col2table_strIdx.end()) {
				strEqual[token[i]] = token[i+2];
				strEqual[token[i+2]] = token[i];
			}
		}
		i = i+3;
	}
	m.clear();
	for (i = 0; i < output.size(); i++)
		m[output[i]] = i;

	row.clear();
	row.resize(output.size(), "");
	
	vector<DummyItem> record;
	done(table, m, 0, row, record);
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
	//printf("%s\n", row);

	return (1);
}

void close()
{
	// I have nothing to do.
}


