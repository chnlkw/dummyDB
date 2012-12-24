#include "dummydb.h"
#include "utils.h"
#include "berkeleydb.h"

using namespace std;

map<string, vector<string> > table2name;
map<string, vector<string> > table2type;
map<string, vector<string> > table2pkey;
map<string, pair<string, int>> col2table_intIdx;
map<string, pair<string, int>> col2table_strIdx;
map<string, DummyQuery> table2query;
map<string, int> col2index;
vector<vector<string>> colData;
vector<string> result;
BaseDB dummyDB;
bool prepareProject = false;

bool compareTable(string table1, string table2) {
	return dummyDB.tables[table1]->GetDataSize() < dummyDB.tables[table2]->GetDataSize();
}

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
	utils::tokenize(sql.c_str(), token);
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

	DummyQuery q = table2query[table[depth]];
	unique_ptr<BaseTable::Cursor> ret;
	for (auto it = q.colIntEqual.begin(); it != q.colIntEqual.end(); it++) {
		q.create(it->first, record[it->second.first].intdata[it->second.second]);
	}
	for (auto it = q.colIntLess.begin(); it != q.colIntLess.end(); it++) {
		q.create(it->first, INT_MIN, record[it->second.first].intdata[it->second.second]-1);
	}
	for (auto it = q.colIntGreater.begin(); it != q.colIntGreater.end(); it++) {
		q.create(it->first, record[it->second.first].intdata[it->second.second]+1, INT_MAX);
	}
	for (auto it = q.colStrEqual.begin(); it != q.colStrEqual.end(); it++) {
		q.create(it->first, record[it->second.first].strdata[it->second.second]);
	}
	if (q.intequal.size() == 0 && q.strequal.size() == 0 && q.intrange.size() == 0) {
		ret = dummyDB.tables[table[depth]]->cursor();
	} else {
		int min = INT_MAX;
		int count = 0;
		int rank;
		for (auto it = q.intrange.begin(); it != q.intrange.end(); it++) {
			const int temp = dummyDB.tables[table[depth]]->CountIntKeyRange(it->first, it->second.low, it->second.high);
			if (temp < min) {
				rank = count;
				min = temp;
			}
			count++;
		}
		for (auto it = q.intequal.begin(); it != q.intequal.end(); it++) {
			const int temp = dummyDB.tables[table[depth]]->CountIntKey(it->first, it->second);
			if (temp < min) {
				rank = count;
				min = temp;
			}
			count++;
		}
		for (auto it = q.strequal.begin(); it != q.strequal.end(); it++) {
			const int temp = dummyDB.tables[table[depth]]->CountStrKey(it->first, it->second);
			if (temp < min) {
				rank = count;
				min = temp;
			}
			count++;
		}
		if (rank < q.intrange.size()) {
			auto it = q.intrange.begin();
			for (int i = 0; i < rank; i++, it++) {}
			ret = dummyDB.tables[table[depth]]->cursor(it->first, it->second.low, it->second.high, q);
		} else if (rank < q.intequal.size()+q.intrange.size()) {
			auto it = q.intequal.begin();
			for (int i = q.intrange.size(); i < rank; i++, it++) {}
			ret = dummyDB.tables[table[depth]]->cursor(it->first, it->second, q);
		} else {
			auto it = q.strequal.begin();
			for (int i = q.intrange.size()+q.intequal.size(); i < rank; i++, it++) {}
			ret = dummyDB.tables[table[depth]]->cursor(it->first, it->second, q);
		}
	}

	for (ret->Init(); !ret->Empty() ; ret->Next()) {
		auto data = ret->getdata();
		for (int z = 0; z < pos.size(); z++) {
			auto it1 = col2table_intIdx.find(pos[z].first);
			if (it1 != col2table_intIdx.end() && it1->second.first == table[depth]) {
				row[pos[z].second] = to_string(data.intdata[it1->second.second]);
			}
			auto it2 = col2table_strIdx.find(pos[z].first);
			if (it2 != col2table_strIdx.end() && it2->second.first == table[depth]) {
				row[pos[z].second] = data.strdata[it2->second.second];
			}
		}
		record.push_back(data);
		done(table, m, depth + 1, row, record);
		record.pop_back();
		
	}
}

void ProjectDone(string& table, vector<string>& output) {
	int colSize = output.size();
	int* index = new int[colSize];
	for (int i = 0; i < colSize; i++) {
		index[i] = col2index[output[i]];
	}
	int resSize = colData[index[0]].size();
	string str;
	for (int i = 0; i < resSize; i++) {
		str = colData[index[0]][i];
		for (int j = 1; j < colSize; j++) {
			str += ","+colData[index[j]][i];
		}
		result.push_back(str);
	}
	delete index;
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
		col2index[column[i]] = colData.size();
		colData.push_back(vector<string>());
	}
	// be careful that here, we assume, are only int keys
	nIntKey = nInt;
	nStrKey = nStr;
#ifdef USE_DB_CXX
	unique_ptr<BaseTable> table(new BDBTable(tablename, nInt, nIntKey, nStr, nStrKey, StringTypeLen));
#else
	unique_ptr<BaseTable> table(new DummyTable(nInt, nIntKey, nStr, nStrKey, StringTypeLen));
#endif
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
		utils::tokenize(sql.c_str(), token);
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
			//prepareProject = true;
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
		utils::split_csv(row[i].c_str(), token);
		//cout << tableName << " " << type.size() << " " << token.size() << endl;
		//cout << row[i].c_str() << endl;
		//cout << "i:"<< i << " ";
		for (int j = 0; j < token.size(); j++) {
			if (type[j] == "INTEGER") {
			  //cout << atoi(token[j].c_str()) <<" ";
				dummyItem.intdata.push_back(atoi(token[j].c_str()));
			} else {
				dummyItem.strdata.push_back(token[j]);
			}
			if (prepareProject) {
				int idx = col2index[column[j]];
				colData[idx].push_back(token[j]);
			}
		}
		//cout << endl;
		dummyDB.tables[tableName]->Insert(dummyItem);
	}
}

void preprocess()
{
	// I am too clever; I don't need it.
}

void createQuery(vector<string>& table, vector<string>& token, int& i) {
  for (i++; i < token.size(); i++) {
		if (token[i+2][0] >= '0' && token[i+2][0] <= '9') {
			pair<string, int>& table_intIdx = col2table_intIdx[token[i]];
			string& tableName = table_intIdx.first;
			int idx = table_intIdx.second;
			if (token[i+1] == "=") {
				table2query[tableName].create(idx, atoi(token[i+2].c_str()));
			} else if (token[i+1] == "<") {
				table2query[tableName].create(idx, INT_MIN, atoi(token[i+2].c_str())-1);
			} else {
				table2query[tableName].create(idx, atoi(token[i+2].c_str())+1, INT_MAX);
			}
		} else if (token[i+2][0] == '\'') {
			pair<string, int>& table_strIdx = col2table_strIdx[token[i]];
			string& tableName = table_strIdx.first;
			int idx = table_strIdx.second;
			table2query[tableName].create(idx, token[i+2]);
		} else {
			auto it1 = col2table_intIdx.find(token[i]);
			if (it1 != col2table_intIdx.end()) {
				auto& table_intIdx = col2table_intIdx[token[i]];
				string& tableName = table_intIdx.first;
				int intIdx = table_intIdx.second;
				auto& table_intIdx2 = col2table_intIdx[token[i+2]];
				string& tableName2 = table_intIdx2.first;
				int intIdx2 = table_intIdx2.second;
				int j = 0;
				for (; j < table.size(); j++) {
					if (table[j] == tableName || table[j] == tableName2) break;
				}
				if (table[j] == tableName2) {
					if (token[i+1] == "=") {
						table2query[tableName].create(intIdx, j, intIdx2, 0);
					} else if (token[i+1] == "<") {
						table2query[tableName].create(intIdx, j, intIdx2, 1);
					} else {
						table2query[tableName].create(intIdx, j, intIdx2, 2);
					}
				} else {
				  if (token[i+1] == "=") {
						table2query[tableName2].create(intIdx2, j, intIdx, 0);
					} else if (token[i+1] == ">") {
						table2query[tableName2].create(intIdx2, j, intIdx, 1);
					} else {
						table2query[tableName2].create(intIdx2, j, intIdx, 2);
					}
				}
			}
			auto it2 = col2table_strIdx.find(token[i]);
			if (it2 != col2table_strIdx.end()) {
				auto& table_strIdx = col2table_strIdx[token[i]];
				string& tableName = table_strIdx.first;
				int strIdx = table_strIdx.second;
				auto& table_strIdx2 = col2table_strIdx[token[i+2]];
				string& tableName2 = table_strIdx2.first;
				int strIdx2 = table_strIdx2.second;
				int j = 0;
				for (; j < table.size(); j++) {
					if (table[j] == tableName || table[j] == tableName2) break;
				}
				if (table[j] == tableName2) {
					table2query[tableName].create(strIdx, j, strIdx2, 3);
				} else {
				  table2query[tableName2].create(strIdx2, j, strIdx, 3);
				}
			}
		}
		i = i+3;
	}
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
	utils::tokenize(sql.c_str(), token);
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
	sort(table.begin(), table.end(), compareTable);
	if (i >= token.size() && table.size() == 1 && output.size() == 2) {
		//ProjectDone(table[0], output);
		//return;
	}
	createQuery(table, token, i);
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
	
#ifdef PRINT_ROW
	printf("%s\n", row);
#endif

	return (1);
}

void close()
{
	// I have nothing to do.
}


