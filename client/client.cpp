#include "includes.h"


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
string resultBuffer[3000];
string *w, *r;
bool prepareProject = false;
bool isOver;
BaseDB dummyDB;

bool compareTable(string table1, string table2) {
	return dummyDB.tables[table1]->GetDataSize() < dummyDB.tables[table2]->GetDataSize();
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

void assembleResult(vector< pair<int, pair<int, int>> >& pos, vector<DummyItem>& record) {
#ifdef USE_THREAD
  string& str = *w;
  if (pos[0].second.first == 0) {
  	str = to_string(record[pos[0].first].intdata[pos[0].second.second]);
  } else {
  	str = record[pos[0].first].strdata[pos[0].second.second];
  }
  for (int i = 1; i < pos.size(); i++) {
  	if (pos[i].second.first == 0) {
  	  str += ","+to_string(record[pos[i].first].intdata[pos[i].second.second]);
  	} else {
  	  str += ","+record[pos[i].first].strdata[pos[i].second.second];
  	}
  }
  while(w == r-1 || w == r+2999) {
    usleep(1);
  }
  w == resultBuffer+2999? w -= 2999: w++;
#else
  string str;
  if (pos[0].second.first == 0) {
  	str = to_string(record[pos[0].first].intdata[pos[0].second.second]);
  } else {
  	str = record[pos[0].first].strdata[pos[0].second.second];
  }
  for (int i = 1; i < pos.size(); i++) {
  	if (pos[i].second.first == 0) {
  	  str += ","+to_string(record[pos[i].first].intdata[pos[i].second.second]);
  	} else {
  	  str += ","+record[pos[i].first].strdata[pos[i].second.second];
  	}
  }
  result.push_back(str);
#endif
}

DummyQuery getTempQuery(const string& tableName, vector<DummyItem>& record) {
	DummyQuery q = table2query[tableName];
	int col, seq, col2;
	for (auto it = q.colIntEqual.begin(); it != q.colIntEqual.end(); it++) {
		tie(col, seq, col2) = *it;
		q.create(col, record[seq].intdata[col2]);
	}
	for (auto it = q.colIntLess.begin(); it != q.colIntLess.end(); it++) {
		tie(col, seq, col2) = *it;
		q.create(col, INT_MIN, record[seq].intdata[col2]-1);
	}
	for (auto it = q.colIntGreater.begin(); it != q.colIntGreater.end(); it++) {
		tie(col, seq, col2) = *it;
		q.create(col, record[seq].intdata[col2]+1, INT_MAX);
	}
	for (auto it = q.colStrEqual.begin(); it != q.colStrEqual.end(); it++) {
		tie(col, seq, col2) = *it;
		q.create(col, record[seq].strdata[col2]);
	}
	return q;
}

void done(const vector<string>& table, vector< pair<int, pair<int, int>> >& pos,
	int depth, vector<DummyItem>& record)
{
	string str;
	if (depth == table.size()) {
		assembleResult(pos, record);
		return;
	}
  
	DummyQuery q = getTempQuery(table[depth], record);
	unique_ptr<BaseTable::Cursor> ret;
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
		record.push_back(data);
		done(table, pos, depth + 1, record);
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
	for (int i = 0; i < row.size(); i++) {
		DummyItem dummyItem;
		vector<string> token;
		utils::split_csv(row[i].c_str(), token);
		for (int j = 0; j < token.size(); j++) {
			if (type[j] == "INTEGER") {
				dummyItem.intdata.push_back(atoi(token[j].c_str()));
			} else {
				dummyItem.strdata.push_back(token[j]);
			}
			if (prepareProject) {
				int idx = col2index[column[j]];
				colData[idx].push_back(token[j]);
			}
		}
		dummyDB.tables[tableName]->Insert(dummyItem);
	}
}

void preprocess()
{
	// I am too clever; I don't need it.
}

void createQuery(vector<string>& table, vector<string>& token, int& i, map<string, int>& table_pos) {
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
				int seq = table_pos[tableName];
				auto& table_intIdx2 = col2table_intIdx[token[i+2]];
				string& tableName2 = table_intIdx2.first;
				int intIdx2 = table_intIdx2.second;
				int seq2 = table_pos[tableName2];
				if (seq2 < seq) {
					if (token[i+1] == "=") {
						table2query[tableName].create(intIdx, seq2, intIdx2, 0);
					} else if (token[i+1] == "<") {
						table2query[tableName].create(intIdx, seq2, intIdx2, 1);
					} else {
						table2query[tableName].create(intIdx, seq2, intIdx2, 2);
					}
				} else {
				  if (token[i+1] == "=") {
						table2query[tableName2].create(intIdx2, seq, intIdx, 0);
					} else if (token[i+1] == ">") {
						table2query[tableName2].create(intIdx2, seq, intIdx, 1);
					} else {
						table2query[tableName2].create(intIdx2, seq, intIdx, 2);
					}
				}
			}
			auto it2 = col2table_strIdx.find(token[i]);
			if (it2 != col2table_strIdx.end()) {
				auto& table_strIdx = col2table_strIdx[token[i]];
				string& tableName = table_strIdx.first;
				int strIdx = table_strIdx.second;
				int seq = table_pos[tableName];
				auto& table_strIdx2 = col2table_strIdx[token[i+2]];
				string& tableName2 = table_strIdx2.first;
				int strIdx2 = table_strIdx2.second;
				int seq2 = table_pos[tableName2];
				if (seq2 < seq) {
					table2query[tableName].create(strIdx, seq2, strIdx2, 3);
				} else {
				  table2query[tableName2].create(strIdx2, seq, strIdx, 3);
				}
			}
		}
		i = i+3;
	}
}

void clearQuery(vector<string>& table) {
  for (int i = 0; i < table.size(); i++) {
	  table2query[table[i]].clear();
	}
}

void executeThread(vector<string> table, vector< pair<int, pair<int, int>> > pos) {
	vector<DummyItem> record;
	done(table, pos, 0, record);
	clearQuery(table);
	isOver = true;
}

void execute(const string& sql)
{
	vector<string> token, output, table;
	map<string, int> table_pos;
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
	for (int z = 0; z < table.size(); z++) {
		table_pos[table[z]] = z;
	}
	if (i >= token.size() && table.size() == 1 && output.size() == 2) {
		//ProjectDone(table[0], output);
		//return;
	}
	createQuery(table, token, i, table_pos);
	
	vector< pair<int, pair<int, int>> > pos;
	for (i = 0; i < output.size(); i++) {
		auto it1 = col2table_intIdx.find(output[i]);
		if (it1 != col2table_intIdx.end()) {
			auto& table_intIdx = col2table_intIdx[output[i]];
			string& tableName = table_intIdx.first;
			int intIdx = table_intIdx.second;
			int seq = table_pos[tableName];
			pos.push_back( pair<int, pair<int, int>>(seq, pair<int, int>(0, intIdx)) );
		}
		auto it2 = col2table_strIdx.find(output[i]);
		if (it2 != col2table_strIdx.end()) {
			auto& table_strIdx = col2table_strIdx[output[i]];
			string& tableName = table_strIdx.first;
			int strIdx = table_strIdx.second;
			int seq = table_pos[tableName];
			pos.push_back( pair<int, pair<int, int>>(seq, pair<int, int>(1, strIdx)) );
		}
	}
#ifdef USE_THREAD
	isOver = false;
	w = r = resultBuffer;
	thread doneThread(executeThread, move(table), move(pos));
	doneThread.detach();
#else
	vector<DummyItem> record;
	done(table, pos, 0, record);
	clearQuery(table);
#endif
}

int next(char *row)
{
#ifdef USE_THREAD
	if (isOver && w == r)
    return (0);
  while(w == r && !isOver) {
    usleep(1);
  }
  if (isOver && w == r)
    return (0);
	strcpy(row, (*r).c_str());
  r == resultBuffer+2999? r -= 2999: r++;
#else
	if (result.size() == 0)
		return (0);
	strcpy(row, result.back().c_str());
	result.pop_back();
#endif
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



