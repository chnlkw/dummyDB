#include "dummydb.h"

typedef unique_ptr<BaseTable::Cursor> CursorPointer;

const DummyItem DummyTable::GetData(int index) const {
	return data[index];
}

const int DummyTable::GetDataSize() const {
	return data.size();
}
/*
const multimap<int, int>& DummyTable::GetIntKey(int index) {
	return IntKey[index];
}

const unordered_multimap<string, int>& DummyTable::GetStrKey(int index) {
	return StrKey[index];
}
*/
CursorPointer DummyTable::cursor(DummyQuery q)
{
	typedef decltype(data.begin()) It;
	auto fun = [](It it) { return *it; };
	return CursorPointer(new DummyCursor<It, decltype(fun)>(data.begin(), data.end(), move(fun), q));
}
CursorPointer DummyTable::cursor(int idx, int low, int high, DummyQuery q)
{
	typedef decltype(IntKey[idx].begin()) It;
	auto fun = [this](It it) { return data[it->second]; };
	return CursorPointer(new DummyCursor<It, decltype(fun)>(IntKey[idx].lower_bound(low), IntKey[idx].upper_bound(high), move(fun), q));
}
CursorPointer DummyTable::cursor(int idx, int intkey, DummyQuery q)
{
	typedef decltype(IntKey[idx].begin()) It;
	auto fun = [this](It it) { return data[it->second]; };
	return CursorPointer(new DummyCursor<It, decltype(fun)>(IntKey[idx].lower_bound(intkey), IntKey[idx].upper_bound(intkey), move(fun), q));
}
CursorPointer DummyTable::cursor(int idx, string strkey, DummyQuery q)
{
	typedef decltype(StrKey[idx].begin()) It;
	auto fun = [this](It it) { return data[it->second]; };
	return CursorPointer(new DummyCursor<It, decltype(fun)>(StrKey[idx].equal_range(strkey).first, StrKey[idx].equal_range(strkey).second, move(fun), q));
}
/*
vector<int> DummyTable::Get()
{
	vector<int> ret;
	for(int i = 0; i < data.size(); i++) {
		ret.push_back(i);
	}
	return ret;
}

vector<int> DummyTable::Get(DummyQuery& q)
{
	vector<int> ret;
	if (q.intequal.size() == 0 && q.strequal.size() == 0 && q.intrange.size() == 0) {
		for(int i = 0; i < data.size(); i++) {
			ret.push_back(i);
		}
		return ret;
	}	
	int min = INT_MAX;
	int count = 0;
	int rank;
	for (auto it = q.intrange.begin(); it != q.intrange.end(); it++) {
		int temp;
		if (it->second.low == INT_MIN) {
			auto it2 = IntKey[it->first].lower_bound(it->second.high);
			temp = 0;
			for(it2--; it2 != IntKey[it->first].begin(); it2--) {
				temp++;
			}
			temp++;
			if (temp < min) {
				rank = count;
				min = temp;
			}
		}
		if (it->second.high == INT_MAX) {
			auto it2 = IntKey[it->first].lower_bound(it->second.low);
			temp = 0;
			for(; it2 != IntKey[it->first].end(); it2++) {
				temp++;
			}
			if (temp < min) {
				rank = count;
				min = temp;
			}
		}
		count++;
	}
	for (auto it = q.intequal.begin(); it != q.intequal.end(); it++) {
		auto range = IntKey[it->first].equal_range(it->second);
		int temp = 0;
		for (auto it2 = range.first; it2 != range.second; it2++) {
			temp++;
		}
		if (temp < min) {
			rank = count;
			min = temp;
		}
		count++;
	}
	for (auto it = q.strequal.begin(); it != q.strequal.end(); it++) {
		auto range = StrKey[it->first].equal_range(it->second);
		int temp = 0;
		for (auto it2 = range.first; it2 != range.second; it2++) {
			temp++;
		}
		if (temp < min) {
			rank = count;
			min = temp;
		}
		count++;
	}
	if (rank < q.intrange.size()) {
		auto it = q.intrange.begin();
		for (int i = 0; i < rank; i++, it++) {}
		ret = GetIntKeyRange(it->first, it->second.low, it->second.high, q);
	} else if (rank < q.intequal.size()+q.intrange.size()) {
		auto it = q.intequal.begin();
		for (int i = q.intrange.size(); i < rank; i++, it++) {}
		ret = GetIntKey(it->first, it->second, q);
	} else {
		auto it = q.strequal.begin();
		for (int i = q.intrange.size()+q.intequal.size(); i < rank; i++, it++) {}
		ret = GetStrKey(it->first, it->second, q);
	}
	return ret;
}

vector<int> DummyTable::GetIntKey(int idx, int key)
{
	vector<int> ret;
	for (auto it = IntKey[idx].lower_bound(key); it != IntKey[idx].upper_bound(key); it++)
	{
			ret.push_back(it->second);
	}
	return ret;
}

vector<int> DummyTable::GetIntKey(int idx, int key, DummyQuery& q)
{
	vector<int> ret;
	for (auto it = IntKey[idx].lower_bound(key); it != IntKey[idx].upper_bound(key); it++)
	{
		if (q.match(data[it->second]))
			ret.push_back(it->second);
	}
	return ret;
}

vector<int> DummyTable::GetIntKeyRange(int idx, int low, int high)
{
	vector<int> ret;
	for (auto it = IntKey[idx].lower_bound(low); it != IntKey[idx].upper_bound(high); it++)
	{
			ret.push_back(it->second);
	}
	return ret;
}

vector<int> DummyTable::GetIntKeyRange(int idx, int low, int high, DummyQuery& q)
{
	vector<int> ret;
	for (auto it = IntKey[idx].lower_bound(low); it != IntKey[idx].upper_bound(high); it++)
	{
		if (q.match(data[it->second]))
			ret.push_back(it->second);
	}
	return ret;
}


vector<int> DummyTable::GetStrKey(int idx, string str)
{
	vector<int> ret;
	auto range = StrKey[idx].equal_range(str);
	for (auto it = range.first; it != range.second; it++)
	{
			ret.push_back(it->second);
	}
	return ret;
}

vector<int> DummyTable::GetStrKey(int idx, string str, DummyQuery& q)
{
	vector<int> ret;
	auto range = StrKey[idx].equal_range(str);
	for (auto it = range.first; it != range.second; it++)
	{
		if (q.match(data[it->second]))
			ret.push_back(it->second);
	}
	return ret;
}
*/
