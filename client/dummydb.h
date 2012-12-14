
#include "includes.h"
#include "defs.h"

using namespace std;

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

struct DummyItem
{
	vector<int> intdata;
	vector<string> strdata;
};

struct DummyQuery
{
private:
	struct IntRangeQuery
	{
		int low, high; // close interval [low, high]
	};
	map<int, IntRangeQuery> intrange;
	map<int, int> intequal;
	map<int, string> strequal;
public:
	void create(int col, int val) {
		intequal[col] = val;	
	}
	void create(int col, int low, int high) {
		IntRangeQuery *range = new IntRangeQuery;
		range->low = low;
		range->high = high;
		intrange[col] = *range;
	}
	void create(int col, string str) {
		strequal[col] = str;
	}
	bool match(DummyItem &item)
	{
		for (const auto &it : intequal)
		{
			if (item.intdata[it.first] != it.second)
				return false;
		}
		for (const auto &it : intrange)
		{
			if (item.intdata[it.first] < it.second.low || item.intdata[it.first] > it.second.high)
				return false;
		}
		for (const auto &it : strequal)
		{
			if (item.strdata[it.first] != it.second)
				return false;
		}
		return true;
	}
};

class DummyTable
{
public:
	int nInt, nIntKey;
	int nStr, nStrKey;
	vector<int> StringTypeLen;
	vector<DummyItem> data;
	vector<multimap<int, DummyItem>> IntKey;
	vector<unordered_map<string, DummyItem>> StrKey;
	map<string, int> col2intIdx;
	map<string, int> col2strIdx;
	/*DummyTable(int nInt, int nIntKey, int nStr, int nStrKey, vector<int>& StringTypeLen, map<string, int>& intCol, map<string, int>& strCol) :
		nInt(nInt), nIntKey(nIntKey), nStr(nStr), nStrKey(nStrKey), intCol(intCol), strCol(strCol), StringTypeLen(StringTypeLen),
		IntKey(nIntKey), StrKey(nStrKey)
	{
	}*/
	DummyTable(int nInt = 0, int nIntKey = 0, int nStr = 0, int nStrKey = 0) : nInt(nInt), nIntKey(nIntKey), nStr(nStr), nStrKey(nStrKey)
	{
	}
	bool Insert(DummyItem &item)
	{
		data.push_back(item);
		for (int i = 0; i < nIntKey; i++)
			IntKey[i].insert(make_pair(item.intdata[i], item));
		for (int i = 0; i < nStrKey; i++)
			StrKey[i].insert(make_pair(item.strdata[i], item));
		return true;
	}

	vector<DummyItem> Get();
	vector<DummyItem> Get(DummyQuery q);
	vector<DummyItem> GetIntKey(int idx, int key);
	vector<DummyItem> GetIntKey(int idx, int key, DummyQuery q);
	vector<DummyItem> GetIntKeyRange(int idx, int low, int high);
	vector<DummyItem> GetIntKeyRange(int idx, int low, int high, DummyQuery q);
	vector<DummyItem> GetStrKey(int idx, string str);
	vector<DummyItem> GetStrKey(int idx, string str, DummyQuery q);

	template <class It>
	class Cursor
	{
		It iter, iter_end;
	public:
		Cursor(It iter, It iter_end) :
			iter(iter), iter_end(iter_end)
		{
		}
		bool HasNext()
		{
			return iter != iter_end;
		}
		void Next()
		{
			iter++;
		}
		auto Data() -> decltype(*iter)
		{
			return *iter;
		}
	};

	auto cursor() -> Cursor<vector<DummyItem>::iterator>
	{
		return Cursor<vector<DummyItem>::iterator>(data.begin(), data.end());
	}
	auto cursor(int idx, int low, int high) -> Cursor<multimap<int, DummyItem>::iterator>
	{
		return Cursor<multimap<int, DummyItem>::iterator>(IntKey[idx].lower_bound(low), IntKey[idx].upper_bound(high));
	}
	auto cursor(int idx, int intkey) -> Cursor<multimap<int, DummyItem>::iterator>
	{
		auto range = IntKey[idx].equal_range(intkey);
		return Cursor<multimap<int, DummyItem>::iterator>(range.first, range.second);
	}
	auto cursor(int idx, string str) -> Cursor<unordered_map<string, DummyItem>::iterator>
	{
		auto range = StrKey[idx].equal_range(str);
		return Cursor<unordered_map<string, DummyItem>::iterator>(range.first, range.second);
	}

};

class DummyDB
{
public:
	int nTable;
	map<string, DummyTable> tables;
	DummyDB() : nTable(0)
	{
	}
	int CreateTable(DummyTable& table, const string& name)
	{
		tables[name] = table;
		return nTable++;
	}
};
