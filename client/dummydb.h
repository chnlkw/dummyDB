#include "includes.h"
#include "defs.h"

#ifndef _DUMMY_DB_H_

#define _DUMMY_DB_H_

using namespace std;

struct DummyItem
{
	vector<int> intdata;
	vector<string> strdata;
};

struct DummyQuery
{
	struct IntRangeQuery
	{
		int low, high; // close interval [low, high]
	};
	map<int, IntRangeQuery> intrange;
	map<int, int> intequal;
	map<int, string> strequal;
public:
	bool match(DummyItem &item)
	{
		for( auto &it : intequal)
		{
			if (item.intdata[it.first] != it.second)
				return false;
		}
		for( auto &it : intrange)
		{
			if (item.intdata[it.first] < it.second.low || item.intdata[it.first] > it.second.high)
				return false;
		}
		for( auto &it : strequal)
		{
			if (item.strdata[it.first] != it.second)
				return false;
		}
		return true;
	}
};

class DummyTable
{
private:
	int nInt, nIntKey;
	int nStr, nStrKey;
	vector<int> StringTypeLen;
	vector<DummyItem> data;
public:
	DummyTable(int nInt, int nIntKey, int nStr, int nStrKey, vector<int> StringTypeLen) :
		nInt(nInt), nIntKey(nIntKey), nStr(nStr), nStrKey(nStrKey), StringTypeLen(StringTypeLen)
	{
	}
	bool Insert(DummyItem item)
	{
		data.push_back(item);
		return true;
	}
	vector<DummyItem> Get(DummyQuery q)
	{
		vector<DummyItem> ret;
		for (auto it = data.begin(); it != data.end(); it++)
		{
			if (q.match(*it))
				ret.push_back(*it);
		}
		return ret;
	}
};

class DummyDB
{
private:
	int nTable;
	vector<DummyTable> tables;
public:
	DummyDB() : nTable(0)
	{
	}
	int CreateTable(DummyTable table)
	{
		tables.push_back(table);
		return nTable++;
	}
};

#endif
