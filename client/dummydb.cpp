#include "dummydb.h"

vector<DummyItem> DummyTable::Get()
{
	return data;
}

vector<DummyItem> DummyTable::Get(DummyQuery& q)
{
	vector<DummyItem> ret;

	for (auto it = data.begin(); it != data.end(); it++)
	{
		if (q.match(*it))
			ret.push_back(*it);
	}
	return ret;
}

vector<DummyItem> DummyTable::GetIntKey(int idx, int key)
{
	vector<DummyItem> ret;
	for (auto it = IntKey[idx].lower_bound(key); it != IntKey[idx].upper_bound(key); it++)
	{
			ret.push_back(it->second);
	}
	return ret;
}

vector<DummyItem> DummyTable::GetIntKey(int idx, int key, DummyQuery& q)
{
	vector<DummyItem> ret;
	for (auto it = IntKey[idx].lower_bound(key); it != IntKey[idx].upper_bound(key); it++)
	{
		if (q.match(it->second))
			ret.push_back(it->second);
	}
	return ret;
}

vector<DummyItem> DummyTable::GetIntKeyRange(int idx, int low, int high)
{
	vector<DummyItem> ret;
	for (auto it = IntKey[idx].lower_bound(low); it != IntKey[idx].upper_bound(high); it++)
	{
			ret.push_back(it->second);
	}
	return ret;
}

vector<DummyItem> DummyTable::GetIntKeyRange(int idx, int low, int high, DummyQuery& q)
{
	vector<DummyItem> ret;
	for (auto it = IntKey[idx].lower_bound(low); it != IntKey[idx].upper_bound(high); it++)
	{
		if (q.match(it->second))
			ret.push_back(it->second);
	}
	return ret;
}


vector<DummyItem> DummyTable::GetStrKey(int idx, string str)
{
	vector<DummyItem> ret;
	auto range = StrKey[idx].equal_range(str);
	for (auto it = range.first; it != range.second; it++)
	{
			ret.push_back(it->second);
	}
	return ret;
}

vector<DummyItem> DummyTable::GetStrKey(int idx, string str, DummyQuery& q)
{
	vector<DummyItem> ret;
	auto range = StrKey[idx].equal_range(str);
	for (auto it = range.first; it != range.second; it++)
	{
		if (q.match(it->second))
			ret.push_back(it->second);
	}
	return ret;
}
