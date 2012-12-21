#include "berkeleydb.h"


vector<DummyItem> BDBTable::Get()
{
	vector<DummyItem> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key, data;
	int ret;
	while ((ret = cursorp->get(&key, &data, DB_NEXT)) == 0)
	{
		DummyItem item = to_item(data, nInt, nStr);
		answer.push_back(item);
	}
	return answer;
}

vector<DummyItem> BDBTable::Get(DummyQuery& q)
{
	vector<DummyItem> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key, data;
	int ret;
	while ((ret = cursorp->get(&key, &data, DB_NEXT)) == 0)
	{
		int k = *(int*)key.get_data();
		DummyItem item = to_item(data, nInt, nStr);
		answer.push_back(item);
	}
	cursorp->close();
	return answer;
}

vector<DummyItem> BDBTable::GetIntKey(int idx, int key)
{
	vector<DummyItem> ret;

	return ret;
}

vector<DummyItem> BDBTable::GetIntKey(int idx, int key, DummyQuery& q)
{
	vector<DummyItem> ret;

	return ret;
}

vector<DummyItem> BDBTable::GetIntKeyRange(int idx, int low, int high)
{
	vector<DummyItem> ret;

	return ret;
}

vector<DummyItem> BDBTable::GetIntKeyRange(int idx, int low, int high, DummyQuery& q)
{
	vector<DummyItem> ret;

	return ret;
}


vector<DummyItem> BDBTable::GetStrKey(int idx, string str)
{
	vector<DummyItem> ret;

	return ret;
}

vector<DummyItem> BDBTable::GetStrKey(int idx, string str, DummyQuery& q)
{
	vector<DummyItem> ret;

	return ret;
}
