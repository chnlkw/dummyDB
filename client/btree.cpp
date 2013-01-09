
#include "btree.h"
BPlusTree<64, int, size_t> btree;

using namespace std;

int main1()
{
	//btree.printblk();
	vector<pair<int,size_t>> a(10000);
	for (int i = 0; i < 10000; i++)
	{
		a[i].first = i;
		a[i].second = rand();
	}
	random_shuffle(a.begin(), a.end());
	for (auto &p : a)
	{
		btree.Insert(p.first, p.second);
		//btree.printblk();
	}
	random_shuffle(a.begin(), a.end());
	for (auto &p : a)
	{
		size_t v;
		btree.GetValue(p.first, v);
		assert(v == p.second);
		
	}
	auto it = btree.LowerBound(0);
	while (!it.isEnd())
	{
		cout << "iter " << it.Key() << ' ' << it.Val() << endl;
		it.Next();
	}
	system("pause");
	return 0;
}
