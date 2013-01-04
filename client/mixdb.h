#include "dummydb.h"

class MixTable : public BaseTable
{
	vector<unique_ptr<BaseTable>> tables;
public:
	MixTable(int nInt, int nIntKey, int nStr, int nStrKey, vector<int>& StringTypeLen) :
		BaseTable(nInt, nIntKey, nStr, nStrKey, StringTypeLen)
	{
	}

	void NewTable(BaseTable *table)
	{
		tables.emplace_back(table);
	}
	virtual bool Insert(DummyItem &item)  override;
	virtual void UpdateKey() override;
	virtual const DummyItem GetData(int index) const override;

	virtual const int GetDataSize() const override;
	
	virtual const int CountIntKey(int idx, int key) override;
	virtual const int CountIntKeyRange(int idx, int low, int high)  override;
	virtual const int CountStrKey(int idx, string str) override;

	class MixCursor : public Cursor
	{
	private:
		vector<unique_ptr<Cursor>> cursors;
	public:
		MixCursor(DummyQuery q = DummyQuery()) : Cursor(q) {}
		void add_cursor(unique_ptr<Cursor> &&cursor)
		{
			cursors.push_back(move(cursor));
		}
		virtual DummyItem NextItem() override
		{
			while (!cursors.empty() && cursors.back()->Empty())
				cursors.pop_back();
			assert(!cursors.empty());
			return cursors.back()->getdata();
		}
		virtual bool isEmpty() override
		{
			while (!cursors.empty() && cursors.back()->Empty())
				cursors.pop_back();
			return cursors.empty() || cursors.back()->Empty();
		}
	};

	virtual unique_ptr<Cursor> cursor(DummyQuery q = DummyQuery()) override;
	virtual unique_ptr<Cursor> cursor(int idx, int low, int high, DummyQuery q = DummyQuery()) override;
	virtual unique_ptr<Cursor> cursor(int idx, int intkey, DummyQuery q = DummyQuery()) override;
	virtual unique_ptr<Cursor> cursor(int idx, string str, DummyQuery q = DummyQuery()) override;
};
