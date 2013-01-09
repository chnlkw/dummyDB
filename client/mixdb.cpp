#include "mixdb.h"

bool MixTable::Insert(DummyItem &item)
{
	return tables.back()->Insert(item);
}

void MixTable::UpdateKey()
{
	for (auto &table : tables)
		table->UpdateKey();
}

const DummyItem MixTable::GetData(int index) const
{
	for (auto &table : tables)
	{
		auto count = table->GetDataSize();
		if (index < count)
			return table->GetData(index);
		index -= count;
	}
	return DummyItem();
}

const int MixTable::GetDataSize() const
{
	int size = 0;
	for (auto &table : tables)
		size += table->GetDataSize();
	return size;
}


typedef unique_ptr<BaseTable::Cursor> CursorPointer;

CursorPointer MixTable::cursor(DummyQuery q)
{
	auto pcursor = new MixCursor(q);
	for (auto &table : tables)
		pcursor->add_cursor(table->cursor(q));
	return CursorPointer(pcursor);
}

CursorPointer MixTable::cursor(int idx, int low, int high, DummyQuery q)
{
	auto pcursor = new MixCursor(q);
	for (auto &table : tables)
		pcursor->add_cursor(table->cursor(idx, low, high, q));
	return CursorPointer(pcursor);
}

CursorPointer MixTable::cursor(int idx, int intkey, DummyQuery q)
{
	auto pcursor = new MixCursor(q);
	for (auto &table : tables)
		pcursor->add_cursor(table->cursor(idx, intkey, q));
	return CursorPointer(pcursor);
}

CursorPointer MixTable::cursor(int idx, string strkey, DummyQuery q)
{
	auto pcursor = new MixCursor(q);
	for (auto &table : tables)
		pcursor->add_cursor(table->cursor(idx, strkey, q));
	return CursorPointer(pcursor);
}
