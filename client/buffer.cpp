#include "buffer.h"

char name[]="123 ";

std::shared_ptr<Cache<IndexBlockSize>> pcache(new Cache<IndexBlockSize> (CACHE_SIZE));
char EmptyBlock[IndexBlockSize];

int main11()
{
	std::set<int> s;
	s.insert(1);
	s.insert(2);
	s.insert(3);
	printf("%d\n", *s.begin());
	s.erase(s.begin());
	printf("%d\n", *s.begin());
	std::cerr << "test buffer\n";
	Buffer<IndexBlockShift> b("data/buf.bin", pcache);
	b.Append(name, sizeof(name));
	b.Append(name, sizeof(name));
	b.Append(name, sizeof(name));
	b.Append(name, sizeof(name));
	for (int i = 0; i < 10; i++)
	{
		char *s = b.Get<char>(i);
		printf("%s\n", s);
	}
	return 0;
}

template <uint32_t BlockShift>
class A
{
public:
	template< class T>
	class B
	{
		T s;
	};
};

template <int i>
void aaaa()
{
	A<2>::B<int> ab();
	typename Buffer<i>::template Test<int> t;
}
