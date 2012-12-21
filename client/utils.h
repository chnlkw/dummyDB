#include "includes.h"

template <class T>
std::string tostring(T data)
{
	std::stringstream ss;
	ss << data;
	return ss.str();
}
