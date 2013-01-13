#pragma once

#include <cassert>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <unordered_map>
#include <string>
#include <vector>
#include <climits>
#include <sstream>
#include <utility>
#include <algorithm>
#include <functional>
#include <thread>
#include <deque>
#include <mutex>
#include <iostream>
#include <fstream>
#include <cstdint>
#include <set>

#include "defs.h"

#ifndef WINDOWS

#include <unistd.h>
#include <sys/stat.h>

#endif

#include "utils.h"

#include <fcntl.h>
