#pragma once

#include <cassert>
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
#include <mutex>

#include <iostream>

#include "defs.h"

#ifndef WINDOWS

#include <unistd.h>

#endif

#include "utils.h"
#include "dummydb.h"

#ifdef USE_DB_CXX

#include "berkeleydb.h"

#endif

