#include "rec_shared_mutex.h"

thread_local int rec_shared_mutex::count = 0;

