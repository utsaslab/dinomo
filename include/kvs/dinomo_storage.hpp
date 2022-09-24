#ifndef DINOMO_STORAGE_H_
#define DINOMO_STORAGE_H_

#ifdef SHARED_NOTHING
#include "kvs/kvs_handlers.hpp"
#endif

int run_server (int num_concurr_msgs);

#endif
