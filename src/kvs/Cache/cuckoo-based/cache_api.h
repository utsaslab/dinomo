#ifndef CACHE_API_H
#define CACHE_API_H
#include "string-key.h"
#include "scalable-cache.h"

void Init(size_t cache_size, int num_threads);
uint64_t Warmup(char *key, size_t key_len, char *val, size_t val_len);
uint64_t Put(char *key, size_t key_len, char *val, size_t val_len);
int Get(char *key, size_t key_len, char *val, size_t &val_len, uint64_t version);
int Del(char *key, size_t key_len, uint64_t version);
void Final();

#endif
