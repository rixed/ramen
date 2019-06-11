#ifndef MISC_H_190603
#define MISC_H_190603

#define SIZEOF_ARRAY(x) (sizeof(x) / sizeof(*(x)))

typedef unsigned __int128 uint128_t;
typedef __int128 int128_t;

bool startsWith(std::string const &, std::string const &);
bool endsWith(std::string const &, std::string const &);

#endif