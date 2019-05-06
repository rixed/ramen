#ifndef CONF_H_190504
#define CONF_H_190504
#include <string>

class KWidget;

namespace conf {

extern std::string my_uid;
extern std::string my_errors;

void registerWidget(std::string const &key, KWidget *);
void unregisterWidget(std::string const &key, KWidget *);

void askLock(std::string const &key);
void askUnlock(std::string const &key);

};

#endif
