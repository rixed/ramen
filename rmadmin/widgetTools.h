#ifndef WINDGETTOOLS_H_190604
#define WINDGETTOOLS_H_190604
#include <QString>

class QTabWidget;

bool tryFocusTab(QTabWidget *, QString const &);
void focusLastTab(QTabWidget *);

#endif
