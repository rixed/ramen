#ifndef MENU_H_190731
#define MENU_H_190731
#include <QMenuBar>

extern QMenuBar *globalMenuBar;

class GraphModel;

void setupGlobalMenu(GraphModel *, bool with_beta_features);

#endif
