#include <QTabWidget>

bool tryFocusTab(QTabWidget *w, QString const &label)
{
  for (int i = 0; i < w->count(); i++) {
    if (w->tabText(i) == label) {
      w->setCurrentIndex(i);
      return true;
    }
  }
  return false;
}

void focusLastTab(QTabWidget *w)
{
  w->setCurrentIndex(w->count() - 1);
}
