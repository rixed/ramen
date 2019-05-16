#ifndef PROGRAMINFOBOX_H_190516
#define PROGRAMINFOBOX_H_190516
#include <QWidget>

class ProgramItem;
class QLabel;

class ProgramInfoBox : public QWidget
{
  Q_OBJECT

  ProgramItem const *p;

public:
  ProgramInfoBox(ProgramItem const *, QWidget *parent = nullptr);
};

#endif
