#ifndef FUNCTIONINFOBOX_H_190516
#define FUNCTIONINFOBOX_H_190516
#include <QWidget>

class FunctionItem;
class QLabel;

class FunctionInfoBox : public QWidget
{
  Q_OBJECT

  FunctionItem const *f;

public:
  FunctionInfoBox(FunctionItem const *, QWidget *parent = nullptr);
};

#endif
