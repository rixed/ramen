#ifndef FUNCTIONINFOBOX_H_190516
#define FUNCTIONINFOBOX_H_190516
#include <string>
#include <QWidget>

class FunctionItem;

class FunctionInfoBox : public QWidget
{
  Q_OBJECT

  FunctionItem const *f;
  std::string pref;  // key prefix

public:
  FunctionInfoBox(FunctionItem const *, QWidget *parent = nullptr);
};

#endif
