#ifndef TIMECHARTEDITFORM_H_200306
#define TIMECHARTEDITFORM_H_200306
#include <memory>
#include <string>
#include <QWidget>
#include "AtomicForm.h"

/* This is an AtomicForm that's composed of a single AtomicWidget,
 * as all the values are all within of a single KValue. */
class TimeChartEditWidget;

class TimeChartEditForm : public AtomicForm
{
  Q_OBJECT

public:
  TimeChartEditWidget *editWidget;

  TimeChartEditForm(std::string const &key, QWidget *parent = nullptr);
};

#endif
