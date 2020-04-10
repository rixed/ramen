#ifndef ATOMICWIDGETALTERNATIVE_190830
#define ATOMICWIDGETALTERNATIVE_190830
/* Sometime one had several possible editors, each of them an AtomicWidget, but
 * want it to look like a single one to AtomicForm. */
#include <vector>
#include "AtomicWidget.h"

class AtomicWidgetAlternative : public AtomicWidget
{
  Q_OBJECT

  std::vector<AtomicWidget *> widgets;
  int currentWidget;

public:
  AtomicWidgetAlternative(QWidget *parent = nullptr);

  int addWidget(AtomicWidget *);

  void setEnabled(bool enabled) override;

  std::shared_ptr<conf::Value const> getValue() const override;

  bool setValue(std::string const &, std::shared_ptr<conf::Value const>) override;

  void setCurrentWidget(int);

  std::string const &key() const override;

  void saveKey(std::string const &newKey) override;

public slots:
  bool setKey(std::string const &) override;
};

#endif
