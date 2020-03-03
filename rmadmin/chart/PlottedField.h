#ifndef PLOTTEDFIELD_H_200303
#define PLOTTEDFIELD_H_200303
/* A Plottedfield stores the configuration to plot a given column in a TimePlot. */
#include <memory>
#include <vector>

class Function;
class QWidget;

class PlottedField
{
  std::shared_ptr<Function> function;

  int column;
  std::vector<int> factors;

  enum Representation {
    Independent, Stacked, StackCentered
  } representation;

public:
  PlottedField(
    std::shared_ptr<Function>, int column_, std::vector<int> factors,
    Representation);

  void paint(QWidget *) const;
};

#endif
