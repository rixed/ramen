#ifndef PROGRAMITEM_H_190509
#define PROGRAMITEM_H_190509
#include <vector>
#include "OperationsItem.h"

class FunctionItem;

class ProgramItem : public OperationsItem
{
public:
  // As we are going to point to item from their children we do not want them
  // to move in memory, so let's use a vector of pointers:
  std::vector<FunctionItem *> functions;
  ProgramItem(OperationsItem *parent, QString const &name);
  ~ProgramItem();
  QVariant data(int) const;
  void reorder(OperationsModel const *);
};

std::ostream &operator<<(std::ostream &, ProgramItem const &);

#endif
