#ifndef PROGRAMITEM_H_190509
#define PROGRAMITEM_H_190509
#include <vector>
#include "GraphItem.h"

class FunctionItem;
class GraphViewSettings;

class ProgramItem : public GraphItem
{
protected:
  std::vector<std::pair<QString const, QString const>> labels() const;
public:
  // As we are going to point to item from their children we do not want them
  // to move in memory, so let's use a vector of pointers:
  std::vector<FunctionItem *> functions;

  ProgramItem(GraphItem *treeParent, QString const &name, GraphViewSettings const *);
  ~ProgramItem();

  void reorder(GraphModel const *);
  QRectF operationRect() const;
  bool isTopHalf() const;
};

std::ostream &operator<<(std::ostream &, ProgramItem const &);

#endif
