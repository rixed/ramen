#ifndef PROGRAMITEM_H_190509
#define PROGRAMITEM_H_190509
#include <vector>
#include "GraphItem.h"

class GraphViewSettings;
class FunctionItem;

struct Program : public GraphData
{
  Program(QString const &name_) : GraphData(name_) {}
};

class ProgramItem : public GraphItem
{
protected:
  std::vector<std::pair<QString const, QString const>> labels() const;
public:
  // As we are going to point to item from their children we do not want them
  // to move in memory, so let's use a vector of pointers:
  std::vector<FunctionItem *> functions;

  ProgramItem(
    GraphItem *treeParent, std::unique_ptr<Program>, GraphViewSettings const *);

  QVariant data(int column, int role) const;
  void reorder(GraphModel const *);
  QRectF operationRect() const;

  bool isTopHalf() const;
  bool isWorking() const;
};

std::ostream &operator<<(std::ostream &, ProgramItem const &);

#endif
