#ifndef CONFSUBTREE_H_200320
#define CONFSUBTREE_H_200320
#include <vector>
#include <QString>

class ConfSubTree
{
  std::vector<ConfSubTree *> children;

public:
  /* Cannot be const because SubTrees are constructed inplace, but
   * that's the idea: */
  QString name;

  ConfSubTree *parent; // nullptr only for root

  bool isTerm;

  ConfSubTree(QString const &name_, ConfSubTree *parent_, bool isTerm_);

  // Copy:
  ConfSubTree(ConfSubTree const &other, ConfSubTree *parent_);

  ~ConfSubTree();

  int count() const;

  ConfSubTree const *child(int pos) const;

  ConfSubTree *child(int pos);

  int childNum(ConfSubTree const *child) const;

  void dump(QString const &indent = "") const;

  // Useful in gdb:
  void __attribute__((noinline)) __attribute__((used))
    dump_c(int const indent = 0) const;

  ConfSubTree *insertAt(int pos, QString const &name, bool isTerm);
};

#endif
