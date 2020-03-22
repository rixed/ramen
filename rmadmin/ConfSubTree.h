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

  /* Terminals have non empty strings attached to them, that's the value of
   * the data UserRole value (whereas the DisplayRole is the above name) */
  QString const termValue;

  bool isTerm() const { return !termValue.isEmpty(); }

  ConfSubTree(QString const &name_, ConfSubTree *parent_,
              QString const &termValue_);

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

  ConfSubTree *insertAt(int pos, QString const &name, QString const &termValue);

  QString nameFromRoot(QString const &sep) const;
};

#endif
