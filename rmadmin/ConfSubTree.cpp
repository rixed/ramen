#include <cassert>
#include <iostream>
#include <QDebug>

#include "ConfSubTree.h"

static bool const verbose(false);

ConfSubTree::ConfSubTree(
  QString const &name_,
  ConfSubTree *parent_,
  QString const &termValue_)
  : name(name_), parent(parent_), termValue(termValue_)
{
  if (verbose)
    qDebug() << "ConfSubTree: Creating ConfSubTree(name=" << name
             << ", parent=" << (parent ? parent->name : "none") << ")";
  children.reserve(10);
}

ConfSubTree::ConfSubTree(ConfSubTree const &other, ConfSubTree *parent_)
  : name(other.name), parent(parent_), termValue(other.termValue)
{
  // Discard this content
  children.clear();

  if (verbose)
    qDebug() << "ConfSubTree: Copying a ConfSubTree with"
             << other.count() << "children";

  for (ConfSubTree const *c : other.children) {
    ConfSubTree *myChild = new ConfSubTree(*c, this);
    children.push_back(myChild);
  }
}

ConfSubTree::~ConfSubTree()
{
  for (ConfSubTree *c : children) delete c;
}

int ConfSubTree::count() const
{
  return children.size();
}

ConfSubTree const *ConfSubTree::child(int pos) const
{
  assert(pos >= 0 && pos < (ssize_t)children.size());
  return children[pos];
}

ConfSubTree *ConfSubTree::child(int pos)
{
  return const_cast<ConfSubTree *>(const_cast<ConfSubTree const *>(this)->child(pos));
}

int ConfSubTree::childNum(ConfSubTree const *child) const
{
  if (verbose)
    qDebug() << "ConfSubTree: childNum(" << child->name << ")" << "of" << name;

  for (size_t c = 0; c < children.size(); c ++) {
    if (children[c]->name == child->name &&
        children[c] != child)
      qCritical() << "not unique child address for" << child->name << ";"
                  << children[c] << "vs" << child;
    if (children[c] == child) return c;
  }
  assert(!"Not a child");
  return -1;
}

void ConfSubTree::dump(QString const &indent) const
{
  for (ConfSubTree *c : children) {
    qDebug() << "ConfSubTree:" << indent << c->name << "(parent="
             << c->parent->name << ")";
    c->dump(indent + "  ");
  }
}

void __attribute__((noinline)) __attribute__((used))
  ConfSubTree::dump_c(int const indent) const
{
  char indent_[indent+1];
  int i(0);
  for (; i < indent; i++) indent_[i] = ' ';
  indent_[i] = '\0';

  for (ConfSubTree *c : children) {
    std::cout << "ConfSubTree:" << indent_ << c->name.toStdString() << "(parent="
              << (c->parent ? c->parent->name.toStdString() : "NULL") << ")"
              << std::endl;
    c->dump_c(indent + 1);
  }
}

ConfSubTree *ConfSubTree::insertAt(
  int pos, QString const &name, QString const &termValue)
{
  assert(pos >= 0 && pos <= (ssize_t)children.size());
  ConfSubTree *s = new ConfSubTree(name, this, termValue);
  children.insert(children.begin() + pos, s);
  return s;
}

QString ConfSubTree::nameFromRoot(QString const &sep) const
{
  if (! parent)
    return name;
  else
    return parent->nameFromRoot(sep) + sep + name;
}
