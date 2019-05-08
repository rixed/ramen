#include <cassert>
#include <iostream>
#include "OperationsModel.h"
#include "conf.h"

OperationsItem::~OperationsItem() {}

FunctionItem::FunctionItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent), name(name_) {}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return name;
}

void FunctionItem::reorder() {}

ProgramItem::ProgramItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent), name(name_) {}

ProgramItem::~ProgramItem()
{
  for (FunctionItem *function : functions) {
    delete function;
  }
}

QVariant ProgramItem::data(int column) const
{
  assert(column == 0);
  return name;
}

void ProgramItem::reorder()
{
  for (size_t i = 0; i < functions.size(); i++)
    functions[i]->row = i;
}

SiteItem::SiteItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent), name(name_) {}

SiteItem::~SiteItem()
{
  for (ProgramItem *program : programs) {
    delete program;
  }
}

QVariant SiteItem::data(int column) const
{
  assert(column == 0);
  return name;
}

void SiteItem::reorder()
{
  for (size_t i = 0; i < programs.size(); i++)
    programs[i]->row = i;
}

OperationsModel::OperationsModel(QObject *parent) :
  QAbstractItemModel(parent)
{
  /* Register callbacks but for now also populate manually: */
  conf::autoconnect("sites/", [this](conf::Key const &k, KValue const *kv) {
    // TODO: check we are really interested in k (ie it affects either a row or a column)
    (void)k;
    QObject::connect(kv, &KValue::valueCreated, this, &OperationsModel::keyCreated);
    QObject::connect(kv, &KValue::valueChanged, this, &OperationsModel::keyChanged);
  });

  SiteItem *siteA = new SiteItem(nullptr, "siteA");
  sites.push_back(siteA);

  ProgramItem *programA = new ProgramItem(siteA, "programA");
  siteA->programs.push_back(programA);
  programA->functions.push_back(new FunctionItem(programA, "functionA"));
  programA->functions.push_back(new FunctionItem(programA, "functionB"));
  programA->functions.push_back(new FunctionItem(programA, "functionC"));
  programA->reorder();

  ProgramItem *programB = new ProgramItem(siteA, "programB");
  siteA->programs.push_back(programB);
  programB->functions.push_back(new FunctionItem(programB, "functionA"));
  programB->functions.push_back(new FunctionItem(programB, "functionD"));
  programB->reorder();

  siteA->reorder();

  reorder();

  for (SiteItem const *site : sites) {
    std::cout << *site;
  }
}

QModelIndex OperationsModel::index(int row, int column, QModelIndex const &parent) const
{
  assert(column == 0);
  if (!parent.isValid()) { // Asking for a site
    if ((size_t)row >= sites.size()) return QModelIndex();
    SiteItem *site = sites[row];
    assert(site->parent == nullptr);
    return createIndex(row, column, static_cast<OperationsItem *>(site));
  }

  OperationsItem *parentPtr = static_cast<OperationsItem *>(parent.internalPointer());
  // Maybe a site?
  SiteItem *parentSite = dynamic_cast<SiteItem *>(parentPtr);
  if (parentSite) { // bingo!
    if ((size_t)row >= parentSite->programs.size()) return QModelIndex();
    ProgramItem *program = parentSite->programs[row];
    assert(program->parent == parentPtr);
    return createIndex(row, column, static_cast<OperationsItem *>(program));
  }
  // Maybe a program?
  ProgramItem *parentProgram = dynamic_cast<ProgramItem *>(parentPtr);
  if (parentProgram) {
    if ((size_t)row >= parentProgram->functions.size()) return QModelIndex();
    FunctionItem *function = parentProgram->functions[row];
    assert(function->parent == parentPtr);
    return createIndex(row, column, static_cast<OperationsItem *>(function));
  }
  // There is no alternative
  assert(!"Someone should RTFM on indexing");
}

QModelIndex OperationsModel::parent(QModelIndex const &index) const
{
  OperationsItem *item =
    static_cast<OperationsItem *>(index.internalPointer());
  OperationsItem *parent = item->parent;

  if (! parent) {
    // We must be a site then:
    assert(nullptr != dynamic_cast<SiteItem *>(item));
    return QModelIndex(); // parent is "root"
  }

  return createIndex(parent->row, 0, parent);
}

int OperationsModel::rowCount(QModelIndex const &parent) const
{
  if (!parent.isValid()) {
    // That must be "root" then:
    return sites.size();
  }

  OperationsItem *parentPtr =
    static_cast<OperationsItem *>(parent.internalPointer());
  SiteItem *parentSite = dynamic_cast<SiteItem *>(parentPtr);
  if (parentSite) {
    return parentSite->programs.size();
  }
  ProgramItem *parentProgram = dynamic_cast<ProgramItem *>(parentPtr);
  if (parentProgram) {
    return parentProgram->functions.size();
  }
  FunctionItem *parentFunction = dynamic_cast<FunctionItem *>(parentPtr);
  if (parentFunction) {
    return 0;
  }

  assert(!"how is indexing working, again?");
}

int OperationsModel::columnCount(QModelIndex const &parent) const
{
  (void)parent;
  return 1;
}

QVariant OperationsModel::data(QModelIndex const &index, int role) const
{
  if (!index.isValid()) return QVariant();

  if (role != Qt::DisplayRole) return QVariant();

  OperationsItem *item =
    static_cast<OperationsItem *>(index.internalPointer());
  return item->data(index.column());
}

void OperationsModel::reorder()
{
  for (size_t i = 0; i < sites.size(); i++)
    sites[i]->row = i;
}

void OperationsModel::keyCreated(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  // TODO: maybe update the model
  std::cerr << "OperationsModel key " << k << " created with value " << *v << '\n';
}

void OperationsModel::keyChanged(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  // TODO: maybe update the model
  std::cerr << "OperationsModel key " << k << " changed to " << *v << '\n';
}

std::ostream &operator<<(std::ostream &os, SiteItem const &s)
{
  os << "Site[" << s.row << "]:" << s.name.toString().toStdString() << '\n';
  for (ProgramItem const *program : s.programs) {
    os << *program << '\n';
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, ProgramItem const &p)
{
  os << "  Program[" << p.row << "]:" << p.name.toString().toStdString() << '\n';
  for (FunctionItem const *function : p.functions) {
    os << *function << '\n';
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, FunctionItem const &f)
{
  os << "    Function[" << f.row << "]:" << f.name.toString().toStdString();
  return os;
}
