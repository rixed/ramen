#include <cassert>
#include <iostream>
#include <QRegularExpression>
#include "OperationsModel.h"
#include "conf.h"

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
  for (int i = 0; (size_t)i < sites.size(); i++) {
    if (sites[i]->row != i) {
      sites[i]->row = i;
      sites[i]->setPos(0, i * 130);
      emit positionChanged(createIndex(i, 0, static_cast<OperationsItem *>(sites[i])));
    }
  }
}

class ParsedKey {
public:
  bool valid;
  QString site, program, function, property;
  ParsedKey(conf::Key const &k)
  {
    static QRegularExpression re(
      "^sites/(?<site>[^/]+)/"
      "("
        "functions/(?<program>.+)/"
        "(?<function>[^/]+)/"
        "(?<function_property>is_used|parents/\\d)"
      "|"
        "(?<site_property>is_master)"
      ")$"
      ,
      QRegularExpression::DontCaptureOption
    );
    assert(re.isValid());
    QString subject = QString::fromStdString(k.s);
    QRegularExpressionMatch match = re.match(subject);
    valid = match.hasMatch();
    if (valid) {
      site = match.captured("site");
      program = match.captured("program");
      function = match.captured("function");
      property = match.captured("function_property");
      if (property.isNull()) {
        property = match.captured("site_property");
      }
    }
  }
};

void OperationsModel::keyCreated(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  ParsedKey pk(k);
  // TODO: maybe update the model
  std::cerr << "OperationsModel key " << k << " created with value " << *v << " is valid:" << pk.valid << '\n';

  if (pk.valid) {
    assert(pk.site.length() > 0);

    SiteItem *siteItem = nullptr;
    for (SiteItem *si : sites) {
      if (si->name == pk.site) {
        siteItem = si;
        break;
      }
    }
    if (! siteItem) {
      siteItem = new SiteItem(nullptr, pk.site);
      int idx = sites.size(); // as we insert at the end for now
      beginInsertRows(QModelIndex(), idx, idx);
      sites.insert(sites.begin()+idx, siteItem);
      reorder();
      endInsertRows();
    }

    if (pk.program.length() > 0) {
      ProgramItem *programItem = nullptr;
      for (ProgramItem *pi : siteItem->programs) {
        if (pi->name == pk.program) {
          programItem = pi;
          break;
        }
      }
      if (! programItem) {
        programItem = new ProgramItem(siteItem, pk.program);
        int idx = siteItem->programs.size();
        QModelIndex parent = createIndex(siteItem->row, 0, static_cast<OperationsItem *>(siteItem));
        beginInsertRows(parent, idx, idx);
        siteItem->programs.insert(siteItem->programs.begin()+idx, programItem);
        siteItem->reorder(this);
        endInsertRows();
      }

      if (pk.function.length() > 0) {
        FunctionItem *functionItem = nullptr;
        for (FunctionItem *fi : programItem->functions) {
          if (fi->name == pk.function) {
            functionItem = fi;
            break;
          }
        }
        if (! functionItem) {
          functionItem = new FunctionItem(programItem, pk.function);
          int idx = programItem->functions.size();
          QModelIndex parent = createIndex(programItem->row, 0, static_cast<OperationsItem *>(programItem));
          beginInsertRows(parent, idx, idx);
          programItem->functions.insert(programItem->functions.begin()+idx, functionItem);
          programItem->reorder(this);
          endInsertRows();
        }
        functionItem->setProperty(pk.property, v);
      } else {
        programItem->setProperty(pk.property, v);
      }
    } else {
      siteItem->setProperty(pk.property, v);
    }
  }
}

void OperationsModel::keyChanged(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  // TODO: maybe update the model
  std::cerr << "OperationsModel key " << k << " changed to " << *v << '\n';
}

std::ostream &operator<<(std::ostream &os, SiteItem const &s)
{
  os << "Site[" << s.row << "]:" << s.name.toStdString() << '\n';
  for (ProgramItem const *program : s.programs) {
    os << *program << '\n';
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, ProgramItem const &p)
{
  os << "  Program[" << p.row << "]:" << p.name.toStdString() << '\n';
  for (FunctionItem const *function : p.functions) {
    os << *function << '\n';
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, FunctionItem const &f)
{
  os << "    Function[" << f.row << "]:" << f.name.toStdString();
  return os;
}
