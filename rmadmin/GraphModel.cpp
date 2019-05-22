#include <cassert>
#include <iostream>
#include <list>
#include <QRegularExpression>
#include "GraphModel.h"
#include "conf.h"
#include "FunctionItem.h"
#include "ProgramItem.h"
#include "SiteItem.h"

GraphModel::GraphModel(GraphViewSettings const *settings_, QObject *parent) :
  QAbstractItemModel(parent),
  settings(settings_),
  paletteSize(100)
{
  conf::autoconnect("^sites/", [this](conf::Key const &k, KValue const *kv) {
    // This is going to be called from the OCaml thread. But that should be
    // OK since connect itself is threadsafe. Once we return, the KV value
    // is going to be set and therefore a signal emitted. This signal will
    // be queued for the Qt thread in which lives GraphModel to dequeue.
    std::cout << "connect a new KValue for " << k << " to the graphModel..." << std::endl;
    QObject::connect(kv, &KValue::valueCreated, this, &GraphModel::updateKey);
    QObject::connect(kv, &KValue::valueChanged, this, &GraphModel::updateKey);
  });
}

QModelIndex GraphModel::index(int row, int column, QModelIndex const &parent) const
{
  assert(column == 0);
  if (!parent.isValid()) { // Asking for a site
    if ((size_t)row >= sites.size()) return QModelIndex();
    SiteItem *site = sites[row];
    assert(site->treeParent == nullptr);
    return createIndex(row, column, static_cast<GraphItem *>(site));
  }

  GraphItem *parentPtr = static_cast<GraphItem *>(parent.internalPointer());
  // Maybe a site?
  SiteItem *parentSite = dynamic_cast<SiteItem *>(parentPtr);
  if (parentSite) { // bingo!
    if ((size_t)row >= parentSite->programs.size()) return QModelIndex();
    ProgramItem *program = parentSite->programs[row];
    assert(program->treeParent == parentPtr);
    return createIndex(row, column, static_cast<GraphItem *>(program));
  }
  // Maybe a program?
  ProgramItem *parentProgram = dynamic_cast<ProgramItem *>(parentPtr);
  if (parentProgram) {
    if ((size_t)row >= parentProgram->functions.size()) return QModelIndex();
    FunctionItem *function = parentProgram->functions[row];
    assert(function->treeParent == parentPtr);
    return createIndex(row, column, static_cast<GraphItem *>(function));
  }
  // There is no alternative
  assert(!"Someone should RTFM on indexing");
}

QModelIndex GraphModel::parent(QModelIndex const &index) const
{
  GraphItem *item =
    static_cast<GraphItem *>(index.internalPointer());
  GraphItem *treeParent = item->treeParent;

  if (! treeParent) {
    // We must be a site then:
    assert(nullptr != dynamic_cast<SiteItem *>(item));
    return QModelIndex(); // parent is "root"
  }

  return createIndex(treeParent->row, 0, treeParent);
}

int GraphModel::rowCount(QModelIndex const &parent) const
{
  if (!parent.isValid()) {
    // That must be "root" then:
    return sites.size();
  }

  GraphItem *parentPtr =
    static_cast<GraphItem *>(parent.internalPointer());
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

int GraphModel::columnCount(QModelIndex const &parent) const
{
  (void)parent;
  return 1;
}

QVariant GraphModel::data(QModelIndex const &index, int role) const
{
  if (!index.isValid()) return QVariant();

  if (role != Qt::DisplayRole) return QVariant();

  GraphItem *item =
    static_cast<GraphItem *>(index.internalPointer());
  return item->data(index.column());
}

void GraphModel::reorder()
{
  for (int i = 0; (size_t)i < sites.size(); i++) {
    if (sites[i]->row != i) {
      sites[i]->row = i;
      sites[i]->setPos(0, i * 130);
      emit positionChanged(createIndex(i, 0, static_cast<GraphItem *>(sites[i])));
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
        "(?<function_property>"
          "is_used|parents/\\d|startup_time|"
          "event_time/min|event_time/max|"
          "total/(tuples|bytes|cpu)|"
          "max/ram|"
          "archives/(times|num_files|current_size|alloc_size)"
        ")"
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

FunctionItem const *GraphModel::findWorker(std::shared_ptr<conf::Worker const> w)
{
  //std::cout << "Look for worker " << *w << std::endl;
  for (SiteItem const *siteItem : sites) {
    if (siteItem->name == w->site) {
      for (ProgramItem const *programItem : siteItem->programs) {
        if (programItem->name == w->program) {
          for (FunctionItem const *functionItem : programItem->functions) {
            if (functionItem->name == w->function) {
              //std::cout << "Found: " << functionItem->fqName().toStdString() << std::endl;
              return functionItem;
            } else {
              //std::cout << "...not " << functionItem->name.toStdString() << std::endl;
            }
          }
          //std::cout << "No such function: " << w->function.toStdString() << std::endl;
          return nullptr;
        }
      }
      //std::cout << "No such program: " << w->program.toStdString() << std::endl;
      return nullptr;
    }
  }
  //std::cout << "No such site: " << w->site.toStdString() << std::endl;
  return nullptr;
}

void GraphModel::setFunctionParent(FunctionItem const *parent, FunctionItem *child, int idx)
{
  if ((size_t)idx < child->parents.size()) {
    FunctionItem const *prev = child->parents[idx];
    if (prev) emit relationRemoved(prev, child);
  } else {
    child->parents.resize(idx+1);
  }
  child->parents[idx] = parent;
  emit relationAdded(parent, child);
}

/* In case we receive a child before its parents we have to wait for the
 * parent before setting up the relationship: */
struct PendingSetParent {
  /* FIXME: FunctionItem destructors should look in here and remove pending
   * SetParent for them! */
  FunctionItem *child;
  int idx;
  std::shared_ptr<conf::Worker const> worker;

  PendingSetParent(FunctionItem *child_, int idx_, std::shared_ptr<conf::Worker const> worker_) :
    child(child_), idx(idx_), worker(worker_) {}
};
static std::list<PendingSetParent> pendingSetParents;

void GraphModel::delaySetFunctionParent(FunctionItem *child, int idx, std::shared_ptr<conf::Worker const> w)
{
  // Check that there is no parent already pending for that slot:
  for (PendingSetParent &p : pendingSetParents) {
    if (p.child == child && p.idx == idx) {
      p.worker = w;
      return;
    }
  }
  pendingSetParents.emplace_back(child, idx, w);
}

void GraphModel::retrySetParents()
{
  for (auto it = pendingSetParents.begin(); it != pendingSetParents.end(); ) {
    FunctionItem const *parent = findWorker(it->worker);
    if (parent) {
      //std::cout << "Resolved pending parent" << std::endl;
      setFunctionParent(parent, it->child, it->idx);
      it = pendingSetParents.erase(it);
    } else {
      it++;
    }
  }
}

void GraphModel::setFunctionProperty(FunctionItem *functionItem, QString const &p, std::shared_ptr<conf::Value const> v)
{
  static QString const parents_prefix("parents/");
  if (p == "is_used") {
    std::shared_ptr<conf::Bool const> cf =
      std::dynamic_pointer_cast<conf::Bool const>(v);
    if (cf) functionItem->isUsed = cf->b;
  } else if (p == "startup_time") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) functionItem->startupTime = cf->d;
  } else if (p == "event_time/min") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) functionItem->eventTimeMin = cf->d;
  } else if (p == "event_time/max") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) functionItem->eventTimeMax = cf->d;
  } else if (p == "total/tuples") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) functionItem->totalTuples = cf->i;
  } else if (p == "total/bytes") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) functionItem->totalBytes = cf->i;
  } else if (p == "total/cpu") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) functionItem->totalCpu = cf->d;
  } else if (p == "max/ram") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) functionItem->maxRAM = cf->i;
  } else if (p == "archives/times") {
    // TODO
  } else if (p == "archives/num_files") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) functionItem->numArcFiles = cf->i;
  } else if (p == "archives/current_size") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) functionItem->numArcBytes = cf->i;
  } else if (p == "archives/alloc_size") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) functionItem->allocArcBytes = cf->i;
  } else if (p.startsWith(parents_prefix)) {
    int idx = p.mid(parents_prefix.length()).toInt();
    if (idx >= 0 && (size_t)idx < functionItem->parents.size()+100000) {
      std::shared_ptr<conf::Worker const> w =
        std::dynamic_pointer_cast<conf::Worker const>(v);
      if (w) {
        /* Try to locate the GraphItem of this worker. If it's not
         * there yet, enqueue this worker somewhere and revisit this
         * once a new function appears. */
        FunctionItem const *parent = findWorker(w);
        if (parent) {
          std::cout << "Set immediate parent" << std::endl;
          setFunctionParent(parent, functionItem, idx);
        } else {
          std::cout << "Set delayed parent" << std::endl;
          delaySetFunctionParent(functionItem, idx, w);
        }
      } else {
        std::cout << "Ignoring function parent " << idx
                  << " because it is not a worker" << std::endl;
      }
    } else {
      std::cout << "Ignoring bogus request to alter function "
                << idx << "th parent" << std::endl;
    }
  }
}

void GraphModel::setProgramProperty(ProgramItem *, QString const &, std::shared_ptr<conf::Value const>)
{
}

void GraphModel::setSiteProperty(SiteItem *siteItem, QString const &p, std::shared_ptr<conf::Value const> v)
{
  if (p == "is_master") {
    std::shared_ptr<conf::Bool const> b =
      std::dynamic_pointer_cast<conf::Bool const>(v);
    if (b) siteItem->isMaster = b->b;
  }
}

void GraphModel::updateKey(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  ParsedKey pk(k);
  /*std::cout << "GraphModel key " << k << " set to value " << *v
            << " is valid:" << pk.valid << std::endl;*/

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
      siteItem = new SiteItem(nullptr, pk.site, settings, paletteSize);
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
        programItem = new ProgramItem(siteItem, pk.program, settings, paletteSize);
        int idx = siteItem->programs.size();
        QModelIndex parent =
          createIndex(siteItem->row, 0, static_cast<GraphItem *>(siteItem));
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
          functionItem = new FunctionItem(programItem, pk.function, settings, paletteSize);
          int idx = programItem->functions.size();
          QModelIndex parent =
            createIndex(programItem->row, 0, static_cast<GraphItem *>(programItem));
          beginInsertRows(parent, idx, idx);
          programItem->functions.insert(programItem->functions.begin()+idx, functionItem);
          programItem->reorder(this);
          endInsertRows();
          // Since we have a new function, maybe we can solve some of the
          // pendingSetParents?
          retrySetParents();
          emit functionAdded(functionItem);
        }
        setFunctionProperty(functionItem, pk.property, v);
        functionItem->update();
      } else {
        setProgramProperty(programItem, pk.property, v);
        programItem->update();
      }
    } else {
      setSiteProperty(siteItem, pk.property, v);
      siteItem->update();
    }
  }
}

std::ostream &operator<<(std::ostream &os, SiteItem const &s)
{
  os << "Site[" << s.row << "]:" << s.name.toStdString() << std::endl;
  for (ProgramItem const *program : s.programs) {
    os << *program << std::endl;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, ProgramItem const &p)
{
  os << "  Program[" << p.row << "]:" << p.name.toStdString() << std::endl;
  for (FunctionItem const *function : p.functions) {
    os << *function << std::endl;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, FunctionItem const &f)
{
  os << "    Function[" << f.row << "]:" << f.name.toStdString();
  return os;
}
