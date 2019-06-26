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
  settings(settings_)
{
  conf::autoconnect("^sites/", [this](conf::Key const &k, KValue const *kv) {
    // This is going to be called from the OCaml thread. But that should be
    // OK since connect itself is threadsafe. Once we return, the KV value
    // is going to be set and therefore a signal emitted. This signal will
    // be queued for the Qt thread in which lives GraphModel to dequeue.
    std::cout << "connect a new KValue for " << k << " to the graphModel..." << std::endl;
    connect(kv, &KValue::valueCreated, this, &GraphModel::updateKey);
    connect(kv, &KValue::valueChanged, this, &GraphModel::updateKey);
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
  QString site, program, function, property, signature;
  ParsedKey(conf::Key const &k)
  {
    static QRegularExpression re(
      "^sites/(?<site>[^/]+)/"
      "("
        "workers/(?<program>.+)/"
        "(?<function>[^/]+)/"
        "(?<function_property>"
          "worker|startup_time/(first|last)|"
          "event_time/(min|max)|"
          "total/(tuples|bytes|cpu)|"
          "max/ram|"
          "archives/(times|num_files|current_size|alloc_size)|"
          "instances/(?<signature>[^/]+)/(?<instance_property>[^/]+)"
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
        signature = match.captured("signature");
        property = match.captured("instance_property");
      }
      if (property.isNull()) {
        property = match.captured("site_property");
      }
    }
  }
};

FunctionItem const *GraphModel::find(QString const &site, QString const &program, QString const &function)
{
  /*std::cout << "Look for function " << site.toStdString() << "/"
                                      << program.toStdString() << "/"
                                      << function.toStdString() << std::endl;*/
  for (SiteItem const *siteItem : sites) {
    if (siteItem->name == site) {
      for (ProgramItem const *programItem : siteItem->programs) {
        if (programItem->name == program) {
          for (FunctionItem const *functionItem : programItem->functions) {
            if (functionItem->name == function) {
              //std::cout << "Found: " << functionItem->fqName().toStdString() << std::endl;
              return functionItem;
            } else {
              //std::cout << "...not " << functionItem->name.toStdString() << std::endl;
            }
          }
          //std::cout << "No such function: " << function.toStdString() << std::endl;
          return nullptr;
        }
      }
      //std::cout << "No such program: " << program.toStdString() << std::endl;
      return nullptr;
    }
  }
  //std::cout << "No such site: " << site.toStdString() << std::endl;
  return nullptr;
}

void GraphModel::addFunctionParent(FunctionItem const *parent, FunctionItem *child)
{
  child->parents.push_back(parent);
  emit relationAdded(parent, child);
}

/* In case we receive a child before its parents we have to wait for the
 * parent before setting up the relationship: */
struct PendingAddParent {
  /* FIXME: FunctionItem destructors should look in here and remove pending
   * AddParent for them! */
  FunctionItem *child;
  QString const site, program, function;

  PendingAddParent(FunctionItem *child_, QString const &site_, QString const &program_, QString const function_) :
    child(child_), site(site_), program(program_), function(function_) {}
};
static std::list<PendingAddParent> pendingAddParents;

void GraphModel::removeParents(FunctionItem *child)
{
  for (size_t i = 0; i < child->parents.size(); i ++) {
    emit relationRemoved(child->parents[i], child);
  }
  child->parents.clear();

  // Also go through the pendingAddParents:
  for (auto it = pendingAddParents.begin(); it != pendingAddParents.end(); ) {
    if (it->child == child) {
      it = pendingAddParents.erase(it);
    } else {
      it ++;
    }
  }
}

void GraphModel::delayAddFunctionParent(FunctionItem *child, QString const &site, QString const &program, QString const &function)
{
  pendingAddParents.emplace_back(child, site, program, function);
}

void GraphModel::retryAddParents()
{
  for (auto it = pendingAddParents.begin(); it != pendingAddParents.end(); ) {
    FunctionItem const *parent = find(it->site, it->program, it->function);
    if (parent) {
      //std::cout << "Resolved pending parent" << std::endl;
      addFunctionParent(parent, it->child);
      it = pendingAddParents.erase(it);
    } else {
      it ++;
    }
  }
}

void GraphModel::setFunctionProperty(SiteItem const *siteItem, ProgramItem const *programItem, FunctionItem *functionItem, QString const &p, std::shared_ptr<conf::Value const> v)
{
  std::cout << "setFunctionProperty for property " << p.toStdString() << std::endl;
  if (p == "worker") {
    std::shared_ptr<conf::Worker const> cf =
      std::dynamic_pointer_cast<conf::Worker const>(v);
    if (cf) {
      functionItem->worker = cf;

      for (auto ref : cf->parent_refs) {
        /* If the parent is not local then assume the existence of a top-half
         * for this function running on the remote site: */
        QString psite, pprog, pfunc;
        if (ref->site == siteItem->name) {
          psite = ref->site;
          pprog = ref->program;
          pfunc = ref->function;
        } else {
          psite = ref->site;
          pprog = programItem->name;
          pfunc = functionItem->name;
        }
        /* Try to locate the GraphItem of this parent. If it's not
         * there yet, enqueue this worker somewhere and revisit this
         * once a new function appears. */
        FunctionItem const *parent = find(psite, pprog, pfunc);
        if (parent) {
          std::cout << "Set immediate parent" << std::endl;
          addFunctionParent(parent, functionItem);
        } else {
          std::cout << "Set delayed parent" << std::endl;
          delayAddFunctionParent(functionItem, psite, pprog, pfunc);
        }
      }

      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "startup_time/first") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) {
      functionItem->firstStartupTime = cf->d;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "startup_time/last") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) {
      functionItem->lastStartupTime = cf->d;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "event_time/min") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) {
      functionItem->eventTimeMin = cf->d;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "event_time/max") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) {
      functionItem->eventTimeMax = cf->d;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "total/tuples") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) {
      functionItem->totalTuples = cf->i;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "total/bytes") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) {
      functionItem->totalBytes = cf->i;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "total/cpu") {
    std::shared_ptr<conf::Float const> cf =
      std::dynamic_pointer_cast<conf::Float const>(v);
    if (cf) {
      functionItem->totalCpu = cf->d;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "max/ram") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) {
      functionItem->maxRAM = cf->i;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "archives/times") {
    // TODO
    emit storagePropertyChanged(functionItem);
  } else if (p == "archives/num_files") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) {
      functionItem->numArcFiles = cf->i;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "archives/current_size") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) {
      functionItem->numArcBytes = cf->i;
      emit storagePropertyChanged(functionItem);
    }
  } else if (p == "archives/alloc_size") {
    std::shared_ptr<conf::Int const> cf =
      std::dynamic_pointer_cast<conf::Int const>(v);
    if (cf) {
      functionItem->allocArcBytes = cf->i;
      emit storagePropertyChanged(functionItem);
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
  if (!pk.valid) return;

  assert(pk.site.length() > 0);

  SiteItem *siteItem = nullptr;
  for (SiteItem *si : sites) {
    if (si->name == pk.site) {
      siteItem = si;
      break;
    }
  }
  if (! siteItem) {
    siteItem = new SiteItem(nullptr, pk.site, settings);
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
      programItem = new ProgramItem(siteItem, pk.program, settings);
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
        functionItem = new FunctionItem(programItem, pk.function, settings);
        int idx = programItem->functions.size();
        QModelIndex parent =
          createIndex(programItem->row, 0, static_cast<GraphItem *>(programItem));
        beginInsertRows(parent, idx, idx);
        programItem->functions.insert(programItem->functions.begin()+idx, functionItem);
        programItem->reorder(this);
        endInsertRows();
        // Since we have a new function, maybe we can solve some of the
        // pendingAddParents?
        retryAddParents();
        emit functionAdded(functionItem);
      }
      setFunctionProperty(siteItem, programItem, functionItem, pk.property, v);
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
