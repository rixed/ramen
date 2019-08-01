#include <cassert>
#include "conf.h"
#include "once.h"
#include "SourcesModel.h"

static bool const debug = false;

SourcesModel::SourcesModel(QObject *parent) :
  QAbstractItemModel(parent)
{
  root = new DirItem("");

  conf::autoconnect("^sources/.*/ramen$", [this](conf::Key const &, KValue const *kv) {
    Once::connect(kv, &KValue::valueCreated, this, &SourcesModel::addSourceText);
    connect(kv, &KValue::valueChanged, this, &SourcesModel::updateSourceText);
  });
  conf::autoconnect("^sources/.*/info$", [this](conf::Key const &, KValue const *kv) {
    Once::connect(kv, &KValue::valueCreated, this, &SourcesModel::addSourceInfo);
    connect(kv, &KValue::valueChanged, this, &SourcesModel::updateSourceInfo);
  });
}

QModelIndex SourcesModel::index(int row, int column, QModelIndex const &parent) const
{
  if (row < 0 || column < 0 || column > 1) return QModelIndex();

  DirItem const *parentDir =
    parent.isValid() ? static_cast<DirItem const *>(parent.internalPointer())
                     : root;
  if (! parentDir) return QModelIndex();

  if (row > parentDir->numRows()) return QModelIndex();
  return createIndex(row, column, parentDir->children[row]);
}

QModelIndex SourcesModel::parent(QModelIndex const &index) const
{
  if (! index.isValid()) return QModelIndex();
  TreeItem const *item = static_cast<TreeItem const *>(index.internalPointer());
  if (! item->parent) return QModelIndex();
  return indexOfItem(item->parent);
}

int SourcesModel::rowCount(QModelIndex const &parent) const
{
  TreeItem const *item =
    parent.isValid() ? static_cast<TreeItem const *>(parent.internalPointer())
                     : root;
  return item->numRows();
}

int SourcesModel::columnCount(QModelIndex const &) const
{
  return 1;
}

QVariant SourcesModel::data(QModelIndex const &index, int role) const
{
  if (!index.isValid() || index.column() != 0) return QVariant();

  TreeItem const *item = static_cast<TreeItem const *>(index.internalPointer());

  switch (role) {
    case Qt::DisplayRole:
      return QVariant(item->name);
    default:
      return QVariant();
  }
}

QString const baseNameOfKey(conf::Key const &k)
{
  // Take everything after first slash and before last:
  size_t fst = k.s.find('/');
  size_t lst = k.s.rfind('/');
  if (fst == std::string::npos || lst <= fst) {
    std::cout << "Key " << k << " is invalid for a source" << std::endl;
    return QString();
  }
  return QString::fromStdString(k.s.substr(fst + 1, lst - fst - 1));
}

QString const sourceNameOfKey(conf::Key const &k)
{
  // Take everything after first slash and before last:
  size_t fst = k.s.find('/');
  size_t lst = k.s.rfind('/');
  if (fst == std::string::npos || lst <= fst) {
    std::cout << "Key " << k << " is invalid for a source" << std::endl;
    return QString();
  }
  return QString::fromStdString(k.s.substr(fst + 1, lst - fst - 1) +
                                "." + k.s.substr(lst+1));
}

conf::Key const keyOfSourceName(QString const &sourceName, char const *newExtension)
{
  std::string f(sourceName.toStdString());
  size_t i = f.rfind('.');
  /* Any source name is supposed to have an extension from which to tell the
   * language it's written in. */
  assert(i != std::string::npos);

  std::string const ext =
    newExtension ?
      newExtension : f.substr(i+1, f.length() - i - 1);

  return conf::Key("sources/" + f.substr(0, i) + "/" + ext);
}

void SourcesModel::addSourceText(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString sourceName(baseNameOfKey(k));

  // Will create all the intermediary TreeItems, calling begin/endInsertRows::
  FileItem *file = createAll(sourceName, root);
  if (file) {
    std::shared_ptr<conf::RamenValueValue const> s =
      std::dynamic_pointer_cast<conf::RamenValueValue const>(v);
    if (s) {
      file->setText(s->v);
    } else {
      std::cout << "Source text not of string type?!" << std::endl;
    }
  }
}

void SourcesModel::updateSourceText(conf::Key const &, std::shared_ptr<conf::Value const>)
{
  // TODO
}

void SourcesModel::addSourceInfo(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  QString sourceName(baseNameOfKey(k));
  if (debug) std::cout << "addSourceInfo for " << sourceName.toStdString() << std::endl;

  // Will create all the intermediary TreeItems, calling begin/endInsertRows::
  FileItem *file = createAll(sourceName, root);
  if (file) {
    std::shared_ptr<conf::SourceInfo const> s =
      std::dynamic_pointer_cast<conf::SourceInfo const>(v);
    if (s) {
      file->setInfo(s);
    } else {
      std::cout << "Source info not of SourceInfo type?!" << std::endl;
    }
  }
}

void SourcesModel::updateSourceInfo(conf::Key const &, std::shared_ptr<conf::Value const>)
{
  // TODO
}

QModelIndex SourcesModel::indexOfItem(TreeItem const *item) const
{
  if (! item->parent) return QModelIndex();
  DirItem const *parentDir = dynamic_cast<DirItem const *>(item->parent);
  assert(parentDir);

  // FIXME: seriously?
  int row = 0;
  for (auto it = parentDir->children.constBegin();
       it != parentDir->children.constEnd(); it++) {
    if (*it == item) break;
    row ++;
  }

  if (row >= parentDir->numRows()) return QModelIndex();
  return createIndex(row, 0, (void *)item);
}

SourcesModel::FileItem *SourcesModel::createAll(QString const &sourceName, DirItem *root)
{
  QStringList names = sourceName.split("/", QString::SkipEmptyParts);
  if (names.isEmpty()) return nullptr;

  FileItem *ret = nullptr;
  do {
    QString const &nextName = names.takeFirst();
    bool const lastName = names.isEmpty();
    // Look for either a dir by that name or where to add it:
    int row = 0;
    bool needNewItem = true;
    for (auto it = root->children.constBegin();
         it != root->children.constEnd();
         it++
    ) {
      if ((*it)->name == nextName) {
        if (! lastName && (*it)->isDir()) {
          if (debug) std::cout << "createAll: Same directory name" << std::endl;
          DirItem *sub = dynamic_cast<DirItem *>(*it);
          assert(sub);  // because isDir()
          root = sub;
          needNewItem = false;
          break;
        } else if (lastName && ! (*it)->isDir()) {
          if (debug) std::cout << "createAll: Same file" << std::endl;
          ret = dynamic_cast<FileItem *>(*it);
          assert(ret);  // because !isDir()
          needNewItem = false;
          break;
        } else {
          /* Same name while not same type: Create a new dir with same name,
           * this is not UNIX. */
          if (debug) std::cout << "createAll: file and dir with same name!" << std::endl;
          break;
        }
      } else if ((*it)->name > nextName) {
        break;
      }
      row ++;
    }
    if (needNewItem) {
      if (debug) std::cout << "createAll: create new " << lastName << std::endl;
      emit beginInsertRows(indexOfItem(root), row, row);
      if (lastName) {
        // Create the final file
        ret = new FileItem(nextName, sourceName, root);
        root->addItem(ret, row);
      } else {
        // Add a subdirectory and "recurse"
        DirItem *sub = new DirItem(nextName, root);
        root->addItem(sub, row);
        root = sub;
      }
      emit endInsertRows();
    }
  } while (!ret);

  return ret;
}
