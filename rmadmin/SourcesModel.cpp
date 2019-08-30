#include <cassert>
#include <QApplication>
#include <QStyle>
#include <QAbstractItemModel>
#include "conf.h"
#include "misc.h"
#include "Resources.h"
#include "SourcesModel.h"

static bool const verbose = true;

SourcesModel::SourcesModel(QObject *parent) :
  QAbstractItemModel(parent)
{
  root = new DirItem("");

  connect(&kvs, &KVStore::valueCreated,
          this, &SourcesModel::addSource);
  connect(&kvs, &KVStore::valueDeleted,
          this, &SourcesModel::delSource);
}

#define NUM_COLUMNS 3

QModelIndex SourcesModel::index(int row, int column, QModelIndex const &parent) const
{
  if (row < 0 || column < 0 || column >= NUM_COLUMNS) return QModelIndex();

  DirItem const *parentDir =
    parent.isValid() ? static_cast<DirItem const *>(parent.internalPointer())
                     : root;
  if (! parentDir) return QModelIndex();

  if (row >= parentDir->numRows()) return QModelIndex();
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
  return NUM_COLUMNS;
}

QVariant SourcesModel::data(QModelIndex const &index, int role) const
{
  Q_ASSERT(checkIndex(index, QAbstractItemModel::CheckIndexOption::IndexIsValid));
  if (! index.isValid()) return QVariant();

  TreeItem const *item = static_cast<TreeItem const *>(index.internalPointer());

  switch (role) {
    case Qt::DisplayRole:
      switch (index.column()) {
        case 0:
          return QVariant(item->name);
        case 1:
          {
            // Button to show the compilation result:
            if (item->isDir()) return QVariant();
            std::shared_ptr<conf::SourceInfo const> info(sourceInfoOfItem(item));
            if (! info)
              return QVariant();
            else if (info->errMsg.isEmpty())
              return Resources::get()->infoPixmap;
            else
              return Resources::get()->errorPixmap;
          }
        case 2:
          {
            // Button to run the program
            if (item->isDir()) return QVariant();
            std::shared_ptr<conf::SourceInfo const> info(sourceInfoOfItem(item));
            if (! info)
              return Resources::get()->waitPixmap;
            else if (info->errMsg.isEmpty())
              return Resources::get()->playPixmap;
            else
              return QVariant();
          }
        default:
          return QVariant();
      }
      break;
    default:
      return QVariant();
  }
}

QString const baseNameOfKey(std::string const &k)
{
  // Take everything after first slash and before last:
  size_t fst = k.find('/');
  size_t lst = k.rfind('/');
  if (fst == std::string::npos || lst <= fst) {
    std::cout << "Key " << k << " is invalid for a source" << std::endl;
    return QString();
  }
  return QString::fromStdString(k.substr(fst + 1, lst - fst - 1));
}

QString const sourceNameOfKey(std::string const &k)
{
  // Take everything after first slash and before last:
  size_t fst = k.find('/');
  size_t lst = k.rfind('/');
  if (fst == std::string::npos || lst <= fst) {
    std::cout << "Key " << k << " is invalid for a source" << std::endl;
    return QString();
  }
  return QString::fromStdString(k.substr(fst + 1, lst - fst - 1) +
                                "." + k.substr(lst+1));
}

std::string const keyOfSourceName(QString const &sourceName, char const *newExtension)
{
  std::string f(sourceName.toStdString());
  size_t i = f.rfind('.');

  /* Any source name is supposed to have an extension from which to tell the
   * language it's written in. */
  assert(newExtension || i != std::string::npos);

  std::string const ext =
    newExtension ?
      newExtension : f.substr(i+1, f.length() - i - 1);

  return
    i != std::string::npos ?
      std::string("sources/" + f.substr(0, i) + "/" + ext) :
      std::string("sources/" + f + "/" + ext);
}

bool SourcesModel::isMyKey(std::string const &k) const
{
  return startsWith(k, "sources/") && endsWith(k, "/ramen");
}

void SourcesModel::addSource(KVPair const &kvp)
{
  if (! isMyKey(kvp.first)) return;

  QStringList names =
    QString::fromStdString(kvp.first).split("/", QString::SkipEmptyParts);
  if (names.length() <= 2) {
    std::cerr << "addSource: invalid source key " << kvp.first << std::endl;
    return;
  }

  names.removeFirst();  // "sources"
  names.removeLast(); // extension
  createAll(kvp.first, names, root);
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

SourcesModel::FileItem *SourcesModel::createAll(
  std::string const &sourceKey, QStringList &names, DirItem *root)
{
  FileItem *ret = nullptr;
  do {
    QString const &nextName = names.takeFirst();
    bool const lastName = names.isEmpty();
    // Look for either a dir by that name or where to add it:
    int row = 0;
    bool needNewItem = true;
    for (auto it = root->children.constBegin();
         it != root->children.constEnd();
         it ++
    ) {
      if ((*it)->name == nextName) {
        if (! lastName && (*it)->isDir()) {
          if (verbose) std::cout << "createAll: Same directory name" << std::endl;
          DirItem *sub = dynamic_cast<DirItem *>(*it);
          assert(sub);  // because isDir()
          root = sub;
          needNewItem = false;
          break;
        } else if (lastName && ! (*it)->isDir()) {
          if (verbose) std::cout << "createAll: Same file" << std::endl;
          ret = dynamic_cast<FileItem *>(*it);
          assert(ret);  // because !isDir()
          needNewItem = false;
          break;
        } else {
          /* Same name while not same type: Create a new dir with same name,
           * this is not UNIX. */
          if (verbose) std::cout << "createAll: file and dir with same name!" << std::endl;
          break;
        }
      } else if ((*it)->name > nextName) {
        break;
      }
      row ++;
    }
    if (needNewItem) {
      if (verbose) std::cout << "createAll: create new " << lastName << std::endl;
      beginInsertRows(indexOfItem(root), row /* first row */, row /* last */);
      if (lastName) {
        // Create the final file
        ret = new FileItem(nextName, sourceKey, root);
        root->addItem(ret, row);
      } else {
        // Add a subdirectory and "recurse"
        DirItem *sub = new DirItem(nextName, root);
        root->addItem(sub, row);
        root = sub;
      }
      endInsertRows();
    }
  } while (!ret);

  return ret;
}

std::string const SourcesModel::keyOfIndex(QModelIndex const &index) const
{
  // Retrieve the key for the info:
  SourcesModel::TreeItem const *item =
    static_cast<SourcesModel::TreeItem const *>(index.internalPointer());
  if (item->isDir()) return std::string();

  SourcesModel::FileItem const *file =
    dynamic_cast<SourcesModel::FileItem const *>(item);
  return file->sourceKey;
}

std::shared_ptr<conf::SourceInfo const> SourcesModel::sourceInfoOfItem(TreeItem const *item) const
{
  if (item->isDir()) return nullptr;

  SourcesModel::FileItem const *file =
    dynamic_cast<SourcesModel::FileItem const *>(item);

  std::string const infoKey = changeSourceKeyExt(file->sourceKey, "info");

  std::shared_ptr<conf::Value const> v;
  kvs.lock.lock_shared();
  auto it = kvs.map.find(infoKey);
  if (it != kvs.map.end()) v = it->second.val;
  kvs.lock.unlock_shared();

  if (! v) return nullptr;
  return std::dynamic_pointer_cast<conf::SourceInfo const>(v);
}

void SourcesModel::delSource(KVPair const &kvp)
{
  if (! isMyKey(kvp.first)) return;

  QStringList names =
    QString::fromStdString(kvp.first).split("/", QString::SkipEmptyParts);
  if (names.length() <= 2) {
    std::cerr << "Invalid source key " << kvp.first << std::endl;
    return;
  }

  names.removeFirst();  // "sources"
  names.removeLast(); // extension

  deleteAll(names, root);
}

void SourcesModel::deleteAll(QStringList &names, DirItem *root)
{
  assert(! names.isEmpty());

  /* Locate this name in root children: */
  int row;
  for (row = 0; row < root->children.count(); row ++) {
    if (root->children[row]->name == names.first()) {
      goto found;
    }
  }
  std::cerr << "Cannot find " << names.first().toStdString() << " in children"
            << std::endl;
  return;
found:

  names.removeFirst();

  if (names.count() > 0) {
    DirItem *child = dynamic_cast<DirItem *>(root->children[row]);
    assert(child);
    deleteAll(names, child);
    if (child->children.count() > 0) return;
  }

  if (verbose)
    std::cout << "SourcesModel: Deleting leaf "
              << root->children[row]->name.toStdString() << std::endl;

  beginRemoveRows(indexOfItem(root), row, row);
  delete root->children[row];
  endRemoveRows();
}
