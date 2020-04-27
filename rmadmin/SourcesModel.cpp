#include <cassert>
#include <QtGlobal>
#include <QApplication>
#include <QDebug>
#include <QStyle>
#include <QAbstractItemModel>
#include "conf.h"
#include "misc.h"
#include "Resources.h"
#include "SourcesModel.h"

static bool const verbose(false);

SourcesModel::SourcesModel(QObject *parent) :
  QAbstractItemModel(parent)
{
  root = new DirItem("");

  connect(kvs, &KVStore::keyChanged,
          this, &SourcesModel::onChange);
}

void SourcesModel::onChange(QList<ConfChange> const &changes)
{
  for (int i = 0; i < changes.length(); i++) {
    ConfChange const &change { changes.at(i) };
    switch (change.op) {
      case KeyCreated:
        addSource(change.key, change.kv);
        break;
      case KeyDeleted:
        delSource(change.key, change.kv);
        break;
      default:
        break;
    }
  }
}

QModelIndex SourcesModel::index(int row, int column, QModelIndex const &parent) const
{
  if (row < 0 || column < 0 || column >= NumColumns) return QModelIndex();

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
  return NumColumns;
}

QVariant SourcesModel::data(QModelIndex const &index, int role) const
{
# if (QT_VERSION >= QT_VERSION_CHECK(5, 11, 0))
  Q_ASSERT(checkIndex(index, QAbstractItemModel::CheckIndexOption::IndexIsValid));
# endif
  if (! index.isValid()) return QVariant();

  TreeItem const *item = static_cast<TreeItem const *>(index.internalPointer());

  switch (role) {
    case Qt::DisplayRole:
      switch (index.column()) {
        case SrcPath:
          return QVariant(item->name);
        case Action1:
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
        case Action2:
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
    qDebug() << "Key" << QString::fromStdString(k) << "is invalid for a source";
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
    qDebug() << "Key" << QString::fromStdString(k) << "is invalid for a source";
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
  return startsWith(k, "sources/") && (
           endsWith(k, "/ramen") || endsWith(k, "/alert"));
}

void SourcesModel::addSource(std::string const &key, KValue const &)
{
  if (! isMyKey(key)) return;

  QStringList names =
    QString::fromStdString(key).split("/", QString::SkipEmptyParts);
  if (names.length() <= 2) {
    qCritical() << "addSource: invalid source key" << QString::fromStdString(key);
    return;
  }

  names.removeFirst();  // "sources"
  QString const extension(names.takeLast());
  std::string sourceKeyPrefix(removeExt(key, '/'));
  createAll(sourceKeyPrefix, names, extension, root);
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
  std::string const &sourceKeyPrefix,
  QStringList &names,
  QString const &extension,
  DirItem *root)
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
          if (verbose) qDebug() << "createAll: Same directory name";
          DirItem *sub = dynamic_cast<DirItem *>(*it);
          assert(sub);  // because isDir()
          root = sub;
          needNewItem = false;
          break;
        } else if (lastName && ! (*it)->isDir()) {
          if (verbose) qDebug() << "createAll: Same file";
          ret = dynamic_cast<FileItem *>(*it);
          assert(ret);  // because !isDir()
          ret->addExtension(extension);
          needNewItem = false;
          break;
        } else {
          /* Same name while not same type: Create a new dir with same name,
           * this is not UNIX. */
          if (verbose) qDebug() << "createAll: file and dir with same name!";
          break;
        }
      } else if ((*it)->name > nextName) {
        break;
      }
      row ++;
    }
    if (needNewItem) {
      if (verbose) qDebug() << "createAll: create new" << lastName;
      beginInsertRows(indexOfItem(root), row /* first row */, row /* last */);
      if (lastName) {
        // Create the final file
        ret = new FileItem(nextName, sourceKeyPrefix, root);
        ret->addExtension(extension);
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

std::string const SourcesModel::keyPrefixOfItem(SourcesModel::TreeItem const *item) const
{
  // Retrieve the key for the info:
  if (item->isDir()) return std::string();

  SourcesModel::FileItem const *file =
    dynamic_cast<SourcesModel::FileItem const *>(item);
  assert(file);
  return file->sourceKeyPrefix;
}

std::string const SourcesModel::keyPrefixOfIndex(QModelIndex const &index) const
{
  // Retrieve the key for the info:
  SourcesModel::TreeItem const *item =
    static_cast<SourcesModel::TreeItem const *>(index.internalPointer());
  return keyPrefixOfItem(item);
}

QModelIndex const SourcesModel::indexOfKeyPrefix(std::string const &prefix)
{
  TreeItem *item = itemOfKeyPrefix(prefix);
  if (! item) return QModelIndex();
  return indexOfItem(item);
}

SourcesModel::TreeItem *SourcesModel::itemOfKeyPrefix(std::string const &prefix)
{
  QStringList names =
    QString::fromStdString(prefix).split("/", QString::SkipEmptyParts);
  if (names.length() <= 1) {
    qCritical() << "Invalid key prefix" << QString::fromStdString(prefix);
    return nullptr;
  }
  names.removeFirst();  // "sources"

  TreeItem *item = root;
  do {
    if (names.isEmpty()) return item;
    QString const &nextName = names.takeFirst();
    /* Because we have at least one name left after item.
     * Crash here? Check you've sent a key _prefix_ not a full key. */
    assert(item->isDir());
    DirItem *dir = static_cast<DirItem *>(item);

    for (auto it = dir->children.begin();
         it != dir->children.end();
         it ++
    ) {
      if ((*it)->name == nextName) {
        item = *it;
        goto found;
      }
    }
    qCritical() << "Cannot find key prefix" << QString::fromStdString(prefix);
    return nullptr;
found:;
  } while (true);
}

std::shared_ptr<conf::SourceInfo const> SourcesModel::sourceInfoOfItem(TreeItem const *item) const
{
  if (item->isDir()) return nullptr;

  SourcesModel::FileItem const *file =
    dynamic_cast<SourcesModel::FileItem const *>(item);
  assert(file);

  std::string const infoKey = file->sourceKeyPrefix + "/info";

  std::shared_ptr<conf::Value const> v;
  kvs->lock.lock_shared();
  auto it = kvs->map.find(infoKey);
  if (it != kvs->map.end()) v = it->second.val;
  kvs->lock.unlock_shared();

  if (! v) return nullptr;
  return std::dynamic_pointer_cast<conf::SourceInfo const>(v);
}

void SourcesModel::delSource(std::string const &key, KValue const &)
{
  if (! isMyKey(key)) return;

  QStringList names =
    QString::fromStdString(key).split("/", QString::SkipEmptyParts);
  if (names.length() <= 2) {
    qCritical() << "Invalid source key" << QString::fromStdString(key);
    return;
  }

  names.removeFirst();  // "sources"
  QString const extension(names.takeLast());

  deleteAll(names, extension, root);
}

void SourcesModel::deleteAll(
  QStringList &names,
  QString const &extension,
  DirItem *root)
{
  assert(! names.isEmpty());

  /* Locate this name in root children: */
  int row;
  for (row = 0; row < root->children.count(); row ++) {
    if (root->children[row]->name == names.first()) {
      goto found;
    }
  }
  qCritical() << "Cannot find" << names.first() << "in children";
  return;
found:

  names.removeFirst();

  if (names.count() == 0) {
    if (verbose)
      qDebug() << "SourcesModel: Deleting extension"
               << root->children[row]->name << "/" << extension;

    FileItem *file =
      dynamic_cast<FileItem *>(root->children[row]);
    assert(file); // Because we only delete full path to files

    file->delExtension(extension);
    if (file->extensions.isEmpty()) {
      beginRemoveRows(indexOfItem(root), row, row);
      delete file;
      endRemoveRows();
    }
  } else {
    DirItem *dir = dynamic_cast<DirItem *>(root->children[row]);
    assert(dir); // Because we only delete full path to files
    deleteAll(names, extension, dir);
    if (dir->children.count() > 0) return;
    // delete that empty dir
    beginRemoveRows(indexOfItem(root), row, row);
    delete dir;
    endRemoveRows();
  }
}
