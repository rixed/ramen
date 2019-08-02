#include <cassert>
#include <QApplication>
#include <QStyle>
#include "conf.h"
#include "once.h"
#include "Resources.h"
#include "SourcesModel.h"

static bool const debug = false;

SourcesModel::SourcesModel(QObject *parent) :
  QAbstractItemModel(parent)
{
  root = new DirItem("");

  conf::autoconnect("^sources/.*/ramen$", [this](conf::Key const &, KValue const *kv) {
    Once::connect(kv, &KValue::valueCreated, this, &SourcesModel::addSource);
    // TODO: del
  });
}

#define NUM_COLUMNS 3

QModelIndex SourcesModel::index(int row, int column, QModelIndex const &parent) const
{
  if (row < 0 || column < 0 || column >= NUM_COLUMNS ) return QModelIndex();

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
  return NUM_COLUMNS;
}

QVariant SourcesModel::data(QModelIndex const &index, int role) const
{
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

void SourcesModel::addSource(conf::Key const &k, std::shared_ptr<conf::Value const>)
{
  createAll(k, root);
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

SourcesModel::FileItem *SourcesModel::createAll(conf::Key const &sourceKey, DirItem *root)
{
  QStringList names =
    QString::fromStdString(sourceKey.s).split("/", QString::SkipEmptyParts);
  if (names.length() <= 2) return nullptr;
  names.removeFirst();  // "sources"
  QString const ext = names.takeLast();

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
      emit beginInsertRows(indexOfItem(root), row /* first row */, row /* last */);
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
      emit endInsertRows();
    }
  } while (!ret);

  return ret;
}

conf::Key const SourcesModel::keyOfIndex(QModelIndex const &index) const
{
  // Retrieve the key for the info:
  SourcesModel::TreeItem const *item =
    static_cast<SourcesModel::TreeItem const *>(index.internalPointer());
  if (item->isDir()) return conf::Key::null;

  SourcesModel::FileItem const *file =
    dynamic_cast<SourcesModel::FileItem const *>(item);
  return file->sourceKey;
}

std::shared_ptr<conf::SourceInfo const> SourcesModel::sourceInfoOfItem(TreeItem const *item) const
{
  if (item->isDir()) return nullptr;

  SourcesModel::FileItem const *file =
    dynamic_cast<SourcesModel::FileItem const *>(item);

  conf::Key const infoKey = conf::changeSourceKeyExt(file->sourceKey, "info");

  KValue const *kv = nullptr;
  conf::kvs_lock.lock_shared();
  if (conf::kvs.contains(infoKey)) kv = &conf::kvs[infoKey];
  conf::kvs_lock.unlock_shared();

  if (! kv) return nullptr;
  return std::dynamic_pointer_cast<conf::SourceInfo const>(kv->val);
}
