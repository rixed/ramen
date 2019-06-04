#include <cassert>
#include "conf.h"
#include "SourcesModel.h"

SourcesModel::SourcesModel(QObject *parent) :
  QAbstractItemModel(parent)
{
  root = new DirItem("");

  conf::autoconnect("^sources/.*/text", [this](conf::Key const &, KValue const *kv) {
    connect(kv, &KValue::valueCreated, this, &SourcesModel::addSourceText);
    connect(kv, &KValue::valueChanged, this, &SourcesModel::updateSourceText);
  });
  conf::autoconnect("^sources/.*/info", [this](conf::Key const &, KValue const *kv) {
    connect(kv, &KValue::valueCreated, this, &SourcesModel::addSourceInfo);
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

void SourcesModel::addSourceText(conf::Key const &k, std::shared_ptr<conf::Value const> v)
{
  int const numTrimmed = 8 + 5;
  assert(k.s.length() > numTrimmed);
  QString sourceName =
    QString::fromStdString(k.s.substr(8, k.s.length() - numTrimmed));

  // Will create all the intermediary TreeItems, calling begin/endInsertRows::
  FileItem *file = createAll(sourceName, k, root);
  if (file) {
    std::shared_ptr<conf::String const> s =
      std::dynamic_pointer_cast<conf::String const>(v);
    if (s) {
      file->setText(s);
    } else {
      std::cout << "SourceText not of string type?!" << std::endl;
    }
  }
}

void SourcesModel::updateSourceText(conf::Key const &, std::shared_ptr<conf::Value const>)
{
  // TODO
}

void SourcesModel::addSourceInfo(conf::Key const &, std::shared_ptr<conf::Value const>)
{
  // TODO
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

SourcesModel::FileItem *SourcesModel::createAll(QString const &sourceName, conf::Key const &origKey, DirItem *root)
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
    for (auto it = root->children.constBegin(); it != root->children.constEnd(); it++) {
      if (! lastName && (*it)->name == nextName && (*it)->isDir()) {
        DirItem *sub = dynamic_cast<DirItem *>(*it);
        assert(sub);  // because isDir
        root = sub;
        needNewItem = false;
        break;
      } else if ((*it)->name > nextName) {
        break;
      }
      row ++;
    }
    if (needNewItem) {
      emit beginInsertRows(indexOfItem(root), row, row);
      if (lastName) {
        // Create the final file
        ret = new FileItem(nextName, origKey, root);
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
