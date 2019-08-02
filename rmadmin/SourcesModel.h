#ifndef SOURCESMODEL_H_190530
#define SOURCESMODEL_H_190530
#include <memory>
#include <QAbstractItemModel>
#include "confValue.h"
#include "confKey.h"

class SourcesModel : public QAbstractItemModel
{
  Q_OBJECT

  friend class SourcesView;

protected:
  // The tree of source files is stored as a list of those:
  struct TreeItem
  {
    QString name;
    TreeItem *parent; // or nullptr for root

    TreeItem() : parent(nullptr) {}
    TreeItem(QString name_, TreeItem *parent_ = nullptr) :
      name(name_), parent(parent_) {}

    // The number of subrows:
    virtual int numRows() const = 0;
    virtual bool isDir() const = 0;
  };

  struct DirItem : public TreeItem
  {
    QList<TreeItem *> children;

    DirItem(QString name_, TreeItem *parent_ = nullptr) :
      TreeItem(name_, parent_) {}

    int numRows() const { return children.length(); }
    bool isDir() const { return true; }
    void addItem(TreeItem *i, int row)
    {
      children.insert(row, i);
    }
  };

  struct FileItem : public TreeItem
  {
    conf::Key const sourceKey;

    FileItem(QString name_, conf::Key const &sourceKey_, TreeItem *parent_ = nullptr) :
      TreeItem(name_, parent_), sourceKey(sourceKey_) {}

    int numRows() const { return 0; }
    bool isDir() const { return false; }
  };

  DirItem *root;

private:
  QModelIndex indexOfItem(TreeItem const *) const;

  // Construct from the root and the "absolute" name; returns the created
  // file (or nullptr if the sourceName was empty):
  FileItem *createAll(conf::Key const &, DirItem *);

public:
  SourcesModel(QObject *parent = nullptr);

  QModelIndex index(int row, int column, QModelIndex const &parent) const;
  QModelIndex parent(QModelIndex const &index) const;
  int rowCount(QModelIndex const &parent) const;
  int columnCount(QModelIndex const &parent) const;
  QVariant data(QModelIndex const &index, int role) const;

  conf::Key const keyOfIndex(QModelIndex const &index) const;
  std::shared_ptr<conf::SourceInfo const> sourceInfoOfItem(TreeItem const *) const;

private slots:
  void addSource(conf::Key const &, std::shared_ptr<conf::Value const>);
};

/*
 * Helpers:
 */

// Returns the source file name with extension
QString const sourceNameOfKey(conf::Key const &);

// Returns the source file name without extension
QString const baseNameOfKey(conf::Key const &);

// The other way around:
conf::Key const keyOfSourceName(QString const &, char const *newExtension = nullptr);

#endif
