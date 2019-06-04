#ifndef SOURCESMODEL_H_190530
#define SOURCESMODEL_H_190530
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
    // As the key might have additional '/' it's safer to store it as is:
    conf::Key origKey;
    std::shared_ptr<conf::String const> origText;
    std::optional<conf::SourceInfo> sourceInfo;
    // TODO: current key and text, if they have been changed

    FileItem(conf::Key const &origKey_) : origKey(origKey_) {}
    FileItem(QString name_, conf::Key const &origKey_, TreeItem *parent_ = nullptr) :
      TreeItem(name_, parent_), origKey(origKey_) {}
    int numRows() const { return 0; }
    bool isDir() const { return false; }
    void setText(std::shared_ptr<conf::String const> s) { origText = s; }
  };

  DirItem *root;

private:
  QModelIndex indexOfItem(TreeItem const *) const;

  // Construct from the root and the "absolute" name; returns the created
  // file (or nullptr if the sourceName was empty):
  FileItem *createAll(QString const &sourceName, conf::Key const &origKey, DirItem *);

public:
  SourcesModel(QObject *parent = nullptr);

  QModelIndex index(int row, int column, QModelIndex const &parent) const;
  QModelIndex parent(QModelIndex const &index) const;
  int rowCount(QModelIndex const &parent) const;
  int columnCount(QModelIndex const &parent) const;
  QVariant data(QModelIndex const &index, int role) const;

private slots:
  void addSourceText(conf::Key const &, std::shared_ptr<conf::Value const>);
  void updateSourceText(conf::Key const &, std::shared_ptr<conf::Value const>);
  void addSourceInfo(conf::Key const &, std::shared_ptr<conf::Value const>);
  void updateSourceInfo(conf::Key const &, std::shared_ptr<conf::Value const>);
};

#endif
