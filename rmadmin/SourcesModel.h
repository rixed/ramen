#ifndef SOURCESMODEL_H_190530
#define SOURCESMODEL_H_190530
#include <memory>
#include <QAbstractItemModel>
#include "confValue.h"
#include "KVPair.h"

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

    virtual ~TreeItem() = 0;

    // The number of subrows:
    virtual int numRows() const = 0;
    virtual bool isDir() const = 0;
  };

  struct DirItem : public TreeItem
  {
    QList<TreeItem *> children;

    DirItem(QString name_, TreeItem *parent_ = nullptr) :
      TreeItem(name_, parent_) {}

    ~DirItem()
    {
      // Children remove themselves from this list:
      while (children.count() > 0) delete children.first();

      if (! parent) return;
      DirItem *dir = static_cast<DirItem *>(parent);
      if (! dir->children.removeOne(this))
        std::cerr << "Dir " << name.toStdString() << " has been abandoned!"
                  << std::endl; // Life goes on
    }

    int numRows() const { return children.length(); }
    bool isDir() const { return true; }
    void addItem(TreeItem *i, int row)
    {
      children.insert(row, i);
    }
  };

  struct FileItem : public TreeItem
  {
    std::string const sourceKey;

    FileItem(QString name_, std::string const &sourceKey_, TreeItem *parent_ = nullptr) :
      TreeItem(name_, parent_), sourceKey(sourceKey_) {}

    ~FileItem()
    {
      if (! parent) return;
      DirItem *dir = static_cast<DirItem *>(parent);
      if (! dir->children.removeOne(this))
        std::cerr << "File " << name.toStdString() << " has been abandoned!"
                  << std::endl; // Life goes on
    }

    int numRows() const { return 0; }
    bool isDir() const { return false; }
  };

  DirItem *root;

private:
  QModelIndex indexOfItem(TreeItem const *) const;

  /* Construct from the root and the "absolute" name; returns the created
   * file (or nullptr if the sourceName was empty): */
  FileItem *createAll(std::string const &, QStringList &names, DirItem *);
  /* Destruct that file, and the dirs that become empty: */
  void deleteAll(QStringList &names, DirItem *);

  bool isMyKey(std::string const &) const;

public:
  SourcesModel(QObject *parent = nullptr);

  QModelIndex index(int row, int column, QModelIndex const &parent) const;
  QModelIndex parent(QModelIndex const &index) const;
  int rowCount(QModelIndex const &parent) const;
  int columnCount(QModelIndex const &parent) const;
  QVariant data(QModelIndex const &index, int role) const;

  std::string const keyOfIndex(QModelIndex const &index) const;
  std::shared_ptr<conf::SourceInfo const> sourceInfoOfItem(TreeItem const *) const;

private slots:
  void addSource(KVPair const &);
  void delSource(KVPair const &);
};

inline SourcesModel::TreeItem::~TreeItem() {} // stupid language!

/*
 * Helpers:
 */

// Returns the source file name with extension
QString const sourceNameOfKey(std::string const &);

// Returns the source file name without extension
QString const baseNameOfKey(std::string const &);

// The other way around:
std::string const keyOfSourceName(QString const &, char const *newExtension = nullptr);

#endif
