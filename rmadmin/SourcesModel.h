#ifndef SOURCESMODEL_H_190530
#define SOURCESMODEL_H_190530
#include <memory>
#include <QAbstractItemModel>
#include <QDebug>
#include <QtGlobal>
#include "conf.h"
#include "confValue.h"

struct KValue;

class SourcesModel : public QAbstractItemModel
{
  Q_OBJECT

public:
  enum Columns {
    SrcPath = 0,
    Action1,
    Action2,
    NumColumns,
  };

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

    bool isRoot() const { return parent == nullptr; }

    QString fqName() const
    {
      return
        !parent || parent->isRoot() ?
            name :
            parent->fqName() + "/" + name;
    }
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
        qWarning() << "Dir" << name
                   << "has been abandoned!"; // Life goes on
    }

    int numRows() const { return children.length(); }
    bool isDir() const { return true; }
    void addItem(TreeItem *i, int row)
    {
      children.insert(row, i);
    }
  };

  /* Files encompass all existing source extensions. Having another layer in
   * the source-tree just for extensions would not be convenient as most of the
   * time the only extension present will be "ramen". It's best to add an
   * additional extension switcher in the source editor when it makes sense. */
  struct FileItem : public TreeItem
  {
    /* Without the extension: */
    std::string const sourceKeyPrefix;

    QList<QString> extensions;

    FileItem(QString name_, std::string const &sourceKeyPrefix_,
             TreeItem *parent_ = nullptr) :
      TreeItem(name_, parent_), sourceKeyPrefix(sourceKeyPrefix_) {}

    ~FileItem()
    {
      if (! parent) return;
      DirItem *dir = static_cast<DirItem *>(parent);
      if (! dir->children.removeOne(this))
        qCritical() << "File" << name << "has been abandoned!";
    }

    int numRows() const { return 0; }
    bool isDir() const { return false; }

    void addExtension(QString const &extension)
    {
      if (extensions.contains(extension)) return;
      extensions += extension;
    }

    void delExtension(QString const &extension)
    {
      extensions.removeOne(extension);
    }
  };

  DirItem *root;

  QModelIndex indexOfItem(TreeItem const *) const;

private:
  /* Construct from the root and the "absolute" name; returns the created
   * file (or nullptr if the sourceName was empty): */
  FileItem *createAll(
    std::string const &, QStringList &names, QString const &extension, DirItem *);
  /* Destruct that file, and the dirs that become empty: */
  void deleteAll(QStringList &names, QString const &extension, DirItem *);

  bool isMyKey(std::string const &) const;

  void addSource(std::string const &, KValue const &);
  void delSource(std::string const &, KValue const &);

public:
  SourcesModel(QObject *parent = nullptr);

  QModelIndex index(int row, int column, QModelIndex const &parent) const;
  QModelIndex parent(QModelIndex const &index) const;
  int rowCount(QModelIndex const &parent) const;
  int columnCount(QModelIndex const &parent) const;
  QVariant data(QModelIndex const &index, int role) const;

  std::string const keyPrefixOfItem(SourcesModel::TreeItem const *) const;
  std::string const keyPrefixOfIndex(QModelIndex const &index) const;
  TreeItem *itemOfKeyPrefix(std::string const &);
  QModelIndex const indexOfKeyPrefix(std::string const &);
  std::shared_ptr<conf::SourceInfo const> sourceInfoOfItem(TreeItem const *) const;

private slots:
  void onChange(QList<ConfChange> const &);
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
