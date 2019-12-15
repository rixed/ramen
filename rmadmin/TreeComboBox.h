#ifndef TREECOMBOBOX_H_191215
#define TREECOMBOBOX_H_191215
/* Like a QComboBox, but that actually work with trees.
 * Notice that the displayed text is that of the model DisplayRole,
 * so there is no way to display the full path of an item (TODO).
 *
 * Also, given the QComboBox does not return the QModelIndex of the
 * selected item but only, at best, it's integer index, then the actual
 * item has to be retrieved from the model "by hand".
 *
 * Finally, to make it usable as a FunctionSelector, it must also be
 * possible to prevent the selection of any non-leaf item. */
#include <QComboBox>

class QTreeView;
class QWidget;

class TreeComboBox : public QComboBox
{
  Q_OBJECT
  Q_PROPERTY(bool allowNonLeafSelection
             READ allowNonLeafSelection
             WRITE setAllowNonLeafSelection)

  bool m_allowNonLeafSelection;

protected:
  QTreeView *treeView;

public:
  TreeComboBox(QWidget *parent = nullptr);
  void showPopup() override;
  QModelIndex getCurrent() const;

  bool allowNonLeafSelection() const { return m_allowNonLeafSelection; }
  void setAllowNonLeafSelection(bool v) { m_allowNonLeafSelection = v; }
};

#endif
