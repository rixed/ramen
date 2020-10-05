#ifndef POPUPLISTVIEW_H_201005
#define POPUPLISTVIEW_H_201005
/* like a QListView, but as a separate popup that can be closed with esc/enter */
#include <QListView>

class PopupListView : public QListView {
  Q_OBJECT

public:
  PopupListView(QWidget *parent = nullptr);

protected:
  void keyPressEvent(QKeyEvent *event) override;
};

#endif
