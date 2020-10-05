#include <QKeyEvent>
#include "PopupListView.h"

PopupListView::PopupListView(QWidget *parent)
  : QListView(parent)
{
  /* Not using Qt::Popup because we do not want this to be modal: */
  setWindowFlags(Qt::Tool | Qt::FramelessWindowHint);
  setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Preferred);
}

/* Since this is not modal the window won't automatically close on escape
 * so let's reimplement this: */
void PopupListView::keyPressEvent(QKeyEvent *event)
{
  switch (event->key()) {
    case Qt::Key_Escape:
    case Qt::Key_Return:
    case Qt::Key_Enter:
      hide();
      break;
    default:
      QListView::keyPressEvent(event);
      break;
  }
}
