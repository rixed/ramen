#ifndef GROWINGTEXTEDIT_H_200405
#define GROWINGTEXTEDIT_H_200405
/* A QTextEdit which (prefered) size grows with its content, inspired from
 * https://stackoverflow.com/a/47711588 */
#include <QTextEdit>

class GrowingTextEdit : public QTextEdit
{
  Q_OBJECT

public:
  GrowingTextEdit(QWidget *parent = nullptr);

  QSize sizeHint() const override;

protected:
  void resizeEvent(QResizeEvent *) override;

public slots:
  void onTextChange();
};

#endif
