#ifndef RAMENSYNTAXHIGHLIGHTER_190729
#define RAMENSYNTAXHIGHLIGHTER_190729
#include <QSyntaxHighlighter>

class RamenSyntaxHighlighter : public QSyntaxHighlighter
{
  Q_OBJECT

public:
  RamenSyntaxHighlighter(QTextDocument *parent = nullptr);

protected:
  void highlightBlock(const QString &text);
};

#endif
