#include <cassert>
#include <vector>
#include <QDebug>
#include <QTextCharFormat>
#include <QRegularExpression>
#include "RamenSyntaxHighlighter.h"

RamenSyntaxHighlighter::RamenSyntaxHighlighter(QTextDocument *parent) :
  QSyntaxHighlighter(parent) {}

struct Rules {
  struct SyntaxRule {
    QTextCharFormat format;
    QRegularExpression re;
    SyntaxRule(QTextCharFormat format_, QString str) :
      format(format_),
      re(str, QRegularExpression::CaseInsensitiveOption |
              QRegularExpression::DontCaptureOption)
    {
      if (! re.isValid()) {
        qDebug() << "Invalid regexp" << str << ":" << re.errorString();
        assert(!"Invalid RE");
      }
      re.optimize();
    }
  };

  std::vector<SyntaxRule> rules;

  /* Formats overwrite each others (on a char by char basis). So the rules
   * that come last overwrite earlier rules. */
  Rules() {
    QTextCharFormat keyword;
    keyword.setFontWeight(QFont::Bold);
    keyword.setForeground(Qt::darkGreen);
    rules.emplace_back(keyword,
      "\\b(parameter|parameters|default|defaults|to|run|if|define|lazy|"
          "persist|for|querying|query|every|while|as|doc|event|starts|starting|"
          "at|with|duration|stops|stopping|ending|select|"
          "yield|merge|on|timeout|after|sort|until|by|where|"
          "group|notify|commit|flush|keep|all|before|collectd|netflow|"
          "graphite|listen|read|delete|file|separator|no|escape|"
          "preprocess|factor|factors|from|this|site|sites|globally|"
          "locally|skip|nulls)\\b");

    QTextCharFormat ops;
    ops.setFontWeight(QFont::Bold);
    rules.emplace_back(ops,
      "\\b(>|>=|<|<=|=|<>|!=|in|not|like|starts|ends|"
          "\\+|-|\\|\\||\\|\\?|\\*|//|/|%|&|\\||#|<<|>>|^|is|null|"
          "begin|end)\\b");

    QTextCharFormat func;
    func.setFontWeight(QFont::Bold);
    func.setForeground(Qt::darkYellow);
    rules.emplace_back(func,
      "\\b(age|abs|length|lower|upper|now|random|exp|log|log10|sqrt|"
          "ceil|floor|round|truncate|hash|min|max|sum|avg|and|or|first|"
          "largest|smallest|latest|oldest|earlier|all|percentile|th|lag|"
          "season_moveavg|moveavg|"
          "season_fit|fit|season_fit_multi|fit_multi|smooth|remember|"
          "distinct|hysteresis|histogram|split|format_time|parse_time|"
          "variant|max|greatest|min|least|print|reldiff|sample|substring|"
          "get|changed|rank|of|is|in|top|over|at|past|count|"
          "st|nd|rd|case|when|end|if|then|else|coalesce)\\b");

    QTextCharFormat number;
    number.setFontWeight(QFont::Bold);
    number.setForeground(Qt::darkCyan);
    rules.emplace_back(number, "[0-9]+");

    QTextCharFormat character;
    character.setFontWeight(QFont::Bold);
    character.setFontItalic(true);
    character.setForeground(Qt::darkMagenta);
    rules.emplace_back(character,
      "#\\\\[^\\\\]|" // char non escaped
      "#\\\\\\\\[0-9a-zA-Z]+" // escaped char
      );

    QTextCharFormat type;
    type.setFontWeight(QFont::Bold);
    type.setForeground(Qt::blue);
    rules.emplace_back(type,
      "\\b(float|string|bool|boolean|u8|u16|u32|u64|u128|i8|i16|i32|i64|i128|"
          "eth|ip4|ip6|ip|cidr4|cidr6|cidr)\\b");

    QTextCharFormat units;
    units.setForeground(Qt::gray);
    rules.emplace_back(units, "{[^{]*}");

    QTextCharFormat string;
    string.setFontWeight(QFont::Bold);
    string.setFontItalic(true);
    string.setForeground(Qt::darkMagenta);
    rules.emplace_back(string,
      "\"\"|" // empty string
      "\"[^\"\\\\]\"|" // string with a single char
      "\"(\\\\.|[^\"\\\\]{2})+[^\"\\\\]?\"|" // or with escaped chars at even positions
      "\"[^\"\\\\](\\\\.|[^\"\\\\]{2})+[^\"\\\\]?\""); // or at odd positions

    QTextCharFormat comment;
    comment.setForeground(Qt::gray);
    rules.emplace_back(comment, "--.*$");
  }
};

static Rules rules;

void RamenSyntaxHighlighter::highlightBlock(const QString &text)
{
  for (auto &rule : rules.rules) {
    QRegularExpressionMatchIterator i = rule.re.globalMatch(text);
    while (i.hasNext()) {
      QRegularExpressionMatch m = i.next();
      setFormat(m.capturedStart(), m.capturedLength(), rule.format);
    }
  }
}
