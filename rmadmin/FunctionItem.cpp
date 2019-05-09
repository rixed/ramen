#include "FunctionItem.h"

FunctionItem::FunctionItem(OperationsItem *parent, QString name_) :
  OperationsItem(parent, Qt::blue),
  name(name_)
{}

FunctionItem::~FunctionItem() {}

QVariant FunctionItem::data(int column) const
{
  assert(column == 0);
  return QVariant(name);
}

void FunctionItem::setProperty(QString const &p, std::shared_ptr<conf::Value const> v)
{
  static QString const parents_prefix("parents/");
  if (p == "is_used") {
    std::shared_ptr<conf::Bool const> b =
      std::dynamic_pointer_cast<conf::Bool const>(v);
    if (b) isUsed = b->b;
  } else if (p.startsWith(parents_prefix)) {
    int idx = p.mid(parents_prefix.length()).toInt();
    if (idx >= 0 && idx < 100000) {
      std::shared_ptr<conf::Worker const> w =
        std::dynamic_pointer_cast<conf::Worker const>(v);
      if (w) {
        if ((size_t)idx >= parents.size()) parents.resize(idx+1);
        parents[idx] = LazyRef<conf::Worker, FunctionItem>(*w);
        std::cout << "Setting parent " << idx << std::endl;
      } else {
        std::cerr << "Ignoring function parent " << idx
                  << " because it is not a worker" << std::endl;
      }
    } else {
      std::cerr << "Ignoring bogus request to alter function "
                << idx << "th parent" << std::endl;
    }
  }
}
