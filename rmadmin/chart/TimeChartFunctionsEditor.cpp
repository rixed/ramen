#include <QToolBox>
#include <QVBoxLayout>
#include "confValue.h"
#include "chart/TimeChartFunctionEditor.h"
#include "chart/TimeChartFunctionFieldsModel.h"

#include "chart/TimeChartFunctionsEditor.h"

TimeChartFunctionsEditor::TimeChartFunctionsEditor(QWidget *parent)
  : QToolBox(parent)
{}

bool TimeChartFunctionsEditor::setValue(
  std::shared_ptr<conf::DashboardWidgetChart const> v)
{
  /* Sources are ordered by name */

  size_t v_i(0); // index in v->sources
  int t_i(0); // index in items
  for (;
    v_i < v->sources.size() || t_i < count();
    v_i++, t_i++
  ) {
    if (v_i >= v->sources.size()) {
      allFieldsChanged(t_i);
      removeItem(t_i--);
    } else if (t_i >= count()) {
      conf::DashboardWidgetChart::Source const &src = v->sources[v_i];
      TimeChartFunctionEditor *e = new TimeChartFunctionEditor(
        src.site, src.program, src.function);
      connect(e, &TimeChartFunctionEditor::fieldChanged,
              this, &TimeChartFunctionsEditor::fieldChanged);
      e->setValue(src);
      addItem(e, src.name);
    } else {
      int const c(v->sources[v_i].name.compare(itemText(t_i)));
      if (c == 0) {
        TimeChartFunctionEditor *e =
          static_cast<TimeChartFunctionEditor *>(widget(t_i));
        e->setValue(v->sources[v_i]);
      } else if (c < 0) {
        // v->source comes first
        conf::DashboardWidgetChart::Source const &src = v->sources[v_i];
        TimeChartFunctionEditor *e = new TimeChartFunctionEditor(
          src.site, src.program, src.function);
        connect(e, &TimeChartFunctionEditor::fieldChanged,
                this, &TimeChartFunctionsEditor::fieldChanged);
        e->setValue(src);
        (void)insertItem(v_i, e, src.name);
      } else if (c > 0) {
        // QToolBox item comes first, and must therefore be deleted
        allFieldsChanged(t_i);
        removeItem(t_i--);
      }
    }
  }

  return true;
}

void TimeChartFunctionsEditor::allFieldsChanged(int tab_idx)
{
  TimeChartFunctionEditor *e =
    static_cast<TimeChartFunctionEditor *>(widget(tab_idx));
  conf::DashboardWidgetChart::Source const &source(e->model->source);
  for (conf::DashboardWidgetChart::Column const &field : source.fields) {
    emit fieldChanged(source.site, source.program, source.function, field.name);
  }
}
