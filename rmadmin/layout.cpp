#include <vector>
#include <map>
#include <cassert>
#include <z3++.h>
#include "layout.h"

using namespace std;
using namespace z3;

namespace layout {

bool solve(vector<Node> *nodes, unsigned max_x, unsigned max_y)
{
  assert(max_x * max_y >= nodes->size());
  context c;
  optimize opt(c);
  params p(c);
  p.set("priority", c.str_symbol("pareto"));
  p.set(":timeout", 5000U); // 1s should be enough for everyone
  opt.set(p);

  // First, we need all function coordinates:
  vector<expr> xs, ys;
  // While at it, take notice of the existing sites and programs:
  multimap<string, size_t> sites;
  multimap<pair<string, string>, size_t> programs;

  for (size_t i = 0; i < nodes->size(); i++) {
    Node &n = (*nodes)[i];
    // Hopefully z3 will copy that name in a safe place...
    string name = n.site +"/"+ n.program +"/"+ n.function;
    xs.emplace_back(c.int_const((name + "/x").c_str()));
    ys.emplace_back(c.int_const((name + "/y").c_str()));

    // constrain the space to the given square:
    opt.add(xs.back() >= 0 && xs.back() < c.int_val(max_x));
    opt.add(ys.back() >= 0 && ys.back() < c.int_val(max_y));

    sites.insert({n.site, i});
    programs.insert({{n.site, n.program}, i});
  }

  // First set of constraints: keep the sites together.
  // ss is the min and max of the xs and the ys: (xmin, xmax), (ymin, ymnax).
  map<string, pair<pair<expr, expr>, pair<expr, expr>>> ss;
  for (auto const sites_it : sites) {
    auto it = ss.find(sites_it.first);
    size_t idx = sites_it.second;
    if (it == ss.end()) {
      ss.emplace(
        sites_it.first,
        pair<pair<expr, expr>, pair<expr, expr>>(
          { xs[idx], xs[idx] },
          { ys[idx], ys[idx] }
        )
      );
    } else {
      it->second.first.first   = min(it->second.first.first,   xs[idx]);
      it->second.first.second  = max(it->second.first.second,  xs[idx]);
      it->second.second.first  = min(it->second.second.first,  ys[idx]);
      it->second.second.second = max(it->second.second.second, ys[idx]);
    }
  }
  // Minimize the size of each sites:
  for (auto it : ss) {
    opt.minimize(it.second.first.second  - it.second.first.first);
    opt.minimize(it.second.second.second - it.second.second.first);
  }
  // Prevent different sites to intersect each others:
  for (auto s1 : ss) {
    for (auto s2 : ss) {
      if (s1.first == s2.first) continue;
      opt.add(
        s1.second.first.first   >= s2.second.first.second ||
        s1.second.first.second  <= s2.second.first.first ||
        s1.second.second.first  >= s2.second.second.second ||
        s1.second.second.second <= s2.second.second.first);
    }
  }

  // Then, also keep the programs together.
  // ps is the min and max of the xs and the ys: (xmin, xmax), (ymin, ymnax).
  map<pair<string, string>, pair<pair<expr, expr>, pair<expr, expr>>> ps;
  for (auto const programs_it : programs) {
    auto it = ps.find(programs_it.first);
    size_t idx = programs_it.second;
    if (it == ps.end()) {
      ps.emplace(
        programs_it.first,
        pair<pair<expr, expr>, pair<expr, expr>>(
          { xs[idx], xs[idx] },
          { ys[idx], ys[idx] }
        )
      );
    } else {
      it->second.first.first   = min(it->second.first.first,   xs[idx]);
      it->second.first.second  = max(it->second.first.second,  xs[idx]);
      it->second.second.first  = min(it->second.second.first,  ys[idx]);
      it->second.second.second = max(it->second.second.second, ys[idx]);
    }
  }
  // Minimize the size of each programs:
  for (auto it : ps) {
    opt.minimize(it.second.first.second  - it.second.first.first);
    opt.minimize(it.second.second.second - it.second.second.first);
  }
  // Prevent different programs to intersect each others:
  for (auto p1 : ps) {
    for (auto p2 : ps) {
      if (p1.first == p2.first) continue;
      opt.add(
        p1.second.first.first   >= p2.second.first.second ||
        p1.second.first.second  <= p2.second.first.first ||
        p1.second.second.first  >= p2.second.second.second ||
        p1.second.second.second <= p2.second.second.first);
    }
  }

  // Minimize the number of back-links, and the height of links, considering
  // a back link is as bad as 10 steps in y:
  expr one  = c.int_val(1);
  expr zero = c.int_val(0);
  expr back = c.int_val(10);
  expr dy_ok = c.int_val(4);
  expr numBadLinks = zero;
  for (size_t i = 0; i < nodes->size(); i++) {
    Node &n = (*nodes)[i];
    for (size_t p : n.parents) {
      numBadLinks = numBadLinks
                  + ite(xs[p] >= xs[i], back, zero)
                  + ite(ys[p] > ys[i] + dy_ok,
                          ys[p] - ys[i] - dy_ok,
                          ite(ys[i] > ys[p] + dy_ok,
                                ys[i] - ys[p] - dy_ok,
                                zero));
    }
  }
  opt.minimize(numBadLinks);

  // TODO: optionally, favor preexisting positions

  // Finally (for now), do not allow functions to get too close, ie to be
  // in the same "tile".
  expr_vector tiles(c);
  for (unsigned i = 0; i < xs.size(); i++) {
    tiles.push_back(ys[i] * c.int_val((unsigned)xs.size()) + xs[i]);
  }
  opt.add(distinct(tiles));

  std::cout << opt << "\n";

  switch (opt.check()) {
    case unsat:
      cerr << "Cannot solve layout!?\n";
      return false;
    case unknown:
      cerr << "Timeout while solving layout, YMMV\n";
      break;
    case sat:
      break;
  }

  model m = opt.get_model();
  cout << "num bad links = " << m.eval(numBadLinks, true).get_numeral_uint() << '\n';
  for (unsigned i = 0; i < xs.size(); i++) {
    Node &n = (*nodes)[i];
    n.x = m.eval(xs[i], true).get_numeral_uint();
    n.y = m.eval(ys[i], true).get_numeral_uint();
    cout << n.site << "/" << n.program << "/" << n.function << " at "<< n.x <<", "<< n.y << '\n';
  }

  return true;
}

};
