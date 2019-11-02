#include <vector>
#include <map>
#include <cassert>
#include <chrono>
#include <z3++.h>
#include <QDebug>
#include "layout.h"

using namespace std;
using namespace z3;

namespace layout {

#define SINGLE_COST

//#define TOT_SIZE_SMALL
#define MIN_BACKLINKS_FUNC
//#define GLOBAL_TILE_EXCLUSION
#define SINGLE_MIN_SIZE // not relevant if SINGLE_COST
//#define MIN_BACKLINKS_PROG
//#define MIN_BACKLINKS_SITE
#define SOURCES_AT_0
#define MATERIALIZE_SITES
#define MATERIALIZE_PROGS

bool solve(vector<Node> *nodes, unsigned max_x, unsigned max_y)
{
  assert(max_x * max_y >= nodes->size());
  context c;
  optimize opt(c);
  params p(c);
  p.set("priority", c.str_symbol("pareto"));
  p.set(":timeout", 8000U); // 8s should be enough for everyone
  opt.set(p);
  expr one  = c.int_val(1);
  expr zero = c.int_val(0);

  // First, we need all function coordinates:
  vector<expr> xs, ys;
  // While at it, take notice of the existing sites and programs and of
  // the connections between sites:
  multimap<string, size_t> sites;
  multimap<pair<string, string>, size_t> programs;
  // Connections between sites and between programs of the same sites:
  multimap<string, string> siteToSites;
  multimap<pair<string, string>, pair<string, string>> progToProgs;

  for (size_t i = 0; i < nodes->size(); i++) {
    Node &n = (*nodes)[i];
    // Hopefully z3 will copy that name in a safe place...
    string name = n.site +"/"+ n.program +"/"+ n.function;
    xs.emplace_back(c.int_const((name + "/x").c_str()));
    ys.emplace_back(c.int_const((name + "/y").c_str()));

    // Constrain the space to the given square:
#ifndef MATERIALIZE_SITES
    opt.add(xs.back() >= 0 && xs.back() < c.int_val(max_x));
    opt.add(ys.back() >= 0 && ys.back() < c.int_val(max_y));
#endif

    sites.insert({n.site, i});
    programs.insert({{n.site, n.program}, i});
    for (size_t pIdx : n.parents) {
      Node &p = (*nodes)[pIdx];
      if (p.site == n.site) {
        if (p.program != n.program)
          progToProgs.insert({{p.site, p.program}, {n.site, n.program}});
      } else {
        siteToSites.insert({p.site, n.site});
      }
    }
  }

# ifdef SINGLE_COST
  expr cost = zero; // a single expression to minimize is simpler
# endif

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
#ifdef MATERIALIZE_SITES
  for (auto it = ss.begin(); it != ss.end(); it++) {
    string name = it->first;

    expr e = c.int_const((name + "/x/min").c_str());
    opt.add(e == it->second.first.first && e >= 0 && e < c.int_val(max_x));
    it->second.first.first = e;

    e = c.int_const((name + "/x/max").c_str());
    opt.add(e == it->second.first.second && e >= 0 && e < c.int_val(max_x));
    it->second.first.second = e;

    e = c.int_const((name + "/y/min").c_str());
    opt.add(e == it->second.second.first && e >= 0 && e < c.int_val(max_y));
    it->second.second.first = e;

    e = c.int_const((name + "/y/max").c_str());
    opt.add(e == it->second.second.second && e >= 0 && e < c.int_val(max_y));
    it->second.second.second= e;
  }
#endif

  // Minimize the size of each sites:
  for (auto it : ss) {
#ifdef SINGLE_COST
    cost = cost +
      (it.second.first.second  - it.second.first.first) +
      (it.second.second.second - it.second.second.first);
#else
# ifdef SINGLE_MIN_SIZE
    opt.minimize(
      (it.second.first.second  - it.second.first.first) +
      (it.second.second.second - it.second.second.first)
    );
# else
    opt.minimize(it.second.first.second  - it.second.first.first);
    opt.minimize(it.second.second.second - it.second.second.first);
# endif
#endif
  }
  // Prevent different sites to intersect each others:
  for (auto s1 : ss) {
    for (auto s2 : ss) {
      if (s1.first >= s2.first) continue;
      opt.add(
        s1.second.first.first   > s2.second.first.second ||
        s1.second.first.second  < s2.second.first.first ||
        s1.second.second.first  > s2.second.second.second ||
        s1.second.second.second < s2.second.second.first);
    }
  }

#ifdef MIN_BACKLINKS_SITE
  // Also minimize the number of child sites not at the right of their
  // parents:
  expr numBadSiteLinks = zero;
  for (auto const connSites_it : siteToSites) {
    auto srcSitePos = ss.find(connSites_it.first);
    auto dstSitePos = ss.find(connSites_it.second);
    numBadSiteLinks = numBadSiteLinks
                    + ite(dstSitePos->second.first.first >
                          srcSitePos->second.first.second,
                          zero, one);
  }
# ifdef SINGLE_COST
  cost = cost + numBadSiteLinks;
# else
  opt.minimize(numBadSiteLinks);
# endif
#endif

#ifdef TOT_SIZE_SMALL
  // Also, keep the max x and max y of all the sites as small as possible,
  // to avoid sprawl (minimize the max of xmax+ymax):
  expr maxTotSize = zero;
  for (auto s : ss) {
    maxTotSize =
      max(maxTotSize, s.second.first.second + s.second.second.second);
  }
# ifdef SINGLE_COST
  cost = cost + maxTotSize;
# else
  opt.minimize(maxTotSize);
# endif
#endif

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
#ifdef MATERIALIZE_PROGS
  for (auto it = ps.begin(); it != ps.end(); it++) {
    string name = it->first.first +"/"+ it->first.second;

    expr e = c.int_const((name + "/x/min").c_str());
    opt.add(e == it->second.first.first);
    it->second.first.first = e;

    e = c.int_const((name + "/x/max").c_str());
    opt.add(e == it->second.first.second);
    it->second.first.second = e;

    e = c.int_const((name + "/y/min").c_str());
    opt.add(e == it->second.second.first);
    it->second.second.first = e;

    e = c.int_const((name + "/y/max").c_str());
    opt.add(e == it->second.second.second);
    it->second.second.second= e;
  }
#endif

  // Minimize the size of each programs:
  for (auto it : ps) {
#ifdef SINGLE_COST
    cost = cost +
      (it.second.first.second  - it.second.first.first) +
      (it.second.second.second - it.second.second.first);
#else
# ifdef SINGLE_MIN_SIZE
    opt.minimize(
      (it.second.first.second  - it.second.first.first) +
      (it.second.second.second - it.second.second.first)
    );
# else
    opt.minimize(it.second.first.second  - it.second.first.first);
    opt.minimize(it.second.second.second - it.second.second.first);
# endif
#endif
  }
  // Prevent different programs (of same site) to intersect each others:
  for (auto p1 : ps) {
    for (auto p2 : ps) {
      if (p1.first.first != p2.first.first ||
          p1.first.second >= p2.first.second) continue;
      opt.add(
        p1.second.first.first   > p2.second.first.second ||
        p1.second.first.second  < p2.second.first.first ||
        p1.second.second.first  > p2.second.second.second ||
        p1.second.second.second < p2.second.second.first);
    }
  }

#ifdef MIN_BACKLINKS_PROG
  // Also within each site, minimize the number of child programs not at the
  // right of their parents:
  expr numBadProgLinks = zero;
  for (auto const connProgs_it : progToProgs) {
    auto srcProgPos = ps.find(connProgs_it.first);
    auto dstProgPos = ps.find(connProgs_it.second);
    numBadProgLinks = numBadProgLinks
                    + ite(dstProgPos->second.first.first >
                          srcProgPos->second.first.second,
                          zero, one);
  }
# ifdef SINGLE_COST
  cost = cost + numBadProgLinks;
# else
  opt.minimize(numBadProgLinks);
# endif
#endif

#ifdef MIN_BACKLINKS_FUNC
  // Minimize the number of back-links, and the height of links, considering
  // a back link is as bad as 10 steps in y:
  expr back = c.int_val(10);
  // Favor going down:
  expr dy_ok_neg = c.int_val(1);
  expr dy_ok_pos = c.int_val(4);
  expr numBadLinks = zero;
  for (size_t i = 0; i < nodes->size(); i++) {
    Node &n = (*nodes)[i];
    for (size_t p : n.parents) {
      numBadLinks = numBadLinks
                  + ite(xs[p] >= xs[i], back, zero)
                  + ite(ys[p] > ys[i] + dy_ok_neg,
                          ys[p] - ys[i] - dy_ok_neg,
                          ite(ys[i] > ys[p] + dy_ok_pos,
                                ys[i] - ys[p] - dy_ok_pos,
                                zero));
    }
  }
# ifdef SINGLE_COST
  cost = cost + numBadLinks;
# else
  opt.minimize(numBadLinks);
# endif
#endif

#ifdef SOURCES_AT_0
  // Any function with no input has X=0:
  for (size_t i = 0; i < nodes->size(); i++) {
    Node &n = (*nodes)[i];
    if (n.parents.size() == 0) {
      opt.add(xs[i] == zero);
    }
  }
#endif

# ifdef SINGLE_COST
  opt.minimize(cost);
# endif

  // TODO: optionally, favor preexisting positions

  // Finally (for now), do not allow functions to get too close, ie to be
  // in the same "tile".
  // TODO: we should be able to have a small distinct per program given the
  // above. try it.
#ifdef GLOBAL_TILE_EXCLUSION
  expr_vector tiles(c);
  for (size_t i = 0; i < xs.size(); i++) {
    tiles.push_back(ys[i] * c.int_val((int)xs.size()) + xs[i]);
  }
  opt.add(distinct(tiles));
#else
  map<pair<string, string>, expr_vector> tilesOfProgram;
  for (size_t i = 0; i < nodes->size(); i++) {
    Node &n = (*nodes)[i];
    qDebug() << "node[" << i << "] for" << QString::fromStdString(n.site)
             << "," << QString::fromStdString(n.program);
    pair<string, string> const k(n.site, n.program);
    expr pos = ys[i] * c.int_val((int)xs.size()) + xs[i];
    auto it = tilesOfProgram.find(k);
    if (it == tilesOfProgram.end()) {
      expr_vector v(c);
      v.push_back(pos);
      tilesOfProgram.emplace(k, v);
    } else {
      it->second.push_back(pos);
    }
  }
  for (auto &it : tilesOfProgram) {
    if (it.second.size() > 1)
      opt.add(distinct(it.second));
  }
#endif

  //qDebug() << opt;

  auto start = chrono::high_resolution_clock::now();

  switch (opt.check()) {
    case unsat:
      qDebug() << "Cannot solve layout!?";
      return false;
    case unknown:
      qDebug() << "Timeout while solving layout, YMMV";
      break;
    case sat:
      auto stop = chrono::high_resolution_clock::now();
      auto duration = chrono::duration_cast<chrono::milliseconds>(stop - start);
      qDebug() << "Solved in:" <<duration.count() << "ms";
      break;
  }

  model m = opt.get_model();
# ifdef MIN_BACKLINKS_FUNC
//  qDebug() << "num bad links =" << m.eval(numBadLinks, true).get_numeral_uint();
  qDebug() << "cost =" << m.eval(cost, true).get_numeral_uint();
# endif
  for (size_t i = 0; i < xs.size(); i++) {
    Node &n = (*nodes)[i];
    n.x = m.eval(xs[i], true).get_numeral_uint();
    n.y = m.eval(ys[i], true).get_numeral_uint();
    qDebug() << QString::fromStdString(n.site) << "/"
             << QString::fromStdString(n.program) << "/"
             << QString::fromStdString(n.function) << "at" << n.x << "," << n.y;
  }

  return true;
}

};
