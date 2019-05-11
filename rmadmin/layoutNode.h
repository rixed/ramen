#ifndef LAYOUTNODE_H_190510
#define LAYOUTNODE_H_190510
#include <string>
#include <vector>

namespace layout {

/* The problem we pass to the solver is expressed with the help of this
 * simple objects: */
struct Node {
  // User defined properties:
  std::string site;
  std::string program;
  std::string function;
  std::vector<size_t> parents;  // indices in the array passed to solve

  // Here the solver will write the solution (in _tiles_)
  int x, y;

  Node(std::string const &, std::string const &, std::string const &);
  ~Node();
  void addParent(size_t idx);
};

};

#endif
