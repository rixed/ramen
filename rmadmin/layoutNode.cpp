#include "layoutNode.h"

namespace layout {

// Initialization

Node::Node(std::string const &site_, std::string const &program_, std::string const &function_) :
  site(site_), program(program_), function(function_)
{
  parents.reserve(5);
}

Node::~Node() {}

void Node::addParent(size_t idx)
{
  parents.push_back(idx);
}

};
