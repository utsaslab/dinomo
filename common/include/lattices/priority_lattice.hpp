#ifndef INCLUDE_LATTICES_PRIORITY_LATTICE_HPP_
#define INCLUDE_LATTICES_PRIORITY_LATTICE_HPP_

#include "core_lattices.hpp"

template <class P, class V>
struct PriorityValuePair {
  P priority;
  V value;

  // Initialize at a high value since the merge logic is taking the minimum
  PriorityValuePair(P p = INT_MAX, V v = {}) : priority(p), value(v) {}

  unsigned size() { return sizeof(P) + value.size(); }
};

template <class P, class V, class Compare = std::less<P>>
class PriorityLattice : public Lattice<PriorityValuePair<P, V>> {
  using Element = PriorityValuePair<P, V>;
  using Base = Lattice<Element>;

 protected:
  void do_merge(const Element& p) override {
    Compare compare;
    if (compare(p.priority, this->element.priority)) {
      this->element = p;
    }
  }

 public:
  PriorityLattice() : Base(Element()) {}

  PriorityLattice(const Element& p) : Base(p) {}

  MaxLattice<unsigned> size() { return {this->element.size()}; }
};

#endif
