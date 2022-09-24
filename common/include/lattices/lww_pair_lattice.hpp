#ifndef INCLUDE_LATTICES_LWW_PAIR_LATTICE_HPP_
#define INCLUDE_LATTICES_LWW_PAIR_LATTICE_HPP_

#include "core_lattices.hpp"

template <typename T>
struct TimestampValuePair {
  unsigned long long timestamp{0};
  T value;

  TimestampValuePair<T>() {
    timestamp = 0;
    value = T();
  }

  // need this because of static cast
  TimestampValuePair<T>(const unsigned long long& a) {
    timestamp = 0;
    value = T();
  }

  TimestampValuePair<T>(const unsigned long long& ts, const T& v) {
    timestamp = ts;
    value = v;
  }
  unsigned size() const { return value.size() + sizeof(unsigned long long); }
};

template <typename T>
class LWWPairLattice : public Lattice<TimestampValuePair<T>> {
 protected:
  void do_merge(const TimestampValuePair<T>& p) {
    if (p.timestamp >= this->element.timestamp) {
      this->element.timestamp = p.timestamp;
      this->element.value = p.value;
    }
  }

 public:
  LWWPairLattice() : Lattice<TimestampValuePair<T>>(TimestampValuePair<T>()) {}
  LWWPairLattice(const TimestampValuePair<T>& p) :
      Lattice<TimestampValuePair<T>>(p) {}
  MaxLattice<unsigned> size() { return {this->element.size()}; }
};

#endif  // INCLUDE_LATTICES_LWW_PAIR_LATTICE_HPP_
