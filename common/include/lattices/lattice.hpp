#ifndef INCLUDE_LATTICES_LATTICE_HPP_
#define INCLUDE_LATTICES_LATTICE_HPP_

template <typename T>
class Lattice {
 protected:
  T element;
  virtual void do_merge(const T &e) = 0;

 public:
  // Lattice<T>() { assign(bot()); }

  Lattice<T>(const T &e) { assign(e); }

  Lattice<T>(const Lattice<T> &other) { assign(other.reveal()); }

  virtual ~Lattice<T>() = default;

  Lattice<T> &operator=(const Lattice<T> &rhs) {
    assign(rhs.reveal());
    return *this;
  }

  bool operator==(const Lattice<T> &rhs) const {
    return this->reveal() == rhs.reveal();
  }

  const T &reveal() const { return element; }

  void merge(const T &e) { return do_merge(e); }

  void merge(const Lattice<T> &e) { return do_merge(e.reveal()); }

  void assign(const T e) { element = e; }

  void assign(const Lattice<T> &e) { element = e.reveal(); }
};

#endif  // INCLUDE_LATTICES_LATTICE_HPP_
