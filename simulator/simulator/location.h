#ifndef __LOCATION_H__
#define __LOCATION_H__

#include <iostream>

class Location {
  public:
    int switch_idx, node_idx, gpu_idx;
    Location(int x=-1, int y=-1, int z=-1) : switch_idx(x), node_idx(y), gpu_idx(z) {}
    Location& operator= (const Location& location);
    friend std::ostream& operator<< (std::ostream &out, const Location &location);
    bool operator== (const Location& location) const;
};

Location& Location::operator= (const Location &location) {
    switch_idx = location.switch_idx;
    node_idx = location.node_idx;
    gpu_idx = location.gpu_idx;
    return *this;
}

std::ostream& operator<< (std::ostream &out , const Location &location) {
  out << "(" << location.switch_idx << "," << location.node_idx << "," << location.gpu_idx << ")";
  return out;
}

bool Location::operator== (const Location& location) const {
  if (switch_idx == location.switch_idx && node_idx == location.node_idx && gpu_idx == location.gpu_idx) {
    return true;
  } else {
    return false;
  }
}

#endif // location.h