#ifndef __SWITCH_H__
#define __SWITCH_H__

#include <vector>
#include "node.h"

class Switch {
  private:
    std::vector<Node*> Nodes;
    int idle_GPUs;
  public:
    Switch();
    void add_node(Node* node) { Nodes.push_back(node); }
    void set_idle_GPUs(int num) { idle_GPUs = num; }
    void increase_idle_GPUs() { idle_GPUs += 1; }
    void decrease_idle_GPUs() { idle_GPUs -= 1; }
    int get_idle_GPUs() { return idle_GPUs; }
    Node* get_node_ptr(int idx) { return Nodes[idx]; }
    std::vector<Node*>::iterator Nodes_begin() { return Nodes.begin(); }
    std::vector<Node*>::iterator Nodes_end() { return Nodes.end(); }
};

Switch::Switch() {}

#endif // switch.h
