#ifndef __CLUSTER_H__
#define __CLUSTER_H__

#include <vector>
#include <list>
#include <string>
#include <fstream>
#include <algorithm>
#include <cassert>

// #include "node.h"
// #include "switch.h"
#include "location.h"
#include "util.h"

extern std::ostringstream oss;

int kGPU_p_node; // deprecated??

enum GPU_type {V100, K80, TitanX};
// 0 for no job. job id start from 1
struct GPU {
  GPU_type type;
  int reserved_job;
  int running_job;
  long long lease;
};

class Cluster {
 public:
  Cluster(std::string topology_config_file);
  int get_node_size() { return _n_gpu_p_node; }
  int get_idle_GPUs();
  int get_num_usable_GPUs();
  std::vector<Location> get_usable_GPUs();

  void set_gpu_process(int GPU_process, Location location) { _GPU_cluster[location.switch_idx][location.node_idx][location.gpu_idx].running_job = GPU_process; }
  int get_gpu_process(Location location) { return _GPU_cluster[location.switch_idx][location.node_idx][location.gpu_idx].running_job; }
  void get_gpu_processes(const std::vector<int> &nodes, std::vector<int> &jobs);


  void set_reserve_job(int job_id, Location location) { _GPU_cluster[location.switch_idx][location.node_idx][location.gpu_idx].reserved_job = job_id; }
  int get_reserve_job(Location location) { return _GPU_cluster[location.switch_idx][location.node_idx][location.gpu_idx].reserved_job; }
  GPU_type get_gpu_type(Location location) { return _GPU_cluster[location.switch_idx][location.node_idx][location.gpu_idx].type; }

  bool try_alloc(int job_id, int req, std::vector<Location>& locations);
  Location provide_gpu(int job_id); // TODO: alloc GPUs from GPU0
  std::vector<Location> provide_gpu(int job_id, int req, int consolidate_effect);
  std::vector<Location> get_idle_locations();
  void provide_gpu(int job_id, int req, std::vector<Location>& locations); // will be deprecated

  void provide_gpu(int job_id, Location location);
  void provide_gpu(int job_id, std::vector<Location> locations);
  void release_gpu(Location location);

  std::vector<Location> determine_placement(int job_id, int req,
                                        int consolidate_effect,
                                        std::vector<Location>& avail_GPUs);
  std::vector<Location> determine_placement_with_fix(int job_id, int req,
                                            int consolidate_effect,
                                            std::vector<Location>& fixed_locs,
                                            std::vector<Location>& avail_GPUs);

  void cluster_status();
  int get_total_GPUs() { return _n_total_GPUs; }

  void set_gpu_lease(time_type lease, Location loc) { _GPU_cluster[loc.switch_idx][loc.node_idx][loc.gpu_idx].lease = lease; }
  bool is_lease_event(time_type current_event_time);
  bool is_lease_expired(Location loc, time_type current_event_time) { return current_event_time >= _GPU_cluster[loc.switch_idx][loc.node_idx][loc.gpu_idx].lease; }
  bool is_usable(Location);
  // bool is_usable(Location, int jid);
  bool is_available_gpus(std::vector<Location>, int jid);
  bool is_leasable_gpus(std::vector<Location>, int jid, time_type);
  std::vector<Location> all_GPUs();

 private:
  std::vector<std::vector<std::vector<GPU> > > _GPU_cluster;
  int _n_switch_p_cluster;
  int _n_node_p_switch;
  int _n_gpu_p_node;

  int _n_total_GPUs;
  int _n_idle_GPUs;
};
Cluster::Cluster(std::string topology_config_file) {
  std::ostringstream oss;
  oss << "[Cluster] Construct cluster\n"; // for debugging
  put_log(oss,kInfo);
  // read value from topology_config_file
  // int num_switch;// = 1;
  // int num_node;// = 10;
  // int num_gpu;// = 4;
  std::ifstream file;
  file.open(topology_config_file);
  file >> _n_switch_p_cluster;
  file >> _n_node_p_switch;
  file >> _n_gpu_p_node;

// DO: vector declare num_switch x num_node x num_gpu size

  GPU gpu{K80,0,0,0};
  _GPU_cluster = std::vector<std::vector<std::vector<GPU>>>(_n_switch_p_cluster, std::vector<std::vector<GPU>> (_n_node_p_switch, std::vector<GPU> (_n_gpu_p_node, gpu)));


  // kGPU_p_node = num_gpu;
  kGPU_p_node = _n_gpu_p_node;

  // total_GPUs = idle_count_in_cluster;
  _n_total_GPUs = _n_switch_p_cluster * _n_node_p_switch * _n_gpu_p_node;
  _n_idle_GPUs = _n_total_GPUs;
}

int Cluster::get_idle_GPUs() {
  // return _n_idle_GPUs;
// CHECK: may need to check _gpu_cluster
  int count = 0;
  for (int i = 0; i < _n_switch_p_cluster; ++i) {
    for (int j = 0; j < _n_node_p_switch; ++j) {
      count += std::count_if(_GPU_cluster[i][j].begin(),
                            _GPU_cluster[i][j].end(),
                            [](GPU gpu) { return gpu.running_job == 0; });
    }
  }
  return count;
}

int Cluster::get_num_usable_GPUs() {
  int count = 0;
  for (const auto &s : _GPU_cluster) {
    for (const auto &n : s) {
      for (const auto &g : n) {
        if (g.running_job == 0 && g.reserved_job == 0) {
          count++;
        }
      }
    }
  }
  return count;
}

std::vector<Location> Cluster::get_usable_GPUs() {
  std::vector<Location> locs;
  for (int i = 0; i < _n_switch_p_cluster; ++i) {
    for (int j = 0; j < _n_node_p_switch; ++j) {
      for (int k = 0; k < _n_gpu_p_node; ++k) {
        if (_GPU_cluster[i][j][k].running_job == 0 && _GPU_cluster[i][j][k].reserved_job == 0) {
          locs.push_back(Location(i,j,k));
        }
      }
    }
  }
  return locs;
}

void Cluster::get_gpu_processes(const std::vector<int> &node_idcs, std::vector<int> &job_ids) {
  for (const int& idx : node_idcs) {
    for (int g = 0; g < _n_gpu_p_node; ++g) {
      job_ids.push_back(_GPU_cluster[0][idx][g].running_job);
    }
    // Node* node = Switches[0]->get_node_ptr(idx);
    // for (auto it = node->GPUs_begin(); it != node->GPUs_end(); ++it) {
    //   job_ids.push_back((*it)->running_job);
    // }
  }
  std::sort(job_ids.begin(),job_ids.end());
  job_ids.erase(std::unique(job_ids.begin(),job_ids.end()),job_ids.end());
}

// CHECK: this is used only in delay scheduling. but it seems weird. need to check behavior again
// return true for allocation success, false for fail
bool Cluster::try_alloc(int job_id, int req, std::vector<Location>& locations) {
  oss << "Try alloc job" << job_id << std::endl;
  put_log(oss,kDebug);
  // sort node idx in increasing order of idle GPU
  std::list<int> avail_nodes;
  int max_val = 0;
  for (int i = 0; i < _n_switch_p_cluster; ++i) {
    for (int j = 0; j < _n_node_p_switch; ++j) {
      int avail = 0;
      for (int k = 0; k < _n_gpu_p_node; ++k) {
        if (_GPU_cluster[i][j][k].running_job == 0) {
          avail += 1;
        }
      }
      if (max_val < avail) {
        max_val = avail;
      }
    }
  }
  // for (auto Switch : Switches) {
  //   for (auto Node = Switch->Nodes_begin(); Node != Switch->Nodes_end(); ++Node) {
  //     int avail = (*Node)->get_idle_GPUs();
  //     if (max_val < avail)
  //       max_val = avail;
  //   }
  // }
  while (max_val > 0) {
    for (int i = 0; i < _n_switch_p_cluster; ++i) {
      for (int j = 0; j < _n_node_p_switch; ++j) {
        int avail = 0;
        for (int k = 0; k < _n_gpu_p_node; ++k) {
          if (_GPU_cluster[i][j][k].running_job == 0) {
            avail += 1;
          }
        }
        if (avail == max_val) {
          avail_nodes.push_back(j);
        }
      }
    }
    max_val -= 1;
  }
  // while(max_val > 0) {
  //   for (auto Switch : Switches) {
  //     for (auto Node = Switch->Nodes_begin(); Node != Switch->Nodes_end(); ++Node) {
  //       if ((*Node)->get_idle_GPUs() == max_val) {
  //         avail_nodes.push_back(Node - Switch->Nodes_begin());
  //       }
  //     }
  //   }
  //   max_val -= 1;
  // }
  oss << "[Debug] node list: ";
  for (auto elem : avail_nodes) {
    oss << elem << "(" 
        << std::count_if(_GPU_cluster[0][elem].begin(),
                         _GPU_cluster[0][elem].end(),
                         [](GPU gpu) { return gpu.running_job == 0; })
        << ") ";
    // oss << elem << "(" << Switches[0]->get_node_ptr(elem)->get_idle_GPUs() << ") ";
  }
  oss << std::endl;
  put_log(oss,kDebug);

  // if gpu is not avail right away, take it and reserve it
  bool fit_found = false;
  int needs = req;
  // Node* node_ptr = Switches[0]->get_node_ptr(0);
  // int node_size = node_ptr->GPUs_end() - node_ptr->GPUs_begin();
  int node_size = _n_node_p_switch;

  // find 'needs' amount empty and consolidated GPUs, if found, fit_found=true
  while (needs >= node_size) {
    for (int node_idx : avail_nodes) {
      // node_ptr = Switches[0]->get_node_ptr(node_idx);
      int n_node_idle_GPUs = 0;
      for (int g = 0; g < _n_gpu_p_node; ++g) {
        if (_GPU_cluster[0][node_idx][g].running_job == 0)
          n_node_idle_GPUs += 1;
      }
      // if (node_ptr->get_idle_GPUs() < node_size) {
      if (n_node_idle_GPUs < node_size) { // CHECK: suspicious
        needs = -1;
        break;
      // } else if (node_ptr->get_idle_GPUs() == node_size) {
      } else if (n_node_idle_GPUs == node_size) {
        bool is_idle_node = true;
        for (int g = 0; g < _n_gpu_p_node; ++g) {
          if (_GPU_cluster[0][node_idx][g].running_job != 0 || _GPU_cluster[0][node_idx][g].reserved_job != 0) {
            is_idle_node = false;
          }
        }
        // for (auto it_g_ptr = node_ptr->GPUs_begin(); it_g_ptr != node_ptr->GPUs_end(); ++it_g_ptr) {
        //   if ((*it_g_ptr)->running_job != 0 || (*it_g_ptr)->reserved_job != 0) {
        //     is_idle_node = false;
        //   }
        // }
        if (is_idle_node) {
          needs -= node_size;
          for (int i = 0; i < node_size; ++i) {
            Location location(0,node_idx,i);
            locations.push_back(location);
          }
          if (needs == 0) {
            fit_found = true;
            break;
          } else if (needs < node_size) {
            break;
          }
        } else { // this avail node is not fully available...
          // oss << "[Debug] Error: Unimplemented area.\n"
          //     << "        avail nodes are enough but they are not fully available." << std::endl;
          // put_log(oss,kDebug);
          // assert(false);
          needs = -1;
          break;
        }
      } else {
        // TODO: raise error - idle GPUs cannot bigger than node_size
        assert(false);
      }
    }
  }
  // try alloc from least idle nodes
  if (needs > 0){
    for (auto rit = avail_nodes.rbegin(); rit != avail_nodes.rend(); ++rit) {
      // node_ptr = Switches[0]->get_node_ptr(*rit);
      int avail = 0;
      for (int g = 0; g < _n_gpu_p_node; ++g) {
        if (_GPU_cluster[0][*rit][g].running_job == 0 && _GPU_cluster[0][*rit][g].reserved_job == 0) {
          avail += 1;
        }
      }
      // for (auto it_g_ptr = node_ptr->GPUs_begin(); it_g_ptr != node_ptr->GPUs_end(); ++it_g_ptr) {
      //   if ((*it_g_ptr)->running_job == 0 && (*it_g_ptr)->reserved_job == 0) {
      //     avail += 1;
      //   }
      // }
      if (avail < needs) {
        continue; // check next node
      } else { // the node can be used!
        for (int g = 0; g < _n_gpu_p_node; ++g) {
          if (needs > 0 && _GPU_cluster[0][*rit][g].running_job == 0 && _GPU_cluster[0][*rit][g].reserved_job == 0) {
            Location location(0,*rit,g);
            locations.push_back(location);
            needs -= 1;
          }
        }
        // for (auto it_g_ptr = node_ptr->GPUs_begin(); it_g_ptr != node_ptr->GPUs_end(); ++it_g_ptr) {
        //   if (needs > 0 && (*it_g_ptr)->running_job == 0 && (*it_g_ptr)->reserved_job == 0) {
        //     int gpu_idx = it_g_ptr-node_ptr->GPUs_begin();
        //     Location location(0,*rit,gpu_idx);
        //     locations.push_back(location);
        //     needs -= 1;
        //   }
        // }
        if (needs != 0) {
          // TODO: raise error - the node supposed to accomodate the needs
          assert(false);
        }
        fit_found = true;
        break;
      }
    }
  }
  // reserve first non reserved nodes
  if (!fit_found) {
    locations.clear();
    std::list<int>::iterator it;
    for (it = avail_nodes.begin(); it != avail_nodes.end(); ++it){
      int node_idx = *it;
      // node_ptr = Switches[0]->get_node_ptr(node_idx);
      if (req >= node_size) {
        bool is_reservable = true;
        for (int g = 0; g < _n_gpu_p_node; ++g) {
          if (_GPU_cluster[0][node_idx][g].reserved_job != 0) {
            is_reservable = false;
            break;
          }
        }
        // for (auto it_g_ptr = node_ptr->GPUs_begin(); it_g_ptr != node_ptr->GPUs_end(); ++it_g_ptr) {
        //   if ((*it_g_ptr)->reserved_job != 0) {
        //     is_reservable = false;
        //     break;
        //   }
        // }
        if (is_reservable) {
          req -= node_size;
          for (int i = 0; i < node_size; ++i) {
            Location location(0,node_idx,i);
            locations.push_back(location);
          }
          if (req == 0) {
            break;
          }
        }
      } else if (req < node_size) {
        int avail = 0;
        for (int g = 0; g < _n_gpu_p_node; ++g) {
          if (_GPU_cluster[0][node_idx][g].reserved_job == 0) {
            avail += 1;
          }
        }
        // for (auto it_g_ptr = node_ptr->GPUs_begin(); it_g_ptr != node_ptr->GPUs_end(); ++it_g_ptr) {
        //   if ((*it_g_ptr)->reserved_job == 0) {
        //     avail += 1;
        //   }
        // }
        if (avail < req) {
          continue;
        } else {
          for (int g = 0; g < _n_gpu_p_node; ++g) {
            if (req > 0 && _GPU_cluster[0][node_idx][g].reserved_job == 0) {
              Location location(0,node_idx,g);
              locations.push_back(location);
              req -= 1;
            }
          }
          // for (auto it_g_ptr = node_ptr->GPUs_begin(); it_g_ptr != node_ptr->GPUs_end(); ++it_g_ptr) {
          //   if (req > 0 && (*it_g_ptr)->reserved_job == 0) {
          //     int gpu_idx = it_g_ptr-node_ptr->GPUs_begin();
          //     Location location(0,node_idx,gpu_idx);
          //     locations.push_back(location);
          //     req -= 1;
          //   }
          // }
          if (req == 0) {
            break;
          }
        }
      }
    }
    if (it == avail_nodes.end()) {
      std::string msg = "No resource to reserve job" + std::to_string(job_id);
      throw std::logic_error(msg);
    }
    // mark cluster gpu to reserve at locations
    for (Location loc : locations) {
      set_reserve_job(job_id,loc);
    }
  } else {
    // mark cluster gpu to using at location
    for (Location loc : locations) {
      set_gpu_process(job_id,loc);
    }
  }
  return fit_found;
}

// give first found GPU to 'job_id'
Location Cluster::provide_gpu(int job_id) {
  std::string job_name = "Job" + std::to_string(job_id); // depricated
  for (int i = 0; i < _n_switch_p_cluster; ++i) {
    for (int j = 0; j < _n_node_p_switch; ++j) {
      for (int k = 0; k < _n_gpu_p_node; ++k) {
        if (_GPU_cluster[i][j][k].running_job == 0 && _GPU_cluster[i][j][k].reserved_job == 0) {
          _GPU_cluster[i][j][k].running_job = job_id;
          Location location(i,j,k);
          oss << "[Cluster] Provide GPU " << location << std::endl;
          put_log(oss,kInfo);
          return location;
        }
      }
    }
  }
  // TODO: reaching here is an error. idle GPU exists, but not found.
  assert(false);
}

struct NodeAvail {
  int node_idx;
  std::vector<int> gpu_idcs;
};
// NOTE: redundant to Cluster::get_usable_GPUs
std::vector<Location> Cluster::get_idle_locations() {
  std::vector<Location> locations;
  locations = this->get_usable_GPUs();
  // for (auto Switch : Switches) {
  //   for (auto Node = Switch->Nodes_begin(); Node != Switch->Nodes_end(); ++Node) {
  //     for (auto GPU = (*Node)->GPUs_begin(); GPU != (*Node)->GPUs_end(); ++GPU) {
  //       if ((*GPU)->running_job == 0 && (*GPU)->reserved_job == 0) {
  //         locations.push_back(Location(0, Node - Switch->Nodes_begin(), GPU - (*Node)->GPUs_begin()));
  //       }
  //     }
  //   }
  // }
  return locations;
}
bool descending_n_gpu(NodeAvail A, NodeAvail B) {
  if (A.gpu_idcs.size() != B.gpu_idcs.size())
    return A.gpu_idcs.size() > B.gpu_idcs.size();
  else
    return A.node_idx < B.node_idx;
}
std::vector<Location> find_placement_gather(size_t req, std::vector<NodeAvail> &node_avail) {
  std::vector<Location> placement;
  // sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

  // if (req == 1) {
  //   int ridx = node_avail.size();
  //   while (true) {
  //     ridx -= 1;
  //     if (ridx < 0) {
  //       break;
  //     }
  //     if (node_avail[ridx].gpu_idcs.size() % 2 != 0) {
  //       placement.push_back(Location(0, node_avail[ridx].node_idx, node_avail[ridx].gpu_idcs[0]));
  //       node_avail[ridx].gpu_idcs.erase(node_avail[ridx].gpu_idcs.begin(), node_avail[ridx].gpu_idcs.begin() + req);
  //       req = 0;
  //       return placement;
  //       break;
  //     }
  //   }
  // }

  size_t idx = 0;
  // while (req > 0) {
  size_t max_g = node_avail[idx].gpu_idcs.size();
  while (req >= max_g) {
    // if (req >= max_g) {
      for (size_t i = 0; i < max_g; ++i) {
        placement.push_back(Location(0, node_avail[idx].node_idx, node_avail[idx].gpu_idcs[i]));
      }
      req -= max_g;
      node_avail[idx].gpu_idcs.clear();
      if (req == 0)
        break;
      idx += 1;
      assert(idx < node_avail.size());
      // if (idx >= node_avail.size()) {
      //   throw req;
      // }
      max_g = node_avail[idx].gpu_idcs.size();
    // }
  }
    // else {
  if (req > 0) {
    int ridx = node_avail.size() - 1;
    while (true) {
      if (ridx < 0)
        assert(false); // TODO: what if node exist but big enough node not found??? handle it!!
      if (node_avail[ridx].gpu_idcs.size() >= req)
        break;
      ridx -= 1;
    }
    for (size_t i = 0; i < req; ++i) {
      placement.push_back(Location(0, node_avail[ridx].node_idx, node_avail[ridx].gpu_idcs[i]));
    }
    node_avail[ridx].gpu_idcs.erase(node_avail[ridx].gpu_idcs.begin(), node_avail[ridx].gpu_idcs.begin() + req);
    req = 0;
  }
  // }

  return placement;
}
std::vector<Location> find_placement_fill_frag(size_t req, std::vector<NodeAvail> &node_avail) {
  std::vector<Location> placement;
  // sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

  int ridx = node_avail.size();

  // if (req == 1) {
  //   while (true) {
  //     ridx -= 1;
  //     if (ridx < 0) {
  //       break;
  //     }
  //     if (node_avail[ridx].gpu_idcs.size() % 2 != 0) {
  //       placement.push_back(Location(0, node_avail[ridx].node_idx, node_avail[ridx].gpu_idcs[0]));
  //       node_avail[ridx].gpu_idcs.erase(node_avail[ridx].gpu_idcs.begin(), node_avail[ridx].gpu_idcs.begin() + req);
  //       req = 0;
  //       break;
  //     }
  //   }
  // }

  ridx = node_avail.size();
  while (req != 0) {
    ridx -= 1;
    if (ridx < 0) {
      assert(false);
    }
    if (req > node_avail[ridx].gpu_idcs.size()) {
      for (size_t i = 0; i < node_avail[ridx].gpu_idcs.size(); ++i) {
        placement.push_back(Location(0, node_avail[ridx].node_idx, node_avail[ridx].gpu_idcs[i]));
      }
      req -= node_avail[ridx].gpu_idcs.size();
      node_avail[ridx].gpu_idcs.clear();
    } else {
      for (size_t i = 0; i < req; ++i) {
        placement.push_back(Location(0, node_avail[ridx].node_idx, node_avail[ridx].gpu_idcs[i]));
      }
      node_avail[ridx].gpu_idcs.erase(node_avail[ridx].gpu_idcs.begin(), node_avail[ridx].gpu_idcs.begin() + req);
      req = 0;
    }
  }

  return placement;
}
std::vector<Location> find_placement_scatter(size_t req, std::vector<NodeAvail> &node_avail) {
  std::vector<Location> placement;
  // sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

  int ridx = node_avail.size();

  while (req > 0) {
    ridx -= 1;
    if (ridx < 0) {
      ridx += node_avail.size();
    }
    if (node_avail[ridx].gpu_idcs.size() == 0) {
      if (ridx == 0) {
        assert(false);
      }
      continue;
    }
    placement.push_back(Location(0, node_avail[ridx].node_idx, node_avail[ridx].gpu_idcs[0]));
    node_avail[ridx].gpu_idcs.erase(node_avail[ridx].gpu_idcs.begin());
    req -= 1;

  //   if (req > node_avail[ridx].gpu_idcs.size()) {
  //     for (int i = 0; i < node_avail[ridx].gpu_idcs.size(); ++i) {
  //       placement.push_back(Location(0, node_avail[ridx].node_idx, node_avail[ridx].gpu_idcs[i]));
  //     }
  //     req -= node_avail[ridx].gpu_idcs.size();
  //     node_avail[ridx].gpu_idcs.clear();
  //   } else {
  //     for (int i = 0; i < req; ++i) {
  //       placement.push_back(Location(0, node_avail[ridx].node_idx, node_avail[ridx].gpu_idcs[i]));
  //     }
  //     node_avail[ridx].gpu_idcs.erase(node_avail[ridx].gpu_idcs.begin(), node_avail[ridx].gpu_idcs.begin() + req);
  //     req = 0;
  //   }
  }

  return placement;
}

std::vector<Location> Cluster::all_GPUs() {
  std::vector<Location> locations(_n_total_GPUs);
  for (int i = 0; i < _n_switch_p_cluster; ++i) {
    for (int j = 0; j < _n_node_p_switch; ++j) {
      for (int k = 0; k < _n_gpu_p_node; ++k) {
        locations[i * _n_node_p_switch * _n_gpu_p_node + j * _n_gpu_p_node + k] = Location(i,j,k);
      }
    }
  }

  // for (auto Switch : Switches) {
  //   for (auto Node = Switch->Nodes_begin(); Node != Switch->Nodes_end(); ++Node) {
  //     for (auto GPU = (*Node)->GPUs_begin(); GPU != (*Node)->GPUs_end(); ++GPU) {
  //         locations.push_back(Location(0, Node - Switch->Nodes_begin(), GPU - (*Node)->GPUs_begin()));
  //     }
  //   }
  // }
  return locations;
}
// std::vector<NodeAvail>::iterator find(int idx, std::vector<NodeAvail>& node_avails) {
//   auto it = node_avails.begin();
//   for (; it != node_avails.end(); ++it) {
//     if (it->node_idx == idx)
//       break;
//   }
//   return it;
// }

std::vector<NodeAvail> convert_loc_to_node_avail(std::vector<Location>& locs) {
// std::vector<NodeAvail> convert_loc_to_node_avail(std::vector<Location>& loc) {
  std::vector<NodeAvail> node_avails;
  for (const auto& e : locs) {
    // auto it = find(e.node_idx, node_avails);
    int loc_n_idx = e.node_idx;
    auto it = std::find_if(node_avails.begin(), node_avails.end(),
                           [loc_n_idx](NodeAvail node) {
                             return node.node_idx == loc_n_idx;
                           });
    if (it != node_avails.end()) {
      it->gpu_idcs.push_back(e.gpu_idx);
    } else {
      std::vector<int> gpus{e.gpu_idx};
      NodeAvail node_avail = {e.node_idx, gpus};
      node_avails.push_back(node_avail);
    }
  }

  return node_avails;
}
std::vector<Location> Cluster::determine_placement(int job_id, int req,
                                                   int consolidate_effect,
                                                   std::vector<Location>& avail_GPUs) {
// std::vector<Location> Cluster::determine_placement(int job_id, int req,
//                                                    int consolidate_effect,
//                                                    std::vector<Location>& avail_GPUs) {
  std::vector<Location> placement;
  std::vector<NodeAvail> node_avail = convert_loc_to_node_avail(avail_GPUs);

  std::sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

  if (req < 0)
    assert(false);
  if (consolidate_effect > 0) {
    placement = find_placement_gather(req, node_avail);
  } else if (consolidate_effect == 0) {
  // } else {
    placement = find_placement_fill_frag(req, node_avail);
  } else {
    placement = find_placement_scatter(req, node_avail);
  }

  return placement;
}


std::vector<Location> find_placement_gather_with_fix(size_t additional_req,
                                            std::vector<NodeAvail> &node_avail,
                                            std::vector<NodeAvail>& node_fixed) {
  std::vector<Location> placement;
  // do something here considering node_fixed.
  // sort(node_fixed.begin(), node_fixed.end(), descending_n_gpu);
  // for (auto it = node_fixed.begin(); it != node_fixed.end(); ++it) {
  //   for (auto avail : node_avail) {
  //     if () //합이 최대인 곳을 고르기..? 차선: 최대한 많이 넣을 수 있는 곳을 고르기!
  //   }
  // }

  // int num_fixed = node_fixed.size();
  // int additional_req = req - node_fixed.size();

  // sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

  for (auto it_avail = node_avail.begin(); it_avail != node_avail.end();) {
  // for (auto it_avail = node_avail.begin(); it_avail != node_avail.end(); ++it_avail) {
    if (node_fixed.empty() || additional_req == 0)
      break;
    int avail_node_idx = it_avail->node_idx;
    auto it_fixed = std::find_if(node_fixed.begin(),node_fixed.end(),
                                 [avail_node_idx](NodeAvail node) {
                                     return node.node_idx == avail_node_idx;
                                 });
    if (it_fixed != node_fixed.end()) {
      if (additional_req >= it_avail->gpu_idcs.size()) {
        additional_req -= it_avail->gpu_idcs.size();
        for (int gidx : it_avail->gpu_idcs) {
          placement.push_back(Location(0, it_avail->node_idx, gidx));
        }
        it_avail = node_avail.erase(it_avail);
        // --it_avail;
      } else {
        for (size_t i = 0; i < additional_req; ++i) {
          placement.push_back(Location(0, it_avail->node_idx, it_avail->gpu_idcs[i]));
        }
        it_avail->gpu_idcs.erase(it_avail->gpu_idcs.begin(), it_avail->gpu_idcs.begin() + additional_req);
        additional_req = 0;
        ++it_avail;
      }
    } else {
      ++it_avail;
    }
    

    // for (auto fixed : node_fixed) { // if node_fixed is empty, endless loop
    //   if (it_avail->node_idx == fixed.node_idx) {
    //     if (additional_req >= it_avail->gpu_idcs.size()) {
    //       additional_req -= it_avail->gpu_idcs.size();
    //       for (int gidx : it_avail->gpu_idcs) {
    //         placement.push_back(Location(0, it_avail->node_idx, gidx));
    //       }
    //       it_avail = node_avail.erase(it_avail);
    //       // --it_avail;
    //     } else {
    //       for (int i = 0; i < additional_req; ++i) {
    //         placement.push_back(Location(0, it_avail->node_idx, it_avail->gpu_idcs[i]));
    //       }
    //       it_avail->gpu_idcs.erase(it_avail->gpu_idcs.begin(), it_avail->gpu_idcs.begin() + additional_req);
    //       additional_req = 0;
    //       ++it_avail;
    //     }
    //   }
    // }
    // if (additional_req == 0) {
    //   break;
    // }
  }

  if (additional_req != 0) {
    std::sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

    std::vector<Location> more_placement = find_placement_gather(additional_req, node_avail);
    placement.insert(placement.end(), more_placement.begin(), more_placement.end());
  }

  return placement;
}

std::vector<Location> find_placement_scatter_with_fix(size_t additional_req,
                                            std::vector<NodeAvail> &node_avail,
                                            std::vector<NodeAvail>& node_fixed) {
  std::vector<Location> placement;

  for (auto it_avail = node_avail.begin(); it_avail != node_avail.end();) {
  // for (auto it_avail = node_avail.begin(); it_avail != node_avail.end(); ++it_avail) {
    if (node_fixed.empty() || additional_req == 0)
      break;
    int avail_node_idx = it_avail->node_idx;
    auto it_fixed = std::find_if(node_fixed.begin(),node_fixed.end(),
                                 [avail_node_idx](NodeAvail node) {
                                     return node.node_idx == avail_node_idx;
                                 });
    if (it_fixed == node_fixed.end()) {
    // if (find(it_avail->node_idx, node_fixed) == node_fixed.end()) {
      placement.push_back(Location(0, it_avail->node_idx, it_avail->gpu_idcs.back()));
      additional_req -= 1;
      if (it_avail->gpu_idcs.size() == 1) {
        it_avail = node_avail.erase(it_avail);
        // --it_avail;
      } else {
        it_avail->gpu_idcs.pop_back();
        ++it_avail;
      }
    } else {
      ++it_avail;
    }
    // if (additional_req == 0) {
    //   break;
    // }
  }

  if (additional_req != 0) {
    std::sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

    std::vector<Location> more_placement = find_placement_scatter(additional_req, node_avail);
    placement.insert(placement.end(), more_placement.begin(), more_placement.end()); 
  }

  return placement;
}

// select location from avail GPUs to meet consolidate effect including fixed locs
std::vector<Location> Cluster::determine_placement_with_fix(int job_id, int req,
                                                   int consolidate_effect,
                                                   std::vector<Location>& fixed_locs,
                                                   std::vector<Location>& avail_GPUs) {
  std::vector<Location> placement;
  std::vector<NodeAvail> node_avail = convert_loc_to_node_avail(avail_GPUs);
  std::vector<NodeAvail> node_fixed = convert_loc_to_node_avail(fixed_locs);

  size_t additional_req = size_t(req) - fixed_locs.size();

  std::sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

  if (consolidate_effect > 0) {
    placement = find_placement_gather_with_fix(additional_req, node_avail, node_fixed);
  } else if (consolidate_effect == 0) {
    placement = find_placement_fill_frag(additional_req, node_avail);
  } else {
    placement = find_placement_scatter_with_fix(additional_req, node_avail, node_fixed);
  }

  placement.insert(placement.end(),fixed_locs.begin(),fixed_locs.end());

  return placement;
}

//DONE: make locality sensitivity discriminatie inverse sensitivity
std::vector<Location> Cluster::provide_gpu(int job_id, int req, int consolidate_effect) {
  std::vector<Location> placement;
  std::vector<NodeAvail> node_avail;

  for (int i = 0; i < _n_switch_p_cluster; ++i) {
    for (int j = 0; j < _n_node_p_switch; ++j) {
      std::vector<int> gpu_idcs;
      for (int k = 0; k < _n_gpu_p_node; ++k) {
        if (_GPU_cluster[i][j][k].running_job == 0 && _GPU_cluster[i][j][k].reserved_job == 0) {
          gpu_idcs.push_back(k);
        }
      }
      if (!gpu_idcs.empty()) {
        NodeAvail node = { j, gpu_idcs };
        node_avail.push_back(node);
      }
    }
  }

  // for (auto Switch : Switches) {
  //   for (auto Node = Switch->Nodes_begin(); Node != Switch->Nodes_end(); ++Node) {
  //     std::vector<int> gpu_idcs;
  //     for (auto GPU = (*Node)->GPUs_begin(); GPU != (*Node)->GPUs_end(); ++GPU) {
  //       if ((*GPU)->running_job == 0 && (*GPU)->reserved_job == 0) {
  //         gpu_idcs.push_back(GPU - (*Node)->GPUs_begin());
  //       }
  //     }
  //     if (!gpu_idcs.empty()) {
  //       NodeAvail node = { static_cast<int>(Node - Switch->Nodes_begin()), gpu_idcs };
  //       node_avail.push_back(node);
  //     }
  //   }
  // }
  sort(node_avail.begin(), node_avail.end(), descending_n_gpu);

  if (consolidate_effect > 0) {
    placement = find_placement_gather(req, node_avail);
  } else if (consolidate_effect == 0) {
    placement = find_placement_fill_frag(req, node_avail);
  } else {
    placement = find_placement_scatter(req, node_avail);
  }

  for (Location location : placement) {
    provide_gpu(job_id, location);
  }

  return placement;
}

// give 'req' amount GPU to 'job_id' as best fit
// TODO: optimize algorithm in loop (= first for and while loops)
void Cluster::provide_gpu(int job_id, int req, std::vector<Location>& locations) {
  std::string job_name = "Job" + std::to_string(job_id); // depricated
  std::list<int> avail_nodes;
  int max_val = 0;
  for (int i = 0; i < _n_switch_p_cluster; ++i) {
    for (int j = 0; j < _n_node_p_switch; ++j) {
      int avail = 0;
      for (int k = 0; k < _n_gpu_p_node; ++k) {
        if (_GPU_cluster[i][j][k].running_job == 0) {
          avail += 1;
        }
      }
      if (max_val < avail) {
        max_val = avail;
      }
    }
  }
  while (max_val > 0) {
    for (int i = 0; i < _n_switch_p_cluster; ++i) {
      for (int j = 0; j < _n_node_p_switch; ++j) {
        int avail = 0;
        for (int k = 0; k < _n_gpu_p_node; ++k) {
          if (_GPU_cluster[i][j][k].running_job == 0) {
            avail += 1;
          }
        }
        if (avail == max_val) {
          avail_nodes.push_back(j);
        }
      }
    }
    max_val -= 1;
  }
  oss << "[Debug] node list: ";
  for (auto node_idx : avail_nodes) {
    oss << node_idx << "(" 
        << std::count_if(_GPU_cluster[0][node_idx].begin(),
                         _GPU_cluster[0][node_idx].end(),
                         [](GPU gpu) { return gpu.running_job == 0; })
        << ") ";
  }
  oss << std::endl;
  put_log(oss,kDebug);


  while (req > 0) {
    int n_node_idle_GPUs = std::count_if(_GPU_cluster[0][avail_nodes.front()].begin(),
                                        _GPU_cluster[0][avail_nodes.front()].end(),
                                        [](GPU gpu) { return gpu.running_job == 0; });
    if (req >= n_node_idle_GPUs) {
      int alloc = n_node_idle_GPUs;
    
    // if (req >= Switches[0]->get_node_ptr(avail_nodes.front())->get_idle_GPUs()) {
    //   int alloc = Switches[0]->get_node_ptr(avail_nodes.front())->get_idle_GPUs();
      // req -= alloc;
      // check for "locations" (= mark job_id here as much as alloc)

      // auto node_ptr = Switches[0]->get_node_ptr(avail_nodes.front());
      for (int g = 0; g < _n_gpu_p_node; ++g) {
        if (alloc > 0
            && _GPU_cluster[0][avail_nodes.front()][g].running_job == 0
            && _GPU_cluster[0][avail_nodes.front()][g].reserved_job == 0) {
          _GPU_cluster[0][avail_nodes.front()][g].running_job = job_id;
          alloc -= 1;
          req -= 1;
          Location location(0,avail_nodes.front(),g);
          locations.push_back(location);
        }
      }
      // for (auto GPU = node_ptr->GPUs_begin(); GPU != node_ptr->GPUs_end(); ++GPU) {
      //   if (alloc > 0 && (*GPU)->running_job == 0 && (*GPU)->reserved_job == 0) {
      //     int GPU_idx = GPU - node_ptr->GPUs_begin();
      //     node_ptr->set_GPU_process(GPU_idx, job_id);
      //     alloc -= 1;
      //     req -= 1;
      //     node_ptr->decrease_idle_GPUs();
      //     Switches[0]->decrease_idle_GPUs();
      //     Location location(0,avail_nodes.front(),GPU_idx);
      //     locations.push_back(location);
      //   }
      // }
      avail_nodes.pop_front();
      // oss << "Debug 1\n";
      // put_log(oss,kDebug);
      // assert(alloc == 0); // this is possible. when there is reserved GPU, this job can take less than idle_GPUs.
    } else {
      // find >=req from back of list
      for (auto rit = avail_nodes.rbegin(); rit != avail_nodes.rend(); ++rit) {
        // auto node_ptr = Switches[0]->get_node_ptr(*rit);
        int n_rev_node_idle_GPUs = std::count_if(_GPU_cluster[0][*rit].begin(),
                                                _GPU_cluster[0][*rit].end(),
                                                [](GPU gpu) { return gpu.running_job == 0; }); 

        if (n_rev_node_idle_GPUs >= req) {
        // if (node_ptr->get_idle_GPUs() >= req) {
          // use req amount GPU from the location(= mark job_id here as much as req)
          for (int g = 0; g < _n_gpu_p_node; ++g) {
            if (req > 0
                && _GPU_cluster[0][*rit][g].running_job == 0
                && _GPU_cluster[0][*rit][g].reserved_job == 0) {
              _GPU_cluster[0][*rit][g].running_job = job_id;
              req -= 1;
              Location location(0,*rit,g);
              locations.push_back(location);
            } 
          }
          // for (auto GPU = node_ptr->GPUs_begin(); GPU != node_ptr->GPUs_end(); ++GPU) {
          //   if (req > 0 && (*GPU)->running_job == 0 && (*GPU)->reserved_job == 0) {
          //     int GPU_idx = GPU - node_ptr->GPUs_begin();
          //     int node_idx = *rit;
          //     node_ptr->set_GPU_process(GPU_idx, job_id);
          //     req -= 1;
          //     node_ptr->decrease_idle_GPUs();
          //     Switches[0]->decrease_idle_GPUs();
          //     Location location(0, node_idx, GPU_idx);
          //     locations.push_back(location);
          //   }
          // }
          // oss << "Debug 2\n";
          // put_log(oss,kDebug);
          assert(req == 0);
        }
      }
    }
  }

}

// give GPU at 'location' to 'job_id'
void Cluster::provide_gpu(int job_id, Location location) {
  set_gpu_process(job_id,location);
  set_reserve_job(0,location);
  // idle_GPUs -= 1;
  _n_idle_GPUs -= 1;
  // Switches[location.switch_idx]->decrease_idle_GPUs();
  // Switches[location.switch_idx]->get_node_ptr(location.node_idx)->decrease_idle_GPUs();
  oss << "[Cluster] Provide GPU " << location << std::endl;
  put_log(oss,kInfo);
}
void Cluster::provide_gpu(int job_id, std::vector<Location> locations) {
  for (auto loc : locations) {
    this->provide_gpu(job_id, loc);
  }
}

void Cluster::release_gpu(Location location) {
  // Switches[location.switch_idx]->get_node_ptr(location.node_idx)->set_GPU_status(location.gpu_idx, idle);
  set_gpu_process(0, location);
  // idle_GPUs += 1;
  _n_idle_GPUs += 1;
  // Switches[location.switch_idx]->increase_idle_GPUs();
  // Switches[location.switch_idx]->get_node_ptr(location.node_idx)->increase_idle_GPUs();
  oss << "[Cluster] Release GPU " << location << std::endl;
  put_log(oss,kInfo);
}

void Cluster::cluster_status() {
  oss << "--- cluster status ---\n";
  for (int s = 0; s < _n_switch_p_cluster; ++s) {
  // for (std::vector<Switch*>::iterator it_switch_ptr = Switches.begin();
  //      it_switch_ptr != Switches.end(); ++it_switch_ptr) 
  // {
    oss << "{" << std::endl;
    for (int n = 0; n < _n_node_p_switch; ++n) {
    // for (std::vector<Node*>::iterator it_node_ptr = (*it_switch_ptr)->Nodes_begin();
    //      it_node_ptr != (*it_switch_ptr)->Nodes_end(); ++it_node_ptr) 
    // {
      oss << "[ ";
      for (int g = 0; g < _n_gpu_p_node; ++g) {
      // for (std::vector<GPU*>::iterator it_GPU_ptr = (*it_node_ptr)->GPUs_begin();
      //      it_GPU_ptr != (*it_node_ptr)->GPUs_end(); ++it_GPU_ptr) 
      // {
        GPU gpu = _GPU_cluster[s][n][g];
        oss << std::setw(8) << gpu.running_job << " (Q-";
        int nj = gpu.reserved_job;
        for (int i = 0; i < 3; ++i) {
          if (nj < 1000)
            oss << '_';
          nj *= 10;
        }
        // int nj = (*it_GPU_ptr)->reserved_job; 
        // while(nj < 1000) {
        //   nj *= 10;
        //   oss << ' ';
        // }
        // oss << std::setw(4) << (*it_GPU_ptr)->reserved_job << ',' << (*it_GPU_ptr)->lease << ')';
        oss << gpu.reserved_job << ", L:" << gpu.lease << ')';
        // if ((*it_GPU_ptr)->second == idle)
        //   std::cout << "idle" << " ";
        // else
        //   std::cout << "busy" << " ";
      }
      oss << "]" << std::endl;
    }
    oss << "}" << std::endl;
  }
  oss << "----------------------\n";
  put_log(oss,kCore);
}

// void Cluster::set_gpu_lease(time_type lease, Location loc) {
//   Switches[loc.switch_idx]->get_node_ptr(loc.node_idx)->set_GPU_lease(loc.gpu_idx, lease);
// }

bool Cluster::is_lease_event(time_type current_event_time) {
  for (int i = 0; i < _n_switch_p_cluster; ++i) {
    for (int j = 0; j < _n_node_p_switch; ++j) {
      for (int k = 0; k < _n_gpu_p_node; ++k) {
        if (_GPU_cluster[i][j][k].lease == current_event_time) {
          return true;
        }
      }
    }
  }
  // for (auto it_s = Switches.begin(); it_s != Switches.end(); ++it_s) {
  //   for (auto it_n = (*it_s)->Nodes_begin(); it_n != (*it_s)->Nodes_end(); ++it_n) {
  //     for (auto it_g = (*it_n)->GPUs_begin(); it_g != (*it_n)->GPUs_end(); ++it_g) {
  //       if ((*it_g)->lease == current_event_time)
  //         return true;
  //     }
  //   }
  // }
  return false;
}

// bool Cluster::is_lease_expired(Location loc, time_type current_event_time) {
//   return current_event_time >= Switches[loc.switch_idx]->get_node_ptr(loc.node_idx)->get_GPU_lease(loc.gpu_idx);
// }
bool Cluster::is_usable(Location loc) {
  return _GPU_cluster[loc.switch_idx][loc.node_idx][loc.gpu_idx].running_job == 0
      && _GPU_cluster[loc.switch_idx][loc.node_idx][loc.gpu_idx].reserved_job == 0;
  // return Switches[loc.switch_idx]->get_node_ptr(loc.node_idx)->get_GPU_process(loc.gpu_idx) == 0
  //     && Switches[loc.switch_idx]->get_node_ptr(loc.node_idx)->get_reserve(loc.gpu_idx) == 0;
}
// bool Cluster::is_usable(Location loc, int job_id) {
//   auto np = Switches[loc.switch_idx]->get_node_ptr(loc.node_idx);
//   return np->get_GPU_process(loc.gpu_idx) == 0
//          && (np->get_reserve(loc.gpu_idx) == 0 || np->get_reserve(loc.gpu_idx) == job_id);
// }

bool Cluster::is_available_gpus(std::vector<Location> locs, int job_id) {
  for (auto loc : locs) {
    if (_GPU_cluster[loc.switch_idx][loc.node_idx][loc.gpu_idx].running_job != 0 ||
        (_GPU_cluster[loc.switch_idx][loc.node_idx][loc.gpu_idx].reserved_job != 0 &&
         _GPU_cluster[loc.switch_idx][loc.node_idx][loc.gpu_idx].reserved_job != job_id)) {
          return false;
    }
    // if (Switches[loc.switch_idx]->get_node_ptr(loc.node_idx)->get_GPU_process(loc.gpu_idx) != 0 ||
    //     (Switches[loc.switch_idx]->get_node_ptr(loc.node_idx)->get_reserve(loc.gpu_idx) != 0 &&
    //     Switches[loc.switch_idx]->get_node_ptr(loc.node_idx)->get_reserve(loc.gpu_idx) != job_id)) {
    //   return false;
    // }
  }
  return true;
}

bool Cluster::is_leasable_gpus(std::vector<Location> locs, int job_id, time_type current_time) {
  for (auto loc : locs) {
    auto np = _GPU_cluster[loc.switch_idx][loc.node_idx];
    if (!(np[loc.gpu_idx].running_job == 0 
          && (np[loc.gpu_idx].lease <= current_time
              || np[loc.gpu_idx].reserved_job == 0
              || np[loc.gpu_idx].reserved_job == job_id))) {
    // auto np = Switches[loc.switch_idx]->get_node_ptr(loc.node_idx); 
    // if (!(np->get_GPU_process(loc.gpu_idx) == 0 
    //       && (np->get_GPU_lease(loc.gpu_idx) <= current_time
    //           || np->get_reserve(loc.gpu_idx) == 0
    //           || np->get_reserve(loc.gpu_idx) == job_id))) {
      return false;
    }
  }
  return true;
}

#endif // cluster.h
