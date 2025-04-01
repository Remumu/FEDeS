#ifndef __NODE_H__
#define __NODE_H__

#include <vector>
#include <string>
#include <tuple>

enum GPU_type {V100, K80, TitanX};
// enum GPU_status {idle, busy}; // occupied is changed to busy
// typedef std::pair<GPU_type,std::string> GPU; // GPU type, job id => GPU type, reserved job, occupy job
// The reason I did not make GPU as a class was complexity of referencing cluster->switch->node
// friend or inheritance may solve it
// friend will work, but for that, many component has to be changed. Keep for now!

// typedef std::tuple<GPU_type, int /*reservedJob*/, int /*occupyJob*/> GPU; // 0 for no job. job id start from 1
struct GPU {
  GPU_type type;
  int reserved_job;
  int running_job;
  long long lease;
};

class Node {
  private:
    std::vector<GPU*> GPUs;
    int idle_GPUs;
  public:
    Node();
    void add_GPU(GPU &gpu) { GPUs.push_back(&gpu); }
    void set_idle_GPUs(int num) { idle_GPUs = num; }
    void increase_idle_GPUs() { idle_GPUs += 1; }
    void decrease_idle_GPUs() { idle_GPUs -= 1; }
    int get_idle_GPUs(); // { return idle_GPUs; }
    void set_GPU_process(int GPU_idx, int GPU_process) { GPUs[GPU_idx]->running_job = GPU_process; }
    int get_GPU_process(int GPU_idx) { return GPUs[GPU_idx]->running_job; }
    void set_reserve(int GPU_idx, int jid) { GPUs[GPU_idx]->reserved_job = jid; }
    int get_reserve(int GPU_idx) { return GPUs[GPU_idx]->reserved_job; }
    GPU_type get_GPU_type(int GPU_idx) { return GPUs[GPU_idx]->type; }
    void set_GPU_lease(int GPU_idx, long long lease) { GPUs[GPU_idx]->lease = lease; }
    long long get_GPU_lease(int GPU_idx) { return GPUs[GPU_idx]->lease; }

    std::vector<GPU*>::iterator GPUs_begin() { return GPUs.begin(); }
    std::vector<GPU*>::iterator GPUs_end() { return GPUs.end(); }
};

Node::Node() {}

// update and get idle #GPU
int Node::get_idle_GPUs() {
  int count = 0;
  for (GPU* device : GPUs) {
    if (device->running_job == 0) {
      count += 1;
    }
  }
  set_idle_GPUs(count);
  return idle_GPUs;
}

#endif // __NODE_H__
