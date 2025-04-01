#ifndef __ACTION_H__
#define __ACTION_H__

#include "location.h"
#include "util.h"
#include "duration.hpp"

// enum GPU_type {V100, K80, TitanX};
enum ActionType {compute, aggregate};
enum ContainerStatus {Cold, Warm, Unknown};
// enum ModelType {ResNet56, Inception3, ResNet50, ResNet152, ResNeXt, VGG19_Cifar, VGG19};
// enum DataType {Cifar10, ImageNet};

// extern const int kPeriodN;

class Action {
  private:
    ActionType _type;
    ContainerStatus _warmness;

    time_type _start_time;
    time_type _duration;
    time_type _end_time;

    time_type _expected_next_dur;
    
    int _job_id;
    int _global_step; // moved to job.h, if it does not cause problem, remove this.
    int _index; // logical worker index

    int _kPeriodN;
    // GPU_type _gpu_type;
    // std::vector<Location*> locations;
    Location _location;

  public:
    // constructor
    Action(ActionType, ContainerStatus, time_type, int, int, int, int, Location, int);
    void calc_duration(int, int, int, double, int, int, ContainerStatus, ActionDuration*);
    // void set_duration(int model, int, ContainerStatus); // upcomming
    // void add_duration(time_type duration) { _duration += duration; } // deprecated
    time_type get_end_time() const { return _end_time; }
    void set_end_time(time_type end_time) { _end_time = end_time; }
    ActionType get_type() const { return _type; }
    int get_job_id() const { return _job_id; }
    int get_kPeriodN() const { return _kPeriodN; }
    void set_global_step(int global_step) { _global_step = global_step; }
    int get_global_step() const { return _global_step; }
    time_type get_exp_next_dur() const { return _expected_next_dur; }
    int get_index() const { return _index; }
    void set_location(Location location) { _location = location; }
    Location get_location() const { return _location; }
};


Action::Action(ActionType type, ContainerStatus warmness, time_type start_time,
               int job_id, int global_step, int index, int logical_workers,
               Location location, int kPeriodN) {
  _type = type;
  _warmness = warmness;
  _start_time = start_time;
  _duration = 0;
  _job_id = job_id;
  _global_step = global_step;
  _index = index;
  // _gpu_type = gpu;
  _location = location;
  _kPeriodN = kPeriodN;
}

time_type get_cold_time(int model, int logical_worker) {
  time_type cold_time;
  if (model == ResNet56 || model == PCIFAR) {
    cold_time = 40000;
  } else if (model == VGG19_Cifar) {
    cold_time = 60000;
  } else if (model == ResNeXt) {
    cold_time = 75000;
  } else if (model == ResNet50 || model == PIMAGENET) {
    if (logical_worker == 8) {
      cold_time = 90000;
    } else if (logical_worker == 16) {
      cold_time = 120000;
    } else {
      cold_time = 65000;
    }
  } else if (model == Inception3) {
    if (logical_worker == 8 || logical_worker == 16) {
      cold_time = 130000;
    } else {
      cold_time = 90000;
    }
  } else if (model == VGG19) {
    if (logical_worker == 8) {
      cold_time = 95000;
    } else if (logical_worker == 16) {
      cold_time = 130000;
    } else {
      cold_time = 70000;
    }
  } else if (model == LSTM) {
    if (logical_worker == 8) {
      cold_time = 70000;
    } else if (logical_worker == 16) {
      cold_time = 80000;
    } else {
      cold_time = 60000;
    }
  } else if (model == GNMT) {
    if (logical_worker == 8) {
      cold_time = 130000;
    } else if (logical_worker == 16) {
      cold_time = 140000;
    } else {
      cold_time = 120000;
    }
  } else if (model == GPT2_SMALL) {
    if (logical_worker <= 4) {
      cold_time = 34000;
    } else if (logical_worker == 8) {
      cold_time = 39000;
    } else if (logical_worker == 16) {
      cold_time = 40000;
    }
  } else if (model == BERTLARGE) {
    if (logical_worker <= 4) {
      cold_time = 35000;
    } else if (logical_worker == 8) {
      cold_time = 42000;
    } else if (logical_worker == 16) {
      cold_time = 51000;
    }
  }
  return cold_time;
  // return 0;
}
// TODO: consider model, batchsize, locality(=distance), and whatever else. (syncN, wave)
// TODO: this only called for aggregator duration. aggregate action need to consider #wave of N scaled compute action duration
void Action::calc_duration(int model, int batchsize, int logical_worker,
                           double distance, int syncN, int wave,
                           ContainerStatus compute_action_warmness,
                           ActionDuration* adp) {
  time_type train_time(0), cold_time(0);

  cold_time = get_cold_time(model, logical_worker);

  train_time = adp->get_dur(model, logical_worker, distance, syncN, wave);
  if (ignore_framework) {
    train_time = get_duration_wo_framework(model, logical_worker);
  }
  if (ignore_init || ignore_framework) {
    cold_time = 0;
  }

  _duration = train_time;

  _expected_next_dur = _duration;

  if (_warmness == Cold || compute_action_warmness == Cold) {
    _duration += cold_time;
  }
  
  _end_time = _start_time + _duration;
}

#endif // __ACTION_H__
