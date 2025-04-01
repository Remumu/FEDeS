#ifndef __JOB_H__
#define __JOB_H__

#include <string>
#include <vector>
#include <list>
#include <iostream>
#include <algorithm>
#include <tuple>
#include <assert.h>
#include <exception>
#include <limits.h>

#include "action.h"
#include "cluster.h"
#include "location.h"
// #include "util.h"

enum Status{kNone,kSubmit,kQueue,kReady,kRun,kYield,kWait,kEnd};
extern std::ostringstream oss;
// extern const int kPeriodN;
extern const unsigned kMinGuarantee;

struct FairRecord {
  time_type time_stamp;
  int num_job;
  double share;
  int gpu_load;
  int uniq_node;
  int gpu_usg;

  bool operator==(const FairRecord& rhs) const {
    return num_job   == rhs.num_job   &&
           share     == rhs.share     &&
           gpu_load  == rhs.gpu_load  &&
           uniq_node == rhs.uniq_node &&
           gpu_usg   == rhs.gpu_usg;
  }
};

class Job {
  private:
    int _id;

    time_type _submit_time;
    time_type _admitted_time;
    int _gpu_req;

    int _batch_size;
    int _iteration;
    ModelType _model;
    DataType _dataset;
    time_type _ckpt_time, _init_time, _load_time;

    int _global_step;
    int _remaining_steps;
    time_type _remaining_time;
    time_type _start_time_stamp;
    time_type _end_time;
    time_type _end_time_stamp;
    // int _restart_time_stamp;
    time_type _time_stamp;
    time_type _duration;
    time_type _duration_earn; // duration earn is time job is run until evicted. used for step counting

    double _ftf; // used in themis sorting

    time_type _executed_time; // used in las promote
    time_type _pending_time; // used in las promote
    int _promote_count;

    time_type _duration_one_step;
    // std::vector<Location> _locations;

    time_type _ready_time;
    time_type _yield_time;
    int _yield_count;

    // for delay schedule
    int _try_count;
    time_type _time_out_time;
    time_type _back_off_time;

    time_type _ideal_run_time;
    unsigned int _rank_irt;
    // record time_stamp and num_jobs at every changes of submitted jobs, so that ideal share can be computed after job end
    std::vector<FairRecord> _fair_records;
    double _fairness; // ideal share is max-min
    double _fairness2; // ideal share is 1/n
    double _fairness3; // ideal share is half max-min
    double _fairness4; // ideal share is proportional to gpu load
    double _fairness5; // GPU share fairness

    double _avg_loc_dist;
    double _avg_loc_bit;

    double _n_avg;
    time_type _acc_njob;

    Status _status;

  public:
    Job(int, time_type, int, int, int, ModelType, DataType);
    void scheduled(time_type current_event_time);
    void run(time_type);
    void update_status(time_type current_event_time);
    void evicted(time_type current_event_time);
    void yield(time_type current_event_time);
    void end(time_type);

    void set_admitted_time(time_type event_time) {
      _admitted_time = event_time;
    }
    time_type get_admitted_time() const { return _admitted_time; }

    // update records for fairness computation. this function should be called when total #jobs changes
    // time: current event time
    // num_jobs: how many jobs were in cluster right before 'time'
    void update_fair_records(time_type, int, double, int);
    void print_fair_records() const;
    void clear_fair_records() { _fair_records.clear(); }
    bool is_fair_records_empty() { return _fair_records.empty(); }

    time_type calc_ideal_one_step() const;
    double calc_fairness(Cluster*, bool);

    int calc_duration_one_step();
    int calc_distance();
    std::vector<Location> locations;
    int count_uniq_node();

    // int calc_remaining_time(int);
    // int get_remaining_time() { return _remaining_time; }
    time_type calc_remain_time_to_compare();

    //debug
    int get_remain_step() const { return _remaining_steps; }

    int get_id() const { return _id; }
    time_type get_submit_time() const { return _submit_time; }
    int get_gpu_req() const { return _gpu_req; }
    time_type get_duration() const { return _duration; }

    time_type get_start_time_stamp() const {return _start_time_stamp;}
    time_type get_end_time() const {return _end_time;}

    void set_start_time_stamp(time_type start_time) { _start_time_stamp = start_time; }
    // void set_end_time(int end_time) { _end_time = end_time; }

    bool is_end() { return _global_step >= _iteration; }

    void set_rank_irt(unsigned rank) { _rank_irt = rank; }
    time_type get_irt() const { return _ideal_run_time; }
    double get_fairness() const { return _fairness; }
    double get_fairness2() const {return _fairness2;}
    double get_fairness3() const  {return _fairness3;}
    double get_fairness4() const {return _fairness4;}
    double get_fairness5() const {return _fairness5;}
    double get_locality_dist() const { return _avg_loc_dist; }
    double get_locality_bit() const { return _avg_loc_bit; }
    double get_n_avg() const { return _n_avg; }
    void statistics(bool) const;

    Status get_status() const { return _status; }
    void set_status(Status status) { _status = status; }

    time_type get_ready_time() const { return _ready_time; }
    time_type get_yield_time() const { return _yield_time; }
    int get_yield_count() const { return _yield_count; }

    int get_try_count() const { return _try_count; }
    void increase_try_count() { _try_count += 1; }
    void reset_try_count() { _try_count = 0; } // 0 or -1?
    void set_time_out_time(time_type time_out_time) { _time_out_time = time_out_time; }
    time_type get_time_out_time() const { return _time_out_time; }

    void set_back_off(time_type back_off_time) { _back_off_time = back_off_time; }
    time_type get_back_off() const { return _back_off_time; }

    // double get_rho(time_type current_event_time, Cluster*, double) const;
    double get_rho() const { return _ftf; }
    double compute_finish_time_fairness(time_type current_event_time, Cluster*, double);

    time_type get_pending_time() { return _pending_time; }
    void reset_pending_time() { _pending_time = 0; }
    time_type get_executed_time() { return _executed_time; }
    void reset_executed_time() { _executed_time = 0;}
    void update_time_for_promote(time_type elapsed_time);
    void increase_promote_count() { _promote_count += 1; }
};
Job::Job(int id, time_type submit_time, int gpu_req, int batch_size,
         int iteration, ModelType model, DataType dataset) {
  _id = id;
  _submit_time = submit_time;
  _gpu_req = gpu_req;
  _batch_size = batch_size;
  _iteration = iteration;
  _model = model;
  _dataset = dataset;
  _global_step = 0;
  _start_time_stamp = LLONG_MAX;
  _duration = 0;
  _duration_earn = 0;
  _remaining_steps = _iteration;
  _remaining_time = -1;
  _status = kNone;
  _acc_njob = 0;
  _try_count = 0;
  _yield_count = 0;
  _pending_time = 0;
  _executed_time = 0;
  _promote_count = 0;
  _ckpt_time = 20000;
  if (ignore_init) {
    _ckpt_time = 0;
    _init_time = 0;
  } else if (dataset == Cifar10) { // || ignore_scale_factor) { // deprecated
    _init_time = 57000;
    if (model == ResNeXt) {
      _init_time = 46000;
      if (gpu_req == 1)
        _init_time = 50350;
      else if (gpu_req == 2)
        _init_time = 60000;
      else if (gpu_req == 4)
        _init_time = 80333;
      else if (gpu_req == 8)
        // _init_time = 93000;
        _init_time = 113000;
      else if (gpu_req == 16)
        // _init_time = 68767;
        _init_time = 138767;
    } else if (model == VGG19_Cifar) {
      _init_time = 51000;
      if (gpu_req == 1)
        _init_time = 31000;
      else if (gpu_req == 2)
        _init_time = 32750;
      else if (gpu_req == 4)
        _init_time = 38000;
      else if (gpu_req == 8)
        // _init_time = 43000;
        _init_time = 63000;
      else if (gpu_req == 16)
        _init_time = 80000;
    }
  } else if (dataset == ImageNet) {
    _init_time = 180000;
    if (model == Inception3) {
      _init_time = 256000;
      if (gpu_req == 1) {
        _init_time = 64000;
        // _init_time = 189886;
      } else if (gpu_req == 2) {
        _init_time = 215250;
      } else if (gpu_req == 4) {
        _init_time = 130000;
        // _init_time = 259917;
      } else if (gpu_req == 8) {
        _init_time = 170000;
        // _init_time = 292971;
      } else if (gpu_req == 16){
        _init_time = 155000;
        // _init_time = 290500;
      }
    } else if (model == ResNet50) {
      _init_time = 245000;
      if (gpu_req == 1) {
        _init_time = 55000;
        // _init_time = 171679;
      } else if (gpu_req == 2) {
        _init_time = 70000;
        // _init_time = 201500;
      } else if (gpu_req == 4) {
        _init_time = 110000;
        // _init_time = 256885;
      } else if (gpu_req == 8) {
        _init_time = 130000;
        // _init_time = 280103;
      } else if (gpu_req == 16) {
        _init_time = 130000;
        // _init_time = 295117;
      }
    } else if (model == VGG19) {
      _init_time = 219000;
      if (gpu_req == 1) {
        // _init_time = 35000;
        _init_time = 168367;
      } else if (gpu_req == 2) {
        _init_time = 216500;
      } else if (gpu_req == 4) {
        // _init_time = 120000;
        _init_time = 243429;
      } else if (gpu_req == 8) {
        // _init_time = 95000;
        _init_time = 263151;
      } else if (gpu_req == 16) {
        // _init_time = 95000;
        _init_time = 265500;
      }
    }
  } else if (model == LSTM) {
    _init_time = 180000;
    // _init_time = 50000;
  } else if (model == GNMT) {
    // _init_time = 105000;
    _init_time = 200000;
  } else if (model == GPT2_SMALL) {
    if (gpu_req == 1) {
      _init_time = 45826;
    } else if (gpu_req == 2) {
      _init_time = 44900;
    } else if (gpu_req == 4) {
      _init_time = 46030;
    } else if (gpu_req == 8) {
      _init_time = 47342;
    } else if (gpu_req == 16) {
      _init_time = 48049;
    }
  } else if (model == BERTLARGE) {
    if (gpu_req == 1) {
      _init_time = 71902;
    } else if (gpu_req == 2) {
      _init_time = 68149;
    } else if (gpu_req == 4) {
      _init_time = 71147;
    } else if (gpu_req == 8) {
      _init_time = 77120;
    } else if (gpu_req == 16) {
      _init_time = 76025;
    }
  }
  _load_time = _init_time;
}

// return the number of nodes where job is placed
int Job::calc_distance() {
  std::vector<int> node_uniq;
  for (auto location : locations) {
    auto it = find(node_uniq.begin(), node_uniq.end(), location.node_idx);
    if (it == node_uniq.end()) {
      node_uniq.push_back(location.node_idx);
    }
  }
  return node_uniq.size();
}
// duplicated for consistency with msjob
int Job::count_uniq_node() {
  std::vector<int> node_uniq;
  for (auto location : locations) {
    auto it = find(node_uniq.begin(), node_uniq.end(), location.node_idx);
    if (it == node_uniq.end()) {
      node_uniq.push_back(location.node_idx);
    }
  }
  return node_uniq.size();
}

int Job::calc_duration_one_step() {
  int distribution = calc_distance();
  try {
    _duration_one_step = get_one_step_duration(_model, _gpu_req, distribution);
  } catch (int num) {
    oss << "[Job" << _id << "] ERROR: Unknown model or unavailable num_gpu : "
        << num << std::endl;
    put_log(oss, kCore);
    assert(false);
  } catch (double num) {
    oss << "[Job" << _id << "] ERROR: Unavailable distribution : " << num
        << std::endl;
    put_log(oss, kCore);
    assert(false);
  }

  if (ignore_framework) {
    _duration_one_step = get_duration_wo_framework(_model, _gpu_req);
  }

  return 0;
}

time_type Job::calc_ideal_one_step() const {
  time_type duration_one_step;
  int ideal_distribution = _gpu_req / kGPU_p_node;
  if (_gpu_req % kGPU_p_node != 0) {
    ideal_distribution += 1;
  }
  try {
    duration_one_step = get_one_step_duration(_model, _gpu_req,
                                              ideal_distribution);
  } catch (int num) {
    oss << "[Job" << _id << "] ERROR: Unknown model or unavailable num_gpu : "
        << num << std::endl;
    put_log(oss, kCore);
    assert(false);
  } catch (double num) {
    oss << "[Job" << _id << "] ERROR: Unavailable distribution : " << num
        << std::endl;
    put_log(oss, kCore);
    assert(false);
  }

  return duration_one_step;
}

void Job::print_fair_records() const {
  oss << "Job" << _id << std::endl;
  for (const auto& record : _fair_records) {
    oss << record.time_stamp << ',' << record.share << ',' << record.num_job << std::endl;
  }
  put_log(oss,kInfo);
}

void Job::update_fair_records(time_type current_event_time, int num_jobs, double share, int gpu_load) {
  int uniq_node = 0;
  int gpu_usg = 0;
  if (_status == kRun || _status == kReady || _status == kYield)
    uniq_node = count_uniq_node();
  if (_status == kRun || _status == kReady || _status == kYield) {
    gpu_usg = _gpu_req;
  }
  FairRecord fair_record = {
      current_event_time, // time_stamp
      num_jobs,           // num_job
      share,              // share
      gpu_load,           // gpu_load
      uniq_node,          // uniq_node
      gpu_usg             // gpu_usg
  };
/// CHECK: consider seperate this block as a function
  if (_fair_records.empty()) {
    _acc_njob += (current_event_time - _submit_time) * num_jobs;
  } else {
    _acc_njob += (current_event_time - _fair_records.back().time_stamp) * num_jobs;
  }

  if (!_fair_records.empty() && fair_record == _fair_records.back()) {
    _fair_records.back().time_stamp = current_event_time;
  } else {
    _fair_records.push_back(fair_record);
  }
}
// compute fairness
// divisor is JCT, divider is time with ideal share
double Job::calc_fairness(Cluster* cluster_ptr, bool admission_control) {
  // std::ostringstream oss;
  // std::cout << "[Debug] calc fairness of job" << _id << "\n";
  // if (_id == 16) {
  //   for (auto elem : _fair_records) {
  //     std::cout << "(" << elem.first << "," << elem.second << ") ";
  //   }
  //   std::cout << std::endl;
  // }
  int total_GPUs = cluster_ptr->get_total_GPUs();
  double aux(0), aux2(0), aux3(0), aux4(0), aux5(0), aux6(0), aux7(0), aux8(0);
  time_type time_stamp;
  if (admission_control)
    time_stamp = _admitted_time;  //_submit_time;
  else
    time_stamp = _submit_time;
  for (auto record : _fair_records) {
    double ideal_share = record.share; // gpu_share
    double ideal_share2 = (double)total_GPUs / record.num_job; // total_GPUs / num_jobs
    double ideal_share3 = (ideal_share2 > _gpu_req) ? _gpu_req : ideal_share2;
    double gpu_load = record.gpu_load/(double)total_GPUs;
    if (gpu_load < 1) {
      gpu_load = 1;
    }

    int node_size = cluster_ptr->get_node_size();
    int min_place = _gpu_req / node_size;
    if (_gpu_req % node_size > 0) {
      min_place += 1;
    }
    // double loc_score = std::get<4>(elem) / (double)min_place;
    double loc_score;
    if (record.uniq_node == 0) {
      loc_score = 0;
    } else {
      loc_score = (double)min_place / record.uniq_node;
    }
    int loc_bit = (record.uniq_node == min_place) ? 1 : 0;

    time_type weight = record.time_stamp - time_stamp;
    assert(weight >= 0);
    aux += ideal_share * weight; // ideal GPU share
    aux2 += ideal_share2 * weight;
    aux3 += ideal_share3 * weight;
    aux4 += gpu_load * weight;
    aux5 += loc_score * weight;
    aux6 += loc_bit * weight;
    aux7 += record.num_job * weight;
    aux8 += record.gpu_usg * weight; // real GPU usg
    time_stamp = record.time_stamp;
  }
  // std::cout << "time_stamp: " << time_stamp << ", end_time_stamp: " << _end_time_stamp << std::endl;
  if (time_stamp != _end_time_stamp) {
    oss << "time record: ";
    for (auto record : _fair_records) {
      oss << record.time_stamp << " ";
    }
    oss << std::endl;
    oss << "time_stamp: " << time_stamp << ", end_time_stamp: " << _end_time_stamp << std::endl;
    put_log(oss,kDebug);
    // assert(time_stamp == _end_time);
    throw std::logic_error("Last time stamp is not match with end time");
  }
  time_type jct;
  if (admission_control) {
    jct = _end_time_stamp - _admitted_time;  //_submit_time;
  } else {
    jct = _end_time_stamp - _submit_time;
  }
  double ideal_avg_share = aux / jct;
  double ideal_avg_share2 = aux2 / jct;
  // double ideal_avg_share3 = aux3 / jct;
  double avg_gpu_load = aux4 / jct;

  _avg_loc_dist = aux5 / _duration;
  _avg_loc_bit = aux6 / _duration;

  _n_avg = aux7 / jct;
  double ideal_avg_share3 = (double)total_GPUs / _n_avg;
  if (ideal_avg_share3 > _gpu_req)
    ideal_avg_share3 = _gpu_req;

  oss << "[Debug] weighted average share: " << ideal_avg_share << " or " << ideal_avg_share2 << std::endl;
  put_log(oss,kDebug);

  time_type ideal_duration_one_step = calc_ideal_one_step();

  _ideal_run_time = ideal_duration_one_step * _iteration + _init_time;
  // _fairness = ideal_duration_one_step * _iteration / ( jct * ideal_avg_share / _gpu_req ); // change divisor, divider
  // std::cout << "Job" << _id << " fairness: (" << jct << " * " << ideal_avg_share << " / " << _gpu_req << " ) / (" << ideal_duration_one_step << " * " << _iteration << ")\n";
  _fairness = jct / (ideal_duration_one_step * _gpu_req / ideal_avg_share * _iteration + _init_time);
  // _fairness2 = jct / (ideal_duration_one_step * _n_avg * _iteration + _init_time);
  _fairness2 = jct / (ideal_duration_one_step * avg_gpu_load * _iteration + _init_time);
  // _fairness2 = jct / (ideal_duration_one_step * _gpu_req / ideal_avg_share2 * _iteration + _init_time);
  // _fairness3 = (jct * ideal_avg_share3 / _gpu_req ) / (ideal_duration_one_step * _iteration + _init_time);
  _fairness3 = jct / (ideal_duration_one_step * _gpu_req / ideal_avg_share3 * _iteration + _init_time);
  _fairness4 = jct / (ideal_duration_one_step * _iteration * avg_gpu_load);
  _fairness5 = aux8 / aux;
  return _fairness;
}

// Initiation for training job
// GPU placement is set before this function is called
void Job::scheduled(time_type current_event_time) {
  int ret_code = calc_duration_one_step();
  oss << "[Debug] Job" << _id << " ret_code(duration): " << ret_code
      << '(' << _duration_one_step << ')' << std::endl;
  put_log(oss,kDebug);
  assert(ret_code == 0);

  if (_start_time_stamp == LLONG_MAX) {
    _start_time_stamp = current_event_time;
    _ready_time = current_event_time + _init_time;
  } else {
    _ready_time = current_event_time + _load_time;
  }
  _time_stamp = current_event_time;

  _status = kReady;
  _try_count = 0;
}

// start training
void Job::run(time_type current_event_time) {
  time_type _progress_time_in_this_turn = current_event_time - _time_stamp;
  _time_stamp = current_event_time;
  _duration += _progress_time_in_this_turn;
  // int ret_code = calc_duration_one_step();
  // oss << "[Debug] Job" << _id << " ret_code(duration): " << ret_code
  //     << '(' << _duration_one_step << ')' << std::endl;
  // put_log(oss,kDebug);
  // assert(ret_code == 0);
  _remaining_time = _remaining_steps * _duration_one_step;
  _end_time = current_event_time + _remaining_time;
  _status = kRun;
}

// Called when any event triggered
/// CHECK: or maybe only in scheduling event is triggered.
/// since it seems only _remaining_time is uesed in scheduling
void Job::update_status(time_type current_event_time) {
  if (_status == kRun) {
    time_type _progress_time_in_this_turn = current_event_time - _time_stamp;
    _time_stamp = current_event_time; // becareful. is _time_stamp working fine?
    _duration += _progress_time_in_this_turn;
    _duration_earn += _progress_time_in_this_turn;
    _remaining_time -= _progress_time_in_this_turn;
  // } else if (_status == kQueue) {
  //   time_type progress_time_in_this_turn = current_event_time - _time_stamp;
  }
  // _remaining_steps -= _progress_time_in_this_turn / _duration_one_step;
}

void Job::evicted(time_type current_event_time) {
  time_type _progress_time_in_this_turn = current_event_time - _time_stamp;
  _time_stamp = current_event_time;
  _duration += _progress_time_in_this_turn;
  // _duration_earn += _progress_time_in_this_turn;
  // _remaining_steps -= _duration_earn / _duration_one_step; // _remaining_time will be calcuated based on _remaining_steps when job is rescheduled
  // _duration_earn = 0;
  locations.clear();
  // Need to release gpu at this moment!
  // Remove from list too!
  _status = kQueue;
}

void Job::yield(time_type current_event_time) {
  // TODO: put ckpt_time according to models
  // maybe this time can be collapesd as data moved out from GPU to CPU mem, new job may be able to be started
  // time_type ckpt_time = 17000; // in milliseconds. this may have to be scaled as temporal dimension is scaled.
  _yield_time = current_event_time + _ckpt_time;
  _yield_count += 1;

  // time compute for progress checking
  time_type _progress_time_in_this_turn = current_event_time - _time_stamp;
  _time_stamp = current_event_time;
  _duration += _progress_time_in_this_turn;
  if (_status == kRun) {
    _duration_earn += _progress_time_in_this_turn;
    _remaining_steps -= _duration_earn / _duration_one_step; // _remaining_time will be calcuated based on _remaining_steps when job is rescheduled
  }
  _duration_earn = 0;

  _status = kYield;
}

void Job::end(time_type current_event_time) {
  
  // evicted(current_event_time);

  time_type _progress_time_in_this_turn = current_event_time - _time_stamp;
  _time_stamp = current_event_time;
  _duration += _progress_time_in_this_turn;
  // _duration_earn += _progress_time_in_this_turn;
  // _remaining_steps -= _duration_earn / _duration_one_step; // _remaining_time will be calcuated based on _remaining_steps when job is rescheduled
  // _duration_earn = 0;
  locations.clear();

  _end_time_stamp = current_event_time;

  _remaining_steps = 0;
  _remaining_time = 0;
  _status = kEnd;
  // Need to release gpu at this moment!
  // Remove from list too!
}

// Unexpected value error cases are handled by calc_duration_one_step function
time_type Job::calc_remain_time_to_compare() {
  // when the job is before first scheduled
  if (_remaining_time == -1) {
    // use consolidated placement value
    time_type duration_one_step = calc_ideal_one_step();

    return _remaining_steps * duration_one_step;
  } else {
    return _remaining_time;
  }
}

// // return finish time fairness
// double Job::get_rho(time_type current_event_time, Cluster* cluster_ptr, double N_default) const {
//   return this->compute_finish_time_fairness(current_event_time, cluster_ptr, N_default);
// }
// (pastTime + iterLeft * iterTime) / (iterTotal * iterTime * Navg)
double Job::compute_finish_time_fairness(time_type current_event_time, Cluster* cluster_ptr, double N_default) {
/*
  time_type time_stamp = _submit_time;
  long long aux = 0;
/// CHECK: consier accumulating values instead of fair recrods?
  for (const auto& record : _fair_records) {
    time_type weight = record.time_stamp - time_stamp;
    assert(weight >= 0);
// base version
    aux += record.num_job * weight;
// end base version
// idea 1
    // if (record.gpu_load > cluster_ptr->get_total_GPUs())
    //   aux += record.num_job * weight;
    // else
    //   aux += weight;
// end idea 1
// idea 2
    // if (record.gpu_load > cluster_ptr->get_total_GPUs())
    //   aux += record.gpu_load * weight;
    // else
    //   aux += cluster_ptr->get_total_GPUs() * weight;
// end idea 2
    time_stamp = record.time_stamp;
  }
*/
  double Navg = N_default;
  // if (current_event_time != _submit_time)
  //   Navg = (double)aux / (current_event_time - _submit_time);
  if (_acc_njob != 0) {
    Navg = (double)_acc_njob / (current_event_time - _submit_time);
  }

// idea 2
  // Navg /= cluster_ptr->get_total_GPUs();
// end idea 2
   // for t_share, ideal step length or realistic step length?
/// CHECK: remain steps is not updated in running
  time_type t_share = current_event_time - _submit_time + _remaining_steps * calc_ideal_one_step();
  if (_status == kQueue || _status == kWait)
    t_share += _init_time;
  // double n_gpu_share;
// 1/N version
  // if (cluster_ptr->get_total_GPUs() / Navg > _gpu_req)
  //   n_gpu_share = _gpu_req;
  // else
  //   n_gpu_share = cluster_ptr->get_total_GPUs() / Navg;
  // double t_ideal = _iteration * calc_ideal_one_step() * _gpu_req / n_gpu_share + _init_time;
// end 1/N version

// N_avg version (Themis equation)
  double t_ideal = _iteration * calc_ideal_one_step() * Navg + _init_time;

  _ftf = t_share / t_ideal;
  return t_share / t_ideal;
}

void Job::update_time_for_promote(time_type elapsed_time) {
  if (_status == kRun || _status == kReady || _status == kYield) {
    _executed_time += elapsed_time;
  } else if (_status == kQueue || _status == kWait) {
    _pending_time += elapsed_time;
  }
}

// TODO: add queue time, pending time
void Job::statistics(bool admission_control) const {
  // std::ostringstream oss;
  time_type submitted_time;
  if (admission_control) {
    submitted_time = _admitted_time;
  } else {
    submitted_time = _submit_time;
  }
  oss << "Job" << _id << "," << submitted_time << "," << _start_time_stamp
      << "," << (_end_time_stamp - submitted_time) << "," << _duration << ","
      << _fairness << "," << _fairness2 << "," << _fairness3 << ","
      << _fairness4 << ',' << _fairness5 << ',' << _avg_loc_dist << ","
      << _avg_loc_bit << "," << _yield_count << ',' << _n_avg << ','
      << _promote_count << ','
      << ModelNameStr[_model] << ',' << _gpu_req << ','
      << _rank_irt << ',' << _ideal_run_time
      << std::endl;
  put_log(oss,kQuiet);
}


/******************************************************************************
 * Class MicroserviceJob
 * This class simulate behavior of GRML, serverless based DLT framework
******************************************************************************/
class MicroserviceJob {
  private:
    int _id;

    time_type _submit_time;
    bool _submit_flag;
    time_type _admitted_time;
    time_type _scheduled_time;
    unsigned _gpu_req;
    int _sync_N;
    // int _logical_worker; // deprecated
    // int _gpu_req_queued;
    unsigned _fair_share;
    unsigned _perf_share;
    // int _share;

    int _batch_size;
    int _iteration;
    ModelType _model;
    std::string _dataset;

    int _global_step;
    time_type _start_time;
    time_type _end_time;
    time_type _restart_time;
    time_type _duration;
    time_type _expected_end_time;

    time_type _ideal_run_time;
    unsigned int _rank_irt;
    double _ooit_inc_p_gpu;

    unsigned int _cold_start_count;

    unsigned int _counter_dist_expand;
    unsigned int _counter_unlucky_expand;
    unsigned int _counter_lucky_expand;
    unsigned int _counter_lucky_shrink;

    double _avg_wave;
    double _avg_gpu_usg;

    std::vector<double> _locality_records;

    // record time_stamp and num_jobs at every changes of submitted jobs, so that ideal share can be computed after job end
    std::vector<FairRecord> _fair_records;
    double _fairness;
    double _fairness2;
    double _fairness3;
    double _fairness4;
    double _fairness5;

    double _rho_inv;

    double _avg_loc_dist;
    double _avg_loc_bit;
    double _avg_loc_content;

    double _n_avg;

    // int _wave_done_action;
    int _iteration_done_action;
  // depricated
    // std::vector<Action> _Wave; // _logical_worker (=_gpu_req/#wave) elem
    // std::vector<Action> _Iter; // _gpu_req  elem
    std::vector<Action> _ComputeWave; // used later instead of _Wave // for now 1D, if it consider waves detail, it become 2D and affect _Aggr locating
    std::vector<Action> _Aggr; // _gpu_req elem
    // std::vector<Action> _Aggregator; // used later instead of _Aggr
    bool _stop;
    std::vector<Location> _reserved_locations;
    std::vector<Location> _prev_locs;

    bool _evict;
    time_type _attained_service;
    time_type _time_stamp_as; // time stamp for compute _attained_service time
    time_type _online_jct;
    time_type _time_stamp_oj;
    time_type _online_run;
    int _kick_count;
    int _sfl_count;
    int _iq_kick_count;

    time_type _queued_time;
    time_type _runned_time;
    time_type _serviced_time;

    time_type _remain_service;
    time_type _remain_time;
    double _alpha_fairness_value;
    unsigned _prev_fair_share;

    enum MSJStatus {kNone, kSubmit, kQueue, kRun, kSync, kWait, kEnd};
    MSJStatus _status;

    // Status _status;
    
  public:
    // Status _status;

    MicroserviceJob(int, time_type, int, int, int, ModelType, DataType);

    void set_kPeriodN(int kPeriodN) { _sync_N = kPeriodN; }

    ModelType get_model() const { return _model; }
    bool is_loc_sensitive() const;
    bool is_loc_sensitive_wrt_NN() const;
    bool is_loc_sensitive_wrt_LW() const;
    int get_loc_sensitivity_wrt_wave() const;

    void set_submit_flag() { _submit_flag = true; }
    bool get_submit_flag() const { return _submit_flag; }
    void scheduled(time_type current_event_time); // action(s) be scheduled
    void evicted(time_type current_event_time);
    void accum_dur_from_sched(time_type current_event_time);
    void sync_iter_action(time_type current_event_time);
    void wait_in_iq(time_type current_event_time);
    bool is_in_wait() const { return _status == MSJStatus::kWait; }

    int calc_distance();
    int count_uniq_node() const;

    void calc_remain_service(ActionDuration*);
    // NOTE: remain_service has to be updated by calc func before called by get
    time_type get_remain_service() const { return _remain_service; }
    time_type get_remain_time() const { return _remain_time; }
    double get_alpha_fairness_value() const { return _alpha_fairness_value; }
    void set_prev_fair_share() { _prev_fair_share = _fair_share; }
    unsigned get_prev_fair_share() const { return _prev_fair_share; }
    void update_alpha_fairness_value(double value) { _alpha_fairness_value = value; }

    time_type calc_remain_time(ActionDuration* adp, unsigned gpu_req, int distance);
    double calc_alpha_fairness_value(ActionDuration*, double alpha, int num_gpu, int prev_share, double round_interval=1.8);

    const Action& get_action(int action_idx) const { return _Aggr[action_idx]; }
    int get_action_global_step(int action_idx) const { return _Aggr[action_idx].get_global_step(); }
    time_type get_action_end_time(int action_idx) const { return _Aggr[action_idx].get_end_time(); }
    bool is_past_action_end(int action_idx, time_type current_event_time) {
      return _Aggr[action_idx].get_end_time() < current_event_time;
    }
    void update_action(int action_idx, time_type current_event_time);

    void invoke_iteration(time_type, Cluster*, ActionDuration*,
                          ContainerStatus);
 
    time_type compute_expected_end_time() const;
    time_type get_expected_end_time() const { return _expected_end_time; }
    void set_expected_end_time(time_type ete) { _expected_end_time = ete; }
    // void invoke_iteration(time_type, Cluster*, ActionDuration*, std::list<Action*>*,
    //                       std::list<time_type>*, std::list<time_type>*,
    //                       ContainerStatus);

    void invoke_iteration_tms(time_type, Cluster*, ActionDuration*, std::list<Action*>*,
                          std::list<time_type>*, std::list<time_type>*,
                          std::vector<Location>&);
    void invoke_iteration_tms(time_type, Cluster*, ActionDuration*, std::list<Action*>*,
                          std::list<time_type>*, std::list<time_type>*);

    void end(time_type);

    void set_admitted_time(time_type event_time) {
      _admitted_time = event_time;
    }
    // int get_admitted_time() {
    //   return _admitted_time;
    // }

    double locality_score(Cluster*) const;
    void update_locality_records(Cluster*);
    bool is_best_placed(Cluster*) const;
    double mean_locality_score() const;
    void reset_locality_records() { _locality_records.clear(); }
    std::vector<int> get_node_idx() const;
    std::vector<Location> get_locations() const;
    void set_stop(bool signal) { _stop = signal; }
    bool get_stop_flag() const { return _stop; }
    void reserve_move(Cluster*, /*std::vector<Location>&,*/ std::vector<Location>&);
    std::vector<Location> get_reserve_locations() const { return _reserved_locations; }
    void release_reservation(Cluster*);
    void release_lease_expired_reservation(Cluster*, time_type);

    void set_evict(bool signal) { _evict = signal; }
    bool get_evict_flag() const { return _evict; }

    void set_rho_inv(double rho_inv) { _rho_inv = rho_inv; }
    double get_rho_inv() const { return _rho_inv; }
    // double compute_finish_time_fairness(time_type, Cluster*, ActionDuration*);
    // double compute_finish_time_fairness(time_type, Cluster*, ActionDuration*, int share);
    void compute_and_set_rho_inv(time_type, int njobs, Cluster*, ActionDuration*);
    double compute_rho_inv(time_type, int njobs, Cluster*, ActionDuration*, int share);

    void update_service_time(time_type);
    time_type get_service_time() const { return _attained_service; }
    void update_online_jct(time_type);
    time_type get_online_jct() const { return _online_jct; }
    time_type get_online_run() const { return _online_run; }

    void increase_kick_count() { _kick_count += 1; }
    int get_kick_count() const { return _kick_count; }
    void increase_sfl_count() { _sfl_count += 1; }
    int get_sfl_count() const { return _sfl_count; }
    void increase_iq_kick_count() { _iq_kick_count += 1; }
    int get_iq_kick_count() const { return _iq_kick_count; }


    // update records for fairness computation. this function should be called when total #jobs changes
    // time: current event time
    // num_jobs: how many jobs were in cluster right before 'time'
    std::vector<FairRecord> get_fair_records() const { return _fair_records; }
    void update_fair_records(time_type, int, double, int);
    void clear_fair_records() { _fair_records.clear(); }
    bool is_fair_records_empty() { return _fair_records.empty(); }

    time_type calc_ideal_one_step(ActionDuration*) const;
    double calc_fairness_for_alloc(Cluster*, ActionDuration*, bool, int);
    double calc_fairness(Cluster*, ActionDuration*, bool);

    int get_id() const { return _id; }
    time_type get_submit_time() const { return _submit_time; }
    unsigned get_gpu_req() const { return _gpu_req; }
    size_t get_gpu_usg() const { return _ComputeWave.size(); }

    time_type get_queued_time() const { return _queued_time; }
    void add_queued_time(time_type q) { _queued_time += q; }
    time_type get_runned_time() const { return _runned_time; }
    void add_runned_time(time_type q) { _runned_time += q; }
    time_type get_serviced_time() const { return _serviced_time; }
    void add_serviced_time(time_type q) { _serviced_time += q; }

    time_type get_start_time_stamp() const {return _start_time;}

    // void set_gpu_req_queued(int req) { _gpu_req_queued = req; }
    // int get_gpu_req_queued() { return _gpu_req_queued; }

    void set_fair_share(unsigned fair_share) { _fair_share = fair_share; }
    // how about integrate set_fair_share function in to one by getting ScheduleType as param?
    // The ScheduleType is defined in config.h and it uses this file as header.. consider this later..
    void set_2p_fair_share(unsigned fair_share);
    void set_2p_perf_share(unsigned fair_share);
    void set_divisor_fair_share(unsigned fair_share);
    void set_2m_fair_share(unsigned fair_share);
    unsigned get_fair_share() const { return _fair_share; }
    unsigned get_perf_share() const { return _perf_share; }
    unsigned get_share() const { return _fair_share + _perf_share; }

    // void set_start_time(time_type start_time) { _start_time = start_time; } // deprecated
    time_type get_end_time() const { return _end_time; }
    // void set_wave_done_action(int wave_done_actions) { _wave_done_action = wave_done_actions; }
    // int get_wave_done_action() const { return _wave_done_action; }
    void set_iter_done_action(int iteration_done_actions) { _iteration_done_action = iteration_done_actions; }
    int get_iter_done_action() const { return _iteration_done_action; }

    bool is_end() const { return _global_step >= _iteration; }

    void set_global_step(int global_step) { _global_step = global_step; }

    void set_rank_irt(unsigned rank) { _rank_irt = rank; }
    time_type get_irt() const { return _ideal_run_time; }
    double get_fairness() const { return _fairness; }
    double get_fairness2() const { return _fairness2; }
    double get_fairness3() const { return _fairness3; }
    double get_fairness4() const { return _fairness4; }
    double get_fairness5() const { return _fairness5; }

    double get_locality_dist() const { return _avg_loc_dist; }
    double get_locality_bit() const { return _avg_loc_bit; }
    double get_locality_con() const { return _avg_loc_content; }
    unsigned get_cold_count() const { return _cold_start_count; }
    double get_avg_wave() const { return _avg_wave; }
    double get_avg_gpu_usg() const { return _avg_gpu_usg; }
    double get_n_avg() const { return _n_avg; }
    void statistics(bool) const;
};

MicroserviceJob::MicroserviceJob(int id, time_type submit_time, int gpu_req, int batch_size, int iteration,
         ModelType model, DataType dataset)
{
  _submit_time = submit_time;
  _submit_flag = false;
  _gpu_req = gpu_req;
  // _sync_N = kPeriodN;
  _batch_size = batch_size;
  _iteration = iteration;
  _model = model;
  _dataset = dataset;
  _id = id;
  _fair_share = 0; // default in resource-enough cluster
  _perf_share = 0;
  _global_step = 0;
  // _start_time = -1;
  _start_time = LONG_LONG_MAX;
  _expected_end_time = LONG_LONG_MAX;
  _end_time = -1;
  _duration = 0;
  _iteration_done_action = 0;
  _cold_start_count = 0;
  _counter_dist_expand = 0;
  _counter_unlucky_expand = 0;
  _counter_lucky_expand = 0;
  _counter_lucky_shrink = 0;
  _stop = false;
  _evict = false;
  _attained_service = 0;
  _time_stamp_oj = submit_time;
  _online_jct = 0;
  _online_run = 0;
  _kick_count = 0;
  _sfl_count = 0;
  _iq_kick_count = 0;
  _alpha_fairness_value = 0;
  _prev_fair_share = 0;
  _status = MSJStatus::kNone;
}

bool MicroserviceJob::is_loc_sensitive() const {
  if (loc_sd_rule == kAS) {
    return true;
  } if (loc_sd_rule == kNN) {
    return is_loc_sensitive_wrt_NN();
  } else if (loc_sd_rule == kLW) {
    return is_loc_sensitive_wrt_LW();
  } else if (loc_sd_rule == kPhase) {
    return get_loc_sensitivity_wrt_wave() > 0;
  }
  return false;
}

bool MicroserviceJob::is_loc_sensitive_wrt_NN() const {
  bool ret = false;
  if (_model == VGG19_Cifar || _model == VGG19) {
    ret = true;
  }
  return ret;
}
bool MicroserviceJob::is_loc_sensitive_wrt_LW() const {

  // bool ret = true;
  // if ((_model == VGG19 && (_gpu_req == 16 || _gpu_req == 8)) ||
  //     (_model == Inception3 && _gpu_req == 16)) {
  //   ret = false;
  // }


// sync with real exp
  bool ret = false;
  if (_model == VGG19_Cifar) {
    ret = true;
  } else if (_model == VGG19) {
    ret = true;
    if (_gpu_req == 16 || _gpu_req == 8) {
      ret = false;
    }
  }


  return ret;
}

// CHECK: may need to update sens array
// positive: sensitive, negative: inverse sensitive, zero: insensitive
int get_loc_effect(int period_k, ModelType model, unsigned gpu_req, /*int share,*/int phase) {
  int sens[3][9][6] = {
    {
      {0, -2, 0, 0, 0, -2}, // resnet50
      {4, -2, 2, -8, 0, 0}, // vgg i
      {0, 0, 2, -3, -2, 0}, // inception
      {1, 0, 0, 2, 0, 2}, // resnext
      {0, 1, -3, 5, -2, -3}, // vgg c
      {0, -1, -4, 6, 0, -1}, // lstm
      {3, -2, 2, -5, -2, 0}, // gnmt
      {1, 1, 1, 1, 1, 1}, // pcifar
      {2, 2, 2, 2, 2, 2} // pimagenet
    }, // n10
    {
      // {-4, 5, -5, 0, 0, -2}, // resnet50
      // {-4, 9, -3, -8, -13, -2}, // vgg i
      // {-1, 0, -1, -3, -4, 1}, // inception
      // {1, 1, 0, 8, 0, 0}, // resnext
      // {2, -6, 11, -4, -4, -5}, // vgg c
      // {-3, 0, -2, 0, -2, -2}, // lstm
      // {1, 2, 1, 0, 1, -1}, // gnmt
      {2, 5, 2, 0, 3, 0}, // resnet50
      {1, 9, 4, -3, 10, 2}, // vgg i
      {2, 1, 2, 0, 0, 1}, // inception
      {1, 1, 1, 8, 0, 0}, // resnext
      {2, 2, 11, 2, 0, 8}, // vgg c
      {1, 3, 0, 0, 0, 0}, // lstm
      {0, 2, 0, 0, 1, 1}, // gnmt
      {1, 1, 1, 1, 1, 1}, // pcifar
      {2, 2, 2, 2, 2, 2} // pimagenet
    }, // n5
    {
      {0, 10, 16, 4, 10, 5}, // resnet50
      {9, 2, 15, 4, 9, 6}, // vgg i
      {1, 0, 4, 0, 0, 3}, // inception
      {3, 1, 0, 2, 5, 0}, // resnext
      {6, 9, 10, 0, 5, 4}, // vgg c
      {10, 2, 2, 0, -6, -3}, // lstm
      {5, 8, 4, 0, 6, 2}, // gnmt
      {1, 1, 1, 1, 1, 1}, // pcifar
      {2, 2, 2, 2, 2, 2} // pimagenet
    } // n1 
  };

  int i(-1), j(-1), k(-1);
  if (period_k == 10) {
    i = 0;
  } else if (period_k == 5) {
    i = 1;
  } else if (period_k == 1) {
    i = 2;
  }
  if (model == ResNet50) {
    j = 0;
  } else if (model == VGG19) {
    j = 1;
  } else if (model == Inception3) {
    j = 2;
  } else if (model == ResNeXt) {
    j = 3;
  } else if (model == VGG19_Cifar) {
    j = 4;
  } else if (model == LSTM) {
    j = 5;
  } else if (model == GNMT) {
    j = 6;
  } else if (model == PCIFAR) {
    j = 7;
  } else if (model == PIMAGENET) {
    j = 8;
  }
  if (gpu_req == 4) {
    k = 0;
  } else if (gpu_req == 8 && phase == 1) {
    k = 1;
  } else if (gpu_req == 8 && phase == 2) {
    k = 2;
  } else if (gpu_req == 16 && phase == 1) {
    k = 3;
  } else if (gpu_req == 16 && phase == 2) {
    k = 4;
  } else if (gpu_req == 16 && phase <= 4) {
    k = 5;
  }

  if (i < 0 || j < 0 || k < 0) {
    return 0;
  }
  return sens[i][j][k];
}

// CHECK: may need to change is_loc_sensitive to this function in provide_gpu function's argument
int MicroserviceJob::get_loc_sensitivity_wrt_wave() const {
// change placement rule to just consolidating for all.
  return 1;

  unsigned share = _perf_share + _fair_share;
  unsigned waves;
  if (share != 0)
    waves = _gpu_req / share +
            ((_gpu_req % share == 0) ? 0 : 1);
  else
    waves = 1;
  return get_loc_effect(_sync_N, _model, _gpu_req, waves);
}

void MicroserviceJob::calc_remain_service(ActionDuration* adp) {
  int remain_step = _iteration - _global_step;
  if (remain_step < 0) {
    remain_step = 0;
  }
  time_type ideal_iter_time = calc_ideal_one_step(adp);
  time_type remain_service = remain_step * _gpu_req * ideal_iter_time;
  _remain_service =  remain_service;
}

time_type MicroserviceJob::calc_remain_time(ActionDuration* adp, unsigned gpu_req, int distance) {
  // int remain_step = _iteration - _global_step;

  int iter = (_iteration / _sync_N) * _sync_N;
  iter += (_iteration % _sync_N == 0) ? 0 : _sync_N;
  int remain_step = iter - _global_step;
  if (remain_step <= 0) {
    remain_step = 1;
  }

  if (gpu_req == 0){
    gpu_req = _gpu_req;
  }

  // int distance = 1;
  int dis = distance;
  // if (gpu_req >= 4) {
  //   dis = 2;
  // }

  int wave = 1;
  if (_gpu_req > gpu_req){
    wave = static_cast<int>(_gpu_req / gpu_req);
  }

  time_type iter_time = adp->get_dur(_model, _gpu_req, dis, _sync_N,
                                   wave) / _sync_N;
  time_type remain_time = remain_step * iter_time;
  _remain_time = remain_time;
  return remain_time;
}

double MicroserviceJob::calc_alpha_fairness_value(ActionDuration* adp,
                                                  double alpha, int num_gpu,
                                                  int prev_share,
                                                  double round_interval) {
  if (num_gpu == 0) {
    return 0;
  }

  double cold = static_cast<double>(get_cold_time(_model, _gpu_req)) / 1000.0;
  int iter = (_iteration / _sync_N) * _sync_N;
  iter += (_iteration % _sync_N == 0) ? 0 : _sync_N;
  double remain_step = (double)iter - (double)_global_step;
  if (remain_step <= 0) {
    remain_step = 1;
  }

  double ori_train_time =
      static_cast<double>(adp->get_dur(_model, _gpu_req, 1, _sync_N, 1)) /
      static_cast<double>(_sync_N) / 1000.0;

  double train_time =
      static_cast<double>(adp->get_dur(_model, _gpu_req, 1, _sync_N,
                                       static_cast<int>(_gpu_req / num_gpu))) /
      static_cast<double>(_sync_N) / 1000.0;

  double estimated_remain_time = static_cast<double>(remain_step) * train_time;
  double estimated_full_remain_time =
      static_cast<double>(remain_step) * ori_train_time;

  if (get_gpu_usg() < size_t(num_gpu)) {
    return (estimated_full_remain_time + cold) / (estimated_remain_time + cold);
  } else {
    return (estimated_full_remain_time + cold) / (estimated_remain_time);
  }
}

int get_2p_floor(int num) {
  if (num < 0) {
    throw num;
  }
  if (num != 0) {
    int count = 0;
    while (num != 1) {
      num = num >> 1;
      count += 1;
    }
    for (int i = 0; i < count; ++i) {
      num = num << 1;
    }
  }
  return num;
}

void MicroserviceJob::set_2p_fair_share(unsigned fair_share) {
  try {
    fair_share = get_2p_floor(fair_share);
  } catch (int err) {
    oss << "Job" << _id << " cannot have " << err << " share" << std::endl;
    put_log(oss, kCore);
    assert(false);
  }
  if (fair_share > _gpu_req) {
    fair_share = _gpu_req;
  }
  _fair_share = fair_share;
}
void MicroserviceJob::set_2p_perf_share(unsigned perf_share) {
  try {
    perf_share = get_2p_floor(perf_share);
  } catch (int err) {
    oss << "Job" << _id << " cannot have " << err << " share" << std::endl;
    put_log(oss, kCore);
    assert(false);
  }
  if (perf_share > _gpu_req) {
    perf_share = _gpu_req;
  }
  _perf_share = perf_share;
}
void MicroserviceJob::set_divisor_fair_share(unsigned fair_share) {
  int i = 0;
  if (fair_share < 1) {
    oss << "Error for share value" << std::endl;
    put_log(oss,kDebug);
    assert(false);
  }
  if (fair_share > kMinGuarantee) {
    while (true) {
      i += 1;
      if (fair_share >= _gpu_req / i) {
        _fair_share = _gpu_req / i;
        break;
      }
    }
  } else {
    _fair_share = fair_share;
  }
}
void MicroserviceJob::set_2m_fair_share(unsigned fair_share) {
  if (fair_share < 2) {
    _fair_share = 1;
  } else {
    unsigned m = fair_share / 2;
    _fair_share = m * 2;
  }
}

void MicroserviceJob::scheduled(time_type current_event_time) {
  if (_start_time == LONG_LONG_MAX) {
    _start_time = current_event_time;
    _scheduled_time = current_event_time;
    _time_stamp_as = current_event_time;
  }
  // else {
    _restart_time = current_event_time;
  // }
  _status = MSJStatus::kRun;
}

void MicroserviceJob::evicted(time_type current_event_time) {
  // if (_status != kYield) {
  //   if (_duration == 0) {
  //     _duration = current_event_time - _start_time;
  //     // _restart_time = _start_time;
  //   } else
  //     _duration += (current_event_time - _restart_time);
  // }
  if (_status == MSJStatus::kRun) accum_dur_from_sched(current_event_time);
  _ComputeWave.clear();
  _Aggr.clear();
  _prev_locs.clear();
  _status = MSJStatus::kQueue;
}
void MicroserviceJob::accum_dur_from_sched(time_type current_event_time) {
  // if (_status == kRun) {
  //   if (_duration == 0)
  //     _duration = current_event_time - _start_time;
  //   else
      _duration += (current_event_time - _restart_time);
  // }
  // _status = kYield;
}
void MicroserviceJob::sync_iter_action(time_type current_event_time) {
  if (_status == MSJStatus::kRun) {
    _prev_locs.clear();
    accum_dur_from_sched(current_event_time);
    _prev_locs.reserve(_ComputeWave.size());
    for (size_t i = 0; i < _ComputeWave.size(); ++i) {
      _prev_locs[i] = _ComputeWave[i].get_location();
    }
    // for (Action& action : _ComputeWave) {
    //   _prev_locs.push_back(action.get_location());
    // } // TODO: too slow....
  }
  _status = MSJStatus::kSync;
}
void MicroserviceJob::wait_in_iq(time_type current_event_time) {
  // if (_status != kYield) {
  //   if (_duration == 0)
  //     _duration = current_event_time - _start_time;
  //   else
  //     _duration += (current_event_time - _restart_time);
  // }
  if (_status == MSJStatus::kRun) accum_dur_from_sched(current_event_time);
  _ComputeWave.clear();
  _Aggr.clear();
  _prev_locs.clear();
  _status = MSJStatus::kWait;
}

// TODO: aggr action also shrink! distance checking should be changed.
// iterate _Wave and calc max value of distance to Aggregator
int MicroserviceJob::calc_distance() { // deprecated
  int distMax = 0, dist = 0;
  for (std::vector<Action>::iterator comp = _ComputeWave.begin(); comp != _ComputeWave.end(); ++comp) {
    Location comp_loc = (comp)->get_location();
    for (std::vector<Action>::iterator aggr = _Aggr.begin(); aggr != _Aggr.end(); ++aggr) {
      Location aggr_loc = (aggr)->get_location();
      if (comp_loc.switch_idx != aggr_loc.switch_idx) {
        dist = 2; // inter switch
      }
      else if (comp_loc.node_idx != aggr_loc.node_idx) {
        dist = 1; // inter node
      }
      else {
        dist = 0; // intra node
      }
      distMax = (dist > distMax) ? dist : distMax;
    }
  }
  return distMax;
}

int MicroserviceJob::count_uniq_node() const {
  std::vector<Location> locations;
  for (Action action : _ComputeWave) {
    locations.push_back(action.get_location());
  }
  std::vector<int> node_uniq;
  for (auto location : locations) {
    auto it = find(node_uniq.begin(), node_uniq.end(), location.node_idx);
    if (it == node_uniq.end()) {
      node_uniq.push_back(location.node_idx);
    }
  }
  return node_uniq.size();
}

// used implementing release invoker with locality
std::list<std::pair<std::pair<int,int>,int>> bin_counts;
void find_position(std::pair<int,int> node_loc,
              std::list<std::pair<std::pair<int,int>,int>>::iterator &it) {
  for (it = bin_counts.begin(); it != bin_counts.end(); ++it) {
    if (it->first == node_loc) {
      break;
    }
  }
}
bool compare_func(Location A, Location B) {
  std::list<std::pair<std::pair<int,int>,int>>::iterator ita, itb;
  int a, b;
  find_position(std::make_pair(A.switch_idx,A.node_idx),ita);
  find_position(std::make_pair(B.switch_idx,B.node_idx),itb);
  a = ita->second;
  b = itb->second;
  if (a != b) {
    return (a > b);
  } else {
    if (A.node_idx != B.node_idx)
      return A.node_idx < B.node_idx;
    else
      return A.gpu_idx < B.gpu_idx;
  }
}
void sort_locations(std::vector<Location> &locations) {
  bin_counts.clear();
  for (Location loc : locations) {
    std::pair<int,int> key = std::make_pair(loc.switch_idx,loc.node_idx);
    int val = 0;
    std::list<std::pair<std::pair<int,int>,int>>::iterator position;
    find_position(key, position);
    if (position != bin_counts.end()) {
      val = position->second;
      position->second = val + 1;
    } else {
      val = 1;
      bin_counts.push_back(std::make_pair(key,val));
    }
  }
  std::sort(locations.begin(),locations.end(),compare_func);
}
double loc_score(Cluster* cluster_ptr, std::vector<Location> &locs) {
  double loc_score;
  int node_use, node_ideal, node_size;
  // node_use = count_uniq_node();

  std::vector<int> node_uniq;
  for (auto location : locs) {
    auto it = find(node_uniq.begin(), node_uniq.end(), location.node_idx);
    if (it == node_uniq.end()) {
      node_uniq.push_back(location.node_idx);
    }
  }
  node_use = node_uniq.size();

  node_size = cluster_ptr->get_node_size();
  node_ideal = locs.size() / node_size;
  if (locs.size() % node_size != 0) {
    node_ideal += 1;
  }
  loc_score = (double)node_use / node_ideal;
  return loc_score;
}

void MicroserviceJob::update_action(int action_idx, time_type current_event_time) {
  auto& action = _Aggr[action_idx];
  time_type action_end_time = action.get_end_time();
  time_type past_time = current_event_time - action_end_time;
  int iter_progressed = past_time / action.get_exp_next_dur();
  iter_progressed += (past_time % action.get_exp_next_dur() == 0 ? 0 : 1);
  int global_step = action.get_global_step() + iter_progressed * action.get_kPeriodN();
  time_type next_action_end_time = action_end_time + iter_progressed * action.get_exp_next_dur();

  action.set_global_step(global_step);
  action.set_end_time(next_action_end_time);
}

void MicroserviceJob::invoke_iteration(
    time_type current_event_time,
    Cluster* cluster_ptr,
    ActionDuration* adp,
    ContainerStatus start_condition) {
  oss << "[JOB" << _id << "] invoke iteration at " << _global_step << "step\n";
  put_log(oss,kInfo);
  _global_step += _sync_N;
  // if (start_condition != Warm) {
  //   oss << "[Job" << _id << "] (re)scheduled.\n";
  //   put_log(oss,kInfo);
    scheduled(current_event_time);
  // }

  // invocation status
  oss << "[Job" << _id << "] Share: " << _perf_share << " + " << _fair_share
      << ", Wave vector size: " << _ComputeWave.size() << std::endl;
  put_log(oss,kInfo);
  unsigned share = _fair_share + _perf_share;

  if (start_condition != Cold) {
    if (share < _ComputeWave.size()) {
      std::cerr << "[Error] deprecated flow\n";
      assert(false);
      // oss << "[Job" << _id << "] shrink\n";
      // put_log(oss,kInfo);

    } else if (share == _ComputeWave.size()) {
      oss << "[Job" << _id << "] stay\n";
      put_log(oss,kInfo);
      for (unsigned i = 0; i < share; i++) {
        Location prev_loc = _ComputeWave[i].get_location();
        int process = cluster_ptr->get_gpu_process(prev_loc);
        if (process != 0) {
          oss << "Unexpected error...\n";
          oss << "[Debug] Job" << process << " exist in prev loc" <<std::endl;
          put_log(oss,kDebug);
          assert(false);
        }
        cluster_ptr->provide_gpu(_id, prev_loc);
        Action action(compute, start_condition, current_event_time, _id,
                      _global_step, i, share, prev_loc, _sync_N);
        _ComputeWave[i] = action;
      }
      for (unsigned i = 0; i < _gpu_req; i++) {
        Location location = _Aggr[i].get_location();
        Action action(aggregate, Warm, current_event_time, _id, _global_step,
                  i, share, location, _sync_N);
        _Aggr[i] = action;
      }
    } else { // _fair_share > _ComputeWave.size()
      oss << "[Job" << _id << "] expand\n";
      put_log(oss,kInfo);
      start_condition = Cold;
    }
  }
  // case of expand or (re)scheduled
  if (start_condition == Cold) {
    ContainerStatus start_condition = Cold;
    _cold_start_count += 1;
    // force to cold start all actions when cold start action exist. there is another option for expand case, which need to implement later
    _ComputeWave.clear();
    if (_stop) {
      // int i = 0;
      // sort_locations(_reserved_locations);
      // ContainerStatus start_condition;
      bool is_subloc = true;
      for (auto rloc : _reserved_locations) {
        // if (std::find(previous_locations.begin(),previous_locations.end(),rloc) == previous_locations.end()) {
        if (std::find(_prev_locs.begin(),_prev_locs.end(),rloc) == _prev_locs.end()) {
          is_subloc = false;
          break;
        }
      }
      if (is_subloc) {
        start_condition = Warm;
        _cold_start_count -= 1;
      } else {
        // start_condition = Cold;
        increase_sfl_count();
      }
      for (size_t i = 0; i < _reserved_locations.size(); ++i) {
      // for (auto loc : _reserved_locations) {
        cluster_ptr->provide_gpu(_id,_reserved_locations[i]);
        Action action(compute, start_condition, current_event_time, _id,
                      _global_step, i, share, _reserved_locations[i], _sync_N);
        _ComputeWave.push_back(action);
        // i += 1;
      }
    } else {
      std::cerr << "[Error] deprecated flow\n";
      assert(false); 
      // std::vector<Location> locations;
      // // cluster_ptr->provide_gpu(_id, share, locations);
      // locations = cluster_ptr->provide_gpu(_id, share, get_loc_sensitivity_wrt_wave());
      // oss << "[Debug] locations finished\n";
      // put_log(oss,kDebug);
      // for (int i = 0; i < share; i++) {
      //   Location location = locations[i]; // provide GPU with best fit
      //   Action action(compute, start_condition, current_event_time, _id,
      //                 _global_step, i, share, location);
      //   _ComputeWave.push_back(action);
      // }
    }
    _Aggr.clear();
    for (unsigned i = 0; i < _gpu_req; i++) {
      Location location = _ComputeWave[i%share].get_location();
      Action action(aggregate, start_condition, current_event_time, _id,
                    _global_step, i, share, location, _sync_N);
      _Aggr.push_back(action);
    }
  }

  // calculate duration of aggregate actions and use it for next event trigger
  // int distance = count_uniq_node();
  double distance = locality_score(cluster_ptr);
  double prev_loc_score = loc_score(cluster_ptr, _prev_locs);
  if (share > _prev_locs.size() && distance > 1 && _start_time != current_event_time) {
    _counter_dist_expand += 1;
    if (get_loc_sensitivity_wrt_wave() > 0) {
      _counter_unlucky_expand += 1;
    }
  }
  if (distance == 1 && prev_loc_score > 1 && get_loc_sensitivity_wrt_wave() > 0) {
    if (share > _prev_locs.size() && _start_time != current_event_time) {
      _counter_lucky_expand += 1;
    } else if (share < _prev_locs.size()) {
      _counter_lucky_shrink += 1;
    }
  }
  int waves = _gpu_req/share +
              ((_gpu_req%share == 0) ? 0 : 1);
  for (auto it = _Aggr.begin(); it != _Aggr.end(); ++it) {
    (it)->calc_duration(_model, _batch_size, _gpu_req, distance, _sync_N,
                        waves, start_condition, adp);
    oss << "[Job" << _id << "] " << _global_step
        << " steps' aggregate Action " << it-_Aggr.begin()
        << " end time: " << (it)->get_end_time() << std::endl;
    put_log(oss,kInfo);
    // Running_action_list_ptr->push_back(&*it);
    // end_event_list_ptr->push_back((&*it)->get_end_time());
  }

  // int iter_total = _iteration / _sync_N * _sync_N +
  //                  (_iteration % _sync_N == 0 ? 0 : _sync_N);
  // int remain_step = iter_total - _global_step;
  // // int remain_step = _iteration - _global_step;
  // if (remain_step < 0) {
  //   remain_step = 0;
  //   assert(false);
  // }
  // time_type ete = _Aggr[0].get_end_time() +
  //                 (remain_step / _sync_N * _Aggr[0].get_exp_next_dur());
  // if (ete != _expected_end_time) {
  //   for (auto it = job_end_events_ptr->begin(); it != job_end_events_ptr->end();
  //        ++it) {
  //     if (*it == _expected_end_time) {
  //       job_end_events_ptr->erase(it);
  //       break;
  //     }
  //   }
  //   job_end_events_ptr->push_back(ete);
  //   _expected_end_time = ete;
  // }
}

time_type MicroserviceJob::compute_expected_end_time() const {
  int iter_total = _iteration / _sync_N * _sync_N +
                   (_iteration % _sync_N == 0 ? 0 : _sync_N);
  int remain_step = iter_total - _global_step;
  if (remain_step < 0) {
    remain_step = 0;
    assert(false);
  }
  time_type ete = _Aggr[0].get_end_time() +
                  (remain_step / _sync_N * _Aggr[0].get_exp_next_dur());
  return ete;
}

// // integration of try_next_iter, start_fist_iter, restart_iter functions
// void MicroserviceJob::invoke_iteration(
//     time_type current_event_time,
//     Cluster* cluster_ptr,
//     ActionDuration* adp,
//     std::list<Action*>* Running_action_list_ptr,
//     std::list<time_type>* end_event_list_ptr,
//     std::list<time_type>* job_end_events_ptr,
//     ContainerStatus start_condition) {
//   oss << "[JOB" << _id << "] invoke iteration at " << _global_step << "step\n";
//   put_log(oss,kInfo);
//   _global_step += _sync_N;
//   // if (start_condition != Warm) {
//   //   oss << "[Job" << _id << "] (re)scheduled.\n";
//   //   put_log(oss,kInfo);
//     scheduled(current_event_time);
//   // }

//   // invocation status
//   oss << "[Job" << _id << "] Share: " << _perf_share << " + " << _fair_share
//       << ", Wave vector size: " << _ComputeWave.size() << std::endl;
//   put_log(oss,kInfo);
//   int share = _fair_share + _perf_share;

//   // std::vector<Location> previous_locations = _prev_locs;

//   // if (!_ComputeWave.empty()) {
//   //   for (auto elem : _ComputeWave) {
//   //     previous_locations.push_back(elem.get_location());
//   //   }
//   // }
//   if (start_condition != Cold) {
//     if (share < _ComputeWave.size()) {
//       std::cerr << "[Error] deprecated flow\n";
//       assert(false);
//       // oss << "[Job" << _id << "] shrink\n";
//       // put_log(oss,kInfo);
//       // sort_locations(previous_locations);
//       // std::vector<int> idcs;
//       // int idx = 0;
//       // for (int i = 0; i < share; i++) {
//       //   Location prev_loc;
//       //   if (get_loc_sensitivity_wrt_wave() < 0) {
//       //     oss << "[Job" << _id << "] in locality inverse sensitivity" << std::endl;
//       //     put_log(oss,kDebug);
//       //     if (i != 0) {
//       //       while (true) {
//       //         idx += 1;
//       //         if (idx == previous_locations.size()) {
//       //           idx = 0;
//       //           for (auto rit = idcs.rbegin(); rit != idcs.rend(); ++rit) {
//       //             previous_locations.erase(previous_locations.begin() + *rit);
//       //           }
//       //           idcs.clear();
//       //           break;
//       //         }
//       //         if (!(previous_locations[idx].node_idx == previous_locations[idx-1].node_idx)) {
//       //           break;
//       //         }
//       //       }
//       //     }
//       //     prev_loc = previous_locations[idx];
//       //     idcs.push_back(idx);
//       //   } else {
//       //     prev_loc = previous_locations[i];
//       //   }
//       //   oss << "Select " << prev_loc << " for shrink" << std::endl;
//       //   put_log(oss,kDebug);
//       //   int process = cluster_ptr->get_gpu_process(prev_loc);
//       //   if (process != 0) {
//       //     oss << "Unexpected error...\n";
//       //     oss << "[Debug] Job" << process << " exist in prev loc" << prev_loc << std::endl;
//       //     put_log(oss,kDebug);
//       //     assert(false);
//       //   }
//       //   cluster_ptr->provide_gpu(_id, prev_loc);
//       //   Action action(compute, start_condition, current_event_time, _id,
//       //                 _global_step, i, share, prev_loc);
//       //   _ComputeWave[i] = action;
//       // }
//       // // remove un-belonged action from _ComputeWave
//       // int removal = _ComputeWave.size() - share;
//       // for (int i = 0; i < removal; ++i) {
//       //   _ComputeWave.pop_back();
//       // }
//       // // for (std::vector<Action>::iterator it = _ComputeWave.end();
//       // //      it != _ComputeWave.begin() + _fair_share; --it) { ///////////// is this fine?? maybe consider reverse iterator?
//       // //   _ComputeWave.erase(it);
//       // // }

//       // for (int i = 0; i < _gpu_req; i++) {
//       //   Location location = _ComputeWave[i%share].get_location(); // TODO: cold start can occur here!
//       //   Action action(aggregate, Warm, current_event_time, _id, _global_step,
//       //             i, share, location);
//       //   _Aggr[i] = action;
//       // }
//     } else if (share == _ComputeWave.size()) {
//       oss << "[Job" << _id << "] stay\n";
//       put_log(oss,kInfo);
//       for (int i = 0; i < share; i++) {
//         Location prev_loc = _ComputeWave[i].get_location();
//         int process = cluster_ptr->get_gpu_process(prev_loc);
//         if (process != 0) {
//           oss << "Unexpected error...\n";
//           oss << "[Debug] Job" << process << " exist in prev loc" <<std::endl;
//           put_log(oss,kDebug);
//           assert(false);
//         }
//         cluster_ptr->provide_gpu(_id, prev_loc);
//         Action action(compute, start_condition, current_event_time, _id,
//                       _global_step, i, share, prev_loc, _sync_N);
//         _ComputeWave[i] = action;
//       }
//       for (int i = 0; i < _gpu_req; i++) {
//         Location location = _Aggr[i].get_location();
//         Action action(aggregate, Warm, current_event_time, _id, _global_step,
//                   i, share, location, _sync_N);
//         _Aggr[i] = action;
//       }
//     } else { // _fair_share > _ComputeWave.size()
//       oss << "[Job" << _id << "] expand\n";
//       put_log(oss,kInfo);
//       start_condition = Cold;
//     }
//   }
//   // case of expand or (re)scheduled
//   if (start_condition == Cold) {
//     ContainerStatus start_condition = Cold;
//     _cold_start_count += 1;
//     // force to cold start all actions when cold start action exist. there is another option for expand case, which need to implement later
//     _ComputeWave.clear();
//     if (_stop) {
//       // int i = 0;
//       // sort_locations(_reserved_locations);
//       // ContainerStatus start_condition;
//       bool is_subloc = true;
//       for (auto rloc : _reserved_locations) {
//         // if (std::find(previous_locations.begin(),previous_locations.end(),rloc) == previous_locations.end()) {
//         if (std::find(_prev_locs.begin(),_prev_locs.end(),rloc) == _prev_locs.end()) {
//           is_subloc = false;
//           break;
//         }
//       }
//       if (is_subloc) {
//         start_condition = Warm;
//         _cold_start_count -= 1;
//       } else {
//         // start_condition = Cold;
//         increase_sfl_count();
//       }
//       for (int i = 0; i < _reserved_locations.size(); ++i) {
//       // for (auto loc : _reserved_locations) {
//         cluster_ptr->provide_gpu(_id,_reserved_locations[i]);
//         Action action(compute, start_condition, current_event_time, _id,
//                       _global_step, i, share, _reserved_locations[i], _sync_N);
//         _ComputeWave.push_back(action);
//         // i += 1;
//       }
//     } else {
//       std::cerr << "[Error] deprecated flow\n";
//       assert(false); 
//       // std::vector<Location> locations;
//       // // cluster_ptr->provide_gpu(_id, share, locations);
//       // locations = cluster_ptr->provide_gpu(_id, share, get_loc_sensitivity_wrt_wave());
//       // oss << "[Debug] locations finished\n";
//       // put_log(oss,kDebug);
//       // for (int i = 0; i < share; i++) {
//       //   Location location = locations[i]; // provide GPU with best fit
//       //   Action action(compute, start_condition, current_event_time, _id,
//       //                 _global_step, i, share, location);
//       //   _ComputeWave.push_back(action);
//       // }
//     }
//     _Aggr.clear();
//     for (int i = 0; i < _gpu_req; i++) {
//       Location location = _ComputeWave[i%share].get_location();
//       Action action(aggregate, start_condition, current_event_time, _id,
//                     _global_step, i, share, location, _sync_N);
//       _Aggr.push_back(action);
//     }
//   }

//   // calculate duration of aggregate actions and use it for next event trigger
//   // int distance = count_uniq_node();
//   double distance = locality_score(cluster_ptr);
//   double prev_loc_score = loc_score(cluster_ptr, _prev_locs);
//   if (share > _prev_locs.size() && distance > 1 && _start_time != current_event_time) {
//     _counter_dist_expand += 1;
//     if (get_loc_sensitivity_wrt_wave() > 0) {
//       _counter_unlucky_expand += 1;
//     }
//   }
//   if (distance == 1 && prev_loc_score > 1 && get_loc_sensitivity_wrt_wave() > 0) {
//     if (share > _prev_locs.size() && _start_time != current_event_time) {
//       _counter_lucky_expand += 1;
//     } else if (share < _prev_locs.size()) {
//       _counter_lucky_shrink += 1;
//     }
//   }
//   int waves = _gpu_req/share +
//               ((_gpu_req%share == 0) ? 0 : 1);
//   for (auto it = _Aggr.begin(); it != _Aggr.end(); ++it) {
//     (it)->calc_duration(_model, _batch_size, _gpu_req, distance, _sync_N,
//                         waves, start_condition, adp);
//     oss << "[Job" << _id << "] " << _global_step
//         << " steps' aggregate Action " << it-_Aggr.begin()
//         << " end time: " << (it)->get_end_time() << std::endl;
//     put_log(oss,kInfo);
//     // Running_action_list_ptr->push_back(&*it);
//     end_event_list_ptr->push_back((&*it)->get_end_time());
//   }

//   int iter_total = _iteration / _sync_N * _sync_N +
//                    (_iteration % _sync_N == 0 ? 0 : _sync_N);
//   int remain_step = iter_total - _global_step;
//   // int remain_step = _iteration - _global_step;
//   if (remain_step < 0) {
//     remain_step = 0;
//     assert(false);
//   }
//   time_type ete = _Aggr[0].get_end_time() +
//                   (remain_step / _sync_N * _Aggr[0].get_exp_next_dur());
//   if (ete != _expected_end_time) {
//     for (auto it = job_end_events_ptr->begin(); it != job_end_events_ptr->end();
//          ++it) {
//       if (*it == _expected_end_time) {
//         job_end_events_ptr->erase(it);
//         break;
//       }
//     }
//     job_end_events_ptr->push_back(ete);
//     _expected_end_time = ete;
//   }
// }


// The job is not in running. but can be from action queue job
// NOTE: this is called only when job is not running
void MicroserviceJob::invoke_iteration_tms(time_type current_event_time,
                                       Cluster* cluster_ptr,
                                       ActionDuration* adp,
                                       std::list<Action*>* Running_action_list_ptr,
                                       std::list<time_type>* end_event_list_ptr,
                                       std::list<time_type>* job_end_events_ptr,
                                       std::vector<Location>& locs) {
  oss << "[JOB" << _id << "] invoke iteration at " << _global_step << "step\n";
  put_log(oss,kInfo);

  _global_step += _sync_N;

  oss << "[Job" << _id << "] (re)scheduled.\n";
  put_log(oss,kInfo);

  scheduled(current_event_time);

  oss << "[Job" << _id << "] Share: " << _perf_share << " + " << _fair_share
      << ", Wave vector size: " << _ComputeWave.size() << std::endl;
  put_log(oss,kInfo);

  unsigned share = _fair_share + _perf_share;

  ContainerStatus start_condition = Cold;
  _cold_start_count += 1;

  for (auto loc : locs) {
    cluster_ptr->provide_gpu(_id,loc); 
  }

  for (unsigned i = 0; i < share; i++) {
    Location location = locs[i];
    Action action(compute, start_condition, current_event_time, _id,
                  _global_step, i, share, location, _sync_N);
    _ComputeWave.push_back(action);
  }

  for (unsigned i = 0; i < _gpu_req; i++) {
    Location location = _ComputeWave[i%share].get_location();
    Action action(aggregate, start_condition, current_event_time, _id,
                  _global_step, i, share, location, _sync_N);
    _Aggr.push_back(action);
  }

  // calculate duration of aggregate actions and use it for next event trigger
  double distance = locality_score(cluster_ptr);
  int waves = _gpu_req/share +
              ((_gpu_req%share == 0) ? 0 : 1);
  for (auto it = _Aggr.begin(); it != _Aggr.end(); ++it) {
    (it)->calc_duration(_model, _batch_size, _gpu_req, distance, _sync_N,
                        waves, start_condition, adp);
    oss << "[Job" << _id << "] " << _global_step
        << " steps' aggregate Action " << it-_Aggr.begin()
        << " end time: " << (it)->get_end_time() << std::endl;
    put_log(oss,kInfo);
    Running_action_list_ptr->push_back(&*it);
    end_event_list_ptr->push_back((&*it)->get_end_time());
  }

  int iter_total = _iteration / _sync_N * _sync_N +
                   (_iteration % _sync_N == 0 ? 0 : _sync_N);
  int remain_step = iter_total - _global_step;
  // int remain_step = _iteration - _global_step;
  if (remain_step < 0) {
    remain_step = 0;
    assert(false);
  }
  time_type ete = _Aggr[0].get_end_time() +
                  (remain_step / _sync_N * _Aggr[0].get_exp_next_dur());
  if (ete != _expected_end_time) {
    for (auto it = job_end_events_ptr->begin(); it != job_end_events_ptr->end();
         ++it) {
      if (*it == _expected_end_time) {
        job_end_events_ptr->erase(it);
        break;
      }
    }
    job_end_events_ptr->push_back(ete);
    _expected_end_time = ete;
  }
}
void MicroserviceJob::invoke_iteration_tms(time_type current_event_time,
                                       Cluster* cluster_ptr,
                                       ActionDuration* adp,
                                       std::list<Action*>* Running_action_list_ptr,
                                       std::list<time_type>* end_event_list_ptr,
                                       std::list<time_type>* job_end_events_ptr) {
  this->invoke_iteration_tms(current_event_time, cluster_ptr, adp,
                             Running_action_list_ptr, end_event_list_ptr,
                             job_end_events_ptr, _reserved_locations);
}

void MicroserviceJob::end(time_type current_event_time) {
  _end_time = current_event_time;
  // if (_status != kYield) {
  //   if (_duration == 0) {
  //     _duration = _end_time - _start_time;
  //   } else {
  //     _duration += _end_time - _restart_time;
  //   }
  // }
  if (_status == MSJStatus::kRun) accum_dur_from_sched(current_event_time);  
  _ComputeWave.clear();
  _Aggr.clear();
  _prev_locs.clear();
  oss << "[Job" << _id << "] job end in " << _end_time - _submit_time << "(" << _duration << ")" << std::endl;
  put_log(oss,kInfo);
  _status = MSJStatus::kEnd;
}

double MicroserviceJob::locality_score(Cluster* cluster_ptr) const {
  double loc_score;
  int node_use, node_ideal, node_size;
  node_use = count_uniq_node();
  node_size = cluster_ptr->get_node_size();
  node_ideal = _ComputeWave.size() / node_size;
  if (_ComputeWave.size() % node_size != 0) {
    node_ideal += 1;
  }
  loc_score = (double)node_use / node_ideal;
  return loc_score;
}

// NOTE: do this only for running job
// NOTE: periodic recording may miss data between period
void MicroserviceJob::update_locality_records(Cluster* cluster_ptr) {
  double loc_score = locality_score(cluster_ptr);
  _locality_records.push_back(loc_score);
}

bool MicroserviceJob::is_best_placed(Cluster* cluster_ptr) const {
  bool is_ideal = false;
  double loc_score = locality_score(cluster_ptr);
  if (loc_score == 1) {
    is_ideal = true;
  }
  return is_ideal;
}

double MicroserviceJob::mean_locality_score() const {
  double avg_score;
  if (_locality_records.empty()) {
    return 0;
  }
  double sum = 0;
  for (double num : _locality_records) {
    sum += num;
  }
  avg_score = sum / _locality_records.size();
  oss << "[Job" << _id << "] locality score: " << avg_score << std::endl;
  put_log(oss,kDebug);
  return avg_score;
}

std::vector<int> MicroserviceJob::get_node_idx() const {
  std::vector<int> node_idcs;
  for (Action action : _ComputeWave) {
    node_idcs.push_back(action.get_location().node_idx);
  }
  std::sort(node_idcs.begin(),node_idcs.end());
  node_idcs.erase(std::unique(node_idcs.begin(),node_idcs.end()),node_idcs.end());
  return node_idcs;
}

std::vector<Location> MicroserviceJob::get_locations() const {
  std::vector<Location> locations;
  for (Action action : _ComputeWave) {
    locations.push_back(action.get_location());
  }
  return locations;
}

void MicroserviceJob::reserve_move(Cluster* cluster_ptr,
                                   std::vector<Location>& allocs) {
  oss << "[Job" << _id << "] reserve move";
  for (const auto& loc : allocs) {
    oss << " " << loc;
  }
  oss << std::endl;
  put_log(oss,kInfo);
  std::vector<Location> prev_locs;
  for (auto elem : _ComputeWave) {
    prev_locs.push_back(elem.get_location());
  }
  sort_locations(prev_locs);

  std::vector<Location> new_locs = allocs;
  sort_locations(new_locs);

  if (new_locs != prev_locs) {
    for (auto loc : new_locs) {
      cluster_ptr->set_reserve_job(_id, loc);
    }
    _reserved_locations = new_locs;
    _stop = true;
  }
}

void MicroserviceJob::release_reservation(Cluster* cluster_ptr) {
  for (auto loc : _reserved_locations) {
    int rjid = cluster_ptr->get_reserve_job(loc);
    if (rjid != _id && rjid != 0) {
      std::cerr << "[Warning] Job" << rjid << " is found while release_reservation of "
                << "job" << _id << " in " << loc << std::endl;
    } else {
      cluster_ptr->set_reserve_job(0,loc);
    }
  }
  _reserved_locations.clear();
  _stop = false;
}
void MicroserviceJob::release_lease_expired_reservation(Cluster* cluster_ptr, time_type current_event_time) {
  // for (auto loc : _reserved_locations) {
  for (auto it = _reserved_locations.begin(); it != _reserved_locations.end();) {
    Location loc = *it;
    int rjid = cluster_ptr->get_reserve_job(loc);
    if (cluster_ptr->is_lease_expired(loc, current_event_time)
        && (rjid == _id || rjid == 0)) {
      cluster_ptr->set_reserve_job(0,loc);
      it = _reserved_locations.erase(it);
    } else {
      if (rjid != _id && rjid != 0) {
        std::cerr << "[Warning] Job" << rjid << " is found while release_reservation of "
                  << "job" << _id << " in " << loc << std::endl;
      }
      ++it;
    }
  }
  // _reserved_locations.clear();
  if (_reserved_locations.empty()) {
    _stop = false;
  }
}

void MicroserviceJob::update_service_time(time_type current_event_time) {
  _attained_service += _ComputeWave.size() * (current_event_time - _time_stamp_as);
  _online_run += (current_event_time - _time_stamp_as);
  _time_stamp_as = current_event_time;
}

void MicroserviceJob::update_online_jct(time_type current_event_time) {
  _online_jct += (current_event_time - _time_stamp_oj);
  _time_stamp_oj = current_event_time;
}

time_type MicroserviceJob::calc_ideal_one_step(ActionDuration* adp) const {
  time_type train_time;
  int ideal_distance = 1;
  int ideal_wave = 1;
  train_time = adp->get_dur(_model, _gpu_req, ideal_distance, _sync_N,
                                   ideal_wave) / _sync_N;
  if (ignore_framework) {
    train_time = get_duration_wo_framework(_model, _gpu_req);
  }

  return train_time;
}

void MicroserviceJob::update_fair_records(time_type current_event_time,
                                          int num_jobs,
                                          double fair_share,
                                          int gpu_load) {
  FairRecord fair_record = {
      current_event_time, // time_stamp
      num_jobs,           // num_job
      fair_share,         // share
      gpu_load,           // gpu_load
      count_uniq_node(),  // uniq_node
      static_cast<int>(_ComputeWave.size()) // gpu_usg
  };
  if (!_fair_records.empty() && fair_record == _fair_records.back()) {
    _fair_records.back().time_stamp = current_event_time;
  } else {
    _fair_records.push_back(fair_record);
  }
}

double MicroserviceJob::calc_fairness_for_alloc(Cluster* cluster_ptr,
                                      ActionDuration* adp,
                                      bool admission_control,
                                      int gpu_req) {
  double aux(0);
  time_type time_stamp;
  if (admission_control) {
    time_stamp = _admitted_time;
  } else {
    time_stamp = _submit_time;
  }

  for (const FairRecord& record : _fair_records) {
    double ideal_share = record.share; // share
    time_type weight = record.time_stamp - time_stamp;
    aux += ideal_share * weight; // <- ideal share GPU

    time_stamp = record.time_stamp;
  }

  time_type elapsed_time;
  if (admission_control) {
    elapsed_time = time_stamp - _admitted_time; // _scheduled_time;
  } else {
    // elapsed_time = _end_time - _scheduled_time; // no....xx
    elapsed_time = time_stamp - _submit_time;
  }

  time_type ideal_duration_one_step = calc_ideal_one_step(adp);
  // int done_iter = _global_step;
  int iter = (_iteration / _sync_N) * _sync_N;
  iter += (_iteration % _sync_N == 0) ? 0 : _sync_N;
  // int remain_iter = iter - _global_step;
  int cold = 0;
  if (!ignore_init) {
    cold = get_cold_time(_model, _gpu_req);
  }
  time_type remain_time_for_alloc = calc_remain_time(adp, gpu_req, 1);
  aux += (double)_gpu_req * remain_time_for_alloc;

  double ideal_avg_share = 0;
  ideal_avg_share = aux / (elapsed_time + remain_time_for_alloc);

  double fairness = (elapsed_time + remain_time_for_alloc) / (ideal_duration_one_step * _gpu_req / ideal_avg_share * iter + cold);
  return fairness;
}

// compute fairness
// divisor is JCT, divider is time with ideal share
double MicroserviceJob::calc_fairness(Cluster* cluster_ptr,
                                      ActionDuration* adp,
                                      bool admission_control) {
  // std::ostringstream oss;
  // std::cout << "[Debug] calc fairness of job" << _id << "\n";
  // if (_id == 2) {
  //   std::cout << "[Debug] job" << _id << " records: ";
  //   for (auto elem : _fair_records) {
  //     std::cout << "(" << elem.first << "," << elem.second << ") ";
  //   }
  //   std::cout << std::endl;
  // }

  int total_GPUs = cluster_ptr->get_total_GPUs();
  // _ideal_share = (double)total_GPUs / numJobs;

  double aux(0),  // Mm
         aux2(0), // 1/n
         aux3(0), // 1/n-down
         aux4(0), // GPU_load
         aux5(0), // locality_distance
         aux6(0), // locality_bit
         aux7(0), // wave
         aux8(0), // gpu
         aux9(0), // cluster_jobs
         auxa(0); // locality_content
  time_type time_stamp;
  if (admission_control) {
    time_stamp = _admitted_time;
  } else {
    time_stamp = _submit_time;
  }

  for (const FairRecord& record : _fair_records) {
    double ideal_share = record.share; // share
    double ideal_share2 = (double)total_GPUs / record.num_job;
    // double ideal_share3;
    // if (ideal_share2 > _gpu_req) {
    //   ideal_share3 = _gpu_req;
    // } else {
    //   ideal_share3 = ideal_share2;
    // }
    double ideal_share3 = (ideal_share2 > _gpu_req) ? _gpu_req : ideal_share2;
    double gpu_load = record.gpu_load / (double)total_GPUs;
    if (gpu_load < 1) {
      gpu_load = 1;
    }

    int node_size = cluster_ptr->get_node_size();
    int min_placement = record.gpu_usg / node_size;
    if (record.gpu_usg % node_size > 0) {
      min_placement += 1;
    }
    double loc_score;
    int loc_bit;
    int loc_con = 0;
    if (min_placement == 0) {
      // something wrong.
      oss << "[Debug] At time " << record.time_stamp << ", Job" << _id
          << " has 0 min_placement" << std::endl;
      put_log(oss,kDebug);
      loc_score = 0;
      loc_bit = 0;
    } else {
      loc_score = (double)min_placement / record.uniq_node;
      loc_bit = (record.uniq_node == min_placement) ? 1 : 0;
    }

    int wave = 0;
    if (record.gpu_usg != 0) {
      wave = _gpu_req / record.gpu_usg
          + ((_gpu_req % record.gpu_usg == 0) ? 0 : 1);
    }
    if (unsigned(record.gpu_usg) > _gpu_req) {
      assert(false);
    }

    int loc_sen = get_loc_effect(_sync_N, _model, _gpu_req, wave);
    if (loc_sen == 0
        || (loc_sen > 0 && record.uniq_node == min_placement)
        || (loc_sen < 0 && record.uniq_node != min_placement))
      loc_con = 1;

    time_type weight = record.time_stamp - time_stamp;
    if (weight < 0) {
      std::cerr << "[Error] Job" << _id << " bound_end - bound_start = "
          << record.time_stamp << " - " << time_stamp
          << " returns negative value" << std::endl;
      // put_log(oss,kDebug);
      assert(weight >= 0);
    }
    aux += ideal_share * weight; // <- ideal share GPU
    aux2 += ideal_share2 * weight;
    aux3 += ideal_share3 * weight;
    aux4 += gpu_load * weight;
    aux5 += loc_score * weight;
    aux6 += loc_bit * weight;
    aux7 += wave * weight;
    aux8 += record.gpu_usg * weight; // <- real GPU usage
    aux9 += record.num_job * weight;
    auxa += loc_con * weight;

    time_stamp = record.time_stamp;
  }

  if (time_stamp != _end_time) {
    oss << "time record: ";
    for (const FairRecord& record : _fair_records) {
      oss << record.time_stamp << " ";
    }
    oss << std::endl;
    oss << "time_stamp: " << time_stamp << ", end_time_stamp: " << _end_time
        << std::endl;
    put_log(oss,kDebug);
    // assert(time_stamp == _end_time);
    throw std::logic_error("Last time stamp is not match with end time");
  }
  time_type jct;
  if (admission_control) {
    jct = _end_time - _admitted_time; // _scheduled_time;
  } else {
    // jct = _end_time - _scheduled_time; // no....xx
    jct = _end_time - _submit_time;
  }
  double ideal_avg_share = aux / jct;
  // double ideal_avg_share2 = aux2 / jct;
  // double ideal_avg_share3 = aux3 / jct;
  double avg_gpu_load = aux4 / jct;

  _avg_loc_dist = aux5 / _duration;
  _avg_loc_bit = aux6 / _duration;
  _avg_loc_content = auxa / _duration; // shuffle waiting time is not included..

  _avg_wave = aux7 / _duration;
  _avg_gpu_usg = aux8 / _duration;

  _n_avg = aux9 / jct;
  double ideal_avg_share3 = (double)total_GPUs / _n_avg;
  if (ideal_avg_share3 > _gpu_req)
    ideal_avg_share3 = _gpu_req;
  
  oss << "[Debug] weighted average share: " << ideal_avg_share << std::endl;
  put_log(oss,kDebug);

  time_type ideal_duration_one_step = calc_ideal_one_step(adp);

  // _fairness = ideal_duration_one_step * _iteration / ( jct * ideal_avg_share / _gpu_req ); // change divisor, divider
  // std::cout << "Job" << _id << " fairness: (" << jct << " * " << ideal_avg_share << " / " << _gpu_req << " ) / (" << ideal_duration_one_step << " * " << _iteration << ")\n";
  int iter = (_iteration / _sync_N) * _sync_N;
  iter += (_iteration % _sync_N == 0) ? 0 : _sync_N;
  int cold = 0;
  if (!ignore_init) {
    // if (_model == ResNet56) {
    //   cold = 40000;
    // } else if (_model == VGG19_Cifar) {
    //   cold = 40000;
    // } else if (_model == ResNeXt) {
    //   cold = 40000;
    // } else {
    //   cold = 60000;
    // }
    cold = get_cold_time(_model, _gpu_req);
  }
  _ideal_run_time = ideal_duration_one_step * iter + cold;
  _ooit_inc_p_gpu = adp->get_ooit_inc_p_gpu(_model, _gpu_req);

  _fairness = jct / (ideal_duration_one_step * _gpu_req / ideal_avg_share * iter + cold);
  // _fairness2 = jct / (ideal_duration_one_step * _gpu_req / ideal_avg_share2 * iter + cold);
  // _fairness2 = jct / (ideal_duration_one_step * _n_avg * iter + cold);
  _fairness2 = jct / (ideal_duration_one_step * avg_gpu_load * iter + cold);
  // _fairness3 = ( jct * ideal_avg_share3 / _gpu_req ) / (ideal_duration_one_step * iter + cold);
  _fairness3 = jct / (ideal_duration_one_step * _gpu_req / ideal_avg_share3 * iter + cold);
  _fairness4 = jct / (ideal_duration_one_step * iter * avg_gpu_load);
  _fairness5 = aux8 / aux;
  // std::cout << "[Debug] duration one step: " << ideal_duration_one_step << ", jct: " << jct << std::endl;
  return _fairness;
}

// // return finish time fairness
// // Tsh = Tcur - Tstart + iter_left * iter_time(G)
// // Tid = iter_total * iter_serial_time / min(Rc / Navg, job_demand_max) 
// double MicroserviceJob::compute_finish_time_fairness(time_type current_event_time, Cluster* cluster_ptr, ActionDuration* adp, int share) {
//   time_type time_stamp = _submit_time;
//   long long aux = 0;
//   for (auto record : _fair_records) {
//     time_type weight = record.time_stamp - time_stamp;
//     assert(weight >= 0);
//     aux += record.num_job * weight;
//     time_stamp = record.time_stamp;
//   }
//   // if (current_event_time == _submit_time)
//   //   return 0;
//   double Navg = (double)aux / (current_event_time - _submit_time);

//   // int share = _fair_share + _perf_share;
//   if (share == 0) {
//     _rho_inv = 0;
//   } else {
//     // TODO: distance 값이 실제가 아닐 수 있음. share를 받는다면 distance는 어떤 값으로 생각할 것인가?
//     double distance = locality_score(cluster_ptr);
//     int waves = _gpu_req/share +
//                 ((_gpu_req%share == 0) ? 0 : 1);
//     time_type t_share = current_event_time - _submit_time
//                         + (_iteration - _global_step)
//                         * adp->get_dur(_model, _gpu_req, distance,
//                                               kPeriodN, waves)
//                         / kPeriodN;

//   // N_avg version (Themis equation)
//     double t_ideal = _iteration * calc_ideal_one_step(adp) * _gpu_req
//                     / std::min(cluster_ptr->get_total_GPUs() / Navg,
//                                 (double)_gpu_req);

//     // return t_share / t_ideal;
//     // _rho = t_share / t_ideal;
//     _rho_inv = t_ideal / t_share;
//   }
// }
// double MicroserviceJob::compute_finish_time_fairness(time_type current_event_time, Cluster* cluster_ptr, ActionDuration* adp) {
//   return compute_finish_time_fairness(current_event_time, cluster_ptr, adp, _fair_share + _perf_share);
// }


// return finish time fairness
// Tsh = Tcur - Tstart + iter_left * iter_time(G)
// Tid = iter_total * iter_serial_time / min(Rc / Navg, job_demand_max) 
double MicroserviceJob::compute_rho_inv(time_type current_event_time, int njobs, Cluster* cluster_ptr, ActionDuration* adp, int share) {
  double rho_inv;
  time_type time_stamp = _submit_time;
  long long aux = 0;
  for (auto record : _fair_records) {
    time_type weight = record.time_stamp - time_stamp;
    assert(weight >= 0);
    aux += record.num_job * weight;
    time_stamp = record.time_stamp;
  }
  // if (current_event_time == _submit_time)
  //   return 0;
  double Navg = (double)aux / (current_event_time - _submit_time);
  if (current_event_time == _submit_time)
    Navg = njobs;

  // int share = _fair_share + _perf_share;
  if (share == 0) {
    rho_inv = 0;
  } else {
    // TODO: distance 값이 실제가 아닐 수 있음. share를 받는다면 distance는 어떤 값으로 생각할 것인가?
    // double distance = locality_score(cluster_ptr);
    double distance = 1;
    int waves = _gpu_req/share +
                ((_gpu_req%share == 0) ? 0 : 1);
    time_type t_share = current_event_time - _submit_time
                        + (_iteration - _global_step)
                        * adp->get_dur(_model, _gpu_req, distance,
                                              _sync_N, waves)
                        / _sync_N;

  // N_avg version (Themis equation)
    double t_ideal = _iteration * calc_ideal_one_step(adp) * Navg;
    // double t_ideal = _iteration * calc_ideal_one_step(adp) * _gpu_req
    //                 / std::min(cluster_ptr->get_total_GPUs() / Navg,
    //                             (double)_gpu_req);

    // return t_share / t_ideal;
    // _rho = t_share / t_ideal;
    rho_inv = t_ideal / t_share;
  }
  return rho_inv;
}
void MicroserviceJob::compute_and_set_rho_inv(time_type current_event_time, int njobs, Cluster* cluster_ptr, ActionDuration* adp) {
  // _rho_inv = compute_rho_inv(current_event_time, cluster_ptr, adp, _fair_share + _perf_share);
  _rho_inv = compute_rho_inv(current_event_time, njobs, cluster_ptr, adp, _ComputeWave.size());
}

void MicroserviceJob::statistics(bool admission_control) const {
  // std::ostringstream oss;
  time_type submitted_time;
  if (admission_control) {
    submitted_time = _admitted_time;
  } else {
    submitted_time = _submit_time;
  }
  oss << "Job" << _id << ","
      << submitted_time << "," << _start_time
      << "," << _end_time - submitted_time << "," << _duration << ","
      << _fairness << "," << _fairness2 << "," << _fairness3 << ","
      << _fairness4 << "," << _fairness5 << "," << _avg_loc_dist << ","
      << _avg_loc_bit << "," << _cold_start_count << "," << _avg_wave << ","
      << _avg_gpu_usg << "," << _n_avg << "," << _kick_count << ","
      << _sfl_count << "," << _avg_loc_content << "," << _iq_kick_count << ","
      << _counter_dist_expand << "," << _counter_unlucky_expand << ","
      << _counter_lucky_expand << "," << _counter_lucky_shrink << ','
      << ModelNameStr[_model] << ',' << _gpu_req << ',' << _sync_N << ','
      << _rank_irt << ',' << _ideal_run_time << ',' << _ooit_inc_p_gpu
      << std::endl;
  put_log(oss,kQuiet);
}

#endif // __JOB_H__
