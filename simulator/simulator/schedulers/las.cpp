#include "../util.h"
#include "../cluster.h"
#include "../job.h"
#include "util.h"


struct SSTime {
  time_type start_time;
  time_type submit_time;
  SSTime(time_type start, time_type submit) : start_time(start), submit_time(submit) {}
};
struct SSTimeCmp {
  bool operator()(const SSTime& lhs, const SSTime& rhs) const {
    return (lhs.start_time == rhs.start_time)
               ? (lhs.submit_time < rhs.submit_time)
               : (lhs.start_time < rhs.start_time);
  }
};

// // enqueue function for two level queue
// void enqueue_job(std::list<Job*> &q1, std::list<Job*> &q2, Job* j, time_type threshold) {
//   // if ((j->get_gpu_req() * j->get_duration()) > threshold) {
//     // q2.push_back(j);
//   // } else {
//     q1.push_back(j);
//   // }
// }
void enqueue_job(std::multimap<SSTime, Job*, SSTimeCmp>& q1, Job* j) {
  q1.insert({{j->get_start_time_stamp(), j->get_submit_time()}, j});
}
void update_key_start_time(std::multimap<SSTime, Job*, SSTimeCmp>& q1, Job* j) {
  SSTime oldkey = {LONG_LONG_MAX, j->get_submit_time()};
  auto it = q1.lower_bound(oldkey);
  for (; it != q1.upper_bound(oldkey); ++it) {
    if (it->second == j) {
      break;
    }
  }
  SSTime newkey = {j->get_start_time_stamp(), j->get_submit_time()};
  auto nh = q1.extract(it);
  nh.key() = newkey;
  q1.insert(std::move(nh));
  // q1.erase(it);
  // q1.insert({newkey, j});
}
// // dequeue function for two level queue
// void dequeue_job(std::list<Job*> &q1, std::list<Job*> &q2, Job* j) {
//   q1.remove(j);
//   q2.remove(j);
// }
void dequeue_job(std::multimap<SSTime, Job*, SSTimeCmp>& q1, std::multimap<SSTime, Job*, SSTimeCmp>& q2, Job* j) {
  SSTime sst = {j->get_start_time_stamp(), j->get_submit_time()};
  for (auto it = q1.lower_bound(sst); it != q1.upper_bound(sst); ++it) {
    if (it->second == j) {
      q1.erase(it);
      break;
    }
  }
  for (auto it = q2.lower_bound(sst); it != q2.upper_bound(sst); ++it) {
    if (it->second == j) {
      q2.erase(it);
      break;
    }
  }
}
// // rearrange function for two level queue
// void rearrange_queue(std::list<Job*>& q1, std::list<Job*>& q2,
//                      time_type threshold, int promote_knob) {
//   if (promote_knob > 0) {
//     for (auto it = q2.begin(); it != q2.end();) {
//       if ((*it)->get_pending_time() > 0 &&
//           (*it)->get_pending_time() >=
//               (long long)promote_knob * (*it)->get_executed_time()) {
//         (*it)->reset_pending_time();
//         (*it)->reset_executed_time();
//         (*it)->increase_promote_count();
//         oss << "[Debug] Job" << (*it)->get_id() << " is promoted" << std::endl;
//         put_log(oss,kDebug);
//         q1.push_back(*it);
//         it = q2.erase(it);
//       } else {
//         it++;
//       }
//     }
//   }
//   // std::list<Job*>::iterator it;
//   std::vector<std::list<Job*>::iterator> del;
//   for (auto it = q1.begin(); it != q1.end(); ++it) {
//     // if (((*it)->get_gpu_req() * (*it)->get_duration()) > threshold) {
//     if (((*it)->get_gpu_req() * (*it)->get_executed_time()) > threshold) {
//       q2.push_back(*it);
//       oss << "[Debug] Job" << (*it)->get_id() << " is demoted" << std::endl;
//       put_log(oss,kDebug);
//       del.push_back(it);
//     }
//   }
//   for (auto elem : del) {
//     q1.erase(elem);
//   }
//   q1.sort(ascend_start_t_ascend_submit_t<Job>);
//   q2.sort(ascend_start_t_ascend_submit_t<Job>);
// }
void rearrange_queue(std::multimap<SSTime, Job*, SSTimeCmp>& q1,
                     std::multimap<SSTime, Job*, SSTimeCmp>& q2,
                     time_type threshold,
                     int promote_knob) {
  if (promote_knob > 0) {
    for (auto it = q2.begin(); it != q2.end();) {
      auto jptr = it->second;
      if (jptr->get_pending_time() > 0 &&
          jptr->get_pending_time() >=
              jptr->get_executed_time() * promote_knob) {
        jptr->reset_pending_time();
        jptr->reset_executed_time();
        jptr->increase_promote_count();
        oss << "[Debug] Job" << jptr->get_id() << " is promoted" << std::endl;
        put_log(oss,kDebug);
        q1.insert(*it);
        it = q2.erase(it);
      } else {
        it++;
      }
    }
  }

  for (auto it = q1.begin(); it != q1.end();) {
    auto jptr = it->second;
    if (jptr->get_gpu_req() * jptr->get_executed_time() > threshold) {
      oss << "[Debug] Job" << jptr->get_id() << " is demoted" << std::endl;
      put_log(oss,kDebug);
      q2.insert(*it);
      it = q1.erase(it);
    } else {
      it++;
    }
  }
}

/******************************************************************************
Tiresias-L multi queue with discretized priority

******************************************************************************/
time_type multi_level_queue_las_scheduling(
    Cluster* cluster_ptr,
    // std::list<time_type>& start_event_list,
    std::vector<std::pair<time_type, int>>& submit_events,
    // std::list<time_type>& end_event_list_stale,

    // std::list<Job>& Job_list,
    std::unordered_map<int, Job>& jobs,

    // std::list<Job*>& Running_Job_list,
    // std::list<Job*>& Queueing_Job_list,
    // ScheduleType schedule_type,

    bool admission_control,
    double &avg_gpu_load,
    double &avg_frag_w_q,
    time_type queue_threshold,
    int promote_knob) {
  oss << "Scheduling: LAS(multi level queue)\n";
  put_log(oss,kCore);

  // std::multiset<time_type> end_event_list;
  std::multimap<time_type, int> end_events;
  std::map<int, Job*> Running_Job_list, Queueing_Job_list;

  std::list<Job*> admission_queue; // rejected jobs in admission control
  const int reject_threshold = cluster_ptr->get_total_GPUs()
                                   * kAdmissionThreshold;
 
  // std::vector<Job*> Jobs_in_cluster;
  std::map<int, Job*> Jobs_in_cluster;
  bool change_job_in_cluster;

  // If attained service is less than queue threshold put to Queue1, if not put to Queue2.
  // std::list<Job*> Queue1, Queue2;
  std::multimap<SSTime, Job*, SSTimeCmp> Queue1, Queue2;

  std::list<Job*> Wait_list;
  std::list<Job*> Yield_list;
  std::list<Job*> Ready_list;

  avg_gpu_load = 0;
  avg_frag_w_q = 0;
  time_type acc_frag_w_q = 0;
  time_type q_time = 0;

  time_type current_event_time = 0;
  int counter = 0;
  while (submit_events.size() + end_events.size()
          + Queueing_Job_list.size() > 0) {
    change_job_in_cluster = false;

    // ___ 0 ___ goto upcomming event
    time_type prev_event_time = current_event_time;
    current_event_time =
        upcomming_event_time(submit_events, end_events, current_event_time);

    // loop status
    oss << "\n======== " << ++counter << "th loop ========\n";
    oss << "[Time] " << current_event_time << " ms" << std::endl;
    put_log(oss,kCore);
    oss << "-------- list status --------\n"
        << "Start event:\t" << submit_events.size() << std::endl
        << "End event:\t" << end_events.size() << std::endl
        << "Rejected job:\t" << admission_queue.size() << std::endl
        << "Running job:\t" << Running_Job_list.size() << std::endl
        << "Queueing job:\t" << Queueing_Job_list.size() << std::endl
        << "Queue1 elem:\t" << Queue1.size() << std::endl
        << "Queue2 elem:\t" << Queue2.size() << std::endl
        << "-----------------------------\n";
    put_log(oss,kCore);

    int q1req = 0;
    int q2r = 0;
    int q2usg = 0;
    for (auto [sst, j] : Queue1) {
      q1req += j->get_gpu_req();
    }
    for (auto [sst, j] : Queue2) {
      if (j->get_status() == kRun) {
        q2r += 1;
        q2usg += j->get_gpu_req();
      }
    }
    oss << "Queue1 req:\t" << q1req << std::endl
        << "Queue2 run:\t" << q2r << std::endl
        << "Queue2 use:\t" << q2usg << std::endl;
    put_log(oss,kCore);

    int yjc = 0;
    for (auto [id, jpt] : Running_Job_list) {
      if (jpt->get_status() == kYield) {
        yjc += 1;
      }
    }
    int wjc = 0;
    for (auto [id, jpt] : Queueing_Job_list) {
      if (jpt->get_status() == kWait) {
        wjc += 1;
      }
    }
    oss << "Yielding job:\t" << yjc << std::endl
        << "Waiting job:\t" << wjc << std::endl;
    put_log(oss,kInfo);
    oss << "Running job list: ";
    for (auto [id, jpt] : Running_Job_list) {
      oss << jpt->get_id() << ' ';
    }
    oss << std::endl;
    put_log(oss,kDebug);
    oss << "Queueing job list: ";
    for (auto [id, jpt] : Queueing_Job_list) {
      oss << jpt->get_id() << ' ';
    }
    oss << std::endl;
    put_log(oss,kDebug);

    // record info of time and gpu share for fairness compute
    int idx_gpu_reqs = 0;
    std::list<std::pair<int,int> > gpu_reqs;
    int gpu_load = 0; // sum of requested GPUs of submitted jobs
    int gpu_util = 0; // how many GPUs are occupied
    for (auto [id, job] : Jobs_in_cluster) {
      gpu_reqs.push_back(std::make_pair(idx_gpu_reqs,job->get_gpu_req()));
      idx_gpu_reqs += 1;
      gpu_load += job->get_gpu_req();
    }
    for (auto [id, jptr] : Running_Job_list) {
      // if (jptr->get_status() == kRun)
      gpu_util += jptr->get_gpu_req();
    }
    oss << "GPU load: " << gpu_load << std::endl;
    oss << "GPU usage: " << gpu_util << std::endl;
    oss << "Job load: " << Jobs_in_cluster.size() << std::endl;
    put_log(oss,kCore);
    avg_gpu_load += gpu_load * (current_event_time - prev_event_time);
    if (!Queueing_Job_list.empty()) {
      acc_frag_w_q += (cluster_ptr->get_total_GPUs() - gpu_util) * (current_event_time - prev_event_time);
      q_time += current_event_time - prev_event_time;
    }
    std::vector<double> shares = max_min_share(gpu_reqs,cluster_ptr);
    idx_gpu_reqs = 0;
    for (auto [id, job] : Jobs_in_cluster) {
      // if (job->get_status() == kOut) {
      //   job->update_fair_records(current_event_time, Jobs_in_cluster.size(), shares[idx_gpu_reqs], gpu_load);
      // } else {
      job->update_fair_records(current_event_time, Jobs_in_cluster.size(),
                               shares[idx_gpu_reqs], gpu_load);
      // }
      idx_gpu_reqs += 1;
// To maintain run & pending time
      // job->update_status(current_event_time);
      job->update_time_for_promote(current_event_time - prev_event_time);
    }
    // Jobs_in_cluster.clear();

    // check yield completed
    std::vector<Job*> yield_done_list;
    for (Job* j : Yield_list) {
      if (j->get_yield_time() == current_event_time) {
        for (auto it = end_events.lower_bound(j->get_yield_time());
             it != end_events.upper_bound(j->get_yield_time()); ++it) {
          if (it->second == j->get_id()) {
            end_events.erase(it);
            break;
          }
        }
        // release resource of job j
        for (Location location : j->locations) {
          cluster_ptr->release_gpu(location);
        }
        j->evicted(current_event_time);
/// CHECK: this may not incur new schedule decision
        change_job_in_cluster = true;
        yield_done_list.push_back(j);
        oss << "[Debug] Job" << j->get_id() << " complete yield" << std::endl;
        put_log(oss,kDebug);
      }
    }
    for (Job* j : yield_done_list) {
      Yield_list.remove(j);
      // Running_Job_list.remove(j);
      // Queueing_Job_list.push_back(j);
      Running_Job_list.erase(j->get_id());
      Queueing_Job_list.insert({j->get_id(), j});
    }

    // check ready list. if done, start to run training.
    std::vector<Job*> ready_done_list;
    for (Job* j : Ready_list) {
      if (j->get_ready_time() == current_event_time) {
        j->run(current_event_time); // job status changed in this call
        // end_event_list.insert(j->get_end_time());
        for (auto it = end_events.lower_bound(j->get_ready_time());
             it != end_events.upper_bound(j->get_ready_time()); ++it) {
          if (it->second == j->get_id()) {
            end_events.erase(it);
            break;
          }
        }
        end_events.insert({j->get_end_time(), j->get_id()});
        ready_done_list.push_back(j);
        oss << "[Debug] Job" << j->get_id()
            << " complete initialization of training" << std::endl;
        put_log(oss,kDebug);
      }
    }
    for (Job* j : ready_done_list) {
      Ready_list.remove(j);
    }

    // ___ 1 ___ handle job ending event
    // std::vector<std::list<Job*>::iterator> expired_jobs;
    for (auto it = end_events.lower_bound(current_event_time);
         it != end_events.upper_bound(current_event_time);) {
      auto jid = it->second;
      auto jptr = &jobs.at(jid);
      if (jptr->get_status() == kRun &&
          jptr->get_end_time() == current_event_time) {
        for (Location loc : jptr->locations) cluster_ptr->release_gpu(loc);
        jptr->end(current_event_time);
        jptr->calc_fairness(cluster_ptr, admission_control);
        jptr->clear_fair_records();
        dequeue_job(Queue1, Queue2, jptr);
        Running_Job_list.erase(jid);
        Jobs_in_cluster.erase(jid);
        change_job_in_cluster = true;
        oss << "[Scheduling] End Job " << jid << std::endl;
        put_log(oss, kInfo);
        it = end_events.erase(it);
      } else {
        ++it;
      }
    }

    // for (std::list<Job*>::iterator it_job_ptr = Running_Job_list.begin();
    //      it_job_ptr != Running_Job_list.end(); ++it_job_ptr)
    // {
    //   if ((*it_job_ptr)->get_status() == kRun &&
    //       (*it_job_ptr)->get_end_time() == current_event_time) {
    //     for (Location location : (*it_job_ptr)->locations) {
    //       cluster_ptr->release_gpu(location);
    //     }
    //     (*it_job_ptr)->end(current_event_time);
    //     (*it_job_ptr)->calc_fairness(cluster_ptr, admission_control);
    //     (*it_job_ptr)->clear_fair_records();
    //     dequeue_job(Queue1, Queue2, *it_job_ptr);
    //     oss << "[Scheduling] End Job " << (*it_job_ptr)->get_id() << std::endl;
    //     put_log(oss,kInfo);
    //     expired_jobs.push_back(it_job_ptr);

    //     auto it = std::find(Jobs_in_cluster.begin(),Jobs_in_cluster.end(),*it_job_ptr);
    //     Jobs_in_cluster.erase(it);
    //   }
    //   // progress all running jobs (= update remain time)
    //   (*it_job_ptr)->update_status(current_event_time);
    // }
    // if (!expired_jobs.empty()) {
    //   change_job_in_cluster = true; // end job flag
    //   for (const auto& elem : expired_jobs) {
    //     Running_Job_list.erase(elem);
    //   }
    // }

    // execute job in wait list
    std::vector<Job*> wait_done_list;
    for (Job* j : Wait_list) {
      if (cluster_ptr->get_idle_GPUs() >= j->get_gpu_req()) {
        // // TODO: locality consolidation need to change GPU alloc scheme.
        // for (int i = 0; i < j->get_gpu_req(); ++i) {
        //   Location location = cluster_ptr->provide_gpu(j->get_id());
        //   j->locations.push_back(location);
        // }
// MD TEST BEGIN
/*
        std::vector<Location> locations;
        cluster_ptr->provide_gpu(j->get_id(),j->get_gpu_req(),locations);
        for (Location loc : locations) {
          j->locations.push_back(loc);
        }
*/
        // TEST validity of new consolidating allocation
        j->locations = cluster_ptr->provide_gpu(j->get_id(), j->get_gpu_req(),true);
// MD TEST END --> they were equal

        //and then record time using func scheduled
        j->scheduled(current_event_time); // j status is changed here
        if (j->get_start_time_stamp() == current_event_time)
          update_key_start_time(Queue1, j);
        // end_event_list.insert(j->get_ready_time());
        end_events.insert({j->get_ready_time(), j->get_id()});
        Ready_list.push_back(j);
        wait_done_list.push_back(j);
        oss << "[Debug] Job" << j->get_id() << " complete wait" << std::endl;
        put_log(oss,kDebug);
      } else {
        break; // no back-filling. possible option: selective backfilling with priority based on wait time
      }
    }
    for (Job* j : wait_done_list) {
      // remove from wait list and queue(?)
      Wait_list.remove(j); // is this safe??
      // Queueing_Job_list.remove(j);
      // Running_Job_list.push_back(j);
      Queueing_Job_list.erase(j->get_id());
      Running_Job_list.insert({j->get_id(), j});
    }


    // if it is elastic scheduling, allocation needs to be changed considering shrink and expand
    // make new list to start this turn. if job pre-exist, place same. check yield, new alloc
    // ___ 2 ___ Check availability before start job newly
    std::vector<Job*> job_to_start; // job to start in this event round
    // std::vector<Job*> job_in_ready;
    // std::vector<Job*> job_to_enqueue;
    assert(job_to_start.empty());
    // assert(job_in_ready.empty());
    // assert(job_to_enqueue.empty());
    // int total_GPU_req = 0;
    // int new_GPU_req = 0;

    // add new job to admission queue
    oss << "[Debug] New job:";
    while (!submit_events.empty()) {
      if (submit_events.back().first == current_event_time) {
        auto jid = submit_events.back().second;
        submit_events.pop_back();
        auto jptr = &jobs.at(jid);
        oss << ' ' << jid;
        if (admission_control == true) {
          admission_queue.push_back(jptr);
          jptr->set_status(kSubmit);
        } else {
          // Queueing_Job_list.push_back(jptr);
          Queueing_Job_list.insert({jid, jptr});
          // enqueue_job(Queue1, Queue2, jptr, queue_threshold);
          enqueue_job(Queue1, jptr);
          jptr->set_status(kQueue);
          change_job_in_cluster = true;
          Jobs_in_cluster.insert({jid, jptr});
        }
      } else {
        break;
      }
    }
    // for (Job &job : Job_list) {
    //   if (job.get_submit_time() == current_event_time &&
    //       job.get_status() == kNone) { //job.get_submit_flag() == false) {
    //     oss << ' ' << job.get_id();
    //     if (admission_control == true) {
    //       admission_queue.push_back(&job);
    //       job.set_status(kSubmit);
    //     } else {
    //       Queueing_Job_list.push_back(&job);
    //       Jobs_in_cluster.push_back(&job);
    //       enqueue_job(Queue1,Queue2,&job,queue_threshold);
    //       job.set_status(kQueue);
    //       change_job_in_cluster = true;
    //     }
    //   }
    // }
    oss << std::endl;
    put_log(oss,kDebug);
    // check cluster load and if possible accept more jobs
    // add running job and queueing job gpu req and compare it with capacity threshold.
    if (admission_control && !admission_queue.empty()) {
      int GPU_reqs = 0;
      for (auto [id, elem] : Running_Job_list) {
        GPU_reqs += elem->get_gpu_req();
      }
      for (auto [id, elem] : Queueing_Job_list) {
        GPU_reqs += elem->get_gpu_req();
      }
      while (!admission_queue.empty()) {
        auto jptr = admission_queue.front();
        if (GPU_reqs + admission_queue.front()->get_gpu_req()
                <= reject_threshold) {
          change_job_in_cluster = true;
          Jobs_in_cluster.insert({jptr->get_id(), jptr});
          // Queueing_Job_list.push_back(admission_queue.front());
          Queueing_Job_list.insert({jptr->get_id(), jptr});
          // enqueue_job(Queue1, Queue2, jptr, queue_threshold);
          enqueue_job(Queue1, jptr);
          GPU_reqs += admission_queue.front()->get_gpu_req();
          admission_queue.front()->set_admitted_time(current_event_time);
          admission_queue.front()->set_status(kQueue);
          admission_queue.pop_front();
        } else {
          break;
        }
      }
    }
// only active when flag is active
if (change_job_in_cluster)
{
   /// CHECK: maybe it is ok to omit update_status for running jobs
    rearrange_queue(Queue1, Queue2, queue_threshold, promote_knob);
    int gpu_req_aux = 0;
    for (Job* j : Wait_list) {
      gpu_req_aux += j->get_gpu_req();
    } // jobs in yield status are not counted in gpu consumption. instead wait list consume the space yielding job had
    for (Job* j : Ready_list) {
      gpu_req_aux += j->get_gpu_req();
    }
    // possible states in running : kReady, kRun, kYield
    // possible states in queueing : kQueue, kWait
    for (auto [sst, j] : Queue1) {
      Status j_status = j->get_status();
      if (j_status == kQueue || j_status == kRun) {
        if (j->get_gpu_req() + gpu_req_aux <= cluster_ptr->get_total_GPUs()) {
          gpu_req_aux += j->get_gpu_req();
          job_to_start.push_back(j);
        // } else {
          // job_to_enqueue.push_back(j);
        }
      }
    }
    for (auto [sst, j] : Queue2) {
      Status j_status = j->get_status();
      if (j_status == kQueue || j_status == kRun) {
        if (j->get_gpu_req() + gpu_req_aux <= cluster_ptr->get_total_GPUs()) {
          gpu_req_aux += j->get_gpu_req();
          job_to_start.push_back(j);
        // } else {
          // job_to_enqueue.push_back(j);
        }
      }
    }

    // if running job is not in job_to_start evict them
    // then, if job_to_start is not in running job, newly alloc them. this must be fit in GPU
    // WHILE, if you want to consider locality consolidation, divide job_to_start into two groups
    // then, allocate locality-aware jobs first, if there is no more local placement available, yield locality-non-aware jobs if needed
    // but, anyway, this case is for Tiresias-L. following Tiresias-L method be enough.
    // ignore locality consolidation for now. but no migration as possible
    // TODO: implement contents in two for loop


    // if job_ptr is not in job_to_start
    // evict job and release resource
    for (auto [jid, jptr] : Running_Job_list) {
      /// CHECK: jot_to_start can be considered to be other stl container
      if (jptr->get_status() == kRun && !is_elem(jptr, job_to_start)) {
        auto range = end_events.equal_range(jptr->get_end_time());
        for (auto it = range.first; it != range.second; ++it) {
          if (it->first == jptr->get_end_time()) {
            if (it->second == jid) {
              end_events.erase(it);
              break;
            }
          } else {
            /// TODO: throw error
            break;
          }
        }
        jptr->yield(current_event_time);
        end_events.insert({jptr->get_yield_time(), jid});
        Yield_list.push_back(jptr);
        oss << "[Debug] Job" << jid << " is yielding in "
            << jptr->get_yield_time() << "ms" << std::endl;
        put_log(oss, kDebug);
      }
    }

    // std::vector<std::list<Job*>::iterator> evicted_jobs;
    // for (std::list<Job*>::iterator it_job_ptr = Running_Job_list.begin();
    //      it_job_ptr != Running_Job_list.end(); ++it_job_ptr) {
    //   if ((*it_job_ptr)->get_status() == kRun &&
    //       !is_elem(*it_job_ptr, job_to_start)) {
    //     // remove the preempting job's endtime from end event list
    //     // for (std::list<time_type>::iterator it = end_event_list.begin();
    //     //      it != end_event_list.end(); ++it) {
    //     //   if (*it == (*it_job_ptr)->get_end_time()) {
    //     //     end_event_list.erase(it);
    //     //     break;
    //     //   }
    //     // }
    //     end_event_list.erase(end_event_list.find((*it_job_ptr)->get_end_time()));
    //     evicted_jobs.push_back(it_job_ptr);
    //   }
    // }
    // for (std::list<Job*>::iterator elem : evicted_jobs) {
    //   (*elem)->yield(current_event_time);
    //   end_event_list.insert((*elem)->get_yield_time());
    //   // Running_Job_list.erase(elem); // not this! this done in next event
    //   Yield_list.push_back(*elem); // is this necessary?
    //   oss << "[Debug] Job" << (*elem)->get_id() << " is yielding in "
    //       << (*elem)->get_yield_time() << "ms" << std::endl;
    //   put_log(oss,kDebug);
    // }

    // if job_ptr in job_to_start is not in Running_Job_list
    // schedule the job_ptr: edit list and allocate resource
    std::vector<Job*> incoming_jobs;
    for (Job* jptr : Wait_list) {
      incoming_jobs.push_back(jptr);
    }
    for (Job* job_ptr : job_to_start) {
      // if (!is_elem(job_ptr, Running_Job_list)) {
      if (Running_Job_list.count(job_ptr->get_id()) == 0) {
        incoming_jobs.push_back(job_ptr);
      }
    }
    // kWait, kQueue exist here
    for (Job* elem : incoming_jobs) {
      if (cluster_ptr->get_idle_GPUs() < elem->get_gpu_req()) {
        // set job status to wait and when there is avail resource, schedule this. (manage in wait_list)
        // if (!is_elem(elem,Wait_list)) {
        if (elem->get_status() != kWait) {
          elem->set_status(kWait);
          Wait_list.push_back(elem);
        }
        oss << "[Debug] Job" << elem->get_id() << " waits for release resource"
            << std::endl;
        put_log(oss,kDebug);
      } else {
        elem->locations = cluster_ptr->provide_gpu(elem->get_id(),elem->get_gpu_req(),true);
        elem->scheduled(current_event_time); // job status is changed here
        if (elem->get_start_time_stamp() == current_event_time)
          update_key_start_time(Queue1, elem);
        Wait_list.remove(elem);
        // end_event_list.push_back(elem->get_ready_time());
        end_events.insert({elem->get_ready_time(), elem->get_id()});
        Ready_list.push_back(elem);
        // Running_Job_list.push_back(elem);
        Running_Job_list.insert({elem->get_id(), elem});
        // Queueing_Job_list.remove(elem); // This was omitted!
        Queueing_Job_list.erase(elem->get_id());
      }
    }

    // Queueing_Job_list.sort(submit_time_compare<Job>);
    // Queueing_Job_list.sort([](const auto& a, const auto& b) {
    //                          return a->get_submit_time() < b->get_submit_time();
    //                        });
}
    // // update data for fairness metric
    // if (true) {
    //   // for (auto job : Jobs_in_cluster) {
    //   //   if (job->get_status() == kOut) {
    //   //     job->update_fair_records(current_event_time, 0);
    //   //   } else {
    //   //     job->update_fair_records(current_event_time, job->get_gpu_req());
    //   //   }
    //   // }
    //   // Jobs_in_cluster.clear();

    //   // Jobs_in_cluster = Running_Job_list + Queueing_Job_list;
    //   // for (auto elem : Running_Job_list) {
    //   //   Jobs_in_cluster.push_back(elem);
    //   // }
    //   // for (auto elem : Queueing_Job_list) {
    //   //   Jobs_in_cluster.push_back(elem);
    //   // }
    // }

    // Check end event list
    // end_event_list.sort();
    // end_event_list.unique();

    // std::cout << "[Schedule] Next end event: ";
    // for (const auto& elem : end_event_list)
    //   {std::cout << elem << " ";}
    // std::cout << std::endl;
    oss << "[Schedule] Cluster: " << cluster_ptr->get_idle_GPUs()
              << " idle\n";
    put_log(oss,kInfo);
    cluster_ptr->cluster_status();
  } // end while loop
  avg_gpu_load /= current_event_time;
  if (acc_frag_w_q != 0) avg_frag_w_q = (double)acc_frag_w_q / q_time;
  oss << "[Info] avg gpu load: " << avg_gpu_load << std::endl;
  put_log(oss,kInfo);
  return current_event_time;
}