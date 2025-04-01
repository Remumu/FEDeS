#include "../util.h"
#include "../cluster.h"
#include "../job.h"
#include "util.h"

/******************************************************************************
* finish time fairness scheduling:
* No preemption, may be backfilling
* variable round length knob (default : 10 min)

added first starting overhead ( == load overhead)
******************************************************************************/
time_type round_based_priority_scheduling(Cluster* cluster_ptr,
                          // std::list<time_type>& start_event_list,
                          std::vector<std::pair<time_type, int> >& submit_events,
                          // std::list<time_type>& end_event_list,
                          // std::list<Job>& Job_list,
                          std::unordered_map<int, Job>& jobs,
                          // std::list<Job*>& Running_Job_list,
                          // std::list<Job*>& Queueing_Job_list,
                          ScheduleType schedule_type,
                          bool admission_control,
                          double &avg_gpu_load,
                          double &avg_frag_w_q,
                          time_type round_length) {
  if (schedule_type == THEMIS) {
    oss << "Scheduling: Themis\n";
    put_log(oss,kCore);
  } else if (schedule_type == RLAS) {
    oss << "Scheduling: Round LAS\n";
    put_log(oss,kCore);
  }

  std::multimap<time_type, int> end_events;
  std::map<int, Job*> Running_Job_list, Queueing_Job_list;

  std::list<Job*> admission_queue; // rejected jobs in admission control
  const int reject_threshold = cluster_ptr->get_total_GPUs() * kAdmissionThreshold;

  // std::vector<Job*> Jobs_in_cluster;
  std::map<int, Job*> Jobs_in_cluster;
  bool change_job_in_cluster;

  std::list<Job*> Wait_list;
  std::list<Job*> Yield_list;
  std::list<Job*> Ready_list;

  avg_gpu_load = 0;
  avg_frag_w_q = 0;
  time_type acc_frag_w_q = 0;
  time_type q_time = 0;

  time_type current_event_time = 0;
  int counter = 0;
  time_type t_round_event = -1;
  while (submit_events.size() + end_events.size()
          + Queueing_Job_list.size() > 0) {
    change_job_in_cluster = false;

    // ___ 0 ___ goto upcomming event
    time_type prev_event_time = current_event_time;
    // current_event_time = upcomming_event_time(start_event_list, end_event_list, current_event_time);
    current_event_time =
        upcomming_event_time(submit_events, end_events, current_event_time);

    bool round_event = false;
    if (current_event_time == t_round_event) {
      for (auto it = end_events.lower_bound(current_event_time);
           it != end_events.upper_bound(current_event_time); ++it) {
        if (it->second == 0) {
          end_events.erase(it);
          break;
        }
      }
      round_event = true;
    }
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
        << "-----------------------------\n";
    put_log(oss,kCore);

    oss << "Running job list: ";
    for (auto [id, jpt] : Running_Job_list) {
      oss << jpt->get_id() << ' ';
    }
    oss << std::endl;
    put_log(oss,kDebug);

    // record info of time and gpu share for fairness compute
    int idx_gpu_reqs = 0;
    std::list<std::pair<int,int> > gpu_reqs;
    int gpu_load = 0;
    int gpu_util = 0;
    for (auto [id, job] : Jobs_in_cluster) {
      gpu_reqs.push_back(std::make_pair(idx_gpu_reqs,job->get_gpu_req()));
      idx_gpu_reqs += 1;
      gpu_load += job->get_gpu_req();
    }
    for (auto [id, jptr] : Running_Job_list) {
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
    }
    // Jobs_in_cluster.clear();

    // check yield completion
    for (auto itj = Yield_list.begin(); itj != Yield_list.end(); ++itj) {
      if ((*itj)->get_yield_time() == current_event_time) {
        for (auto it = end_events.lower_bound((*itj)->get_yield_time());
             it != end_events.upper_bound((*itj)->get_yield_time()); ++it) {
          if (it->second == (*itj)->get_id()) {
            end_events.erase(it);
            break;
          }
        }
        for (Location loc : (*itj)->locations) {
          cluster_ptr->release_gpu(loc);
        }
        (*itj)->evicted(current_event_time);
        // Running_Job_list.remove(*itj);
        Running_Job_list.erase((*itj)->get_id());
        // Queueing_Job_list.push_back(*itj);
        Queueing_Job_list.insert({(*itj)->get_id(), *itj});
        change_job_in_cluster = true; /// CHECK: this may not be turned on here
        Yield_list.erase(itj--); /// CHECK: safe??
      }
    }

    // check ready list. if done, start to run training.
    std::vector<Job*> ready_done_list;
    for (Job* j : Ready_list) {
      if (j->get_ready_time() == current_event_time) {
        j->run(current_event_time); // job status changed in this call
        for (auto it = end_events.lower_bound(j->get_ready_time());
             it != end_events.upper_bound(j->get_ready_time()); ++it) {
          if (it->second == j->get_id()) {
            end_events.erase(it);
            break;
          }
        }
        end_events.insert({j->get_end_time(), j->get_id()});
        ready_done_list.push_back(j);
        oss << "[Debug] Job" << j->get_id() << " complete initializing training" << std::endl;
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
      if (jid != 0) {
        auto jptr = &jobs.at(jid);
        if (jptr->get_status() == kRun &&
            jptr->get_end_time() == current_event_time) {
          for (Location loc : jptr->locations) cluster_ptr->release_gpu(loc);
          jptr->end(current_event_time);
          jptr->calc_fairness(cluster_ptr, admission_control);
          jptr->clear_fair_records();
          Running_Job_list.erase(jid);
          Jobs_in_cluster.erase(jid);
          change_job_in_cluster = true;
          oss << "[Scheduling] End Job " << jid << std::endl;
          put_log(oss, kInfo);
          it = end_events.erase(it);
        } else {
          ++it;
        }
      } else {
        ++it;
      }
    }

    // for (std::list<Job*>::iterator it_job_ptr = Running_Job_list.begin();
    //      it_job_ptr != Running_Job_list.end(); ++it_job_ptr)
    // {
    //   if ((*it_job_ptr)->get_status() == kRun && (*it_job_ptr)->get_end_time() == current_event_time) { // end job flag
    //     for (Location location : (*it_job_ptr)->locations)
    //       cluster_ptr->release_gpu(location);
    //     (*it_job_ptr)->end(current_event_time);
    //     (*it_job_ptr)->calc_fairness(cluster_ptr, admission_control);
    //     (*it_job_ptr)->clear_fair_records();
    //     oss << "[Scheduling] End Job " << (*it_job_ptr)->get_id()
    //               << std::endl;
    //     put_log(oss,kInfo);
    //     expired_jobs.push_back(it_job_ptr);

    //     auto it = std::find(Jobs_in_cluster.begin(),Jobs_in_cluster.end(),*it_job_ptr);
    //     Jobs_in_cluster.erase(it);
    //   }
    //   // progress all running jobs (= update remain time)
    //   (*it_job_ptr)->update_status(current_event_time);
    // }
    // if (!expired_jobs.empty()) {
    //   change_job_in_cluster = true;
    //   for (const auto& elem : expired_jobs) {
    //     Running_Job_list.erase(elem);
    //   }
    // }


    for (auto itj = Wait_list.begin(); itj != Wait_list.end(); ++itj) {
      if (cluster_ptr->get_idle_GPUs() >= (*itj)->get_gpu_req()) {
        (*itj)->locations = cluster_ptr->provide_gpu(
            (*itj)->get_id(), (*itj)->get_gpu_req(), true);
        (*itj)->scheduled(current_event_time);
        // end_event_list.push_back((*itj)->get_ready_time());
        end_events.insert({(*itj)->get_ready_time(), (*itj)->get_id()});
        Ready_list.push_back(*itj);
        // Queueing_Job_list.remove(*itj);
        Queueing_Job_list.erase((*itj)->get_id());
        // Running_Job_list.push_back(*itj);
        Running_Job_list.insert({(*itj)->get_id(), *itj});
        oss << "[Debug] Job" << (*itj)->get_id() << " complete wait"
            << std::endl;
        put_log(oss,kDebug);
        Wait_list.erase(itj--); ///CHECK: safe??
      }
    }

    // if it is elastic scheduling, allocation needs to be changed considering shrink and expand
    // make new list to start this turn. if job pre-exist, place same. check yield, new alloc
    // ___ 2 ___ Check availability before start job newly
    std::vector<Job*> job_to_start; // job to start in this event round
    std::vector<Job*> job_in_ready;
    // std::vector<Job*> job_to_enqueue;
    // assert(job_to_start.empty());
    // assert(job_in_ready.empty());
    // assert(job_to_enqueue.empty());
    int total_GPU_req = 0;
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
          jptr->set_status(kQueue);
          change_job_in_cluster = true;
          Jobs_in_cluster.insert({jid, jptr});
        }
      } else {
        break;
      }
    }
    // for (Job &job : Job_list) {
    //   if (job.get_submit_time() == current_event_time && job.get_status() == kNone) {
    //     oss << ' ' << job.get_id();
    //     if (admission_control == true) {
    //       admission_queue.push_back(&job);
    //       job.set_status(kSubmit);
    //     } else {
    //       Queueing_Job_list.push_back(&job);
    //       Jobs_in_cluster.push_back(&job);
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
        if (GPU_reqs + admission_queue.front()->get_gpu_req() <= reject_threshold) { // new job flag
          change_job_in_cluster = true;
          Queueing_Job_list.insert({jptr->get_id(), jptr});
          Jobs_in_cluster.insert({jptr->get_id(), jptr});
          GPU_reqs += admission_queue.front()->get_gpu_req();
          admission_queue.front()->set_admitted_time(current_event_time);
          admission_queue.front()->set_status(kQueue);
          admission_queue.pop_front();
        } else {
          break;
        }
      }
    }

/// CHECK: 할당 이벤트를 조정해야할지도...
    if (round_event || change_job_in_cluster) {
      job_in_ready.reserve(Jobs_in_cluster.size());
      job_to_start.reserve(cluster_ptr->get_total_GPUs());

      for (auto [jid, job_ptr] : Queueing_Job_list) {
        if (job_ptr->get_status() == kQueue) {
          job_in_ready.push_back(job_ptr);
        }
      }
      if (round_event) {
        for (auto [jid, jptr] : Running_Job_list) {
/// CHECK: for stauts kRun only?
          jptr->update_status(current_event_time);
          job_in_ready.push_back(jptr);
          // should kYield also contained here? is it possible to be it here?
        }
        // job_in_ready.insert(job_in_ready.end(),Running_Job_list.begin(),Running_Job_list.end());
      }

      double N_default = Running_Job_list.size() + Queueing_Job_list.size();

      if (schedule_type == THEMIS) {
        for (auto &job : job_in_ready) {
          job->compute_finish_time_fairness(current_event_time, cluster_ptr, N_default);
        }
        auto descending_rho = [](Job* A, Job* B)->bool {
          return A->get_rho() > B->get_rho();
        };
        std::sort(job_in_ready.begin(),job_in_ready.end(),descending_rho);
      } else if (schedule_type == RLAS) {
        auto ascending_service = [current_event_time](Job* A, Job* B)->bool {
          return A->get_duration() * A->get_gpu_req() < B->get_duration() * B->get_gpu_req();
        };
        std::sort(job_in_ready.begin(),job_in_ready.end(),ascending_service);
      }

      if (change_job_in_cluster) {
        for (auto [jid, jptr] : Running_Job_list) {
          if (jptr->get_status() != kYield)
            total_GPU_req += jptr->get_gpu_req();
        }
      }
      for (Job* jptr : Wait_list) {
        total_GPU_req += jptr->get_gpu_req();
      }
      job_to_start.insert(job_to_start.end(), Wait_list.begin(), Wait_list.end());

      for(Job* job_ptr : job_in_ready) {
        if (job_ptr->get_gpu_req() + total_GPU_req <= cluster_ptr->get_total_GPUs()) {
          total_GPU_req += job_ptr->get_gpu_req();
          job_to_start.push_back(job_ptr);
        } else {
          // do backfilling
          continue;
          // job_to_enqueue.push_back(job_ptr); // add to queueing job list
        }
      }

// job in running list can be one of kReady, kRun, kYield
// change run & ready state to yield
      if (round_event) {
        // for (auto itj = Running_Job_list.begin(); itj != Running_Job_list.end(); ++itj) {
        for (auto [jid, jptr] : Running_Job_list) {
          if (jptr->get_status() != kYield && !is_elem(jptr, job_to_start)) {
            oss << "[Debug] Job" << jptr->get_id()
                << " is preempted from status " << jptr->get_status()
                << std::endl;
            put_log(oss,kDebug);
/// NOTE: ready 상태 중에도 yield로 바로 넘어가게 되어 있음.
            if (jptr->get_status() == kReady) {
              Ready_list.remove(jptr);
              for (auto it = end_events.lower_bound(jptr->get_ready_time());
                   it != end_events.upper_bound(jptr->get_ready_time()); ++it) {
                if (it->second == jid) {
                  end_events.erase(it);
                  break;
                }
              }
              // auto itt = std::find(end_event_list.begin(),end_event_list.end(),(*itj)->get_ready_time());
              // assert(itt != end_event_list.end());
              // end_event_list.erase(itt);
            } else {
              for (auto it = end_events.lower_bound(jptr->get_end_time());
                   it != end_events.upper_bound(jptr->get_end_time()); ++it) {
                if (it->second == jid) {
                  end_events.erase(it);
                  break;
                }
              }
              // auto itt = std::find(end_event_list.begin(), end_event_list.end(), (*itj)->get_end_time());
              // assert(itt != end_event_list.end());
              // end_event_list.erase(itt);
            }
            jptr->yield(current_event_time);
            end_events.insert({jptr->get_yield_time(), jid});
            Yield_list.push_back(jptr);
          }
        }
      }

      for (Job* job_ptr : job_to_start) {
        // if (!is_elem(job_ptr, Running_Job_list)) {
        if (Running_Job_list.count(job_ptr->get_id()) == 0) {
          if (cluster_ptr->get_idle_GPUs() < job_ptr->get_gpu_req()) {
            if (job_ptr->get_status() != kWait) {
              job_ptr->set_status(kWait);
              Wait_list.push_back(job_ptr);
            }
          } else {
            job_ptr->locations = cluster_ptr->provide_gpu(job_ptr->get_id(),job_ptr->get_gpu_req(),true);
            job_ptr->scheduled(current_event_time); // job status is changed to kReady
            Wait_list.remove(job_ptr);
            // end_event_list.push_back(job_ptr->get_ready_time());
            end_events.insert({job_ptr->get_ready_time(), job_ptr->get_id()});
            Ready_list.push_back(job_ptr);
            // Running_Job_list.push_back(job_ptr);
            Running_Job_list.insert({job_ptr->get_id(), job_ptr});
            // Queueing_Job_list.remove(job_ptr);
            Queueing_Job_list.erase(job_ptr->get_id());
          }
        }
      }
    }

    // // update data for fairness metric
    // // Jobs_in_cluster = Running_Job_list + Queueing_Job_list;
    // for (auto elem : Running_Job_list) {
    //   Jobs_in_cluster.push_back(elem);
    // }
    // for (auto elem : Queueing_Job_list) {
    //   Jobs_in_cluster.push_back(elem);
    // }

    if (!Jobs_in_cluster.empty()) {
      if (t_round_event == -1 || round_event) {
        t_round_event = current_event_time + round_length;
        // end_event_list.push_back(t_round_event);
        end_events.insert({t_round_event, 0});
      }
    } else if (t_round_event != -1) {
      // auto it = std::find(end_event_list.begin(),end_event_list.end(),t_round_event);
      for (auto it = end_events.lower_bound(t_round_event); it != end_events.upper_bound(t_round_event); ++it) {
        if (it->second == 0) {
          end_events.erase(it);
          break;
        }
      }
      // assert(it != end_event_list.end());
      // // if (it != end_event_list.end()) {
      //   end_event_list.erase(it);
      // // }
      t_round_event = -1;
    }  /// NOTE: sementic may be changed

    // // Check end event list
    // end_event_list.sort();
    // // end_event_list.unique();

    // std::cout << "[Schedule] Next end event: ";
    // for (const auto& elem : end_event_list)
    //   {std::cout << elem << " ";}
    // std::cout << std::endl;
    oss << "[Schedule] Cluster: " << cluster_ptr->get_idle_GPUs() << " idle\n";
    put_log(oss,kInfo);
    cluster_ptr->cluster_status();
  } // end while loop
  avg_gpu_load /= current_event_time;
  if (acc_frag_w_q != 0) avg_frag_w_q = (double)acc_frag_w_q / q_time;
  oss << "[Info] avg gpu load: " << avg_gpu_load << std::endl;
  put_log(oss,kInfo);
  return current_event_time;
}