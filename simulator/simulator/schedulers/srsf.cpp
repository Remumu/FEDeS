#include "../util.h"
#include "../cluster.h"
#include "../job.h"
#include "util.h"

/******************************************************************************
* Priority based preemptive gang scheduling:
* two event list contain time stamp
* check earlier ts and process the event.
* if both at the same time, end event fist
* after check end event,
* pick job to start by considering priority and num of GPUs from job in ready
* preemption can occur
* then, alloc and place GPUs to the jobs
rejecting job only does not occur events
combine checkpointing related overhead
  pseudo
    check preempting (save ckpt) & schedule in priority
    check job end
    check job start
    if end or start
      sort for alloc
    else
      next loop
******************************************************************************/
time_type monolithic_preemptive_scheduling(
    Cluster* cluster_ptr,
    // std::list<time_type>& start_event_list,
    std::vector<std::pair<time_type,int>>& submit_events,
    std::unordered_map<int, Job>& jobs,
    ScheduleType schedule_type,
    bool admission_control,
    double &avg_gpu_load,
    double &avg_frag_w_q) {
  oss << "Scheduling: ";
  if (schedule_type == SRTF) {
    oss << "SRTF\n";
  } else if (schedule_type == SRSF) {
    oss << "SRSF\n";
  }
  put_log(oss,kCore);

  // std::list<time_type> end_event_list;
  std::multimap<time_type, int> end_events;
  // std::list<Job*> Running_Job_list, Queueing_Job_list;
  // std::set<int> Running_Job_list, Queueing_Job_list;
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
  while (submit_events.size() + end_events.size()
          + Queueing_Job_list.size() > 0) {
    change_job_in_cluster = false;

    // ___ 0 ___ goto upcomming event
    time_type prev_event_time = current_event_time;
    // current_event_time = upcomming_event_time(start_event_list, end_event_list, current_event_time);
    current_event_time = upcomming_event_time(submit_events, end_events, current_event_time);

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
        job->update_fair_records(current_event_time, Jobs_in_cluster.size(), shares[idx_gpu_reqs], gpu_load);
      // }
      idx_gpu_reqs += 1;
    }
    // Jobs_in_cluster.clear();

    // check yield completed
    std::vector<Job*> yield_done_list;
    for (Job* j : Yield_list) {
      if (j->get_yield_time() == current_event_time) {
        auto range = end_events.equal_range(j->get_yield_time());
        for (auto it = range.first; it != range.second; ++it) {
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
        yield_done_list.push_back(j);
        oss << "[Debug] Job" << j->get_id() << " complete yield" << std::endl;
        put_log(oss,kDebug);
      }
    }
    for (Job* j : yield_done_list) {
      Yield_list.remove(j);
      // Running_Job_list.remove(j);
      Running_Job_list.erase(j->get_id());
      // Queueing_Job_list.push_back(j);
      Queueing_Job_list.insert({j->get_id(), j});
    }

    // check ready list. if done, start to run training.
    std::vector<Job*> ready_done_list;
    for (Job* j : Ready_list) {
      if (j->get_ready_time() == current_event_time) {
        j->run(current_event_time); // job status changed in this call
        // end_event_list.push_back(j->get_end_time());
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
      auto jptr = &jobs.at(jid);
      if (jptr->get_status() == kRun &&
          jptr->get_end_time() == current_event_time) {
        for (Location loc : jptr->locations) cluster_ptr->release_gpu(loc);
        jptr->end(current_event_time);
        Running_Job_list.erase(jid);
        Jobs_in_cluster.erase(jid);
        change_job_in_cluster = true;
        oss << "[Scheduling] End Job " << jid << std::endl;
        put_log(oss, kInfo);
        it = end_events.erase(it);
      } else {
        ++it;
      }
    }  // CHECK: update status는 스케줄링 이벤트 때 하도록.

    // for (std::list<Job*>::iterator it_job_ptr = Running_Job_list.begin();
    //      it_job_ptr != Running_Job_list.end(); ++it_job_ptr)
    // {
    //   if ((*it_job_ptr)->get_status() == kRun && (*it_job_ptr)->get_end_time() == current_event_time) { // end job flag
    //     for (Location location : (*it_job_ptr)->locations)
    //       cluster_ptr->release_gpu(location);
    //     (*it_job_ptr)->end(current_event_time);
    //     oss << "[Scheduling] End Job " << (*it_job_ptr)->get_id()
    //               << std::endl;
    //     put_log(oss,kInfo);
    //     expired_jobs.push_back(it_job_ptr);
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

    // execute job in wait list
    std::vector<Job*> wait_done_list;
    for (Job* j : Wait_list) {
      if (cluster_ptr->get_idle_GPUs() >= j->get_gpu_req()) {
        // schedule job j
        //first GPU alloc
// LOC TEST BEGIN
/*
        for (int i = 0; i < j->get_gpu_req(); ++i) {
          // TODO: locality consolidation need to change GPU alloc scheme.
          Location location = cluster_ptr->provide_gpu(j->get_id());
          j->locations.push_back(location);
        }
*/
        j->locations = cluster_ptr->provide_gpu(j->get_id(),j->get_gpu_req(),true);
// LOC TEST END
        //and then record time using func scheduled
        j->scheduled(current_event_time); // j status is changed here
        // end_event_list.push_back(j->get_ready_time());
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
      Queueing_Job_list.erase(j->get_id());
      // Running_Job_list.push_back(j);
      Running_Job_list.insert({j->get_id(), j});
    }


    // if it is elastic scheduling, allocation needs to be changed considering shrink and expand
    // make new list to start this turn. if job pre-exist, place same. check yield, new alloc
    // ___ 2 ___ Check availability before start job newly
    std::vector<Job*> job_to_start; // job to start in this event round
    std::vector<Job*> job_in_ready;
    std::vector<Job*> job_to_enqueue;
    assert(job_to_start.empty());
    assert(job_in_ready.empty());
    assert(job_to_enqueue.empty());
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
    // for (auto& [id, job] : Jobs) { // CHECK: 여기를 어떻게 빠르게 만들지?
    //   if (job.get_submit_time() == current_event_time && job.get_status() == kNone) { //job.get_submit_flag() == false) {
    //     oss << ' ' << job.get_id();
    //     if (admission_control == true) {
    //       admission_queue.push_back(&job);
    //       job.set_status(kSubmit);
    //     } else {
    //       Queueing_Job_list.push_back(&job);
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
          Jobs_in_cluster.insert({jptr->get_id(), jptr});
          // Queueing_Job_list.push_back(admission_queue.front());
          Queueing_Job_list.insert({jptr->get_id(), jptr});
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
    // combine running job list and queueing_job_list in Run and Queue status only
    // and sort them in certain priority
    // and then, cut them to be fit on GPUs of cluster
    oss << "[Debug] Queue job";
    for (auto [jid, job_ptr] : Queueing_Job_list) {
      oss << ' ' << job_ptr->get_id();
      if (job_ptr->get_status() == kQueue) {
        job_in_ready.push_back(job_ptr);
      }
    }
    oss << std::endl;
    put_log(oss,kDebug);

    oss << "[Debug] Running Job#(end_time):";
    for (auto [jid, job_ptr] : Running_Job_list) {
      if (job_ptr->get_status() == kRun) {
/// DONE: update status here.
        job_ptr->update_status(current_event_time);
        oss << ' ' << job_ptr->get_id()
            << '(' << job_ptr->get_end_time() << ')';
        job_in_ready.push_back(job_ptr);
      }
    }
    oss << std::endl;
    put_log(oss,kDebug);

    // sort jobs in priority. TODO: this only happens when event(new out or in of job) occurs
    if (schedule_type == SRTF) {
      std::sort(job_in_ready.begin(),job_in_ready.end(),
                [](const auto& a, const auto& b) {
                    return a->calc_remain_time_to_compare() < b->calc_remain_time_to_compare();
                }); // ascending remain time
    } else if (schedule_type == SRSF) {
      std::sort(job_in_ready.begin(),job_in_ready.end(),
                [](const auto& a, const auto& b) {
                    return a->get_gpu_req() * a->calc_remain_time_to_compare()
                         < b->get_gpu_req() * b->calc_remain_time_to_compare();
                }); // ascending remain service
    } else {
      oss << "[Debug] Unknown schedule type\n";
      put_log(oss,kDebug);
      assert(false);
    }


    for (auto elem : Wait_list) {
      total_GPU_req += elem->get_gpu_req();
    }
    for (auto elem : Ready_list) {
      total_GPU_req += elem->get_gpu_req();
    }

    for(Job* job_ptr : job_in_ready) {
      if (job_ptr->get_gpu_req() + total_GPU_req <= cluster_ptr->get_total_GPUs()) {
        total_GPU_req += job_ptr->get_gpu_req();
        job_to_start.push_back(job_ptr);
      } // else put in queue?
      else {
        job_to_enqueue.push_back(job_ptr); // add to queueing job list
      }
    }

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
    //   if ((*it_job_ptr)->get_status() == kRun && !is_elem(*it_job_ptr, job_to_start)) {
    //     // remove the job's endtime from end event list
    //     for (std::list<time_type>::iterator it = end_event_list.begin(); it != end_event_list.end(); ++it) {
    //       if (*it == (*it_job_ptr)->get_end_time()) {
    //         end_event_list.erase(it);
    //         break;
    //       }
    //     }
    //     evicted_jobs.push_back(it_job_ptr);
    //   }
    // }
    // for (std::list<Job*>::iterator elem : evicted_jobs) {
    //   (*elem)->yield(current_event_time);
    //   end_event_list.push_back((*elem)->get_yield_time());
    //   // Running_Job_list.erase(elem); // not this! this done in next event
    //   Yield_list.push_back(*elem); // is this necessary?
    //   oss << "[Debug] Job" << (*elem)->get_id() << " is yielding in " << (*elem)->get_yield_time() << "ms" << std::endl;
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
    for (Job* elem : incoming_jobs) {
      if (cluster_ptr->get_idle_GPUs() < elem->get_gpu_req()) {
        // set job status to wait and when there is avail resource, schedule this. (manage in wait_list)
        if (elem->get_status() != kWait) {
          elem->set_status(kWait);
          Wait_list.push_back(elem);
        }
        oss << "[Debug] Job" << elem->get_id() << " waits for resource" << std::endl;
        put_log(oss,kDebug);
      } else {
        elem->locations = cluster_ptr->provide_gpu(elem->get_id(),elem->get_gpu_req(),true);
        elem->scheduled(current_event_time); // job status is changed here
        Wait_list.remove(elem); // if it was in wait list
        // end_event_list.push_back(elem->get_ready_time());
        end_events.insert({elem->get_ready_time(), elem->get_id()});
        Ready_list.push_back(elem);
        // Running_Job_list.push_back(elem);
        Running_Job_list.insert({elem->get_id(), elem});
        // Queueing_Job_list.remove(elem); // This was omitted!
        Queueing_Job_list.erase(elem->get_id());
      }
    }

    // // Queueing_Job_list.sort(submit_time_compare<Job>);
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
    //   for (auto elem : Running_Job_list) {
    //     Jobs_in_cluster.push_back(elem);
    //   }
    //   for (auto elem : Queueing_Job_list) {
    //     Jobs_in_cluster.push_back(elem);
    //   }
    // }

    // // Check end event list
    // end_event_list.sort();
    // // end_event_list.unique();

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