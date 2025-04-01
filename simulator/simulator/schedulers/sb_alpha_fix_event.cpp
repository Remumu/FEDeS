#include <math.h>

#include <cfenv>
#include <ctime>

#include <set>
#include "../cluster.h"
#include "../job.h"
#include "../util.h"
#include "util.h"

extern const unsigned kMinGuarantee;

// struct ActionID {
//   int job_id;
//   int action_idx;
// };

// void add_action_end_events(MicroserviceJob *jptr,
//                            std::multimap<time_type, ActionID> &end_events) {
//   for (auto i = 0; i < jptr->get_gpu_req(); ++i) {
//     auto action_end_time = jptr->get_action_end_time(i);
//     end_events.insert({action_end_time, {jptr->get_id(), i}});
//   }
// }
// void update_job_end_events(MicroserviceJob *jptr,
//                            std::multimap<time_type, int> &job_end_events) {
//   auto new_ete = jptr->compute_expected_end_time();
//   auto old_ete = jptr->get_expected_end_time();
//   if (new_ete != old_ete) {
//     for (auto it_jee = job_end_events.lower_bound(old_ete);
//          it_jee != job_end_events.upper_bound(old_ete);) {
//       if (it_jee->second == jptr->get_id()) {
//         job_end_events.erase(it_jee);
//         break;
//       } else {
//         it_jee++;
//       }
//     }
//     job_end_events.insert({new_ete, jptr->get_id()});
//     jptr->set_expected_end_time(new_ete);
//   }
// }

/******************************************************************************
 * Serverless scheduling alpha-fairness
 ******************************************************************************/
// time_type lsm_simple_alpha_serverless_scheduling(
time_type lsm_simple_alpha_serverless_scheduling(
    Cluster *cluster_ptr,
    ActionDuration *adp,
    // std::list<time_type> &start_event_list,
    std::vector<std::pair<time_type, int>> &submit_events,
    // std::list<time_type> &end_event_list,
    // std::list<MicroserviceJob> &Job_list,
    std::unordered_map<int, MicroserviceJob> &jobs,
    // std::list<MicroserviceJob *> &Running_Job_list,
    // std::list<MicroserviceJob *> &Queueing_Job_list,
    // std::list<Action *> &Running_Action_list,
    // std::list<MicroserviceJob *> &Action_Queued_Job_list,
    bool admission_control,
    double &avg_gpu_load,
    double &avg_frag_w_q,
    time_type evolve_interval,
    double relocate_threshold,
    // time_type round_length,
    double &effi_gpu_load,
    double &fair_gpu_load,
    const double &alpha) {
  oss << "Scheduling: serverless alpha-fairness\n";
  oss << "Event fix version\n";
  put_log(oss, kCore);

  std::multimap<time_type, ActionID> end_events;
  std::map<int, MicroserviceJob*> Running_Job_list, Queueing_Job_list, Action_Queued_Job_list;
  // std::multimap<int, Action*> Running_Action_list;

  // std::list<time_type> job_end_events;
  std::multimap<time_type, int> job_end_events;

  avg_gpu_load = 0;
  avg_frag_w_q = 0;
  time_type acc_frag_w_q = 0;
  time_type q_time = 0;

  // time_type prev_event_time = 0;
  time_type current_event_time = 0;
  std::feclearexcept(FE_OVERFLOW);
  std::feclearexcept(FE_UNDERFLOW);

  clock_t start_sch, end_sch, start_dp, end_dp;
  double max_duration_sch = -1;
  double max_duration_dp = -1;
  size_t max_job_num = 0;
  // int max_case = 0;

  int counter = 0;
  if (evolve_interval != 0) {
    // end_event_list.push_back(start_event_list.front() / evolve_interval
    //                             * evolve_interval + evolve_interval);
    auto event = submit_events.back();
    end_events.insert(
        {event.first / evolve_interval * evolve_interval + evolve_interval,
         {0, 0}});
/// CHECK: job end events 에 넣어줘야하는게 아닐까?
  }

  std::list<MicroserviceJob *> admission_queue;
  // const int reject_threshold =
  //     cluster_ptr->get_total_GPUs() * kAdmissionThreshold;
  // int gpu_load = 0;
  // int job_counts = 0;

  // std::vector<MicroserviceJob *> Jobs_in_cluster;
  std::map<int, MicroserviceJob *> Jobs_in_cluster;
  bool end_occur;
  std::list<MicroserviceJob *> prev_selected_jobs;
  std::list<MicroserviceJob *> Wait_list;

  while (submit_events.size() + end_events.size() +
         Running_Job_list.size() + Queueing_Job_list.size() +
         Action_Queued_Job_list.size() > 0) {
    time_type prev_event_time = current_event_time;

    bool stop_exist = false;
    for (auto [id, j] : Running_Job_list) {
      if (j->get_stop_flag()) {
        stop_exist = true;
        break;
      }
    }
    bool flag_evict_event = false;
    for (auto [id, j] : Running_Job_list) {
      if (j->get_evict_flag()) {
        flag_evict_event = true;
        break;
      }
    }
    bool flag_invoker_queue = false;
    if (!Action_Queued_Job_list.empty()) {
      flag_invoker_queue = true;
    }
    bool flag_real_iq = false;
    if (!Wait_list.empty()) {
      flag_real_iq = true;
    }

    // time_type sel_elem = start_event_list.front();
    time_type sel_elem = submit_events.back().first;
    time_type nearest_job_end = job_end_events.begin()->first;
    if (flag_real_iq || flag_invoker_queue || flag_evict_event || stop_exist) {
      // current_event_time = upcomming_event_time(
      //     start_event_list, end_event_list, current_event_time);
      current_event_time = upcomming_event_time(
          submit_events, end_events, current_event_time);
    } else {
      // current_event_time = upcomming_event_time(
      //     start_event_list, job_end_events, current_event_time);
      current_event_time = upcomming_event_time(
          submit_events, job_end_events, current_event_time);
      // end_event_list.remove_if([current_event_time](time_type t) {
      //   return t <= current_event_time;
      // });

      for (auto it = end_events.begin();
           it != end_events.upper_bound(current_event_time);) {
        if (it->first == current_event_time) {
          break;
        } else if (it->first < current_event_time) {
/// DONE: update action's status
          int jid = it->second.job_id;
          int a_idx = it->second.action_idx;
          auto jptr = &jobs.at(jid);
          if (jptr->is_past_action_end(a_idx, current_event_time)) {
            jptr->update_action(a_idx, current_event_time);
            time_type updated_action_time = jptr->get_action_end_time(a_idx);
/// CHECK: is this safe?
            end_events.insert({updated_action_time, {jid, a_idx}});
            /// NOTE: assume all action had same end time
            if (unsigned(a_idx) == jptr->get_gpu_req() - 1) {
              jptr->set_global_step(jptr->get_action_global_step(a_idx));
            }
          }

          it = end_events.erase(it);
        } else {
          it++;
        }
      }

      for (auto it = end_events.begin(); it != end_events.upper_bound(current_event_time);) {
        if (it->first < current_event_time) {
          it = end_events.erase(it);
        } else {
          it++;
        }
      }
      // // std::set<int> checked_jobs;
      // std::unordered_set<int> checked_jobs;
      // checked_jobs.reserve(Running_Job_list.size());
      // for (auto action : Running_Action_list) {
      //   if (current_event_time > action->get_end_time()) {
      //     time_type action_end_time = action->get_end_time();
      //     time_type past_time = current_event_time - action_end_time;
      //     int iter_progressed = past_time / action->get_exp_next_dur();
      //     iter_progressed += (past_time % action->get_exp_next_dur() == 0 ? 0 : 1);
      //     int global_step =
      //         action->get_global_step() + (iter_progressed ) * action->get_kPeriodN();
      //     time_type next_action_end_time =
      //         action_end_time +
      //         (iter_progressed ) * action->get_exp_next_dur();
      //     action->set_global_step(global_step);
      //     action->set_end_time(next_action_end_time);

      //     if (checked_jobs.find(action->get_job_id()) == checked_jobs.end()) {
      //       // TODO: can be simpler when Job_list is a vector
      //       auto itj =
      //           get_job_ptr_iterator(action->get_job_id(), Running_Job_list);
      //       (*itj)->set_global_step(global_step);
      //       end_event_list.push_back(next_action_end_time);
      //     }
      //     checked_jobs.insert(action->get_job_id());
      //   }
      // }

    }

    // 
    bool flag_shuffle_event = false;
    std::list<MicroserviceJob*> loc_targets;
    if (evolve_interval != 0 && current_event_time % evolve_interval == 0) {
      oss << "[Schedule] Shuffle event" << std::endl;
      put_log(oss,kInfo);
      // for (auto j : Jobs_in_cluster) {
      //   if (j->is_loc_sensitive()) {
      //     j->update_locality_records(cluster_ptr);
      //   }
      // }
      for (auto [id, j] : Running_Job_list) {
        if (!j->get_evict_flag()) {
          // if (j->is_loc_sensitive()
          //     && j->mean_locality_score() > relocate_threshold) {
          //   loc_targets.push_back(j);
          // }
          if (j->is_loc_sensitive()) {
            std::vector<Location> prev_sched;
            if (j->get_stop_flag()) {
              prev_sched = j->get_reserve_locations();
            } else {
              prev_sched = j->get_locations();
            }
            size_t node_size = cluster_ptr->get_node_size();
            std::set<int> uniq_node;
            for (const auto& loc : prev_sched) {
              uniq_node.insert(loc.node_idx);
            }
            size_t n_node_real = uniq_node.size();
            size_t n_node_ideal =
                (prev_sched.size() / node_size) +
                ((prev_sched.size() % node_size) == 0 ? 0 : 1);
            if (n_node_real > n_node_ideal) {
              loc_targets.push_back(j);
            }
          }
        }
      }
      for (auto [id, j] : Action_Queued_Job_list) {
        if (j->get_stop_flag()) {
          if (j->is_loc_sensitive()) {
            std::vector<Location> prev_sched = j->get_reserve_locations();
            size_t node_size = cluster_ptr->get_node_size();
            std::set<int> uniq_node;
            for (const auto &loc : prev_sched) {
              uniq_node.insert(loc.node_idx);
            }
            size_t n_node_real = uniq_node.size();
            size_t n_node_ideal = prev_sched.size() / node_size +
                                  (prev_sched.size() % node_size == 0 ? 0 : 1);
            if (n_node_real > n_node_ideal) {
              loc_targets.push_back(j);
            }
          }
        }
      }
      if (!loc_targets.empty()) {
        flag_shuffle_event = true;
      }
    }

    // if (current_event_time != sel_elem && current_event_time !=
    // job_end_events.front() && Action_Queued_Job_list.empty()) {
    //   if (evolve_interval == 0 || (evolve_interval != 0 &&
    //   (current_event_time % evolve_interval != 0) && !stop_exist &&
    //   !loc_event)) {
    //     if (round_length == 0 || (round_length != 0 && current_event_time %
    //     round_length != 0) && !evict_exist)
    //       simplify = true;
    //   }
    // }
    bool flag_job_submit_or_end = false;
    bool flag_job_submit = false;
    if (current_event_time == sel_elem) {
      flag_job_submit = true;
    }
    bool flag_job_ex_end = false;
    if (current_event_time == nearest_job_end) {
      flag_job_ex_end = true;
    }
    flag_job_submit_or_end = flag_job_submit || flag_job_ex_end;

    // loop status
    if (flag_job_submit_or_end || flag_invoker_queue || flag_evict_event || stop_exist ||
        flag_real_iq || flag_shuffle_event) {
      oss << "\n========== " << ++counter << "th loop ==========\n";
      oss << "\n[Scheduler] TIME: " << current_event_time << std::endl;
      put_log(oss, kCore);
      oss << "-------- list status --------\n"
          << "Start event:\t" << submit_events.size() << std::endl
          << "End event:\t" << job_end_events.size() << std::endl
          << "Rejected job:\t" << admission_queue.size() << std::endl
          << "Running job:\t" << Running_Job_list.size() << std::endl
          << "Queueing job:\t" << Queueing_Job_list.size() << std::endl
          << "Queueing actions' job:\t" << Action_Queued_Job_list.size()
          << std::endl
          << "-----------------------------\n";
      put_log(oss, kCore);
    }

    if (flag_job_submit_or_end || flag_invoker_queue || flag_evict_event || stop_exist ||
        flag_real_iq || flag_shuffle_event) {
      oss << "\nWait list size: " << Wait_list.size() << std::endl;
      put_log(oss, kCore);
      if (!Wait_list.empty()) {
        oss << "Wait list:";
        for (auto elem : Wait_list) {
          oss << ' ' << elem->get_id();
        }
        oss << std::endl;
      }
      put_log(oss, kDebug);

      // if (verbose >= LogLevel::kDebug) {
      //   oss << "\nRunning Action list: ";
      //   for (std::list<Action *>::iterator it_action_ptr =
      //           Running_Action_list.begin();
      //       it_action_ptr != Running_Action_list.end(); ++it_action_ptr) {
      //     oss << (*it_action_ptr)->get_job_id() << "_"
      //         << (*it_action_ptr)->get_global_step() << "_"
      //         << (*it_action_ptr)->get_index() << "("
      //         << (*it_action_ptr)->get_location() << ") ";
      //   }
      //   oss << std::endl << std::endl;
      //   put_log(oss, kDebug);
      // }

      // update fair records here
      int idx_gpu_reqs = 0;
      std::list<std::pair<int, int>> gpu_reqs;
      int gpu_load = 0;
      int gpu_util = 0;
      for (auto [id, elem] : Jobs_in_cluster) {
        gpu_reqs.push_back(std::make_pair(idx_gpu_reqs, elem->get_gpu_req()));
        idx_gpu_reqs += 1;
        gpu_load += elem->get_gpu_req();
        elem->update_online_jct(current_event_time);
      }
      for (auto [id, msjptr] : Running_Job_list) {
        gpu_util += msjptr->get_gpu_usg();
      }
      oss << "GPU load: " << gpu_load << std::endl;
      oss << "GPU usage: " << gpu_util << std::endl;
      oss << "Job load: " << Jobs_in_cluster.size() << std::endl;
      put_log(oss, kCore);
      avg_gpu_load += gpu_load * (current_event_time - prev_event_time);
      // if (!Queueing_Job_list.empty() || !Action_Queued_Job_list.empty()) {
      if (!Queueing_Job_list.empty()) {
        acc_frag_w_q += (cluster_ptr->get_total_GPUs() - gpu_util) * (current_event_time - prev_event_time);
        q_time += current_event_time - prev_event_time;
      }

      for (auto [id, jptr] : Queueing_Job_list) {
        jptr->add_queued_time(current_event_time - prev_event_time);
      }
      for (auto [id, jptr] : Running_Job_list) {
        jptr->add_runned_time(current_event_time - prev_event_time);
        jptr->add_serviced_time((jptr->get_fair_share()) *
                                (current_event_time - prev_event_time));
      }

      std::vector<double> shares = max_min_share(gpu_reqs, cluster_ptr);
      idx_gpu_reqs = 0;
      for (auto [id, job] : Jobs_in_cluster) {
        job->update_fair_records(current_event_time, Jobs_in_cluster.size(),
                                 shares[idx_gpu_reqs], gpu_load);
        idx_gpu_reqs += 1;
        oss << "[Debug] Job" << job->get_id() << " update fair record\n";
        put_log(oss, kDebug);
      }
      // Jobs_in_cluster.clear();
      for (auto [id, j] : Running_Job_list) {
        j->update_service_time(current_event_time);
      }
    }

    end_occur = false;
    // bool need_FS = false;
    // handle action ending event
    for (auto it = end_events.lower_bound(current_event_time);
         it != end_events.upper_bound(current_event_time);) {
      auto jid = it->second.job_id;
      auto jptr = &jobs.at(jid);
      auto a_idx = it->second.action_idx;
      const Action& action = jptr->get_action(a_idx);
      if (it->first != current_event_time) {
        break;
      } else {
        if (action.get_end_time() == current_event_time) {
          cluster_ptr->release_gpu(action.get_location());
          int iter_done_action = jptr->get_iter_done_action() + 1;
          oss << "[Scheduler] End Action " << jid << "-" << iter_done_action << std::endl;
          put_log(oss, kInfo);
          if (unsigned(iter_done_action) == jptr->get_gpu_req()) {
            jptr->sync_iter_action(current_event_time);
            iter_done_action = 0;
            if (jptr->is_end()) {
              jptr->end(current_event_time);
              if (jptr->get_stop_flag()) {
                jptr->release_reservation(cluster_ptr);
              }
              Running_Job_list.erase(jid);
              Jobs_in_cluster.erase(jid);
              end_occur = true;
              for (auto itj = job_end_events.lower_bound(current_event_time);
                   itj != job_end_events.upper_bound(current_event_time);
                   ++itj) {
                if (itj->second == jid) {
                  job_end_events.erase(itj);
                  break;
                }
              }
            } else if (jptr->get_evict_flag()) {
              oss << "[Debug] Job" << jid << " evict flag found" << std::endl;
              put_log(oss, kDebug);
              jptr->evicted(current_event_time);
              jptr->increase_kick_count();
              Running_Job_list.erase(jid);
              Queueing_Job_list.insert({jid, jptr});
              jptr->set_evict(false);
              for (auto itj = job_end_events.lower_bound(jptr->get_expected_end_time());
                   itj != job_end_events.upper_bound(jptr->get_expected_end_time());
                   ++itj) {
                if (itj->second == jid) {
                  job_end_events.erase(itj);
                  break;
                }
              }
            } else if (jptr->get_stop_flag()) {
              oss << "[Debug] Job" << jid << " stop flag found" << std::endl;
              put_log(oss, kDebug);
              std::vector<Location> locs = jptr->get_reserve_locations();
              bool avail = true;
              for (auto loc : locs) {
                if (cluster_ptr->get_gpu_process(loc) != 0) {
                  avail = false;
                }
              }
              if (avail) {
                // invoke actions in new loc
                jptr->invoke_iteration(current_event_time, cluster_ptr, adp, Cold); // NOTE: this can invoke warm actions in shrink
                // for (auto i = 0; i < jptr->get_gpu_req(); ++i) {
                //   auto action_end_time = jptr->get_action_end_time(i);
                //   end_events.insert({action_end_time, {jid, i}});
                // }
                add_action_end_events(jptr, end_events);
                // auto new_ete = jptr->compute_expected_end_time();
                // auto old_ete = jptr->get_expected_end_time();
                // if (new_ete != old_ete) {
                //   // job_end_events.erase(old_ete);
                //   for (auto it_jee = job_end_events.lower_bound(old_ete);
                //        it_jee != job_end_events.upper_bound(old_ete);) {
                //     if (it_jee->second == jid) {
                //       job_end_events.erase(it_jee);
                //       break;
                //     } else {
                //       it_jee++;
                //     }
                //   }
                //   job_end_events.insert({new_ete, jid});
                //   jptr->set_expected_end_time(new_ete);
                // }
                update_job_end_events(jptr, job_end_events);
                jptr->set_stop(false);
                jptr->release_reservation(cluster_ptr);
                jptr->reset_locality_records();
              } else {
                // need to wait until loc is avail
                Wait_list.push_back(jptr);
                jptr->wait_in_iq(current_event_time);
                for (auto itj = job_end_events.lower_bound(jptr->get_expected_end_time());
                     itj != job_end_events.upper_bound(jptr->get_expected_end_time());
                     ++itj) {
                  if (itj->second == jid) {
                    job_end_events.erase(itj);
                    break;
                  }
                }
              }
            } else {
              oss << "[Debug] continue next action" << std::endl;
              put_log(oss, kDebug);
              if (unsigned(cluster_ptr->get_idle_GPUs()) >= jptr->get_fair_share()) {
                jptr->invoke_iteration(current_event_time, cluster_ptr, adp,
                                      Warm);  // check avail GPUs at the moment
                add_action_end_events(jptr, end_events);
                update_job_end_events(jptr, job_end_events);
              } else {
                // if available GPU is not enough, push to Action queued job list
                Action_Queued_Job_list.insert({jid, jptr});
                jptr->evicted(current_event_time);
                oss << "[Schedule] Job" << jptr->get_id() << "'s action queued from running\n";
                put_log(oss, kInfo);
                Running_Job_list.erase(jid);

                std::cerr << "[Error] out of expected flow\n";
                assert(false);
                exit(-1);
              }
            }
          }
          jptr->set_iter_done_action(iter_done_action);
        }

        it = end_events.erase(it);
      }

    }

    // std::vector<std::list<Action *>::iterator> expired_actions;
    // for (std::list<Action *>::iterator it_action_ptr =
    //          Running_Action_list.begin();
    //      it_action_ptr != Running_Action_list.end(); ++it_action_ptr) {
    //   if (current_event_time == (*it_action_ptr)->get_end_time()) {
    //     // the action will be removed from running action list
    //     expired_actions.push_back(it_action_ptr);
    //     cluster_ptr->release_gpu((*it_action_ptr)->get_location());
    //     int action_job_id = (*it_action_ptr)->get_job_id();
    //     std::list<MicroserviceJob *>::iterator it_job_ptr =
    //         get_job_ptr_iterator(action_job_id, Running_Job_list);
    //     int iteration_done_action = (*it_job_ptr)->get_iter_done_action() + 1;
    //     oss << "[Scheduler] End Action " << action_job_id << "-"
    //         << iteration_done_action << std::endl;
    //     put_log(oss, kInfo);
    //     if (iteration_done_action == (*it_job_ptr)->get_gpu_req()) {
    //       (*it_job_ptr)->sync_iter_action(current_event_time);
    //       iteration_done_action = 0;
    //       if ((*it_job_ptr)->is_end()) {
    //         (*it_job_ptr)->end(current_event_time);
    //         if ((*it_job_ptr)->get_stop_flag()) {
    //           (*it_job_ptr)->release_reservation(cluster_ptr);
    //         }
    //         Running_Job_list.erase(it_job_ptr);
    //         // need_FS = true;
    //         end_occur = true;
    //       } else if ((*it_job_ptr)->get_evict_flag()) {
    //         oss << "[Debug] Job" << (*it_job_ptr)->get_id()
    //             << " evict flag found" << std::endl;
    //         put_log(oss, kDebug);
    //         (*it_job_ptr)->evicted(current_event_time);
    //         (*it_job_ptr)->increase_kick_count();
    //         Running_Job_list.erase(it_job_ptr);
    //         Queueing_Job_list.push_back(*it_job_ptr);
    //         (*it_job_ptr)->set_evict(false);
    //       } else if ((*it_job_ptr)->get_stop_flag()) {
    //         oss << "[Debug] Job" << (*it_job_ptr)->get_id()
    //             << " stop flag found" << std::endl;
    //         put_log(oss, kDebug);
    //         std::vector<Location> locs = (*it_job_ptr)->get_reserve_locations();
    //         bool avail = true;
    //         for (auto loc : locs) {
    //           if (cluster_ptr->get_gpu_process(loc) != 0) {
    //             avail = false;
    //           }
    //         }
    //         if (avail) {
    //           // invoke actions in new loc
    //           (*it_job_ptr)
    //               ->invoke_iteration(current_event_time, cluster_ptr, adp,
    //                                  &Running_Action_list, &end_event_list,
    //                                  &job_end_events, Cold); // NOTE: this can invoke warm actions in shrink
    //           (*it_job_ptr)->set_stop(false);
    //           (*it_job_ptr)->release_reservation(cluster_ptr);
    //           (*it_job_ptr)->reset_locality_records();
    //         } else {
    //           // need to wait until loc is avail
    //           Wait_list.push_back(*it_job_ptr);
    //           (*it_job_ptr)->wait_in_iq(current_event_time);
    //         }
    //       } else {  // handle change of logical worker in invoke_iteration
    //         oss << "[Debug] continue next action" << std::endl;
    //         put_log(oss, kDebug);
    //         if (cluster_ptr->get_idle_GPUs() >=
    //             (*it_job_ptr)->get_fair_share()) {
    //           (*it_job_ptr)
    //               ->invoke_iteration(current_event_time, cluster_ptr, adp,
    //                                  &Running_Action_list, &end_event_list,
    //                                  &job_end_events,
    //                                  Warm);  // check avail GPUs at the moment
    //         } else {
    //           // if available GPU is not enough, push to Action queued job list
    //           Action_Queued_Job_list.push_back(*it_job_ptr);
    //           (*it_job_ptr)->evicted(current_event_time);
    //           oss << "[Schedule] Job" << (*it_job_ptr)->get_id()
    //               << "'s action queued from running\n";
    //           put_log(oss, kInfo);
    //           Running_Job_list.erase(it_job_ptr);

    //           std::cerr << "[Error] out of expected flow\n";
    //           assert(false);
    //           exit(-1);
    //         }
    //       }
    //     }
    //     // set or reset iter_done_action
    //     (*it_job_ptr)->set_iter_done_action(iteration_done_action);
    //   }
    // }
    // // remove done action from running action list
    // for (const auto &elem : expired_actions) {
    //   Running_Action_list.erase(elem);
    // }

    if (flag_real_iq) {
      std::vector<MicroserviceJob *> done;
      for (auto j : Wait_list) {
        if (j->get_stop_flag()) {
          std::vector<Location> locs = j->get_reserve_locations();
          bool avail = true;
          for (auto loc : locs) {
            if (cluster_ptr->get_gpu_process(loc) != 0) {
              avail = false;
            }
          }
          if (avail == true) {
            j->invoke_iteration(current_event_time, cluster_ptr, adp,
                                Cold);
            add_action_end_events(j, end_events);
            update_job_end_events(j, job_end_events);
            done.push_back(j);
            j->set_stop(false);
            j->release_reservation(cluster_ptr);
          }
        } else {
          std::cerr << "[Error] out of expected flow\n";
          assert(false);
          exit(-1);
        }
      }
      for (auto elem : done) {
        Wait_list.remove(elem);
      }
    }

    if (flag_invoker_queue) {
      // Optional.
      // Wheather compute FS before invoke invoker level queued jobs
      // or invoke them right away and then compute FS
      // For now, invoke right away version.
      // ___ 2 ___
      // Try job in Action Queued Job list // is this always be able to
      // allocate?
      // Action_Queued_Job_list.sort([](const auto& a, const auto& b) {
      //                               return a->get_submit_time() <
      //                               b->get_submit_time();
      //                             });

/// NOTE: action queued job list will be sorted in its id.
      for (auto it = Action_Queued_Job_list.begin();
           it != Action_Queued_Job_list.end();) {
        auto id = it->first;
        auto jptr = it->second;
        oss << "Action_Queued_Job " << jptr->get_id() << "\n";
        put_log(oss, kInfo);

        if (cluster_ptr->is_available_gpus(jptr->get_reserve_locations(),
                                           jptr->get_id())) {
          jptr->invoke_iteration(current_event_time, cluster_ptr, adp, Cold);
          add_action_end_events(jptr, end_events);
          update_job_end_events(jptr, job_end_events);
          jptr->set_stop(false);
          jptr->release_reservation(cluster_ptr);
          Running_Job_list.insert({id, jptr});

          // rescheduled_jobs.push_back(it_job_ptr);
          it = Action_Queued_Job_list.erase(it);
        } else {
          it++;
        }
      }
    }

    //   std::vector<std::list<MicroserviceJob *>::iterator> rescheduled_jobs;
    //   for (std::list<MicroserviceJob *>::iterator it_job_ptr =
    //            Action_Queued_Job_list.begin();
    //        it_job_ptr != Action_Queued_Job_list.end(); ++it_job_ptr) {
    //     oss << "Action_Queued_Job " << (*it_job_ptr)->get_id() << "\n";
    //     put_log(oss, kInfo);

    //     if (cluster_ptr->is_available_gpus(
    //             (*it_job_ptr)->get_reserve_locations(),
    //             (*it_job_ptr)->get_id())) {
    //       (*it_job_ptr)
    //           ->invoke_iteration(current_event_time, cluster_ptr, adp,
    //                              &Running_Action_list, &end_event_list,
    //                              &job_end_events, Cold);

    //       (*it_job_ptr)->set_stop(false);
    //       (*it_job_ptr)->release_reservation(cluster_ptr);
    //       Running_Job_list.push_back(*it_job_ptr);

    //       rescheduled_jobs.push_back(it_job_ptr);
    //     }
    //   }
    //   for (const auto &elem : rescheduled_jobs) {
    //     Action_Queued_Job_list.erase(elem);
    //   }
    // }

    if (flag_job_submit || end_occur) {
      // if (flag_job_submit_or_end) {
      // add new job to admission queue and admit job to cluster within
      // threshold
/// CHECK: maybe submit checking can be only flagged in job submit flag
      while (!submit_events.empty()) {
        if (submit_events.back().first == current_event_time) {
          auto jid = submit_events.back().second;
          submit_events.pop_back();
          auto jptr = &jobs.at(jid);
          oss << "Job" << jid << " is submitted.\n";
          put_log(oss, kInfo);
          if (admission_control == true) {
            admission_queue.push_back(jptr);
          } else {
            Queueing_Job_list.insert({jid, jptr});
            Jobs_in_cluster.insert({jid, jptr});
          }
          jptr->set_submit_flag();
        } else {
          break;
        }
      }

      // for (MicroserviceJob &job : Job_list) {
      //   if (job.get_submit_time() == current_event_time &&
      //       job.get_submit_flag() == false) {  // since time checking can occur
      //                                          // twice, check submit flag
      //     oss << "Job" << job.get_id() << " is submitted.\n";
      //     put_log(oss, kInfo);
      //     if (admission_control == true) {
      //       admission_queue.push_back(&job);
      //     } else {
      //       Queueing_Job_list.push_back(&job);
      //       // need function submitted() to check submit time..?
      //     }
      //     job.set_submit_flag();
      //   }
      // }

      // ___ lsm ___
      std::list<MicroserviceJob *> selected_jobs;
      std::list<MicroserviceJob *> job_to_reallocate_lsm;

      for (auto [id, jptr] : Running_Job_list) {
        job_to_reallocate_lsm.push_back(jptr);
      }
      for (auto [id, jptr] : Action_Queued_Job_list) {
        job_to_reallocate_lsm.push_back(jptr);
      }
      for (auto [id, jptr] : Queueing_Job_list) {
        job_to_reallocate_lsm.push_back(jptr);
      }

      int total_gpus = cluster_ptr->get_total_GPUs();
      // int added_gpus = 0;
      int remaining_gpus = total_gpus;

      for (auto j : job_to_reallocate_lsm) {
        j->set_prev_fair_share();
        j->calc_remain_time(adp, j->get_gpu_req(), 1);
        j->calc_remain_service(adp);
      }

      job_to_reallocate_lsm.sort([](const auto &a, const auto &b) {
        return a->get_submit_time() < b->get_submit_time();
        // return a->get_remain_service() < b->get_remain_service();
      });

      double round_interval = 1;
      std::vector<double> jobs;
      std::vector<int> alpha_result;
      std::vector<int> gpu_reqs;

      int sums = 0;
      for (auto jptr : job_to_reallocate_lsm) {
        gpu_reqs.push_back(jptr->get_gpu_req());
        sums += jptr->get_gpu_req();
      }

      std::vector<int> jid_indexes;

      if (sums <= cluster_ptr->get_total_GPUs()) {
        for (int gpu_req : gpu_reqs) {
          alpha_result.push_back(gpu_req);
        }
        int ind = 0;
        for (auto jptr : job_to_reallocate_lsm) {
          int share = alpha_result[ind++];
          jptr->set_fair_share(share);
          if (share != 0) {
            selected_jobs.push_back(jptr);
          }
        }
      } else {
        start_sch = clock();
        if (job_to_reallocate_lsm.size() > max_job_num) {
          max_job_num = job_to_reallocate_lsm.size();
        }
        std::vector<std::vector<int>> gpu_allocations;
        std::vector<std::vector<double>> values;
        std::vector<std::vector<bool>> is_valid(job_to_reallocate_lsm.size(),
                                                std::vector<bool>(6, true));
        int k = 0;
        double util_min = std::numeric_limits<double>::max();
        double util_max = std::numeric_limits<double>::lowest();
        for (auto jptr : job_to_reallocate_lsm) {
          jid_indexes.push_back(k);
          std::vector<int> gpu_allocation;
          std::vector<double> value;
          gpu_allocation.push_back(0);
          double value_0 = jptr->calc_alpha_fairness_value(
              adp, alpha, 0, jptr->get_gpu_usg(), round_interval);
          value.push_back(value_0);
          if (value_0 < util_min) {
            util_min = value_0;
          }
          if (value_0 > util_max) {
            util_max = value_0;
          }
          int jj = 1;
          for (unsigned i = 1; i <= 16; i *= 2) {
            gpu_allocation.push_back(i);
            if (jptr->get_gpu_req() < i ||
                (i == 1 && jptr->get_gpu_req() >= 2 && kMinGuarantee == 2)) {
              value.push_back(-1);
              is_valid[k][jj] = false;
            } else {
              double alpha_value = jptr->calc_alpha_fairness_value(
                  adp, alpha, i, jptr->get_gpu_usg(), round_interval);
              value.push_back(alpha_value);
              if (alpha_value < util_min) {
                util_min = alpha_value;
              }
              if (alpha_value > util_max) {
                util_max = alpha_value;
              }
            }
            jj++;
          }
          gpu_allocations.push_back(gpu_allocation);
          values.push_back(value);
          k++;
        }

        double value_min = std::numeric_limits<double>::max();
        double value_max = std::numeric_limits<double>::lowest();

        for (unsigned i = 0; i < values.size(); i++) {
          for (unsigned j = 0; j < values[i].size(); j++) {
            if (is_valid[i][j]) {
              if (alpha >= 1.0) {
                values[i][j] += pow(10, -8);
              }

              if (alpha != 1.0) {
                values[i][j] = pow(values[i][j], (double)1.0 - (double)alpha) /
                               ((double)1.0 - (double)alpha);
                if ((bool)std::fetestexcept(FE_OVERFLOW) ||
                    (bool)std::fetestexcept(FE_UNDERFLOW)) {
                  std::cout << "FLOWERROR "
                            << (bool)std::fetestexcept(FE_OVERFLOW) << ", "
                            << (bool)std::fetestexcept(FE_UNDERFLOW)
                            << std::endl;
                }
              } else {
                values[i][j] = log((values[i][j]));
                if ((bool)std::fetestexcept(FE_OVERFLOW) ||
                    (bool)std::fetestexcept(FE_UNDERFLOW)) {
                  std::cout << "FLOWERROR "
                            << (bool)std::fetestexcept(FE_OVERFLOW) << ", "
                            << (bool)std::fetestexcept(FE_UNDERFLOW)
                            << std::endl;
                }
              }
              if (std::isinf(values[i][j]) || std::isnan(values[i][j])) {
                std::cout << "VALUEERROR" << std::endl;
              } else {
                if (values[i][j] < value_min) {
                  value_min = values[i][j];
                }
                if (values[i][j] > value_max) {
                  value_max = values[i][j];
                }
              }
            }
          }
        }

        double d_value_max = -1;
        for (size_t i = 0; i < values.size(); i++) {
          auto pit = job_to_reallocate_lsm.begin();
          std::advance(pit, i);
          double rt = static_cast<double>((*pit)->get_remain_time()) / 1000.0;
          double cold = static_cast<double>(get_cold_time(
                            (*pit)->get_model(), (*pit)->get_gpu_req())) /
                        1000.0;
          for (size_t j = 0; j < values[i].size(); j++) {
            if (is_valid[i][j]) {
              values[i][j] += ((double)-1.0 * (value_min));
              if ((bool)std::fetestexcept(FE_OVERFLOW) ||
                  (bool)std::fetestexcept(FE_UNDERFLOW)) {
                std::cout << "FLOWERROR "
                          << (bool)std::fetestexcept(FE_OVERFLOW) << ", "
                          << (bool)std::fetestexcept(FE_UNDERFLOW)
                          << std::endl;
              }
              values[i][j] *= (1.0 / (rt + cold));
              if ((bool)std::fetestexcept(FE_OVERFLOW) ||
                  (bool)std::fetestexcept(FE_UNDERFLOW)) {
                std::cout << "FLOWERROR "
                          << (bool)std::fetestexcept(FE_OVERFLOW) << ", "
                          << (bool)std::fetestexcept(FE_UNDERFLOW)
                          << std::endl;
              }
              if (j > 0) {
                if (values[i][j] - values[i][j - 1] > d_value_max) {
                  d_value_max = values[i][j] - values[i][j - 1];
                }
              }
            }
          }
        }

        for (auto j : job_to_reallocate_lsm) {
          j->set_fair_share(0);
        }

        d_value_max *= pow(10, -6);
        start_dp = clock();

        int num_jobs = job_to_reallocate_lsm.size();
        std::vector<double> max_perf(cluster_ptr->get_total_GPUs() + 1, 0.0);
        std::vector<std::vector<int>> gpu_distribution(
            cluster_ptr->get_total_GPUs() + 1, std::vector<int>(num_jobs, 0));
        for (int i = 0; i < num_jobs; i++) {
          for (int j = cluster_ptr->get_total_GPUs(); j > 0; j--) {
            for (int k = 1; k < 6; k++) {
              int gpu_count = gpu_allocations[i][k];
              if (j >= gpu_count && is_valid[i][k]) {
                if (max_perf[j - gpu_count] + values[i][k] > max_perf[j] &&
                    values[i][k] - values[i][k - 1] > d_value_max) {
                  max_perf[j] = max_perf[j - gpu_count] + values[i][k];
                  for (int t = 0; t < num_jobs; t++) {
                    gpu_distribution[j][t] = gpu_distribution[j - gpu_count][t];
                  }
                  gpu_distribution[j][i] = gpu_count;
                }
              }
            }
          }
        }

        double max_p = -1.0;
        int index_max_p = 0;
        for (int i = 0; i < cluster_ptr->get_total_GPUs() + 1; i++) {
          if (max_p < max_perf[i] &&
              !double_isSame(max_p, max_perf[i], alpha)) {
            max_p = max_perf[i];
            index_max_p = i;
          }
        }
        std::vector<int> alpha_result;
        for (int i = 0; i < num_jobs; i++) {
          alpha_result.push_back(gpu_distribution[index_max_p][i]);
        }

        int ind = 0;
        for (auto jptr : job_to_reallocate_lsm) {
          int share = alpha_result[ind++];
          jptr->set_fair_share(share);
          if (share != 0) {
            selected_jobs.push_back(jptr);
            remaining_gpus -= share;
          }
        }
        end_dp = clock();

        for (int n_gpu = 1; n_gpu <= 16; n_gpu *= 2) {
          for (auto j : job_to_reallocate_lsm) {
            int current_fs = j->get_fair_share();
            if (n_gpu - current_fs > 0 && unsigned(n_gpu) <= j->get_gpu_req()) {
              if (n_gpu - current_fs <= remaining_gpus) {
                if (!is_elem(j, selected_jobs)) {
                  selected_jobs.push_back(j);
                }
                remaining_gpus -= (n_gpu - current_fs);
                j->set_fair_share(n_gpu);
              }
            }
          }
        }
      }
      end_sch = clock();

      double dp_duration = (double)(end_dp - start_dp) / CLOCKS_PER_SEC;
      double sch_duration = (double)(end_sch - start_sch) / CLOCKS_PER_SEC;
      if (dp_duration > max_duration_dp) {
        max_duration_dp = dp_duration;
      }
      if (sch_duration > max_duration_sch) {
        max_duration_sch = sch_duration;
      }

      // std::cout << "allocation" << std::endl;
      // for (auto j : job_to_reallocate_lsm) {
      //   std::cout << j->get_fair_share() << " ";
      // }
      // std::cout << std::endl;

      // if (!flag_shuffle_event) {
//// TODO: determine placement based on share and previous usg for jobs in job_to_reallocate
        determine_placement(Running_Job_list, Queueing_Job_list,
                            Action_Queued_Job_list, job_to_reallocate_lsm,
                            selected_jobs, Wait_list,
                            end_events, job_end_events, current_event_time,
                            cluster_ptr, adp);
      // } else {
      //   determine_placement_w_shuffle(
      //       Running_Job_list, Queueing_Job_list, Action_Queued_Job_list,
      //       job_to_reallocate_lsm, SelectedJobs, ShuffleJobs, Wait_list,
      //       Running_Action_list, end_event_list, job_end_events,
      //       current_event_time, cluster_ptr, adp);
      // }
    } else if (flag_shuffle_event) {


      shuffle_placements(Running_Job_list, Queueing_Job_list, Action_Queued_Job_list,
                    loc_targets, Wait_list, end_events,
                    job_end_events, current_event_time, cluster_ptr, adp);
    }
    if (flag_shuffle_event) {
      // end_event_list.push_back(current_event_time + evolve_interval);
      end_events.insert({current_event_time + evolve_interval, {0, 0}});
/// CHECK: job events 에 넣어야할지?
    }
    if (flag_job_submit_or_end || flag_invoker_queue || flag_evict_event || stop_exist ||
        flag_real_iq || flag_shuffle_event) {
      // prev_event_time = current_event_time;
      // for (auto elem : Running_Job_list) {
      //   // Jobs_in_cluster.push_back(elem);
      //   Jobs_in_cluster.insert(elem);
      // }
      // for (auto elem : Action_Queued_Job_list) {
      //   Jobs_in_cluster.insert(elem);
      // }
      // for (auto elem : Queueing_Job_list) {
      //   Jobs_in_cluster.insert(elem);
      // }

      oss << "[Schedule] Cluster: " << cluster_ptr->get_idle_GPUs() << " idle"
          << std::endl;
      put_log(oss, kInfo);
      cluster_ptr->cluster_status();
    }
    // // Check end event list
    // end_event_list.sort();
    // end_event_list.unique();
    if (verbose >= LogLevel::kDebug) {
      oss << "[Schedule] Next end event: ";
      for (auto [t, aid] : end_events) {
        oss << t << '(' << aid.job_id << '-' << aid.action_idx << ')' << " ";
      }
      oss << std::endl;
      put_log(oss, kDebug);
    }

    // job_end_events.remove(current_event_time);
    // job_end_events.sort();
    if (verbose >= LogLevel::kDebug) {
      oss << "[Debug] job end events: ";
      for (auto [t, id] : job_end_events) {
        oss << t << '(' << id << ')' << ' ';
      }
      oss << std::endl;
      put_log(oss, kDebug);
    }
  }  // end while loop

  avg_gpu_load /= current_event_time;
  if (acc_frag_w_q != 0) avg_frag_w_q = double(acc_frag_w_q) / q_time;

  effi_gpu_load /= current_event_time;
  fair_gpu_load /= current_event_time;
  oss << "[Info] avg gpu load: " << avg_gpu_load << std::endl;
  put_log(oss, kDebug);

  oss << "max schd: " << max_duration_sch << ", max dp: " << max_duration_dp
      << ", max job: " << max_job_num << std::endl;
  put_log(oss, kQuiet);
  // adp->print_action_durations();
  return current_event_time;
}
