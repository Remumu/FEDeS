#ifndef __SCHED_UTIL_H__
#define __SCHED_UTIL_H__

#include <assert.h>

#include <list>
#include <set>
#include <unordered_set>
#include <map>
#include <unordered_map>
#include <algorithm>

#include "../cluster.h"
#include "../job.h"
#include "../util.h"

const double kAdmissionThreshold = 2.0;

enum ScheduleType {
  GRML,
  GRML_2P,
  GRML_DIV,
  GRML_2M,
  GRDL_OPT,
  GRDL_LSM,
  S_ALPHA_ZERO,
  GRDL_SRSF,
  S_ALPHA_ONE,
  S_MM,
  S_MM_CP,
  S_SRSF,
  S_THEMIS,
  S_TIRESIAS,
  FIFO,
  SRTF,
  SRSF,
  LAS,
  DELAY,
  THEMIS,
  RLAS,
  FIFOBL
};

enum RoundRule { kRR, kLas, kProp };

struct ActionID {
  int job_id;
  int action_idx;
};

time_type upcomming_event_time(std::list<time_type>& start_event_list,
                               std::list<time_type>& end_event_list,
                               time_type current_time) {
  time_type time = -1;
  // TODO: change assert to cpp fault handler?
  assert(end_event_list.front() >= 0 && start_event_list.front() >= 0);
  if (end_event_list.empty() && start_event_list.empty()) {
    if (time == current_time) {
      oss << "[Error] Program reached unimplemented area in "
             "main.upcomming_event_time().\n";
      put_log(oss, kDebug);
    }
    // this can happen when all jobs in cluster end at the same time but queue
    // is remain
    time = current_time;
  } else if (end_event_list.empty()) {
    time = start_event_list.front();
    start_event_list.pop_front();
    oss << "[Event] start\n";
  } else if (start_event_list.empty()) {
    time = end_event_list.front();
    // end_event_list.pop_front();
    end_event_list.remove(time);
    oss << "[Event] end\n";
  } else {
    time_type start_t = start_event_list.front();
    time_type end_t = end_event_list.front();
    if (start_t < end_t) {
      time = start_t;
      start_event_list.pop_front();
      oss << "[Event] start\n";
    } else if (start_t > end_t) {
      time = end_t;
      // end_event_list.pop_front();
      end_event_list.remove(time);
      oss << "[Event] end\n";
    } else {
      time = end_t;
      // end_event_list.pop_front();
      end_event_list.remove(time);
      start_event_list.pop_front();
      oss << "[Event] end and start\n";
    }
  }
  put_log(oss, kInfo);
  return time;
}

template<typename T>
time_type upcomming_event_time(
    std::vector<std::pair<time_type, int>> submit_events,
    std::multimap<time_type, T> end_events, time_type current_event_time) {
  time_type time = -1;
  if (end_events.empty() && submit_events.empty()) {
    time = current_event_time;
  } else {
    time_type submit_t = LONG_LONG_MAX;
    time_type end_t = LONG_LONG_MAX;
    if (!submit_events.empty()) submit_t = submit_events.back().first;
    if (!end_events.empty()) end_t = end_events.begin()->first;

    time = std::min({submit_t, end_t});
    if (time == submit_t) {
      oss << "[Event] start\n";
    }
    if (time == end_t) {
      oss << "[Event] end\n";
    }
  }
  put_log(oss, kInfo);

  assert(time >= current_event_time);
  return time;
}

// compute max-min fairness
bool compare_req(const std::pair<int, int>& first,
                 const std::pair<int, int>& second) {
  return (first.second < second.second);
}
bool compare_idx(const std::pair<int, int>& first,
                 const std::pair<int, int>& second) {
  return (first.first < second.first);
}
// gpu_reqs is assumed to be sorted in increasing order of elem.first
std::vector<double> max_min_share(std::list<std::pair<int, int> >& gpu_reqs,
                                  Cluster* cluster_ptr) {
  std::vector<double> ret_vec;
  std::list<std::pair<int, double> > shares;
  gpu_reqs.sort(compare_req);

  double total = (double)cluster_ptr->get_total_GPUs();
  int nums = gpu_reqs.size();
  for (const auto& elem : gpu_reqs) {
    double share = total / nums;
    if (elem.second < share) {
      share = elem.second; /// CHECK: share가 순서에 영향을 받겠는데? 순서 고정 필요
    }
    total -= share;
    nums -= 1;

    shares.push_back(std::make_pair(elem.first, share));
  }

  shares.sort(compare_idx);
  ret_vec.reserve(shares.size());
  for (const auto& elem : shares) {
    ret_vec.push_back(elem.second);
  }

  return ret_vec;
}

template <typename T, typename C>
bool is_elem(T x, C container) {
  for (T elem : container) {
    if (x == elem) {
      return true;
    }
  }
  return false;
}

template <typename T_JOB>
bool ascend_start_t_ascend_submit_t(T_JOB* A, T_JOB* B) {
  time_type i = A->get_start_time_stamp();
  time_type j = B->get_start_time_stamp();
  if (i == j) {
    time_type k = A->get_submit_time();
    time_type l = B->get_submit_time();
    return (k < l);  // increasing in submit time
  } else {
    return (i < j);  // increasing
  }
}

/******************************************************************************
 * MicroserviceJob schedulers utility functions
******************************************************************************/
void add_action_end_events(MicroserviceJob *jptr,
                           std::multimap<time_type, ActionID> &end_events) {
  for (int i = 0; size_t(i) < jptr->get_gpu_req(); ++i) {
    auto action_end_time = jptr->get_action_end_time(i);
    end_events.insert({action_end_time, {jptr->get_id(), i}});
  }
}
void update_job_end_events(MicroserviceJob *jptr,
                           std::multimap<time_type, int> &job_end_events) {
  auto new_ete = jptr->compute_expected_end_time();
  auto old_ete = jptr->get_expected_end_time();
  if (new_ete != old_ete) {
    for (auto it_jee = job_end_events.lower_bound(old_ete);
         it_jee != job_end_events.upper_bound(old_ete);) {
      if (it_jee->second == jptr->get_id()) {
        job_end_events.erase(it_jee);
        break;
      } else {
        it_jee++;
      }
    }
    job_end_events.insert({new_ete, jptr->get_id()});
    jptr->set_expected_end_time(new_ete);
  }
}

void determine_warm_placement(std::vector<Location>& prev_sched,
                              Cluster* cluster_ptr, MicroserviceJob* job,
                              std::vector<Location>& canGPUs) {
  if (job->get_share() == prev_sched.size()) {
    for (auto loc : prev_sched) {
      auto it = std::find(canGPUs.begin(), canGPUs.end(), loc);
      if (it != canGPUs.end()) canGPUs.erase(it);
    }
  }
  if (job->get_share() < prev_sched.size()) {
    std::vector<Location> placement = cluster_ptr->determine_placement(
        job->get_id(), job->get_share(), job->is_loc_sensitive(), prev_sched);
    // std::vector<Location> placement =
    // cluster_ptr->determine_placement(job->get_id(), job->get_share(),
    // job->get_loc_sensitivity_wrt_wave(), prev_sched);
    job->reserve_move(cluster_ptr, placement);
    // job->set_stop(true);
    // cluster_ptr->set_reserve_job(job->get_id(), placement);
    for (auto loc : placement) {
      auto it = std::find(canGPUs.begin(), canGPUs.end(), loc);
      if (it != canGPUs.end()) canGPUs.erase(it);
    }
  }
}
void determine_cold_placement(
    std::map<int, MicroserviceJob*>& RunningJobs,
    std::map<int, MicroserviceJob*>& QueueingJobs,
    std::map<int, MicroserviceJob*>& AQJobs,
    // std::list<MicroserviceJob*>& AllSubmittedJobs,
    std::list<MicroserviceJob*>& SelectedJobs,
    std::list<MicroserviceJob*>& IQJobs,
    // std::list<Action*>& RunningActions,
    std::multimap<time_type, ActionID>& end_events,
    std::multimap<time_type, int>& job_end_times,
    time_type current_event_time,
    Cluster* cluster_ptr,
    ActionDuration* adp,
    MicroserviceJob* job,
    std::vector<Location>& canGPUs) {
// void determine_cold_placement(
//     std::list<MicroserviceJob*>& RunningJobs,
//     std::list<MicroserviceJob*>& QueueingJobs,
//     std::list<MicroserviceJob*>& AQJobs,
//     // std::list<MicroserviceJob*>& AllSubmittedJobs,
//     std::list<MicroserviceJob*>& SelectedJobs,
//     std::list<MicroserviceJob*>& IQJobs,
//     // std::list<Action*>& RunningActions,
//     std::list<time_type>& end_events,
//     std::list<time_type>& job_end_times,
//     time_type current_event_time,
//     Cluster* cluster_ptr,
//     ActionDuration* adp,
//     MicroserviceJob* job,
//     std::vector<Location>& canGPUs) {
  std::vector<Location> placement = cluster_ptr->determine_placement(
      job->get_id(), job->get_share(), job->is_loc_sensitive(), canGPUs);
  // std::vector<Location> placement =
  // cluster_ptr->determine_placement(job->get_id(), job->get_share(),
  // job->get_loc_sensitivity_wrt_wave(), canGPUs);
  for (auto loc : placement) {
    auto it = std::find(canGPUs.begin(), canGPUs.end(), loc);
    if (it != canGPUs.end()) canGPUs.erase(it);
  }
  job->reserve_move(cluster_ptr, placement);
  // if ((!is_elem(job, RunningJobs) || job->is_in_wait()) &&
  if ((RunningJobs.find(job->get_id()) == RunningJobs.end() || job->is_in_wait()) &&
      cluster_ptr->is_available_gpus(placement, job->get_id())) {

    if (job->is_in_wait()) {
      IQJobs.remove(job);
    } else {
      AQJobs.erase(job->get_id());
      QueueingJobs.erase(job->get_id());
      RunningJobs.insert({job->get_id(), job});
    }
    // job->invoke_iteration(current_event_time, cluster_ptr, adp, &RunningActions,
    //                       &end_events, &job_end_times, Cold);
    job->invoke_iteration(current_event_time, cluster_ptr, adp, Cold);
    add_action_end_events(job, end_events);
    update_job_end_events(job, job_end_times);
    // IQJobs.remove(job);
    // AQJobs.remove(job);
    // QueueingJobs.remove(job);
    // RunningJobs.push_back(job);
    job->set_stop(false);
    job->release_reservation(cluster_ptr);
  } else {
    // job->reserve_move(cluster_ptr, placement);
    // cluster_ptr->set_reserve_job(job->get_id(), placement);
    // canGPUs.remove(placement);
    // if (is_elem(job, RunningJobs)) {
    if (RunningJobs.find(job->get_id()) != RunningJobs.end()) {
      // job->set_stop(true);
    // } else if (is_elem(job, QueueingJobs)) {
    } else if (QueueingJobs.find(job->get_id()) != QueueingJobs.end()) {
      // job->set_stop(false); // since it is not running. is this handled by
      // when it is invoked?
      AQJobs.insert({job->get_id(), job});
      QueueingJobs.erase(job->get_id());
      // job->wait_in_iq(current_event_time);
    }
  }
}

bool descending_absolute_loc_sensitivity(MicroserviceJob* A,
                                         MicroserviceJob* B);
// void determine_placements(std::list<MicroserviceJob*> &RunningJobs,
void determine_placement(std::map<int, MicroserviceJob*> &RunningJobs,
                         std::map<int, MicroserviceJob*> &QueueingJobs,
                         std::map<int, MicroserviceJob*> &AQJobs,
                         std::list<MicroserviceJob*> &AllSubmittedJobs,
                         std::list<MicroserviceJob*> &SelectedJobs,
                         std::list<MicroserviceJob*> &IQJobs,
                        //  std::list<Action*> &RunningActions,
                         std::multimap<time_type, ActionID> &end_events,
                         std::multimap<time_type, int> &job_end_times,
                         time_type current_event_time,
                         Cluster* cluster_ptr,
                         ActionDuration* adp
                         ) {
// void determine_placement(std::list<MicroserviceJob*> &RunningJobs,
//                          std::list<MicroserviceJob*> &QueueingJobs,
//                          std::list<MicroserviceJob*> &AQJobs,
//                          std::list<MicroserviceJob*> &AllSubmittedJobs,
//                          std::list<MicroserviceJob*> &SelectedJobs,
//                          std::list<MicroserviceJob*> &IQJobs,
//                         //  std::list<Action*> &RunningActions,
//                          std::list<time_type> &end_events,
//                          std::list<time_type> &job_end_times,
//                          time_type current_event_time,
//                          Cluster* cluster_ptr,
//                          ActionDuration* adp
//                          ) {
/*
for job in all_jobs
	if !job.reserve.empty()
		cluster.release_reserve(job.reserve)
		job.reserve.clear()
		job.release_stop_flag()
		job.release_evict_flag()

for job in job_to_noalloc
	if job in R
		job.set_evict()
	elif job in NA
	 # action queud job 보다 next alloc job 이라는 표현이 더 적절할듯
	 # queue에서 따로 안빼고 그대로 두는게 나을거 같지만 지금은 일단 두자...
		NA.remove(job)
		Q.push(job)
	elif job in Q
		nothing
*/
  for (auto job : AllSubmittedJobs) {
  // clear previous schedule plan
    if (job->get_stop_flag()) {
      job->release_reservation(cluster_ptr);
      job->set_stop(false);
    }
    job->set_evict(false);

// share == 0 cases
    if (!is_elem(job, SelectedJobs)) {
      // if (is_elem(job, RunningJobs)) {
      if (RunningJobs.find(job->get_id()) != RunningJobs.end()) {
        if (job->is_in_wait()) {
          IQJobs.remove(job);
          // RunningJobs.remove(job);
          RunningJobs.erase(job->get_id());
          // QueueingJobs.push_back(job);
          QueueingJobs.insert({job->get_id(), job});
          job->evicted(current_event_time);
        } else {
          job->set_evict(true);
        }
        // job->_status = kYield;
      // } else if (is_elem(job, AQJobs)) {
      } else if (AQJobs.find(job->get_id()) != AQJobs.end()) {
        // AQJobs.remove(job);
        AQJobs.erase(job->get_id());
        // QueueingJobs.push_back(job);
        QueueingJobs.insert({job->get_id(), job});
        job->evicted(current_event_time);
      }
    }
  }
/*
canGPUs <- clusterGPUs

for job in job_to_realloc # NOTE: job with share != 0
	prev_sched = (job in R) ? job.place : empty

	# CHECK: RUN job의 reservation 은 여기서 무시하고 새로 할 것인가?
	# ---> 무시하면 됨. action queud job의 reservation 도 무시하면 됨. 
	# invoker queue는 이전 action 이 끝나야 순간적으로 발생하는 것임. controller가 자료구조로 가지고 있는 할당 계획표( == reservation) 와는  다른 것!!!
	#/ 그렇다면 s_Themis는 제대로 동작하는게 맞아?? --> yes!


	if job.share == prev_sched.size
		canGPUs.remove(prev_sched)

	if job.share < prev_sched.size # NOTE: prev_sched_size != 0 <=> no Q job here
		placement = cluster.find_placement_shrink(prev_sched)
		job.reserve <- placement
		job.set_stop_flag()
		cluster.set_reserve(placement,job)
		canGPUs.remove(placement)
*/

// handle warm cases
  std::vector<Location> canGPUs = cluster_ptr->all_GPUs();
  // std::list<Location> canGPUs(allGPUs.begin(),allGPUs.end());
  for (auto job : SelectedJobs) {
    std::vector<Location> prev_sched;
    // if (is_elem(job, RunningJobs)) {
    if (RunningJobs.find(job->get_id()) != RunningJobs.end()) {
      prev_sched = job->get_locations();
    }
    if (job->get_fair_share() <= prev_sched.size()) {
      determine_warm_placement(prev_sched, cluster_ptr, job, canGPUs);
    }

    // if (job->get_share() == prev_sched.size()) {
    //   for (auto loc : prev_sched) {
    //     auto it = std::find(canGPUs.begin(), canGPUs.end(), loc);
    //     if (it != canGPUs.end()) canGPUs.erase(it);
    //   }
    // }
    // if (job->get_share() < prev_sched.size()) {
    //   std::vector<Location> placement = cluster_ptr->determine_placement(
    //       job->get_id(), job->get_share(), job->is_loc_sensitive(), prev_sched);
    //   // std::vector<Location> placement =
    //   // cluster_ptr->determine_placement(job->get_id(), job->get_share(),
    //   // job->get_loc_sensitivity_wrt_wave(), prev_sched);
    //   job->reserve_move(cluster_ptr, placement);
    //   // job->set_stop(true);
    //   // cluster_ptr->set_reserve_job(job->get_id(), placement);
    //   for (auto loc : placement) {
    //     auto it = std::find(canGPUs.begin(), canGPUs.end(), loc);
    //     if (it != canGPUs.end()) canGPUs.erase(it);
    //   }
    // }
  }

/*
for job in job_to_realloc # NOTE: loop seperatly to place after warm
	prev_sched = (job in R) ? job.place : empty
	if job.share > prev_sched.size

		placement = cluster.find_placement_expand(canGPUs)

		if job in AQ or Q && placement is usable
			RUN
			AQ.remove(job)
			Q.remove(job)
			RUN.push(job)
		else
			job.reserve <- placement
			job.set_stop_flag()
			cluster.set_reserve(placement,job)
			canGPUs.remove(placement)

			if job in Run
				job.set_stop_flag()

			if job in Q
				AQ.push(job)
*/

//// CHECK: check the results with this
  // std::sort(SelectedJobs.begin(),SelectedJobs.end(),descending_absolute_loc_sensitivity);
  // SelectedJobs.sort(descending_absolute_loc_sensitivity);
  SelectedJobs.sort([](const auto& a, const auto& b) {
    return a->get_submit_time() < b->get_submit_time();
  });

  // # NOTE: loop seperatly to place cold after warm
  for (auto job : SelectedJobs) {
    std::vector<Location> prev_sched;
    // if (is_elem(job, RunningJobs)) {
    if (RunningJobs.find(job->get_id()) != RunningJobs.end()) {
      prev_sched = job->get_locations();
      if (prev_sched.size() == 0) {
        job->increase_iq_kick_count();
      }
    }
    if (job->get_share() > prev_sched.size()) {
      determine_cold_placement(
          RunningJobs, QueueingJobs, AQJobs, SelectedJobs,
          IQJobs, end_events, job_end_times, current_event_time,
          cluster_ptr, adp, job, canGPUs);

      // std::vector<Location> placement = cluster_ptr->determine_placement(
      //     job->get_id(), job->get_share(), job->is_loc_sensitive(), canGPUs);
      // // std::vector<Location> placement =
      // // cluster_ptr->determine_placement(job->get_id(), job->get_share(),
      // // job->get_loc_sensitivity_wrt_wave(), canGPUs);
      // for (auto loc : placement) {
      //   auto it = std::find(canGPUs.begin(), canGPUs.end(), loc);
      //   if (it != canGPUs.end()) canGPUs.erase(it);
      // }
      // job->reserve_move(cluster_ptr, placement);
      // if (!is_elem(job, RunningJobs) &&
      //     cluster_ptr->is_available_gpus(placement, job->get_id())) {
      //   job->invoke_iteration(current_event_time, cluster_ptr, adp,
      //                         &RunningActions, &end_events, &job_end_times,
      //                         Cold);
      //   AQJobs.remove(job);
      //   QueueingJobs.remove(job);
      //   RunningJobs.push_back(job);
      //   job->set_stop(false);
      //   job->release_reservation(cluster_ptr);
      // } else {
      //   // job->reserve_move(cluster_ptr, placement);
      //   // cluster_ptr->set_reserve_job(job->get_id(), placement);
      //   // canGPUs.remove(placement);
      //   if (is_elem(job, RunningJobs)) {
      //     job->set_stop(true);
      //   } else if (is_elem(job, QueueingJobs)) {
      //     // job->set_stop(false); // since it is not running. is this handled by when it is invoked?
      //     AQJobs.push_back(job);
      //     QueueingJobs.remove(job);
      //   }
      // }
    }
  }
}


// selected jobs로는 돌고 있지만 만족되지 않은 job들과 해당 job과 노드를 공유하는 job들. and 현재 evict 대상이 아닐것.
// 만족되지 않았다는 어떻게 판단하나? 현재 실행 중인 것과 앞으로 바뀔 것 무엇을 기준으로?
// job이 예약이 걸려있다면, 예약한 자리를 기준으로, 그렇지 않다면 현재 사용 중 위치를 기준으로.
// 그렇다면, job 은 running job list가 아니라 aq에 있을수도 있다.
// slelected jobs <- if job.prev_sched is bad-loc for job in running_job(non-evict-flag) + aq

// 대상 GPU는 selected jobs의 GPU와 evict 할 job 이 들고 있는 본인 외 예약이 없는 GPU와 
// 이사가려는 job의 버릴 위치도 포함?
// usable GPU 까지 포함.
// avail_GPUs <- 

void shuffle_placements(std::map<int, MicroserviceJob*>& RunningJobs,
                       std::map<int, MicroserviceJob*>& QueueingJobs,
                       std::map<int, MicroserviceJob*>& AQJobs,
                      //  std::list<MicroserviceJob*>& AllSubmittedJobs,
                       std::list<MicroserviceJob*>& SelectedJobs,
                       std::list<MicroserviceJob*>& IQJobs,
                      //  std::list<Action*>& RunningActions,
                       std::multimap<time_type, ActionID>& end_events,
                       std::multimap<time_type, int>& job_end_times,
                       time_type current_event_time, Cluster* cluster_ptr,
                       ActionDuration* adp) {
// void shuffle_placements(std::list<MicroserviceJob*>& RunningJobs,
//                        std::list<MicroserviceJob*>& QueueingJobs,
//                        std::list<MicroserviceJob*>& AQJobs,
//                       //  std::list<MicroserviceJob*>& AllSubmittedJobs,
//                        std::list<MicroserviceJob*>& SelectedJobs,
//                        std::list<MicroserviceJob*>& IQJobs,
//                       //  std::list<Action*>& RunningActions,
//                        std::list<time_type>& end_events,
//                        std::list<time_type>& job_end_times,
//                        time_type current_event_time, Cluster* cluster_ptr,
//                        ActionDuration* adp) {
  std::vector<Location> avail_GPUs = cluster_ptr->get_usable_GPUs();
  // for (auto job : SelectedJobs) {
  //   std::vector<Location> locs = job->get_locations();
  //   avail_GPUs.insert(avail_GPUs.end(), locs.begin(), locs.end());
  // }

  std::vector<int> nodes;
  // std::vector<int> jobs;
  for (auto j : SelectedJobs) {
    std::vector<int> node_idx;// = j->get_node_idx();
    if (!j->get_stop_flag()) {
      node_idx = j->get_node_idx();
    } else {
      std::vector<Location> locs = j->get_reserve_locations();
      for (auto loc : locs) {
        node_idx.push_back(loc.node_idx);
      }
    }
    nodes.insert(nodes.end(), node_idx.begin(), node_idx.end());
  }
  std::sort(nodes.begin(), nodes.end());
  nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
  // TODO: need to fix this
  // cluster_ptr->get_gpu_processes(nodes, jobs);

  std::list<MicroserviceJob*> jobs_to_shuffle;
  oss << "Job to shuffle:\n";
  oss << "Running jobs:\n";
  for (auto [id, j] : RunningJobs) {
    // if (!j->get_evict_flag() && !j->get_stop_flag() && is_elem(j->get_id(), jobs)) {
    // if (!j->get_evict_flag() && is_elem(j->get_id(), jobs)) {
    if (!j->get_evict_flag()) {
      bool is_side = false;
      std::vector<Location> j_locs;
      if (j->get_stop_flag()) {
        j_locs = j->get_reserve_locations();
      } else {
        j_locs = j->get_locations();
      }
      for (auto loc : j_locs) {
        if (is_elem(loc.node_idx,nodes)) {
          is_side = true;
          break;
        }
      }
      if (is_side) {
        jobs_to_shuffle.push_back(j);
        avail_GPUs.insert(avail_GPUs.end(), j_locs.begin(), j_locs.end());
        oss << "Job" << j->get_id() << "[" << j->get_fair_share() << "]";
        for (auto loc : j_locs) {
          oss << " " << loc;
        }
        oss << std::endl;
      }
      // jobs_to_shuffle.push_back(j);
      // std::vector<Location> j_locs = j->get_locations();
      // avail_GPUs.insert(avail_GPUs.end(), j_locs.begin(), j_locs.end());
      // // is the insert safe? will value used from the j_locs? not reference? The reference can live out from loop?
      // oss << "Job" << j->get_id() << "[" << j->get_fair_share() << "]";
      // for (auto loc : j_locs) {
      //   oss << " " << loc;
      // }
      // oss << std::endl;
    }
  }
  oss << "AQ jobs:\n";
  for (auto [id, j] : AQJobs) {
    // if (j->get_stop_flag() && is_elem(j->get_id(), jobs)) {
    if (j->get_stop_flag()) {
      bool is_side = false;
      std::vector<Location> j_locs = j->get_reserve_locations();
      for (auto loc : j_locs) {
        if (is_elem(loc.node_idx, nodes)) {
          is_side = true;
          break;
        }
      }
      if (is_side) {
        jobs_to_shuffle.push_back(j);
        avail_GPUs.insert(avail_GPUs.end(), j_locs.begin(), j_locs.end());
        oss << "Job" << j->get_id() << "[" << j->get_fair_share() << "]";
        for (auto loc : j_locs) {
          oss << " " << loc;
        }
        oss << std::endl;
      }
    }
  }
  put_log(oss,kDebug);

  int total_req = 0;
//// CHECK: 전처리 필요? 이전 reservation을 지운다거나?
  for (auto job : jobs_to_shuffle) {
    total_req += job->get_fair_share();
    if (job->get_stop_flag()) {
      job->release_reservation(cluster_ptr);
      job->set_stop(false);
    }
    // job->set_evict(false);
  }

  // std::sort(SelectedJobs.begin(), SelectedJobs.end(),
  //           descending_absolute_loc_sensitivity);
  jobs_to_shuffle.sort(descending_absolute_loc_sensitivity);

  oss << "[Debug] Shuffle with " << jobs_to_shuffle.size() << "jobs [" << total_req
      << "reqs] with " << avail_GPUs.size() << " GPUs\n";
  for (auto loc : avail_GPUs) {
    oss << loc << '\n';
  }
  put_log(oss, kDebug);

  for (auto job : jobs_to_shuffle) {
    determine_cold_placement(
        RunningJobs, QueueingJobs, AQJobs, jobs_to_shuffle,
        IQJobs, end_events, job_end_times, current_event_time,
        cluster_ptr, adp, job, avail_GPUs);
  }
}


// id matched job found in running list, get pointer of it
std::list<MicroserviceJob*>::iterator get_job_ptr_iterator(
    int job_id, std::list<MicroserviceJob*>& Running_Job_list) {
  std::list<MicroserviceJob*>::iterator it_job_ptr;
  for (it_job_ptr = Running_Job_list.begin();
       it_job_ptr != Running_Job_list.end(); ++it_job_ptr) {
    if ((*it_job_ptr)->get_id() == job_id) {
      return it_job_ptr;
    }
  }
  oss << "[Error] not matching job in Running job list with id " << job_id
      << std::endl;
  put_log(oss, kDebug);
  assert(false);
}

bool descending_absolute_loc_sensitivity(MicroserviceJob* A,
                                         MicroserviceJob* B) {
  int i, j;
  if (loc_sd_rule == kPhase) {
    i = A->get_loc_sensitivity_wrt_wave();
    j = B->get_loc_sensitivity_wrt_wave();
  } else {
    i = A->is_loc_sensitive();
    j = B->is_loc_sensitive();
  }
  if (i < 0) i = 0 - i;
  if (j < 0) j = 0 - j;
  return (i > j);  // decreasing in locality sensitivity
}

void locality_shuffle(std::list<MicroserviceJob*>& Running_Job_list,
                      std::vector<MicroserviceJob*>& valid_targets,
                      double threshold, Cluster* cluster_ptr) {
  std::vector<MicroserviceJob*> targets;
  std::vector<int> nodes;
  std::vector<int> jobs;
  std::vector<Location> avail_locs;
  std::vector<NodeAvail> node_cap;

  avail_locs = cluster_ptr->get_idle_locations();

  // add jobs to targets, which use same node with targets
  for (auto j : valid_targets) {
    std::vector<int> node_idx = j->get_node_idx();
    nodes.insert(nodes.end(), node_idx.begin(), node_idx.end());
    // is this insert safe? will value used from the node_idx? not reference?
  }
  std::sort(nodes.begin(), nodes.end());
  nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
  // find jobs whose resource is belong to the "nodes", and they will be the targets
  cluster_ptr->get_gpu_processes(nodes, jobs);

  for (auto j : Running_Job_list) {
    if (is_elem(j->get_id(), jobs)) {
      targets.push_back(j);
      std::vector<Location> j_locs = j->get_locations();
      avail_locs.insert(avail_locs.end(), j_locs.begin(), j_locs.end());
      // is the insert safe? will value used from the j_locs? not reference? The reference can live out from loop?
    }
  }

  // for (auto j : valid_targets) {
  //   targets.push_back(j);
  //   std::vector<Location> j_locs = j->get_locations();
  //   avail_locs.insert(avail_locs.end(),j_locs.begin(),j_locs.end());
  // }

  // sort avail_locs in decreasing order of # avail GPUs
  for (auto loc : avail_locs) {
    std::vector<NodeAvail>::iterator it;
    for (it = node_cap.begin(); it != node_cap.end(); ++it) {
      if (loc.node_idx == it->node_idx) {
        break;
      }
    }
    if (it == node_cap.end()) {
      std::vector<int> gpus = {loc.gpu_idx};
      NodeAvail node = {loc.node_idx, gpus};
      node_cap.push_back(node);
    } else {
      (it->gpu_idcs).push_back(loc.gpu_idx);
      // it->second += 1; // is this possible..?
    }
  }

  // sort target jobs in loc-sensitivity and locality scocre
  std::sort(targets.begin(), targets.end(),
            descending_absolute_loc_sensitivity);
  // std::sort(targets.begin(),targets.end(),lrsf);
  // reserve resources to jobs and make up stop signal
  for (auto j : targets) {
    sort(node_cap.begin(), node_cap.end(), descending_n_gpu);

    oss << "[Debug] Job" << j->get_id() << " (" << j->get_share()
        << ") is a target job" << std::endl;
    oss << "[Debug] node capacity: ";
    for (auto n : node_cap) {
      oss << n.node_idx << '(' << n.gpu_idcs.size() << ") ";
    }
    oss << std::endl;
    put_log(oss, kDebug);

    int req = j->get_share();
    // int idx = 0;
    std::vector<Location> allocs;
    if (j->get_loc_sensitivity_wrt_wave() > 0) {
      allocs = find_placement_gather(req, node_cap);
    } else if (j->get_loc_sensitivity_wrt_wave() == 0) {
      allocs = find_placement_fill_frag(req, node_cap);
      // allocs = find_placement_gather(req, node_cap);
    } else {
      allocs = find_placement_scatter(req, node_cap);
    }
    j->reserve_move(cluster_ptr, allocs);
  }
}

// (service time * GPU req) / queue time
// if queue time is zero, it loses and sort in just (service * gpu_req)
bool proportion_compare(MicroserviceJob* A, MicroserviceJob* B) {
  double i;
  double j;
  time_type qA = A->get_online_jct() - A->get_online_run();
  time_type qB = B->get_online_jct() - B->get_online_run();

  if (qA == 0 && qB == 0) {
    i = A->get_online_run() * A->get_gpu_req();
    j = B->get_online_run() * B->get_gpu_req();
  } else if (qA != 0 && qB != 0) {
    i = (A->get_online_run() * A->get_gpu_req()) / (double)qA;
    j = (B->get_online_run() * B->get_gpu_req()) / (double)qB;
    if (A->get_online_run() == 0 && B->get_online_run() == 0) {
      i = A->get_gpu_req() / (double)qA;
      j = B->get_gpu_req() / (double)qB;
    }
  } else if (qA == 0) {
    return false;
  } else if (qB == 0) {
    return true;
  }
  if (i == j) {
    i = A->get_gpu_req();
    j = B->get_gpu_req();
  }
  return (i < j);
}

// compare_gpu_req_and_submit
bool ascend_gpu_req_descend_submit_t(const MicroserviceJob* jobA,
                                     const MicroserviceJob* jobB) {
  int i = jobA->get_gpu_req();
  int j = jobB->get_gpu_req();
  if (i == j) {
    // TODO: sort in what?
    int k = jobA->get_submit_time();
    int l = jobB->get_submit_time();
    return (k > l);  // descending order of submit time
  } else {
    return (i < j);  // ascending order in gpu req
  }
  // return ( jobA->get_gpu_req() < jobB->get_gpu_req() ); // sort in ascending
}

bool ascend_gpu_req_remainder_descend_submit_t(const MicroserviceJob* jobA,
                                               const MicroserviceJob* jobB) {
  int i = jobA->get_gpu_req() - jobA->get_share();
  int j = jobB->get_gpu_req() - jobB->get_share();
  if (i == j) {
    int k = jobA->get_submit_time();
    int l = jobB->get_submit_time();
    return (k > l);  // descending order of submit time
  } else {
    return (i < j);  // ascending order in gpu req
  }
  // return ( jobA->get_gpu_req() < jobB->get_gpu_req() ); // sort in ascending
}

/******************************************************************************
 * s-tiresias utility functions
 ******************************************************************************/
// enqueue function for two level queue
void enqueue_job(std::list<MicroserviceJob*> &q1, std::list<MicroserviceJob*> &q2, MicroserviceJob* j, time_type threshold) {
  if ((j->get_service_time()) > threshold) {
    q2.push_back(j);
    // q2.sort(start_time_compare);
  } else {
    q1.push_back(j);
    // q1.sort(start_time_compare);
  }
}
// dequeue function for two level queue
void dequeue_job(std::list<MicroserviceJob*> &q1, std::list<MicroserviceJob*> &q2, MicroserviceJob* j) {
  q1.remove(j);
  q2.remove(j);
}
// rearrange function for two level queue
void rearrange_queue(std::list<MicroserviceJob*> &q1, std::list<MicroserviceJob*> &q2, time_type threshold) {
  std::list<MicroserviceJob*>::iterator it;
  std::vector<std::list<MicroserviceJob*>::iterator> del;
  for (it = q1.begin(); it != q1.end(); ++it) {
    if (((*it)->get_service_time()) > threshold) {
      q2.push_back(*it);
      del.push_back(it);
    }
  }
  for (auto elem : del) {
    q1.erase(elem);
  }
  q1.sort(ascend_start_t_ascend_submit_t<MicroserviceJob>);
  q2.sort(ascend_start_t_ascend_submit_t<MicroserviceJob>);
}

/******************************************************************************
 * s-themis utility function(s)
 ******************************************************************************/
time_type upcomming_event_time(std::list<time_type>& start_event_list,
                               std::list<time_type>& end_event_list,
                               std::list<time_type>& lease_end_events,
                               time_type current_time) {
  time_type time = -1;
  // TODO: change assert to cpp fault handler?
  assert(end_event_list.front() >= 0 && start_event_list.front() >= 0);
  if (end_event_list.empty() && start_event_list.empty()) {
    if (time == current_time) {
      oss << "[Error] Program reached unimplemented area in "
             "main.upcomming_event_time().\n";
      put_log(oss, kDebug);
    }
    // this can happen when all jobs in cluster end at the same time but queue
    // is remain
    time = current_time;
  }
  else {
    time_type start_t = LONG_LONG_MAX;
    time_type end_t = LONG_LONG_MAX;
    time_type lease_t = LONG_LONG_MAX;
    if (!start_event_list.empty())
      start_t = start_event_list.front();
    if (!end_event_list.empty())
      end_t = end_event_list.front();
    if (!lease_end_events.empty())
      lease_t = lease_end_events.front();
    time = std::min({start_t, end_t, lease_t});
    assert(time >= current_time);
    if (time == start_t) {
      start_event_list.pop_front();
      oss << "[Event] start\n";
    }
    if (time == end_t) {
      end_event_list.pop_front();
      oss << "[Event] end\n";
    }
    if (time == lease_t) {
      lease_end_events.pop_front();
      oss << "[Event] lease\n";
    }
  }
  put_log(oss, kInfo);
  return time;
}

/******************************************************************************
 * s-alpha utility function(s)
 ******************************************************************************/
bool double_isSame(double left, double right, double alpha) {
  double e = std::numeric_limits<double>::epsilon();
  if (abs(left - right) < e) {
    return true;
  } else {
    return false;
  }
}

/******************************************************************************
 * Job(Gang) schedulers utility function(s)
 ******************************************************************************/
bool try_count_compare(Job* A, Job* B) {
  int i = A->get_try_count();
  int j = B->get_try_count();
  if (i == j) {
    time_type k = A->get_submit_time();
    time_type l = B->get_submit_time();
    oss << "[Debug] Comp sub" << A->get_id() << '(' << k << ") " << B->get_id()
        << '(' << l << ')' << std::endl;
    put_log(oss, kDebug);
    return (k < l);  // increasing in submit time
  } else {
    oss << "[Debug] Comp try" << A->get_id() << '(' << i << ") " << B->get_id()
        << '(' << j << ')' << std::endl;
    put_log(oss, kDebug);
    return (i > j);  // decreasing in try count
  }
}


#endif