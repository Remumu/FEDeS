#include <iostream>
#include <list>
#include <string>
#include <algorithm>
#include <assert.h>
#include <math.h>
#include <fstream>

#include "config.h"
#include "action.h"
#include "job.h"
#include "cluster.h"
#include "util.h"
#include "schedulers/util.h"

// #include "schedulers/s_tiresias.cpp"
// #include "schedulers/s_themis_fix_event.cpp"
#include "schedulers/sb_alpha_fix_event.cpp"
// #include "schedulers/s_srtf_fix_event.cpp"

// #include "schedulers/delay.cpp"
// #include "schedulers/fifo.cpp"
#include "schedulers/themis.cpp"
#include "schedulers/srsf.cpp"
#include "schedulers/las.cpp"


// const int kPeriodN = 5;
// const int kPeriodN = 5;

// const int kMinGuarantee = 2;
const unsigned kMinGuarantee = 1;

// const double kAdmissionThreshold = 2.0;

// enum RoundRule {kRR, kLas, kProp};

LogLevel verbose;
std::ostringstream oss;
// bool ignore_scale_factor; // deprecated
bool ignore_framework;
bool ignore_init;
bool ignore_wave;


LocSensDivRule loc_sd_rule = kNN;

void print_usage() {
  oss << "Error: Insufficient options to run\n"
      << "OPTIONS\n"
      << "    -v NUM, --verbose NUM\n"
      << "        NUM is verbose level among 0, 1, 2.\n"
      << "        Bigger number is more verbose.\n"
      << "        Default is 0\n"
      << "    -t FILE, --trace FILE\n"
      << "        This option is necessary\n"
      << "    -c FILE, --cluster FILE\n"
      << "        FILE is topology file.\n"
      << "        This option is necessary\n"
      << "    -s VAL, --schedule VAL\n"
      << "        VAL is one of fifo, grml, srtf, srsf, las, delay, themis,\n"
      << "                      FIFO, GRML, SRTF, SRSF, LAS, DELAY, THEMIS.\n"
      << "        Default is FIFO\n"
      << "    -q NUM, --queue-threshold NUM\n"
      << "        NUM is threshold of multi level queue in millisecond unit.\n"
      << "        This option only used with LAS.\n"
      << "        Default value is 11520000, which is 3.2 hours GPU time.\n"
      << "    --admission\n"
      << "        Activate admission control\n"
      << "        Default is disabled\n"
      << "    --locality-interval NUM\n"
      << "        Time interval for periodic locality improvement for grml\n"
      << "        NUM in minute unit\n"
      << "        Default is 10 minutes\n"
      << "        0 value disable locality improving\n"
      << "    --locality-threshold NUM\n"
      << "        Threshold to activate locality improving for grml\n"
      << "        Default is 1.5\n"
      << "    --round-length NUM\n"
      << "        Time interval for round-robin for grml\n"
      << "        NUM in minute unit\n"
      << "        Default is 60 minutes\n"
      << "        0 value disable round-robin\n"
      << "    --round-rule VAL\n"
      << "        Change rule for round of grml\n"
      << "        VAL is one of rr, las, prop\n"
      << "        Default is rr\n"
      // << "    --round-rule-las\n"
      // << "        Change rule for round-robin to the least attained service time\n"
      // << "        Without this option, the default behavior is conventional round-robin\n"
      << "    --perf-ratio VAL\n"
      << "        Resource ratio for performance optimization\n"
      << "        VAL is double type variable in range [0, 1]\n"
      << "        Setting 0 for fair scheduling, 1 for performance scheduling\n"
      << "    --perf-knob VAL\n"
      << "        This knob allows VAL times resource requiring job to be scheduled for performance.\n"
      << "        Minimum value is 1. Increasing VAL may cause jobs to run in phase.\n"
      << std::endl;
  put_log(oss,kCore);
}
void print_args(bool admission_control, ScheduleType scheduling) {
  // value options
  if (admission_control) {
    oss << "Admission threshold: " << kAdmissionThreshold << std::endl;
  } else {
    oss << "Admission control is not used" << std::endl;
  }
  if (scheduling < FIFO) {
    oss << "Minimum guarantee GPU: " << kMinGuarantee << std::endl;
  }
  if (scheduling == GRML)
    oss << "Wave option: fair_share" << std::endl;
  else if (scheduling == GRML_2P)
    oss << "Wave option: power of 2" << std::endl;
  else if (scheduling == GRML_DIV)
    oss << "Wave option: divisor" << std::endl;
  else if (scheduling == GRML_2M)
    oss << "Wave option: multiply of 2" << std::endl;
  if (ignore_framework) {
    oss << "Ignore throughput difference from framework" << std::endl;
  }
  put_log(oss,kCore);
}

// compute coefficient of variation
double calcCV(std::vector<double>& vec) {
  double mean(0), sd(0);
  int len = vec.size();
  for (double num : vec) {
    mean += num;
  }
  mean /= len;
  for (double num : vec) {
    sd += ((mean - num) * (mean - num));
  }
  sd /= len;
  sd = sqrt(sd);
  return sd/mean;
}

template<class T>
void compute_and_set_irt_rank(std::unordered_map<int, T> &jobs) {
  std::vector<std::pair<int, time_type> > irts;
  for (auto& [id, job] : jobs) {
    irts.push_back(std::make_pair(job.get_id(), job.get_irt()));
  }

  std::sort(irts.begin(), irts.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });

  for (size_t i = 0; i < irts.size(); ++i) {
    jobs.at(irts[i].first).set_rank_irt(i + 1);
  }
}

void write_record_file(std::unordered_map<int, MicroserviceJob>& jobs,
                       std::string rc_file_name) {
  std::ofstream record_file(rc_file_name, std::ios::out);
  int last_id = jobs.size() + 1;
  record_file << "[";
  for (const auto& [id, jptr] : jobs) {
    // if (jptr.get_model() == VGG19 || jptr.get_model() == VGG19_Cifar) {
    if (true) {
      record_file << "{\"id\": " << jptr.get_id() << ','
                  << "\"model\": " << jptr.get_model() << ','
                  << "\"l_worker\": " << jptr.get_gpu_req() << ','
                  << "\"submit_time\": " << jptr.get_submit_time() << ','
                  << "\"start_time\": " << jptr.get_start_time_stamp() << ','
                  << "\"record\": [";
      auto records = jptr.get_fair_records();
      for (auto it = records.begin(); it != records.end();) {
        auto record = *it;
        record_file << "{\"time\": " << record.time_stamp
                    << ",\"node_usg\": " << record.uniq_node
                    << ",\"gpu_usg\": " << record.gpu_usg << "}";
        if (++it != records.end()) {
          record_file << ',';
        }
      }
      record_file << "]}";
      if (jptr.get_id() != last_id) {
        record_file << ',';
      }
    }
  }
  record_file << "]";
  record_file.close();
}
void compute_and_print_statistics(
    std::unordered_map<int, MicroserviceJob>& jobs,
    time_type makespan, double avg_gpu_load, double avg_frag_w_q,
    double effi_gpu_load, double fair_gpu_load) {
  double d_fair(0), d_fair_sq(0),
         d_fairN(0), d_fairN_sq(0),
         mean_fair(0), mean_fair2(0),
         mean_fair3(0), mean_fair4(0), mean_fair5(0),
         maxF(0), minF(1000), fairnessT,
         NmaxF(0), NminF(1000), fairnessN,
         mean_loc1(0), mean_loc2(0), mean_loc3(0),
         avg_c(0), avg_w(0), avg_g(0), avg_s(0),
         mean_n_avg(0);
  time_type sum_jct(0);
  double avg_jct;
  size_t num_jobs = jobs.size();
  std::vector<double> nums, nums2, nums3, nums4, nums5;
  double cv, cv2, cv3, cv4, cv5;

  for (const auto& [id, job] : jobs) {
    d_fairN += fabs(job.get_fairness2() - 1);
    d_fairN_sq += fabs(job.get_fairness2() - 1) * fabs(job.get_fairness2() - 1);
    d_fair += fabs(job.get_fairness() - 1);
    d_fair_sq += fabs(job.get_fairness() - 1) * fabs(job.get_fairness() - 1);

    mean_fair += job.get_fairness();
    mean_fair2 += job.get_fairness2();
    mean_fair3 += job.get_fairness3();
    mean_fair4 += job.get_fairness4();
    mean_fair5 += job.get_fairness5();

    mean_loc1 += job.get_locality_dist();
    mean_loc2 += job.get_locality_bit();
    mean_loc3 += job.get_locality_con();

    avg_c += job.get_cold_count();
    avg_w += job.get_avg_wave();
    avg_g += job.get_avg_gpu_usg();
    avg_s += job.get_sfl_count();

    mean_n_avg += job.get_n_avg();
    sum_jct += job.get_end_time() - job.get_submit_time();

    if (job.get_fairness() < minF) minF = job.get_fairness();
    if (job.get_fairness() > maxF) maxF = job.get_fairness();
    if (job.get_fairness2() < NminF) NminF = job.get_fairness2();
    if (job.get_fairness2() > NmaxF) NmaxF = job.get_fairness2();

    nums.push_back(job.get_fairness());
    nums2.push_back(job.get_fairness2());
    nums3.push_back(job.get_fairness3());
    nums4.push_back(job.get_fairness4());
    nums5.push_back(job.get_fairness5());
  }

  d_fair = d_fair / num_jobs;
  d_fair_sq = sqrt(d_fair_sq / num_jobs);
  d_fairN = d_fairN / num_jobs;
  d_fairN_sq = sqrt(d_fairN_sq / num_jobs);

  mean_fair = mean_fair / num_jobs;
  mean_fair2 = mean_fair2 / num_jobs;
  mean_fair3 = mean_fair3 / num_jobs;
  mean_fair4 = mean_fair4 / num_jobs;
  mean_fair5 = mean_fair5 / num_jobs;

  mean_loc1 = mean_loc1 / num_jobs;
  mean_loc2 = mean_loc2 / num_jobs;
  mean_loc3 = mean_loc3 / num_jobs;

  avg_c /= num_jobs;
  avg_w /= num_jobs;
  avg_g /= num_jobs;
  avg_s /= num_jobs;

  mean_n_avg /= num_jobs;
  avg_jct = sum_jct / (double)num_jobs;

  fairnessT = maxF / minF;
  fairnessN = NmaxF / NminF;

  cv = calcCV(nums);
  cv2 = calcCV(nums2);
  cv3 = calcCV(nums3);
  cv4 = calcCV(nums4);
  cv5 = calcCV(nums5);

  oss << "\nMakespan," << makespan / 1000 << ",avgJCT," << avg_jct / 1000
      << ",FairnessT," << fairnessT << ",avg_gpu_load," << avg_gpu_load
      << ",cold_per_job," << avg_c << ",avg_wave," << avg_w
      << ",avg_effi_gpu_load," << effi_gpu_load
      << ",avg_fair_gpu_load," << fair_gpu_load << ",avg_gpu_usg," << avg_g
      << ",mean_n_avg," << mean_n_avg << ",avg_shuffle," << avg_s
      << ",maxFairT," << maxF << ",minFairT," << minF << ",dfairT," << d_fair
      << ",dfairSq," << d_fair_sq << ",FairnessN," << fairnessN
      << ",maxFairN," << NmaxF << ",minFairN," << NminF
      << ",dfairN," << d_fairN << ",dfairNSq," << d_fairN_sq
      << ",avg_frag_w_q," << avg_frag_w_q
      << std::endl
      << "Fairness\n"
      << "Mm,1/n,1/n-Down,GPU_load,GPU_share\n"
      << mean_fair << "," << mean_fair2 << "," << mean_fair3 << ","
      << mean_fair4 << "," << mean_fair5 << std::endl
      << "Fairness CV\n"
      << cv << ',' << cv2 << ',' << cv3 << ',' << cv4 << ',' << cv5 << std::endl
      << "Locality\n"
      << "dist,bit,con\n"
      << mean_loc1 << ',' << mean_loc2 << ',' << mean_loc3 << std::endl;
  put_log(oss,kQuiet);
}

// consider return to struct??
void compute_and_print_statistics(std::unordered_map<int, Job> &jobs,
                                  time_type makespan, double avg_gpu_load, double avg_frag_w_q) {
  double d_fair(0), d_fair_sq(0),
         d_fairN(0), d_fairN_sq(0),
         mean_fair(0), mean_fair2(0),
         mean_fair3(0), mean_fair4(0), mean_fair5(0),
         maxF(0), minF(1000), fairnessT,
         NmaxF(0), NminF(1000), fairnessN,
         mean_loc1(0), mean_loc2(0),
         avg_y(0), mean_n_avg(0);
  time_type sum_jct(0);
  double avg_jct;
  size_t num_jobs = jobs.size();
  std::vector<double> nums, nums2, nums3, nums4, nums5;
  double cv, cv2, cv3, cv4, cv5;

  for (const auto& [id, job] : jobs) {
    d_fair += fabs(job.get_fairness() - 1);
    d_fair_sq += fabs(job.get_fairness() - 1) * fabs(job.get_fairness() - 1);
    d_fairN += fabs(job.get_fairness2() - 1);
    d_fairN_sq += fabs(job.get_fairness2() - 1) * fabs(job.get_fairness2() - 1);
    mean_fair += job.get_fairness();
    mean_fair2 += job.get_fairness2();
    mean_fair3 += job.get_fairness3();
    mean_fair4 += job.get_fairness4();
    mean_fair5 += job.get_fairness5();
    mean_loc1 += job.get_locality_dist();
    mean_loc2 += job.get_locality_bit();
    avg_y += job.get_yield_count();
    mean_n_avg += job.get_n_avg();
    sum_jct += job.get_end_time() - job.get_submit_time();

    if (job.get_fairness() < minF) minF = job.get_fairness();
    if (job.get_fairness() > maxF) maxF = job.get_fairness();
    if (job.get_fairness2() < NminF) NminF = job.get_fairness2();
    if (job.get_fairness2() > NmaxF) NmaxF = job.get_fairness2();

    nums.push_back(job.get_fairness());
    nums2.push_back(job.get_fairness2());
    nums3.push_back(job.get_fairness3());
    nums4.push_back(job.get_fairness4());
    nums5.push_back(job.get_fairness5());
  }

  d_fair = d_fair / num_jobs;
  d_fair_sq = sqrt(d_fair_sq / num_jobs);
  d_fairN = d_fairN / num_jobs;
  d_fairN_sq = sqrt(d_fairN_sq / num_jobs);

  mean_fair = mean_fair / num_jobs;
  mean_fair2 = mean_fair2 / num_jobs;
  mean_fair3 = mean_fair3 / num_jobs;
  mean_fair4 = mean_fair4 / num_jobs;
  mean_fair5 = mean_fair5 / num_jobs;

  mean_loc1 = mean_loc1 / num_jobs;
  mean_loc2 = mean_loc2 / num_jobs;
  avg_y /= num_jobs;
  mean_n_avg /= num_jobs;
  avg_jct = sum_jct / (double)num_jobs;

  fairnessT = maxF / minF;
  fairnessN = NmaxF / NminF;

  cv = calcCV(nums);
  cv2 = calcCV(nums2);
  cv3 = calcCV(nums3);
  cv4 = calcCV(nums4);
  cv5 = calcCV(nums5);

  oss << "\nMakespan," << makespan / 1000 << ",avgJCT," << avg_jct / 1000
      << ",FairnessT," << fairnessT << ",avg_gpu_load," << avg_gpu_load
      << ",yield_per_job," << avg_y << ",mean_n_avg," << mean_n_avg
      << ",maxFairT," << maxF << ",minFairT," << minF << ",dFair," << d_fair
      << ",dFairSq," << d_fair_sq << ",FairnessN," << fairnessN << ",maxFairN,"
      << NmaxF << ",minFairN," << NminF << ",dfairN," << d_fairN << ",dfairNSq,"
      << d_fairN_sq << ",avg_frag_w_q," << avg_frag_w_q << std::endl
      << "Fairness\n"
      << "Mm,1/n,1/n-Down,GPU_load,GPU_share\n"
      << mean_fair << "," << mean_fair2 << "," << mean_fair3 << ","
      << mean_fair4 << "," << mean_fair5 << std::endl
      << "Fairness CV\n"
      << cv << ',' << cv2 << ',' << cv3 << ',' << cv4 << ',' << cv5 << std::endl
      << "Locality\n"
      << "dist,bit\n"
      << mean_loc1 << ',' << mean_loc2 << std::endl;
  put_log(oss, kQuiet);
}

/// CHECK: consider passing arguments as struct to scheduling functions
/*
struct Args {
  // Verbose verbose;
  // bool ignore_framework = false;
  // bool ignore_init = false;
  // bool ignore_wave = false;

  std::string trace_file = "";
  std::string topology_file = "";
  std::string action_duration_file = "";

  // std::string rc_file_name = ""; // deprecated
  std::string sim_log_dir = "";
  std::string sched_name = "";
  std::string ls_std_name = "";

  ScheduleType scheduling = FIFO;
  bool admission_control = false;

  time_type queue_threshold = 11520000; // 48m x 4GPU
  int promote_knob = 0;
  time_type gang_round_length = 10*60*1000;

  time_type evolve_interval = 10*60*1000;
  double relocate_threshold = 1.5;

  time_type round_length = 60*60*1000;
  RoundRule round_rule = kRR;

  double perf_schedule_ratio = 1; // range from 0 to 1.
  double perf_accept_knob = 1; // range should be studied in {1, 1.5, 2, 2.5, 3}

  double alpha = 1.0;

  double s_tms_filter = 0.8;
  time_type s_tms_lease_len = 10*60*1000;
  time_type s_trs_qtrsd = 60*60*1000;

};
*/
int main(int argc, char** argv) {
  // std::ios::sync_with_stdio(false);
  // command line argument control
  verbose = LogLevel::kCore;
  // ignore_scale_factor = false; // deprecated
  ignore_framework = false;
  ignore_init = false;
  ignore_wave = false;

  std::string trace_file = "";
  std::string topology_file = "";
  std::string action_duration_file = "";

  std::string rc_file_name = ""; // deprecated
  std::string sim_log_dir = "";
  std::string sched_name = "";
  std::string ls_std_name = "";

  ScheduleType scheduling = FIFO;
  bool admission_control = false;

  time_type queue_threshold = 11520000; // 48m x 4GPU
  int promote_knob = 0;
  time_type gang_round_length = 10*60*1000;

  time_type evolve_interval = 10*60*1000;
  double relocate_threshold = 1.5;

  // time_type round_length = 60*60*1000;
  // RoundRule round_rule = kRR;

  // double perf_schedule_ratio = 1; // range from 0 to 1.
  // double perf_accept_knob = 1; // range should be studied in {1, 1.5, 2, 2.5, 3}

  double alpha = 1.0;

  // double s_tms_filter = 0.8;
  // time_type s_tms_lease_len = 10*60*1000;
  // time_type s_trs_qtrsd = 60*60*1000;

  // LocSensDivRule loc_sd_rule = kNN;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--admission") {
      admission_control = true;
    // } else if (arg == "--zero") {
    //   ignore_scale_factor = true; // deprecated
    // } else if (arg == "--round-rule-las") {
    //   round_rule = kLas;
    } else if (arg == "--ignore-framework") {
      ignore_framework = true;
    } else if (arg == "--ignore-init") {
      ignore_init = true;
    } else if (arg == "--ignore-wave") {
      ignore_wave = true;
    } else if (i+1 != argc) {
      std::string val = argv[i+1];
      i += 1;
      if (arg == "-v" || arg == "--verbose") {
        verbose = static_cast<LogLevel>(stoi(val));
      } else if (arg == "-t" || arg == "--trace") {
        trace_file = val;
      } else if (arg == "-c" || arg == "--cluster") {
        topology_file = val;
      } else if (arg == "-ad" || arg == "--action_duration") {
        action_duration_file = val;
      } else if (arg == "-s" || arg == "--schedule") {
        sched_name = val;
        if (val == "grml" || val == "GRML") {
          scheduling = GRML;
        } else if (val == "grml-2p" || val == "GRML_2P") {
          scheduling = GRML_2P;
        } else if (val == "grml-div" || val == "GRML_DIV") {
          scheduling = GRML_DIV;
        } else if (val == "grml-2m" || val == "GRML_2M") {
          scheduling = GRML_2M;
        } else if (val == "grdl-opt" || val == "GRDL_OPT") {
          scheduling = GRDL_OPT;
        } else if (val == "grdl-lsm" || val == "GRDL_LSM") {
          scheduling = GRDL_LSM;
        } else if (val == "s-alpha-zero" || val == "S_ALPHA_ZERO") {
          scheduling = S_ALPHA_ZERO;
        } else if (val == "grdl-srsf" || val == "GRDL_SRSF") {
          scheduling = GRDL_SRSF;
        } else if (val == "s-alpha-one" || val == "S_ALPHA_ONE") {
          scheduling = S_ALPHA_ONE;
        } else if (val == "s-mm" || val == "S_MM") {
          scheduling = S_MM;
        } else if (val == "s-mm-cp" || val == "S_MM_CP") {
          scheduling = S_MM_CP;
        } else if (val == "s-themis" || val == "S_THEMIS") {
          scheduling = S_THEMIS;
        } else if (val == "s-tiresias" || val == "S_TIRESIAS") {
          scheduling = S_TIRESIAS;
        } else if (val == "s-srsf" || val == "S_SRSF") {
          scheduling = S_SRSF;
        } else if (val == "srtf" || val == "SRTF") {
          scheduling = SRTF;
        } else if (val == "srsf" || val == "SRSF") {
          scheduling = SRSF;
        } else if (val == "las" || val == "LAS") {
          scheduling = LAS;
        } else if (val == "delay" || val == "DELAY") {
          scheduling = DELAY;
        } else if (val == "themis" || val == "THEMIS") {
          scheduling = THEMIS;
        } else if (val == "rlas" || val == "RLAS") {
          scheduling = RLAS;
        } else if (val == "fifobl" || val == "FIFOBL") {
          scheduling = FIFOBL;
        } else {
          scheduling = FIFO;
        }
      } else if (arg == "-q" || arg == "--queue-threshold") {
        queue_threshold = stoll(val);
      } else if (arg == "-p" || arg == "--promote-knob") {
        promote_knob = stoi(val);
      } else if (arg == "-r" || arg == "--gang-round-length") {
        gang_round_length = stoll(val) * 60 * 1000;
      } else if (arg == "--locality-interval") {
        evolve_interval = stoll(val) * 60 * 1000;
      } else if (arg == "--locality-threshold") {
        relocate_threshold = stod(val);
      // } else if (arg == "--round-length") {
      //   round_length = stoll(val) * 60 * 1000;
      // } else if (arg == "--round-rule") {
      //   if (val == "rr") {
      //     round_rule = kRR;
      //   } else if (val == "las") {
      //     round_rule = kLas;
      //   } else if (val == "prop") {
      //     round_rule = kProp;
      //   }
      } else if (arg == "--loc-sd-rule") {
        ls_std_name = val;
        if (val == "as") {
          loc_sd_rule = kAS;
        } else if (val == "nn") {
          loc_sd_rule = kNN;
        } else if (val == "lw") {
          loc_sd_rule = kLW;
        } else if (val == "phase") {
          loc_sd_rule = kPhase;
        }
      } else if (arg == "--sim-log-dir") {
        sim_log_dir = val;
      // } else if (arg == "--perf-ratio") {
      //   perf_schedule_ratio = stod(val);
      // } else if (arg == "--perf-knob") {
      //   perf_accept_knob = stod(val);
      } else if (arg == "--alpha") {
        alpha = stod(val);
      // } else if (arg == "--stms-filter") {
      //   s_tms_filter = stod(val);
      // } else if (arg == "--stms-lease") {
      //   s_tms_lease_len = stoll(val);
      // } else if (arg == "--strs-qtrsd") {
      //   s_trs_qtrsd = stoll(val);
      } else {
        oss << "Unknown option " << arg << std::endl;
        put_log(oss,kCore);
        return -1;
      }
    } else {
      oss << "Unknown option " << arg << std::endl;
      put_log(oss,kCore);
      return -1;
    }
  }
  if (trace_file.empty() || topology_file.empty() ||
      (scheduling < FIFO && action_duration_file.empty())) {
    print_usage();
    return 1;
  }

  print_args(admission_control, scheduling);


  // construct cluster
  Cluster cluster(topology_file);
  ActionDuration actionDuration(action_duration_file);
  // debug purpose
  // actionDuration.print_action_durations();

  // std::list<time_type> start_event_list;
  // std::list<time_type> end_event_list;

  time_type makespan;
  double avg_gpu_load(0);
  double avg_frag_w_q(-1);
  double effi_gpu_load(0);
  double fair_gpu_load(0);

  if (scheduling < FIFO) {
    // rc_file_name = sim_log_dir + "/jr_" + sched_name + "_" + ls_std_name + ".json";
    // std::cout << "rc_file name: " <<  rc_file_name << std::endl;
    // std::ofstream record_file(rc_file_name, std::ios::out);// | std::ios::app); // deprecated

    // std::list<MicroserviceJob> Job_list;
    std::unordered_map<int, MicroserviceJob> jobs;

    // std::list<MicroserviceJob *> Running_Job_list;
    // std::list<MicroserviceJob *> Queueing_Job_list;
    // std::list<Action *> Running_Action_list;
    // std::list<MicroserviceJob *> Action_Queued_Job_list;

    // parse_trace(trace_file, &Job_list);
    // set_job_start_event(&Job_list, &start_event_list);
    std::vector<std::pair<time_type, int> > submit_events;
    parse_trace(trace_file, jobs, submit_events);
    std::sort(submit_events.begin(), submit_events.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    set_job_kPeriodN(jobs);

    // if (scheduling == GRDL_OPT)
    //   makespan = weighted_mm_serverless_scheduling(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list, scheduling,
    //         admission_control, avg_gpu_load, evolve_interval, relocate_threshold, round_length, round_rule,
    //         perf_schedule_ratio, perf_accept_knob);
    // else if (scheduling == GRDL_LSM)
    //   makespan = lsm_simple_serverless_scheduling(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list, scheduling,
    //         admission_control, avg_gpu_load, evolve_interval, relocate_threshold, round_length, round_rule,
    //         perf_schedule_ratio, perf_accept_knob, effi_gpu_load, fair_gpu_load);
    // else if (scheduling == S_ALPHA_ZERO) // G-ALPHA
    if (scheduling == S_ALPHA_ZERO) // G-ALPHA
      makespan = lsm_simple_alpha_serverless_scheduling(&cluster, &actionDuration, submit_events,
            jobs, admission_control, avg_gpu_load, avg_frag_w_q, evolve_interval, relocate_threshold,
            effi_gpu_load, fair_gpu_load, alpha);
    // else if (scheduling == GRDL_SRSF) // G-AFS && G-MM-SRSF && G-LAS-MM && G-phase
    //   makespan = lsm_simple_srsf_phase_serverless_scheduling(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list, scheduling,
    //         admission_control, avg_gpu_load, evolve_interval, relocate_threshold, round_length, round_rule,
    //         perf_schedule_ratio, perf_accept_knob, effi_gpu_load, fair_gpu_load);
    // else if (scheduling == S_ALPHA_ONE) //
    //   makespan = lsm_alpha_one_serverless_scheduling(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list, scheduling,
    //         admission_control, avg_gpu_load, evolve_interval, relocate_threshold, round_length, round_rule,
    //         perf_schedule_ratio, perf_accept_knob, effi_gpu_load, fair_gpu_load);
    // else if (scheduling == S_MM) //
    //   makespan = lsm_s_mm_serverless_scheduling(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list, scheduling,
    //         admission_control, avg_gpu_load, evolve_interval, relocate_threshold, round_length, round_rule,
    //         perf_schedule_ratio, perf_accept_knob, effi_gpu_load, fair_gpu_load);
    // else if (scheduling == S_MM_CP) //
    //   makespan = lsm_s_mm_serverless_scheduling_copy(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list, scheduling,
    //         admission_control, avg_gpu_load, evolve_interval, relocate_threshold, round_length, round_rule,
    //         perf_schedule_ratio, perf_accept_knob, effi_gpu_load, fair_gpu_load);
    // else if (scheduling == S_SRSF) // G-SRSF && G-SRSF-p
    //   makespan = lsm_srtf_efficiency_serverless_scheduling(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list,
    //         admission_control, avg_gpu_load, evolve_interval, relocate_threshold, round_length,
    //         perf_schedule_ratio, perf_accept_knob, effi_gpu_load, fair_gpu_load);
    // else if (scheduling == S_THEMIS)
    //   makespan = s_themis_schedule(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list,
    //         admission_control, avg_gpu_load, s_tms_filter, s_tms_lease_len);
    // else if (scheduling == S_TIRESIAS)
    //   makespan = s_tiresias_schedule(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list,
    //         admission_control, avg_gpu_load, s_trs_qtrsd);
    // else
    //   makespan = serverless_scheduling(&cluster, &actionDuration, start_event_list,
    //         end_event_list, Job_list, Running_Job_list, Queueing_Job_list,
    //         Running_Action_list, Action_Queued_Job_list, scheduling,
    //         admission_control, avg_gpu_load, evolve_interval, relocate_threshold, round_length, round_rule);

    for (auto& [id, job] : jobs) {
      if (!job.is_fair_records_empty()) {
        try {
          job.calc_fairness(&cluster, &actionDuration, admission_control);
        } catch (std::exception& e) {
          std::cerr << e.what() << std::endl;
        }
      }
    }
    compute_and_set_irt_rank(jobs);


    if (!sim_log_dir.empty()) {
      rc_file_name = sim_log_dir + "/jr_" + sched_name + "_" + ls_std_name + ".json";
      write_record_file(jobs, rc_file_name);
    }

    // double avg_c(0), avg_w(0), avg_g(0), avg_s(0);
    oss << "Job#,Admitted_time,Scheduled_time,JCT,Running_time,fairness(Mm),"
           "fairness(1/n),fairness(1/n-down),fairness(GPU_load),"
           "fairness(GPU_share),locality(dist),locality(bit),cold_count,wave,"
           "gpu_usg,n_avg,round_out,#shuffle,locality(content),iq_ignore,"
           "dist_expand,unlucky_expand,lucky_expand,lucky_shrink,model,"
           "gpu_req,k_val,rank_irt,ideal_run_time,ooit_inc_p_gpu\n";
    put_log(oss,kQuiet);
    for (size_t i = 0; i < jobs.size(); ++i) {
      jobs.at(i+1).statistics(admission_control);
    }
    compute_and_print_statistics(jobs, makespan, avg_gpu_load, avg_frag_w_q,
                                 effi_gpu_load, fair_gpu_load);
  }
  else {
    // std::list<Job> Job_list;
    std::unordered_map<int, Job> jobs; // if there is a lots of hash collision, use map instead
    std::vector<std::pair<time_type, int> > submit_events;
    parse_trace(trace_file, jobs, submit_events);
    std::sort(submit_events.begin(), submit_events.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    // if (scheduling == DELAY) {
    //   makespan = fifo_delay_scheduling(&cluster, start_event_list,
    //           end_event_list, Jobs, Running_Job_list, Queueing_Job_list,
    //           scheduling, admission_control, avg_gpu_load);
    // } else if (scheduling == FIFO) {
    //   makespan = fifo_scheduling(&cluster, start_event_list,
    //           end_event_list, Jobs, Running_Job_list, Queueing_Job_list,
    //           scheduling, admission_control, avg_gpu_load, false);
    // } else if (scheduling == FIFOBL) {
    //   makespan = fifo_scheduling(&cluster, start_event_list,
    //           end_event_list, Jobs, Running_Job_list, Queueing_Job_list,
    //           scheduling, admission_control, avg_gpu_load, true);
    // } else if (scheduling == LAS) {
    if (scheduling == LAS) {
      makespan = multi_level_queue_las_scheduling(
          &cluster, submit_events, jobs, admission_control, avg_gpu_load,
          avg_frag_w_q, queue_threshold, promote_knob);
    } else if (scheduling == THEMIS || scheduling == RLAS) {
    // if (scheduling == THEMIS || scheduling == RLAS) {
      makespan = round_based_priority_scheduling(
          &cluster, submit_events, jobs, scheduling, admission_control,
          avg_gpu_load, avg_frag_w_q, gang_round_length);
    } else {
      makespan = monolithic_preemptive_scheduling(
          &cluster, submit_events, jobs, scheduling, admission_control,
          avg_gpu_load, avg_frag_w_q);
    }

    for (auto& [id, job] : jobs) {
      if (!job.is_fair_records_empty()) {
        try {
          job.calc_fairness(&cluster, admission_control);
        } catch (std::exception& e) {
          std::cerr << e.what() << std::endl;
        }
      }
    }
    compute_and_set_irt_rank(jobs);

    oss << "Job#,Admitted_time,Scheduled_time,JCT,Running_time,"
           "fairness(Mm),fairness(1/n),fairness(1/n-down),"
           "fairness(GPU_load),fairness(GPU_share),"
           "locality(dist),locality(bit),yield_count,n_avg,promote_count,"
           "model,gpu_req,rank_irt,ideal_run_time\n";
    put_log(oss,kQuiet);
    // for (const auto& [id, job] : jobs) {
    //   job.statistics(admission_control);
    // }
    for (size_t i = 0; i < jobs.size(); ++i) {
      jobs.at(i+1).statistics(admission_control);
    }
    compute_and_print_statistics(jobs, makespan, avg_gpu_load, avg_frag_w_q);
  }

  return 0;
}