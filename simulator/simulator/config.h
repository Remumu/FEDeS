#ifndef __CONFIG_H__
#define __CONFIG_H__

#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <list>
#include <map>
#include <unordered_map>
#include <iterator>

#include "job.h"
#include "util.h"

// enum ScheduleType {GRML, GRML_2P, GRML_DIV, GRML_2M, GRDL_OPT, GRDL_LSM, GRDL_ALPHA, SRTF_LSM, S_THEMIS, FIFO, SRTF, SRSF, LAS, DELAY, THEMIS, RLAS, FIFOBL};

// // parse workload's trace file, generate Job, append to job_list
// template<class T>
// int parse_trace(std::string trace_file, std::list<T>* job_list_ptr) {
//   oss << "Parsing trace file\n";
//   put_log(oss,kCore);
//   std::ifstream file;
//   std::string line, model;
//   time_type submit_time;
//   int GPU_req, iterations;
//   int batch_size{32};
//   DataType e_dataset{Cifar10};
//   ModelType e_model{ResNet56};
//   file.open(trace_file);
//   int id = 0;
//   while (getline(file, line)) {
//     ++id;
//     std::stringstream ss(line);
//     ss >> submit_time;
//     ss >> model;
//     ss >> GPU_req;
//     ss >> iterations;
//     if (model == "ResNet56") {
//       batch_size = 128;
//       e_dataset = Cifar10;
//       e_model = ResNet56;
//     }
//     else if (model == "Inception3") {
//       batch_size = 64;
//       e_dataset = ImageNet;
//       e_model = Inception3;
//     }
//     else if (model == "ResNet50") {
//       batch_size = 64;
//       e_dataset = ImageNet;
//       e_model = ResNet50;
//     }
//     else if (model == "ResNet152") {
//       batch_size = 32;
//       e_dataset = ImageNet;
//       e_model = ResNet152;
//     }
//     else if (model == "VGG19-I") {
//       batch_size = 32;
//       e_dataset = ImageNet;
//       e_model = VGG19;
//     }
//     else if (model == "ResNeXt") {
//       batch_size = 128;
//       e_dataset = Cifar10;
//       e_model = ResNeXt;
//     }
//     else if (model == "VGG19") {
//       batch_size = 128;
//       e_dataset = Cifar10;
//       e_model = VGG19_Cifar;
//     } else if (model == "LSTM") {
//       batch_size = 64;
//       e_dataset = PennTreeBank;
//       e_model = LSTM;
//     } else if (model == "GNMT") {
//       batch_size = 64;
//       e_dataset = wmt16_de_en;
//       e_model = GNMT;
//     } else if (model == "GPT2-SMALL") {
//       batch_size = 2;
//       e_dataset = WikiText;
//       e_model = GPT2_SMALL;
//     } else if (model == "BERTLARGE") {
//       batch_size = 4;
//       e_dataset = SQuAD;
//       e_model = BERTLARGE;
//     } else if (model == "PCIFAR") {
//       batch_size = 129;
//       e_dataset = Cifar10;
//       e_model = PCIFAR;
//     } else if (model == "PIMAGENET") {
//       batch_size = 81;
//       e_dataset = ImageNet;
//       e_model = PIMAGENET;
//     }
//     T job_new(id, submit_time*1000, GPU_req, batch_size, iterations, e_model, e_dataset);
//     job_list_ptr->push_back(job_new);
//   }
//   file.close();
//   return 0;
// }

template <class T>
int parse_trace(std::string trace_file, std::unordered_map<int, T>& jobs,
                std::vector<std::pair<time_type, int>>& submit_events) {
  oss << "Parsing trace file\n";
  put_log(oss,kCore);
  std::ifstream file;
  std::string line, model;
  time_type submit_time;
  int GPU_req, iterations;
  int batch_size{32};
  DataType e_dataset{Cifar10};
  ModelType e_model{ResNet56};
  file.open(trace_file);
  size_t num_line = 1;
  while(std::getline(file, line)) {
    ++num_line;
  }
  jobs.reserve(num_line);
  submit_events.reserve(num_line);
  file.clear();
  file.seekg(0);
  int id = 0;
  while (getline(file, line)) {
    ++id;
    std::stringstream ss(line);
    ss >> submit_time;
    ss >> model;
    ss >> GPU_req;
    ss >> iterations;
    if (model == "ResNet56") {
      batch_size = 128;
      e_dataset = Cifar10;
      e_model = ResNet56;
    }
    else if (model == "Inception3") {
      batch_size = 64;
      e_dataset = ImageNet;
      e_model = Inception3;
    }
    else if (model == "ResNet50") {
      batch_size = 64;
      e_dataset = ImageNet;
      e_model = ResNet50;
    }
    else if (model == "ResNet152") {
      batch_size = 32;
      e_dataset = ImageNet;
      e_model = ResNet152;
    }
    else if (model == "VGG19-I") {
      batch_size = 32;
      e_dataset = ImageNet;
      e_model = VGG19;
    }
    else if (model == "ResNeXt") {
      batch_size = 128;
      e_dataset = Cifar10;
      e_model = ResNeXt;
    }
    else if (model == "VGG19") {
      batch_size = 128;
      e_dataset = Cifar10;
      e_model = VGG19_Cifar;
    } else if (model == "LSTM") {
      batch_size = 64;
      e_dataset = PennTreeBank;
      e_model = LSTM;
    } else if (model == "GNMT") {
      batch_size = 64;
      e_dataset = wmt16_de_en;
      e_model = GNMT;
    } else if (model == "GPT2-SMALL") {
      batch_size = 2;
      e_dataset = WikiText;
      e_model = GPT2_SMALL;
    } else if (model == "BERTLARGE") {
      batch_size = 4;
      e_dataset = SQuAD;
      e_model = BERTLARGE;
    } else if (model == "PCIFAR") {
      batch_size = 129;
      e_dataset = Cifar10;
      e_model = PCIFAR;
    } else if (model == "PIMAGENET") {
      batch_size = 81;
      e_dataset = ImageNet;
      e_model = PIMAGENET;
    }
    T job_new(id, submit_time*1000, GPU_req, batch_size, iterations, e_model, e_dataset);
    // jobs[id] = job_new;
    jobs.insert({id, job_new});
    // job_list_ptr->push_back(job_new);
    submit_events.push_back(std::make_pair(submit_time*1000, id));
  }
  file.close();
  return 0;
}

// template<class T>
// int set_job_start_event(std::list<T>* job_list, std::list<time_type>* start_event_list) {
//   oss << "Setting job start events\n";
//   put_log(oss,kInfo);
//   for (typename std::list<T>::iterator it_job = job_list->begin(); it_job != job_list->end(); ++it_job) {
//     time_type submit_time = it_job->get_submit_time();
//     start_event_list->push_back(submit_time);
//   }
//   start_event_list->sort();
//   start_event_list->unique();
//   oss << "Start event list: ";
//   for (const auto& elem : *start_event_list) {
//     oss << elem << " ";
//   }
//   oss << std::endl;
//   put_log(oss,kDebug);
//   return 0;
// }

// int set_job_kPeriodN(std::list<MicroserviceJob>* job_list) {

//   bool is_fixed_gb = true;
//   const int kvals[6][5] = {
//     {256, 128, 64, 32, 16}, // GPT
//     {128, 64, 32, 16, 8}, // BERT
//     {128, 64, 32, 16, 8}, // ResNet
//     {32, 16, 8, 4, 0}, // ResNeXt
//     {32, 16, 8, 4, 2}, // VGG
//     {64, 32, 16, 8, 0} // GNMT
//   };
//   for (auto& job : *job_list) {
//     int kval = 0;

//     ModelType e_model = job.get_model();
//     int GPU_req = job.get_gpu_req();

//     int idx_m = -1, idx_w = -1;
//     if (e_model == GPT2_SMALL) {
//       idx_m = 0;
//     } else if (e_model == BERTLARGE) {
//       idx_m = 1;
//     } else if (e_model == ResNet50) {
//       idx_m = 2;
//     } else if (e_model == ResNeXt) {
//       idx_m = 3;
//     } else if (e_model == VGG19) {
//       idx_m = 4;
//     } else if (e_model == GNMT) {
//       idx_m = 5;
//     }

//     if (is_fixed_gb) {
//       if (GPU_req == 1) {
//         idx_w = 0;
//       } else if (GPU_req == 2) {
//         idx_w = 1;
//       } else if (GPU_req == 4) {
//         idx_w = 2;
//       } else if (GPU_req == 8) {
//         idx_w = 3;
//       } else if (GPU_req == 16) {
//         idx_w = 4;
//       }
//       if (idx_m >= 0 && idx_m < 6 && idx_w >= 0 && idx_w < 5)
//         kval = kvals[idx_m][idx_w];
//       if (kval == 0) {
//         std::cerr << "[Error] FIXED_GBS Cannot find k value for "
//                   << ModelNameStr[e_model] << ' ' << GPU_req << std::endl;
//         assert(false);
//         // kval = 5;
//       }
//     } else {
//       if (idx_m == -1) {
//         oss << "[Warning] Cannot find k value for " << ModelNameStr[e_model]
//             << ". Using k=5 insted" << std::endl;
//         put_log(oss, kCore);
//         kval = 5;
//       } else {
//         size_t i = sizeof(kvals[idx_m]) / sizeof(kvals[idx_m][0]);
//         for (; i > 0; --i) {
//           if (kvals[idx_m][i - 1] != 0) break;
//         }
//         kval = kvals[idx_m][i - 1];
//       }
//     }
//     if (kval == 0) {
//       std::cerr << "[Error] k value cannot be zero" << std::endl;
//       assert(false);
//     }
//     job.set_kPeriodN(kval);
//   }
//   return 0;
// }

int set_job_kPeriodN(std::unordered_map<int, MicroserviceJob>& jobs) {
  bool is_fixed_gb = true;
  const int kvals[6][5] = {
    {256, 128, 64, 32, 16}, // GPT
    {128, 64, 32, 16, 8}, // BERT
    {128, 64, 32, 16, 8}, // ResNet
    {32, 16, 8, 4, 0}, // ResNeXt
    {32, 16, 8, 4, 2}, // VGG
    {64, 32, 16, 8, 0} // GNMT
  };
  for (auto& [id, job] : jobs) {
    int kval = 0;

    ModelType e_model = job.get_model();
    int GPU_req = job.get_gpu_req();

    int idx_m = -1, idx_w = -1;
    if (e_model == GPT2_SMALL) {
      idx_m = 0;
    } else if (e_model == BERTLARGE) {
      idx_m = 1;
    } else if (e_model == ResNet50) {
      idx_m = 2;
    } else if (e_model == ResNeXt) {
      idx_m = 3;
    } else if (e_model == VGG19) {
      idx_m = 4;
    } else if (e_model == GNMT) {
      idx_m = 5;
    }

    if (is_fixed_gb) {
      if (GPU_req == 1) {
        idx_w = 0;
      } else if (GPU_req == 2) {
        idx_w = 1;
      } else if (GPU_req == 4) {
        idx_w = 2;
      } else if (GPU_req == 8) {
        idx_w = 3;
      } else if (GPU_req == 16) {
        idx_w = 4;
      }
      if (idx_m >= 0 && idx_m < 6 && idx_w >= 0 && idx_w < 5)
        kval = kvals[idx_m][idx_w];
      if (kval == 0) {
        std::cerr << "[Error] FIXED_GBS Cannot find k value for "
                  << ModelNameStr[e_model] << ' ' << GPU_req << std::endl;
        assert(false);
        // kval = 5;
      }
    } else {
      if (idx_m == -1) {
        oss << "[Warning] Cannot find k value for " << ModelNameStr[e_model]
            << ". Using k=5 insted" << std::endl;
        put_log(oss, kCore);
        kval = 5;
      } else {
        size_t i = sizeof(kvals[idx_m]) / sizeof(kvals[idx_m][0]);
        for (; i > 0; --i) {
          if (kvals[idx_m][i - 1] != 0) break;
        }
        kval = kvals[idx_m][i - 1];
      }
    }
    if (kval == 0) {
      std::cerr << "[Error] k value cannot be zero" << std::endl;
      assert(false);
    }
    job.set_kPeriodN(kval);
  }
  return 0;
}

#endif
