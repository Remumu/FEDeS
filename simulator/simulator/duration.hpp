#ifndef DURATION_H_
#define DURATION_H_

#include "util.h"

enum ModelType {
  ResNet56,
  Inception3,
  ResNet50,
  ResNet152,
  ResNeXt,
  VGG19_Cifar,
  VGG19,
  LSTM,
  GNMT,
  GPT2_SMALL,
  BERTLARGE,
  PCIFAR,
  PIMAGENET
};
// Get model name string from enum value
const std::string ModelNameStr[13] = {
    "ResNet56",    "Inception3", "ResNet50", "ResNet152", "ResNeXt",
    "VGG19_Cifar", "VGG19",      "LSTM",     "GNMT",      "GPT2_SMALL",
    "BERTLARGE",   "PCIFAR",     "PIMAGENET"};
// std::string get_model_name_str(ModelType model) {
//   return ModelNameStr[model];
// }

enum DataType { Cifar10, ImageNet, PennTreeBank, wmt16_de_en, WikiText, SQuAD };

time_type get_one_step_duration(int model, int num_gpu, int distribution);

class ActionDuration {
  public:
    ActionDuration(std::string file);
    // bool load_durations(std::string action_dur_file);
    time_type get_dur(int model, int logical_worker, double distance, int syncN, int wave);

    double get_ooit_inc_p_gpu(int model, int logical_worker) const;

    // debug purpose
    void print_action_durations();

  private:
    static const size_t _num_model = 11;
    static const size_t _num_cases = 21;
    time_type _action_durations[_num_model][_num_cases] = {};

    double _one_over_itertime_increment_per_gpu[_num_model][5] = {
        {1, 0.5, 0.255, 0.130, 0.066}, // ResNet50
        {1, 0.5, 0.251, 0.129, 0.065}, // Inception3
        {1, 0.5, 0.258, 0.136, 0.075}, // VGG19-I
        {1, 0.5, 0.265, 0.145, 0.077}, // VGG19-C
        {1, 0.5, 0.253, 0.130, 0.065}, // ResNeXt
        {1, 0.5, 0.257, 0.133, 0.069}, // LSTM
        {1, 0.5, 0.259, 0.142, 0.074}, // GNMT
        {1, 0.5, 0.260, 0.139, 0.072}, // GPT2-S
        {1, 0.5, 0.264, 0.142, 0.083}, // BERT-L
        {1, 0.5, 0.25, 0.125, 0.0625},
        {1, 0.5, 0.25, 0.125, 0.0625}
    };
};

// deprecated
// time_type get_action_duration(int model, int logical_worker, double distance, int syncN, int wave);
time_type get_duration_wo_framework (int model, int logical_worker);

#endif // DURATION_H_