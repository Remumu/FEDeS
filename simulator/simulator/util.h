#ifndef __UTIL_H__
#define __UTIL_H__

#include <iostream>
#include <string>
#include <sstream>
#include <iomanip>

typedef long long time_type;

enum LogLevel {kQuiet=-1, kCore, kInfo, kDebug};

enum LocSensDivRule { kAS, kNN, kLW, kPhase };

extern LogLevel verbose;
extern std::ostringstream oss;
// extern bool ignore_scale_factor; // deprecated
extern bool ignore_framework;
extern bool ignore_init;
extern bool ignore_wave;

extern LocSensDivRule loc_sd_rule;

// extern int kGPU_p_node;

// print string stream and empty it.
void put_log(std::ostringstream& oss, LogLevel priority);

#endif