#include "util.h"

void put_log(std::ostringstream& oss, LogLevel priority) {
  if (priority < verbose) {
    std::cout << oss.str();
  }
  oss.str("");
  oss.clear();
}