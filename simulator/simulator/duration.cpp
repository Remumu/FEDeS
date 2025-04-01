#include "duration.hpp"

#include <iostream>
#include <fstream>

time_type get_one_step_duration(int model, int num_gpu, int distribution) {
  time_type duration_one_step;
  if (model == VGG19_Cifar) {
    if (num_gpu == 1) {
      duration_one_step = 168;
      // duration_one_step = 146;
    } else if (num_gpu == 2) {
      duration_one_step = 173;
    } else if (num_gpu == 4) {
      if (distribution == 1) {
        duration_one_step = 194;
        // duration_one_step = 169;
      } else if (distribution == 2) {
        duration_one_step = 243;
      } else if (distribution <= 4) {
        // duration_one_step = 227;
        duration_one_step = 243;
      } else {
        throw distribution;
      }
    } else if (num_gpu == 8) {
      if (distribution == 2) {
        duration_one_step = 242;
      // } else if (distribution == 3) {
      //   duration_one_step = 482;
      } else if (distribution <= 8) {
        // duration_one_step = 294;
        duration_one_step = 352;
      } else {
        throw distribution;
      }
    } else if (num_gpu == 16) {
      if (distribution == 4) {
        duration_one_step = 276;
      // } else if (distribution == 5) {
      //   duration_one_step = 507;
      } else if (distribution <= 16) {
        duration_one_step = 320;
      } else {
        throw distribution;
      }
    } else {
      throw num_gpu;
    }
  } else if (model == ResNeXt) {
    if (num_gpu == 1) {
      duration_one_step = 880;
    } else if (num_gpu == 2) {
      duration_one_step = 891;
    } else if (num_gpu == 4) {
      if (distribution == 1) {
        duration_one_step = 903;
      } else if (distribution <= 4) {
        duration_one_step = 907;
      }
    } else if (num_gpu == 8) {
      if (distribution == 2) {
        duration_one_step = 910;
      // } else if (distribution == 3) {
        // duration_one_step = 948;
      } else if (distribution <= 8) {
        duration_one_step = 922;
      }
    } else if (num_gpu == 16) {
      if (distribution == 4) {
        duration_one_step = 923;
      } else if (distribution <= 16) {
        duration_one_step = 907;
      }
    } else {
      throw num_gpu;
    }
  } else if (model == ResNet50) {
    if (num_gpu == 1) {
      duration_one_step = 1173;
      // duration_one_step = 1136;
    } else if (num_gpu == 2) {
      duration_one_step = 1216;
      // duration_one_step = 1186;
      if (distribution == 2) {
        duration_one_step = 1216;
      }
    } else if (num_gpu == 4) {
      if (distribution == 1) {
        duration_one_step = 1239;
      // } else if (distribution == 2) {
      //   duration_one_step = 1313;
      // } else if (distribution == 3) {
      //   duration_one_step = 1304;
      } else if (distribution <= 4) {
        duration_one_step = 1247;
      }
    } else if (num_gpu == 8) {
      if (distribution == 2) {
        duration_one_step = 1277;
      // } else if (distribution == 3) {
        // duration_one_step = 1359;
      } else if (distribution <= 8) {
        duration_one_step = 1298;
      }
    } else if (num_gpu == 16) {
      if (distribution == 4) {
        duration_one_step = 1363;
      } else if (distribution <= 16) {
        duration_one_step = 1369;
      }
    }
  } else if (model == Inception3) {
    if (num_gpu == 1) {
      // duration_one_step = 1848;
      duration_one_step = 1940;
    } else if (num_gpu == 2) {
      duration_one_step = 1952;
    } else if (num_gpu == 4) {
      if (distribution == 1) {
        // duration_one_step = 1992;
        duration_one_step = 2092;
      } else if (distribution == 2) {
        duration_one_step = 2130;
      } else if (distribution <= 4) {
        duration_one_step = 2142;
      }
    } else if (num_gpu == 8) {
      if (distribution == 2) {
        duration_one_step = 2098;
      } else if (distribution == 3) {
        duration_one_step = 2176;
      } else if (distribution <= 8) {
        // duration_one_step = 2097;
        duration_one_step = 2176;
      }
    } else if (num_gpu == 16) {
      if (distribution == 4) {
        // duration_one_step = 2079;
        duration_one_step = 2179;
      } else if (distribution <= 16) {
        duration_one_step = 2207;
      }
    }
  } else if (model == VGG19) {
    if (num_gpu == 1) {
      duration_one_step = 1037;
    } else if (num_gpu == 2) {
      duration_one_step = 1161;
    } else if (num_gpu == 4) {
      if (distribution == 1) {
        duration_one_step = 1324;
      // } else if (distribution == 2) {
        // duration_one_step = 1670;
      } else if (distribution <= 4) {
        duration_one_step = 1350;
      }
    } else if (num_gpu == 8) {
      if (distribution == 2) {
        duration_one_step = 1453;
      // } else if (distribution == 3) {
        // duration_one_step = 1959;
      } else if (distribution <= 8) {
        duration_one_step = 1680;
      }
    } else if (num_gpu == 16) {
      duration_one_step = (distribution == 4) ? 1569 : 1708;
    }
  } else if (model == LSTM) {
    if (num_gpu == 1) {
      duration_one_step = 603;
    } else if (num_gpu == 2) {
      duration_one_step = 742;
    } else if (num_gpu == 4) {
      if (distribution == 1) {
        // duration_one_step = 764;
        duration_one_step = 804;
      // } else if (distribution == 2) {
      //   duration_one_step = 653;
      // } else if (distribution == 3) {
      //   duration_one_step = 657;
      } else {
        // duration_one_step = 626;
        // duration_one_step = 783;
        duration_one_step = 823;
      }
    } else if (num_gpu == 8) {
      // duration_one_step = (distribution == 2) ? 813 : 806;
      duration_one_step = (distribution == 2) ? 893 : 916;
    } else if (num_gpu == 16) {
      // duration_one_step = (distribution == 4) ? 825 : 824;
      duration_one_step = (distribution == 4) ? 925 : 944;
    }
  } else if (model == GNMT) {
    if (num_gpu == 1) {
      duration_one_step = 1447;
    } else if (num_gpu == 2) {
      duration_one_step = 1579;
    } else if (num_gpu == 4) {
      duration_one_step = (distribution == 1) ? 1624 : 1624;
    } else if (num_gpu == 8) {
      duration_one_step = (distribution == 2) ? 1677 : 1779;
      // if (distribution == 2) {
      //   duration_one_step = 1820;
      // } else if (distribution == 3) {
      //   duration_one_step = 1997;
      // } else if (distribution <= 8) {
      //   duration_one_step = 2192;
      // }
    } else if (num_gpu == 16) {
      duration_one_step = (distribution == 4) ? 1686 : 1913;
    }
  } else if (model == GPT2_SMALL) {
    if (num_gpu == 1) {
      duration_one_step = 1268;
    } else if (num_gpu == 2) {
      duration_one_step = 1378;
    } else if (num_gpu == 4) {
      if (distribution == 1) {
        duration_one_step = 1422;
      } else {
        duration_one_step = 1428;
      }
    } else if (num_gpu == 8) {
      duration_one_step = (distribution == 2) ? 1486 : 1478;
    } else if (num_gpu == 16) {
      duration_one_step = (distribution == 4) ? 1495 : 1508;
    }
  } else if (model == BERTLARGE) {
    if (num_gpu == 1) {
      duration_one_step = 1854;
    } else if (num_gpu == 2) {
      duration_one_step = 1996;
    } else if (num_gpu == 4) {
      duration_one_step = (distribution == 1) ? 2072 : 2059;
    } else if (num_gpu == 8) {
      duration_one_step = (distribution == 2) ? 2115 : 2120;
    } else if (num_gpu == 16) {
      duration_one_step = (distribution == 4) ? 2156 : 2275;
    }
  } else {
    throw model;
  }

  return duration_one_step;
}

ActionDuration::ActionDuration(std::string action_dur_file) {
  std::ifstream file;
  std::string line;
  size_t model;
  time_type time;
  try {
    file.open(action_dur_file);
    oss << "[Debug] Open file " << action_dur_file << std::endl;
    put_log(oss,kDebug);
    while (getline(file,line)) {
        oss << "[Debug] read line " << line << std::endl;
        put_log(oss,kDebug);
      if (line == "ResNet50") {
        model = 0;
      } else if (line == "Inception3") {
        model = 1;
      } else if (line == "VGG19") {
        model = 2;
      } else if (line == "VGG19_Cifar") {
        model = 3;
      } else if (line == "ResNeXt") {
        model = 4;
      } else if (line == "LSTM") {
        model = 5;
      } else if (line == "GNMT") {
        model = 6;
      } else if (line == "GPT2_SMALL") {
        model = 7;
      } else if (line == "BERTLARGE") {
        model = 8;
      } else if (line == "PCIFAR") {
        model = 9;
      } else if (line == "PIMAGENET") {
        model = 10;
      } else {
        continue;
      }
      for (size_t i = 0; i < _num_cases; ++i) {
        getline(file,line);
        oss << "[Debug] read line " << line << std::endl;
        time = time_type(std::stod(line) * 1000);
        oss << "[Debug] save time " << time << std::endl;
        put_log(oss,kDebug);
        _action_durations[model][i] = time;
      }
    }
    file.close();
  }
  catch (const std::ifstream::failure& e) {
    std::cerr << "Exception opening/reading file " << action_dur_file << '\n'
              << e.what() << '\n'
              << e.code() << '\n';
  }
}

time_type ActionDuration::get_dur(int model, int logical_worker, double distance, int syncN, int wave) {
  int idxm(-1), idxc(-1);
  // distance = 1;
  // if (syncN == 5) {
    if (model == ResNet50)
      idxm = 0;
    else if (model == Inception3)
      idxm = 1;
    else if (model == VGG19)
      idxm = 2;
    else if (model == VGG19_Cifar)
      idxm = 3;
    else if (model == ResNeXt)
      idxm = 4;
    else if (model == LSTM)
      idxm = 5;
    else if (model == GNMT)
      idxm = 6;
    else if (model == GPT2_SMALL)
      idxm = 7;
    else if (model == BERTLARGE)
      idxm = 8;
    else if (model == PCIFAR)
      idxm = 9;
    else if (model == PIMAGENET)
      idxm = 10;

    if (logical_worker == 1) {
      idxc = 0;
    } else if (logical_worker == 2) {
      if (wave == 1) {
        idxc = 1;
      } else if (wave == 2) {
        idxc = 2;
      }
    } else if (logical_worker == 4) {
      if (wave == 1) {
        if (distance == 1)
          idxc = 3;
        else
          idxc = 4;
      } else if (wave == 2) {
        idxc = 5;
      } else if (wave == 4) {
        idxc = 6;
      }
    } else if (logical_worker == 8) {
      if (wave == 1) {
        if (distance == 1)
          idxc = 7;
        else
          idxc = 8;
      } else if (wave == 2) {
        if (distance == 1)
          idxc = 9;
        else
          idxc = 10;
      } else if (wave == 4) {
        idxc = 11;
      } else if (wave == 8) {
        idxc = 12;
      }
    } else if (logical_worker == 16) {
      if (wave == 1) {
        if (distance == 1)
          idxc = 13;
        else
          idxc = 14;
      } else if (wave == 2) {
        if (distance == 1)
          idxc = 15;
        else
          idxc = 16;
      } else if (wave == 4) {
        if (distance == 1)
          idxc = 17;
        else
          idxc = 18;
      } else if (wave == 8) {
        idxc = 19;
      } else if (wave == 16) {
        idxc = 20;
      }
    }
  // }

  try {
    if (idxm == -1 || idxc == -1) {
      std::string msg = "Cannot find appropriate index of action duration for the status(model:" + std::to_string(model) + ",gpu:" + std::to_string(logical_worker) + ",phase:" + std::to_string(wave) + ")\n";
      throw std::invalid_argument(msg);
    }
  } catch (const std::invalid_argument& e) {
    std::cerr << "Exception: " << e.what();
  }
  return _action_durations[idxm][idxc];
}
void ActionDuration::print_action_durations() {
    std::cout << "Action durations: " << '\n';
    for (size_t i = 0; i < _num_model; ++i) {
        for (size_t j = 0; j < _num_cases; ++j)
            std::cout << _action_durations[i][j] << ' ';
        std::cout << '\n';
    }
}

double ActionDuration::get_ooit_inc_p_gpu(int model, int logical_worker) const {
  int idxm(-1), idxc(-1);
  if (model == ResNet50)
    idxm = 0;
  else if (model == Inception3)
    idxm = 1;
  else if (model == VGG19)
    idxm = 2;
  else if (model == VGG19_Cifar)
    idxm = 3;
  else if (model == ResNeXt)
    idxm = 4;
  else if (model == LSTM)
    idxm = 5;
  else if (model == GNMT)
    idxm = 6;
  else if (model == GPT2_SMALL)
    idxm = 7;
  else if (model == BERTLARGE)
    idxm = 8;
  else if (model == PCIFAR)
    idxm = 9;
  else if (model == PIMAGENET)
    idxm = 10;

  if (logical_worker == 1) {
    idxc = 0;
  } else if (logical_worker == 2) {
    idxc = 1;
  } else if (logical_worker == 4) {
    idxc = 2;
  } else if (logical_worker == 8) {
    idxc = 3;
  } else if (logical_worker == 16) {
    idxc = 4;
  }
  try {
    if (idxm == -1 || idxc == -1) {
      std::string msg =
          "Cannot find appropriate index of action duration for the "
          "status(model:" +
          std::to_string(model) + ",gpu:" + std::to_string(logical_worker) +
          ")\n";
      throw std::invalid_argument(msg);
    }
  } catch (const std::invalid_argument& e) {
    std::cerr << "Exception: " << e.what();
  }

  return _one_over_itertime_increment_per_gpu[idxm][idxc];
};

time_type get_duration_wo_framework (int model, int logical_worker) {
  time_type train_time;
  if (model == VGG19_Cifar) {
    if (logical_worker == 1) {
    train_time = 191;
    } else if (logical_worker == 2) {
    train_time = 226;
    } else if (logical_worker == 4) {
    train_time = 243;
    } else if (logical_worker == 8) {
    train_time = 351;
    } else if (logical_worker == 16) {
    train_time = 423;
    }
  } else if (model == ResNeXt) {
    if (logical_worker == 1) {
    train_time = 878;
    } else if (logical_worker == 2) {
    train_time = 897;
    } else if (logical_worker == 4) {
    train_time = 895;
    } else if (logical_worker == 8) {
    train_time = 893;
    } else if (logical_worker == 16) {
    train_time = 913;
    }
  } else if (model == ResNet50) {
    if (logical_worker == 2 || logical_worker == 1) {
    train_time = 1239;
    } else if (logical_worker == 4) {
    train_time = 1203;
    } else if (logical_worker == 8) {
    train_time = 1232;
    } else if (logical_worker == 16) {
    train_time = 1329;
    }
  } else if (model == Inception3) {
    if (logical_worker == 2 || logical_worker == 1) {
    train_time = 1935;
    } else if (logical_worker == 4) {
    train_time = 1964;
    } else if (logical_worker == 8) {
    train_time = 2026;
    } else if (logical_worker == 16) {
    train_time = 2052;
    }
  } else if (model == VGG19) {
    if (logical_worker == 1) {
    train_time = 1171;
    } else if (logical_worker == 2) {
    train_time = 1962;
    } else if (logical_worker == 4) {
    train_time = 1570;
    } else if (logical_worker == 8) {
    train_time = 2500;
    } else if (logical_worker == 16) {
    train_time = 3000;
    }
  } else {
    throw model;
  }
  return train_time;
}
