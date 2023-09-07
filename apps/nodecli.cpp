#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <unordered_map>

#include "CBufParser.h"
#include "node/core.h"
#include "node/nodeerr.h"
#include "node/nodelib.h"
#include "node/nodemaster.h"
#include "registry.h"
#include "toolbox/rate.h"
#include "toolbox/termtool.h"
#include "vlog.h"

#define MAX(a, b) ((a > b) ? a : b)
#define MIN(a, b) ((a < b) ? a : b)

#define SERVER_IP "127.0.0.1"

#define BUFFER_SIZE 8196

using namespace node;
using namespace toolbox;

static bool verbose = false;
static bool exit_process = false;
static bool follow = false;

void Usage() {
  printf("Node command line toolset, v1.0\n");
  printf("  Usage: node [OPTIONS] COMMAND\n");
  printf("  Options:\n");
  printf("    --ip <server ip>   : use the designated IP for the node server\n");
  printf("    -v                 : verbose\n");
  printf("    -h                 : show this help\n");
  printf("    -f                 : follow, do not exit a command and keep. For print\n");
  printf("\n");
  printf("  Commands:\n");
  printf("    ls   : list all topics on the system\n");
  printf("    mon  : monitor the topics on the system and their publishings\n");
  printf("    info : information on nodemaster\n");
  printf("    kill : kill the nodemaster\n");
  printf("    print <topic_name> : print the latest message on a given topic\n");
  printf("    get <key> : print the latest value associated with given key in the store\n");
  printf("    set <key> <value>: add or update the key-value store with key-value pair\n");
  printf(
      "    store <regex-optional>: print latest snapshot of the key-value store. Only supports * wildcard\n");
  printf(
      "    shutdown : this will trigger the usual course of shutting down our threads. It sends a SIGINT "
      "to launcher at the server_ip ip. It will send the request to localhost if no ip is provided\n");
  exit(0);
}

enum Command {
  UNKNOWN,
  LIST_TOPICS,
  MONITOR_TOPICS,
  NM_INFO,
  KILL_NM,
  PRINT_MSG,
  GET_VALUE,
  SET_VALUE,
  PRINT_STORE,
  SHUTDOWN
};

static constexpr const char* TopicTypeToStr(node::TopicType type) {
  switch (type) {
    case node::TopicType::PUB_SUB:
      return "PUB_SUB";
    case node::TopicType::RPC:
      return "RPC";
    case node::TopicType::SINK_SRC:
      return "SNK_SRC";
    case node::TopicType::UNKNOWN:
      return "UNKNOWN";
  }
  return "";
}

static double get_current_time() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  double now = double(ts.tv_sec) + double(ts.tv_nsec) / 1e9;
  return now;
}

static void handle_sigint(int sig) {
  (void)sig;
  exit_process = true;
}

bool DoListTopics(std::string& server_ip) {
  std::vector<topic_info> topics;

  if (get_all_topic_info(topics, server_ip) != node::SUCCESS) {
    return false;
  }

  printf("Found %d topics on the system\n", (int)topics.size());
  for (auto& ti : topics) {
    if (verbose) {
      printf(" TOPIC:    %s\n", ti.topic_name.c_str());
      printf(" MSG NAME: %s\n", ti.msg_name.c_str());
      printf(" MSG HASH: %" PRIx64 "\n", ti.msg_hash);
      printf(" MSG TYPE: %s\n", TopicTypeToStr(ti.type));
      if (ti.cn_info.is_network) {
        printf(" NETWORK:  YES\n");
        printf(" CHN IP:   %s\n", ti.cn_info.channel_ip.c_str());
        printf(" CHN PORT: %d\n", ti.cn_info.channel_port);
      } else {
        printf(" NETWORK:  NO\n");
        printf(" CHN PATH: %s\n", ti.cn_info.channel_path.c_str());
        printf(" CHN SIZE: %d\n", ti.cn_info.channel_size);
      }
      printf(" PUB PID:  %d\n", ti.publisher_pid);
      printf(" VISIBLE:  %d\n", ti.visible);
      printf("\n");
    } else {
      printf("topic: %-20s type: %-8s message: %-20s  ", ti.topic_name.c_str(), TopicTypeToStr(ti.type),
             ti.msg_name.c_str());
      if (ti.cn_info.is_network) {
        printf("network: YES\n");
      } else {
        std::string pub_exe = "/proc/";
        pub_exe += std::to_string(ti.publisher_pid) + "/exe";
        char exebuffer[256] = {};
        auto unused = readlink(pub_exe.c_str(), exebuffer, sizeof(exebuffer));
        (void)unused;
        printf("network: NO  publisher:  %s\n", exebuffer);
      }
    }
  }

  return true;
}

static void printCentered(const char* strToPrint, int sz) {
  int strsz = strlen(strToPrint);
  int prespace = 0;
  int extraspace = 0;
  if (sz >= strsz) {
    prespace = (sz - strsz) / 2;
    extraspace = (sz - strsz) % 2;
    printf("%*s%s%*s", prespace, "", strToPrint, prespace + extraspace, "");
  } else {
    // What we print is smaller than the space we have, we have to compress
    printf("%*s%s", sz - 3, strToPrint, "...");
  }
}

static void printHeaderColor(bool& sw) {
  printf("%s", sw ? BG_CYAN : BG_MAGENTA);
  sw = !sw;
}

void handleInput(bool& up, bool& down) {
  char inbuf[6] = {0};
  int bytes = read(STDIN_FILENO, inbuf, 6);
  if (bytes > 0) {
    // Check for complex things first
    if (bytes == 3 && inbuf[0] == 0x1B && inbuf[1] == 0x5B && inbuf[2] == 0x41) {
      // Up arrow
      up = true;
    } else if (bytes == 3 && inbuf[0] == 0x1B && inbuf[1] == 0x5B && inbuf[2] == 0x42) {
      // Down arrow
      down = true;
    } else if (bytes == 3 && inbuf[0] == 0x1B && inbuf[1] == 0x5B && inbuf[2] == 0x43) {
      // Right arrow
    } else if (bytes == 3 && inbuf[0] == 0x1B && inbuf[1] == 0x5B && inbuf[2] == 0x44) {
      // Left arrow
    } else if (bytes == 3 && inbuf[0] == 0x1B && inbuf[1] == 0x5B && inbuf[2] == 0x48) {
      // Home key
    } else if (bytes == 3 && inbuf[0] == 0x1B && inbuf[1] == 0x5B && inbuf[2] == 0x46) {
      // End Key
    } else if (bytes == 4 && inbuf[0] == 0x1B && inbuf[1] == 0x5B && inbuf[2] == 0x35 && inbuf[3] == 0x7E) {
      // Pg Up
    } else if (bytes == 4 && inbuf[0] == 0x1B && inbuf[1] == 0x5B && inbuf[2] == 0x36 && inbuf[3] == 0x7E) {
      // Pg Down
    } else if (bytes == 1 && inbuf[0] == 0x1B) {
      // ESC key
      exit_process = true;
    }
  }
}

bool DoMonitorTopics(std::string& server_ip) {
  std::unordered_map<std::string, observer> obsmap;
  std::unordered_map<std::string, Rate> rates;
  double publish_rate, current_time;
  int rows_to_skip = 0;
  char linebuf[4096];
  publish_rate = 0.0;

  node::core ncore;

  // For now, this can keep the screen sane unless there is a fatal or such.
  // A better solution would be to force vlog to stderr or some other pipe,
  // and then print it at the bottom of the screen, but that is quite a bit of work
  set_log_level_string("ALWAYS");
  signal(SIGINT, handle_sigint);
  InitTerminalSize();
  clrscr();
  SetRawTerminal();
  hidecursor();

  while (!exit_process) {
    std::vector<topic_info> topics;
    if (auto err = get_all_topic_info(topics, server_ip); err != node::SUCCESS) {
      RevertTerminalSettings();
      showcursor();
      printf("Error obtaining topics on the system: %s\n", NodeErrorToStr(err));
      return false;
    }

    // Handle input here
    bool up = false, down = false;
    handleInput(up, down);
    if (exit_process) break;

    // Prepare the screen
    clrscr();
    const auto written_n = write(STDOUT_FILENO, "\x1b[H", 3);  // Set the cursor on the top left
    assert(written_n == 3);
    (void)written_n;

    // Let's center the title
    sprintf(linebuf, "NODE MON Topics being published in the system: %d", (int)topics.size());

    int cl = cols();
    int rw = rows();
    printf("%s", REVERSED);
    printCentered(linebuf, cl);
    printf("%s\r\n", RESET);

    // Look to see if we have new topics to subscribe to
    for (auto& ti : topics) {
      if (obsmap.count(ti.topic_name) == 0) {
        // create the observer if we have one
        auto obs = ncore.observe(ti.topic_name);
        obs.set_node_name("node mon");
        auto ret = obs.open(0.01f, 0.0f);
        if (ret != node::SUCCESS) {
          printf("Could not connect to topic %s\r\n", ti.topic_name.c_str());
          continue;
        }
        Rate rate;
        rate.addPoint(obs.get_num_published(), get_current_time());
        obsmap[ti.topic_name] = std::move(obs);
        rates[ti.topic_name] = rate;
      }
    }

    // Figure out how big data is, what can we do
    int max_topic_length = 0;
    int max_msg_type_length = 0;
    // Fixed length fields
    int msg_length = 10;
    int rate_length = 11;
    int type_length = 6;

    // Final field size
    int real_topic_length = 0;
    int real_msg_type_length = 0;

    for (auto& ti : topics) {
      max_topic_length = MAX(static_cast<int>(ti.topic_name.size()), max_topic_length);
      max_msg_type_length = MAX(static_cast<int>(ti.msg_name.size()), max_msg_type_length);
    }

    // For spacing considerations
    max_msg_type_length++;
    max_topic_length++;

    if (max_topic_length + max_msg_type_length + msg_length + rate_length + type_length + 5 <= cl) {
      // Everything fits!
      int extra = cl - 5 - msg_length - rate_length - type_length - max_topic_length - max_msg_type_length;
      real_topic_length = max_topic_length + extra / 2;
      real_msg_type_length = max_msg_type_length + extra / 2;
    } else {
      // We drop the message type, and we might have to squeeze the topic
      real_topic_length = cl - 4 - msg_length - rate_length - type_length;
    }

    // Print the column headers
    bool hdr_switch = false;
    printf("%s%s", BOLD, UNDERLINE);
    printHeaderColor(hdr_switch);
    printCentered("TOPIC", real_topic_length);
    if (real_msg_type_length > 0) {
      printHeaderColor(hdr_switch);
      printCentered("MSG TYPE", real_msg_type_length);
    }
    printHeaderColor(hdr_switch);
    printCentered("NUM", msg_length);
    printHeaderColor(hdr_switch);
    printCentered("RATE", rate_length);
    printHeaderColor(hdr_switch);
    printCentered("TYPE", type_length);
    printf("%s\r\n", RESET);

    // Figure out how many rows we can display
    int num_rows_screen = rw - 2;  // Header and title are taken
    int row_counter = 0;
    // It makes no sense that the std library uses unsigned ints in their API.
    if (static_cast<int>(topics.size()) < num_rows_screen) {
      rows_to_skip = 0;
    } else {
      if (up) rows_to_skip = MAX(rows_to_skip - 1, 0);
      if (down) rows_to_skip = MIN(static_cast<int>(topics.size()) - num_rows_screen, rows_to_skip + 1);
    }

    int local_rows_to_skip = rows_to_skip;
    // Print what we have
    for (auto& ti : topics) {
      if (local_rows_to_skip > 0) {
        local_rows_to_skip--;
        continue;
      }
      row_counter++;
      if (row_counter >= num_rows_screen) {
        continue;
      }
      if (obsmap.count(ti.topic_name) == 0) {
        printf("%s WARNING NOT IN TOPIC MAP, skipping", ti.topic_name.c_str());
      } else {
        auto& obs = obsmap[ti.topic_name];
        auto num_pub = obs.get_num_published();
        current_time = get_current_time();

        rates[ti.topic_name].addPoint(num_pub, current_time);
        publish_rate = rates[ti.topic_name].getRate();

        printf("%s%*s", ti.topic_name.c_str(), (int)(real_topic_length - ti.topic_name.size()), "");

        if (real_msg_type_length > 0) {
          printf("%s%*s", ti.msg_name.c_str(), (int)(real_msg_type_length - ti.msg_name.size()), "");
        }
        // Handle the number of messages published
        sprintf(linebuf, "%zu ", num_pub);
        printf("%*s%s", (int)(msg_length - strlen(linebuf)), "", linebuf);

        // Handle the message rate
        sprintf(linebuf, "%.2f/s ", publish_rate);
        printf("%*s%s", (int)(rate_length - strlen(linebuf)), "", linebuf);
      }
      // Handle the topic type
      if (ti.type == node::TopicType::RPC) {
        printf(" RPC\r\n");
      } else if (ti.type == node::TopicType::SINK_SRC) {
        printf(" SNK\r\n");
      } else {
        printf(" %s\r\n", ti.cn_info.is_network ? "NET" : "SHM");
      }
    }

    // Show everything, prepare the next loop
    fflush(stdout);
  }

  for (auto& [key, val] : obsmap) val.preclose();

  RevertTerminalSettings();
  showcursor();
  return true;
}

bool DoNodemasterInfo(const std::string& server_ip) {
  std::vector<std::string> active_peers;
  std::string process_name;
  double uptime = -1;
  int pid = -1;
  auto err = get_nodemaster_info(uptime, pid, process_name, active_peers);
  if (err == SUCCESS) {
    int hh, mm, ss;
    uint64_t sec = uint64_t(uptime);
    std::string str_uptime = "";
    hh = sec / 3600;
    mm = (sec - hh * 3600) / 60;
    ss = sec - hh * 3600 - mm * 60;
    if (hh > 0) {
      str_uptime += std::to_string(hh) + "h";
    }
    if (hh > 0 || mm > 0) {
      str_uptime += std::to_string(mm) + "m";
    }
    str_uptime += std::to_string(ss) + "s";
    printf("Nodemaster at %s - Pid: %d Uptime: %s Process Name: %s Active Peer list: [", server_ip.c_str(),
           pid, str_uptime.c_str(), process_name.c_str());
    for (auto& peer : active_peers) {
      printf("%s ", peer.c_str());
    }
    printf("]\n");
    return true;
  } else {
    printf("Nodemaster information error: %s\n", NodeErrorToStr(err));
    return false;
  }
}

bool DoKillNodemaster(const std::string& server_ip) {
  auto err = request_nodemaster_termination("nodecli", server_ip);
  if (err == SUCCESS) {
    printf("Nodemaster at %s terminated successfully\n", server_ip.c_str());
    return true;
  } else {
    printf("Nodemaster at %s termination error: %s\n", server_ip.c_str(), NodeErrorToStr(err));
    return false;
  }
}

bool DoPrintMsg(const std::string& topic_name) {
  node::core ncore;
  auto obs = ncore.observe(topic_name);
  obs.set_node_name("node print");
  auto err = obs.open(0.01f, 0.0f);
  if (err != node::SUCCESS) {
    printf("Could not connect to topic %s, error: %s\n", topic_name.c_str(), NodeErrorToStr(err));
    return false;
  }

  CBufParser parser;
  if (!parser.ParseMetadata(obs.msg_cbuftxt(), obs.msg_type())) {
    printf("Could not parse cbuf for type %s\n", obs.msg_type().c_str());
    return false;
  }

  if (!follow) {
    MsgBufPtr msg = obs.get_message(err, 100);
    if (err != node::SUCCESS) {
      printf("Could not get a message for topic %s, err: %s\n", topic_name.c_str(), NodeErrorToStr(err));
      return false;
    }

    parser.Print(obs.msg_type().c_str(), msg.getMem(), msg.getSize());
    obs.release_message(msg);
    return true;
  }

  // Setup terminal work
  signal(SIGINT, handle_sigint);
  // InitTerminalSize();
  // clrscr();
  // SetRawTerminal();
  // hidecursor();
  // const auto written_n = write(STDOUT_FILENO, "\x1b[H", 3);  // Set the cursor on the top left
  // assert(written_n == 3);
  // (void)written_n;

  while (!exit_process) {
    MsgBufPtr msg = obs.get_message(err, 100);
    if (err != node::SUCCESS) {
      printf("Could not get a message for topic %s, err: %s\n", topic_name.c_str(), NodeErrorToStr(err));
      break;
    }

    clrscr();
    const auto written_n = write(STDOUT_FILENO, "\x1b[H", 3);  // Set the cursor on the top left
    assert(written_n == 3);
    (void)written_n;

    parser.Print(obs.msg_type().c_str(), msg.getMem(), msg.getSize());
    obs.release_message(msg);
    fflush(stdout);
  }

  RevertTerminalSettings();
  showcursor();
  return true;
}

bool DoGetValue(const std::string& key) {
  std::string value;
  std::string owner;
  auto ret = get_value(key, value, owner);
  if (ret == NodeError::SUCCESS) {
    printf("%s%s", BOLD, GREEN);
    printf("%s : %s (owner: %s)", key.c_str(), value.c_str(), owner.c_str());
    printf("%s\r\n", RESET);
  } else {
    printf("%s%s", BOLD, RED);
    printf("%s not found in the store", key.c_str());
    printf("%s\r\n", RESET);
  }
  return ret;
}

bool DoSetValue(const std::string& key, const std::string& value) {
  auto ret = set_value(key, value);
  if (ret == NodeError::SUCCESS) {
    printf("%s%s", BOLD, GREEN);
    printf("Store updated with %s : %s", key.c_str(), value.c_str());
    printf("%s\r\n", RESET);
  } else {
    printf("%s%s", BOLD, RED);
    printf("Could not update the store");
    printf("%s\r\n", RESET);
  }
  return ret;
}

bool DoPrintStore(const std::string& regex = "") {
  std::vector<std::string> keys, values;
  auto ret = get_store(keys, values, "", true, regex);
  if (ret == NodeError::SUCCESS) {
    printf("%s%s", BOLD, GREEN);
    for (int i = 0; i < keys.size(); i++) {
      printf("%s : %s\n", keys[i].c_str(), values[i].c_str());
    }
    printf("%s\r\n", RESET);
  } else {
    printf("%s%s", BOLD, RED);
    printf("Could not print the store");
    printf("%s\r\n", RESET);
  }
  return ret;
}

bool DoShutDown(const std::string& host_ip = "") {
  NodeError err = request_nodemaster_shutdown("nodecli", host_ip);
  if (err != node::SUCCESS) return false;
  return true;
}

int main(int argc, char* argv[]) {
  std::string server_ip = SERVER_IP;
  std::string print_topic;
  std::string key;
  std::string value;
  std::string regex = "";
  Command cmd = UNKNOWN;
  int return_value = 0;

  if (argc < 2) Usage();

  int i = 1;
  while (i < argc) {
    if (!strcmp(argv[i], "--ip")) {
      if (i + 1 == argc) Usage();
      server_ip = argv[i + 1];
      i++;
    } else if (!strcmp(argv[i], "-v")) {
      verbose = true;
    } else if (!strcmp(argv[i], "-f")) {
      follow = true;
    } else if (!strcmp(argv[i], "-h")) {
      Usage();
    } else if (!strcmp(argv[i], "ls")) {
      if (cmd != UNKNOWN) {
        printf("Two commands cannot be used at once\n");
        exit(-1);
      }
      cmd = LIST_TOPICS;
    } else if (!strcmp(argv[i], "mon")) {
      if (cmd != UNKNOWN) {
        printf("Two commands cannot be used at once\n");
        exit(-1);
      }
      cmd = MONITOR_TOPICS;
    } else if (!strcmp(argv[i], "info")) {
      if (cmd != UNKNOWN) {
        printf("Two commands cannot be used at once\n");
        exit(-1);
      }
      cmd = NM_INFO;
    } else if (!strcmp(argv[i], "kill")) {
      if (cmd != UNKNOWN) {
        printf("Two commands cannot be used at once\n");
        exit(-1);
      }
      cmd = KILL_NM;
    } else if (!strcmp(argv[i], "print")) {
      if (cmd != UNKNOWN) {
        printf("Two commands cannot be used at once\n");
        exit(-1);
      }
      if (i + 1 == argc) Usage();
      print_topic = argv[i + 1];
      i++;
      cmd = PRINT_MSG;
    } else if (!strcmp(argv[i], "get")) {
      if (cmd != UNKNOWN) {
        printf("Two commands cannot be used at once\n");
        exit(-1);
      }
      if (i + 1 == argc) Usage();
      key = argv[i + 1];
      i++;
      cmd = GET_VALUE;
    } else if (!strcmp(argv[i], "set")) {
      if (cmd != UNKNOWN) {
        printf("Two commands cannot be used at once\n");
        exit(-1);
      }
      if (i + 1 == argc) Usage();
      key = argv[i + 1];
      i++;
      if (i + 1 == argc) Usage();
      value = argv[i + 1];
      i++;
      cmd = SET_VALUE;
    } else if (!strcmp(argv[i], "store")) {
      if (cmd != UNKNOWN) {
        printf("Two commands cannot be used at once\n");
        exit(-1);
      }
      if (i + 1 == argc) {
        regex = "";
      } else {
        regex = argv[i + 1];
        i++;
      }
      cmd = PRINT_STORE;
    } else if (!strcmp(argv[i], "shutdown")) {
      if (i + 1 != argc) {
        Usage();
      }
      cmd = SHUTDOWN;
    } else {
      printf("Unrecognized option: %s\n", argv[i]);
    }
    i++;
  }

  switch (cmd) {
    case LIST_TOPICS:
      return_value = DoListTopics(server_ip) ? 0 : -1;
      break;
    case MONITOR_TOPICS:
      return_value = DoMonitorTopics(server_ip) ? 0 : -1;
      break;
    case NM_INFO:
      return_value = DoNodemasterInfo(server_ip) ? 0 : -1;
      break;
    case KILL_NM:
      return_value = DoKillNodemaster(server_ip) ? 0 : -1;
      break;
    case PRINT_MSG:
      return_value = DoPrintMsg(print_topic) ? 0 : -1;
      break;
    case GET_VALUE:
      return_value = DoGetValue(key) ? 0 : -1;
      break;
    case SET_VALUE:
      return_value = DoSetValue(key, value) ? 0 : -1;
      break;
    case PRINT_STORE:
      return_value = DoPrintStore(regex) ? 0 : -1;
      break;
    case SHUTDOWN:
      return_value = DoShutDown(server_ip) ? 0 : -1;
      break;
    default:
      printf("\n Error: unknown command\n");
      return_value = -1;
  }

  return return_value;
}
