#include <algorithm>

#include "gtest/gtest.h"
#include "node/node_utils.h"
#include "node_registry.h"

TEST(KeyValueTest, get_events) {
  node::node_registry registry = node::node_registry();
  registry.set_registry_ip(get_host_ips());
  registry.set_value("key1", "value1", "sb0");
  registry.set_value("key2", "value2", "sb0");
  registry.set_value("key3", "value3", "sb0");
  std::vector<std::string> keys, values;
  registry.get_current_store_snapshot(keys, values, true);
  ASSERT_EQ(keys.size(), 3);
  ASSERT_EQ(values.size(), 3);
}

TEST(KeyValueTest, get_value) {
  node::node_registry registry = node::node_registry();
  registry.set_registry_ip(get_host_ips());
  registry.set_value("key1", "value1", "sb0");
  registry.set_value("key2", "value2", "sb0");
  registry.set_value("key3", "value3", "sb0");
  std::string value = registry.get_value("key2");
  ASSERT_EQ(value, "value2");
}

TEST(KeyValueTest, update_store) {
  node::node_registry registry = node::node_registry();
  registry.set_registry_ip(get_host_ips());
  std::vector<std::string> keys = {"1", "2", "3", "4"};
  std::vector<std::string> values = {"1", "2", "3", "4"};
  std::string owner = "test_1";
  ASSERT_TRUE(registry.update_store(keys, values, owner));
  std::vector<std::string> keys_ret, values_ret;
  registry.get_current_store_snapshot(keys_ret, values_ret, true);
  std::set<std::string> keys_orig(keys.begin(), keys.end()), keys_ret_set(keys_ret.begin(), keys_ret.end());
  std::set<std::string> values_orig(values.begin(), values.end()),
      values_ret_set(values_ret.begin(), values_ret.end());
  ASSERT_TRUE(std::equal(keys_orig.begin(), keys_orig.end(), keys_ret_set.begin()));
  ASSERT_TRUE(std::equal(values_orig.begin(), values_orig.end(), values_ret_set.begin()));
}

TEST(KeyValueTest, different_owner) {
  node::node_registry registry = node::node_registry();
  registry.set_registry_ip(get_host_ips());
  std::vector<std::string> keys = {"1", "2", "3", "4"};
  std::vector<std::string> values = {"1", "2", "3", "4"};
  std::string owner = "test_1";
  ASSERT_TRUE(registry.update_store(keys, values, owner));
  ASSERT_FALSE(registry.set_value("1", "4", "test_2"));
  ASSERT_FALSE(registry.update_store(keys, values, "test_2"));
  std::vector<std::string> keys_ret, values_ret;
  registry.get_current_store_snapshot(keys_ret, values_ret, true);
  std::set<std::string> keys_orig(keys.begin(), keys.end()), keys_ret_set(keys_ret.begin(), keys_ret.end());
  std::set<std::string> values_orig(values.begin(), values.end()),
      values_ret_set(values_ret.begin(), values_ret.end());
  ASSERT_TRUE(std::equal(keys_orig.begin(), keys_orig.end(), keys_ret_set.begin()));
  ASSERT_TRUE(std::equal(values_orig.begin(), values_orig.end(), values_ret_set.begin()));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
