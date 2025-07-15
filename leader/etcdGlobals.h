#pragma once
#include <etcd/Client.hpp>
#include <string>

inline std::string node_ip, node_id;
inline etcd::Client etcdClient("http://127.0.0.1:2379");
inline etcd::Client etcdClientWallet("http://127.0.0.1:2379");