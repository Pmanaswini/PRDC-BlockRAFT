#ifndef SHA512_H
#define SHA512_H

#include <openssl/sha.h>

#include <iomanip>
#include <sstream>
#include <string>

std::string sha512(const std::string& data) {
  unsigned char hash[SHA512_DIGEST_LENGTH];
  SHA512(reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash);

  std::stringstream ss;
  for (unsigned char c : hash) {
    ss << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<unsigned int>(c);
  }
  return ss.str();
}

#endif
