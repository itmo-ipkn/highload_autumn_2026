#include "exposition.hpp"

#include <prometheus/text_serializer.h>

#include <sstream>

std::string SerializeRegistry(const prometheus::Registry& registry) {
    std::ostringstream out;
    prometheus::TextSerializer serializer;
    serializer.Serialize(out, registry.Collect());
    return out.str();
}