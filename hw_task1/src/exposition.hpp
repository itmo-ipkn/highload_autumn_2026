#pragma once

#include <prometheus/registry.h>

#include <string>

std::string SerializeRegistry(const prometheus::Registry& registry);
