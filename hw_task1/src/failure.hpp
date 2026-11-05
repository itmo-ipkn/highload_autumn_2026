#pragma once

#include <cstdlib>
#include <random>

class FailureInjector {
public:
    FailureInjector();

    // Возвращает true, если текущий запрос должен завершиться ошибкой 500.
    bool ShouldFail();

private:
    double rate_;
    std::mt19937 generator_;
    std::uniform_real_distribution<double> distribution_{0.0, 1.0};
};