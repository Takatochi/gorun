#include <cassert>
#include <iostream>
#include "gorun/gorun.hpp"

int main() {
    gorun::Channel<int> ch(1);
    gorun::go(gorun::StackSize::Small, [&]() {
        ch.send(42);
    });
    auto val = ch.recv();
    assert(val.has_value() && *val == 42);
    std::cout << "gorun package test OK (" << gorun::detail::kArchitectureName << ")\n";
    return 0;
}
