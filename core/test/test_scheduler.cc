#include <chrono>
#include <functional>
#include <iostream>

#include "../fiber/thread.h"
#include "../fiber/scheduler.h"

namespace {

constexpr std::chrono::milliseconds kTimeSlice{1000};
constexpr int kOddSlicesPerFiber = 5;

uint64_t GetSliceIndex() {
    using namespace std::chrono;
    const auto now = steady_clock::now().time_since_epoch();
    return duration_cast<milliseconds>(now).count() / kTimeSlice.count();
}

void YieldCurrentFiber() {
    Fiber::ptr self = Fiber::GetThis();
    // 把当前的 fiber 放回到调度队列里面，确保之后还能立刻被调度
    Scheduler::GetThis()->schedule(self);
    Fiber::YieldToHold();
}

void WaitUntilOddSlice() {
    while ((GetSliceIndex() & 1ull) == 0ull) {
        YieldCurrentFiber();
    }
}

void EmitWithinSlice(int fiber_id, uint64_t slice) {
    while (GetSliceIndex() == slice) {
        std::cout << "[fiber " << fiber_id << "] get_here on slice " << slice << "\n";
        YieldCurrentFiber();
    }
}

void test_fiber(int fiber_id) {
    int handled = 0;
    while (handled < kOddSlicesPerFiber) {
        WaitUntilOddSlice();
        const uint64_t slice = GetSliceIndex();
        EmitWithinSlice(fiber_id, slice);
        ++handled;
    }
}

} // namespace

int main(int argc, char** argv) {
    std::cout << "scheduler test begin\n";
    Scheduler sc(3, false, "test");
    sc.start();

    for (int i = 0; i < 100; ++i) {
        sc.schedule(std::bind(&test_fiber, i));
    }

    sc.stop();
    std::cout << "scheduler test end\n";
    return 0;
}
