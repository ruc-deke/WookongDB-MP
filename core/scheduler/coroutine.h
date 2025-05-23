// Author: Chunyue Huang
// Copyright (c) 2024

#pragma once

// Use symmetric_coroutine from boost::coroutine, not asymmetric_coroutine from boost::coroutine2
// symmetric_coroutine meets transaction processing, in which each coroutine can freely yield to another
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING

#include <boost/coroutine/all.hpp>

#include "common.h"

using coro_call_t = boost::coroutines::symmetric_coroutine<void>::call_type;

using coro_yield_t = boost::coroutines::symmetric_coroutine<void>::yield_type;

// For coroutine scheduling
struct Coroutine {
  Coroutine() {}

  // My coroutine ID
  coro_id_t coro_id;

  // Registered coroutine function
  coro_call_t func;

  // bool working = false;
};