//  Copyright (c) 2019 Thomas Heller
//  Copyright (c) 2022 Hartmut Kaiser
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <hpx/config.hpp>
#include <hpx/execution_base/stdexec_forward.hpp>
#include <hpx/execution_base/this_thread.hpp>

#include <thread>

namespace hpx::this_thread::experimental {

    HPX_CXX_CORE_EXPORT using std::this_thread::get_id;
    HPX_CXX_CORE_EXPORT using std::this_thread::sleep_for;
    HPX_CXX_CORE_EXPORT using std::this_thread::sleep_until;
    HPX_CXX_CORE_EXPORT using std::this_thread::yield;

    HPX_CXX_CORE_EXPORT using stdexec::execute_may_block_caller;
    HPX_CXX_CORE_EXPORT using stdexec::execute_may_block_caller_t;

    // this_thread::sync_wait is loaded in the sync_wait.hpp file.
}    // namespace hpx::this_thread::experimental
