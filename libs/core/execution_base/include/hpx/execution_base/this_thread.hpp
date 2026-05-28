//  Copyright (c) 2019 Thomas Heller
//  Copyright (c) 2022 Hartmut Kaiser
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <hpx/config.hpp>
#include <hpx/execution_base/agent_base.hpp>
#include <hpx/execution_base/agent_ref.hpp>
#include <hpx/execution_base/yield_while.hpp>
#include <hpx/modules/timing.hpp>

#include <chrono>
#include <cstdint>

namespace hpx::execution_base {

    namespace detail {

        HPX_CXX_CORE_EXPORT HPX_CORE_EXPORT agent_base& get_default_agent();
    }

    ///////////////////////////////////////////////////////////////////////////
    namespace this_thread {

        namespace detail {

            HPX_CXX_CORE_EXPORT struct agent_storage;
            HPX_CXX_CORE_EXPORT HPX_CORE_EXPORT agent_storage*
            get_agent_storage();
        }    // namespace detail

        HPX_CXX_CORE_EXPORT struct HPX_CORE_EXPORT reset_agent
        {
            explicit reset_agent(agent_base& impl);
            reset_agent(detail::agent_storage*, agent_base& impl);

            reset_agent(reset_agent const&) = delete;
            reset_agent(reset_agent&&) = delete;
            reset_agent& operator=(reset_agent const&) = delete;
            reset_agent& operator=(reset_agent&&) = delete;

            ~reset_agent();

            detail::agent_storage* storage_;
            agent_base* old_;
        };

        HPX_CXX_CORE_EXPORT HPX_CORE_EXPORT hpx::execution_base::agent_ref
        agent();

        HPX_CXX_CORE_EXPORT HPX_CORE_EXPORT void yield(
            char const* desc = "hpx::execution_base::this_thread::yield");
        HPX_CXX_CORE_EXPORT HPX_CORE_EXPORT bool yield_k(std::size_t k,
            char const* desc = "hpx::execution_base::this_thread::yield_k");
        HPX_CXX_CORE_EXPORT HPX_CORE_EXPORT void suspend(
            char const* desc = "hpx::execution_base::this_thread::suspend");

        HPX_CXX_CORE_EXPORT template <typename Rep, typename Period>
        void sleep_for(std::chrono::duration<Rep, Period> const& sleep_duration,
            char const* desc = "hpx::execution_base::this_thread::sleep_for")
        {
            agent().sleep_for(sleep_duration, desc);
        }

        HPX_CXX_CORE_EXPORT template <class Clock, class Duration>
        void sleep_until(
            std::chrono::time_point<Clock, Duration> const& sleep_time,
            char const* desc = "hpx::execution_base::this_thread::sleep_for")
        {
            agent().sleep_until(sleep_time, desc);
        }
    }    // namespace this_thread
}    // namespace hpx::execution_base
