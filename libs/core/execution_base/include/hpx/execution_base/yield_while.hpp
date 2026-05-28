//  Copyright (c) 2019 Thomas Heller
//  Copyright (c) 2022 Hartmut Kaiser
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <hpx/config.hpp>
#include <hpx/modules/timing.hpp>

#ifdef HPX_HAVE_SPINLOCK_DEADLOCK_DETECTION
#include <hpx/execution_base/detail/spinlock_deadlock_detection.hpp>
#include <hpx/modules/errors.hpp>
#endif

#include <chrono>
#include <cstddef>

namespace hpx::execution_base::this_thread {
    HPX_CXX_CORE_EXPORT HPX_CORE_EXPORT bool yield_k(std::size_t k,
        char const* desc);
}    // namespace hpx::execution_base::this_thread

namespace hpx::util {

    namespace detail {

        HPX_CXX_CORE_EXPORT inline bool yield_k(
            std::size_t k, char const* thread_name)
        {
#ifdef HPX_HAVE_SPINLOCK_DEADLOCK_DETECTION
            if (k > 32 && get_spinlock_break_on_deadlock_enabled() &&
                k > get_spinlock_deadlock_detection_limit())
            {
                HPX_THROW_EXCEPTION(hpx::error::deadlock, thread_name,
                    "possible deadlock detected");
            }
#endif
            return hpx::execution_base::this_thread::yield_k(k, thread_name);
        }
    }    // namespace detail

    HPX_CXX_CORE_EXPORT template <bool AllowTimedSuspension, typename Predicate>
    void yield_while(Predicate&& predicate, char const* thread_name = nullptr)
    {
        for (std::size_t k = 0; predicate(); ++k)
        {
            if constexpr (AllowTimedSuspension)
            {
                detail::yield_k(k, thread_name);
            }
            else
            {
                detail::yield_k(k % 16, thread_name);
            }
        }
    }

    HPX_CXX_CORE_EXPORT template <typename Predicate>
    void yield_while(Predicate&& predicate, char const* thread_name = nullptr,
        bool allow_timed_suspension = true)
    {
        if (allow_timed_suspension)
        {
            yield_while<true>(HPX_FORWARD(Predicate, predicate), thread_name);
        }
        else
        {
            yield_while<false>(HPX_FORWARD(Predicate, predicate), thread_name);
        }
    }

    namespace detail {

        // yield_while_count yields until the predicate returns true
        // required_count times consecutively. This function is used in cases
        // where there is a small false positive rate and repeatedly calling the
        // predicate reduces the rate of false positives overall.
        //
        // Note: This is mostly a hack used to work around the raciness of
        // termination detection for thread pools and the runtime and can be
        // replaced if and when a better solution appears.
        HPX_CXX_CORE_EXPORT template <typename Predicate>
        void yield_while_count(Predicate&& predicate,
            std::size_t required_count, char const* thread_name = nullptr,
            bool allow_timed_suspension = true)
        {
            std::size_t count = 0;
            for (std::size_t k = 0; /**/; ++k)
            {
                if (!predicate())
                {
                    if (++count > required_count)
                    {
                        return;
                    }
                }
                else
                {
                    count = 0;
                    detail::yield_k(
                        allow_timed_suspension ? k : k % 16, thread_name);
                }
            }
        }

        // yield_while_count_timeout is similar to yield_while_count, with the
        // addition of a timeout parameter. If the timeout is exceeded, waiting
        // is stopped and the function returns false. If the predicate is
        // successfully waited for the function returns true.
        HPX_CXX_CORE_EXPORT template <typename Predicate>
        bool yield_while_count_timeout(Predicate&& predicate,
            std::size_t required_count,
            hpx::chrono::steady_duration const& rel_time,
            char const* thread_name = nullptr,
            bool allow_timed_suspension = true)
        {
            bool const use_timeout =
                rel_time.value() != std::chrono::steady_clock::duration(0);

            auto const abs_time = rel_time.from_now();

            std::size_t count = 0;
            for (std::size_t k = 0; /**/; ++k)
            {
                if (use_timeout &&
                    abs_time <= std::chrono::steady_clock().now())
                {
                    return false;
                }

                if (!predicate())
                {
                    if (++count > required_count)
                    {
                        return true;
                    }
                }
                else
                {
                    count = 0;
                    detail::yield_k(
                        allow_timed_suspension ? k : k % 16, thread_name);
                }
            }
        }

        // yield_while_count_timeout is similar to yield_while_count, with the
        // addition of a timeout parameter and a stop_token. If the timeout is
        // exceeded or stop is requested, waiting is stopped and the function
        // returns false. If the predicate is successfully waited for the
        // function returns true.
        HPX_CXX_CORE_EXPORT template <typename Predicate, typename StopToken>
        bool yield_while_count_timeout(Predicate&& predicate,
            StopToken&& stop_token, std::size_t required_count,
            hpx::chrono::steady_duration const& rel_time,
            char const* thread_name = nullptr,
            bool allow_timed_suspension = true)
        {
            bool const use_timeout =
                rel_time.value() != std::chrono::steady_clock::duration(0);

            auto const abs_time = rel_time.from_now();

            std::size_t count = 0;
            for (std::size_t k = 0; /**/; ++k)
            {
                if (stop_token.stop_requested())
                {
                    return false;
                }

                if (use_timeout && abs_time <= hpx::chrono::steady_clock::now())
                {
                    return false;
                }

                if (!predicate())
                {
                    if (++count > required_count)
                    {
                        return true;
                    }
                }
                else
                {
                    count = 0;
                    detail::yield_k(
                        allow_timed_suspension ? k : k % 16, thread_name);
                }
            }
        }
    }    // namespace detail
}    // namespace hpx::util
