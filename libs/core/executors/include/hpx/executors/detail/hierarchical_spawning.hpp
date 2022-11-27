//  Copyright (c) 2019-2020 ETH Zurich
//  Copyright (c) 2007-2022 Hartmut Kaiser
//  Copyright (c) 2019 Agustin Berge
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <hpx/config.hpp>
#include <hpx/assert.hpp>
#include <hpx/async_base/launch_policy.hpp>
#include <hpx/async_base/scheduling_properties.hpp>
#include <hpx/coroutines/thread_enums.hpp>
#include <hpx/errors/try_catch_exception_ptr.hpp>
#include <hpx/execution/algorithms/detail/predicates.hpp>
#include <hpx/execution/detail/async_launch_policy_dispatch.hpp>
#include <hpx/execution/detail/post_policy_dispatch.hpp>
#include <hpx/execution/executors/execution.hpp>
#include <hpx/execution/executors/fused_bulk_execute.hpp>
#include <hpx/functional/invoke.hpp>
#include <hpx/futures/future.hpp>
#include <hpx/futures/traits/future_traits.hpp>
#include <hpx/iterator_support/range.hpp>
#include <hpx/pack_traversal/unwrap.hpp>
#include <hpx/synchronization/latch.hpp>
#include <hpx/threading_base/scheduler_base.hpp>
#include <hpx/threading_base/thread_data.hpp>
#include <hpx/threading_base/thread_helpers.hpp>
#include <hpx/threading_base/thread_pool_base.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <type_traits>
#include <utility>
#include <vector>

#include <hpx/concurrency/cache_line_data.hpp>
#include <hpx/concurrency/detail/non_contiguous_index_queue.hpp>
#include <optional>

namespace hpx { namespace parallel { namespace execution { namespace detail {

    ////////////////////////////////////////////////////////////////////////////

    using index_queue_type = hpx::util::cache_aligned_data<
        hpx::concurrency::detail::non_contiguous_index_queue<>>;

    template <typename Policy>
    index_queue_type get_index_queue(Policy policy,
        std::uint32_t const worker_thread, std::uint32_t const size,
        std::uint32_t num_threads)
    {
        index_queue_type queue;
        // thread placement
        hpx::threads::thread_schedule_hint hint =
            hpx::execution::experimental::get_hint(policy);

        using placement = hpx::threads::thread_placement_hint;

        if (hint.placement_mode == placement::breadth_first ||
            hint.placement_mode == placement::breadth_first_reverse)
        {
            auto const num_steps = size / num_threads + 1;
            auto const part_begin = worker_thread;
            auto part_end = (std::min)(
                size + num_threads - 1, part_begin + num_steps * num_threads);
            auto const remainder = (part_end - part_begin) % num_threads;
            if (remainder != 0)
            {
                part_end -= remainder;
            }
            queue.data_.reset(part_begin, part_end, num_threads);
        }
        else
        {
            // the default placement is depth-first
            auto const part_begin = static_cast<std::uint32_t>(
                (worker_thread * size) / num_threads);
            auto const part_end = static_cast<std::uint32_t>(
                ((worker_thread + 1) * size) / num_threads);
            queue.data_.reset(part_begin, part_end);
        }
        return queue;
    }

    template <typename Launch, typename F, typename S, typename... Ts>
    std::vector<hpx::future<detail::bulk_function_result_t<F, S, Ts...>>>
    hierarchical_bulk_async_execute_helper(
        hpx::util::thread_description const& desc,
        threads::thread_pool_base* pool, std::size_t first_thread,
        std::size_t num_threads, std::size_t hierarchical_threshold,
        Launch policy, F&& f, S const& shape, Ts&&... ts)
    {
        HPX_ASSERT(pool);

        using result_type = std::vector<
            hpx::future<typename detail::bulk_function_result_t<F, S, Ts...>>>;

        result_type results;
        std::size_t const size = hpx::util::size(shape);
        results.resize(size);

        auto post_policy = hpx::execution::experimental::with_stacksize(
            policy, threads::thread_stacksize::small_);

        hpx::latch l(size);
        size_t part_size = size / num_threads;

        for (std::size_t t = 0; t != num_threads; ++t)
        {
            auto async_policy = hpx::execution::experimental::with_hint(policy,
                threads::thread_schedule_hint{
                    static_cast<std::int16_t>(first_thread + t)});

            auto index_queue = get_index_queue(policy, t, size, num_threads);

            if (part_size > hierarchical_threshold)
            {
                hpx::detail::post_policy_dispatch<Launch>::call(
                    post_policy, desc, pool, [&, f]() mutable {
                        hpx::optional<std::uint32_t> index;
                        while ((index = index_queue.data_.pop_left()))
                        {
                            auto it =
                                std::next(std::begin(shape), index.value());
                            results[index.value()] =
                                hpx::detail::async_launch_policy_dispatch<
                                    Launch>::call(async_policy, desc, pool, f,
                                    *it, ts...);
                            l.count_down(1);
                        }
                    });
            }
            else
            {
                hpx::optional<std::uint32_t> index;
                while ((index = index_queue.data_.pop_left()))
                {
                    auto it = std::next(std::begin(shape), index.value());
                    results[index.value()] =
                        hpx::detail::async_launch_policy_dispatch<Launch>::call(
                            async_policy, desc, pool, f, *it, ts...);
                    l.count_down(1);
                }
            }
        }
        // HPX_ASSERT(it == hpx::util::end(shape));

        l.wait();

        return results;
    }

    // This specialization avoids creating a future for each of the scheduled
    // tasks. It also avoids an additional allocation by directly returning a
    // hpx::future.
    template <typename Launch, typename F, typename S, typename... Ts>
    decltype(auto) hierarchical_bulk_async_execute_void(
        hpx::util::thread_description const& desc,
        threads::thread_pool_base* pool, std::size_t first_thread,
        std::size_t num_threads, std::size_t hierarchical_threshold,
        Launch policy, F&& f, S const& shape, Ts&&... ts)
    {
        HPX_ASSERT(pool);

        return hpx::detail::async_launch_policy_dispatch<Launch>::call(
            policy, desc, pool,
            [](hpx::util::thread_description const& desc,
                threads::thread_pool_base* pool, std::size_t first_thread,
                std::size_t num_threads, std::size_t hierarchical_threshold,
                Launch policy, std::decay_t<F> f, S const& shape,
                std::decay_t<Ts>... ts) {
                std::size_t const size = hpx::util::size(shape);
                auto post_policy = hpx::execution::experimental::with_stacksize(
                    policy, threads::thread_stacksize::small_);

                std::exception_ptr e;
                hpx::spinlock mtx_e;
                hpx::latch l(size);
                size_t part_size = size / num_threads;

                auto wrapped = [&, f](auto&&... args) mutable {
                    // properly handle all exceptions thrown from 'f'
                    hpx::detail::try_catch_exception_ptr(
                        [&]() {
                            HPX_INVOKE(f, HPX_FORWARD(decltype(args), args)...);
                        },
                        [&](std::exception_ptr ep) {
                            // store the first caught exception only
                            std::lock_guard<hpx::spinlock> lg(mtx_e);
                            if (!e)
                                e = HPX_MOVE(ep);
                        });
                    l.count_down(1);
                };

                for (std::size_t t = 0; t != num_threads; ++t)
                {
                    auto inner_post_policy =
                        hpx::execution::experimental::with_hint(policy,
                            threads::thread_schedule_hint{
                                static_cast<std::int16_t>(first_thread + t)});

                    auto index_queue =
                        get_index_queue(policy, t, size, num_threads);

                    auto&& launcher = [&, wrapped, index_queue](
                                          bool direct) mutable {
                        // reserve last task for direct execution, if needed
                        std::optional<std::uint32_t> direct_index;
                        if (direct)
                            direct_index = index_queue.data_.pop_right();

                        hpx::optional<std::uint32_t> index;
                        while ((index = index_queue.data_.pop_left()))
                        {
                            auto iter =
                                std::next(std::begin(shape), index.value());
                            hpx::detail::post_policy_dispatch<Launch>::call(
                                inner_post_policy, desc, pool, wrapped, *iter,
                                ts...);
                        }

                        // execute last task directly
                        if (direct_index)
                        {
                            auto iter = std::next(
                                std::begin(shape), direct_index.value());
                            HPX_INVOKE(wrapped, *iter, ts...);
                        }
                    };

                    // launch a special thread to schedule work for each core,
                    // except the last one
                    if (t != num_threads - 1 &&
                        part_size > hierarchical_threshold)
                    {
                        hpx::detail::post_policy_dispatch<Launch>::call(
                            post_policy, desc, pool, HPX_MOVE(launcher), true);
                    }
                    else    //if (part_size != 0)
                    {
                        launcher(t == num_threads - 1);
                    }
                }
                //HPX_ASSERT(end == size);

                l.wait();

                // rethrow any exceptions caught during processing the
                // bulk_execute, note that we don't need to acquire the lock
                // at this point as no other threads may access the exception
                // concurrently
                if (e)
                {
                    std::rethrow_exception(HPX_MOVE(e));
                }
            },
            desc, pool, first_thread, num_threads, hierarchical_threshold,
            policy, HPX_FORWARD(F, f), shape, HPX_FORWARD(Ts, ts)...);
    }

    template <typename Launch, typename F, typename S, typename... Ts>
    decltype(auto) hierarchical_bulk_async_execute(
        hpx::util::thread_description const& desc,
        threads::thread_pool_base* pool, std::size_t first_thread,
        std::size_t num_threads, std::size_t hierarchical_threshold,
        Launch policy, F&& f, S const& shape, Ts&&... ts)
    {
        using result_type = detail::bulk_function_result_t<F, S, Ts...>;
        if constexpr (!std::is_void_v<result_type>)
        {
            return hierarchical_bulk_async_execute_helper(desc, pool,
                first_thread, num_threads, hierarchical_threshold, policy,
                HPX_FORWARD(F, f), shape, HPX_FORWARD(Ts, ts)...);
        }
        else
        {
            return hierarchical_bulk_async_execute_void(desc, pool,
                first_thread, num_threads, hierarchical_threshold, policy,
                HPX_FORWARD(F, f), shape, HPX_FORWARD(Ts, ts)...);
        }
    }

    template <typename Launch, typename F, typename S, typename... Ts>
    decltype(auto) hierarchical_bulk_async_execute(
        threads::thread_pool_base* pool, std::size_t first_thread,
        std::size_t num_threads, std::size_t hierarchical_threshold,
        Launch policy, F&& f, S const& shape, Ts&&... ts)
    {
        hpx::util::thread_description const desc(
            f, "hierarchical_bulk_async_execute");

        return hierarchical_bulk_async_execute(desc, pool, first_thread,
            num_threads, hierarchical_threshold, policy, HPX_FORWARD(F, f),
            shape, HPX_FORWARD(Ts, ts)...);
    }

    ////////////////////////////////////////////////////////////////////////////
    template <typename Executor, typename Launch, typename F, typename S,
        typename Future, typename... Ts>
    hpx::future<detail::bulk_then_execute_result_t<F, S, Future, Ts...>>
    hierarchical_bulk_then_execute_helper(Executor&& executor, Launch policy,
        F&& f, S const& shape, Future&& predecessor, Ts&&... ts)
    {
        auto&& func = detail::make_fused_bulk_async_execute_helper(executor,
            HPX_FORWARD(F, f), shape, hpx::make_tuple(HPX_FORWARD(Ts, ts)...));

        // void or std::vector<func_result_type>
        using vector_result_type =
            detail::bulk_then_execute_result_t<F, S, Future, Ts...>;

        // future<vector_result_type>
        using result_future_type = hpx::future<vector_result_type>;

        using shared_state_type =
            hpx::traits::detail::shared_state_ptr_t<vector_result_type>;

        using future_type = std::decay_t<Future>;

        // vector<future<func_result_type>> -> vector<func_result_type>
        shared_state_type p = hpx::lcos::detail::make_continuation_exec_policy<
            vector_result_type>(HPX_FORWARD(Future, predecessor), executor,
            policy,
            [func = HPX_MOVE(func)](
                future_type&& predecessor) mutable -> vector_result_type {
                // use unwrap directly (instead of lazily) to avoid
                // having to pull in dataflow
                return hpx::unwrap(func(HPX_MOVE(predecessor)));
            });

        return hpx::traits::future_access<result_future_type>::create(
            HPX_MOVE(p));
    }
}}}}    // namespace hpx::parallel::execution::detail
