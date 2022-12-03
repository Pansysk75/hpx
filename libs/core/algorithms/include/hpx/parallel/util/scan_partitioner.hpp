//  Copyright (c) 2007-2018 Hartmut Kaiser
//  Copyright (c)      2015 Daniel Bourgeois
//  Copyright (c)      2017 Taeguk Kwon
//  Copyright (c)      2021 Akhil J Nair
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <hpx/config.hpp>
#include <hpx/assert.hpp>
#include <hpx/async_combinators/wait_all.hpp>
#include <hpx/modules/errors.hpp>
#if !defined(HPX_COMPUTE_DEVICE_CODE)
#include <hpx/async_local/dataflow.hpp>
#endif

#include <hpx/execution/executors/execution.hpp>
#include <hpx/execution/executors/execution_parameters.hpp>
#include <hpx/executors/execution_policy.hpp>
#include <hpx/parallel/util/detail/algorithm_result.hpp>
#include <hpx/parallel/util/detail/chunk_size.hpp>
#include <hpx/parallel/util/detail/handle_local_exceptions.hpp>
#include <hpx/parallel/util/detail/scoped_executor_parameters.hpp>
#include <hpx/parallel/util/detail/select_partitioner.hpp>

#include <algorithm>
#include <cstddef>
#include <exception>
#include <list>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

///////////////////////////////////////////////////////////////////////////////
namespace hpx { namespace parallel { namespace util {

    ///////////////////////////////////////////////////////////////////////////
    namespace detail {
        ///////////////////////////////////////////////////////////////////////
        // The static partitioner simply spawns one chunk of iterations for
        // each available core.
        template <typename ExPolicy, typename R, typename Result1,
            typename Result2>
        struct scan_static_partitioner
        {
            using parameters_type = typename ExPolicy::executor_parameters_type;
            using executor_type = typename ExPolicy::executor_type;

            using scoped_executor_parameters =
                detail::scoped_executor_parameters_ref<parameters_type,
                    executor_type>;

            using handle_local_exceptions =
                detail::handle_local_exceptions<ExPolicy>;

            template <typename ExPolicy_, typename FwdIter, typename T,
                typename F1, typename F2, typename F3, typename F4>
            static R call(ExPolicy_ policy, FwdIter first, std::size_t count,
                T&& init, F1&& f1, F2&& f2, F3&& f3, F4&& f4)
            {
#if defined(HPX_COMPUTE_DEVICE_CODE)
                HPX_UNUSED(policy);
                HPX_UNUSED(first);
                HPX_UNUSED(count);
                HPX_UNUSED(init);
                HPX_UNUSED(f1);
                HPX_UNUSED(f2);
                HPX_UNUSED(f3);
                HPX_UNUSED(f4);
                HPX_ASSERT(false);
                return R();
#else
                // inform parameter traits
                scoped_executor_parameters scoped_params(
                    policy.parameters(), policy.executor());

                std::vector<hpx::future<Result1>> workitems;
                std::vector<hpx::future<Result2>> finalitems;
                std::vector<Result1> f2results;
                std::list<std::exception_ptr> errors;
                try
                {
                    HPX_ASSERT(count > 0);

                    auto shape =
                        detail::get_bulk_iteration_shape(policy, first, count);

                    // schedule every chunk on a separate thread
                    std::size_t size = hpx::util::size(shape);

                    //TODO: implement testing thing I removed

                    // Schedule first step of scan algorithm, step 2 is
                    // performed when all f1 tasks are done
                    workitems = execution::bulk_async_execute(
                        policy.executor(),
                        [f1](auto const& elem) {
                            FwdIter it = hpx::get<0>(elem);
                            std::size_t size = hpx::get<1>(elem);
                            return HPX_INVOKE(f1, it, size);
                            ;
                        },
                        shape);

                    // Wait for all f1 tasks to finish
                    if (hpx::wait_all_nothrow(workitems))
                    {
                        handle_local_exceptions::call(workitems, errors);
                    }

                    // perform f2 sequentially in one go
                    f2results.resize(workitems.size());
                    auto result = init;
                    f2results[0] = result;
                    for (std::size_t i = 1; i < workitems.size(); i++)
                    {
                        result = HPX_INVOKE(f2, result, workitems[i].get());
                        f2results[i] = result;
                    }

                    // start all f3 tasks

                    auto _finalitems = execution::bulk_async_execute(
                        policy.executor(),
                        [f3, &shape, &f2results](auto i) mutable {
                            auto iter = std::begin(shape);
                            std::advance(iter, i);
                            auto elem = *iter;
                            return HPX_INVOKE(f3, hpx::get<0>(elem),
                                hpx::get<1>(elem), f2results[i]);
                        },
                        shape.size());

                    _finalitems.wait();

                    scoped_params.mark_end_of_scheduling();
                }
                catch (...)
                {
                    handle_local_exceptions::call(
                        std::current_exception(), errors);
                }
                return reduce(HPX_MOVE(f2results), HPX_MOVE(finalitems),
                    HPX_MOVE(errors), HPX_FORWARD(F4, f4));
#endif
            }

        private:
            template <typename F>
            static R reduce(std::vector<Result1>&& workitems,
                std::vector<hpx::future<Result2>>&& finalitems,
                std::list<std::exception_ptr>&& errors, F&& f)
            {
#if defined(HPX_COMPUTE_DEVICE_CODE)
                HPX_UNUSED(workitems);
                HPX_UNUSED(finalitems);
                HPX_UNUSED(errors);
                HPX_UNUSED(f);
                HPX_ASSERT(false);
                return R();
#else
                // wait for all tasks to finish
                if (hpx::wait_all_nothrow(finalitems) || !errors.empty())
                {
                    // always rethrow if 'errors' is not empty or 'finalitems'
                    // have an exceptional future
                    handle_local_exceptions::call(finalitems, errors);
                }

                try
                {
                    return f(HPX_MOVE(workitems), HPX_MOVE(finalitems));
                }
                catch (...)
                {
                    // rethrow either bad_alloc or exception_list
                    handle_local_exceptions::call(std::current_exception());
                }
#endif
            }
        };

        ///////////////////////////////////////////////////////////////////////
        template <typename ExPolicy, typename R, typename Result1,
            typename Result2>
        struct scan_task_static_partitioner
        {
            template <typename ExPolicy_, typename FwdIter, typename T,
                typename F1, typename F2, typename F3, typename F4>
            static hpx::future<R> call(ExPolicy_&& policy, FwdIter first,
                std::size_t count, T&& init, F1&& f1, F2&& f2, F3&& f3, F4&& f4)
            {
                return execution::async_execute(policy.executor(),
                    [first, count, policy = HPX_FORWARD(ExPolicy_, policy),
                        init = HPX_FORWARD(T, init), f1 = HPX_FORWARD(F1, f1),
                        f2 = HPX_FORWARD(F2, f2), f3 = HPX_FORWARD(F3, f3),
                        f4 = HPX_FORWARD(F4, f4)]() mutable -> R {
                        using partitioner_type =
                            scan_static_partitioner<ExPolicy, R, Result1,
                                Result2>;
                        return partitioner_type::call(
                            HPX_FORWARD(ExPolicy_, policy), first, count,
                            HPX_MOVE(init), f1, f2, f3, f4);
                    });
            }
        };
    }    // namespace detail

    ///////////////////////////////////////////////////////////////////////////
    // ExPolicy:    execution policy
    // R:           overall result type
    // Result1:     intermediate result type of first and second step
    // Result2:     intermediate result of the third step
    template <typename ExPolicy, typename R = void, typename Result1 = R,
        typename Result2 = void>
    struct scan_partitioner
      : detail::select_partitioner<typename std::decay<ExPolicy>::type,
            detail::scan_static_partitioner,
            detail::scan_task_static_partitioner>::template apply<R, Result1,
            Result2>
    {
    };
}}}    // namespace hpx::parallel::util
