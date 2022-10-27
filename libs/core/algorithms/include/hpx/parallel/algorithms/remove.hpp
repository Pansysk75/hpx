//  Copyright (c) 2017 Taeguk Kwon
//  Copyright (c) 2021 Giannis Gonidelis
//  Copyright (c) 2017-2022 Hartmut Kaiser
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/// \file parallel/algorithms/remove.hpp

#pragma once

#if defined(DOXYGEN)
namespace hpx {

    /////////////////////////////////////////////////////////////////////////////
    /// Removes all elements satisfying specific criteria from the range
    /// [first, last) and returns a past-the-end iterator for the new
    /// end of the range. This version removes all elements that are
    /// equal to \a value.
    ///
    /// \note   Complexity: Performs not more than \a last - \a first
    ///         assignments, exactly \a last - \a first applications of
    ///         the operator==().
    ///
    /// \tparam FwdIter     The type of the source iterators used (deduced).
    ///                     This iterator type must meet the requirements of an
    ///                     forward iterator.
    /// \tparam T           The type of the value to remove (deduced).
    ///                     This value type must meet the requirements of
    ///                     \a CopyConstructible.
    ///
    /// \param first        Refers to the beginning of the sequence of elements
    ///                     the algorithm will be applied to.
    /// \param last         Refers to the end of the sequence of elements the
    ///                     algorithm will be applied to.
    /// \param value        Specifies the value of elements to remove.
    ///
    /// The assignments in the parallel \a remove algorithm
    /// execute in sequential order in the calling thread.
    ///
    /// \returns  The \a remove algorithm returns a \a FwdIter.
    ///           The \a remove algorithm returns the iterator to the new end
    ///           of the range.
    ///
    template <typename FwdIter,
        typename T = typename std::iterator_traits<FwdIter>::value_type>
    FwdIter remove(FwdIter first, FwdIter last, T const& value);

    /////////////////////////////////////////////////////////////////////////////
    /// Removes all elements satisfying specific criteria from the range
    /// [first, last) and returns a past-the-end iterator for the new
    /// end of the range. This version removes all elements that are
    /// equal to \a value. Executed according to the policy.
    ///
    /// \note   Complexity: Performs not more than \a last - \a first
    ///         assignments, exactly \a last - \a first applications of
    ///         the operator==().
    ///
    /// \tparam ExPolicy    The type of the execution policy to use (deduced).
    ///                     It describes the manner in which the execution
    ///                     of the algorithm may be parallelized and the manner
    ///                     in which it executes the assignments.
    /// \tparam FwdIter     The type of the source iterators used (deduced).
    ///                     This iterator type must meet the requirements of an
    ///                     forward iterator.
    /// \tparam T           The type of the value to remove (deduced).
    ///                     This value type must meet the requirements of
    ///                     \a CopyConstructible.
    ///
    /// \param policy       The execution policy to use for the scheduling of
    ///                     the iterations.
    /// \param first        Refers to the beginning of the sequence of elements
    ///                     the algorithm will be applied to.
    /// \param last         Refers to the end of the sequence of elements the
    ///                     algorithm will be applied to.
    /// \param value        Specifies the value of elements to remove.
    ///
    /// The assignments in the parallel \a remove algorithm invoked with
    /// an execution policy object of type \a sequenced_policy
    /// execute in sequential order in the calling thread.
    ///
    /// The assignments in the parallel \a remove algorithm invoked with
    /// an execution policy object of type \a parallel_policy or
    /// \a parallel_task_policy are permitted to execute in an unordered
    /// fashion in unspecified threads, and indeterminately sequenced
    /// within each thread.
    ///
    /// \returns  The \a remove algorithm returns a \a hpx::future<FwdIter>
    ///           if the execution policy is of type
    ///           \a sequenced_task_policy or \a parallel_task_policy and
    ///           returns \a FwdIter otherwise.
    ///           The \a remove algorithm returns the iterator to the new end
    ///           of the range.
    ///
    template <typename ExPolicy, typename FwdIter,
        typename T = typename std::iterator_traits<FwdIter>::value_type>
    typename parallel::util::detail::algorithm_result<ExPolicy, FwdIter>::type
    remove(ExPolicy&& policy, FwdIter first, FwdIter last, T const& value);

    /// Removes all elements satisfying specific criteria from the range
    /// [first, last) and returns a past-the-end iterator for the new
    /// end of the range. This version removes all elements for which predicate
    /// \a pred returns true.
    ///
    /// \note   Complexity: Performs not more than \a last - \a first
    ///         assignments, exactly \a last - \a first applications of
    ///         the predicate \a pred.
    ///
    /// \tparam FwdIter     The type of the source iterators used (deduced).
    ///                     This iterator type must meet the requirements of an
    ///                     forward iterator.
    /// \tparam Pred        The type of the function/function object to use
    ///                     (deduced). Unlike its sequential form, the parallel
    ///                     overload of \a remove_if requires \a Pred to meet the
    ///                     requirements of \a CopyConstructible.
    ///
    /// \param first        Refers to the beginning of the sequence of elements
    ///                     the algorithm will be applied to.
    /// \param last         Refers to the end of the sequence of elements the
    ///                     algorithm will be applied to.
    /// \param pred         Specifies the function (or function object) which
    ///                     will be invoked for each of the elements in the
    ///                     sequence specified by [first, last).This is an
    ///                     unary predicate which returns \a true for the
    ///                     required elements. The signature of this predicate
    ///                     should be equivalent to:
    ///                     \code
    ///                     bool pred(const Type &a);
    ///                     \endcode \n
    ///                     The signature does not need to have const&, but
    ///                     the function must not modify the objects passed to
    ///                     it. The type \a Type must be such that an object of
    ///                     type \a FwdIter can be dereferenced and then
    ///                     implicitly converted to Type.
    ///
    /// The assignments in the parallel \a remove_if algorithm
    /// execute in sequential order in the calling thread.
    ///
    /// \returns  The \a remove_if algorithm returns a \a FwdIter.
    ///           The \a remove_if algorithm returns the iterator to the new end
    ///           of the range.
    ///
    template <typename FwdIter, typename Pred>
    FwdIter remove_if(FwdIter first, FwdIter last, Pred&& pred);

    /// Removes all elements satisfying specific criteria from the range
    /// [first, last) and returns a past-the-end iterator for the new
    /// end of the range. This version removes all elements for which predicate
    /// \a pred returns true. Executed according to the policy.
    ///
    /// \note   Complexity: Performs not more than \a last - \a first
    ///         assignments, exactly \a last - \a first applications of
    ///         the predicate \a pred.
    ///
    /// \tparam ExPolicy    The type of the execution policy to use (deduced).
    ///                     It describes the manner in which the execution
    ///                     of the algorithm may be parallelized and the manner
    ///                     in which it executes the assignments.
    /// \tparam FwdIter     The type of the source iterators used (deduced).
    ///                     This iterator type must meet the requirements of an
    ///                     forward iterator.
    /// \tparam Pred        The type of the function/function object to use
    ///                     (deduced). Unlike its sequential form, the parallel
    ///                     overload of \a remove_if requires \a Pred to meet the
    ///                     requirements of \a CopyConstructible.
    ///
    /// \param policy       The execution policy to use for the scheduling of
    ///                     the iterations.
    /// \param first        Refers to the beginning of the sequence of elements
    ///                     the algorithm will be applied to.
    /// \param last         Refers to the end of the sequence of elements the
    ///                     algorithm will be applied to.
    /// \param pred         Specifies the function (or function object) which
    ///                     will be invoked for each of the elements in the
    ///                     sequence specified by [first, last).This is an
    ///                     unary predicate which returns \a true for the
    ///                     required elements. The signature of this predicate
    ///                     should be equivalent to:
    ///                     \code
    ///                     bool pred(const Type &a);
    ///                     \endcode \n
    ///                     The signature does not need to have const&, but
    ///                     the function must not modify the objects passed to
    ///                     it. The type \a Type must be such that an object of
    ///                     type \a FwdIter can be dereferenced and then
    ///                     implicitly converted to Type.
    ///
    /// The assignments in the parallel \a remove_if algorithm invoked with
    /// an execution policy object of type \a sequenced_policy
    /// execute in sequential order in the calling thread.
    ///
    /// The assignments in the parallel \a remove_if algorithm invoked with
    /// an execution policy object of type \a parallel_policy or
    /// \a parallel_task_policy are permitted to execute in an unordered
    /// fashion in unspecified threads, and indeterminately sequenced
    /// within each thread.
    ///
    /// \returns  The \a remove_if algorithm returns a \a hpx::future<FwdIter>
    ///           if the execution policy is of type
    ///           \a sequenced_task_policy or \a parallel_task_policy and
    ///           returns \a FwdIter otherwise.
    ///           The \a remove_if algorithm returns the iterator to the new end
    ///           of the range.
    ///
    template <typename ExPolicy, typename FwdIter, typename Pred>
    typename parallel::util::detail::algorithm_result<ExPolicy, FwdIter>::type
    remove_if(ExPolicy&& policy, FwdIter first, FwdIter last, Pred&& pred);

}    // namespace hpx

#else    // DOXYGEN

#include <hpx/config.hpp>
#include <hpx/concepts/concepts.hpp>
#include <hpx/functional/invoke.hpp>
#include <hpx/iterator_support/traits/is_iterator.hpp>
#include <hpx/parallel/util/detail/sender_util.hpp>
#include <hpx/type_support/unused.hpp>

#include <hpx/algorithms/traits/projected.hpp>
#include <hpx/execution/algorithms/detail/is_negative.hpp>
#include <hpx/execution/algorithms/detail/predicates.hpp>
#include <hpx/executors/execution_policy.hpp>
#include <hpx/parallel/algorithms/detail/dispatch.hpp>
#include <hpx/parallel/algorithms/detail/find.hpp>
#include <hpx/parallel/algorithms/detail/transfer.hpp>
#include <hpx/parallel/util/detail/algorithm_result.hpp>
#include <hpx/parallel/util/detail/clear_container.hpp>
#include <hpx/parallel/util/foreach_partitioner.hpp>
#include <hpx/parallel/util/invoke_projected.hpp>
#include <hpx/parallel/util/loop.hpp>
#include <hpx/parallel/util/partitioner.hpp>
#include <hpx/parallel/util/projection_identity.hpp>
#include <hpx/parallel/util/transfer.hpp>
#include <hpx/parallel/util/zip_iterator.hpp>

#if !defined(HPX_HAVE_CXX17_SHARED_PTR_ARRAY)
#include <boost/shared_array.hpp>
#endif

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace hpx { namespace parallel { inline namespace v1 {
    /////////////////////////////////////////////////////////////////////////////
    // remove_if
    namespace detail {
        /// \cond NOINTERNAL

        template <typename Iter, typename Sent, typename Pred, typename Proj>
        Iter sequential_remove_if(Iter first, Sent last, Pred pred, Proj proj)
        {
            first = hpx::parallel::v1::detail::sequential_find_if<
                hpx::execution::sequenced_policy>(first, last, pred, proj);

            if (first != last)
                for (Iter i = first; ++i != last;)
                    if (!HPX_INVOKE(pred, HPX_INVOKE(proj, *i)))
                    {
                        *first++ = HPX_MOVE(*i);
                    }
            return first;
        }

        template <typename FwdIter>
        struct remove_if : public detail::algorithm<remove_if<FwdIter>, FwdIter>
        {
            remove_if()
              : remove_if::algorithm("remove_if")
            {
            }

            template <typename ExPolicy, typename Iter, typename Sent,
                typename Pred, typename Proj>
            static Iter sequential(
                ExPolicy, Iter first, Sent last, Pred&& pred, Proj&& proj)
            {
                return sequential_remove_if(first, last,
                    HPX_FORWARD(Pred, pred), HPX_FORWARD(Proj, proj));
            }

            template <typename ExPolicy, typename Iter, typename Sent,
                typename Pred, typename Proj>
            static typename util::detail::algorithm_result<ExPolicy, Iter>::type
            parallel(ExPolicy&& policy, Iter first, Sent last, Pred&& pred,
                Proj&& proj)
            {
                typedef hpx::util::zip_iterator<Iter, bool*> zip_iterator;
                typedef util::detail::algorithm_result<ExPolicy, Iter>
                    algorithm_result;
                typedef typename std::iterator_traits<Iter>::difference_type
                    difference_type;

                difference_type count = detail::distance(first, last);

                if (count == 0)
                    return algorithm_result::get(HPX_MOVE(first));

#if defined(HPX_HAVE_CXX17_SHARED_PTR_ARRAY)
                std::shared_ptr<bool[]> flags(new bool[count]);
#else
                boost::shared_array<bool> flags(new bool[count]);
#endif

                using hpx::get;
                using hpx::util::make_zip_iterator;

                // Note: replacing the invoke() with HPX_INVOKE()
                // below makes gcc generate errors
                auto f1 = [pred = HPX_FORWARD(Pred, pred),
                              proj = HPX_FORWARD(Proj, proj)](
                              zip_iterator part_begin,
                              std::size_t part_size) -> void {
                    // MSVC complains if pred or proj is captured by ref below
                    util::loop_n<std::decay_t<ExPolicy>>(part_begin, part_size,
                        [pred, proj](zip_iterator it) mutable {
                            bool f = hpx::invoke(
                                pred, hpx::invoke(proj, get<0>(*it)));

                            get<1>(*it) = f;
                        });
                };

                auto f2 = [flags, first, count](
                              auto&& results) mutable -> Iter {
                    HPX_UNUSED(results);

                    auto part_begin = make_zip_iterator(first, flags.get());
                    auto dest = first;
                    auto part_size = count;

                    using execution_policy_type = std::decay_t<ExPolicy>;
                    if (dest == get<0>(part_begin.get_iterator_tuple()))
                    {
                        // Self-assignment must be detected.
                        util::loop_n<execution_policy_type>(
                            part_begin, part_size, [&dest](zip_iterator it) {
                                if (!get<1>(*it))
                                {
                                    if (dest != get<0>(it.get_iterator_tuple()))
                                        *dest++ = HPX_MOVE(get<0>(*it));
                                    else
                                        ++dest;
                                }
                            });
                    }
                    else
                    {
                        // Self-assignment can't be performed.
                        util::loop_n<execution_policy_type>(
                            part_begin, part_size, [&dest](zip_iterator it) {
                                if (!get<1>(*it))
                                    *dest++ = HPX_MOVE(get<0>(*it));
                            });
                    }
                    return dest;
                };

                return util::partitioner<ExPolicy, Iter, void>::call(
                    HPX_FORWARD(ExPolicy, policy),
                    make_zip_iterator(first, flags.get()), count, HPX_MOVE(f1),
                    HPX_MOVE(f2));
            }
        };
        /// \endcond
    }    // namespace detail
}}}      // namespace hpx::parallel::v1

namespace hpx {
    ///////////////////////////////////////////////////////////////////////////
    // CPO for hpx::remove_if
    inline constexpr struct remove_if_t final
      : hpx::detail::tag_parallel_algorithm<remove_if_t>
    {
        // clang-format off
        template <typename FwdIter,
            typename Pred, HPX_CONCEPT_REQUIRES_(
                hpx::traits::is_iterator<FwdIter>::value &&
                hpx::is_invocable_v<Pred,
                    typename std::iterator_traits<FwdIter>::value_type
                >
            )>
        // clang-format on
        friend FwdIter tag_fallback_invoke(
            hpx::remove_if_t, FwdIter first, FwdIter last, Pred&& pred)
        {
            static_assert((hpx::traits::is_forward_iterator<FwdIter>::value),
                "Required at least forward iterator.");

            return hpx::parallel::v1::detail::remove_if<FwdIter>().call(
                hpx::execution::sequenced_policy{}, first, last,
                HPX_FORWARD(Pred, pred),
                hpx::parallel::util::projection_identity());
        }

        // clang-format off
        template <typename ExPolicy, typename FwdIter,
            typename Pred, HPX_CONCEPT_REQUIRES_(
                hpx::is_execution_policy<ExPolicy>::value &&
                hpx::traits::is_iterator<FwdIter>::value &&
                hpx::is_invocable_v<Pred,
                    typename std::iterator_traits<FwdIter>::value_type
                >
            )>
        // clang-format on
        friend typename parallel::util::detail::algorithm_result<ExPolicy,
            FwdIter>::type
        tag_fallback_invoke(hpx::remove_if_t, ExPolicy&& policy, FwdIter first,
            FwdIter last, Pred&& pred)
        {
            static_assert((hpx::traits::is_forward_iterator<FwdIter>::value),
                "Required at least forward iterator.");

            return hpx::parallel::v1::detail::remove_if<FwdIter>().call(
                HPX_FORWARD(ExPolicy, policy), first, last,
                HPX_FORWARD(Pred, pred),
                hpx::parallel::util::projection_identity());
        }

    } remove_if{};

    ///////////////////////////////////////////////////////////////////////////
    // CPO for hpx::remove
    inline constexpr struct remove_t final
      : hpx::detail::tag_parallel_algorithm<remove_t>
    {
    private:
        // clang-format off
        template <typename FwdIter,
            typename T = typename std::iterator_traits<FwdIter>::value_type,
            HPX_CONCEPT_REQUIRES_(
                hpx::traits::is_iterator<FwdIter>::value
            )>
        // clang-format on
        friend FwdIter tag_fallback_invoke(
            hpx::remove_t, FwdIter first, FwdIter last, T const& value)
        {
            typedef typename std::iterator_traits<FwdIter>::value_type Type;

            return hpx::remove_if(hpx::execution::seq, first, last,
                [value](Type const& a) -> bool { return value == a; });
        }

        // clang-format off
        template <typename ExPolicy, typename FwdIter,
            typename T = typename std::iterator_traits<FwdIter>::value_type,
            HPX_CONCEPT_REQUIRES_(
                hpx::is_execution_policy<ExPolicy>::value &&
                hpx::traits::is_iterator<FwdIter>::value
            )>
        // clang-format on
        friend typename parallel::util::detail::algorithm_result<ExPolicy,
            FwdIter>::type
        tag_fallback_invoke(hpx::remove_t, ExPolicy&& policy, FwdIter first,
            FwdIter last, T const& value)
        {
            typedef typename std::iterator_traits<FwdIter>::value_type Type;

            return hpx::remove_if(HPX_FORWARD(ExPolicy, policy), first, last,
                [value](Type const& a) -> bool { return value == a; });
        }
    } remove{};
}    // namespace hpx

#endif    // DOXYGEN
