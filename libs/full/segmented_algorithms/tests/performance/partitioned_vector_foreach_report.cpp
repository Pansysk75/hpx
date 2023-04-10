    //  Copyright (c) 2021 ETH Zurich
//  Copyright (c) 2021-2022 Hartmut Kaiser
//  Copyright (c) 2014 Grant Mercer
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)


#include <hpx/config.hpp>
#if !defined(HPX_COMPUTE_DEVICE_CODE)
#include <hpx/hpx.hpp>
#include <hpx/hpx_init.hpp>

#include <hpx/include/parallel_generate.hpp>
#include <hpx/include/parallel_minmax.hpp>
#include <hpx/include/partitioned_vector.hpp>
#include <hpx/iostream.hpp>
#include <hpx/modules/timing.hpp>
#include <hpx/modules/testing.hpp>

#include <hpx/modules/program_options.hpp>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <hpx/include/partitioned_vector_predef.hpp>
// #include <hpx/include/runtime.hpp>


///////////////////////////////////////////////////////////////////////////////

struct func
{
    template <typename T>
    void operator()(T& val) const
    {
        val = 2*val + 1;
    }
};

int hpx_main(hpx::program_options::variables_map& vm)
{
    size_t vector_size = vm["vector_size"].as<size_t>();
    int test_count = vm["test_count"].as<int>();

    hpx::partitioned_vector<int> v(
        vector_size, 0, hpx::container_layout(hpx::find_all_localities()));

    {

        hpx::util::perftests_report("for_each", "sequential_executor",
            test_count,
            [&]() { hpx::for_each(hpx::execution::seq, v.begin(), v.end(), func{}); });

    }

    // {

    // hpx::util::perftests_report("for_each", "parallel_executor",
    //     test_count,
    //     [&]() { hpx::for_each(hpx::execution::par, v.begin(), v.end(), func{}); });

    // }


    hpx::util::perftests_print_times();
    
    return hpx::finalize();
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[])
{
    using namespace hpx::program_options;

    options_description cmdline("usage: " HPX_APPLICATION_STRING " [options]");

    // clang-format off
    cmdline.add_options()
        ("vector_size", value<std::size_t>()->default_value(1000),
            "size of vector")
        ("test_count", value<int>()->default_value(100),
            "number of tests to be averaged")
        ;
    // clang-format on
    hpx::init_params init_args;
    init_args.desc_cmdline = cmdline;
    init_args.cfg = {"hpx.os_threads=all"};

    return hpx::init(argc, argv, init_args);
}
#endif