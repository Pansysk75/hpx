//  Copyright (c) 2017 Bibek Wagle
//
//  SPDX-License-Identifier: BSL-1.0
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <hpx/config.hpp>
#if !defined(HPX_COMPUTE_DEVICE_CODE)
#include <hpx/hpx.hpp>
#include <hpx/hpx_init.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/serialization.hpp>
#include <hpx/iostream.hpp>
#include <hpx/modules/testing.hpp>


#include <complex>
#include <cstddef>
#include <string>
#include <vector>

// Declare the action before implementing it, because it needs to call itself
namespace pingpong { namespace server {
    size_t bounce(size_t n);
}}

HPX_PLAIN_ACTION(pingpong::server::bounce, pingpong_bounce_action);

namespace pingpong { namespace server {
    size_t bounce(size_t n)
    {
        if (n == 0){
            return 0;
        }
        else{
            std::vector<hpx::id_type> localities = hpx::find_all_localities();
            hpx::id_type next_locality = localities[n % localities.size()];
            pingpong_bounce_action action;
            return hpx::async(action, next_locality, n-1).get(); 
        }
    }
}}


void test_pingpong(size_t count){
    pingpong_bounce_action action;
    size_t result = hpx::async(action, hpx::find_here(), count).get(); 
    HPX_ASSERT(result == 0);
}


int hpx_main(hpx::program_options::variables_map& vm)
{

    std::size_t const n_bounces = vm["n_bounces"].as<std::size_t>();
    std::size_t const test_count = vm["test_count"].as<std::size_t>();

    hpx::util::perftests_report("pingpong", "XYZ",
                test_count,
                [&]() { test_pingpong(n_bounces); });

    hpx::util::perftests_print_times();

    
    return hpx::finalize();
}


int main(int argc, char* argv[])
{
    using namespace hpx::program_options;

    options_description cmdline(
        "Usage: " HPX_APPLICATION_STRING " [options]");

    cmdline.add_options()
    ("n_bounces", value<std::size_t>()->default_value(100),
        "the number of bounces")
    ("test_count", value<std::size_t>()->default_value(100),
        "the number of tests to be averaged");

    // Initialize and run HPX
    std::vector<std::string> cfg;

    hpx::init_params init_args;
    init_args.desc_cmdline = cmdline;
    init_args.cfg = cfg;

    return hpx::init(argc, argv, init_args);
}

#endif
