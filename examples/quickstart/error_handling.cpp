//  Copyright (c) 2007-2012 Hartmut Kaiser
//  Copyright (c)      2011 Bryce Lelbach
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <hpx/hpx.hpp>
#include <hpx/hpx_init.hpp>
#include <hpx/include/iostreams.hpp>

//[error_handling_raise_exception
void raise_exception()
{
    HPX_THROW_EXCEPTION(hpx::no_success, "raise_exception", "simulated error");
}
HPX_PLAIN_ACTION(raise_exception, raise_exception_action);
//]

///////////////////////////////////////////////////////////////////////////////
int hpx_main()
{
    {
        ///////////////////////////////////////////////////////////////////////
        // Error reporting using exceptions
        //[exception_diagnostic_information
        try {
            // invoke raise_exception() which throws an exception
            raise_exception_action do_it;
            do_it(hpx::find_here());
        }
        catch (hpx::exception const& e) {
            // Print just the essential error information.
            hpx::cout << "caught exception: " << e.what() << "\n\n";

            // Print all of the available diagnostic information as stored with
            // the exception.
            hpx::cout << "diagnostic information:"
                << hpx::diagnostic_information(e) << "\n";
        }
        //]

        // Error reporting using exceptions
        //[exception_diagnostic_elements
        try {
            // invoke raise_exception() which throws an exception
            raise_exception_action do_it;
            do_it(hpx::find_here());
        }
        catch (hpx::exception const& e) {
            // Print the elements of the diagnostic information separately
            hpx::cout << "[locality-id]: " << hpx::get_locality_id(e) << "\n";
            hpx::cout << "[hostname]: "    << hpx::get_host_name(e) << "\n";
            hpx::cout << "[pid]: "         << hpx::get_process_id(e) << "\n";
            hpx::cout << "[function]: "    << hpx::get_function_name(e) << "\n";
            hpx::cout << "[file]: "        << hpx::get_file_name(e) << "\n";
            hpx::cout << "[line]: "        << hpx::get_line_number(e) << "\n";
            hpx::cout << "[os-thread]: "   << hpx::get_os_thread(e) << "\n";
            hpx::cout << "[thread-id]: "   << std::hex << hpx::get_thread_id(e) << "\n";
            hpx::cout << "[thread-description]: "
                << hpx::get_thread_description(e) << "\n";

            hpx::cout << hpx::flush;
        }
        //]

        ///////////////////////////////////////////////////////////////////////
        //[error_handling_diagnostic_information
        // Error reporting using error code
        {
            // Create a new error_code instance.
            hpx::error_code ec;

            // If an instance of an error_code is passed as the last argument while
            // invoking the action, the function will not throw in case of an error
            // but store the error information in this error_code instance instead.
            raise_exception_action do_it;
            do_it(hpx::find_here(), ec);

            // Print just the essential error information.
            hpx::cout << "returned error: " << ec.get_message() << "\n";

            // Print all of the available diagnostic information as stored with
            // the exception.
            hpx::cout << "diagnostic information:"
                << hpx::diagnostic_information(ec) << "\n";

            hpx::cout << hpx::flush;
        }
        //]

        //[error_handling_diagnostic_elements
        // Detailed error reporting using error code
        {
            // Create a new error_code instance.
            hpx::error_code ec;

            // Print the elements of the diagnostic information separately
            hpx::cout << "[locality-id]: " << hpx::get_locality_id(ec) << "\n";
            hpx::cout << "[hostname]: "    << hpx::get_host_name(ec) << "\n";
            hpx::cout << "[pid]: "         << hpx::get_process_id(ec) << "\n";
            hpx::cout << "[function]: "    << hpx::get_function_name(ec) << "\n";
            hpx::cout << "[file]: "        << hpx::get_file_name(ec) << "\n";
            hpx::cout << "[line]: "        << hpx::get_line_number(ec) << "\n";
            hpx::cout << "[os-thread]: "   << hpx::get_os_thread(ec) << "\n";
            hpx::cout << "[thread-id]: "   << std::hex << hpx::get_thread_id(ec) << "\n";
            hpx::cout << "[thread-description]: "
                << hpx::get_thread_description(ec) << "\n";

            hpx::cout << hpx::flush;
        }
        //]
    }

    // Initiate shutdown of the runtime system.
    return hpx::finalize();
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[])
{
    return hpx::init(argc, argv);       // Initialize and run HPX.
}
