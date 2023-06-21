#!/bin/bash -l

# Copyright (c) 2020 ETH Zurich
# Copyright (c) 2022 Hartmut Kaiser
#
# SPDX-License-Identifier: BSL-1.0
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set -ex

hpx_targets=(
    "foreach_report_test"
    "future_overhead_report_test"
    "stream_report_test"
    )
hpx_test_options=(
    "--hpx:ini=hpx.thread_queue.init_threads_count=100 \
    --vector_size=104857 --work_delay=1 \
    --chunk_size=0 --test_count=200"
    "--hpx:ini=hpx.thread_queue.init_threads_count=100 \
    --hpx:queuing=local-priority --test-all \
    --repetitions=40 --futures=207270"
    "--hpx:ini=hpx.thread_queue.init_threads_count=100 \
    --vector_size=518176 --iterations=200 \
    --warmup_iterations=20")


hpx_distributed_targets=(
    "pingpong_performance_report"
)
hpx_distributed_test_options=(
    "--test_count=200"
)


hpx_targets_all=( "${hpx_targets[@]}" "${hpx_distributed_targets[@]}")
# Build binaries for performance tests
${perftests_dir}/driver.py -v -l $logfile build -b release -o build \
    --source-dir ${src_dir} --build-dir ${build_dir} -e $envfile \
    -t "${hpx_targets_all[@]}" ||
    {
        echo 'Build failed'
        configure_build_errors=1
        exit 1
    }


run_test(){
    result=${build_dir}/reports/${executable}.json
    reference=${perftests_dir}/perftest/references/lsu_default/${executable}.json
    result_files+=(${result})
    references_files+=(${reference})
    logfile_tmp=log_perftests_${executable}.tmp

    if [ "${is_distributed}" -eq 0 ]; then
        run_command=("${build_dir}/bin/hpxrun.py ${build_dir}/bin/${executable} -t 4 -- ${test_opts}")
    else
        run_command=("${build_dir}/bin/hpxrun.py ${build_dir}/bin/${executable} -l 2 -t 4 --parcelport mpi -- ${test_opts}")
    fi

    # Run performance tests
    ${perftests_dir}/driver.py -v -l $logfile_tmp perftest run --local True \
        --run_output $result --targets-and-opts "${run_command[@]}" --n_executions $n_executions  ||
        {
            echo 'Running failed'
            test_errors=1
            exit 1
        }
}

n_executions=10
result_files=""
 
# Run tests
index=0
for executable in "${hpx_targets[@]}"; do
    test_opts=${hpx_test_options[$index]}
    is_distributed=0

    run_test

    index=$((index + 1))
done


# Run distributed tests
index=0
for executable in "${hpx_distributed_targets[@]}"; do
    test_opts=${hpx_distributed_test_options[$index]}
    is_distributed=1

    run_test

    index=$((index + 1))
done

# Plot comparison of current result with references
${perftests_dir}/driver.py -v -l $logfile perftest plot compare --references \
    ${references_files[@]} --results ${result_files[@]} \
    -o ${build_dir}/reports/reference-comparison ||
    {
        echo 'Plotting failed: performance drop'
        plot_errors=1
        exit 1
    }
