#!/bin/bash

trace=("a" "b")

### print summary of alpha ###
aplog_dir=(
    "s-alpha-zero-0"
    "s-alpha-zero-1"
    "s-alpha-zero-10"
)

function print_alpha_summary(){
    echo ""
    echo ${aplog_dir[@]}
    for tri in 0 1 ; do
        # printf "${trace[$tri]}"
        echo "${trace[$tri]}"
        for i in 0 1 2 ; do
            file_name="${trace[$tri]}-summary.stats"
            log="logs/${trace[$tri]}/${aplog_dir[$i]}/$file_name"
            if [ $i == 0 ]; then
            # echo $(head -n 1 $log)
            grep Makespan $log | awk -F',' '{print $1, $3, $5, $29, $31, $39, $7, $9, $11}' 
            fi
            grep Makespan $log | awk -F',' '{print $2, $4, $6, $30, $32, $40, $8, $10, $12}' 
            # grep Makespan $log | awk -F',' '{print $1, $3, $5, $29, $31, $39, $7, $9, $11}' 
        done
    done
}

### print summary of srsf, themis, tiresias ###
alog_dir=(
    "srsf"
    "las"
    "themis"
)

function print_a_summary(){
    echo ""
    echo ${alog_dir[@]}
    for tri in 0 1 ; do
        # printf "${trace[$tri]} "
        echo "${trace[$tri]} "
        for i in 0 1 2; do
            # if [ $i -eq 0 ]; then
                file_name="${trace[$tri]}-summary.stats"
            # elif [ $i -eq 1 ]; then
                # file_name="themis${tms_param[$tri]}.log"
            # else
                # file_name="las${trs_param[$tri]}.log"
            # fi

            log="logs/${trace[$tri]}/${alog_dir[$i]}/$file_name"
            # log="logs/${trace[$tri]}/${alog_dir[$i]}/$file_name"
            # echo $log
            if [ $i == 0 ]; then
            # echo $(head -n 1 $log)
            grep Makespan $log | awk -F',' '{print $1, $3, $5, $19, $23, $29, $7, $9, 1}'
            fi
            grep Makespan $log | awk -F',' '{print $2, $4, $6, $20, $24, $30, $8, $10, 1}'
            # grep Makespan $log | awk -F',' '{print $1, $3, $5, $19, $23, $29, $7, $9, 1}'
        done

    done
}

print_alpha_summary
print_a_summary
