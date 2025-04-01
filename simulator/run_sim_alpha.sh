#!/bin/bash
# shell script for running simulator
# set -x

# options:
WORKLOAD=""
SCHEDULE=""
VERBOSE=0

printUsage() {
	echo "Usage: bash run_sim.sh -w <workload> (optional) -s <schedule> -v <verbose> -h"
	echo "    -w workload : workload name to run"
	echo "    (optional)"
	echo "    -s schedule : string val from grml, fifo, srtf, srsf, las, delay"
	echo "                  add this option to run specific one"
	echo "    -t threshold: int value for las queue threshold in minutes"
	echo "    -r round    : int value for themis round length in minutes"
	echo "    -v verbose  : int value 0,1,2"
	echo "                  higher value prints more logs"
	echo "                  (default) 0"
	echo "    -h          : print this usage"
}

#if [ $# -eq 0 ]
#then
#	printUsage
#	exit 0
#fi

# get option
while getopts "w:s:t:r:v:h" opt; do
	case $opt in
		w)
			WORKLOAD=$OPTARG
			;;
		s)
			SCHEDULE=$OPTARG
			;;
		t)
			TRSD=$OPTARG
			;;
		r)
			RL=$OPTARG
			;;
		v)
			VERBOSE=$OPTARG
			;;
		h)
			printUsage
			exit 0
			;;
	esac
done


BASEDIR=$( cd "$(dirname $0)" && pwd)
# TIME_STAMP=$(date)

if [ ! -z $SCHEDULE ]
then
	schedulers=$SCHEDULE
else
	# schedulers=("srsf" "las" "themis")
	# schedulers=("srsf")
	# schedulers=("themis")
	# schedulers=("las")

	# schedulers=("s-srsf" "s-themis" "s-tiresias") 
	# schedulers=("s-srsf")
	# schedulers=("s-tiresias") 
	# schedulers=("s-themis") 

	# schedulers=("s-alpha-zero" "s-alpha-one" "s-mm")
	# schedulers=("s-alpha-one")
	schedulers=("s-alpha-zero")
	# schedulers=("s-mm")
	# schedulers=("grdl-opt")
	# schedulers=("grdl-srsf")
fi

if [ ! -z $WORKLOAD ]
then
	WORKLOADS=$WORKLOAD
	TRSDS=$TRSD
	ROUNDS=$RL
else

	TRSDS=( 980 1440 )
	ROUNDS=( 45 80 )

	STRSDS=( 1040 1000 )
	LEASES=( 20 10 )

	WORKLOADS=("a" "b")

fi

# NUMWL=${#WORKLOADS[@]}
# for (( i=0; i<${NUMWL}; i++));
for WORKLOAD in ${WORKLOADS[@]};
do
	if [[ "$WORKLOAD" == "a" ]]; then
		i=0
	elif [[ "$WORKLOAD" == "b" ]]; then
		i=1
	elif [[ "$WORKLOAD" == "al" ]]; then
		i=2
	elif [[ "$WORKLOAD" == "bl" ]]; then
		i=3
	else
		printf '[Error] parameter for the workload %s is not set.\n' "$WORKLOAD"
		# exit 1
	fi

	# WORKLOAD=${WORKLOADS[$i]}
	TRSD=${TRSDS[$i]}
	PKNOB=0
	RL=${ROUNDS[$i]}
	# RL=60

	echo "Run $WORKLOAD with trsd=${TRSD} pknob=${PKNOB} rl=${RL}"

	WL_DIR="${BASEDIR}/workloads/${WORKLOAD}"
	WL_FILE="${WL_DIR}/${WORKLOAD}.trace"
	if [ ! -f $WL_FILE ]
	then
		echo "File $WL_FILE does not exist"
		exit 1
	fi

#echo $(ls "${workload_dir}/*.topology" | wc -l)
#echo $(find ${workload_dir} -name "*.topology" -type f | wc -l)
	num_topo=$(find ${WL_DIR} -name "*.topology" -type f | wc -l)
	if [ $num_topo -eq 0 ]
	then
		TOPOLOGY_FILE="dumboK80s.topology"
	elif [ $num_topo -eq 1 ]
	then
		TOPOLOGY_FILE=$(find ${WL_DIR} -name "*.topology" -type f)
	else
		echo "Too many topology files in ${WL_DIR}"
		exit 1
	fi


	round=0
	rule=las
#	rule=rr
	
	loc=10
	loc_trsd=1.5
	
	ratio=0
	knob=0.01

	# s-themis params
	# stms_filter=0.8
	# stms_filter=0.2
	stms_filter=0.0
	# stms_filter=0.1
	# stms_lease=1800000
	# stms_lease=65
	stms_lease=25
	stms_lease=10
	stms_lease=${LEASES[$i]}

	# s-tiresias params
	strs_qtrsd=60
	# strs_qtrsd=${TRSD}
	strs_qtrsd=25000
	strs_qtrsd=610
	strs_qtrsd=740
	strs_qtrsd=${STRSDS[$i]}

	LOG_DIR="${BASEDIR}/logs"
	#for knob in 1 1.5 2 2.5
	#for ratio in 0 0.25 0.5 0.75 1
	#do
	# loc=10
	loc=0
	# for loc_sd_rule in "as" "nn" "lw" "phase"; do
	for loc_sd_rule in "phase" ; do
		for al in 0 1 10
		# for al in 0
		do
			for schedule in ${schedulers[*]}; do

				dir="${schedule}-${al}"

				SIM_LOG_DIR=${LOG_DIR}/${WORKLOAD}/${dir}
				if [ ! -d $SIM_LOG_DIR ]; then
					mkdir -p $SIM_LOG_DIR
				fi

				ad_file="${BASEDIR}/simulator/time_action.txt"
				cd ${BASEDIR}

				echo "Running $schedule ${WORKLOAD}"
				SIM_LOG_FILE="${SIM_LOG_DIR}/${WORKLOAD}-${schedule}.log"
	
				TIME_STAMP=$(date)
				./simulate \
						-ad ${ad_file} \
						-q $((${TRSD} * 60000)) -p ${PKNOB}\
						-r ${RL} \
						--alpha ${al} \
						--loc-sd-rule ${loc_sd_rule} \
						--locality-interval ${loc} --locality-threshold ${loc_trsd} \
						--sim-log-dir ${SIM_LOG_DIR} \
						-t $WL_FILE -c $TOPOLOGY_FILE -s $schedule \
						-v $VERBOSE 2>&1 | tee $SIM_LOG_FILE

				RESULT_FILE=${SIM_LOG_DIR}/${WORKLOAD}-results.stats
				SUMMARY_FILE=${SIM_LOG_DIR}/${WORKLOAD}-summary.stats

				job_count=$(grep -cv '^\s*$' ${WL_FILE})
				echo $TIME_STAMP | tee $RESULT_FILE $SUMMARY_FILE
				echo "" | tee -a $RESULT_FILE $SUMMARY_FILE
				echo ${SIM_LOG_FILE##*/} | tee -a $RESULT_FILE $SUMMARY_FILE
				tail -n $(($job_count + 12)) $SIM_LOG_FILE | grep -v Debug | tee -a $RESULT_FILE
				tail -n 9 $SIM_LOG_FILE | tee -a $SUMMARY_FILE
			done
		done
	done
done