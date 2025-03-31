import json
import datetime

def date_to_sec(date_str, start_time):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return int((dt - start_time).total_seconds())

def main():
    f = open("../trace-data/cluster_job_log_formatted.json", "r")
    data = json.load(f)
    f.close()

    # "2017-08-07 00:00:00"
    trace_start_time = datetime.datetime(2017, 8, 7, 0, 0, 0)

    vc = dict()
    valid = 0
    passed_with_zero_gpu = 0
    passed = 0
    failed = 0
    killed = 0
    invalid = 0
    for job in data:
        status = job["status"]
        vc_name = job["vc"]
        if vc_name not in vc:
            vc[vc_name] = []
        job_id = job["jobid"]
        attempts = job["attempts"]
        submitted_time = date_to_sec(job["submitted_time"], trace_start_time)
            
        # computes run time as defined as the delta between
        # the initial attempt's start time and the last attempt's finish time.
        try:
            start_time = date_to_sec(attempts[0]["start_time"], trace_start_time)
        except:
            start_time = None
        try:
            end_time = date_to_sec(attempts[-1]["end_time"], trace_start_time)
        except:
            end_time = None

        try:
            # from "Philly Trace Analysis.ipynb"
            num_of_gpu = sum([len(detail["gpus"]) for detail in attempts[0]["detail"]])
            if num_of_gpu == 0 and status == "Pass":
                passed_with_zero_gpu += 1
        except:
            num_of_gpu == 0
            if status == "Pass":
                passed_with_zero_gpu += 1
        
        if start_time == None or end_time == None or \
            end_time - start_time <= 0 or num_of_gpu == 0:
            invalid += 1
        else:
            valid += 1
            if status == "Pass":
                duration = end_time - start_time
                vc[vc_name].append([submitted_time, num_of_gpu, duration])
                passed += 1
            elif status == "Failed":
                failed += 1
            elif status == "Killed":
                killed += 1

    print(f"valid: {valid} (pass: {passed}, failed: {failed}, killed: {killed}), invalid: {invalid}, total: {valid+invalid}")    
    print(f"vc, #job, min duration, max duration, avg duration, 25%, 50%, 75%, 90%")
    for id in vc.keys():
        jobs = sorted(vc[id], key=lambda x: int(x[0]))
        init_time = jobs[0][0]
        with open(f"vc_durations/{id}-duration.trace", "w") as f:
            for j in jobs:
                for g in reversed([1, 2, 4, 8, 16]):
                    if g <= j[1]:
                        j[1] = g
                        break
                f.write(f"{j[0] - init_time} duration {j[1]} {j[2]}\n")
        jobs = sorted(vc[id], key=lambda x: int(x[2]))
        print(f"{id}, {int(jobs[-1][2])/3600/24}")

if __name__ == '__main__':
    main()
