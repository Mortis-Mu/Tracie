import argparse
import json
import csv
import random
import sys
from scipy import stats

def get_sampler(pdf_info):
    """
    根据配置文件中的PDF信息，返回一个随机采样函数。
    """
    pdf_type = pdf_info["type"]
    params = pdf_info["params"]
    
    try:
        # 获取scipy.stats中的分布对象
        dist_obj = getattr(stats, pdf_type)
        # 使用参数初始化分布
        dist = dist_obj(*params)
        # 返回一个函数，该函数在被调用时返回一个随机样本
        # .rvs(1)[0] 表示获取1个随机变量
        return lambda: max(0, dist.rvs(1)[0])
    except AttributeError:
        print(f"错误：不支持的PDF类型 '{pdf_type}'", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"错误：初始化PDF '{pdf_type}' 失败，参数 {params}: {e}", file=sys.stderr)
        sys.exit(1)

def load_profile(profile_path):
    """
    加载JSON配置文件。
    """
    try:
        with open(profile_path, 'r') as f:
            profile = json.load(f)
            # 为配置文件中的每个参数创建采样器
            samplers = {}
            for param, pdf_info in profile["parameters"].items():
                samplers[param] = get_sampler(pdf_info)
            profile["samplers"] = samplers
            return profile
    except FileNotFoundError:
        print(f"错误：配置文件未找到 '{profile_path}'", file=sys.stderr)
        sys.exit(1)

def generate_workload(profile, num_jobs, w_sat_factor, j_sd_factor):
    """
    工作负载生成器 (实现伪代码1 )。
    这是一个生成器函数，它会 'yield' (产生) 每个作业的信息。
    """
    print(f"开始生成 {num_jobs} 个作业...")
    
    # 从配置文件中获取采样器和参数
    samplers = profile["samplers"]
    app_pool = profile["app_pool"]
    P_B = profile["P_B"] # 批处理作业的概率 
    
    t_total = 0.0 # 伪代码中的 t_total 
    
    for job_id in range(num_jobs):
        # 1. 决定作业类型 (B 或 UF)
        if random.random() < P_B:
            job_type = "B"
            J_D_sampler = samplers["J_D_B"]
            T_D_sampler = samplers["T_D_B"]
            J_AT_sampler = samplers["J_AT_B"]
            T_AT_sampler = samplers["T_AT_B"]
        else:
            job_type = "UF"
            J_D_sampler = samplers["J_D_UF"]
            T_D_sampler = samplers["T_D_UF"]
            J_AT_sampler = samplers["J_AT_UF"]
            T_AT_sampler = samplers["T_AT_UF"]
            
        # 2. 从PDF中采样
        J_D_sample = J_D_sampler() # 作业持续时间
        T_D_sample = T_D_sampler()
        J_AT_sample = J_AT_sampler() # 作业到达间隔
        
        # 确保任务持续时间 > 0，避免除零
        T_D_sample = max(T_D_sample, 1e-6)
        
        # 3. 计算任务数 (JN)
        J_N_calculated = J_D_sample / T_D_sample
        
        # 4. 应用缩放因子 [cite: 194-198]
        # 应用 -wSat 缩放到达时间 
        scaled_arrival_time = t_total + (J_AT_sample * w_sat_factor)
        t_total = scaled_arrival_time # 更新 t_total
        
        # 应用 -jSD 缩放任务数 
        J_N_scaled = int(max(1, J_N_calculated * j_sd_factor))
        
        # 5. 选择一个应用程序
        app_type = random.choice(app_pool)
        
        # 6. 生成任务到达时间 [cite: 209]
        task_arrivals = []
        task_time_within_job = 0.0
        for _ in range(J_N_scaled):
            task_inter_arrival = T_AT_sampler()
            task_time_within_job += task_inter_arrival
            task_arrivals.append(round(task_time_within_job, 6))
            
        # 7. 产生作业及其任务
        yield (job_id, scaled_arrival_time, job_type, app_type, J_N_scaled, task_arrivals)

def main():
    # 1. 设置命令行参数解析 [cite: 192, 195, 196, 198]
    parser = argparse.ArgumentParser(description="Tracie 工作负载生成器 [cite: 191]")
    parser.add_argument('-p', '--profile', type=str, required=True, 
                        help="要使用的工作负载配置文件 (例如 'Google 2011.json') ")
    parser.add_argument('-n', '--num_jobs', type=int, required=True, 
                        help="要生成的作业总数 ")
    parser.add_argument('-wSat', '--w_sat', type=float, default=1.0, 
                        help="作业到达时间缩放因子 ")
    parser.add_argument('-jSD', '--j_sd', type=float, default=1.0, 
                        help="作业持续时间(任务数)缩放因子 ")
    
    args = parser.parse_args()

    # 2. 加载配置文件
    profile = load_profile(args.profile)
    print(f"已加载配置文件: {profile['name']}")

    # 3. 定义输出文件名 [cite: 209]
    jobs_file = 'generated_jobs.csv'
    tasks_file = 'generated_tasks.csv'
    
    # 4. 打开文件并写入
    try:
        with open(jobs_file, 'w', newline='') as jf, open(tasks_file, 'w', newline='') as tf:
            job_writer = csv.writer(jf)
            task_writer = csv.writer(tf)
            
            # 写入表头
            job_writer.writerow(["job_id", "arrival_time_sec", "job_type", "app_type", "task_count"])
            task_writer.writerow(["job_id", "task_arrival_timestamps_within_job"])
            
            # 5. 循环生成器并写入文件 [cite: 205-209]
            job_gen = generate_workload(profile, args.num_jobs, args.w_sat, args.j_sd)
            
            for job_id, arrival, j_type, app, t_count, task_arrivals in job_gen:
                # 写入作业文件
                job_writer.writerow([job_id, round(arrival, 6), j_type, app, t_count])
                
                # 写入任务文件
                task_writer.writerow([job_id] + task_arrivals)

    except IOError as e:
        print(f"错误：写入文件失败: {e}", file=sys.stderr)
        sys.exit(1)
        
    print(f"\n成功！已生成 {args.num_jobs} 个作业。")
    print(f"作业序列已保存到: {jobs_file}")
    print(f"任务序列已保存到: {tasks_file}")

if __name__ == "__main__":
    main()