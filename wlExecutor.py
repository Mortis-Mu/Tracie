import csv
import time
import threading
import argparse
import sys
from queue import Queue
import subprocess  # !!! 1. 导入 subprocess
import shlex       # !!! 2. 导入 shlex

# ... (bcolors 类保持不变) ...
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

# ... (load_jobs_and_tasks 和 get_sim_time 保持不变) ...
def load_jobs_and_tasks(jobs_file, tasks_file):
    """
    加载作业和任务文件。
    """
    jobs = []
    try:
        with open(jobs_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['job_id'] = int(row['job_id'])
                row['arrival_time_sec'] = float(row['arrival_time_sec'])
                row['task_count'] = int(row['task_count'])
                jobs.append(row)
    except FileNotFoundError:
        print(f"{bcolors.FAIL}错误: 作业文件未找到 '{jobs_file}'{bcolors.ENDC}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"{bcolors.FAIL}错误: 读取作业文件失败: {e}{bcolors.ENDC}", file=sys.stderr)
        sys.exit(1)
        
    tasks_db = {}
    try:
        with open(tasks_file, 'r') as f:
            reader = csv.reader(f)
            next(reader) # 跳过表头
            for row in reader:
                job_id = int(row[0])
                # 将任务到达时间戳从字符串转换为浮点数列表
                task_arrivals = [float(t) for t in row[1:]]
                tasks_db[job_id] = task_arrivals
    except FileNotFoundError:
        print(f"{bcolors.FAIL}错误: 任务文件未找到 '{tasks_file}'{bcolors.ENDC}", file=sys.stderr)
        sys.exit(1)
        
    # 按到达时间对作业进行排序
    jobs.sort(key=lambda j: j['arrival_time_sec'])
    
    return jobs, tasks_db

def get_sim_time(start_time):
    """返回自工作负载开始以来的模拟时间"""
    return time.time() - start_time


# ... (run_uf_task 保持不变, 它用于 UF 作业) ...
def run_uf_task(job_id, task_id, task_arrival_time, job_start_time, sim_task_duration):
    """
    在一个单独的线程中模拟一个UF任务（例如一个Web请求）。
    """
    # 1. 等待任务的相对到达时间
    now = time.time()
    job_elapsed = now - job_start_time
    wait_time = task_arrival_time - job_elapsed
    
    if wait_time > 0:
        time.sleep(wait_time)
        
    task_start = time.time()
    # 模拟日志：任务到达
    print(f"  {get_sim_time(WORKLOAD_START_TIME):.2f}s |   Job {job_id} (UF) -> Task {task_id} 到达。")
    
    # 2. 模拟任务执行
    time.sleep(sim_task_duration)
    
    task_end = time.time()
    print(f"  {get_sim_time(WORKLOAD_START_TIME):.2f}s |   Job {job_id} (UF) <- Task {task_id} 完成 (耗时 {task_end - task_start:.3f}s)。")


# !!! 3. run_job 函数是修改的重点 !!!
def run_job(job, task_arrivals, sim_uf_task_duration, sim_batch_task_duration):
    """
    在一个单独的线程中模拟或执行一个完整的作业。
    """
    job_id = job['job_id']
    app_type = job['app_type']
    job_type = job['job_type']
    task_count = job['task_count']
    job_start_time = time.time() 

    if job_type == "UF":
        # UF 作业 (如 nginx, redis) 保持模拟不变
        print(f"{bcolors.OKBLUE}{get_sim_time(WORKLOAD_START_TIME):.2f}s | [服务启动] Job {job_id} (类型: {job_type}, 应用: {app_type}) 启动，等待 {task_count} 个任务...{bcolors.ENDC}")
        
        task_threads = []
        for i, task_arrival in enumerate(task_arrivals):
            t = threading.Thread(
                target=run_uf_task,
                args=(job_id, i, task_arrival, job_start_time, sim_uf_task_duration)
            )
            t.start()
            task_threads.append(t)
            
        for t in task_threads:
            t.join()
            
        print(f"{bcolors.OKBLUE}{get_sim_time(WORKLOAD_START_TIME):.2f}s | [服务完成] Job {job_id} (UF) 已完成所有 {task_count} 个任务。{bcolors.ENDC}")
        
    elif job_type == "B":
        # --- !!! 这是已修改的批处理作业逻辑 !!! ---
        
        # !!! 关键: 'task_count' 来自 wlGenerator.py，基于 J_D / T_D
        # 我们用它来控制 Hadoop 作业的规模。
        
        # !!! 准备工作: 您必须修改以下路径 !!!
        HADOOP_EXAMPLES_JAR = r"\\wsl.localhost\Ubuntu\home\mortis\hadoop-3.4.1\share\hadoop\mapreduce\hadoop-mapreduce-examples-3.4.1.jar"
        
        command_to_run = None
        
        if app_type == "pi":
            # 用 'task_count' 作为 Map 任务的数量
            # 最后一个参数是每个 map 的样本数 (固定为 1000)
            num_maps = task_count
            samples_per_map = 1000
            command_to_run = f"hadoop jar {HADOOP_EXAMPLES_JAR} pi {num_maps} {samples_per_map}"
            
        elif app_type == "wordcount":
            # 'task_count' 无法直接控制 wordcount。
            # 您必须提前在 HDFS 上准备好 /inputs/wordcount_data
            input_dir = "/inputs/wordcount_data"
            output_dir = f"/outputs/wordcount_{job_id}"
            command_to_run = f"hadoop jar {HADOOP_EXAMPLES_JAR} wordcount {input_dir} {output_dir}"

        elif app_type == "grep":
            # 'task_count' 无法直接控制 grep。
            # 您必须提前在 HDFS 上准备好 /inputs/grep_data
            input_dir = "/inputs/grep_data"
            output_dir = f"/outputs/grep_{job_id}"
            regex = "'Tracie'" # 示例正则表达式
            command_to_run = f"hadoop jar {HADOOP_EXAMPLES_JAR} grep {input_dir} {output_dir} {regex}"
            
        elif app_type == "terasort":
            # Terasort 是 I/O 密集型基准测试
            # 'task_count' 可以完美映射为 'teragen' (数据生成) 的记录数
            # 注意：这会生成数据，然后排序。这是一个两步过程。
            # 为简单起见，我们只运行 teragen（写密集型）
            # 或者您可以运行 terasort（读/网络/写密集型）
            
            # --- 方案 A: 运行 Teragen (用 task_count 控制数据大小) ---
            num_rows = task_count * 1000 # 假设 task_count 太小，我们将其放大
            output_dir = f"/outputs/teragen_{job_id}"
            command_to_run = f"hadoop jar {HADOOP_EXAMPLES_JAR} teragen {num_rows} {output_dir}"
            
            # --- 方案 B: 运行 Terasort (假设数据已在 /inputs/terasort_data) ---
            # input_dir = "/inputs/terasort_data"
            # output_dir = f"/outputs/terasort_{job_id}"
            # command_to_run = f"hadoop jar {HADOOP_EXAMPLES_JAR} terasort {input_dir} {output_dir}"
            
        
        if command_to_run:
            # --- 如果是 Hadoop 作业，则真实执行 ---
            print(f"{bcolors.OKCYAN}{get_sim_time(WORKLOAD_START_TIME):.2f}s | [Hadoop] Job {job_id} (应用: {app_type}) 启动...{bcolors.ENDC}")
            print(f"           L 执行命令: {command_to_run}")
            
            try:
                # 使用 shlex.split 安全地分割命令
                args = shlex.split(command_to_run)
                
                # 使用 subprocess.run()，它会等待命令执行完成
                # 这是正确的，因为这个线程就代表这个作业
                subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                
                job_end_time = time.time()
                print(f"{bcolors.OKGREEN}{get_sim_time(WORKLOAD_START_TIME):.2f}s | [Hadoop] Job {job_id} (应用: {app_type}) 完成 (耗时 {job_end_time - job_start_time:.2f}s)。{bcolors.ENDC}")
            
            except subprocess.CalledProcessError as e:
                job_end_time = time.time()
                print(f"{bcolors.FAIL}{get_sim_time(WORKLOAD_START_TIME):.2f}s | [Hadoop] Job {job_id} (应用: {app_type}) 失败 (耗时 {job_end_time - job_start_time:.2f}s)。{bcolors.ENDC}")
                print(f"           L 错误: {e.stderr.decode('utf-8')}")
            except FileNotFoundError:
                print(f"{bcolors.FAIL}错误：Hadoop 命令未找到。请检查您的 PATH 或 HADOOP_EXAMPLES_JAR 路径。{bcolors.ENDC}")

        else:
            # --- 如果是其他批处理作业 (如 rodinia_kmeans)，则回退到模拟 ---
            print(f"{bcolors.OKCYAN}{get_sim_time(WORKLOAD_START_TIME):.2f}s | [模拟] Job {job_id} (类型: {job_type}, 应用: {app_type}) 启动，处理 {task_count} 个任务...{bcolors.ENDC}")
            
            total_duration = task_count * sim_batch_task_duration
            time.sleep(total_duration)
            
            job_end_time = time.time()
            print(f"{bcolors.OKCYAN}{get_sim_time(WORKLOAD_START_TIME):.2f}s | [模拟] Job {job_id} (B) 完成 (总耗时 {job_end_time - job_start_time:.2f}s)。{bcolors.ENDC}")

# ... (main 函数保持不变) ...
def main():
    global WORKLOAD_START_TIME
    
    parser = argparse.ArgumentParser(description="Tracie 工作负载执行器")
    parser.add_argument('--jobs-file', type=str, default='generated_jobs.csv',
                        help="输入的作业CSV文件")
    parser.adds_argument('--tasks-file', type=str, default='generated_tasks.csv',
                        help="输入的任务CSV文件")
    parser.add_argument('--uf-task-time', type=float, default=0.05,
                        help="模拟UF任务（如请求）的持续时间（秒）")
    parser.add_argument('--batch-task-time', type=float, default=0.1,
                        help="模拟Batch任务的持续时间（秒）")
    
    args = parser.parse_args()

    print("正在加载工作负载文件...")
    jobs, tasks_db = load_jobs_and_tasks(args.jobs_file, args.tasks_file)
    print(f"加载了 {len(jobs)} 个作业。")
    
    if not jobs:
        print("没有可执行的作业。退出。")
        return

    print(f"\n{bcolors.BOLD}--- Tracie 工作负载执行器启动 ---{bcolors.ENDC}")
    print(f"模拟配置: UF任务={args.uf_task_time}s, Batch任务={args.batch_task_time}s")
    print("按回车键开始执行...")
    try:
        input()
    except EOFError:
        pass # 在非交互式环境中自动开始

    WORKLOAD_START_TIME = time.time()
    job_threads = []

    try:
        for job in jobs:
            # 1. 计算作业应该在何时被调度
            job_schedule_time = WORKLOAD_START_TIME + job['arrival_time_sec']
            
            # 2. 等待到达调度时间
            current_time = time.time()
            sleep_duration = job_schedule_time - current_time
            
            if sleep_duration > 0:
                time.sleep(sleep_duration)
                
            # 3. 启动作业（在它自己的线程中，以防主调度循环阻塞）
            job_thread = threading.Thread(
                target=run_job,
                args=(job, tasks_db[job['job_id']], args.uf_task_time, args.batch_task_time)
            )
            job_thread.start()
            job_threads.append(job_thread)
            
        # 4. 等待所有作业线程完成
        print(f"\n{bcolors.HEADER}--- 所有作业已调度，等待执行完成... ---{bcolors.ENDC}")
        for t in job_threads:
            t.join()
            
        print(f"\n{bcolors.BOLD}{bcolors.OKGREEN}--- 工作负载执行完毕 ---{bcolors.ENDC}")
        print(f"总模拟时间: {get_sim_time(WORKLOAD_START_TIME):.2f} 秒")
        
    except KeyboardInterrupt:
        print(f"\n{bcolors.FAIL}--- 用户中断！正在强制退出... ---{bcolors.ENDC}")
        # 在真实的应用中，这里需要更优雅地处理线程关闭
        sys.exit(1)

if __name__ == "__main__":
    main()