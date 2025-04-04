import subprocess


def run_script(
        path,
        command,
        arg
):
    try:
        subprocess.run([command, path, arg], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running {path}: {e}")


script_list = [
    "download_index_chain",
    "download_time_series",
    "download_index_stats",
    "download_constituents_stats",
    ]
script_list = [name + '.py' for name in script_list]

api_key = 'you-API-key'
run_command = 'python'

for script_path in script_list:
    print(f"Running script: {script_path}\n")
    run_script(path=script_path, command=run_command, arg=api_key)
