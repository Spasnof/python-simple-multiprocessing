from multiprocessing import freeze_support
from concurrent.futures import ProcessPoolExecutor
import asyncio 
import time
import random
import subprocess

RANDOM_SLEEP = [
    11,
    13,
    12,
    23,
    13,
    15
]

def sleep_random_v2(sleep_max):
    rn = random.randrange(0,sleep_max)
    print(f'sleeping {rn}')
    sp = subprocess.run(f'python sleep_random.py {rn}',shell=True)
    print(f'done sleeping {rn}, exit code is {sp.returncode}')

def sleep_random(sleep_max):
    rn = random.randrange(0,sleep_max)
    print(f'sleeping {rn}')
    time.sleep(rn)
    print(f'done sleeping {rn}')

def blocking(delay):
    time.sleep(delay+1)
    return('EXECUTOR: Completed blocking task number ' + str(delay+1))

def main():

    async def mygen(u: int = 2):
        i = 0
        while i < u:
            yield i
            i += 1



    async def run_blocking(executor, task_no, delay):
        print('MASTER: Sending to executor blocking task number '
            + str(task_no))
        result = await loop.run_in_executor(executor, sleep_random_v2, delay)
        print(result)
        print('MASTER: Well done executor - you seem to have completed '
            'blocking task number ' + str(task_no))

    async def non_blocking(loop):
        tasks = []
        task_no = 1
        with ProcessPoolExecutor(max_workers=5) as executor:
            for i in RANDOM_SLEEP:
                # print(i)
                # spawn the task and let it run in the background
                tasks.append(asyncio.create_task(
                    run_blocking(executor=executor, task_no=task_no, delay=i)))
                task_no += 1
            # if there was an exception, retrieve it now
            await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(non_blocking(loop))


if __name__ == '__main__':
    freeze_support()
    main()