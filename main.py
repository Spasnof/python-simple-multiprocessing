from multiprocessing import freeze_support
from concurrent.futures import ProcessPoolExecutor
import asyncio 
from random import randrange
import subprocess
import timeit 

# MODE
MODE = 2

# Concurrent executors to perform this work.
MAX_WORKERS = 4
# Some inputs that we can iterate over.
RANDOM_SLEEP_UBOUNDS = [
    11,
    13,
    12,
    23,
]

def sleep_random_mode2(sleep_max):
    import time
    random_number = randrange(0,sleep_max)
    print(f'sleeping {sleep_max}')
    time.sleep(sleep_max)
    print(f'Done sleeping {sleep_max}, great success')




def sleep_random(sleep_max):
    """
    This function can be whatever you want it to be, 
    in this case we are just calling a subcommand to sleep for a few seconds.
    Because sleep is a blocking function it makes for a good example.
    """
    random_number = randrange(0,sleep_max)
    print(f'sleeping {sleep_max}')
    # NOTE that shell=True is not secure, so don't use this for untrusted inputs.
    # See https://docs.python.org/3/library/subprocess.html#security-considerations
    sp = subprocess.run(f'python sleep_random.py {sleep_max}',shell=True)
    print(f'Done sleeping {sleep_max}, exit code is {sp.returncode}')


def main():

    async def run_blocking(executor, task_no, delay):
        print(f'MASTER: Sending to executor blocking task number {task_no}')
        result = await loop.run_in_executor(executor, sleep_random, delay)
        print('MASTER: Well done executor - you seem to have completed '
            'blocking task number ' + str(task_no))

    async def non_blocking():
        tasks = []
        # here we use simple incrementing id's but nothing stopping you from using job uuid's ect.
        task_no = 1
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in RANDOM_SLEEP_UBOUNDS:
                # append the task to an empty array of tasks.
                tasks.append(asyncio.create_task(
                    run_blocking(executor=executor, task_no=task_no, delay=i)))
                task_no += 1
            # if there was an exception prior to task creation we would get it here
            await asyncio.gather(*tasks)

    def blocking():
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i in RANDOM_SLEEP_UBOUNDS:
                executor.submit(sleep_random_mode2, i)
    # instantiate the loop and just run everything inside of that

    if MODE == 1:
        loop = asyncio.get_event_loop()
        def mode1():
            loop.run_until_complete(non_blocking())
        print(timeit.timeit(mode1, number=1))
    elif MODE == 2:
        print(timeit.timeit(blocking,number=1))
    else:
        raise NotImplementedError("I didn't make a mode for that")


if __name__ == '__main__':
    freeze_support()
    main()