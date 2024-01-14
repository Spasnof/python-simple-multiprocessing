from multiprocessing import freeze_support
from concurrent.futures import ProcessPoolExecutor
import asyncio 
from random import randrange
import subprocess
 
# Concurrent executors to perform this work.
MAX_WORKERS = 2
# Some inputs that we can iterate over.
RANDOM_SLEEP_UBOUNDS = [
    11,
    13,
    12,
    23,
]

def sleep_random(sleep_max):
    """
    This function can be whatever you want it to be, 
    in this case we are just calling a subcommand to sleep for a few seconds.
    Because sleep is a blocking function it makes for a good example.
    """
    random_number = randrange(0,sleep_max)
    print(f'sleeping {random_number}')
    # NOTE that shell=True is not secure, so don't use this for untrusted inputs.
    # See https://docs.python.org/3/library/subprocess.html#security-considerations
    sp = subprocess.run(f'python sleep_random.py {random_number}',shell=True)
    print(f'Done sleeping {random_number}, exit code is {sp.returncode}')


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

    # instantiate the loop and just run everything inside of that
    loop = asyncio.get_event_loop()
    loop.run_until_complete(non_blocking())


if __name__ == '__main__':
    freeze_support()
    main()