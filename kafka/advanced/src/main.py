from generators import Runner 
from processors import app
import asyncio


async def run_producer():
    runner = Runner(num_simulators=10)
    await runner.create_simulators()
    await runner.run_simulators()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "producer":
        asyncio.run(run_producer())
    else:
        app.main()


# use faust -A main worker -l info to run producer